#!/usr/bin/env python3
"""Locally transcribe reel rows that are missing transcripts, regenerate captions,
and batch-generate slide copy at the end.

Usage:
    python scripts/local_transcribe_reels.py

Optional local dependency:
    pip install faster-whisper

Fallback dependency:
    pip install openai-whisper
"""

from __future__ import annotations

import argparse
from datetime import datetime
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
for _p in (str(REPO_ROOT), str(SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from config import GOOGLE_SHEET_ID  # noqa: E402
from drive import _get_service  # noqa: E402
from pipeline_caption import generate_batch_carousel_copy_with_model, generate_row_caption  # noqa: E402
from run_pipeline import step1_ingest  # noqa: E402
from sheets import get_all_rows, update_caption, update_carousel_fields, update_metadata, update_transcript  # noqa: E402
from utils.error_labels import describe_error  # noqa: E402
from watch_split_folder import watch_folder  # noqa: E402


MEDIA_DIR_SUFFIX = Path("_apps") / "vioo instagram pipeline" / "instagram pipeline media"
SAFE_DELETE_DIRNAME = "safe_for_deletion"

MEDIA_DIR_CANDIDATES = [
    Path.home()
    / "Library"
    / "CloudStorage"
    / "GoogleDrive-voteinorout@gmail.com"
    / "My Drive"
    / MEDIA_DIR_SUFFIX,
    Path("/Users/lisa")
    / "Library"
    / "CloudStorage"
    / "GoogleDrive-voteinorout@gmail.com"
    / "My Drive"
    / MEDIA_DIR_SUFFIX,
    Path("/Users/lisamollica")
    / "Library"
    / "CloudStorage"
    / "GoogleDrive-voteinorout@gmail.com"
    / "My Drive"
    / MEDIA_DIR_SUFFIX,
]


class NoTranscribableAudioError(RuntimeError):
    """Raised when a local media file has no audio stream for Whisper."""


def _default_media_dir() -> Path:
    for candidate in MEDIA_DIR_CANDIDATES:
        if candidate.exists():
            return candidate

    cloud_storage = Path.home() / "Library" / "CloudStorage"
    if cloud_storage.exists():
        matches = sorted(cloud_storage.glob(f"GoogleDrive-*/My Drive/{MEDIA_DIR_SUFFIX}"))
        if matches:
            return matches[0]

    return MEDIA_DIR_CANDIDATES[0]


def _extract_drive_file_id(link: str) -> str:
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", link or "")
    if match:
        return match.group(1)
    parsed = urlparse(link or "")
    return parse_qs(parsed.query).get("id", [""])[0]


def _drive_filename(service, link: str) -> str:
    file_id = _extract_drive_file_id(link)
    if not file_id:
        raise ValueError(f"Could not parse a Drive file id from {link!r}")
    metadata = (
        service.files()
        .get(fileId=file_id, fields="id,name", supportsAllDrives=True)
        .execute()
    )
    return (metadata.get("name") or "").strip()


def _is_instagram_url(url: str) -> bool:
    return "instagram.com/" in (url or "").lower()


def _row_has_caption_source(row: dict) -> bool:
    return bool(
        (row.get("Transcript") or "").strip()
        or (row.get("Original Caption") or "").strip()
        or (row.get("Caption Context") or "").strip()
    )


_INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\u200e\u200f\u2060\ufeff]")


def _clean_public_url(link: str) -> str:
    link = _INVISIBLE_CHARS_RE.sub("", (link or "").strip())
    parsed = urlparse(link)
    if not parsed.scheme or not parsed.netloc:
        return link
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _build_instagram_cta(username: str, link: str) -> str:
    cleaned_username = (username or "").strip().lstrip("@")
    cleaned_link = _clean_public_url(link)
    destination = f"@{cleaned_username} {cleaned_link}" if cleaned_username else cleaned_link
    return f"Comment LINK (on instagram) and we will DM you the link to {destination}"


def _find_local_media_path(media_root: Path, filename: str) -> Path:
    direct = media_root / filename
    if direct.exists():
        return direct

    matches = list(media_root.rglob(filename))
    if matches:
        return matches[0]

    raise FileNotFoundError(f"Could not find {filename!r} under {str(media_root)!r}")


def _stem_without_suffixes(filename: str) -> str:
    stem = Path(filename).stem
    stem = re.sub(r" \(\d+\)$", "", stem)
    return re.sub(r"_thumb(?:_\d+s)?$", "", stem)


def _canonical_media_key(name: str) -> str:
    cleaned = _stem_without_suffixes(name)
    match = re.search(r"(\d{6}_[A-Za-z0-9_-]+)$", cleaned)
    if match:
        return match.group(1)
    return cleaned


def _iter_media_links(row: dict) -> list[str]:
    return [link.strip() for link in (row.get("Media Drive Link") or "").split(",") if link.strip()]


def _active_drive_filenames(rows: list[dict], service) -> tuple[set[str], list[str]]:
    filenames: set[str] = set()
    errors: list[str] = []
    for row in rows:
        row_num = row.get("row_number", "?")
        for link in _iter_media_links(row):
            try:
                filename = _drive_filename(service, link)
                if filename:
                    filenames.add(filename)
            except Exception as exc:
                errors.append(f"Row {row_num}: failed to resolve Drive filename for {link} - {describe_error(exc)}")
    return filenames, errors


def _move_to_archive(path: Path, media_root: Path, archive_root: Path, dry_run: bool) -> Path:
    relative = path.relative_to(media_root)
    destination = archive_root / relative
    if dry_run:
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    final_destination = destination
    counter = 1
    while final_destination.exists():
        suffix = f"_{counter}"
        if destination.suffix:
            final_destination = destination.with_name(f"{destination.stem}{suffix}{destination.suffix}")
        else:
            final_destination = destination.with_name(f"{destination.name}{suffix}")
        counter += 1
    shutil.move(str(path), str(final_destination))
    return final_destination


def _archive_orphaned_media(media_root: Path, rows: list[dict], service, dry_run: bool) -> int:
    active_filenames, filename_errors = _active_drive_filenames(rows, service)
    for error in filename_errors:
        print(error)

    active_stems = {_stem_without_suffixes(filename) for filename in active_filenames}
    active_keys = {_canonical_media_key(filename) for filename in active_filenames}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_root = media_root / SAFE_DELETE_DIRNAME / timestamp
    screenshots_dir = media_root / "screenshots"
    root_image_suffixes = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

    orphan_paths: list[tuple[str, Path]] = []
    for path in sorted(media_root.iterdir()):
        if path.name == SAFE_DELETE_DIRNAME:
            continue
        if path == screenshots_dir:
            continue
        if path.is_file() and path.suffix.lower() in {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"}:
            if path.name not in active_filenames:
                orphan_paths.append(("original", path))
            continue
        if path.is_file() and path.suffix.lower() in root_image_suffixes:
            stem = _stem_without_suffixes(path.name)
            if stem not in active_stems and _canonical_media_key(stem) not in active_keys:
                orphan_paths.append(("image", path))
            continue
        if path.is_dir():
            stem = (
                path.name[: -len("_segments")]
                if path.name.endswith("_segments")
                else path.name
            )
            # Never archive a segments folder while the source video is still
            # present locally — the Drive API lookup may have failed for that
            # row, making the stem absent from active_stems even though the
            # video is live. Archiving here would cause a re-split next run.
            source_exists = any(
                (media_root / f"{stem}{ext}").exists()
                for ext in {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"}
            )
            if source_exists:
                continue
            if stem not in active_stems and _canonical_media_key(stem) not in active_keys:
                orphan_paths.append(("segments", path))

    if screenshots_dir.exists():
        for path in sorted(screenshots_dir.rglob("*")):
            if not path.is_file():
                continue
            if path.name.startswith("."):
                continue
            stem = _stem_without_suffixes(path.name)
            if stem not in active_stems and _canonical_media_key(stem) not in active_keys:
                orphan_paths.append(("screenshot", path))

    if not orphan_paths:
        print("Archive pass found no orphaned local originals, root images, segment folders, or screenshots.")
        return 0

    action = "Would move" if dry_run else "Moved"
    print(
        f"Archive pass found {len(orphan_paths)} orphaned item(s). "
        f"{'Dry run only.' if dry_run else f'Archiving into {archive_root}.'}"
    )
    moved = 0
    for kind, path in orphan_paths:
        destination = _move_to_archive(path, media_root, archive_root, dry_run)
        print(f"{action} {kind}: {path} -> {destination}")
        moved += 1
    return moved


def _get_ffmpeg_path() -> str:
    try:
        import imageio_ffmpeg  # type: ignore

        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return "ffmpeg"


def _extract_audio_for_transcription(video_path: str) -> str:
    fd, audio_path = tempfile.mkstemp(suffix=".wav")
    os.close(fd)
    command = [
        _get_ffmpeg_path(),
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        video_path,
        "-vn",
        "-ac",
        "1",
        "-ar",
        "16000",
        audio_path,
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        try:
            os.unlink(audio_path)
        except Exception:
            pass
        stderr = (result.stderr or result.stdout or "").strip()
        if len(stderr) > 1000:
            stderr = stderr[-1000:]
        if "does not contain any stream" in stderr.lower():
            raise NoTranscribableAudioError("Local media file has no audio stream to transcribe.")
        raise RuntimeError(f"ffmpeg could not extract audio for local transcription. {stderr}".strip())
    return audio_path


def _is_media_decode_error(error: Exception) -> bool:
    message = str(error or "").lower()
    exc_name = f"{error.__class__.__module__}.{error.__class__.__name__}".lower()
    return (
        isinstance(error, IndexError)
        or "tuple index out of range" in message
        or "av." in exc_name
        or "pyav" in message
        or "invalid data found" in message
        or "error opening input" in message
        or "could not open" in message
    )


def _stage_media_for_local_decode(path: str) -> str:
    source = Path(path)
    suffix = source.suffix or ".mp4"
    staged_fd, staged_path = tempfile.mkstemp(suffix=suffix)
    os.close(staged_fd)
    try:
        shutil.copyfile(source, staged_path)
    except Exception:
        try:
            os.unlink(staged_path)
        except Exception:
            pass
        raise
    return staged_path


def _transcribe_with_audio_fallback(path: str, transcribe_file: Callable[[str], str]) -> str:
    staged_path = None
    try:
        staged_path = _stage_media_for_local_decode(path)
        return transcribe_file(staged_path)
    except Exception as first_error:
        if not _is_media_decode_error(first_error):
            raise
        audio_path = None
        try:
            decode_source = staged_path or path
            audio_path = _extract_audio_for_transcription(decode_source)
            return transcribe_file(audio_path)
        except NoTranscribableAudioError:
            raise
        except Exception as retry_error:
            raise RuntimeError(
                f"Local transcription could not decode the video directly or from extracted audio. "
                f"Original error: {first_error}. Retry error: {retry_error}"
            ) from retry_error
        finally:
            if audio_path:
                try:
                    os.unlink(audio_path)
                except Exception:
                    pass
            if staged_path:
                try:
                    os.unlink(staged_path)
                except Exception:
                    pass


def _get_local_transcriber(model_name: str) -> Callable[[str], str]:
    try:
        from faster_whisper import WhisperModel  # type: ignore

        model = WhisperModel(model_name, device="cpu", compute_type="int8")

        def transcribe_file(path: str) -> str:
            segments, _info = model.transcribe(path, vad_filter=True)
            text = " ".join(segment.text.strip() for segment in segments if segment.text.strip()).strip()
            if not text:
                raise ValueError("Local transcription returned no text.")
            return text

        def transcribe(path: str) -> str:
            return _transcribe_with_audio_fallback(path, transcribe_file)

        return transcribe
    except ImportError:
        pass

    try:
        import whisper  # type: ignore

        model = whisper.load_model(model_name)

        def transcribe_file(path: str) -> str:
            result = model.transcribe(path, fp16=False)
            text = (result.get("text") or "").strip()
            if not text:
                raise ValueError("Local transcription returned no text.")
            return text

        def transcribe(path: str) -> str:
            return _transcribe_with_audio_fallback(path, transcribe_file)

        return transcribe
    except ImportError as exc:
        raise RuntimeError(
            "No local Whisper backend is installed. Install one of:\n"
            "  pip install faster-whisper\n"
            "or\n"
            "  pip install openai-whisper"
        ) from exc


def _eligible_rows(rows: list[dict]) -> list[dict]:
    eligible = []
    for row in rows:
        media_type = (row.get("Media Type") or "").strip().lower()
        transcript = (row.get("Transcript") or "").strip()
        media_link = (row.get("Media Drive Link") or "").strip()
        generated_caption = (row.get("Generated Caption") or "").strip()
        status = (row.get("Status") or "").strip().lower()
        if status == "skipped":
            continue
        if status.startswith("error"):
            continue
        if media_type == "reel" and not transcript and media_link:
            eligible.append(row)
            continue
        if status == "slides" or generated_caption:
            continue
        if _row_has_caption_source(row):
            eligible.append(row)
    return eligible


def _update_caption_from_transcript(row: dict, transcript: str) -> dict:
    updated_row = dict(row)
    updated_row["Transcript"] = transcript
    current_top = (updated_row.get("Top Comment") or "").strip()
    instagram_url = (updated_row.get("Instagram URL") or "").strip()
    if not current_top and _is_instagram_url(instagram_url):
        current_top = _build_instagram_cta(updated_row.get("Source Username", ""), instagram_url)
        updated_row["Top Comment"] = current_top
        update_metadata(
            GOOGLE_SHEET_ID,
            row["row_number"],
            updated_row.get("Caption Context", ""),
            updated_row.get("Speaker Name", ""),
            updated_row.get("Required Hashtags", ""),
            current_top,
            updated_row.get("Footer", ""),
        )
    caption = generate_row_caption(updated_row)
    current_status = (row.get("Status") or "").strip()
    if current_status.lower() == "skipped":
        next_status = "skipped"
    elif current_status:
        next_status = "done"
    else:
        next_status = "done"
    update_caption(GOOGLE_SHEET_ID, row["row_number"], caption, next_status)
    updated_row["Generated Caption"] = caption
    updated_row["Status"] = next_status
    return updated_row


def _generate_caption_from_existing_sources(row: dict) -> dict:
    if not _row_has_caption_source(row):
        raise ValueError("No transcript, original caption, or caption context available for caption generation.")
    return _update_caption_from_transcript(row, (row.get("Transcript") or "").strip())


def main() -> int:
    parser = argparse.ArgumentParser(description="Locally transcribe reel rows missing transcripts.")
    parser.add_argument(
        "--media-dir",
        default=None,
        help="Path to your locally synced Drive media folder. Defaults to auto-detecting common Google Drive locations.",
    )
    parser.add_argument(
        "--model",
        default="small",
        help="Local Whisper model name. Examples: tiny, base, small.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of rows to process.",
    )
    parser.add_argument(
        "--row",
        type=int,
        default=0,
        help="Optional Google Sheet row number to process by itself.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print full tracebacks for failed rows.",
    )
    parser.add_argument(
        "--archive-orphans",
        action="store_true",
        help="Deprecated: orphan archiving now runs by default unless --no-archive-orphans is set.",
    )
    parser.add_argument(
        "--no-archive-orphans",
        action="store_true",
        help="Skip moving local originals, root images, segment folders, and screenshots with no matching current sheet row into a safe archive folder.",
    )
    parser.add_argument(
        "--archive-dry-run",
        action="store_true",
        help="Show which orphaned local items would be archived without moving them.",
    )
    args = parser.parse_args()

    media_root = Path(args.media_dir).expanduser() if args.media_dir else _default_media_dir()
    if not media_root.exists():
        raise FileNotFoundError(f"Media directory does not exist: {media_root}")
    print(f"Using media directory: {media_root}")

    print("\n--- Step 1: Ingest ---")
    step1_ingest(GOOGLE_SHEET_ID)

    print("\nRunning split watcher until the media folder is idle...")
    watch_folder(media_root, stop_when_idle=True)

    rows = get_all_rows(GOOGLE_SHEET_ID)
    archive_orphans = not args.no_archive_orphans
    service = None

    targets = _eligible_rows(rows)
    if args.row > 0:
        targets = [row for row in targets if row.get("row_number") == args.row]
    if args.limit > 0:
        targets = targets[: args.limit]

    if targets:
        if service is None:
            service = _get_service()
        transcribe = _get_local_transcriber(args.model)
        print(f"Found {len(targets)} row(s) to process.")
        for row in targets:
            row_num = row["row_number"]
            url = (row.get("Instagram URL") or "").strip()
            media_type = (row.get("Media Type") or "").strip().lower()
            transcript = (row.get("Transcript") or "").strip()
            media_links = [link.strip() for link in (row.get("Media Drive Link") or "").split(",") if link.strip()]
            step = "starting"
            try:
                if media_type == "reel" and not transcript and media_links:
                    step = "looking up Drive filename"
                    filename = _drive_filename(service, media_links[0])
                    if not filename:
                        raise ValueError(f"Drive file did not return a filename for {media_links[0]!r}")
                    step = "finding local media file"
                    local_path = _find_local_media_path(media_root, filename)
                    try:
                        step = f"transcribing {local_path.name}"
                        transcript = transcribe(str(local_path))
                    except NoTranscribableAudioError:
                        step = "generating caption from existing source text"
                        _generate_caption_from_existing_sources(row)
                        print(
                            f"Row {row_num}: no transcribable audio, generated caption from existing source text ({url})"
                        )
                        continue
                    step = "writing transcript to Google Sheets"
                    update_transcript(GOOGLE_SHEET_ID, row_num, transcript)
                    step = "regenerating caption with OpenAI"
                    _update_caption_from_transcript(row, transcript)
                    print(f"Row {row_num}: transcribed and regenerated caption for {filename} ({url})")
                    continue

                step = "generating caption from existing source text"
                _generate_caption_from_existing_sources(row)
                print(f"Row {row_num}: generated caption from existing source text ({url})")
            except Exception as exc:
                print(f"Row {row_num}: failed while {step} - {describe_error(exc)}")
                if args.debug:
                    traceback.print_exc()
    else:
        print("No rows need transcription or fallback caption generation.")

    if archive_orphans:
        if service is None:
            service = _get_service()
        _archive_orphaned_media(media_root, rows, service, dry_run=args.archive_dry_run)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
