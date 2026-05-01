#!/usr/bin/env python3
"""Locally transcribe reel rows that are missing transcripts and regenerate captions.

Usage:
    python scripts/local_transcribe_reels.py

Optional local dependency:
    pip install faster-whisper

Fallback dependency:
    pip install openai-whisper
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import GOOGLE_SHEET_ID  # noqa: E402
from drive import _get_service  # noqa: E402
from pipeline_caption import generate_row_caption  # noqa: E402
from sheets import get_all_rows, update_caption, update_metadata, update_transcript  # noqa: E402


MEDIA_DIR_SUFFIX = Path("_apps") / "vioo instagram pipeline" / "instagram pipeline media"

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


def _clean_public_url(link: str) -> str:
    parsed = urlparse((link or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return (link or "").strip()
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


def _get_local_transcriber(model_name: str) -> Callable[[str], str]:
    try:
        from faster_whisper import WhisperModel  # type: ignore

        model = WhisperModel(model_name, device="cpu", compute_type="int8")

        def transcribe(path: str) -> str:
            segments, _info = model.transcribe(path, vad_filter=True)
            text = " ".join(segment.text.strip() for segment in segments if segment.text.strip()).strip()
            if not text:
                raise ValueError("Local transcription returned no text.")
            return text

        return transcribe
    except ImportError:
        pass

    try:
        import whisper  # type: ignore

        model = whisper.load_model(model_name)

        def transcribe(path: str) -> str:
            result = model.transcribe(path, fp16=False)
            text = (result.get("text") or "").strip()
            if not text:
                raise ValueError("Local transcription returned no text.")
            return text

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
        if media_type == "reel" and not transcript and media_link:
            eligible.append(row)
    return eligible


def _update_caption_from_transcript(row: dict, transcript: str) -> None:
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
    args = parser.parse_args()

    media_root = Path(args.media_dir).expanduser() if args.media_dir else _default_media_dir()
    if not media_root.exists():
        raise FileNotFoundError(f"Media directory does not exist: {media_root}")
    print(f"Using media directory: {media_root}")

    rows = get_all_rows(GOOGLE_SHEET_ID)
    targets = _eligible_rows(rows)
    if args.limit > 0:
        targets = targets[: args.limit]

    if not targets:
        print("No reel rows are missing transcripts.")
        return 0

    service = _get_service()
    transcribe = _get_local_transcriber(args.model)

    print(f"Found {len(targets)} reel row(s) missing transcripts.")
    for row in targets:
        row_num = row["row_number"]
        url = (row.get("Instagram URL") or "").strip()
        media_links = [link.strip() for link in (row.get("Media Drive Link") or "").split(",") if link.strip()]
        if not media_links:
            print(f"Row {row_num}: skipped, no Drive media link.")
            continue
        try:
            filename = _drive_filename(service, media_links[0])
            local_path = _find_local_media_path(media_root, filename)
            transcript = transcribe(str(local_path))
            update_transcript(GOOGLE_SHEET_ID, row_num, transcript)
            _update_caption_from_transcript(row, transcript)
            print(f"Row {row_num}: transcribed and regenerated caption for {filename} ({url})")
        except Exception as exc:
            print(f"Row {row_num}: failed - {exc}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
