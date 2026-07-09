#!/usr/bin/env python3
"""Offline pipeline runner: ingest pending rows, transcribe reels, split videos.

Usage:
    python scripts/run_pipeline.py

Environment:
    GOOGLE_SERVICE_ACCOUNT_JSON  — service account key (set as GitHub Actions secret)
    All other secrets are loaded from Google Secret Manager automatically via config.py.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from article_source import fetch_article_source
from caption import transcribe_video
from config import GOOGLE_DRIVE_FOLDER_ID, GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER, GOOGLE_SHEET_ID
from drive import (
    _get_service,
    download_drive_file,
    get_drive_file_metadata,
    get_or_create_subfolder,
    upload_to_drive,
)
from ingest_helpers import build_filename_prefix, download_file, upload_media_bundle
import pipeline_caption as pipeline_caption_ops
from post_scraper import process_url as process_post_url
from reel_scraper import process_url as process_reel_url
from sheets import (
    get_all_rows,
    get_pending_rows,
    update_caption,
    update_caption_and_metadata,
    update_ingest_result,
    update_metadata,
    update_transcript,
)

generate_row_caption = pipeline_caption_ops.generate_row_caption
row_ready_for_caption = pipeline_caption_ops.row_ready_for_caption

PREVIEW_UPLOAD_SUBFOLDER = "previews"


# ---------------------------------------------------------------------------
# Helpers (ported from workspace.py, no Streamlit deps)
# ---------------------------------------------------------------------------

def _cell_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _is_reel_url(url: str) -> bool:
    lowered = (url or "").lower()
    return "/reel/" in lowered or "/reels/" in lowered


def _is_instagram_url(url: str) -> bool:
    return "instagram.com/" in (url or "").lower()


def _is_article_url(url: str) -> bool:
    from urllib.parse import urlparse
    parsed = urlparse((url or "").strip())
    return parsed.scheme == "https" and bool(parsed.netloc) and not _is_instagram_url(url)


def _article_thumbnail_link(image_url: str, row_number: int | str | None, username: str) -> str:
    image_url = (image_url or "").strip()
    if not image_url:
        return ""

    tmp_dir = tempfile.mkdtemp(prefix="article_thumb_")
    try:
        screenshots_folder_id = get_or_create_subfolder(
            GOOGLE_DRIVE_FOLDER_ID,
            GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
        )
        parsed = urlparse(image_url)
        ext = os.path.splitext(parsed.path or "")[1].lower() or ".jpg"
        if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            ext = ".jpg"
        filename_prefix = build_filename_prefix(row_number, username)
        filename = f"{filename_prefix}article_{row_number or 'thumb'}_thumb{ext}"
        local_path = os.path.join(tmp_dir, filename)
        import requests

        response = requests.get(
            image_url,
            allow_redirects=True,
            timeout=60,
            stream=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
                ),
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                "Referer": "https://www.google.com/",
            },
        )
        response.raise_for_status()
        with open(local_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)
        return upload_to_drive(local_path, filename, screenshots_folder_id)
    except Exception:
        return image_url
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


_INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\u200e\u200f\u2060\ufeff]")


def _clean_public_url(link: str) -> str:
    from urllib.parse import parse_qs, urlparse

    link = _INVISIBLE_CHARS_RE.sub("", (link or "").strip())
    parsed = urlparse(link)
    if not parsed.scheme or not parsed.netloc:
        return link
    ref = parse_qs(parsed.query).get("ref", [None])[0]
    suffix = f"?ref={ref}" if ref else ""
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}{suffix}"


def _build_watch_cta(username: str, link: str) -> str:
    cleaned = (username or "").strip().lstrip("@")
    cleaned_link = _clean_public_url(link)
    destination = f"@{cleaned} {cleaned_link}" if cleaned else cleaned_link
    return f"Comment LINK (on instagram) and we will DM you the link to {destination}"


def _build_read_cta(link: str) -> str:
    return f"Comment LINK (on instagram) and we will DM you the link to {_clean_public_url(link)}"


def _row_caption_inputs(row: dict) -> dict:
    url = (row.get("Instagram URL") or "").strip()
    username = (row.get("Source Username") or "").strip()
    context = (row.get("Caption Context") or "").strip()
    if not context and _is_article_url(url):
        context = (row.get("Original Caption") or "").strip()
    speaker = (row.get("Speaker Name") or "").strip()
    hashtags = (row.get("Required Hashtags") or "").strip()
    top = (row.get("Top Comment") or "").strip()
    if not top and _is_instagram_url(url):
        top = _build_watch_cta(username or speaker, url)
    elif not top and _is_article_url(url):
        top = _build_read_cta(url)
    return {
        "Caption Context": context,
        "Speaker Name": speaker,
        "Required Hashtags": hashtags,
        "Top Comment": top,
        "Footer": "",
    }


def _ffprobe_path() -> str:
    path = shutil.which("ffprobe")
    if not path:
        raise RuntimeError("ffprobe is not installed or not on PATH.")
    return path


def _video_duration_seconds(path: str) -> float:
    cmd = [
        _ffprobe_path(), "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    text = (result.stdout or "").strip()
    return float(text) if text else 0.0


def _segment_name(index: int) -> str:
    words = [
        "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
        "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
        "eighteen", "nineteen", "twenty", "twenty_one", "twenty_two", "twenty_three",
        "twenty_four", "twenty_five", "twenty_six", "twenty_seven", "twenty_eight",
        "twenty_nine", "thirty",
    ]
    if 0 <= index < len(words):
        return words[index]
    return f"{index + 1:02d}"


def _preview_folder_base_name(username: str, media_link: str, row_num: int) -> tuple[str, str]:
    cleaned_username = re.sub(
        r"[^A-Za-z0-9._-]+", "_", (username or "").strip().lstrip("@")
    ).strip("._-")
    if media_link:
        try:
            metadata = get_drive_file_metadata(media_link)
            filename = (metadata.get("name") or "").strip()
            stem = os.path.splitext(filename)[0]
            match = re.match(r"(?P<username>[A-Za-z0-9._-]+)_(?P<date>\d{6})_", stem)
            if match:
                matched_username = (match.group("username") or "").strip("._-")
                matched_date = (match.group("date") or "").strip()
                return f"{matched_username}_{matched_date}", filename
            date_match = re.search(r"(\d{6})", stem)
            if cleaned_username and date_match:
                return f"{cleaned_username}_{date_match.group(1)}", filename
            if stem:
                return stem, filename
            return filename or f"{cleaned_username or 'row'}_{row_num}", filename
        except Exception:
            pass
    fallback = f"{cleaned_username or 'row'}_{row_num}"
    return fallback, ""


def _ensure_preview_folder(
    row_num: int, username: str, handle_text: str, media_link: str
) -> tuple[str, str, str]:
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not configured.")
    preview_root = get_or_create_subfolder(GOOGLE_DRIVE_FOLDER_ID, PREVIEW_UPLOAD_SUBFOLDER)
    folder_base_name, source_filename = _preview_folder_base_name(
        username or handle_text, media_link, row_num
    )
    preview_folder_id = get_or_create_subfolder(preview_root, folder_base_name)
    return preview_folder_id, folder_base_name, source_filename


def _split_video_to_folder(local_video_path: str, output_dir: str, mode: str = "fit") -> list[str]:
    if mode == "fit":
        pad_w = "if(gte(iw/ih\\,4/5)\\,iw\\,trunc(ih*(4/5)/2)*2)"
        pad_h = "if(gte(iw/ih\\,4/5)\\,trunc(iw*(5/4)/2)*2\\,ih)"
        video_filter = (
            f"pad={pad_w}:{pad_h}:(ow-iw)/2:(oh-ih)/2:black,"
            "scale=trunc(iw/2)*2:trunc(ih/2)*2"
        )
    else:
        crop_width = "if(gte(iw/ih\\,4/5)\\,trunc(ih*(4/5)/2)*2\\,iw)"
        crop_height = "if(gte(iw/ih\\,4/5)\\,ih\\,trunc(iw/(4/5)/2)*2)"
        video_filter = (
            f"crop={crop_width}:{crop_height}:(iw-ow)/2:(ih-oh)/2,"
            "scale=trunc(iw/2)*2:trunc(ih/2)*2"
        )
    duration = _video_duration_seconds(local_video_path)
    if duration <= 0:
        raise RuntimeError("Could not determine video duration for splitting.")
    ffmpeg_path = shutil.which("ffmpeg") or "ffmpeg"
    outputs: list[str] = []
    start_seconds = 0.0
    segment_index = 0
    while start_seconds < duration - 0.01:
        clip_duration = min(60.0, duration - start_seconds)
        suffix = "_fit" if mode == "fit" else ""
        output_path = os.path.join(output_dir, f"{_segment_name(segment_index)}{suffix}.mp4")
        subprocess.run(
            [
                ffmpeg_path, "-hide_banner", "-loglevel", "error", "-y",
                "-i", local_video_path,
                "-ss", f"{start_seconds:.3f}",
                "-t", f"{clip_duration:.3f}",
                "-vf", video_filter,
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                "-c:a", "aac", "-b:a", "192k",
                output_path,
            ],
            check=True,
        )
        outputs.append(output_path)
        start_seconds += 60.0
        segment_index += 1
    return outputs


# ---------------------------------------------------------------------------
# Row-level operations
# ---------------------------------------------------------------------------

def _ingest_row(row: dict) -> dict:
    url = row["Instagram URL"].strip()
    tmp_dir = None
    try:
        if _is_article_url(url):
            article = fetch_article_source(url)
            article_source_text = (
                (article.get("source_text") or "").strip()
                or (article.get("summary_text") or "").strip()
            )
            article_username = article.get("domain", "")
            return {
                "username": article_username,
                "media_type": "article",
                "photo_count": "",
                "media_link": "",
                "thumbnail_link": _article_thumbnail_link(article.get("image_url", ""), row.get("row_number"), article_username),
                "original_caption": article_source_text,
                "transcript": article_source_text,
                "status": "ingested",
            }
        if _is_reel_url(url):
            data = process_reel_url(url, include_transcript=False)
        else:
            data = process_post_url(url)
        filename_prefix = build_filename_prefix(row.get("row_number"), data.get("username", ""))
        uploaded = upload_media_bundle(data, filename_prefix=filename_prefix)
        tmp_dir = uploaded["tmp_dir"]
        return {
            "username": data["username"],
            "media_type": data["media_type"],
            "photo_count": data["photo_count"],
            "media_link": uploaded["media_link"],
            "thumbnail_link": uploaded["thumbnail_link"],
            "original_caption": data["original_caption"],
            "transcript": data["transcript"],
            "status": "ingested",
        }
    except Exception as e:
        return {
            "username": "", "media_type": "", "photo_count": "",
            "media_link": "", "thumbnail_link": "", "original_caption": "",
            "transcript": "", "status": f"error: {e}",
        }
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _transcribe_reel_from_drive(row: dict) -> str | None:
    media_link = _cell_text(row.get("Media Drive Link")).strip()
    if not media_link:
        return None
    row_num = row["row_number"]
    tmp_dir = tempfile.mkdtemp(prefix="pipeline_transcribe_")
    try:
        try:
            metadata = get_drive_file_metadata(media_link)
            filename = metadata.get("name") or f"row_{row_num}.mp4"
        except Exception:
            filename = f"row_{row_num}.mp4"
        local_path = os.path.join(tmp_dir, filename)
        download_drive_file(media_link, local_path)
        return transcribe_video(local_path)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _upload_split_videos(
    media_link: str, preview_folder_id: str, mode: str = "fit"
) -> list[dict[str, str]]:
    if not media_link:
        return []
    metadata = get_drive_file_metadata(media_link)
    filename = (metadata.get("name") or "").strip()
    if not filename:
        raise ValueError("Could not determine the source video filename from Drive.")
    tmp_dir = tempfile.mkdtemp(prefix="pipeline_splits_")
    try:
        local_video_path = os.path.join(tmp_dir, filename)
        download_drive_file(media_link, local_video_path)
        split_dir = os.path.join(tmp_dir, "segments")
        os.makedirs(split_dir, exist_ok=True)
        segment_paths = _split_video_to_folder(local_video_path, split_dir, mode=mode)
        uploaded: list[dict[str, str]] = []
        for segment_path in segment_paths:
            segment_filename = os.path.basename(segment_path)
            uploaded.append({
                "label": f"Split {os.path.splitext(segment_filename)[0]}",
                "link": upload_to_drive(segment_path, segment_filename, preview_folder_id),
            })
        return uploaded
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def _ingest_and_caption_row(sheet_id: str, row: dict) -> bool:
    """Ingest one row and generate its caption. Returns True on success."""
    row_num = row["row_number"]
    url = (row.get("Instagram URL") or "").strip()
    print(f"  Row {row_num}: {url[:80]}")
    result = _ingest_row(row)
    try:
        update_ingest_result(
            sheet_id, row_num,
            result["username"], result["media_type"], result["photo_count"],
            result["media_link"], result["thumbnail_link"],
            result["original_caption"], result["transcript"], result["status"],
        )
        enriched_row = {
            **row,
            "Source Username": result["username"],
            "Media Type": result["media_type"],
        }
        inputs = _row_caption_inputs(enriched_row)
        update_metadata(
            sheet_id, row_num,
            inputs["Caption Context"], inputs["Speaker Name"],
            inputs["Required Hashtags"], inputs["Top Comment"], "",
        )
        if result["status"] == "ingested":
            ingested_row = {
                **enriched_row,
                "Photo Count": result["photo_count"],
                "Media Drive Link": result["media_link"],
                "Thumbnail Drive Link": result["thumbnail_link"],
                "Original Caption": result["original_caption"],
                "Transcript": result["transcript"],
                "Status": result["status"],
                "Caption Context": inputs["Caption Context"],
                "Speaker Name": inputs["Speaker Name"],
                "Required Hashtags": inputs["Required Hashtags"],
                "Top Comment": inputs["Top Comment"],
                "Footer": "",
            }
            if row_ready_for_caption(ingested_row):
                caption = generate_row_caption(ingested_row)
                update_caption_and_metadata(
                    sheet_id, row_num, caption, result["status"],
                    inputs["Caption Context"], inputs["Speaker Name"],
                    inputs["Required Hashtags"], inputs["Top Comment"], "",
                )
                print(f"  Row {row_num}: ingested + captioned ({result['media_type']})")
            else:
                print(f"  Row {row_num}: ingested ({result['media_type']}); waiting for transcript before captioning")
            return True
        else:
            print(f"  Row {row_num}: {result['status']}")
            return False
    except Exception as e:
        print(f"  Row {row_num}: sheet write error — {e}")
        return False


def step1_ingest(sheet_id: str) -> int:
    pending = get_pending_rows(sheet_id)
    if not pending:
        print("Step 1: No pending rows.")
        return 0
    print(f"Step 1: Ingesting {len(pending)} pending row(s)…")
    succeeded = 0
    for row in pending:
        if _ingest_and_caption_row(sheet_id, row):
            succeeded += 1
    print(f"Step 1: {succeeded}/{len(pending)} row(s) ingested.")
    return len(pending)


def step2_transcribe(sheet_id: str, all_rows: list[dict]) -> int:
    untranscribed = [
        r for r in all_rows
        if r.get("Media Type", "").strip().lower() == "reel"
        and not r.get("Transcript", "").strip()
        and r.get("Media Drive Link", "").strip()
    ]
    if not untranscribed:
        print("Step 2: No untranscribed reels.")
        return 0
    print(f"Step 2: Transcribing {len(untranscribed)} reel(s) with Whisper…")
    succeeded = 0
    for row in untranscribed:
        row_num = row["row_number"]
        username = _cell_text(row.get("Source Username")).strip() or f"row {row_num}"
        print(f"  Row {row_num} ({username}): downloading…")
        try:
            transcript = _transcribe_reel_from_drive(row)
            if transcript:
                print(f"  Row {row_num}: {len(transcript)} chars, saving…")
                update_transcript(sheet_id, row_num, transcript)
                updated_row = {**row, "Transcript": transcript}
                caption = generate_row_caption(updated_row)
                next_status = (
                    "skipped"
                    if (row.get("Status") or "").strip().lower() == "skipped"
                    else "done"
                )
                update_caption(sheet_id, row_num, caption, next_status)
                print(f"  Row {row_num}: done.")
                succeeded += 1
            else:
                print(f"  Row {row_num}: Whisper returned no transcript.")
        except Exception as e:
            print(f"  Row {row_num}: {e}")
    print(f"Step 2: {succeeded}/{len(untranscribed)} reel(s) transcribed.")
    return succeeded


def step3_split(all_rows: list[dict]) -> int:
    reels = [
        r for r in all_rows
        if r.get("Media Type", "").strip().lower() == "reel"
        and r.get("Media Drive Link", "").strip()
    ]
    if not reels:
        print("Step 3: No reels to split.")
        return 0
    print(f"Step 3: Splitting {len(reels)} reel(s)…")
    succeeded = 0
    for row in reels:
        row_num = row["row_number"]
        username = _cell_text(row.get("Source Username")).strip() or f"row {row_num}"
        print(f"  Row {row_num} ({username}): downloading and splitting…")
        try:
            media_link = _cell_text(row.get("Media Drive Link")).strip().split(",")[0].strip()
            username_clean = _cell_text(row.get("Source Username")).strip().lstrip("@")
            handle_text = _cell_text(row.get("Speaker Name")).strip()
            preview_folder_id, _, _ = _ensure_preview_folder(
                row_num, username_clean, handle_text, media_link
            )
            _upload_split_videos(media_link, preview_folder_id, mode="fill")
            print(f"  Row {row_num}: done.")
            succeeded += 1
        except Exception as e:
            print(f"  Row {row_num}: {e}")
    print(f"Step 3: {succeeded}/{len(reels)} reel(s) split.")
    return succeeded


SAFE_DELETE_SUBFOLDER = "safe_for_deletion"


def step4_cleanup(all_rows: list[dict]) -> int:
    """Move Drive preview subfolders and orphaned root-level items into safe_for_deletion."""
    if not GOOGLE_DRIVE_FOLDER_ID:
        print("Step 4: GOOGLE_DRIVE_FOLDER_ID not configured, skipped.")
        return 0
    service = _get_service()
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Build set of active media filename stems from all rows (used by both phases).
    active_stems: set[str] = set()
    for row in all_rows:
        media_link = _cell_text(row.get("Media Drive Link") or "").strip().split(",")[0].strip()
        if not media_link:
            continue
        try:
            meta = get_drive_file_metadata(media_link)
            stem = os.path.splitext((meta.get("name") or "").strip())[0]
            if stem:
                active_stems.add(stem)
        except Exception:
            pass

    # --- Phase 1: orphaned preview subfolders ---
    moved = 0
    preview_root_id = get_or_create_subfolder(GOOGLE_DRIVE_FOLDER_ID, PREVIEW_UPLOAD_SUBFOLDER)
    result = service.files().list(
        q=(
            f"'{preview_root_id}' in parents and "
            "mimeType = 'application/vnd.google-apps.folder' and "
            "trashed = false"
        ),
        fields="files(id,name)",
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    existing_folders: dict[str, str] = {
        f["name"]: f["id"] for f in result.get("files", [])
        if f["name"] != SAFE_DELETE_SUBFOLDER
    }

    expected_preview_names: set[str] = set()
    for row in all_rows:
        media_link = _cell_text(row.get("Media Drive Link") or "").strip().split(",")[0].strip()
        if not media_link:
            continue
        username = _cell_text(row.get("Source Username") or "").strip().lstrip("@")
        handle_text = _cell_text(row.get("Speaker Name") or "").strip()
        try:
            folder_name, _ = _preview_folder_base_name(username or handle_text, media_link, row["row_number"])
            expected_preview_names.add(folder_name)
        except Exception:
            pass

    orphan_folders = {n: fid for n, fid in existing_folders.items() if n not in expected_preview_names}
    print(f"Step 4: {len(existing_folders)} preview folder(s), {len(orphan_folders)} orphaned.")

    # Shared safe_for_deletion folder at root level for both phases.
    safe_root_id = get_or_create_subfolder(GOOGLE_DRIVE_FOLDER_ID, SAFE_DELETE_SUBFOLDER)
    archive_folder_id = get_or_create_subfolder(safe_root_id, timestamp)

    if orphan_folders:
        for name, folder_id in orphan_folders.items():
            try:
                service.files().update(
                    fileId=folder_id,
                    addParents=archive_folder_id,
                    removeParents=preview_root_id,
                    fields="id,parents",
                    supportsAllDrives=True,
                ).execute()
                print(f"  Moved preview folder to safe_for_deletion: {name}")
                moved += 1
            except Exception as e:
                print(f"  Could not move '{name}': {e}")
        print(f"Step 4: Moved {moved} orphaned preview folder(s) to safe_for_deletion/{timestamp}.")
    else:
        print("Step 4: No orphaned preview folders.")

    # --- Phase 2: root-level _segments folders and Drive-duplicate (n) files ---
    root_result = service.files().list(
        q=f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed = false",
        fields="files(id,name,mimeType)",
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    root_items = root_result.get("files", [])

    root_moved = 0
    for item in root_items:
        name = item["name"]
        file_id = item["id"]
        is_folder = item["mimeType"] == "application/vnd.google-apps.folder"

        # Skip managed subfolders.
        if name in (PREVIEW_UPLOAD_SUBFOLDER, SAFE_DELETE_SUBFOLDER):
            continue

        should_move = False
        reason = ""

        # Old-style _segments folder at root (these belong inside previews/).
        if is_folder and name.endswith("_segments"):
            base = name[: -len("_segments")]
            if base not in active_stems:
                should_move = True
                reason = "orphaned _segments folder"

        # Drive-created duplicate: filename contains " (n)" before the extension.
        elif not is_folder and re.search(r" \(\d+\)(\.[^.]+)?$", name):
            should_move = True
            reason = "Drive duplicate"

        if should_move:
            try:
                service.files().update(
                    fileId=file_id,
                    addParents=archive_folder_id,
                    removeParents=GOOGLE_DRIVE_FOLDER_ID,
                    fields="id,parents",
                    supportsAllDrives=True,
                ).execute()
                print(f"  Moved ({reason}) to safe_for_deletion: {name}")
                root_moved += 1
            except Exception as e:
                print(f"  Could not move '{name}': {e}")

    print(f"Step 4: Moved {root_moved} root-level orphan(s) to safe_for_deletion/{timestamp}.")
    return moved + root_moved


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    print("=== Instagram Pipeline Run ===")

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID is not configured.")
        return 1

    step1_ingest(GOOGLE_SHEET_ID)

    print("Reloading sheet…")
    try:
        all_rows = get_all_rows(GOOGLE_SHEET_ID)
    except Exception as e:
        print(f"ERROR: Could not reload sheet after ingest: {e}")
        return 1

    step2_transcribe(GOOGLE_SHEET_ID, all_rows)
    step3_split(all_rows)
    step4_cleanup(all_rows)

    print("=== Run complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
