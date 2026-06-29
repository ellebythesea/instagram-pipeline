#!/usr/bin/env python3
"""Ingest pending reel rows using yt-dlp instead of Apify.

Use this when Apify is being blocked by Instagram. Requires a cookies file
exported from Chrome while logged in to Instagram.

Usage:
    .venv/bin/python scripts/ingest_with_ytdlp.py

Cookies file:
    Export from Chrome using the "Get cookies.txt LOCALLY" extension while
    logged in to instagram.com. Save the file to the repo root as:
        www.instagram.com_cookies.txt

    The script looks for the cookies file at that path by default.
    Override with --cookies:
        .venv/bin/python scripts/ingest_with_ytdlp.py --cookies /path/to/cookies.txt
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from caption import transcribe_video
from config import GOOGLE_SHEET_ID
from ingest_helpers import build_filename_prefix, upload_media_bundle
from pipeline_caption import generate_row_caption
from sheets import get_pending_rows, update_caption, update_ingest_result

DEFAULT_COOKIES_PATH = REPO_ROOT / "www.instagram.com_cookies.txt"


def _ytdlp_path() -> str:
    venv_yt = os.path.join(os.path.dirname(sys.executable), "yt-dlp")
    if os.path.exists(venv_yt):
        return venv_yt
    return shutil.which("yt-dlp") or "yt-dlp"


def _extract_post_id(url: str) -> str:
    m = re.search(r"/(?:reel|reels|p)/([A-Za-z0-9_-]+)/?", url)
    return m.group(1) if m else "unknown"


def _pick_video_url(formats: list) -> str:
    for fmt in formats:
        if (
            fmt.get("ext") == "mp4"
            and fmt.get("protocol") == "https"
            and not fmt.get("is_dash_periods")
            and fmt.get("video_ext") == "mp4"
        ):
            return fmt["url"]
    for fmt in formats:
        if fmt.get("url") and fmt.get("ext") == "mp4" and fmt.get("protocol") == "https":
            return fmt["url"]
    return ""


def scrape_reel(url: str, cookies_path: str) -> dict:
    """Fetch reel metadata via yt-dlp. Returns the same dict shape as reel_scraper.process_url."""
    ytdlp = _ytdlp_path()
    cmd = [
        ytdlp, "--dump-json", "--no-download", "--quiet", "--no-warnings", "--no-update",
        "--cookies", cookies_path,
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)

    stdout = result.stdout.strip()
    if not stdout:
        raise RuntimeError(f"yt-dlp returned no output. stderr: {result.stderr.strip()[:300]}")

    json_line = next((l for l in stdout.splitlines() if l.startswith("{")), "")
    if not json_line:
        raise RuntimeError(f"yt-dlp output had no JSON: {stdout[:200]}")

    meta = json.loads(json_line)

    username = meta.get("channel") or meta.get("uploader_id") or "unknown"
    original_caption = meta.get("description", "")
    post_id = meta.get("id") or meta.get("display_id") or _extract_post_id(url)
    thumbnail_url = meta.get("thumbnail", "")

    ts = meta.get("timestamp")
    if ts:
        post_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    else:
        upload_date = meta.get("upload_date", "")
        if len(upload_date) == 8:
            post_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
        else:
            post_date = date.today().isoformat()

    video_url = _pick_video_url(meta.get("formats", []))
    if not video_url:
        raise RuntimeError("yt-dlp: no usable progressive video URL found")

    return {
        "username": username,
        "media_type": "reel",
        "media_urls": [video_url],
        "media_kinds": ["video"],
        "media_extensions": [".mp4"],
        "thumbnail_url": thumbnail_url,
        "original_caption": original_caption,
        "transcript": "",
        "photo_count": 0,
        "post_id": post_id,
        "post_date": post_date,
    }


def ingest_row(row: dict, cookies_path: str) -> dict:
    url = (row.get("Instagram URL") or "").strip()
    is_reel = "/reel/" in url.lower() or "/reels/" in url.lower()
    if not is_reel:
        return {"status": "skipped: not a reel"}

    tmp_dir = None
    try:
        data = scrape_reel(url, cookies_path)
        filename_prefix = build_filename_prefix(row.get("row_number"), data["username"])
        uploaded = upload_media_bundle(data, filename_prefix=filename_prefix)
        tmp_dir = uploaded["tmp_dir"]

        transcript = ""
        if uploaded.get("media_paths"):
            try:
                transcript = transcribe_video(uploaded["media_paths"][0]) or ""
            except Exception as e:
                print(f"    Whisper error: {e}")

        return {
            "username": data["username"],
            "media_type": data["media_type"],
            "photo_count": data["photo_count"],
            "media_link": uploaded["media_link"],
            "thumbnail_link": uploaded["thumbnail_link"],
            "original_caption": data["original_caption"],
            "transcript": transcript,
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cookies", default=str(DEFAULT_COOKIES_PATH), help="Path to cookies.txt file")
    args = parser.parse_args()

    cookies_path = args.cookies
    if not os.path.exists(cookies_path):
        print(f"ERROR: Cookies file not found: {cookies_path}")
        print("Export it from Chrome using 'Get cookies.txt LOCALLY' while logged in to instagram.com.")
        sys.exit(1)

    pending = get_pending_rows(GOOGLE_SHEET_ID)
    reels = [r for r in pending if "/reel/" in (r.get("Instagram URL") or "").lower()
             or "/reels/" in (r.get("Instagram URL") or "").lower()]

    if not reels:
        print("No pending reel rows found.")
        return

    print(f"Found {len(reels)} pending reel row(s). Using cookies: {cookies_path}\n")
    succeeded = 0

    for row in reels:
        row_num = row["row_number"]
        url = (row.get("Instagram URL") or "").strip()
        print(f"Row {row_num}: {url}")

        result = ingest_row(row, cookies_path)
        status = result.get("status", "")

        if status == "ingested":
            update_ingest_result(
                GOOGLE_SHEET_ID,
                row_num,
                result["username"],
                result["media_type"],
                result["photo_count"],
                result["media_link"],
                result["thumbnail_link"],
                result["original_caption"],
                result["transcript"],
                "ingested",
            )
            updated_row = {**row, **{
                "Source Username": result["username"],
                "Media Type": result["media_type"],
                "Original Caption": result["original_caption"],
                "Transcript": result["transcript"],
            }}
            caption = generate_row_caption(updated_row)
            update_caption(GOOGLE_SHEET_ID, row_num, caption, "done")
            print(f"  ✓ @{result['username']} — {len(result['transcript'])} char transcript")
            succeeded += 1
        elif status.startswith("skipped"):
            print(f"  — {status}")
        else:
            print(f"  ✗ {status}")

    print(f"\nDone: {succeeded}/{len(reels)} reel(s) ingested.")


if __name__ == "__main__":
    main()
