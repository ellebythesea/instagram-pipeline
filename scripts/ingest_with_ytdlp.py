#!/usr/bin/env python3
"""Ingest pending reel rows using yt-dlp.

Use this when Apify is being blocked by Instagram. Uses your Instagram session
via a cookies file (local or from Secret Manager).

Usage:
    .venv/bin/python scripts/ingest_with_ytdlp.py

Override the cookies file if needed:
    .venv/bin/python scripts/ingest_with_ytdlp.py --cookies /path/to/cookies.txt

Cookies file:
    Export from Chrome using the "Get cookies.txt LOCALLY" extension while
    logged in to instagram.com. Save as www.instagram.com_cookies.txt in the
    repo root (gitignored). Or upload the file contents to Secret Manager as
    'instagram-cookies' for cloud/mobile use.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from caption import transcribe_video
from config import GOOGLE_SHEET_ID
from ingest_helpers import build_filename_prefix, upload_media_bundle
from instagram_cookies import instagram_cookies_file
from pipeline_caption import generate_row_caption
from post_scraper import process_url as scrape_post
from reel_scraper import process_url as scrape_reel
from sheets import get_pending_rows, update_caption, update_ingest_result


def _is_reel(url: str) -> bool:
    return "/reel/" in url.lower() or "/reels/" in url.lower()


def _is_instagram(url: str) -> bool:
    return "instagram.com" in url.lower()


def ingest_row(row: dict, cookies_path: str) -> dict:
    url = (row.get("Instagram URL") or "").strip()
    if not _is_instagram(url):
        return {"status": "skipped: not an Instagram URL"}

    tmp_dir = None
    try:
        if _is_reel(url):
            data = scrape_reel(url, cookies_path=cookies_path)
        else:
            data = scrape_post(url, cookies_path=cookies_path)
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
    parser.add_argument("--cookies", default=None, help="Path to cookies.txt file (overrides auto-detection)")
    args = parser.parse_args()

    with instagram_cookies_file(args.cookies) as cookies_path:
        pending = get_pending_rows(GOOGLE_SHEET_ID)
        instagram_rows = [r for r in pending if _is_instagram(r.get("Instagram URL") or "")]

        if not instagram_rows:
            print("No pending Instagram rows found.")
            return

        print(f"Found {len(instagram_rows)} pending Instagram row(s). Using cookies: {cookies_path}\n")
        succeeded = 0

        for row in instagram_rows:
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

        print(f"\nDone: {succeeded}/{len(instagram_rows)} row(s) ingested.")


if __name__ == "__main__":
    main()
