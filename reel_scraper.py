"""yt-dlp reel scraper — fetches video URL, thumbnail, and metadata.

Uses an authenticated Instagram session via a cookies file.
Swappable module: replace this file to use a different reel source.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone
from urllib.parse import urlparse

from instagram_cookies import instagram_cookies_file


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


def _ext_from_url(url: str, fallback: str) -> str:
    path = urlparse(url).path or ""
    match = re.search(r"(\.[a-zA-Z0-9]{2,5})$", path)
    if not match:
        return fallback
    ext = match.group(1).lower()
    return ext if ext.startswith(".") else f".{ext}"


def process_url(url: str, include_transcript: bool = False, cookies_path: str | None = None) -> dict:
    """Scrape an Instagram reel via yt-dlp.

    Returns:
        username, media_type, media_urls, thumbnail_url,
        original_caption, transcript, photo_count, post_id, post_date
    Raises RuntimeError on failure.
    """
    ytdlp = _ytdlp_path()
    with instagram_cookies_file(cookies_path) as cookies_path:
        cmd = [
            ytdlp, "--dump-json", "--no-download", "--quiet", "--no-warnings", "--no-update",
            "--cookies", cookies_path,
            url,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)

    stdout = result.stdout.strip()
    if not stdout:
        raise RuntimeError(f"yt-dlp returned no output. stderr: {result.stderr.strip()[:300]}")

    json_line = next((line for line in stdout.splitlines() if line.startswith("{")), "")
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
        "media_extensions": [_ext_from_url(video_url, ".mp4")],
        "thumbnail_url": thumbnail_url,
        "original_caption": original_caption,
        "transcript": "",
        "photo_count": 0,
        "post_id": post_id,
        "post_date": post_date,
    }
