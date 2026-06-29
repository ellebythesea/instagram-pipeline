"""yt-dlp post scraper — fetches photo/video URLs, caption, and metadata.

Uses an authenticated Instagram session via a cookies file.
Swappable module: replace this file to use a different post source.
Handles single images, carousels, and video posts.
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


def _ext_from_url(url: str, fallback: str) -> str:
    path = urlparse(url).path or ""
    match = re.search(r"(\.[a-zA-Z0-9]{2,5})$", path)
    if not match:
        return fallback
    ext = match.group(1).lower()
    return ext if ext.startswith(".") else f".{ext}"


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


def _parse_entry(meta: dict) -> dict | None:
    """Turn one yt-dlp JSON object into a media entry dict."""
    formats = meta.get("formats", [])
    if formats:
        video_url = _pick_video_url(formats)
        if video_url:
            return {
                "kind": "video",
                "url": video_url,
                "thumbnail_url": meta.get("thumbnail", ""),
                "ext": _ext_from_url(video_url, ".mp4"),
            }

    # Direct image URL (no formats list — yt-dlp returns url at top level for images)
    direct_url = meta.get("url", "")
    if direct_url and meta.get("ext", "") in ("jpg", "jpeg", "png", "webp"):
        return {
            "kind": "image",
            "url": direct_url,
            "thumbnail_url": direct_url,
            "ext": f".{meta['ext']}",
        }

    # Fallback: thumbnail as image
    thumbnail = meta.get("thumbnail", "")
    if thumbnail:
        return {
            "kind": "image",
            "url": thumbnail,
            "thumbnail_url": thumbnail,
            "ext": _ext_from_url(thumbnail, ".jpg"),
        }

    return None


def process_url(url: str, cookies_path: str | None = None) -> dict:
    """Scrape an Instagram post via yt-dlp.

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

    # Carousels produce one JSON object per line; collect all of them.
    json_lines = [line for line in stdout.splitlines() if line.startswith("{")]
    if not json_lines:
        raise RuntimeError(f"yt-dlp output had no JSON: {stdout[:200]}")

    first = json.loads(json_lines[0])

    username = first.get("channel") or first.get("uploader_id") or "unknown"
    original_caption = first.get("description", "")
    post_id = first.get("id") or first.get("display_id") or _extract_post_id(url)

    ts = first.get("timestamp")
    if ts:
        post_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    else:
        upload_date = first.get("upload_date", "")
        if len(upload_date) == 8:
            post_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
        else:
            post_date = date.today().isoformat()

    entries: list[dict] = []
    seen_urls: set[str] = set()
    for line in json_lines:
        meta = json.loads(line)
        entry = _parse_entry(meta)
        if entry and entry["url"] not in seen_urls:
            entries.append(entry)
            seen_urls.add(entry["url"])

    if not entries:
        raise RuntimeError("yt-dlp: no usable media URLs found in post response")

    media_urls = [e["url"] for e in entries]
    media_kinds = [e["kind"] for e in entries]
    media_extensions = [e["ext"] for e in entries]

    thumbnail_url = (
        next((e["thumbnail_url"] for e in entries if e["kind"] == "image" and e["thumbnail_url"]), "")
        or entries[0]["thumbnail_url"]
    )

    photo_count = sum(1 for k in media_kinds if k == "image")

    if len(entries) == 1 and entries[0]["kind"] == "video":
        media_type = "reel"
        photo_count = 0
    else:
        media_type = "photo"

    return {
        "username": username,
        "media_type": media_type,
        "media_urls": media_urls,
        "media_kinds": media_kinds,
        "media_extensions": media_extensions,
        "thumbnail_url": thumbnail_url,
        "original_caption": original_caption,
        "transcript": "",
        "photo_count": photo_count,
        "post_id": post_id,
        "post_date": post_date,
    }
