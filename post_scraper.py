"""Instagram post scraper — fetches photo/video URLs, caption, and metadata.

Uses Instagram's private API with an authenticated session cookie.
Handles single images, carousels (mixed photo/video), and video posts.
Swappable module: replace this file to use a different post source.
"""
from __future__ import annotations

import http.cookiejar
import re
from datetime import date, datetime, timezone
from urllib.parse import urlparse

import requests

from instagram_cookies import instagram_cookies_file

_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

_IG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "X-IG-App-ID": "936619743392459",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def _shortcode_to_media_id(shortcode: str) -> str:
    n = 0
    for char in shortcode:
        n = n * 64 + _ALPHABET.index(char)
    return str(n)


def _extract_shortcode(url: str) -> str:
    m = re.search(r"/(?:p|reel|reels)/([A-Za-z0-9_-]+)/?", url)
    return m.group(1) if m else ""


def _ext_from_url(url: str, fallback: str) -> str:
    path = urlparse(url).path or ""
    match = re.search(r"(\.[a-zA-Z0-9]{2,5})$", path)
    if not match:
        return fallback
    ext = match.group(1).lower()
    return ext if ext.startswith(".") else f".{ext}"


def _best_image(candidates: list) -> str:
    if not candidates:
        return ""
    return max(candidates, key=lambda c: c.get("width", 0) * c.get("height", 0)).get("url", "")


def _best_video(versions: list) -> str:
    if not versions:
        return ""
    return max(versions, key=lambda v: v.get("width", 0) * v.get("height", 0)).get("url", "")


def _make_session(cookies_path: str) -> requests.Session:
    cj = http.cookiejar.MozillaCookieJar()
    cj.load(cookies_path, ignore_discard=True, ignore_expires=True)
    session = requests.Session()
    session.cookies = requests.utils.cookiejar_from_dict(
        {c.name: c.value for c in cj}
    )
    csrf = session.cookies.get("csrftoken", "")
    session.headers.update({**_IG_HEADERS, "X-CSRFToken": csrf})
    return session


def process_url(url: str, cookies_path: str | None = None) -> dict:
    """Scrape an Instagram post via Instagram's private API.

    Returns:
        username, media_type, media_urls, thumbnail_url,
        original_caption, transcript, photo_count, post_id, post_date
    Raises RuntimeError on failure.
    """
    shortcode = _extract_shortcode(url)
    if not shortcode:
        raise RuntimeError(f"Could not extract shortcode from URL: {url}")

    with instagram_cookies_file(cookies_path) as ck_path:
        session = _make_session(ck_path)
        media_id = _shortcode_to_media_id(shortcode)
        resp = session.get(
            f"https://www.instagram.com/api/v1/media/{media_id}/info/",
            headers={"Referer": f"https://www.instagram.com/p/{shortcode}/"},
            timeout=30,
        )

    if resp.status_code == 401:
        raise RuntimeError("Instagram session expired. Update your cookies.")
    if not resp.ok:
        raise RuntimeError(f"Instagram API error {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    items = data.get("items", [])
    if not items:
        raise RuntimeError("Instagram API returned no items")
    item = items[0]

    username = (item.get("user") or {}).get("username") or "unknown"
    original_caption = (item.get("caption") or {}).get("text") or ""
    post_id = item.get("code") or shortcode

    ts = item.get("taken_at")
    if ts:
        post_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    else:
        post_date = date.today().isoformat()

    media_type_code = item.get("media_type", 1)  # 1=image 2=video 8=carousel
    entries: list[dict] = []

    def _add_image(node: dict) -> None:
        img = _best_image((node.get("image_versions2") or {}).get("candidates", []))
        if img:
            entries.append({"kind": "image", "url": img, "thumbnail_url": img, "ext": ".jpg"})

    def _add_video(node: dict) -> None:
        vid = _best_video(node.get("video_versions", []))
        thumb = _best_image((node.get("image_versions2") or {}).get("candidates", []))
        if vid:
            entries.append({"kind": "video", "url": vid, "thumbnail_url": thumb, "ext": ".mp4"})

    if media_type_code == 8:  # carousel
        for child in item.get("carousel_media", []):
            if child.get("media_type") == 2:
                _add_video(child)
            else:
                _add_image(child)
    elif media_type_code == 2:  # single video
        _add_video(item)
    else:  # single image
        _add_image(item)

    if not entries:
        raise RuntimeError("No media URLs found in Instagram API response")

    media_urls = [e["url"] for e in entries]
    media_kinds = [e["kind"] for e in entries]
    media_extensions = [e["ext"] for e in entries]
    thumbnail_url = next((e["thumbnail_url"] for e in entries if e["thumbnail_url"]), "")
    photo_count = sum(1 for k in media_kinds if k == "image")

    if len(entries) == 1 and entries[0]["kind"] == "video":
        post_media_type = "reel"
        photo_count = 0
    else:
        post_media_type = "photo"

    return {
        "username": username,
        "media_type": post_media_type,
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
