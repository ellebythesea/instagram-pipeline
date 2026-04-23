"""Apify post scraper — fetches photo/video URLs, caption, and metadata.

Swappable module. Replace this file to use a different post source.
Uses apify/instagram-scraper with directUrls input.
"""

import re
from datetime import date, datetime, timezone
from urllib.parse import urlparse

from apify_client import ApifyClient

from config import APIFY_API_TOKEN, APIFY_POST_ACTOR_ID


def _extract_post_id(url: str) -> str:
    m = re.search(r"/(?:reel|reels|p)/([A-Za-z0-9_-]+)/?", url)
    return m.group(1) if m else "unknown"


def _normalize_ext(value: str, fallback: str) -> str:
    cleaned = (value or "").strip().lower()
    if not cleaned:
        return fallback
    if not cleaned.startswith("."):
        cleaned = f".{cleaned}"
    return cleaned


def _ext_from_url(url: str, fallback: str) -> str:
    path = urlparse(url).path or ""
    match = re.search(r"(\.[a-zA-Z0-9]{2,5})$", path)
    if not match:
        return fallback
    return _normalize_ext(match.group(1), fallback)


def _build_media_entry(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None

    video_url = (
        item.get("videoUrl")
        or item.get("videoHDUrl")
        or item.get("videoLDUrl")
        or item.get("video_url")
        or item.get("videoSrc")
    )
    image_url = (
        item.get("displayUrl")
        or item.get("imageUrl")
        or item.get("url")
        or item.get("src")
        or item.get("thumbnailUrl")
    )
    if video_url:
        return {
            "kind": "video",
            "url": video_url,
            "thumbnail_url": image_url or "",
            "ext": _ext_from_url(video_url, ".mp4"),
        }
    if image_url:
        return {
            "kind": "image",
            "url": image_url,
            "thumbnail_url": image_url,
            "ext": _ext_from_url(image_url, ".jpg"),
        }
    return None


def _extract_carousel_entries(item: dict) -> list[dict]:
    entries: list[dict] = []
    seen_urls: set[str] = set()
    candidate_groups = [
        item.get("childPosts"),
        item.get("sidecarChildren"),
        item.get("sidecarMedia"),
        item.get("sidecarMedias"),
        item.get("carouselMedia"),
        item.get("carousel_media"),
        item.get("items"),
        item.get("images"),
        item.get("sidecarImages"),
    ]
    for group in candidate_groups:
        if not isinstance(group, list):
            continue
        for child in group:
            entry = _build_media_entry(child)
            if not entry or entry["url"] in seen_urls:
                continue
            entries.append(entry)
            seen_urls.add(entry["url"])
    return entries


def process_url(url: str) -> dict:
    """Scrape an Instagram post or reel via Apify's general Instagram scraper.

    Returns:
        username, media_type, media_urls, thumbnail_url,
        original_caption, transcript, photo_count, post_id, post_date
    Raises RuntimeError on failure.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    run = client.actor(APIFY_POST_ACTOR_ID).call(
        run_input={
            "directUrls": [url],
            "resultsType": "posts",
            "resultsLimit": 1,
        },
        timeout_secs=300,
    )

    if not run or run.get("status") != "SUCCEEDED":
        raise RuntimeError(
            f"Post actor failed: {run.get('status') if run else 'no response'}"
        )

    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    if not items:
        raise RuntimeError("Post actor returned no items")

    item = items[0]

    username = (
        item.get("ownerUsername")
        or item.get("username")
        or (item.get("owner") or {}).get("username")
        or "unknown"
    )

    original_caption = item.get("caption") or item.get("description") or ""

    post_id = (
        item.get("shortCode")
        or item.get("shortcode")
        or item.get("id")
        or _extract_post_id(url)
    )

    ts = item.get("timestamp") or item.get("takenAtTimestamp")
    if ts:
        try:
            if isinstance(ts, (int, float)):
                post_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            else:
                post_date = str(ts)[:10]
        except Exception:
            post_date = date.today().isoformat()
    else:
        post_date = date.today().isoformat()

    # Prefer top-level video fields when the actor returns a reel/video item.
    video_url = (
        item.get("videoUrl")
        or item.get("videoHDUrl")
        or item.get("video_url")
    )
    carousel_entries = _extract_carousel_entries(item)
    if video_url and not carousel_entries:
        thumbnail_url = (
            item.get("thumbnailUrl")
            or item.get("displayUrl")
            or item.get("imageUrl")
            or ""
        )
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

    if not carousel_entries:
        single = item.get("displayUrl") or item.get("imageUrl") or item.get("url")
        if single:
            carousel_entries = [{
                "kind": "image",
                "url": single,
                "thumbnail_url": single,
                "ext": _ext_from_url(single, ".jpg"),
            }]

    if not carousel_entries:
        raise RuntimeError("No media URLs in post actor response")

    media_urls = [entry["url"] for entry in carousel_entries]
    media_kinds = [entry["kind"] for entry in carousel_entries]
    media_extensions = [entry["ext"] for entry in carousel_entries]
    thumbnail_url = (
        next((entry["thumbnail_url"] for entry in carousel_entries if entry["kind"] == "image" and entry["thumbnail_url"]), "")
        or carousel_entries[0].get("thumbnail_url")
        or item.get("thumbnailUrl")
        or ""
    )
    photo_count = sum(1 for kind in media_kinds if kind == "image")

    return {
        "username": username,
        "media_type": "photo",
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
