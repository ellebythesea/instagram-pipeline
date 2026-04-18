"""Apify post scraper — fetches image URLs, original caption, and metadata.

Swappable module. Replace this file to use a different post source.
Uses apify/instagram-scraper with directUrls input.
"""

import re
from datetime import date, datetime, timezone

from apify_client import ApifyClient

from config import APIFY_API_TOKEN, APIFY_POST_ACTOR_ID


def _extract_post_id(url: str) -> str:
    m = re.search(r"/(?:reel|reels|p)/([A-Za-z0-9_-]+)/?", url)
    return m.group(1) if m else "unknown"


def process_url(url: str) -> dict:
    """Scrape an Instagram photo or carousel via Apify.

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

    # Collect image URLs — handle single photo and carousels
    image_urls = []
    carousel = item.get("images") or item.get("sidecarImages") or []
    for img in carousel:
        if isinstance(img, dict):
            u = img.get("url") or img.get("displayUrl") or img.get("src") or ""
        else:
            u = str(img)
        if u:
            image_urls.append(u)

    if not image_urls:
        single = item.get("displayUrl") or item.get("imageUrl") or item.get("url")
        if single:
            image_urls = [single]

    if not image_urls:
        raise RuntimeError("No image URLs in post actor response")

    return {
        "username": username,
        "media_type": "photo",
        "media_urls": image_urls,
        "thumbnail_url": image_urls[0],
        "original_caption": original_caption,
        "transcript": "",
        "photo_count": len(image_urls),
        "post_id": post_id,
        "post_date": post_date,
    }
