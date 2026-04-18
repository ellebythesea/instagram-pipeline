"""Apify reel scraper — fetches video URL, thumbnail, transcript, and metadata.

Swappable module. Replace this file to use a different reel source.
Expected input schema for actor xMc5Ga1oCONPmWJIa: {"url": "<reel_url>"}
Check your actor's input schema in the Apify console if this fails.
"""

import re
from datetime import date, datetime, timezone

from apify_client import ApifyClient

from config import APIFY_API_TOKEN, APIFY_REEL_ACTOR_ID


def _extract_post_id(url: str) -> str:
    m = re.search(r"/(?:reel|reels|p)/([A-Za-z0-9_-]+)/?", url)
    return m.group(1) if m else "unknown"


def process_url(url: str) -> dict:
    """Scrape an Instagram reel via Apify.

    Returns:
        username, media_type, media_urls, thumbnail_url,
        original_caption, transcript, photo_count, post_id, post_date
    Raises RuntimeError on failure.
    """
    client = ApifyClient(APIFY_API_TOKEN)
    run = client.actor(APIFY_REEL_ACTOR_ID).call(
        run_input={"url": url},
        timeout_secs=300,
    )

    if not run or run.get("status") != "SUCCEEDED":
        raise RuntimeError(
            f"Reel actor failed: {run.get('status') if run else 'no response'}"
        )

    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    if not items:
        raise RuntimeError("Reel actor returned no items")

    item = items[0]

    username = (
        item.get("ownerUsername")
        or item.get("username")
        or (item.get("owner") or {}).get("username")
        or "unknown"
    )

    video_url = (
        item.get("videoUrl")
        or item.get("video_url")
        or item.get("videoHDUrl")
        or item.get("url")
    )
    if not video_url:
        raise RuntimeError("No video URL in reel actor response")

    thumbnail_url = (
        item.get("thumbnailUrl")
        or item.get("thumbnail_url")
        or item.get("displayUrl")
        or item.get("previewUrl")
        or ""
    )

    transcript = (
        item.get("transcript")
        or item.get("transcription")
        or item.get("captions")
        or ""
    )
    if isinstance(transcript, list):
        transcript = " ".join(str(t) for t in transcript)

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

    return {
        "username": username,
        "media_type": "reel",
        "media_urls": [video_url],
        "thumbnail_url": thumbnail_url,
        "original_caption": original_caption,
        "transcript": transcript,
        "photo_count": 0,
        "post_id": post_id,
        "post_date": post_date,
    }
