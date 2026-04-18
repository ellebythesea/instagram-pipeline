"""Download Instagram media via Apify — swappable replacement for instagram.py."""

import os
import shutil
import tempfile
from typing import Optional

import requests
from apify_client import ApifyClient

from config import APIFY_API_TOKEN, APIFY_ACTOR_ID
from instagram import InstagramPost


def download_instagram_post(url: str) -> Optional[InstagramPost]:
    """Download an Instagram post via Apify and return metadata + local media path.

    Returns an InstagramPost or None on failure.
    The caller is responsible for cleaning up the temp directory containing the file.
    """
    tmp_dir = None
    try:
        client = ApifyClient(APIFY_API_TOKEN)

        run = client.actor(APIFY_ACTOR_ID).call(
            run_input={
                "directUrls": [url],
                "resultsType": "posts",
                "resultsLimit": 1,
            },
            timeout_secs=300,
        )

        if not run or run.get("status") != "SUCCEEDED":
            return None

        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        if not items:
            return None

        item = items[0]

        username = (
            item.get("ownerUsername")
            or item.get("username")
            or item.get("owner", {}).get("username")
            or "unknown"
        )
        original_caption = item.get("caption") or item.get("description") or ""
        title = item.get("shortCode") or url

        # Determine media type and URL
        is_video = bool(
            item.get("isVideo")
            or item.get("videoUrl")
            or item.get("type", "").lower() in {"video", "reel"}
        )
        if is_video:
            media_url = (
                item.get("videoUrl")
                or item.get("videoHDUrl")
                or item.get("videoLDUrl")
            )
        else:
            media_url = (
                item.get("displayUrl")
                or item.get("imageUrl")
                or item.get("thumbnailUrl")
            )

        if not media_url:
            return None

        ext = ".mp4" if is_video else ".jpg"
        tmp_dir = tempfile.mkdtemp(prefix="ig_")
        media_path = os.path.join(tmp_dir, f"media{ext}")

        resp = requests.get(media_url, timeout=120, stream=True)
        resp.raise_for_status()
        with open(media_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        return InstagramPost(
            video_path=media_path,
            original_caption=original_caption,
            username=username,
            title=title,
            media_type="video" if is_video else "image",
        )

    except Exception:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return None
