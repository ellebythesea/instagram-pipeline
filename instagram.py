# instagram.py
"""Download Instagram videos and extract metadata using yt-dlp."""

import subprocess
import tempfile
import json
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class InstagramPost:
    video_path: str
    original_caption: str
    username: str
    title: str
    media_type: str = "video"


def download_instagram_post(url: str) -> Optional[InstagramPost]:
    """Download an Instagram video and return its metadata.

    Returns an InstagramPost with paths and metadata, or None on failure.
    The caller is responsible for cleaning up the video file.
    """
    tmp_dir = tempfile.mkdtemp(prefix="ig_")
    output_template = os.path.join(tmp_dir, "%(id)s.%(ext)s")

    # First, grab metadata without downloading
    meta_cmd = [
        "yt-dlp",
        "--no-download",
        "--dump-json",
        "--no-warnings",
        url,
    ]

    try:
        meta_result = subprocess.run(
            meta_cmd,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if meta_result.returncode != 0:
            return None

        meta = json.loads(meta_result.stdout)
        username = meta.get("uploader") or meta.get("channel") or meta.get("uploader_id") or "unknown"
        original_caption = meta.get("description") or meta.get("title") or ""
        title = meta.get("title") or ""

    except Exception:
        username = "unknown"
        original_caption = ""
        title = ""

    # Now download the video
    dl_cmd = [
        "yt-dlp",
        "--no-warnings",
        "-f", "best[ext=mp4]/best",
        "-o", output_template,
        "--no-playlist",
        url,
    ]

    try:
        dl_result = subprocess.run(
            dl_cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if dl_result.returncode != 0:
            return None

        # Find the downloaded file
        files = [f for f in os.listdir(tmp_dir) if os.path.isfile(os.path.join(tmp_dir, f))]
        if not files:
            return None

        video_path = os.path.join(tmp_dir, files[0])

        return InstagramPost(
            video_path=video_path,
            original_caption=original_caption,
            username=username,
            title=title,
        )

    except Exception:
        return None
