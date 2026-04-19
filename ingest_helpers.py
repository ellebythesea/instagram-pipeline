"""Shared helpers for downloading Instagram media and uploading it to Drive."""

import os
import tempfile

import requests

from config import GOOGLE_DRIVE_FOLDER_ID
from drive import upload_to_drive


def download_file(url: str, dest: str) -> None:
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def make_filename(post_id: str, post_date: str, ext: str, index: int = 0) -> str:
    suffix = f"_{index}" if index > 0 else ""
    return f"{post_date}_{post_id}{suffix}{ext}"


def upload_media_bundle(data: dict) -> dict:
    """Download media locally, upload to Drive, and return Drive links."""
    tmp_dir = tempfile.mkdtemp(prefix="ig_")
    ext = ".mp4" if data["media_type"] == "reel" else ".jpg"
    post_id = data["post_id"]
    post_date = data["post_date"]

    media_links = []
    for i, media_url in enumerate(data["media_urls"]):
        filename = make_filename(post_id, post_date, ext, index=i)
        local_path = os.path.join(tmp_dir, filename)
        download_file(media_url, local_path)
        media_links.append(upload_to_drive(local_path, filename, GOOGLE_DRIVE_FOLDER_ID))

    thumbnail_link = ""
    if data.get("thumbnail_url"):
        thumb_filename = f"{post_date}_{post_id}_thumb.jpg"
        thumb_path = os.path.join(tmp_dir, thumb_filename)
        try:
            download_file(data["thumbnail_url"], thumb_path)
            thumbnail_link = upload_to_drive(thumb_path, thumb_filename, GOOGLE_DRIVE_FOLDER_ID)
        except Exception:
            thumbnail_link = media_links[0] if media_links else ""

    return {
        "tmp_dir": tmp_dir,
        "media_link": ", ".join(media_links),
        "thumbnail_link": thumbnail_link,
    }


def upload_thumbnail_only(data: dict) -> dict:
    """Upload only the thumbnail image for a reel/post and skip media upload."""
    tmp_dir = tempfile.mkdtemp(prefix="ig_")
    thumbnail_link = ""

    if data.get("thumbnail_url"):
        thumb_filename = f"{data['post_date']}_{data['post_id']}_thumb.jpg"
        thumb_path = os.path.join(tmp_dir, thumb_filename)
        download_file(data["thumbnail_url"], thumb_path)
        thumbnail_link = upload_to_drive(thumb_path, thumb_filename, GOOGLE_DRIVE_FOLDER_ID)

    return {
        "tmp_dir": tmp_dir,
        "media_link": "",
        "thumbnail_link": thumbnail_link,
    }
