"""Shared helpers for downloading Instagram media and uploading thumbnails to Drive."""

import io
import os
import tempfile
import zipfile
import mimetypes

import requests

from config import GOOGLE_DRIVE_FOLDER_ID, GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER
from drive import get_or_create_subfolder, upload_to_drive


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
    screenshots_folder_id = get_or_create_subfolder(
        GOOGLE_DRIVE_FOLDER_ID,
        GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    )

    media_links = []
    media_paths = []
    for i, media_url in enumerate(data["media_urls"]):
        filename = make_filename(post_id, post_date, ext, index=i)
        local_path = os.path.join(tmp_dir, filename)
        download_file(media_url, local_path)
        media_paths.append(local_path)
        media_links.append(upload_to_drive(local_path, filename, GOOGLE_DRIVE_FOLDER_ID))

    thumbnail_link = ""
    thumbnail_path = ""
    if data.get("thumbnail_url"):
        thumb_filename = f"{post_date}_{post_id}_thumb.jpg"
        thumb_path = os.path.join(tmp_dir, thumb_filename)
        try:
            download_file(data["thumbnail_url"], thumb_path)
            thumbnail_path = thumb_path
            thumbnail_link = upload_to_drive(thumb_path, thumb_filename, screenshots_folder_id)
        except Exception:
            thumbnail_link = media_links[0] if media_links else ""

    return {
        "tmp_dir": tmp_dir,
        "media_paths": media_paths,
        "media_link": ", ".join(media_links),
        "thumbnail_link": thumbnail_link,
        "thumbnail_path": thumbnail_path,
    }


def download_media_bundle(data: dict) -> dict:
    """Download media locally, upload thumbnail to Drive, and return local file paths."""
    tmp_dir = tempfile.mkdtemp(prefix="ig_")
    ext = ".mp4" if data["media_type"] == "reel" else ".jpg"
    post_id = data["post_id"]
    post_date = data["post_date"]

    media_paths = []
    for i, media_url in enumerate(data["media_urls"]):
        filename = make_filename(post_id, post_date, ext, index=i)
        local_path = os.path.join(tmp_dir, filename)
        download_file(media_url, local_path)
        media_paths.append(local_path)

    thumbnail_link = ""
    screenshots_folder_id = get_or_create_subfolder(
        GOOGLE_DRIVE_FOLDER_ID,
        GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    )
    if data.get("thumbnail_url"):
        thumb_filename = f"{post_date}_{post_id}_thumb.jpg"
        thumb_path = os.path.join(tmp_dir, thumb_filename)
        try:
            download_file(data["thumbnail_url"], thumb_path)
            thumbnail_link = upload_to_drive(thumb_path, thumb_filename, screenshots_folder_id)
        except Exception:
            thumbnail_link = ""

    return {
        "tmp_dir": tmp_dir,
        "media_paths": media_paths,
        "media_link": "",
        "thumbnail_link": thumbnail_link,
    }


def upload_thumbnail_only(data: dict) -> dict:
    """Upload only the thumbnail image for a reel/post and skip media upload."""
    tmp_dir = tempfile.mkdtemp(prefix="ig_")
    thumbnail_link = ""
    screenshots_folder_id = get_or_create_subfolder(
        GOOGLE_DRIVE_FOLDER_ID,
        GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    )

    if data.get("thumbnail_url"):
        thumb_filename = f"{data['post_date']}_{data['post_id']}_thumb.jpg"
        thumb_path = os.path.join(tmp_dir, thumb_filename)
        download_file(data["thumbnail_url"], thumb_path)
        thumbnail_link = upload_to_drive(thumb_path, thumb_filename, screenshots_folder_id)

    return {
        "tmp_dir": tmp_dir,
        "media_link": "",
        "thumbnail_link": thumbnail_link,
    }


def build_download_payload(media_paths: list[str], base_name: str) -> tuple[str, bytes, str]:
    """Return filename, bytes, and mime type for browser download."""
    if len(media_paths) == 1:
        path = media_paths[0]
        with open(path, "rb") as f:
            payload = f.read()
        return (
            os.path.basename(path),
            payload,
            mimetypes.guess_type(path)[0] or "application/octet-stream",
        )

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in media_paths:
            zf.write(path, arcname=os.path.basename(path))
    return (f"{base_name}.zip", zip_buffer.getvalue(), "application/zip")
