"""Single-link media downloader page."""

import mimetypes
import os
import sys
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config import GOOGLE_SHEET_ID
from ingest_helpers import upload_media_bundle
from post_scraper import process_url as process_post_url
from reel_scraper import process_url as process_reel_url
from sheets import get_all_rows, update_ingest_result
from utils.auth import require_auth
from utils.styles import inject as inject_styles


def _normalize_url(url: str) -> str:
    return url.strip().rstrip("/")


def _is_reel_url(url: str) -> bool:
    lowered = url.lower()
    return "/reel/" in lowered or "/reels/" in lowered


st.set_page_config(page_title="Media downloader", page_icon="🎞️", layout="centered")
inject_styles()
st.title("Media downloader")
st.caption("Paste one Instagram URL. Reels, single-photo posts, and carousels upload to Drive, then each item can be downloaded individually.")

if not require_auth():
    st.stop()

with st.form("media_download_form"):
    media_url = st.text_input(
        "Instagram URL",
        placeholder="https://www.instagram.com/p/... or /reel/...",
    )
    submitted = st.form_submit_button("Download media", type="primary", use_container_width=True)

if submitted:
    if not media_url.strip():
        st.warning("Please enter an Instagram URL.")
        st.stop()

    tmp_dir = None
    try:
        url = media_url.strip()

        with st.status("Fetching media metadata...", expanded=True) as status:
            if _is_reel_url(url):
                data = process_reel_url(url)
            else:
                data = process_post_url(url)
            status.update(label="Media metadata fetched", state="complete")

        with st.status("Uploading media to Drive...", expanded=True) as status:
            uploaded = upload_media_bundle(data)
            tmp_dir = uploaded["tmp_dir"]
            status.update(label="Media uploaded", state="complete")

        matching_row = next(
            (
                row for row in get_all_rows(GOOGLE_SHEET_ID)
                if _normalize_url(row.get("Instagram URL", "")) == _normalize_url(url)
            ),
            None,
        )
        if matching_row:
            with st.status("Writing media data to Google Sheet...", expanded=True) as status:
                update_ingest_result(
                    GOOGLE_SHEET_ID,
                    matching_row["row_number"],
                    data["username"],
                    data["media_type"],
                    data["photo_count"],
                    uploaded["media_link"],
                    uploaded["thumbnail_link"],
                    data["original_caption"],
                    data["transcript"],
                    "ingested",
                )
                status.update(label=f"Updated sheet row {matching_row['row_number']}", state="complete")

        st.success("Media uploaded to Drive.")
        st.write(f"Username: @{data['username']}")
        st.write(f"Media type: {data['media_type']}")
        if uploaded["media_link"]:
            st.write(f"Media link(s): {uploaded['media_link']}")
        if uploaded["thumbnail_link"]:
            st.write(f"Thumbnail link: {uploaded['thumbnail_link']}")
        if matching_row:
            st.write(f"Sheet row updated: {matching_row['row_number']}")
        else:
            st.info("No matching row found in the sheet for this URL, so nothing was written back to Google Sheets.")

        if data.get("original_caption"):
            with st.expander("Original caption"):
                st.write(data["original_caption"])
        if data.get("transcript"):
            with st.expander("Transcript"):
                st.write(data["transcript"])

        st.subheader("Media files")
        preview_path = uploaded.get("thumbnail_path", "")
        media_paths = uploaded.get("media_paths", [])
        media_links = [link.strip() for link in uploaded.get("media_link", "").split(",") if link.strip()]

        for i, media_path in enumerate(media_paths):
            image_col, action_col = st.columns([1, 2])
            with image_col:
                if data["media_type"] == "photo":
                    st.image(media_path, width=150)
                elif preview_path and os.path.exists(preview_path):
                    st.image(preview_path, width=150)
                else:
                    st.caption("Preview unavailable")

            with action_col:
                file_name = os.path.basename(media_path)
                st.write(file_name)
                if i < len(media_links):
                    st.link_button(f"Open item {i + 1} in Drive", media_links[i], use_container_width=True)
                with open(media_path, "rb") as f:
                    st.download_button(
                        f"Save item {i + 1} to this device",
                        data=f.read(),
                        file_name=file_name,
                        mime=mimetypes.guess_type(file_name)[0] or "application/octet-stream",
                        key=f"download_media_item_{i}",
                        use_container_width=True,
                    )

    except Exception as e:
        st.error(f"Media download failed: {e}")

    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
