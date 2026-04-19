"""Manual reel downloader page."""

import os
import sys
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config import APP_PASSWORD, GOOGLE_SHEET_ID
from ingest_helpers import upload_media_bundle
from reel_scraper import process_url
from sheets import get_all_rows, update_ingest_result


def _check_password() -> bool:
    if not APP_PASSWORD:
        return True
    if st.session_state.get("authenticated"):
        return True
    pwd = st.text_input("Password", type="password")
    if pwd:
        if pwd == APP_PASSWORD:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False


def _normalize_url(url: str) -> str:
    return url.strip().rstrip("/")


st.set_page_config(page_title="Reel Downloader", page_icon="🎞️", layout="centered")
st.title("Reel Downloader")
st.caption("Paste a reel URL to upload just that reel and its thumbnail to Drive.")

if not _check_password():
    st.stop()

with st.form("reel_download_form"):
    reel_url = st.text_input("Instagram Reel URL", placeholder="https://www.instagram.com/reel/...")
    submitted = st.form_submit_button("Download Reel", type="primary", use_container_width=True)

if submitted:
    if not reel_url.strip():
        st.warning("Please enter a reel URL.")
        st.stop()

    if "/reel/" not in reel_url.lower() and "/reels/" not in reel_url.lower():
        st.warning("This page only accepts reel URLs.")
        st.stop()

    tmp_dir = None
    try:
        with st.status("Fetching reel metadata...", expanded=True) as status:
            data = process_url(reel_url.strip())
            status.update(label="Reel metadata fetched", state="complete")

        with st.status("Uploading reel to Drive...", expanded=True) as status:
            uploaded = upload_media_bundle(data)
            tmp_dir = uploaded["tmp_dir"]
            status.update(label="Reel uploaded", state="complete")

        matching_row = next(
            (
                row for row in get_all_rows(GOOGLE_SHEET_ID)
                if _normalize_url(row.get("Instagram URL", "")) == _normalize_url(reel_url)
            ),
            None,
        )
        if matching_row:
            with st.status("Writing reel data to Google Sheet...", expanded=True) as status:
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

        st.success("Reel uploaded to Drive.")
        st.write(f"Username: @{data['username']}")
        st.write(f"Media link: {uploaded['media_link']}")
        if matching_row:
            st.write(f"Sheet row updated: {matching_row['row_number']}")
        else:
            st.info("No matching row found in the sheet for this URL, so nothing was written back to Google Sheets.")
        if uploaded["thumbnail_link"]:
            st.write(f"Thumbnail link: {uploaded['thumbnail_link']}")
        if data.get("original_caption"):
            with st.expander("Original caption"):
                st.write(data["original_caption"])
        if data.get("transcript"):
            with st.expander("Transcript"):
                st.write(data["transcript"])

    except Exception as e:
        st.error(f"Reel download failed: {e}")

    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)
