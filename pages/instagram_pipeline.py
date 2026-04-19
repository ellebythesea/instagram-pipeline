"""Instagram Pipeline Dashboard — ingest and caption generation."""

import os
import sys
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config import (
    APP_PASSWORD,
    GOOGLE_SHEET_ID,
)
from ingest_helpers import upload_media_bundle, upload_thumbnail_only
from sheets import (
    get_all_rows,
    get_pending_rows,
    update_ingest_result,
)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

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


def _ingest_row(row: dict) -> tuple:
    """Process one row through ingest. Returns tuple of result fields + status string."""
    url = row["Instagram URL"].strip()
    tmp_dir = None
    try:
        if "/reel/" in url.lower() or "/reels/" in url.lower():
            from reel_scraper import process_url as process_reel_url
            data = process_reel_url(url)
            uploaded = upload_thumbnail_only(data)
            tmp_dir = uploaded["tmp_dir"]

            return (
                data["username"],
                data["media_type"],
                data["photo_count"],
                uploaded["media_link"],
                uploaded["thumbnail_link"],
                data["original_caption"],
                data["transcript"],
                "ingested",
            )

        from post_scraper import process_url as process_post_url
        data = process_post_url(url)

        if data["photo_count"] <= 1:
            return ("", "", "", "", "", "", "", "skipped: not a carousel")

        uploaded = upload_media_bundle(data)
        tmp_dir = uploaded["tmp_dir"]

        return (
            data["username"],
            data["media_type"],
            data["photo_count"],
            uploaded["media_link"],
            uploaded["thumbnail_link"],
            data["original_caption"],
            data["transcript"],
            "ingested",
        )

    except Exception as e:
        return ("", "", "", "", "", "", "", f"error: {e}")

    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Instagram Pipeline", page_icon="📋", layout="wide")
st.title("Instagram Pipeline")

if not _check_password():
    st.stop()

# --- Status table ---
st.subheader("All Rows")
try:
    all_rows = get_all_rows(GOOGLE_SHEET_ID)
    if all_rows:
        import pandas as pd
        display_cols = ["Instagram URL", "Source Username", "Media Type", "Status", "Generated Caption"]
        df = pd.DataFrame([{c: r.get(c, "") for c in display_cols} for r in all_rows])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No rows in sheet yet. Add Instagram URLs to column A to get started.")
except Exception as e:
    st.warning(f"Could not load sheet: {e}")

st.divider()

ingest_btn = st.button("⬇️ Process New Rows", type="primary", use_container_width=True)

# --- Ingest ---
if ingest_btn:
    try:
        pending = get_pending_rows(GOOGLE_SHEET_ID)
    except Exception as e:
        st.error(f"Could not read sheet: {e}")
        st.stop()

    if not pending:
        st.info("No new rows to process (column A filled, column N empty).")
    else:
        st.write(f"Found **{len(pending)}** row(s) to ingest.")
        st.caption("Reels ingest transcript and metadata into the sheet without uploading the reel video to Drive.")
        progress = st.progress(0)

        for i, row in enumerate(pending):
            row_num = row["row_number"]
            url = row["Instagram URL"]
            label = url[:60] + "..." if len(url) > 60 else url

            with st.status(f"Row {row_num}: {label}", expanded=False) as s:
                username, media_type, photo_count, media_link, thumb_link, orig, transcript, status = _ingest_row(row)
                try:
                    update_ingest_result(
                        GOOGLE_SHEET_ID, row_num, username, media_type,
                        photo_count, media_link, thumb_link, orig, transcript, status,
                    )
                except Exception as e:
                    s.update(label=f"Row {row_num}: error writing to sheet — {e}", state="error")
                    progress.progress((i + 1) / len(pending))
                    continue

                if status.startswith("error"):
                    s.update(label=f"Row {row_num}: {status}", state="error")
                else:
                    s.update(
                        label=f"Row {row_num}: ingested — @{username} ({media_type})",
                        state="complete",
                    )

            progress.progress((i + 1) / len(pending))

        st.success(f"Done. Ingested {len(pending)} row(s).")
