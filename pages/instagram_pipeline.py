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
from pipeline_caption import generate_row_caption
from reel_scraper import process_url as process_reel_url
from sheets import (
    get_all_rows,
    get_pending_rows,
    update_caption,
    update_ingest_result,
    update_transcript,
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


def _ingest_row(row: dict) -> dict:
    """Process one row through ingest and return sheet fields."""
    url = row["Instagram URL"].strip()
    tmp_dir = None
    try:
        if "/reel/" in url.lower() or "/reels/" in url.lower():
            data = process_reel_url(url, include_transcript=False)
            uploaded = upload_thumbnail_only(data)
            tmp_dir = uploaded["tmp_dir"]
            return {
                "username": data["username"],
                "media_type": data["media_type"],
                "photo_count": data["photo_count"],
                "media_link": uploaded["media_link"],
                "thumbnail_link": uploaded["thumbnail_link"],
                "original_caption": data["original_caption"],
                "transcript": data["transcript"],
                "status": "ingested",
            }

        from post_scraper import process_url as process_post_url
        data = process_post_url(url)

        uploaded = upload_media_bundle(data)
        tmp_dir = uploaded["tmp_dir"]

        return {
            "username": data["username"],
            "media_type": data["media_type"],
            "photo_count": data["photo_count"],
            "media_link": uploaded["media_link"],
            "thumbnail_link": uploaded["thumbnail_link"],
            "original_caption": data["original_caption"],
            "transcript": data["transcript"],
            "status": "ingested",
        }

    except Exception as e:
        return {
            "username": "",
            "media_type": "",
            "photo_count": "",
            "media_link": "",
            "thumbnail_link": "",
            "original_caption": "",
            "transcript": "",
            "status": f"error: {e}",
        }

    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _rerun_with_transcript(row: dict) -> None:
    """Fetch a reel transcript on demand, rewrite the sheet row, and regenerate the caption."""
    url = row.get("Instagram URL", "").strip()
    if "/reel/" not in url.lower() and "/reels/" not in url.lower():
        raise ValueError("Re-run with transcript is only available for reel URLs.")

    refreshed = process_reel_url(url, include_transcript=True)
    transcript = (refreshed.get("transcript") or "").strip()
    if not transcript:
        raise ValueError("Apify did not return a transcript for this reel.")

    row_num = row["row_number"]
    update_transcript(GOOGLE_SHEET_ID, row_num, transcript)

    updated_row = dict(row)
    updated_row["Transcript"] = transcript
    updated_row["Source Username"] = refreshed.get("username") or updated_row.get("Source Username", "")
    updated_row["Original Caption"] = refreshed.get("original_caption") or updated_row.get("Original Caption", "")
    updated_row["Media Type"] = refreshed.get("media_type") or updated_row.get("Media Type", "")

    caption = generate_row_caption(updated_row)
    update_caption(GOOGLE_SHEET_ID, row_num, caption, "done")


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Instagram Pipeline", page_icon="📋", layout="wide")
st.title("Instagram Pipeline")

if not _check_password():
    st.stop()

success_message = st.session_state.pop("pipeline_success", "")
error_message = st.session_state.pop("pipeline_error", "")
if success_message:
    st.success(success_message)
if error_message:
    st.error(error_message)

# --- Status table ---
st.subheader("All Rows")
try:
    all_rows = get_all_rows(GOOGLE_SHEET_ID)
    if all_rows:
        header = st.columns([3.2, 1.3, 1.1, 1.1, 2.8, 1.5])
        labels = ["Instagram URL", "Source Username", "Media Type", "Status", "Generated Caption", "Actions"]
        for col, label in zip(header, labels):
            col.markdown(f"**{label}**")

        for row in all_rows:
            cols = st.columns([3.2, 1.3, 1.1, 1.1, 2.8, 1.5])
            cols[0].write(row.get("Instagram URL", ""))
            cols[1].write(row.get("Source Username", ""))
            cols[2].write(row.get("Media Type", ""))
            cols[3].write(row.get("Status", ""))
            generated = (row.get("Generated Caption", "") or "").strip()
            cols[4].write(generated[:120] + ("..." if len(generated) > 120 else ""))

            status = (row.get("Status", "") or "").strip().lower()
            with cols[5]:
                if status == "done":
                    if st.button("Re-run with Transcript", key=f"rerun_transcript_{row['row_number']}"):
                        with st.spinner(f"Refreshing row {row['row_number']} with transcript..."):
                            try:
                                _rerun_with_transcript(row)
                            except Exception as e:
                                st.session_state["pipeline_error"] = f"Row {row['row_number']}: {e}"
                            else:
                                st.session_state["pipeline_success"] = (
                                    f"Row {row['row_number']} refreshed with transcript and caption regenerated."
                                )
                            st.rerun()
                else:
                    st.write("")
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
        st.caption("Photos and carousels upload their media to Drive. Reels still keep thumbnail-only ingest in this batch flow.")
        progress = st.progress(0)

        for i, row in enumerate(pending):
            row_num = row["row_number"]
            url = row["Instagram URL"]
            label = url[:60] + "..." if len(url) > 60 else url

            with st.status(f"Row {row_num}: {label}", expanded=False) as s:
                result = _ingest_row(row)
                try:
                    update_ingest_result(
                        GOOGLE_SHEET_ID,
                        row_num,
                        result["username"],
                        result["media_type"],
                        result["photo_count"],
                        result["media_link"],
                        result["thumbnail_link"],
                        result["original_caption"],
                        result["transcript"],
                        result["status"],
                    )
                except Exception as e:
                    s.update(label=f"Row {row_num}: error writing to sheet — {e}", state="error")
                    progress.progress((i + 1) / len(pending))
                    continue

                if result["status"].startswith("error"):
                    s.update(label=f"Row {row_num}: {result['status']}", state="error")
                else:
                    s.update(
                        label=f"Row {row_num}: ingested — @{result['username']} ({result['media_type']})",
                        state="complete",
                    )

            progress.progress((i + 1) / len(pending))

        st.success(f"Done. Ingested {len(pending)} row(s).")
