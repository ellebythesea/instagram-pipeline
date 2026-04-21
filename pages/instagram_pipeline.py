"""Instagram Pipeline Dashboard — ingest and caption generation."""

import os
import sys
import shutil
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config import (
    APP_PASSWORD,
    GOOGLE_SHEET_ID,
)
from ingest_helpers import upload_media_bundle, upload_thumbnail_only
from pipeline_caption import generate_row_caption
from post_scraper import process_url as process_post_url
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


def _download_media_to_drive(row: dict) -> None:
    """Upload a row's media to Drive from the pipeline table."""
    url = row.get("Instagram URL", "").strip()
    if not url:
        raise ValueError("This row does not have an Instagram URL.")

    tmp_dir = None
    try:
        if "/reel/" in url.lower() or "/reels/" in url.lower():
            data = process_reel_url(url, include_transcript=False)
        else:
            data = process_post_url(url)

        uploaded = upload_media_bundle(data)
        tmp_dir = uploaded["tmp_dir"]
        row_num = row["row_number"]
        update_ingest_result(
            GOOGLE_SHEET_ID,
            row_num,
            data["username"],
            data["media_type"],
            data["photo_count"],
            uploaded["media_link"],
            uploaded["thumbnail_link"],
            data["original_caption"] or row.get("Original Caption", ""),
            row.get("Transcript", ""),
            row.get("Status", "") or "ingested",
        )
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _is_sheets_read_quota_error(error: Exception) -> bool:
    message = str(error)
    return (
        "Quota exceeded for quota metric 'Read requests'" in message
        or "Read requests per minute per user" in message
        or "Exceeded in a metric read request" in message
    )


def _run_with_sheet_quota_countdown(fn, waiting_label: str):
    while True:
        try:
            return fn()
        except Exception as e:
            if not _is_sheets_read_quota_error(e):
                raise
            countdown = st.empty()
            for remaining in range(60, 0, -1):
                countdown.warning(f"{waiting_label} Sheets read quota hit. Retrying in {remaining}s.")
                time.sleep(1)
            countdown.empty()


def _queue_pipeline_action(row_number: int, action: str) -> None:
    queue = st.session_state.setdefault("pipeline_action_queue", [])
    queue.append({"row_number": row_number, "action": action})


def _mark_action_complete(row_number: int, action: str) -> None:
    completed = st.session_state.setdefault("pipeline_action_completed", {})
    completed[f"{row_number}:{action}"] = True


def _is_action_complete(row_number: int, action: str) -> bool:
    completed = st.session_state.setdefault("pipeline_action_completed", {})
    return bool(completed.get(f"{row_number}:{action}"))


def _process_next_queued_action() -> None:
    queue = st.session_state.setdefault("pipeline_action_queue", [])
    if not queue:
        return

    current = queue.pop(0)
    row_number = current["row_number"]
    action = current["action"]

    rows = _run_with_sheet_quota_countdown(
        lambda: get_all_rows(GOOGLE_SHEET_ID),
        "Queued action paused:",
    )
    row = next((r for r in rows if r.get("row_number") == row_number), None)
    if not row:
        st.session_state["pipeline_error"] = f"Row {row_number}: row not found in sheet."
        if queue:
            st.rerun()
        return

    try:
        if action == "transcript":
            with st.spinner(f"Refreshing row {row_number} with transcript..."):
                _rerun_with_transcript(row)
            st.session_state["pipeline_success"] = (
                f"Row {row_number} refreshed with transcript and caption regenerated."
            )
        elif action == "download":
            with st.spinner(f"Uploading row {row_number} media to Drive..."):
                _download_media_to_drive(row)
            st.session_state["pipeline_success"] = f"Row {row_number} media uploaded to Drive."
        else:
            raise ValueError(f"Unknown action: {action}")
        _mark_action_complete(row_number, action)
    except Exception as e:
        st.session_state["pipeline_error"] = f"Row {row_number}: {e}"

    if queue:
        st.rerun()


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Instagram Pipeline", page_icon="📋", layout="wide")
st.title("Instagram Pipeline")

if not _check_password():
    st.stop()

_process_next_queued_action()

success_message = st.session_state.pop("pipeline_success", "")
error_message = st.session_state.pop("pipeline_error", "")
if success_message:
    st.success(success_message)
if error_message:
    st.error(error_message)

# --- Status table ---
st.subheader("All Rows")
try:
    all_rows = _run_with_sheet_quota_countdown(
        lambda: get_all_rows(GOOGLE_SHEET_ID),
        "Loading rows paused:",
    )
    if all_rows:
        import pandas as pd
        df = pd.DataFrame(
            [
                {
                    "Row": r.get("row_number", ""),
                    "Instagram URL": r.get("Instagram URL", ""),
                    "Source Username": r.get("Source Username", ""),
                    "Media Type": r.get("Media Type", ""),
                    "Status": r.get("Status", ""),
                    "Generated Caption": r.get("Generated Caption", ""),
                }
                for r in all_rows
            ]
        )
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Instagram URL": st.column_config.LinkColumn("Instagram URL"),
                "Generated Caption": st.column_config.TextColumn("Generated Caption", width="large"),
            },
        )
    else:
        st.info("No rows in sheet yet. Add Instagram URLs to column A to get started.")
except Exception as e:
    st.warning(f"Could not load sheet: {e}")

st.divider()

ingest_btn = st.button("⬇️ Process New Rows", type="primary", use_container_width=True)

# --- Ingest ---
if ingest_btn:
    try:
        pending = _run_with_sheet_quota_countdown(
            lambda: get_pending_rows(GOOGLE_SHEET_ID),
            "Processing new rows paused:",
        )
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
