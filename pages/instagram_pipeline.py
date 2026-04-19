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
from ingest_helpers import build_download_payload, download_media_bundle, upload_thumbnail_only
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


def _ingest_row(row: dict) -> dict:
    """Process one row through ingest and return sheet fields plus optional download payload."""
    url = row["Instagram URL"].strip()
    tmp_dir = None
    try:
        if "/reel/" in url.lower() or "/reels/" in url.lower():
            from reel_scraper import process_url as process_reel_url
            data = process_reel_url(url)
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
                "download": None,
            }

        from post_scraper import process_url as process_post_url
        data = process_post_url(url)

        uploaded = download_media_bundle(data)
        tmp_dir = uploaded["tmp_dir"]
        filename, payload, mime = build_download_payload(
            uploaded["media_paths"],
            f"{data['post_date']}_{data['post_id']}",
        )

        return {
            "username": data["username"],
            "media_type": data["media_type"],
            "photo_count": data["photo_count"],
            "media_link": uploaded["media_link"],
            "thumbnail_link": uploaded["thumbnail_link"],
            "original_caption": data["original_caption"],
            "transcript": data["transcript"],
            "status": "ingested",
            "download": {
                "filename": filename,
                "payload": payload,
                "mime": mime,
            },
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
            "download": None,
        }

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
        st.caption("Thumbnails stay in Drive for previews. Post media is prepared for device download instead of being uploaded to Drive.")
        progress = st.progress(0)
        download_items = []

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
                    if result["download"]:
                        download_items.append(
                            {
                                "row_number": row_num,
                                "filename": result["download"]["filename"],
                                "payload": result["download"]["payload"],
                                "mime": result["download"]["mime"],
                            }
                        )

            progress.progress((i + 1) / len(pending))

        st.success(f"Done. Ingested {len(pending)} row(s).")
        if download_items:
            st.subheader("Save post media")
            st.caption("Photo and carousel media now downloads to this device. Thumbnails remain in Drive for browser previews.")
            for item in download_items:
                st.download_button(
                    f"Save row {item['row_number']} media",
                    data=item["payload"],
                    file_name=item["filename"],
                    mime=item["mime"],
                    key=f"download_row_{item['row_number']}",
                    use_container_width=True,
                )
