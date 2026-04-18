"""Instagram Pipeline Dashboard — ingest and caption generation."""

import os
import sys
import shutil
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openai
import requests
import streamlit as st

from config import (
    OPENAI_API_KEY,
    APP_PASSWORD,
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_SHEET_ID,
)
from drive import upload_to_drive
from sheets import (
    get_all_rows,
    get_pending_rows,
    get_ingested_rows,
    update_ingest_result,
    update_caption,
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


# ---------------------------------------------------------------------------
# Caption generation (Claude)
# ---------------------------------------------------------------------------

_SYS_PROMPT = (
    "You are a sharp political analyst. Rewrite the transcript into a short, clear social post "
    "under 1300 characters using exactly two simple paragraphs.\n\n"
    "The first paragraph must be 250 characters or fewer and serve as the most important summary. "
    "It must include all hashtags. Use 3 to 5 relevant hashtags total. Prioritize the main people "
    "the post is about, then include one single word subject hashtag that helps with trending news "
    "discovery, followed by any remaining relevant tags. Replace the normal word or phrase in the "
    "sentence with the hashtag version, for example use #DonaldTrump in the sentence instead of "
    "writing the name normally. Do not add a separate hashtag only line at the end.\n\n"
    "The second paragraph should add context using verified facts, dates, and numbers when relevant. "
    "Include direct quotes from the transcript when available. Verify names and quotes carefully. "
    "Any hashtag used in the caption body counts toward the total of 3 to 5 hashtags. Avoid "
    "speculation, flourish, links, or references to Trump's current office status."
)


def _generate_caption(row: dict) -> str:
    content = row.get("Transcript", "").strip() or row.get("Original Caption", "").strip()
    if not content:
        raise ValueError("No transcript or original caption available")

    user_parts = [f"TRANSCRIPT:\n{content}"]
    if row.get("Speaker Name", "").strip():
        user_parts.append(
            f"The speaker in this transcript is: {row['Speaker Name'].strip()}. Reference them by name."
        )
    if row.get("Required Hashtags", "").strip():
        user_parts.append(
            f"These hashtags MUST be included as part of the 3-5 total: {row['Required Hashtags'].strip()}"
        )

    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": _SYS_PROMPT},
            {"role": "user", "content": "\n\n".join(user_parts)},
        ],
        max_tokens=600,
        temperature=0.35,
    )
    caption = response.choices[0].message.content.strip()

    if row.get("Top Comment", "").strip():
        caption = f"{row['Top Comment'].strip()}\n\n{caption}"
    if row.get("Footer", "").strip():
        caption = f"{caption}\n\n{row['Footer'].strip()}"

    return caption


# ---------------------------------------------------------------------------
# Ingest helpers
# ---------------------------------------------------------------------------

def _download_file(url: str, dest: str) -> None:
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def _make_filename(post_id: str, post_date: str, ext: str, index: int = 0) -> str:
    suffix = f"_{index}" if index > 0 else ""
    return f"{post_date}_{post_id}{suffix}{ext}"


def _ingest_row(row: dict) -> tuple:
    """Process one row through ingest. Returns tuple of result fields + status string."""
    url = row["Instagram URL"].strip()
    tmp_dir = tempfile.mkdtemp(prefix="ig_")
    try:
        # Route to correct scraper
        if "/reel" in url.lower():
            from reel_scraper import process_url
        else:
            from post_scraper import process_url

        data = process_url(url)

        ext = ".mp4" if data["media_type"] == "reel" else ".jpg"
        post_id = data["post_id"]
        post_date = data["post_date"]

        # Download and upload all media files
        media_links = []
        for i, media_url in enumerate(data["media_urls"]):
            filename = _make_filename(post_id, post_date, ext, index=i)
            local_path = os.path.join(tmp_dir, filename)
            _download_file(media_url, local_path)
            link = upload_to_drive(local_path, filename, GOOGLE_DRIVE_FOLDER_ID)
            media_links.append(link)

        media_link = ", ".join(media_links)

        # Download and upload thumbnail
        thumbnail_link = ""
        if data["thumbnail_url"]:
            thumb_filename = _make_filename(post_id, post_date, ".jpg") + "_thumb.jpg"
            # Avoid double extension
            thumb_filename = f"{post_date}_{post_id}_thumb.jpg"
            thumb_path = os.path.join(tmp_dir, thumb_filename)
            try:
                _download_file(data["thumbnail_url"], thumb_path)
                thumbnail_link = upload_to_drive(thumb_path, thumb_filename, GOOGLE_DRIVE_FOLDER_ID)
            except Exception:
                thumbnail_link = media_links[0] if media_links else ""

        return (
            data["username"],
            data["media_type"],
            data["photo_count"],
            media_link,
            thumbnail_link,
            data["original_caption"],
            data["transcript"],
            "ingested",
        )

    except Exception as e:
        return ("", "", "", "", "", "", "", f"error: {e}")

    finally:
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

col1, col2 = st.columns(2)
with col1:
    ingest_btn = st.button("⬇️ Process New Rows", type="primary", use_container_width=True)
with col2:
    caption_btn = st.button("✍️ Generate Captions", type="secondary", use_container_width=True)

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

# --- Generate Captions ---
if caption_btn:
    try:
        ingested = get_ingested_rows(GOOGLE_SHEET_ID)
    except Exception as e:
        st.error(f"Could not read sheet: {e}")
        st.stop()

    if not ingested:
        st.info("No ingested rows found. Run 'Process New Rows' first.")
    else:
        st.write(f"Found **{len(ingested)}** row(s) to generate captions for.")
        progress = st.progress(0)

        for i, row in enumerate(ingested):
            row_num = row["row_number"]
            url = row["Instagram URL"]
            label = url[:60] + "..." if len(url) > 60 else url

            with st.status(f"Row {row_num}: {label}", expanded=False) as s:
                try:
                    caption = _generate_caption(row)
                    status = "done"
                except Exception as e:
                    caption = ""
                    status = f"error: caption — {e}"

                try:
                    update_caption(GOOGLE_SHEET_ID, row_num, caption, status)
                except Exception as e:
                    s.update(label=f"Row {row_num}: error writing to sheet — {e}", state="error")
                    progress.progress((i + 1) / len(ingested))
                    continue

                if status.startswith("error"):
                    s.update(label=f"Row {row_num}: {status}", state="error")
                else:
                    s.update(label=f"Row {row_num}: caption generated", state="complete")

            progress.progress((i + 1) / len(ingested))

        st.success(f"Done. Generated captions for {len(ingested)} row(s).")
