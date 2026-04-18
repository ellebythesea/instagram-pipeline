"""Instagram Pipeline — batch-process rows from Google Sheets."""

import os
import sys
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic
import streamlit as st

from caption import _format_caption, _sanitize, transcribe_video
from config import (
    ANTHROPIC_API_KEY,
    APP_PASSWORD,
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_SHEET_ID,
)
from apify_downloader import download_instagram_post
from drive import upload_to_drive
from sheets import get_pending_rows, update_row

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
# Caption generation via Claude
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


def _generate_caption(
    transcript: str,
    speaker_name: str = "",
    required_hashtags: str = "",
) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    user_parts = [f"TRANSCRIPT:\n{transcript}"]
    if speaker_name:
        user_parts.append(
            f"The speaker in this transcript is: {speaker_name}. Reference them by name."
        )
    if required_hashtags:
        user_parts.append(
            f"These hashtags MUST be included as part of the 3-5 total: {required_hashtags}"
        )

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        system=_SYS_PROMPT,
        messages=[{"role": "user", "content": "\n\n".join(user_parts)}],
    )
    return _format_caption(_sanitize(message.content[0].text.strip()))


# ---------------------------------------------------------------------------
# Row processing
# ---------------------------------------------------------------------------

def _process_row(row: dict) -> tuple[str, str, str, str, str]:
    """Process one sheet row. Returns (username, drive_link, transcript, caption, status)."""
    tmp_dir = None
    try:
        post = download_instagram_post(row["url"])
        if post is None:
            return "", "", "", "", "error: download failed"

        tmp_dir = os.path.dirname(post.video_path)
        username = post.username

        try:
            drive_link = upload_to_drive(
                post.video_path,
                os.path.basename(post.video_path),
                GOOGLE_DRIVE_FOLDER_ID,
            )
        except Exception as e:
            return username, "", "", "", f"error: drive upload — {e}"

        transcript = ""
        if post.media_type == "video":
            try:
                transcript = transcribe_video(post.video_path) or ""
            except Exception as e:
                return username, drive_link, "", "", f"error: transcription — {e}"

        try:
            caption = _generate_caption(
                transcript=transcript or "(no transcript available)",
                speaker_name=row["speaker_name"],
                required_hashtags=row["required_hashtags"],
            )
        except Exception as e:
            return username, drive_link, transcript, "", f"error: caption — {e}"

        if row["top_comment"]:
            caption = f"{row['top_comment']}\n\n{caption}"

        return username, drive_link, transcript, caption, "done"

    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Instagram Pipeline", page_icon="🔄", layout="centered")
st.title("Instagram Pipeline")

if not _check_password():
    st.stop()

st.caption("Reads Instagram URLs from the Google Sheet, generates captions, and writes results back.")

if st.button("Process New Rows", type="primary", use_container_width=True):
    with st.spinner("Reading sheet..."):
        try:
            rows = get_pending_rows(GOOGLE_SHEET_ID)
        except Exception as e:
            st.error(f"Could not read sheet: {e}")
            st.stop()

    if not rows:
        st.info("No new rows to process.")
    else:
        st.write(f"Found **{len(rows)}** row(s) to process.")
        progress = st.progress(0)

        for i, row in enumerate(rows):
            row_num = row["row_number"]
            url_display = row["url"] if len(row["url"]) <= 55 else row["url"][:52] + "..."

            with st.status(f"Row {row_num}: {url_display}", expanded=False) as row_status:
                username, drive_link, transcript, caption, status = _process_row(row)

                try:
                    update_row(
                        GOOGLE_SHEET_ID,
                        row_num,
                        username,
                        drive_link,
                        transcript,
                        caption,
                        status,
                    )
                except Exception as e:
                    row_status.update(
                        label=f"Row {row_num}: error: sheet write failed — {e}",
                        state="error",
                    )
                    progress.progress((i + 1) / len(rows))
                    continue

                if status.startswith("error"):
                    row_status.update(label=f"Row {row_num}: {status}", state="error")
                else:
                    row_status.update(
                        label=f"Row {row_num}: done — @{username}", state="complete"
                    )

            progress.progress((i + 1) / len(rows))

        st.success(f"Finished processing {len(rows)} row(s).")
