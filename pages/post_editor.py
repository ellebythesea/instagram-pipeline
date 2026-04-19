"""Post Editor — review ingested posts and fill in metadata."""

import os
import re
import sys
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config import APP_PASSWORD, DEFAULT_POST_FOOTER, GOOGLE_SHEET_ID
from sheets import get_all_rows, update_metadata

PRESET_HASHTAGS = {
    "Good Influence": "#usapolitics",
    "American Experiment Project": "#usa",
}

EDITABLE_STATUSES = {"ingested", "done", "error"}


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


def _drive_image_url(drive_link: str) -> str:
    """Convert a Drive web view link to a direct image URL."""
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", drive_link)
    if m:
        return f"https://drive.google.com/thumbnail?id={m.group(1)}&sz=w1200"
    parsed = urlparse(drive_link)
    file_id = parse_qs(parsed.query).get("id", [""])[0]
    if file_id:
        return f"https://drive.google.com/thumbnail?id={file_id}&sz=w1200"
    return ""


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Post Editor", page_icon="✏️", layout="wide")
st.title("Post Editor")

if not _check_password():
    st.stop()

try:
    all_rows = get_all_rows(GOOGLE_SHEET_ID)
except Exception as e:
    st.error(f"Could not load sheet: {e}")
    st.stop()

rows = [
    r for r in all_rows
    if r.get("Status", "").strip().lower() in EDITABLE_STATUSES
]

if not rows:
    st.info("No ingested posts yet. Run **Process New Rows** on the Pipeline Dashboard first.")
    st.stop()

st.caption(f"Showing {len(rows)} processed post(s). Fill in metadata and save — changes write directly to the Google Sheet.")
st.caption(f"All generated captions automatically end with: {DEFAULT_POST_FOOTER}")

for row in rows:
    row_num = row["row_number"]
    username = row.get("Source Username", "") or "unknown"
    media_type = row.get("Media Type", "")
    status = row.get("Status", "")
    url = row.get("Instagram URL", "")

    header = f"Row {row_num} — @{username}  ·  {media_type}  ·  {status}"
    with st.expander(header, expanded=(status == "ingested")):
        left, right = st.columns([1, 2])

        # --- Left: media preview + read-only fields ---
        with left:
            thumb_link = row.get("Thumbnail Drive Link", "")
            if thumb_link:
                img_url = _drive_image_url(thumb_link)
                if img_url:
                    st.image(img_url, use_container_width=True)
                else:
                    st.link_button("View thumbnail", thumb_link)

            if url:
                st.markdown(f"[Open post on Instagram ↗]({url})")

            orig = row.get("Original Caption", "")
            if orig:
                st.text_area(
                    "Original Caption",
                    value=orig,
                    height=100,
                    disabled=True,
                    key=f"orig_{row_num}",
                )

            transcript = row.get("Transcript", "")
            if transcript:
                st.text_area(
                    "Transcript",
                    value=transcript,
                    height=120,
                    disabled=True,
                    key=f"trans_{row_num}",
                )

        # --- Right: editable metadata + generated caption ---
        with right:
            speaker = st.text_input(
                "Speaker Name",
                value=row.get("Speaker Name", ""),
                placeholder="e.g. Alexandria Ocasio-Cortez",
                key=f"speaker_{row_num}",
            )

            # Preset hashtag selector
            preset_choices = st.multiselect(
                "Add preset hashtags",
                options=list(PRESET_HASHTAGS.keys()),
                default=[],
                key=f"presets_{row_num}",
                help="Selecting a preset appends it to Required Hashtags on save.",
            )

            custom_hashtags = st.text_input(
                "Required Hashtags",
                value=row.get("Required Hashtags", ""),
                placeholder="#CustomTag #AnotherTag",
                key=f"hashtags_{row_num}",
            )

            top_comment = st.text_area(
                "Top Comment",
                value=row.get("Top Comment", ""),
                height=60,
                placeholder="Prepended above the generated caption.",
                key=f"top_{row_num}",
            )

            if st.button("Save", key=f"save_{row_num}", type="primary"):
                # Combine preset tags with custom input
                preset_tags = " ".join(PRESET_HASHTAGS[p] for p in preset_choices)
                combined = " ".join(filter(None, [custom_hashtags.strip(), preset_tags])).strip()
                try:
                    update_metadata(
                        GOOGLE_SHEET_ID,
                        row_num,
                        speaker.strip(),
                        combined,
                        top_comment.strip(),
                        "",
                    )
                    st.success("Saved to sheet.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Save failed: {e}")

        # --- Generated caption ---
        generated = row.get("Generated Caption", "").strip()
        if generated:
            st.divider()
            st.markdown("**Generated Caption** — copy with the button in the top-right corner:")
            st.code(generated, language=None)
