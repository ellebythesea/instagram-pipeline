"""Post Editor — review ingested posts and fill in metadata."""

import os
import re
import sys
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config import APP_PASSWORD, DEFAULT_POST_FOOTER, GOOGLE_SHEET_ID
from pipeline_caption import generate_row_caption
from sheets import get_all_rows, get_ingested_rows, update_caption, update_metadata

PRESET_HASHTAGS = {
    "Good Influence": "#usapolitics",
    "American Experiment Project": "#usa",
}

EDITABLE_STATUSES = {"ingested"}


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


def _looks_like_drive_link(value: str) -> bool:
    s = (value or "").strip().lower()
    return s.startswith("https://drive.google.com/") or s.startswith("http://drive.google.com/")


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

st.markdown(
    """
    <style>
    .stApp [data-testid="stAppViewContainer"] {
        padding-bottom: 9rem;
    }
    div[data-testid="stVerticalBlock"]:has(> div.sticky-generate-anchor) {
        position: fixed;
        right: 1.25rem;
        bottom: 1.25rem;
        width: min(460px, calc(100vw - 2.5rem));
        z-index: 999;
        background: rgba(255, 255, 255, 0.96);
        border: 1px solid rgba(0, 0, 0, 0.08);
        border-radius: 18px;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.08);
        padding: 0.9rem 1rem;
        backdrop-filter: blur(10px);
    }
    .sticky-generate-anchor {
        display: none;
    }
    @media (max-width: 640px) {
        div[data-testid="stVerticalBlock"]:has(> div.sticky-generate-anchor) {
            right: 0.75rem;
            left: 0.75rem;
            width: auto;
            bottom: 0.75rem;
        }
    }
    .editor-row {
        border: 1px solid rgba(0, 0, 0, 0.08);
        border-radius: 18px;
        padding: 1rem 1rem 0.5rem;
        margin-bottom: 1rem;
        background: rgba(255, 255, 255, 0.85);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

for row in rows:
    row_num = row["row_number"]
    username = row.get("Source Username", "") or "unknown"
    media_type = row.get("Media Type", "")
    status = row.get("Status", "")
    url = row.get("Instagram URL", "")

    st.markdown('<div class="editor-row">', unsafe_allow_html=True)
    st.markdown(f"**Row {row_num}** — @{username}  ·  {media_type}  ·  {status}")
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
        if orig and not _looks_like_drive_link(orig):
            st.text_area(
                "Original Caption",
                value=orig,
                height=100,
                disabled=True,
                key=f"orig_{row_num}",
            )
        elif orig:
            st.caption("Original caption for this row looks misaligned. Re-ingest the row to refresh it.")

        transcript = row.get("Transcript", "")
        if transcript:
            st.markdown("**Transcript** — copy with the button in the top-right corner:")
            st.code(transcript, language=None)

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

        generated = row.get("Generated Caption", "").strip()
        if generated:
            st.text_area(
                "Generated Caption Preview",
                value=generated,
                height=90,
                disabled=True,
                key=f"generated_preview_{row_num}",
            )
    st.markdown("</div>", unsafe_allow_html=True)

ingested_rows = [r for r in rows if r.get("Status", "").strip().lower() == "ingested"]

sticky_container = st.container()
with sticky_container:
    st.markdown('<div class="sticky-generate-anchor"></div>', unsafe_allow_html=True)
    info_col, button_col = st.columns([3, 1])
    with info_col:
        if ingested_rows:
            st.caption(f"{len(ingested_rows)} post(s) are ready for caption generation.")
        else:
            st.caption("No ingested posts are ready for caption generation.")
    with button_col:
        generate_btn = st.button(
            "Generate Captions",
            type="primary",
            use_container_width=True,
            disabled=not ingested_rows,
        )

if generate_btn:
    try:
        ingested = get_ingested_rows(GOOGLE_SHEET_ID)
    except Exception as e:
        st.error(f"Could not read sheet: {e}")
        st.stop()

    if not ingested:
        st.info("No ingested rows found.")
    else:
        st.write(f"Found **{len(ingested)}** row(s) to generate captions for.")
        progress = st.progress(0)

        for i, row in enumerate(ingested):
            row_num = row["row_number"]
            url = row["Instagram URL"]
            label = url[:60] + "..." if len(url) > 60 else url
            speaker = st.session_state.get(f"speaker_{row_num}", row.get("Speaker Name", ""))
            preset_choices = st.session_state.get(f"presets_{row_num}", [])
            custom_hashtags = st.session_state.get(f"hashtags_{row_num}", row.get("Required Hashtags", ""))
            top_comment = st.session_state.get(f"top_{row_num}", row.get("Top Comment", ""))
            preset_tags = " ".join(PRESET_HASHTAGS[p] for p in preset_choices)
            combined_hashtags = " ".join(filter(None, [custom_hashtags.strip(), preset_tags])).strip()
            row_for_caption = dict(row)
            row_for_caption["Speaker Name"] = speaker.strip()
            row_for_caption["Required Hashtags"] = combined_hashtags
            row_for_caption["Top Comment"] = top_comment.strip()

            with st.status(f"Row {row_num}: {label}", expanded=False) as s:
                try:
                    update_metadata(
                        GOOGLE_SHEET_ID,
                        row_num,
                        row_for_caption["Speaker Name"],
                        row_for_caption["Required Hashtags"],
                        row_for_caption["Top Comment"],
                        "",
                    )
                    caption = generate_row_caption(row_for_caption)
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
        st.rerun()
