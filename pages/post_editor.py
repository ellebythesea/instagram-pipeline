"""Post Editor — review ingested posts and fill in metadata."""

import os
import re
import sys
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openai
import streamlit as st

from config import GOOGLE_SHEET_ID, OPENAI_API_KEY
from ingest_helpers import upload_media_bundle
from pipeline_caption import carousel_slide_rules, generate_row_caption, row_ready_for_caption
from post_scraper import process_url as process_post_url
from reel_scraper import process_url as process_reel_url
from sheets import (
    get_all_rows,
    update_caption,
    update_caption_context,
    update_ingest_result,
    update_metadata,
    update_transcript,
)
from utils.auth import require_auth
from utils.styles import inject as inject_styles

PRESET_HASHTAGS = {
    "Good Influence": "#usapolitics",
    "American Experiment Project": "#usa",
}

EDITABLE_STATUSES = {"ingested", "done"}
client = openai.OpenAI(api_key=OPENAI_API_KEY)


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


def _drive_view_url(drive_link: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", drive_link or "")
    if m:
        return f"https://drive.google.com/uc?export=view&id={m.group(1)}"
    parsed = urlparse(drive_link or "")
    file_id = parse_qs(parsed.query).get("id", [""])[0]
    if file_id:
        return f"https://drive.google.com/uc?export=view&id={file_id}"
    return ""


def _rerun_with_transcript(row: dict) -> None:
    url = row.get("Instagram URL", "").strip()
    if "/reel/" not in url.lower() and "/reels/" not in url.lower():
        raise ValueError("Transcript rerun is only available for reel URLs.")

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
        update_ingest_result(
            GOOGLE_SHEET_ID,
            row["row_number"],
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
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _extract_image_text(row: dict) -> str:
    media_type = (row.get("Media Type", "") or "").strip().lower()
    if media_type != "photo":
        raise ValueError("Image text extraction is only available for photo or carousel posts.")

    links = [link.strip() for link in (row.get("Media Drive Link", "") or "").split(",") if link.strip()]
    if not links:
        raise ValueError("This row does not have image media links in Drive yet.")

    url = (row.get("Instagram URL") or "").strip()
    image_indexes = list(range(len(links)))
    if url:
        try:
            latest = process_post_url(url)
            media_kinds = latest.get("media_kinds") or []
            filtered_indexes = [i for i, kind in enumerate(media_kinds[: len(links)]) if kind == "image"]
            if filtered_indexes:
                image_indexes = filtered_indexes
        except Exception:
            pass

    image_links = [links[i] for i in image_indexes if i < len(links)]
    if not image_links:
        raise ValueError("This row does not have any image slides available for OCR.")

    content = [{
        "type": "text",
        "text": "Extract all readable text from these images. Return plain text only, in reading order. No labels or commentary.",
    }]
    for link in image_links[:10]:
        view_url = _drive_view_url(link)
        if view_url:
            content.append({"type": "image_url", "image_url": {"url": view_url}})

    if len(content) == 1:
        raise ValueError("Could not build image URLs for OCR.")

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": content}],
        max_tokens=800,
        temperature=0,
    )
    text = response.choices[0].message.content.strip()
    if not text:
        raise ValueError("No text found in the images.")
    return text


def _redo_caption_from_image_text(row: dict) -> None:
    extracted_text = _extract_image_text(row)
    row_num = row["row_number"]
    update_caption_context(GOOGLE_SHEET_ID, row_num, extracted_text)
    update_transcript(GOOGLE_SHEET_ID, row_num, extracted_text)

    updated_row = dict(row)
    updated_row["Caption Context"] = extracted_text
    updated_row["Transcript"] = extracted_text
    caption = generate_row_caption(updated_row)
    update_caption(GOOGLE_SHEET_ID, row_num, caption, "done")


def _build_generic_slides_prompt(row: dict, hashtags: str, caption_context: str) -> str:
    """Build a source-agnostic slides prompt that strips the speaker and adds a research directive."""
    row_num = row.get("row_number", "new")
    username = (row.get("Source Username", "") or "").strip() or "unknown"
    media_type = (row.get("Media Type", "") or "post").strip()
    transcript = (row.get("Transcript", "") or "").strip()
    original_caption = (row.get("Original Caption", "") or "").strip()
    cc = (caption_context or "").strip()

    row_block = "\n".join([
        f"ROW {row_num}",
        f"username: {username}",
        f"media_type: {media_type}",
        "speaker_name: (none — do not attribute to any speaker)",
        "transcript:",
        transcript or "(none)",
        "original_caption:",
        original_caption or "(none)",
        "caption_context:",
        cc or "(none)",
    ])

    hashtag_note = (
        f"\nRequired hashtags to include in the caption: {hashtags}\n"
        if hashtags else ""
    )

    instructions = (
        "You are creating a standalone, source-agnostic informative carousel post.\n\n"
        "CRITICAL: This post must NOT mention, credit, quote, or attribute anything to the original speaker "
        "or the source of the content below. Do not name the speaker. Do not reference the clip, interview, "
        "speech, or original post in any way.\n\n"
        "Instead: identify the underlying topic or main person/subject the content is ABOUT, "
        "and write the post as if it is original research on that topic.\n\n"
        "Mandatory extended research step before writing:\n"
        "* Identify the core topic or main person of interest from the content below.\n"
        "* Search online extensively for additional facts, data, dates, numbers, context, and recent "
        "developments on this topic.\n"
        "* Pull in verified statistics, timelines, key figures, and relevant background that "
        "strengthens the post.\n"
        "* Prefer primary sources, Reuters, AP, government records, court documents, and reputable outlets.\n"
        "* Do not add unverified claims. If context cannot be verified, stay close to the supplied content.\n"
        "* Never cite sources in the JSON output. Use research only to improve accuracy and depth.\n\n"
        "Return ONLY valid JSON as an array. No markdown, no commentary outside JSON.\n\n"
        "Each object must include: row_number, name, text1, text2, text3, generated_caption\n\n"
        "Rules:\n"
        "* Keep row_number exactly as shown\n"
        "* No markdown, no commentary outside JSON\n"
        "* Plain straight double quotes only, no smart quotes\n"
        + carousel_slide_rules()
        + hashtag_note
        + "\nCaption rules:\n"
        "Write a neutral, third-person informative caption under 1300 characters using exactly two simple "
        "paragraphs.\n\n"
        "Never write in first person. Do not use I, me, my, mine, we, us, our, or ours unless inside "
        "a verified direct quote from a named public source. Stay in third person.\n\n"
        "The first paragraph must be 250 characters or fewer and serve as the most important summary. "
        "It must include all required hashtags plus 3 to 5 relevant hashtags total. "
        "Prioritize hashtags for the main subject or topic, then subject-area hashtags for discovery. "
        "Replace the normal word or phrase in the sentence with the hashtag version. "
        "Do not add a separate hashtag-only line at the end.\n\n"
        "The second paragraph adds context using verified facts, dates, and numbers. "
        "Do not refer to any transcript, clip, speech, interview, or video. "
        "Write as if describing the underlying topic directly.\n\n"
        "Do NOT include any call to action asking readers to comment or DM for a link. "
        "Do not include any line about 'Comment LINK', 'Say LINK', 'DM', or any link-retrieval "
        "instructions.\n\n"
        "\nQuality check before final output:\n"
        "* Confirm no reference to the original speaker appears anywhere in the output\n"
        "* Confirm no reference to a clip, transcript, speech, interview, or video\n"
        "* Confirm the post reads as original research on the topic, not a summary of someone's content\n"
        "* Confirm no call-to-action about commenting, DMing, or retrieving a link\n"
        "* Confirm every object has exactly row_number, name, text1, text2, text3, generated_caption\n"
        "* Confirm character limits are respected\n"
        "* Confirm no hashtags, em dashes, smart quotes, markdown, or newlines in slide fields\n\n"
    )
    return instructions + "\n\n" + row_block


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Post Editor", page_icon="✏️", layout="wide")
inject_styles("post_editor")
st.title("Post Editor")

if not require_auth():
    st.stop()

try:
    all_rows = get_all_rows(GOOGLE_SHEET_ID)
except Exception as e:
    st.error(f"Could not load sheet: {e}")
    st.stop()

rows = [
    r for r in all_rows
    if r.get("Instagram URL", "").strip() and r.get("Status", "").strip().lower() in EDITABLE_STATUSES
]

if not rows:
    st.info("No ingested posts yet. Run **Process New Rows** on the Pipeline Dashboard first.")
    st.stop()

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
            st.caption("Original Caption")
            st.code(orig, language=None)
        elif orig:
            st.caption("Original caption for this row looks misaligned. Re-ingest the row to refresh it.")

        generated = row.get("Generated Caption", "").strip()
        if generated:
            st.caption("Generated Caption")
            st.code(generated, language=None)

        transcript = row.get("Transcript", "")
        if transcript:
            st.caption("Transcript")
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

        caption_context = st.text_area(
            "Caption Context",
            value=row.get("Caption Context", ""),
            height=90,
            placeholder="Add any context you want the caption generator to use when the post lacks enough source text.",
            key=f"context_{row_num}",
        )

        top_comment = st.text_area(
            "Top Comment",
            value=row.get("Top Comment", ""),
            height=60,
            placeholder="Prepended above the generated caption.",
            key=f"top_{row_num}",
        )

    st.markdown("</div>", unsafe_allow_html=True)

    action_cols = st.columns(4)
    with action_cols[0]:
        rerun_disabled = "/reel/" not in url.lower() and "/reels/" not in url.lower()
        if st.button(
            "🎙️",
            key=f"post_editor_transcript_{row_num}",
            help="Re-run with transcript",
            disabled=rerun_disabled,
            use_container_width=True,
        ):
            with st.spinner(f"Refreshing row {row_num} with transcript..."):
                try:
                    _rerun_with_transcript(row)
                except Exception as e:
                    st.error(f"Row {row_num}: {e}")
                else:
                    st.success(f"Row {row_num}: transcript caption rerun complete.")
                    st.rerun()
    with action_cols[1]:
        if st.button(
            "⬇️",
            key=f"post_editor_download_{row_num}",
            help="Download media to Drive",
            use_container_width=True,
        ):
            with st.spinner(f"Uploading row {row_num} media to Drive..."):
                try:
                    _download_media_to_drive(row)
                except Exception as e:
                    st.error(f"Row {row_num}: {e}")
                else:
                    st.success(f"Row {row_num}: media uploaded to Drive.")
                    st.rerun()
    with action_cols[2]:
        image_redo_disabled = (media_type or "").strip().lower() != "photo"
        if st.button(
            "🖼️",
            key=f"post_editor_image_text_{row_num}",
            help="Re-do caption from image text",
            disabled=image_redo_disabled,
            use_container_width=True,
        ):
            with st.spinner(f"Extracting image text for row {row_num}..."):
                try:
                    _redo_caption_from_image_text(row)
                except Exception as e:
                    st.error(f"Row {row_num}: {e}")
                else:
                    st.success(f"Row {row_num}: caption regenerated from image text.")
                    st.rerun()
    with action_cols[3]:
        if st.button(
            "Make generic",
            key=f"post_editor_generic_{row_num}",
            help="Build a source-agnostic slides prompt — strips speaker, adds research directive",
            use_container_width=True,
        ):
            _hashtags = st.session_state.get(f"hashtags_{row_num}", row.get("Required Hashtags", ""))
            _preset_choices = st.session_state.get(f"presets_{row_num}", [])
            _preset_tags = " ".join(PRESET_HASHTAGS[p] for p in _preset_choices)
            _combined_hashtags = " ".join(filter(None, [_hashtags.strip(), _preset_tags])).strip()
            _caption_context = st.session_state.get(f"context_{row_num}", row.get("Caption Context", ""))
            st.session_state[f"generic_prompt_{row_num}"] = _build_generic_slides_prompt(
                row, _combined_hashtags, _caption_context
            )

    generic_prompt = st.session_state.get(f"generic_prompt_{row_num}")
    if generic_prompt:
        st.caption("Generic slides prompt — source-agnostic, research-forward. Copy and paste into a research-enabled model:")
        st.code(generic_prompt, language=None)

ingested_rows = [
    r for r in rows
    if r.get("Status", "").strip().lower() == "ingested" and row_ready_for_caption(r)
]

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
    ingested = ingested_rows

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
            caption_context = st.session_state.get(f"context_{row_num}", row.get("Caption Context", ""))
            top_comment = st.session_state.get(f"top_{row_num}", row.get("Top Comment", ""))
            preset_tags = " ".join(PRESET_HASHTAGS[p] for p in preset_choices)
            combined_hashtags = " ".join(filter(None, [custom_hashtags.strip(), preset_tags])).strip()
            row_for_caption = dict(row)
            row_for_caption["Caption Context"] = caption_context.strip()
            row_for_caption["Speaker Name"] = speaker.strip()
            row_for_caption["Required Hashtags"] = combined_hashtags
            row_for_caption["Top Comment"] = top_comment.strip()

            with st.status(f"Row {row_num}: {label}", expanded=False) as s:
                try:
                    update_metadata(
                        GOOGLE_SHEET_ID,
                        row_num,
                        row_for_caption["Caption Context"],
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
