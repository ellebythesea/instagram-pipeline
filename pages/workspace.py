"""Unified workspace shell for the next UI redesign."""

import os
import re
import shutil
import sys
import time
from urllib.parse import parse_qs, urlparse
import html

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openai
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from config import APP_PASSWORD, GOOGLE_SHEET_ID, OPENAI_API_KEY
from ingest_helpers import upload_media_bundle, upload_thumbnail_only
from pipeline_caption import generate_row_caption
from post_scraper import process_url as process_post_url
from reel_scraper import process_url as process_reel_url
import sheets as sheet_ops

MODE_OPTIONS = [
    "Add to sheet",
    "Generate headline",
    "Caption this",
    "Download media",
]

ORG_HASHTAG_OPTIONS = [
    "",
    "Good Influence",
    "American Experiment Project",
]

ORG_HASHTAG_MAP = {
    "Good Influence": "#usapolitics",
    "American Experiment Project": "#usa",
}

EDITABLE_STATUSES = {"ingested", "done"}
client = openai.OpenAI(api_key=OPENAI_API_KEY)

get_all_rows = sheet_ops.get_all_rows
get_pending_rows = sheet_ops.get_pending_rows
update_caption = sheet_ops.update_caption
update_caption_context = sheet_ops.update_caption_context
update_ingest_result = sheet_ops.update_ingest_result
update_metadata = sheet_ops.update_metadata
update_transcript = sheet_ops.update_transcript


def append_link_rows(sheet_id: str, urls: list[str], required_hashtags: str = "") -> None:
    if hasattr(sheet_ops, "append_link_rows"):
        sheet_ops.append_link_rows(sheet_id, urls, required_hashtags)
        return

    cleaned_urls = [url.strip() for url in urls if url.strip()]
    if not cleaned_urls:
        return

    ws = sheet_ops._worksheet(sheet_id)
    rows = []
    for url in cleaned_urls:
        row = [""] * len(sheet_ops._EXPECTED_HEADERS)
        row[0] = url
        row[11] = required_hashtags.strip()
        rows.append(row)
    sheet_ops._with_backoff(ws.append_rows, rows, value_input_option="USER_ENTERED")
    sheet_ops._invalidate_rows_cache(sheet_id)


def update_status(sheet_id: str, row_number: int, status: str) -> None:
    if hasattr(sheet_ops, "update_status"):
        sheet_ops.update_status(sheet_id, row_number, status)
        return

    ws = sheet_ops._worksheet(sheet_id)
    sheet_ops._with_backoff(ws.update, f"N{row_number}", [[status]])
    sheet_ops._invalidate_rows_cache(sheet_id)


def _is_reel_url(url: str) -> bool:
    lowered = (url or "").lower()
    return "/reel/" in lowered or "/reels/" in lowered


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


def _ensure_home_links() -> list[str]:
    links = st.session_state.setdefault("workspace_home_links", [""])
    if not links:
        links.append("")
    return links


def _normalize_home_links(links: list[str]) -> list[str]:
    filled = [link for link in links if (link or "").strip()]
    return filled + [""]


def _action_label(mode: str) -> str:
    return {
        "Add to sheet": "Add",
        "Generate headline": "Generate",
        "Caption this": "Caption",
        "Download media": "Download",
    }.get(mode, "Add")


def _mode_uses_org_hashtag(mode: str) -> bool:
    return mode in {"Add to sheet", "Caption this"}


def _clean_home_links() -> list[str]:
    return [link.strip() for link in st.session_state.get("workspace_home_links", []) if link.strip()]


def _fetch_post_data(url: str) -> dict:
    if _is_reel_url(url):
        return process_reel_url(url, include_transcript=False)
    return process_post_url(url)


def _generate_headlines(source_text: str) -> list[str]:
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": (
                    "You write short, salacious, attention-grabbing political headlines. "
                    "Return exactly 3 distinct headline options. Keep each under 12 words. "
                    "Do not use hashtags. Do not use quotation marks unless essential. "
                    "Do not add labels or extra explanation. Put each headline on its own line."
                ),
            },
            {
                "role": "user",
                "content": f"Write a headline from this Instagram caption:\n\n{source_text}",
            },
        ],
        max_tokens=60,
        temperature=0.9,
    )
    raw_lines = response.choices[0].message.content.strip().splitlines()
    headlines = []
    for line in raw_lines:
        cleaned = line.strip().lstrip("-*0123456789. ").replace("#", "")
        if cleaned:
            headlines.append(cleaned)
        if len(headlines) == 3:
            break
    return headlines


def _generate_caption_from_caption(source_text: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": (
                    "You write sharp political Instagram captions from an existing Instagram caption only. "
                    "Return exactly two short paragraphs, no hashtags, no labels, and no quotation marks unless essential. "
                    "Do not mention transcription or missing audio. Keep it concise, punchy, and readable."
                ),
            },
            {
                "role": "user",
                "content": f"Write a caption from this Instagram caption:\n\n{source_text}",
            },
        ],
        max_tokens=220,
        temperature=0.5,
    )
    return response.choices[0].message.content.strip()


def _build_footered_caption(caption_body: str, username: str) -> str:
    footer_parts = []
    cleaned_username = (username or "").strip().lstrip("@")
    if cleaned_username and cleaned_username.lower() != "unknown":
        footer_parts.append(f"Follow @{cleaned_username} for more.")
    footer_parts.append(
        "Help this information get to more voters. 🇺🇸 "
        "A well-informed electorate is a prerequisite to Democracy. - Thomas Jefferson"
    )
    return f"{caption_body.strip()}\n\n{' '.join(footer_parts)}"


def _drive_image_url(drive_link: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", drive_link or "")
    if m:
        return f"https://drive.google.com/thumbnail?id={m.group(1)}&sz=w1200"
    parsed = urlparse(drive_link or "")
    file_id = parse_qs(parsed.query).get("id", [""])[0]
    if file_id:
        return f"https://drive.google.com/thumbnail?id={file_id}&sz=w1200"
    return ""


def _drive_view_url(drive_link: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", drive_link or "")
    if m:
        return f"https://drive.google.com/uc?export=view&id={m.group(1)}"
    parsed = urlparse(drive_link or "")
    file_id = parse_qs(parsed.query).get("id", [""])[0]
    if file_id:
        return f"https://drive.google.com/uc?export=view&id={file_id}"
    return ""


def _copy_block(label: str, value: str, key: str, empty_text: str = "(none)") -> None:
    display_text = value or empty_text
    escaped_text = html.escape(value or "")
    escaped_label = html.escape(label)
    component_html = f"""
    <div style="margin-top:0.25rem;">
      <div style="
        min-height: 8rem;
        max-height: 14rem;
        overflow: auto;
        white-space: pre-wrap;
        border: 1px solid rgba(15,23,42,0.08);
        border-radius: 16px;
        background: #f8fafc;
        padding: 1rem;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 0.98rem;
        line-height: 1.45;
        color: #0f172a;
      ">{html.escape(display_text)}</div>
      <button
        onclick='navigator.clipboard.writeText({escaped_text!r})'
        style="
          width: 100%;
          margin-top: 0.75rem;
          border: 1px solid rgba(15,23,42,0.14);
          border-radius: 14px;
          background: white;
          color: #0f172a;
          padding: 0.85rem 1rem;
          font-size: 1rem;
          font-weight: 600;
          cursor: pointer;
        "
      >Copy {escaped_label}</button>
    </div>
    """
    components.html(component_html, height=250, scrolling=False)


def _move_selected_row(editor_rows: list[dict], step: int) -> None:
    if not editor_rows:
        return
    row_numbers = [row["row_number"] for row in editor_rows]
    current = st.session_state.get("workspace_selected_row_num", row_numbers[0])
    if current not in row_numbers:
        current = row_numbers[0]
    current_index = row_numbers.index(current)
    next_index = max(0, min(len(row_numbers) - 1, current_index + step))
    st.session_state["workspace_selected_row_num"] = row_numbers[next_index]


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


def _ingest_row(row: dict) -> dict:
    """Process one row through ingest and return sheet fields."""
    url = row["Instagram URL"].strip()
    tmp_dir = None
    try:
        if _is_reel_url(url):
            data = process_reel_url(url, include_transcript=False)
            uploaded = upload_thumbnail_only(data)
            tmp_dir = uploaded["tmp_dir"]
        else:
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
    url = row.get("Instagram URL", "").strip()
    if not _is_reel_url(url):
        raise ValueError("Transcript rerun is only available for reels.")

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
        if _is_reel_url(url):
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
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _extract_image_text(row: dict) -> str:
    media_type = (row.get("Media Type", "") or "").strip().lower()
    if media_type != "photo":
        raise ValueError("Image text extraction is only available for photo or carousel posts.")

    links = [link.strip() for link in (row.get("Media Drive Link", "") or "").split(",") if link.strip()]
    if not links:
        raise ValueError("This row does not have image media links in Drive yet.")

    content = [{
        "type": "text",
        "text": "Extract all readable text from these images. Return plain text only, in reading order. No labels or commentary.",
    }]
    for link in links[:10]:
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


def _queue_workspace_action(row_number: int, action: str) -> None:
    queue = st.session_state.setdefault("workspace_action_queue", [])
    queue.append({"row_number": row_number, "action": action})


def _mark_workspace_action_complete(row_number: int, action: str) -> None:
    completed = st.session_state.setdefault("workspace_action_completed", {})
    completed[f"{row_number}:{action}"] = True


def _is_workspace_action_complete(row_number: int, action: str) -> bool:
    completed = st.session_state.setdefault("workspace_action_completed", {})
    return bool(completed.get(f"{row_number}:{action}"))


def _process_next_workspace_action() -> None:
    queue = st.session_state.setdefault("workspace_action_queue", [])
    if not queue:
        return

    current = queue.pop(0)
    row_number = current["row_number"]
    action = current["action"]

    rows = _run_with_sheet_quota_countdown(
        lambda: get_all_rows(GOOGLE_SHEET_ID),
        "Queued edit action paused:",
    )
    row = next((r for r in rows if r.get("row_number") == row_number), None)
    if not row:
        st.session_state["workspace_error"] = f"Row {row_number}: row not found in sheet."
        if queue:
            st.rerun()
        return

    try:
        if action == "transcript":
            with st.spinner(f"Refreshing row {row_number} with transcript..."):
                _rerun_with_transcript(row)
            st.session_state["workspace_success"] = f"Row {row_number}: transcript rerun complete."
        elif action == "download":
            with st.spinner(f"Uploading row {row_number} media to Drive..."):
                _download_media_to_drive(row)
            st.session_state["workspace_success"] = f"Row {row_number}: media uploaded to Drive."
        elif action == "image_text":
            with st.spinner(f"Extracting image text for row {row_number}..."):
                _redo_caption_from_image_text(row)
            st.session_state["workspace_success"] = f"Row {row_number}: caption regenerated from image text."
        else:
            raise ValueError(f"Unknown action: {action}")
        _mark_workspace_action_complete(row_number, action)
    except Exception as e:
        st.session_state["workspace_error"] = f"Row {row_number}: {e}"

    if queue:
        st.rerun()


def _run_home_mode(mode: str, urls: list[str], org_hashtag: str) -> tuple[str, list[dict]]:
    results = []
    tag_value = ORG_HASHTAG_MAP.get(org_hashtag, "")

    for url in urls:
        if mode == "Generate headline":
            post = _fetch_post_data(url)
            source_text = (post.get("original_caption") or "").strip()
            if not source_text:
                raise ValueError(f"{url}: Apify did not return a caption.")
            results.append(
                {
                    "url": url,
                    "username": post.get("username", ""),
                    "headlines": _generate_headlines(source_text),
                    "caption": _build_footered_caption(
                        _generate_caption_from_caption(source_text),
                        post.get("username", ""),
                    ),
                    "source_caption": source_text,
                }
            )
        elif mode == "Caption this":
            post = _fetch_post_data(url)
            row = {
                "Instagram URL": url,
                "Source Username": post.get("username", ""),
                "Original Caption": (post.get("original_caption") or "").strip(),
                "Transcript": "",
                "Caption Context": "",
                "Speaker Name": "",
                "Required Hashtags": tag_value,
                "Top Comment": "",
            }
            caption = generate_row_caption(row)
            results.append(
                {
                    "url": url,
                    "username": post.get("username", ""),
                    "caption": caption,
                    "source_caption": row["Original Caption"],
                }
            )
        elif mode == "Download media":
            tmp_dir = None
            try:
                post = _fetch_post_data(url)
                uploaded = upload_media_bundle(post)
                tmp_dir = uploaded["tmp_dir"]
                results.append(
                    {
                        "url": url,
                        "username": post.get("username", ""),
                        "media_type": post.get("media_type", ""),
                        "media_link": uploaded.get("media_link", ""),
                        "thumbnail_link": uploaded.get("thumbnail_link", ""),
                    }
                )
            finally:
                if tmp_dir:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    return tag_value, results


st.set_page_config(page_title="Workspace", page_icon="🏠", layout="wide")
st.title("Workspace")

if not _check_password():
    st.stop()

_process_next_workspace_action()

success_message = st.session_state.pop("workspace_success", "")
error_message = st.session_state.pop("workspace_error", "")
if success_message:
    st.success(success_message)
if error_message:
    st.error(error_message)

st.markdown(
    """
    <style>
    .workspace-shell {
        max-width: 1120px;
    }
    section[data-testid="stSidebar"] {
        display: none;
    }
    [data-testid="collapsedControl"] {
        display: none;
    }
    .workspace-note {
        padding: 0.85rem 1rem;
        border: 1px solid rgba(15, 23, 42, 0.08);
        border-radius: 14px;
        background: rgba(248, 250, 252, 0.9);
        margin-bottom: 1rem;
    }
    .workspace-home-card {
        border: 1px solid rgba(15, 23, 42, 0.12);
        border-radius: 24px;
        padding: 1.25rem;
        background: #fff;
        box-shadow: 0 12px 32px rgba(15, 23, 42, 0.06);
        margin-bottom: 1rem;
    }
    .workspace-results-card {
        border: 1px solid rgba(15, 23, 42, 0.12);
        border-radius: 20px;
        padding: 1rem;
        background: #fff;
        margin-top: 1rem;
    }
    .workspace-edit-card {
        border: 1px solid rgba(15, 23, 42, 0.12);
        border-radius: 24px;
        padding: 1.25rem;
        background: #fff;
        margin-bottom: 1rem;
        box-shadow: 0 12px 32px rgba(15, 23, 42, 0.06);
    }
    .workspace-row-tabs [role="radiogroup"] {
        gap: 0.45rem;
        overflow-x: auto;
        white-space: nowrap;
        padding-bottom: 0.25rem;
    }
    .workspace-row-tabs [role="radiogroup"] label {
        border: 1px solid rgba(15, 23, 42, 0.12);
        border-radius: 999px;
        padding: 0.2rem 0.75rem;
        background: #fff;
    }
    .workspace-row-tabs [role="radiogroup"] label:has(input:checked) {
        background: #111827;
        color: white;
        border-color: #111827;
    }
    .workspace-row-summary {
        display: flex;
        gap: 0.65rem;
        flex-wrap: wrap;
        margin: 0.15rem 0 0.85rem;
    }
    .workspace-chip {
        border: 1px solid rgba(15, 23, 42, 0.12);
        border-radius: 999px;
        padding: 0.25rem 0.7rem;
        font-size: 0.85rem;
        color: #334155;
        background: #fff;
    }
    .workspace-home-card .stButton > button {
        min-height: 3.15rem;
        border-radius: 14px;
    }
    .workspace-status-line {
        color: #64748b;
        font-size: 0.92rem;
        margin-bottom: 0.2rem;
    }
    .workspace-section-label {
        font-size: 0.8rem;
        font-weight: 700;
        letter-spacing: 0.03em;
        text-transform: uppercase;
        color: #64748b;
        margin: 0.2rem 0 0.55rem;
    }
    .workspace-action-note {
        font-size: 0.92rem;
        color: #475569;
    }
    .workspace-edit-card .stButton > button {
        min-height: 3rem;
        border-radius: 14px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

home_tab, edit_tab, data_tab = st.tabs(["Home", "Edit", "Data"])

with home_tab:
    st.markdown('<div class="workspace-shell">', unsafe_allow_html=True)

    mode_help = {
        "Generate headline": "Pull the Instagram caption, then return three headline options plus a footered caption.",
        "Caption this": "Generate a caption directly from the Instagram caption using the selected hashtag preset.",
        "Download media": "Download the media and upload it to Drive without adding a row first.",
    }

    links = _normalize_home_links(_ensure_home_links())
    for idx, link in enumerate(list(links)):
        links[idx] = st.text_input(
            "Instagram Link" if idx == 0 else f"Instagram Link {idx + 1}",
            value=link,
            placeholder="https://www.instagram.com/p/... or /reel/...",
            key=f"workspace_home_link_{idx}",
            label_visibility="visible" if idx == 0 else "collapsed",
        )
    st.session_state["workspace_home_links"] = links

    mode = st.selectbox("Action", MODE_OPTIONS, index=0, key="workspace_home_mode")
    if mode in mode_help:
        st.caption(mode_help.get(mode, ""))

    org_hashtag = st.session_state.get("workspace_org_hashtag", "")
    if _mode_uses_org_hashtag(mode):
        org_hashtag = st.selectbox(
            "Apply organization hashtag",
            ORG_HASHTAG_OPTIONS,
            index=ORG_HASHTAG_OPTIONS.index(org_hashtag) if org_hashtag in ORG_HASHTAG_OPTIONS else 0,
            key="workspace_org_hashtag",
        )
    selected_hashtag = ORG_HASHTAG_MAP.get(org_hashtag, "")

    secondary_cols = st.columns(2)
    with secondary_cols[0]:
        if st.button("+ Add another", use_container_width=True):
            st.session_state["workspace_home_links"] = st.session_state["workspace_home_links"] + [""]
            st.rerun()
    with secondary_cols[1]:
        if st.button(_action_label(mode), type="primary", use_container_width=True):
            links_to_process = _clean_home_links()
            if not links_to_process:
                st.warning("Enter at least one Instagram link.")
            elif mode == "Add to sheet":
                try:
                    append_link_rows(
                        GOOGLE_SHEET_ID,
                        links_to_process,
                        selected_hashtag,
                    )
                except Exception as e:
                    st.error(f"Could not add links to sheet: {e}")
                else:
                    st.success(f"Added {len(links_to_process)} link(s) to the sheet.")
                    st.session_state["workspace_home_links"] = [""]
                    st.rerun()
            else:
                with st.spinner(f"{mode} in progress..."):
                    try:
                        tag_value, results = _run_home_mode(mode, links_to_process, org_hashtag)
                    except Exception as e:
                        st.error(f"{mode} failed: {e}")
                    else:
                        st.session_state["workspace_home_results"] = {
                            "mode": mode,
                            "required_hashtag": tag_value,
                            "items": results,
                        }
                        st.success(f"{mode} finished for {len(results)} link(s).")
    home_results = st.session_state.get("workspace_home_results")
    if home_results and home_results.get("mode") == "Generate headline":
        for idx, item in enumerate(home_results.get("items", []), start=1):
            st.markdown('<div class="workspace-results-card">', unsafe_allow_html=True)
            st.caption(f"Result {idx}")
            st.write(f"@{item.get('username') or 'unknown'}")
            st.markdown(f"[Open Instagram link ↗]({item['url']})")
            headline_tabs = st.tabs(["Headline 1", "Headline 2", "Headline 3", "Caption"])
            for tab_idx, headline in enumerate(item.get("headlines", [])[:3]):
                with headline_tabs[tab_idx]:
                    _copy_block(f"headline {tab_idx + 1}", headline, f"workspace_home_headline_{idx}_{tab_idx}")
            with headline_tabs[3]:
                _copy_block("caption", item.get("caption", ""), f"workspace_home_caption_{idx}")
            st.markdown('</div>', unsafe_allow_html=True)

    if home_results and home_results.get("mode") == "Caption this":
        for idx, item in enumerate(home_results.get("items", []), start=1):
            st.markdown('<div class="workspace-results-card">', unsafe_allow_html=True)
            st.caption(f"Caption {idx}")
            st.write(f"@{item.get('username') or 'unknown'}")
            st.markdown(f"[Open Instagram link ↗]({item['url']})")
            _copy_block("caption", item.get("caption", ""), f"workspace_home_caption_only_{idx}")
            st.markdown('</div>', unsafe_allow_html=True)

    if home_results and home_results.get("mode") == "Download media":
        for idx, item in enumerate(home_results.get("items", []), start=1):
            st.markdown('<div class="workspace-results-card">', unsafe_allow_html=True)
            st.caption(f"Download {idx}")
            st.write(f"@{item.get('username') or 'unknown'} · {item.get('media_type') or 'unknown'}")
            st.markdown(f"[Open Instagram link ↗]({item['url']})")
            if item.get("media_link"):
                st.write(f"Media link(s): {item['media_link']}")
            if item.get("thumbnail_link"):
                st.write(f"Thumbnail: {item['thumbnail_link']}")
            st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

with edit_tab:
    st.markdown('<div class="workspace-shell">', unsafe_allow_html=True)
    try:
        editor_rows = _run_with_sheet_quota_countdown(
            lambda: [
                r for r in get_all_rows(GOOGLE_SHEET_ID)
                if r.get("Instagram URL", "").strip()
                and r.get("Status", "").strip().lower() in EDITABLE_STATUSES
            ],
            "Loading editor rows paused:",
        )
    except Exception as e:
        st.error(f"Could not load edit rows: {e}")
        editor_rows = []

    if not editor_rows:
        st.info("No rows yet. Add a link on Home or process new rows on Data.")
    else:
        done_count = sum(1 for row in editor_rows if (row.get("Status", "").strip().lower() == "done"))
        pending_count = len(editor_rows) - done_count
        st.caption("Done rows stay here until you delete them from the sheet.")
        st.markdown(
            (
                '<div class="workspace-row-summary">'
                f'<div class="workspace-chip">{len(editor_rows)} total</div>'
                f'<div class="workspace-chip">{pending_count} in progress</div>'
                f'<div class="workspace-chip">{done_count} done</div>'
                '</div>'
            ),
            unsafe_allow_html=True,
        )
        row_numbers = [row["row_number"] for row in editor_rows]
        if st.session_state.get("workspace_selected_row_num") not in row_numbers:
            st.session_state["workspace_selected_row_num"] = row_numbers[0]
        selected_row_num = st.session_state["workspace_selected_row_num"]
        selected_row_index = row_numbers.index(selected_row_num)

        nav_cols = st.columns([1, 1, 6], vertical_alignment="center")
        with nav_cols[0]:
            if st.button("← Prev", use_container_width=True, disabled=selected_row_index == 0, key="workspace_prev_row"):
                _move_selected_row(editor_rows, -1)
                st.rerun()
        with nav_cols[1]:
            if st.button("Next →", use_container_width=True, disabled=selected_row_index == len(editor_rows) - 1, key="workspace_next_row"):
                _move_selected_row(editor_rows, 1)
                st.rerun()

        options = [
            f"{'✓ ' if (row.get('Status', '').strip().lower() == 'done') else ''}{row['row_number']}"
            for row in editor_rows
        ]
        selected_option_index = row_numbers.index(selected_row_num)
        st.markdown('<div class="workspace-row-tabs">', unsafe_allow_html=True)
        selected_label = st.radio(
            "Edit row",
            options,
            index=selected_option_index,
            horizontal=True,
            label_visibility="collapsed",
            key=f"workspace_selected_row_{selected_row_num}",
        )
        st.markdown('</div>', unsafe_allow_html=True)
        selected_index = options.index(selected_label)
        st.session_state["workspace_selected_row_num"] = row_numbers[selected_index]
        row = editor_rows[selected_index]
        row_num = row["row_number"]
        username = (row.get("Source Username") or "").strip()
        url = (row.get("Instagram URL") or "").strip()
        media_type = (row.get("Media Type") or "").strip().lower()
        generated = (row.get("Generated Caption") or "").strip()
        original_caption = (row.get("Original Caption") or "").strip()
        transcript = (row.get("Transcript") or "").strip()
        caption_context = row.get("Caption Context", "")
        top_comment = row.get("Top Comment", "")
        speaker_name = row.get("Speaker Name", "")
        required_hashtags = row.get("Required Hashtags", "")
        status = (row.get("Status") or "").strip()

        st.markdown('<div class="workspace-edit-card">', unsafe_allow_html=True)
        top_left, top_right = st.columns([1.05, 1.25], vertical_alignment="top")
        with top_left:
            thumb_link = (row.get("Thumbnail Drive Link") or "").strip()
            if thumb_link:
                image_url = _drive_image_url(thumb_link) or thumb_link
                st.image(image_url, width="stretch")
            else:
                st.info("Thumbnail will appear here after ingest.")

        with top_right:
            st.markdown(
                f'<div class="workspace-status-line">Row {row_num} · {media_type or "pending"} · {status or "blank"}</div>',
                unsafe_allow_html=True,
            )
            st.subheader(f"@{username}" if username else f"Row {row_num}")
            if url:
                st.link_button("Open in Instagram", url, use_container_width=True)

            button_cols = st.columns(3)
            with button_cols[0]:
                primary_action = "transcript" if _is_reel_url(url) else "image_text"
                primary_icon = "✓" if _is_workspace_action_complete(row_num, primary_action) else ("🎙️" if _is_reel_url(url) else "🖼️")
                primary_help = "Re-run with transcript" if _is_reel_url(url) else "Get context from text in images"
                if st.button(
                    primary_icon,
                    key=f"workspace_primary_action_{row_num}",
                    help=primary_help,
                    disabled=not url,
                    use_container_width=True,
                ):
                    _queue_workspace_action(row_num, primary_action)
                    st.rerun()
            with button_cols[1]:
                download_icon = "✓" if _is_workspace_action_complete(row_num, "download") else "⬇️"
                if st.button(
                    download_icon,
                    key=f"workspace_download_action_{row_num}",
                    help="Download media to Drive",
                    disabled=not url,
                    use_container_width=True,
                ):
                    _queue_workspace_action(row_num, "download")
                    st.rerun()
            with button_cols[2]:
                done_label = "✓" if status.lower() == "done" else "Done"
                if st.button(
                    done_label,
                    key=f"workspace_done_action_{row_num}",
                    help="Mark row as done",
                    use_container_width=True,
                ):
                    next_status = "ingested" if status.lower() == "done" else "done"
                    update_status(GOOGLE_SHEET_ID, row_num, next_status)
                    st.session_state["workspace_success"] = f"Row {row_num}: status set to {next_status}."
                    st.rerun()

            info_cols = st.columns([1.25, 1])
            with info_cols[0]:
                speaker_name = st.text_input(
                    "Speaker Name",
                    value=speaker_name,
                    key=f"workspace_speaker_{row_num}",
                    placeholder="Enter name",
                )
            with info_cols[1]:
                required_hashtags = st.text_input(
                    "Required Hashtags",
                    value=required_hashtags,
                    key=f"workspace_hashtags_{row_num}",
                    placeholder="#tag1 #tag2",
                )

            add_cols = st.columns([1, 1, 2])
            with add_cols[0]:
                if st.button("🔗", key=f"workspace_toggle_top_{row_num}", help="Add hashtag link / top comment", use_container_width=True):
                    st.session_state[f"workspace_show_top_{row_num}"] = not st.session_state.get(f"workspace_show_top_{row_num}", bool(top_comment))
                    st.rerun()
            with add_cols[1]:
                if st.button("💡", key=f"workspace_toggle_context_{row_num}", help="Add context", use_container_width=True):
                    st.session_state[f"workspace_show_context_{row_num}"] = not st.session_state.get(f"workspace_show_context_{row_num}", bool(caption_context))
                    st.rerun()
            with add_cols[2]:
                if st.button("Update", key=f"workspace_update_{row_num}", type="primary", use_container_width=True):
                    current_context = st.session_state.get(f"workspace_context_{row_num}", caption_context).strip()
                    current_top = st.session_state.get(f"workspace_top_{row_num}", top_comment).strip()
                    current_speaker = st.session_state.get(f"workspace_speaker_{row_num}", speaker_name).strip()
                    current_hashtags = st.session_state.get(f"workspace_hashtags_{row_num}", required_hashtags).strip()
                    update_metadata(
                        GOOGLE_SHEET_ID,
                        row_num,
                        current_context,
                        current_speaker,
                        current_hashtags,
                        current_top,
                        "",
                    )
                    st.session_state["workspace_success"] = f"Row {row_num}: metadata updated."
                    st.rerun()

            if st.session_state.get(f"workspace_show_top_{row_num}", bool(top_comment)):
                st.text_area(
                    "Hashtag Link / Top Comment",
                    value=top_comment,
                    key=f"workspace_top_{row_num}",
                    height=68,
                    placeholder="Prepended above the generated caption.",
                )
            if st.session_state.get(f"workspace_show_context_{row_num}", bool(caption_context)):
                st.text_area(
                    "Context",
                    value=caption_context,
                    key=f"workspace_context_{row_num}",
                    height=90,
                    placeholder="Add context for posts that need more source detail.",
                )

        st.markdown('<div class="workspace-section-label">Content</div>', unsafe_allow_html=True)
        text_tabs = st.tabs(["Caption", "Original caption", "Transcript"])
        with text_tabs[0]:
            _copy_block("caption", generated, f"caption_tab_copy_{row_num}")
            if st.button("Generate caption", key=f"workspace_generate_caption_{row_num}", type="primary", use_container_width=True):
                current_context = st.session_state.get(f"workspace_context_{row_num}", caption_context).strip()
                current_top = st.session_state.get(f"workspace_top_{row_num}", top_comment).strip()
                current_speaker = st.session_state.get(f"workspace_speaker_{row_num}", speaker_name).strip()
                current_hashtags = st.session_state.get(f"workspace_hashtags_{row_num}", required_hashtags).strip()
                row_for_caption = dict(row)
                row_for_caption["Caption Context"] = current_context
                row_for_caption["Top Comment"] = current_top
                row_for_caption["Speaker Name"] = current_speaker
                row_for_caption["Required Hashtags"] = current_hashtags
                update_metadata(
                    GOOGLE_SHEET_ID,
                    row_num,
                    current_context,
                    current_speaker,
                    current_hashtags,
                    current_top,
                    "",
                )
                caption = generate_row_caption(row_for_caption)
                update_caption(GOOGLE_SHEET_ID, row_num, caption, "done")
                st.session_state["workspace_success"] = f"Row {row_num}: caption generated."
                st.rerun()
        with text_tabs[1]:
            _copy_block("original caption", original_caption, f"original_tab_copy_{row_num}")
        with text_tabs[2]:
            _copy_block("transcript", transcript, f"transcript_tab_copy_{row_num}")
        st.markdown("</div>", unsafe_allow_html=True)

        queue = st.session_state.get("workspace_action_queue", [])
        if queue:
            st.markdown(
                f'<div class="workspace-action-note">{len(queue)} queued action(s) waiting to run.</div>',
                unsafe_allow_html=True,
            )
    st.markdown('</div>', unsafe_allow_html=True)

with data_tab:
    st.markdown('<div class="workspace-shell">', unsafe_allow_html=True)
    st.caption("Data view for the Google Sheet plus batch ingest.")

    try:
        all_rows = _run_with_sheet_quota_countdown(
            lambda: get_all_rows(GOOGLE_SHEET_ID),
            "Loading rows paused:",
        )
    except Exception as e:
        st.error(f"Could not load sheet: {e}")
        all_rows = []

    if all_rows:
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
        st.info("No rows in sheet yet.")

    if st.button("Process new rows", type="primary", use_container_width=True, key="workspace_process_rows"):
        try:
            pending = _run_with_sheet_quota_countdown(
                lambda: get_pending_rows(GOOGLE_SHEET_ID),
                "Processing new rows paused:",
            )
        except Exception as e:
            st.error(f"Could not read sheet: {e}")
            pending = []

        if not pending:
            st.info("No new rows to process.")
        else:
            progress = st.progress(0)
            for i, row in enumerate(pending):
                row_num = row["row_number"]
                label = row["Instagram URL"][:60]
                with st.status(f"Row {row_num}: {label}", expanded=False) as status_box:
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
                        status_box.update(label=f"Row {row_num}: error writing to sheet - {e}", state="error")
                    else:
                        if result["status"].startswith("error"):
                            status_box.update(label=f"Row {row_num}: {result['status']}", state="error")
                        else:
                            status_box.update(
                                label=f"Row {row_num}: ingested - @{result['username']} ({result['media_type']})",
                                state="complete",
                            )
                progress.progress((i + 1) / len(pending))
            st.success(f"Done. Ingested {len(pending)} row(s).")
    st.markdown('</div>', unsafe_allow_html=True)
