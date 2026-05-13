"""Unified workspace shell for the next UI redesign."""

import base64
from datetime import datetime, time as dt_time, timedelta
import ast
import hashlib
import json
import html
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
import requests
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openai
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from article_source import fetch_article_source
from config import (
    DEFAULT_POST_FOOTER,
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    GOOGLE_SHEET_ID,
    OPENAI_API_KEY,
)
from drive import (
    copy_drive_file_to_folder,
    download_drive_file,
    get_drive_file_metadata,
    get_or_create_subfolder,
    upload_to_drive,
)
from ingest_helpers import _compact_post_date, build_filename_prefix, upload_media_bundle
import pipeline_caption as pipeline_caption_ops
from post_scraper import process_url as process_post_url
from reel_scraper import process_url as process_reel_url
import sheets as sheet_ops
from utils.auth import require_auth
from utils.error_labels import describe_error
from utils.styles import inject as inject_styles

generate_row_caption = pipeline_caption_ops.generate_row_caption
_strip_top_comment_paragraphs = pipeline_caption_ops._strip_top_comment_paragraphs
generate_carousel_copy_with_model = getattr(
    pipeline_caption_ops,
    "generate_carousel_copy_with_model",
    lambda row, model="gpt-4o": pipeline_caption_ops.generate_carousel_copy(row),
)
generate_carousel_copy = getattr(
    pipeline_caption_ops,
    "generate_carousel_copy",
    lambda _row: {"name": "", "text1": "", "text2": "", "text3": ""},
)
generate_batch_carousel_copy_with_model = getattr(
    pipeline_caption_ops,
    "generate_batch_carousel_copy_with_model",
    lambda rows, model="gpt-5.2": {},
)
REPO_ROOT = Path(__file__).resolve().parents[1]
MISSING_THUMBNAIL_ASSET = REPO_ROOT / "assets" / "workspace-missing-thumbnail.jpg"
MISSING_REEL_THUMBNAIL_ASSET = REPO_ROOT / "assets" / "workspace-missing-reel-thumbnail.jpg"

MODE_OPTIONS = [
    "Add to sheet",
    "Process this",
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

EDITABLE_STATUSES = {"ingested", "done", "slides"}
TRANSCRIPT_SIZE_WARNING_BYTES = 100 * 1024 * 1024
EDITOR_INITIAL_RENDER_LIMIT = 12
INSTAGRAM_CANVAS_WIDTH_PX = 1080
INSTAGRAM_CANVAS_HEIGHT_PX = 1485
PREVIEW_EXPORT_WIDTH_PX = 1080
PREVIEW_EXPORT_HEIGHT_PX = 1350
PREVIEW_EXPORT_SCALE = PREVIEW_EXPORT_HEIGHT_PX / INSTAGRAM_CANVAS_HEIGHT_PX
PREVIEW_EXPORT_FONT_SCALE = 0.92
PREVIEW_CANVAS_WIDTH_PX = 420
PREVIEW_CANVAS_HEIGHT_PX = round(
    PREVIEW_CANVAS_WIDTH_PX * INSTAGRAM_CANVAS_HEIGHT_PX / INSTAGRAM_CANVAS_WIDTH_PX
)
PREVIEW_SLIDE_FONT_FAMILY = "'Poppins', sans-serif"
PREVIEW_SLIDE_FONT_WEIGHT = 500
PREVIEW_SLIDE_LETTER_SPACING = "0.01em"
PREVIEW_SLIDE_LINE_HEIGHT = "1.26"
SLIDE_TWO_FONT_MIN_REM = 1.4
SLIDE_TWO_FONT_VW = 4.4
SLIDE_TWO_FONT_MAX_REM = 3.35
SLIDE_THREE_FONT_MIN_REM = 1.4
SLIDE_THREE_FONT_VW = 4.0
SLIDE_THREE_FONT_MAX_REM = 3.0
PREVIEW_UPLOAD_SUBFOLDER = "previews"
PINNED_TOP_COMMENT_PREFIX = "[[TOP]] "

_client: openai.OpenAI | None = None


def _get_client() -> openai.OpenAI:
    global _client
    if _client is None:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        _client = openai.OpenAI(api_key=OPENAI_API_KEY, timeout=45.0, max_retries=1)
    return _client

get_all_rows = sheet_ops.get_all_rows
get_pending_rows = sheet_ops.get_pending_rows
update_caption = sheet_ops.update_caption
update_caption_and_metadata = getattr(sheet_ops, "update_caption_and_metadata", None)
update_caption_context = sheet_ops.update_caption_context
update_ingest_result = sheet_ops.update_ingest_result
update_metadata = sheet_ops.update_metadata
update_speaker_names_batch = getattr(sheet_ops, "update_speaker_names_batch", None)
update_scheduled_times = sheet_ops.update_scheduled_times
update_transcript = sheet_ops.update_transcript
update_thumbnail_link = getattr(sheet_ops, "update_thumbnail_link", None)
update_carousel_fields = getattr(sheet_ops, "update_carousel_fields", None)
delete_sheet_row = sheet_ops.delete_row
get_fundraising_links = getattr(sheet_ops, "get_fundraising_links", lambda _sheet_id: [])
if hasattr(sheet_ops, "get_last_scheduled_times"):
    get_last_scheduled_times = sheet_ops.get_last_scheduled_times
else:
    def get_last_scheduled_times(sheet_id: str) -> list[str]:
        if hasattr(sheet_ops, "get_last_scheduled_time"):
            value = sheet_ops.get_last_scheduled_time(sheet_id)
            return [value] if value else []
        return []

if hasattr(sheet_ops, "update_last_scheduled_times"):
    update_last_scheduled_times = sheet_ops.update_last_scheduled_times
else:
    def update_last_scheduled_times(sheet_id: str, scheduled_times: list[str]) -> None:
        if hasattr(sheet_ops, "update_last_scheduled_time") and scheduled_times:
            sheet_ops.update_last_scheduled_time(sheet_id, scheduled_times[-1])


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
        row[10] = required_hashtags.strip()
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


def _cell_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _is_instagram_url(url: str) -> bool:
    return "instagram.com/" in (url or "").lower()


def _is_article_url(url: str) -> bool:
    return _is_https_url(url) and not _is_instagram_url(url)


def _format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    units = ["B", "KB", "MB", "GB"]
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    return f"{value:.1f} {unit}"


def _get_remote_file_size(url: str) -> int:
    try:
        response = requests.head(url, allow_redirects=True, timeout=20)
        response.raise_for_status()
        content_length = response.headers.get("Content-Length") or response.headers.get("content-length")
        if content_length:
            return int(content_length)
    except Exception:
        pass

    response = requests.get(url, allow_redirects=True, timeout=20, stream=True)
    response.raise_for_status()
    content_length = response.headers.get("Content-Length") or response.headers.get("content-length")
    if content_length:
        return int(content_length)
    raise ValueError("Could not determine reel file size.")


def _check_reel_transcript_risk(row: dict) -> dict | None:
    url = row.get("Instagram URL", "").strip()
    if not _is_reel_url(url):
        return None

    preview = process_reel_url(url, include_transcript=False)
    media_urls = preview.get("media_urls") or []
    if not media_urls:
        raise ValueError("Could not find the reel video URL for size check.")

    size_bytes = _get_remote_file_size(media_urls[0])
    if size_bytes <= TRANSCRIPT_SIZE_WARNING_BYTES:
        return None

    return {
        "size_bytes": size_bytes,
        "threshold_bytes": TRANSCRIPT_SIZE_WARNING_BYTES,
    }


def _ensure_home_links() -> list[str]:
    if st.session_state.pop("_workspace_reset_home_links", False):
        for key in list(st.session_state.keys()):
            if key.startswith("workspace_home_link_"):
                st.session_state.pop(key, None)
        st.session_state["workspace_home_links"] = [""]

    links = st.session_state.setdefault("workspace_home_links", [""])
    if not links:
        links.append("")
    return links


def _reset_home_links_on_next_render() -> None:
    st.session_state["_workspace_reset_home_links"] = True
    st.session_state["workspace_home_links"] = [""]


def _mark_transcribe_checkbox_for_reset(row: dict) -> None:
    transcribe_key = _workspace_key(row, "transcribe")
    pending = st.session_state.setdefault("workspace_transcribe_reset_rows", [])
    if transcribe_key not in pending:
        pending.append(transcribe_key)


def _workspace_row_identity(row: dict) -> str:
    return "||".join([
        _cell_text(row.get("Instagram URL")).strip(),
        _cell_text(row.get("Media Type")).strip(),
        _cell_text(row.get("Source Username")).strip(),
    ])


def _row_state_token(row: dict) -> str:
    identity = _workspace_row_identity(row) or str(row.get("row_number", ""))
    return hashlib.md5(identity.encode("utf-8")).hexdigest()[:12]


def _workspace_stable_row_key(row: dict, name: str) -> str:
    return f"workspace_{name}_row_{row.get('row_number', '')}"


def _workspace_speaker_key(row: dict) -> str:
    return _workspace_stable_row_key(row, "speaker")


def _workspace_row_state_keys_for_token(token: str) -> list[str]:
    return [
        f"workspace_hashtags_{token}",
        f"workspace_top_{token}",
        f"workspace_context_{token}",
        f"workspace_transcript_warning_{token}",
        f"workspace_transcribe_{token}",
        f"workspace_link_editor_open_{token}",
        f"workspace_link_source_{token}",
        f"workspace_link_url_{token}",
        f"workspace_link_display_{token}",
        f"workspace_link_comment_{token}",
        f"workspace_menu_nonce_{token}",
        f"workspace_thumbnail_upload_{token}",
    ]


def _workspace_key(row: dict, name: str) -> str:
    return f"workspace_{name}_{_row_state_token(row)}"


def _workspace_row_state_keys(row: dict) -> list[str]:
    return _workspace_row_state_keys_for_token(_row_state_token(row))


def _sync_workspace_row_state(row: dict) -> None:
    identity_key = _workspace_stable_row_key(row, "identity")
    token_key = _workspace_stable_row_key(row, "state_token")
    speaker_key = _workspace_speaker_key(row)
    current_identity = _workspace_row_identity(row)
    current_token = _row_state_token(row)
    previous_identity = st.session_state.get(identity_key)
    previous_token = st.session_state.get(token_key)
    if previous_identity == current_identity:
        return
    tokens_to_clear = {current_token}
    if previous_token:
        tokens_to_clear.add(previous_token)
    if previous_identity is not None or previous_token is not None:
        st.session_state.pop(speaker_key, None)
        for token in tokens_to_clear:
            for key in _workspace_row_state_keys_for_token(token):
                st.session_state.pop(key, None)
    st.session_state[identity_key] = current_identity
    st.session_state[token_key] = current_token


def _clear_workspace_row_state(row: dict) -> None:
    identity_key = _workspace_stable_row_key(row, "identity")
    token_key = _workspace_stable_row_key(row, "state_token")
    speaker_key = _workspace_speaker_key(row)
    previous_token = st.session_state.get(token_key)
    tokens_to_clear = {_row_state_token(row)}
    if previous_token:
        tokens_to_clear.add(previous_token)
    st.session_state.pop(speaker_key, None)
    for token in tokens_to_clear:
        for key in _workspace_row_state_keys_for_token(token):
            st.session_state.pop(key, None)
    st.session_state.pop(identity_key, None)
    st.session_state.pop(token_key, None)


def _normalize_home_links(links: list[str]) -> list[str]:
    first = ""
    for link in links:
        if (link or "").strip():
            first = link
            break
    return [first]


def _remove_home_link(index: int) -> None:
    links = st.session_state.get("workspace_home_links", [""])
    next_links = [link for i, link in enumerate(links) if i != index]
    st.session_state["workspace_home_links"] = _normalize_home_links(next_links or [""])


def _action_label(mode: str) -> str:
    return {
        "Add to sheet": "Add",
        "Process this": "Process",
        "Generate headline": "Generate",
        "Caption this": "Caption",
        "Download media": "Download",
    }.get(mode, "Add")


def _mode_uses_org_hashtag(mode: str) -> bool:
    return mode in {"Add to sheet", "Process this", "Caption this"}


def _clean_home_links() -> list[str]:
    return [link.strip() for link in st.session_state.get("workspace_home_links", []) if link.strip()]


def _row_is_dirty(row: dict) -> bool:
    speaker_key = _workspace_speaker_key(row)
    hashtags_key = _workspace_key(row, "hashtags")
    top_key = _workspace_key(row, "top")
    context_key = _workspace_key(row, "context")
    return any(
        [
            _cell_text(st.session_state.get(speaker_key, row.get("Speaker Name", ""))).strip()
            != _cell_text(row.get("Speaker Name")).strip(),
            _cell_text(st.session_state.get(hashtags_key, row.get("Required Hashtags", ""))).strip()
            != _cell_text(row.get("Required Hashtags")).strip(),
            _cell_text(st.session_state.get(top_key, row.get("Top Comment", ""))).strip()
            != _cell_text(row.get("Top Comment")).strip(),
            _cell_text(st.session_state.get(context_key, row.get("Caption Context", ""))).strip()
            != _cell_text(row.get("Caption Context")).strip(),
        ]
    )


def _is_editable_row(row: dict) -> bool:
    if not _cell_text(row.get("Instagram URL")).strip():
        return False

    status = _cell_text(row.get("Status")).strip().lower()
    if status in EDITABLE_STATUSES:
        return True

    # Some rows may already be effectively ingested even if the status field
    # is not one of the editor-specific values yet.
    return any(
        _cell_text(row.get(field, "")).strip()
        for field in [
            "Source Username",
            "Media Type",
            "Media Drive Link",
            "Thumbnail Drive Link",
            "Original Caption",
            "Transcript",
            "Generated Caption",
        ]
    )


def _default_editor_status(row: dict) -> str:
    generated_caption = (row.get("Generated Caption") or "").strip()
    return "done" if generated_caption else "ingested"


def _sort_editor_rows(rows: list[dict]) -> list[dict]:
    def sort_key(row):
        is_skipped = _cell_text(row.get("Status")).strip().lower() == "skipped"
        return (1 if is_skipped else 0, row.get("row_number", 0))

    return sorted(rows, key=sort_key)


def _grid_badges(row: dict) -> list[tuple[str, str]]:
    badges = []
    media_type = _cell_text(row.get("Media Type")).strip().lower()
    status = _cell_text(row.get("Status")).strip().lower()
    if _cell_text(row.get("Generated Caption")).strip():
        badges.append(("C", "Has caption"))
    if _cell_text(row.get("Transcript")).strip():
        badges.append(("T", "Transcribed"))
    if status == "skipped":
        badges.append(("S", "Skipped"))
    try:
        photo_count = int(row.get("Photo Count") or 0)
    except Exception:
        photo_count = 0
    if media_type == "photo" and photo_count > 1:
        badges.append(("P+", "Photo carousel"))
    return badges


def _grid_preview_url(row: dict) -> str:
    thumb_link = _cell_text(row.get("Thumbnail Drive Link")).strip()
    if thumb_link:
        candidate = _safe_browser_image_url(thumb_link)
        if _remote_image_usable(candidate):
            return candidate
    return ""


def _visible_rows_with_target(rows: list[dict], limit: int, target_row_number: str = "") -> list[dict]:
    visible_rows = rows[:limit]
    if target_row_number:
        target_row = next((row for row in rows if str(row.get("row_number", "")) == target_row_number), None)
        is_skipped = _cell_text((target_row or {}).get("Status")).strip().lower() == "skipped"
        if target_row and not is_skipped and all(row.get("row_number") != target_row.get("row_number") for row in visible_rows):
            visible_rows = [*visible_rows, target_row]
    return visible_rows


def _render_editor_grid(editor_rows: list[dict]) -> None:
    cards = []
    missing_image_url = _missing_thumbnail_data_url()
    missing_reel_image_url = _missing_reel_thumbnail_data_url()
    for row in editor_rows:
        row_num = row.get("row_number")
        username = _cell_text(row.get("Source Username")).strip().lstrip("@")
        media_type = _cell_text(row.get("Media Type")).strip().lower() or "post"
        image_url = _grid_preview_url(row)
        fallback_image_url = missing_reel_image_url if media_type == "reel" and missing_reel_image_url else missing_image_url
        badge_html = "".join(
            f'<span class="workspace-grid-badge" title="{html.escape(title)}">{html.escape(label)}</span>'
            for label, title in _grid_badges(row)
        )
        label = f"@{username}" if username else f"Row {row_num}"
        href = f"?workspace_row={row_num}#workspace-row-{row_num}"
        if image_url:
            onerror_attr = (
                f' onerror="this.onerror=null;this.src=\'{html.escape(fallback_image_url)}\';"'
                if fallback_image_url
                else ""
            )
            media_html = (
                f'<img src="{html.escape(image_url)}" alt="{html.escape(label)}" '
                f'loading="lazy" decoding="async"{onerror_attr}>'
            )
        elif fallback_image_url:
            media_html = f'<img src="{html.escape(fallback_image_url)}" alt="{html.escape(label)}" loading="lazy" decoding="async">'
        else:
            media_html = (
                '<div class="workspace-grid-placeholder">'
                f'{html.escape(label)}<br>{html.escape(media_type)}'
                '</div>'
            )
        cards.append(
            f"""
            <a class="workspace-grid-card" href="{html.escape(href)}">
              {media_html}
              <div class="workspace-grid-badges">{badge_html}</div>
              <div class="workspace-grid-meta">{html.escape(label)} · {html.escape(media_type)}</div>
            </a>
            """
        )
    grid_html = "".join(cards)
    st.html(f'<div class="workspace-grid">{grid_html}</div>')


def _scroll_to_editor_row(row_number: str) -> None:
    if not row_number:
        return
    target_id = f"workspace-row-{row_number}"
    script = f"""
    <script>
    const targetId = {json.dumps(target_id)};
    function scrollToTarget(attempt) {{
      const target = window.parent.document.getElementById(targetId);
      if (target) {{
        target.scrollIntoView({{ behavior: "smooth", block: "start" }});
        return;
      }}
      if (attempt < 20) {{
        window.setTimeout(() => scrollToTarget(attempt + 1), 100);
      }}
    }}
    window.setTimeout(() => scrollToTarget(0), 100);
    </script>
    """
    components.html(script, height=0, width=0)


WEEKDAY_OPTIONS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
EASTERN_TZ = ZoneInfo("America/New_York")


def _schedule_day_defaults() -> tuple[str, dt_time]:
    now = datetime.now(EASTERN_TZ)
    return WEEKDAY_OPTIONS[now.weekday()], now.time().replace(second=0, microsecond=0)


def _next_schedule_slot(current_dt: datetime, rollover_minute: int) -> datetime:
    next_dt = current_dt + timedelta(hours=1)
    if next_dt.date() != current_dt.date():
        next_dt = next_dt.replace(hour=9, minute=rollover_minute, second=0, microsecond=0)
    elif next_dt.hour < 9:
        next_dt = next_dt.replace(hour=9, minute=rollover_minute, second=0, microsecond=0)
    return next_dt


def _format_schedule_time(value: dt_time) -> str:
    hour = value.hour % 12 or 12
    suffix = "am" if value.hour < 12 else "pm"
    return f"{hour}:{value.minute:02d}{suffix}"


def _time_parts(value: dt_time) -> tuple[int, int, str]:
    hour = value.hour % 12 or 12
    suffix = "am" if value.hour < 12 else "pm"
    return hour, value.minute, suffix


def _time_from_parts(hour: int, minute: int, suffix: str) -> dt_time:
    normalized_hour = hour % 12
    if suffix == "pm":
        normalized_hour += 12
    return dt_time(normalized_hour, minute)


def _build_schedule_labels(rows: list[dict], start_day: str, start_time: dt_time) -> dict[int, str]:
    if not rows:
        return {}

    start_index = WEEKDAY_OPTIONS.index(start_day)
    anchor = datetime(2026, 1, 5 + start_index, start_time.hour, start_time.minute)
    current = anchor
    labels: dict[int, str] = {}
    rollover_minute = start_time.minute
    for row in rows:
        current = _next_schedule_slot(current, rollover_minute)
        labels[row["row_number"]] = f"{WEEKDAY_OPTIONS[current.weekday()]} {_format_schedule_time(current.time())}"
    return labels


def _last_scheduled_time_labels(rows: list[dict]) -> list[str]:
    scheduled_rows = sorted(
        [
            row for row in rows
            if (row.get("Scheduled Time", "") or "").strip()
        ],
        key=lambda row: row.get("row_number", 0),
    )
    if not scheduled_rows:
        return []
    return [
        (row.get("Scheduled Time", "") or "").strip()
        for row in scheduled_rows[-3:]
        if (row.get("Scheduled Time", "") or "").strip()
    ]


def _persisted_last_scheduled_time_labels(rows: list[dict]) -> list[str]:
    try:
        persisted = get_last_scheduled_times(GOOGLE_SHEET_ID)
        if persisted:
            return persisted
    except Exception:
        pass
    row_labels = _last_scheduled_time_labels(rows)
    return row_labels[-1:] if row_labels else []


def _fetch_post_data(url: str) -> dict:
    if _is_reel_url(url):
        return process_reel_url(url, include_transcript=False)
    return process_post_url(url)


def _fetch_link_data(url: str) -> dict:
    if _is_instagram_url(url):
        post = _fetch_post_data(url)
        return {
            "url": url,
            "username": post.get("username", ""),
            "source_text": (post.get("original_caption") or "").strip(),
            "is_instagram": True,
        }

    article = fetch_article_source(url)
    article_source_text = (
        (article.get("source_text") or "").strip()
        or (article.get("summary_text") or "").strip()
    )
    return {
        "url": article.get("url", url),
        "username": "",
        "display_name": article.get("domain", ""),
        "source_text": article_source_text,
        "is_instagram": False,
    }


def _generate_headlines(source_text: str) -> list[str]:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured.")
    response = _get_client().chat.completions.create(
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


def _build_footered_caption(caption_body: str, username: str, required_hashtags: str = "") -> str:
    footer_parts = []
    cleaned_username = (username or "").strip().lstrip("@")
    if cleaned_username and cleaned_username.lower() != "unknown":
        footer_parts.append(f"Follow @{cleaned_username} for more.")
    footer_parts.append(
        "Help this information get to more voters. 🇺🇸 "
        "A well-informed electorate is a prerequisite to Democracy. - Thomas Jefferson"
    )
    if required_hashtags.strip():
        footer_parts.append(required_hashtags.strip())
    return f"{caption_body.strip()}\n\n{' '.join(footer_parts)}"


def _build_original_caption_preview(
    original_caption: str,
    username: str,
    top_comment: str = "",
    required_hashtags: str = "",
    is_instagram: bool = True,
) -> str:
    top_comment, _ = _decode_top_comment(top_comment)
    original_with_username = (original_caption or "").strip()
    cleaned_username = (username or "").strip().lstrip("@")
    if is_instagram and cleaned_username and original_with_username:
        original_with_username = f"@{cleaned_username}: {original_with_username}"
    original_preview = original_with_username
    if original_preview and (top_comment or "").strip():
        original_preview = f"{original_preview}\n\n{top_comment.strip()}"
    elif (top_comment or "").strip():
        original_preview = top_comment.strip()
    footer_username = username if is_instagram else ""
    return (
        _build_footered_caption(original_preview, footer_username, required_hashtags)
        if original_preview
        else ""
    )


def _ensure_required_hashtags_text(value: str, required_hashtags: str) -> str:
    caption = (value or "").strip()
    required = re.findall(r"#\w+", required_hashtags or "")
    if not caption or not required:
        return caption
    existing = {tag.lower() for tag in re.findall(r"#\w+", caption)}
    missing = [tag for tag in required if tag.lower() not in existing]
    if missing:
        caption = f"{caption}\n\n{' '.join(missing)}"
    return caption


def _caption_tab_value(
    generated: str,
    original_caption: str,
    username: str,
    top_comment: str,
    required_hashtags: str,
    is_instagram: bool,
) -> str:
    generated = (generated or "").strip()
    if generated:
        return _ensure_required_hashtags_text(generated, required_hashtags)
    return _build_original_caption_preview(
        original_caption,
        username,
        top_comment,
        required_hashtags,
        is_instagram=is_instagram,
    )


def _drive_image_url(drive_link: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", drive_link or "")
    if m:
        return f"https://drive.google.com/thumbnail?id={m.group(1)}&sz=w1200"
    parsed = urlparse(drive_link or "")
    file_id = parse_qs(parsed.query).get("id", [""])[0]
    if file_id:
        return f"https://drive.google.com/thumbnail?id={file_id}&sz=w1200"
    return ""


def _safe_image_url(raw_value: str) -> str:
    candidate = _drive_view_url(raw_value) or _drive_image_url(raw_value) or _cell_text(raw_value).strip()
    return candidate if _is_https_url(candidate) else ""


def _drive_view_url(drive_link: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", drive_link or "")
    if m:
        return f"https://drive.google.com/uc?export=view&id={m.group(1)}"
    parsed = urlparse(drive_link or "")
    file_id = parse_qs(parsed.query).get("id", [""])[0]
    if file_id:
        return f"https://drive.google.com/uc?export=view&id={file_id}"
    return ""


def _safe_browser_image_url(raw_value: str) -> str:
    candidate = _drive_view_url(raw_value) or _drive_image_url(raw_value) or _cell_text(raw_value).strip()
    return candidate if _is_https_url(candidate) else ""


@st.cache_data(show_spinner=False, ttl=900)
def _remote_image_usable(url: str) -> bool:
    candidate = (url or "").strip()
    if not candidate:
        return False
    if candidate.startswith("data:image/"):
        return True
    try:
        response = requests.get(candidate, timeout=8, stream=True, allow_redirects=True)
        content_type = (response.headers.get("content-type") or "").lower()
        return response.ok and content_type.startswith("image/")
    except Exception:
        return False


@st.cache_data(show_spinner=False)
def _missing_thumbnail_data_url() -> str:
    if not MISSING_THUMBNAIL_ASSET.exists():
        return ""
    encoded = base64.b64encode(MISSING_THUMBNAIL_ASSET.read_bytes()).decode("ascii")
    return f"data:image/jpeg;base64,{encoded}"


@st.cache_data(show_spinner=False)
def _missing_reel_thumbnail_data_url() -> str:
    if not MISSING_REEL_THUMBNAIL_ASSET.exists():
        return ""
    encoded = base64.b64encode(MISSING_REEL_THUMBNAIL_ASSET.read_bytes()).decode("ascii")
    return f"data:image/jpeg;base64,{encoded}"


def _row_fallback_image_path(media_type: str) -> str:
    if media_type == "reel" and MISSING_REEL_THUMBNAIL_ASSET.exists():
        return str(MISSING_REEL_THUMBNAIL_ASSET)
    if MISSING_THUMBNAIL_ASSET.exists():
        return str(MISSING_THUMBNAIL_ASSET)
    return ""


def _render_dark_media_placeholder(label: str = "") -> None:
    safe_label = html.escape((label or "").strip())
    placeholder_html = f"""
    <div style="
      width: 100%;
      min-height: 360px;
      background: #121722;
      border-radius: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #e2e8f0;
      font-size: 0.95rem;
      font-weight: 600;
      text-align: center;
      padding: 1rem;
      box-sizing: border-box;
    ">{safe_label}</div>
    """
    st.markdown(placeholder_html, unsafe_allow_html=True)


def _ffmpeg_filter_value(value: str) -> str:
    return (
        (value or "")
        .replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\\'")
        .replace(",", "\\,")
    )


def _preview_font_path(bold: bool = False) -> str:
    candidates = (
        [
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/System/Library/Fonts/Supplemental/Helvetica Bold.ttf",
            "/Library/Fonts/Arial Bold.ttf",
        ]
        if bold else
        [
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/System/Library/Fonts/Supplemental/Helvetica.ttf",
            "/Library/Fonts/Arial.ttf",
        ]
    )
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return ""


def _preview_ffmpeg_path() -> str:
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError("ffmpeg is not installed or not on PATH.")
    return ffmpeg_path


def _write_preview_text_file(tmp_dir: str, filename: str, value: str, wrap_width: int) -> str:
    path = os.path.join(tmp_dir, filename)
    wrapped_lines: list[str] = []
    for raw_line in (value or "").splitlines() or [""]:
        cleaned_line = raw_line.strip()
        if not cleaned_line:
            wrapped_lines.append("")
            continue
        wrapped_lines.extend(textwrap.wrap(cleaned_line, width=wrap_width, break_long_words=False) or [""])
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(wrapped_lines).strip())
    return path


def _download_preview_background(url: str, tmp_dir: str) -> str:
    if not _is_https_url(url):
        return ""
    output_path = os.path.join(tmp_dir, "preview_background.jpg")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    with open(output_path, "wb") as handle:
        handle.write(response.content)
    return output_path


def _preview_folder_base_name(username: str, media_link: str, row_num: int) -> tuple[str, str]:
    cleaned_username = re.sub(r"[^A-Za-z0-9._-]+", "_", (username or "").strip().lstrip("@")).strip("._-")
    if media_link:
        try:
            metadata = get_drive_file_metadata(media_link)
            filename = (metadata.get("name") or "").strip()
            stem = os.path.splitext(filename)[0]
            match = re.match(r"(?P<username>[A-Za-z0-9._-]+)_(?P<date>\d{6})_", stem)
            if match:
                matched_username = (match.group("username") or "").strip("._-")
                matched_date = (match.group("date") or "").strip()
                return f"{matched_username}_{matched_date}", filename
            date_match = re.search(r"(\d{6})", stem)
            if cleaned_username and date_match:
                return f"{cleaned_username}_{date_match.group(1)}", filename
            if stem:
                return stem, filename
            return filename or f"{cleaned_username or 'row'}_{row_num}", filename
        except Exception:
            pass
    fallback = f"{cleaned_username or 'row'}_{row_num}"
    return fallback, ""


def _ffprobe_path() -> str:
    ffprobe_path = shutil.which("ffprobe")
    if not ffprobe_path:
        raise RuntimeError("ffprobe is not installed or not on PATH.")
    return ffprobe_path


def _video_duration_seconds(path: str) -> float:
    command = [
        _ffprobe_path(),
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    duration_text = (result.stdout or "").strip()
    return float(duration_text) if duration_text else 0.0


def _refresh_row_thumbnail_from_video(row: dict, offset_seconds: float = 5.0) -> str:
    if update_thumbnail_link is None:
        raise RuntimeError("Thumbnail link updates are not supported in this build.")

    media_links = [link.strip() for link in (_cell_text(row.get("Media Drive Link")) or "").split(",") if link.strip()]
    if not media_links:
        raise ValueError("This row does not have a Drive video link yet.")

    media_link = media_links[0]
    metadata = get_drive_file_metadata(media_link)
    filename = (metadata.get("name") or "").strip()
    if not filename:
        raise ValueError("Could not determine the video filename from Drive.")

    row_num = row["row_number"]
    tmp_dir = tempfile.mkdtemp(prefix="workspace_thumb_")
    try:
        local_video_path = os.path.join(tmp_dir, filename or f"row_{row_num}.mp4")
        download_drive_file(media_link, local_video_path)

        duration_seconds = 0.0
        try:
            duration_seconds = _video_duration_seconds(local_video_path)
        except Exception:
            duration_seconds = 0.0
        capture_seconds = offset_seconds
        if duration_seconds > 0:
            capture_seconds = min(offset_seconds, max(0.0, duration_seconds - 0.25))

        screenshots_folder_id = get_or_create_subfolder(
            GOOGLE_DRIVE_FOLDER_ID,
            GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
        )
        stem, _ext = os.path.splitext(filename)
        screenshot_name = f"{stem}_thumb_{int(round(capture_seconds))}s.jpg"
        screenshot_path = os.path.join(tmp_dir, screenshot_name)
        command = [
            shutil.which("ffmpeg") or "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            f"{capture_seconds:.3f}",
            "-i",
            local_video_path,
            "-frames:v",
            "1",
            screenshot_path,
        ]
        subprocess.run(command, check=True)
        thumbnail_link = upload_to_drive(screenshot_path, screenshot_name, screenshots_folder_id)
        update_thumbnail_link(GOOGLE_SHEET_ID, row_num, thumbnail_link)
        return thumbnail_link
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _replace_row_thumbnail_from_upload(row: dict, uploaded_file) -> str:
    if update_thumbnail_link is None:
        raise RuntimeError("Thumbnail link updates are not supported in this build.")

    row_num = row["row_number"]
    screenshots_folder_id = get_or_create_subfolder(
        GOOGLE_DRIVE_FOLDER_ID,
        GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    )

    media_links = [link.strip() for link in (_cell_text(row.get("Media Drive Link")) or "").split(",") if link.strip()]
    screenshot_stem = f"row_{row_num}_thumb"
    if media_links:
        try:
            metadata = get_drive_file_metadata(media_links[0])
            filename = (metadata.get("name") or "").strip()
            if filename:
                screenshot_stem = f"{os.path.splitext(filename)[0]}_thumb"
        except Exception:
            pass

    source_name = getattr(uploaded_file, "name", "") or ""
    ext = os.path.splitext(source_name)[1].lower() or ".jpg"
    screenshot_name = f"{screenshot_stem}{ext}"

    tmp_dir = tempfile.mkdtemp(prefix="workspace_thumb_upload_")
    try:
        screenshot_path = os.path.join(tmp_dir, screenshot_name)
        with open(screenshot_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        thumbnail_link = upload_to_drive(screenshot_path, screenshot_name, screenshots_folder_id)
        update_thumbnail_link(GOOGLE_SHEET_ID, row_num, thumbnail_link)
        return thumbnail_link
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _render_slide_one_png(
    output_path: str,
    tmp_dir: str,
    handle_text: str,
    headline: str,
    background_url: str,
    headline_font_adjust_px: int = 0,
    background_y_adjust_px: int = 0,
) -> None:
    ffmpeg_path = _preview_ffmpeg_path()
    handle_file = _write_preview_text_file(tmp_dir, "slide1_handle.txt", (handle_text or "@UNKNOWN").upper(), 40)
    headline_file = _write_preview_text_file(tmp_dir, "slide1_headline.txt", headline, 24)
    bold_font = _preview_font_path(bold=True)
    regular_font = _preview_font_path(bold=False) or bold_font
    background_path = _download_preview_background(background_url, tmp_dir)
    handle_font_clause = f":fontfile='{_ffmpeg_filter_value(regular_font)}'" if regular_font else ""
    headline_font_clause = f":fontfile='{_ffmpeg_filter_value(bold_font)}'" if bold_font else ""
    font_size = max(64, round((96 + int(headline_font_adjust_px)) * PREVIEW_EXPORT_FONT_SCALE))
    y_offset = int(background_y_adjust_px)
    overlay_y = round(720 * PREVIEW_EXPORT_SCALE)
    overlay_h = round(900 * PREVIEW_EXPORT_SCALE)
    handle_y = round(1000 * PREVIEW_EXPORT_SCALE)
    headline_y = round(1080 * PREVIEW_EXPORT_SCALE)
    handle_font_size = max(26, round(30 * PREVIEW_EXPORT_FONT_SCALE))
    line_spacing = max(15, round(18 * PREVIEW_EXPORT_FONT_SCALE))
    y_shift = round(y_offset * PREVIEW_EXPORT_SCALE)
    y_pad_expr = f"(oh-ih)/2{y_shift:+d}"

    if background_path:
        input_args = ["-loop", "1", "-i", background_path]
        filter_graph = (
            f"[0:v]scale={PREVIEW_EXPORT_WIDTH_PX}:{PREVIEW_EXPORT_HEIGHT_PX}:force_original_aspect_ratio=decrease,"
            f"pad={PREVIEW_EXPORT_WIDTH_PX}:{PREVIEW_EXPORT_HEIGHT_PX}:(ow-iw)/2:{y_pad_expr}:color=0x121722,"
            f"drawbox=x=0:y={overlay_y}:w={PREVIEW_EXPORT_WIDTH_PX}:h={overlay_h}:color=0x121722@0.90:t=fill,"
            f"drawtext=textfile='{_ffmpeg_filter_value(handle_file)}'{handle_font_clause}:"
            f"fontcolor=white:fontsize={handle_font_size}:line_spacing=8:x=74:y={handle_y},"
            f"drawtext=textfile='{_ffmpeg_filter_value(headline_file)}'{headline_font_clause}:"
            f"fontcolor=white:fontsize={font_size}:line_spacing={line_spacing}:x=72:y={headline_y}"
        )
    else:
        input_args = ["-f", "lavfi", "-i", f"color=c=#121722:s={PREVIEW_EXPORT_WIDTH_PX}x{PREVIEW_EXPORT_HEIGHT_PX}:d=1"]
        filter_graph = (
            f"drawtext=textfile='{_ffmpeg_filter_value(handle_file)}'{handle_font_clause}:"
            f"fontcolor=white:fontsize={handle_font_size}:line_spacing=8:x=74:y={handle_y},"
            f"drawtext=textfile='{_ffmpeg_filter_value(headline_file)}'{headline_font_clause}:"
            f"fontcolor=white:fontsize={font_size}:line_spacing={line_spacing}:x=72:y={headline_y}"
        )

    command = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        *input_args,
        "-frames:v",
        "1",
        "-vf",
        filter_graph,
        output_path,
    ]
    subprocess.run(command, check=True)


def _render_text_slide_png(
    output_path: str,
    tmp_dir: str,
    body_text: str,
    font_adjust_px: int = 0,
    include_link_cta: bool = False,
) -> None:
    ffmpeg_path = _preview_ffmpeg_path()
    body_file = _write_preview_text_file(tmp_dir, os.path.basename(output_path) + ".txt", body_text, 26)
    cta_file = ""
    bold_font = _preview_font_path(bold=True)
    body_font_clause = f":fontfile='{_ffmpeg_filter_value(bold_font)}'" if bold_font else ""
    body_font_size = max(52, round((74 + int(font_adjust_px)) * PREVIEW_EXPORT_FONT_SCALE))
    body_y = round(78 * PREVIEW_EXPORT_SCALE)
    body_line_spacing = max(14, round(16 * PREVIEW_EXPORT_FONT_SCALE))

    filter_parts = [
        f"drawtext=textfile='{_ffmpeg_filter_value(body_file)}'{body_font_clause}:fontcolor=white:fontsize={body_font_size}:"
        f"line_spacing={body_line_spacing}:x=62:y={body_y}"
    ]
    if include_link_cta:
        cta_file = _write_preview_text_file(tmp_dir, "slide3_cta.txt", "Comment LINK for more", 28)
        cta_box_y = round(1380 * PREVIEW_EXPORT_SCALE)
        cta_box_h = round(88 * PREVIEW_EXPORT_SCALE)
        cta_text_y = round(1405 * PREVIEW_EXPORT_SCALE)
        cta_font_size = max(32, round(36 * PREVIEW_EXPORT_FONT_SCALE))
        filter_parts.extend(
            [
                f"drawbox=x=62:y={cta_box_y}:w=470:h={cta_box_h}:color=white@1.0:t=fill",
                f"drawtext=textfile='{_ffmpeg_filter_value(cta_file)}'{body_font_clause}:fontcolor=#121722:fontsize={cta_font_size}:"
                f"line_spacing=8:x=90:y={cta_text_y}",
            ]
        )

    command = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"color=c=#121722:s={PREVIEW_EXPORT_WIDTH_PX}x{PREVIEW_EXPORT_HEIGHT_PX}:d=1",
        "-frames:v",
        "1",
        "-vf",
        ",".join(filter_parts),
        output_path,
    ]
    subprocess.run(command, check=True)


def _upload_preview_pngs(
    row_num: int,
    username: str,
    handle_text: str,
    slide_text1: str,
    slide_text2: str,
    slide_text3: str,
    background_url: str,
    media_link: str = "",
    preview_folder_id: str = "",
    folder_base_name: str = "",
    source_filename: str = "",
    include_source_video: bool = True,
    slide_one_font_adjust: int = 0,
    slide_one_background_adjust: int = 0,
    slide_two_font_adjust: int = 0,
    slide_three_font_adjust: int = 0,
) -> list[dict[str, str]]:
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not configured.")

    tmp_dir = tempfile.mkdtemp(prefix="workspace_previews_")
    uploaded: list[dict[str, str]] = []
    if not preview_folder_id or not folder_base_name:
        preview_folder_id, folder_base_name, resolved_source_filename = _ensure_preview_folder(
            row_num,
            username,
            handle_text,
            media_link,
        )
        if not source_filename:
            source_filename = resolved_source_filename
    safe_handle = (handle_text or username or f"row_{row_num}").strip()

    try:
        slides_to_render: list[tuple[str, callable, dict]] = []
        if (slide_text1 or "").strip():
            slides_to_render.append(
                (
                    "slide1",
                    _render_slide_one_png,
                    {
                        "handle_text": safe_handle,
                        "headline": slide_text1,
                        "background_url": background_url,
                        "headline_font_adjust_px": slide_one_font_adjust,
                        "background_y_adjust_px": slide_one_background_adjust,
                    },
                )
            )
        if (slide_text2 or "").strip():
            slides_to_render.append(
                (
                    "slide2",
                    _render_text_slide_png,
                    {
                        "body_text": slide_text2,
                        "font_adjust_px": slide_two_font_adjust,
                        "include_link_cta": False,
                    },
                )
            )
        if (slide_text3 or "").strip():
            slides_to_render.append(
                (
                    "slide3",
                    _render_text_slide_png,
                    {
                        "body_text": slide_text3,
                        "font_adjust_px": slide_three_font_adjust,
                        "include_link_cta": True,
                    },
                )
            )
        if not slides_to_render:
            raise ValueError("No slide preview text is available to export.")

        if media_link and include_source_video:
            copied_media_link = _copy_source_video_into_preview_folder(media_link, preview_folder_id, source_filename)
            uploaded.append(
                {
                    "label": "Source video",
                    "link": copied_media_link,
                }
            )

        for suffix, renderer, kwargs in slides_to_render:
            output_filename = f"{folder_base_name}_{suffix}.png"
            output_path = os.path.join(tmp_dir, output_filename)
            renderer(output_path=output_path, tmp_dir=tmp_dir, **kwargs)
            uploaded.append(
                {
                    "label": suffix.replace("slide", "Slide "),
                    "link": upload_to_drive(output_path, output_filename, preview_folder_id, overwrite=True),
                }
            )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return uploaded


def _segment_name(index: int) -> str:
    words = [
        "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
        "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
        "eighteen", "nineteen", "twenty", "twenty_one", "twenty_two", "twenty_three",
        "twenty_four", "twenty_five", "twenty_six", "twenty_seven", "twenty_eight",
        "twenty_nine", "thirty", "thirty_one", "thirty_two", "thirty_three",
        "thirty_four", "thirty_five", "thirty_six", "thirty_seven", "thirty_eight",
        "thirty_nine", "forty", "forty_one", "forty_two", "forty_three", "forty_four",
        "forty_five", "forty_six", "forty_seven", "forty_eight", "forty_nine", "fifty",
        "fifty_one", "fifty_two", "fifty_three", "fifty_four", "fifty_five", "fifty_six",
        "fifty_seven", "fifty_eight", "fifty_nine", "sixty",
    ]
    if 0 <= index < len(words):
        return words[index]
    return f"{index + 1:02d}"


def _ensure_preview_folder(row_num: int, username: str, handle_text: str, media_link: str) -> tuple[str, str, str]:
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not configured.")
    preview_root_folder_id = get_or_create_subfolder(GOOGLE_DRIVE_FOLDER_ID, PREVIEW_UPLOAD_SUBFOLDER)
    folder_base_name, source_filename = _preview_folder_base_name(username or handle_text, media_link, row_num)
    preview_folder_id = get_or_create_subfolder(preview_root_folder_id, folder_base_name)
    return preview_folder_id, folder_base_name, source_filename


def _copy_source_video_into_preview_folder(media_link: str, preview_folder_id: str, source_filename: str) -> str:
    if not media_link:
        return ""
    return copy_drive_file_to_folder(media_link, preview_folder_id, source_filename)


def _split_video_to_folder(local_video_path: str, output_dir: str) -> list[str]:
    crop_width = "if(gte(iw/ih\\,4/5)\\,trunc(ih*(4/5)/2)*2\\,iw)"
    crop_height = "if(gte(iw/ih\\,4/5)\\,ih\\,trunc(iw/(4/5)/2)*2)"
    video_filter = f"crop={crop_width}:{crop_height}:(iw-ow)/2:(ih-oh)/2,scale=trunc(iw/2)*2:trunc(ih/2)*2"
    duration = _video_duration_seconds(local_video_path)
    if duration <= 0:
        raise RuntimeError("Could not determine video duration for splitting.")

    ffmpeg_path = shutil.which("ffmpeg") or "ffmpeg"
    outputs: list[str] = []
    start_seconds = 0.0
    segment_index = 0
    while start_seconds < duration - 0.01:
        clip_duration = min(60.0, duration - start_seconds)
        output_path = os.path.join(output_dir, f"{_segment_name(segment_index)}.mp4")
        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            local_video_path,
            "-ss",
            f"{start_seconds:.3f}",
            "-t",
            f"{clip_duration:.3f}",
            "-vf",
            video_filter,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            output_path,
        ]
        subprocess.run(command, check=True)
        outputs.append(output_path)
        start_seconds += 60.0
        segment_index += 1
    return outputs


def _upload_split_videos(media_link: str, preview_folder_id: str) -> list[dict[str, str]]:
    if not media_link:
        return []
    metadata = get_drive_file_metadata(media_link)
    filename = (metadata.get("name") or "").strip()
    if not filename:
        raise ValueError("Could not determine the source video filename from Drive.")

    tmp_dir = tempfile.mkdtemp(prefix="workspace_splits_")
    try:
        local_video_path = os.path.join(tmp_dir, filename)
        download_drive_file(media_link, local_video_path)
        split_dir = os.path.join(tmp_dir, "segments")
        os.makedirs(split_dir, exist_ok=True)
        segment_paths = _split_video_to_folder(local_video_path, split_dir)
        uploaded: list[dict[str, str]] = []
        for segment_path in segment_paths:
            segment_filename = os.path.basename(segment_path)
            uploaded.append(
                {
                    "label": f"Split {os.path.splitext(segment_filename)[0]}",
                    "link": upload_to_drive(segment_path, segment_filename, preview_folder_id),
                }
            )
        return uploaded
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _is_https_url(value: str) -> bool:
    parsed = urlparse((value or "").strip())
    return parsed.scheme == "https" and bool(parsed.netloc)


def _clean_public_url(link: str) -> str:
    parsed = urlparse((link or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return (link or "").strip()
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _build_link_cta(link: str) -> str:
    return f"Comment LINK (on instagram) and we will DM you the link to {_clean_public_url(link)}"


def _build_read_cta(link: str) -> str:
    return f"Comment LINK (on instagram) and we will DM you the link to {_clean_public_url(link)}"


def _build_watch_cta(username: str, link: str) -> str:
    cleaned_username = (username or "").strip().lstrip("@")
    cleaned_link = _clean_public_url(link)
    destination = f"@{cleaned_username} {cleaned_link}" if cleaned_username else cleaned_link
    return f"Comment LINK (on instagram) and we will DM you the link to {destination}"


def _append_top_comment(existing: str, addition: str) -> str:
    existing = (existing or "").strip()
    addition = (addition or "").strip()
    if not existing:
        return addition
    if not addition or addition in existing.split("\n\n"):
        return existing
    return f"{existing}\n\n{addition}"


def _encode_top_comment(value: str, pinned: bool = False) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    return f"{PINNED_TOP_COMMENT_PREFIX}{cleaned}" if pinned else cleaned


def _decode_top_comment(value: str) -> tuple[str, bool]:
    cleaned = (value or "").strip()
    if cleaned.startswith(PINNED_TOP_COMMENT_PREFIX):
        return cleaned[len(PINNED_TOP_COMMENT_PREFIX):].strip(), True
    return cleaned, False


def _close_workspace_menu(row: dict) -> None:
    nonce_key = _workspace_key(row, "menu_nonce")
    st.session_state[nonce_key] = st.session_state.get(nonce_key, 0) + 1
    st.session_state[_workspace_key(row, "link_editor_open")] = False
    st.session_state.pop("workspace_link_dialog_row", None)


def _close_workspace_link_dialog(row: dict) -> None:
    st.session_state.pop("workspace_link_dialog_row", None)
    st.session_state.pop(_workspace_key(row, "link_source"), None)
    st.session_state.pop(_workspace_key(row, "link_url"), None)
    st.session_state.pop(_workspace_key(row, "link_comment"), None)


def _close_workspace_thumbnail_dialog(row: dict) -> None:
    st.session_state.pop("workspace_thumbnail_dialog_row", None)
    st.session_state.pop(_workspace_key(row, "thumbnail_upload"), None)


def _apply_top_comment_to_caption(
    row: dict,
    row_num: int,
    speaker_name: str,
    top_comment: str,
) -> None:
    current_context = st.session_state.get(_workspace_key(row, "context"), row.get("Caption Context", "")).strip()
    current_speaker = st.session_state.get(_workspace_speaker_key(row), speaker_name).strip()
    current_hashtags = st.session_state.get(_workspace_key(row, "hashtags"), row.get("Required Hashtags", "")).strip()
    updated_row = dict(row)
    updated_row["Caption Context"] = current_context
    updated_row["Speaker Name"] = current_speaker
    updated_row["Required Hashtags"] = current_hashtags
    updated_row["Top Comment"] = top_comment
    current_status = (row.get("Status") or "").strip() or "done"
    existing_caption = (row.get("Generated Caption") or "").strip()
    previous_top_comment = (row.get("Top Comment") or "").strip()
    clean_top_comment, pin_top_comment = _decode_top_comment(top_comment)
    if existing_caption:
        caption = existing_caption
        for removable in (previous_top_comment, top_comment):
            removable_text, _ = _decode_top_comment(removable)
            if removable_text:
                caption = _strip_top_comment_paragraphs(caption, removable_text)

        media_type = (row.get("Media Type") or "").strip().lower()
        if media_type != "article" and "\n\n--\n\n" in caption:
            before_divider, after_divider = caption.split("\n\n--\n\n", 1)
            before_divider = before_divider.strip()
            after_divider = after_divider.strip()
            if clean_top_comment:
                if pin_top_comment:
                    before_divider = f"{clean_top_comment}\n\n{before_divider}".strip()
                else:
                    before_divider = f"{before_divider}\n\n{clean_top_comment}".strip()
            caption = f"{before_divider}\n\n--\n\n{after_divider}".strip()
        elif clean_top_comment:
            footer_text = DEFAULT_POST_FOOTER.strip()
            if footer_text and footer_text in caption:
                body, _, trailing = caption.rpartition(footer_text)
                body = body.strip()
                trailing = trailing.strip()
                if pin_top_comment:
                    body = f"{clean_top_comment}\n\n{body}".strip()
                else:
                    body = f"{body}\n\n{clean_top_comment}".strip()
                caption = f"{body}\n\n{footer_text}{trailing}".strip()
            else:
                if pin_top_comment:
                    caption = f"{clean_top_comment}\n\n{caption}".strip()
                else:
                    caption = f"{caption}\n\n{clean_top_comment}".strip()
        caption = _ensure_required_hashtags_text(caption, current_hashtags)
    else:
        caption = ""

    if caption and update_caption_and_metadata is not None:
        update_caption_and_metadata(
            GOOGLE_SHEET_ID,
            row_num,
            caption,
            current_status,
            current_context,
            current_speaker,
            current_hashtags,
            top_comment,
            "",
        )
    else:
        update_metadata(
            GOOGLE_SHEET_ID,
            row_num,
            current_context,
            current_speaker,
            current_hashtags,
            top_comment,
            "",
        )
        if caption:
            update_caption(GOOGLE_SHEET_ID, row_num, caption, current_status)
    st.session_state[_workspace_key(row, "top")] = top_comment


def _current_row_caption_inputs(row: dict) -> dict:
    current_context = st.session_state.get(
        _workspace_key(row, "context"),
        row.get("Caption Context", ""),
    ).strip()
    current_speaker = st.session_state.get(
        _workspace_speaker_key(row),
        row.get("Speaker Name", ""),
    ).strip()
    current_hashtags = st.session_state.get(
        _workspace_key(row, "hashtags"),
        row.get("Required Hashtags", ""),
    ).strip()
    current_top = st.session_state.get(
        _workspace_key(row, "top"),
        row.get("Top Comment", ""),
    ).strip()
    current_top, _ = _decode_top_comment(current_top)
    url = (row.get("Instagram URL") or "").strip()
    current_username = (row.get("Source Username") or "").strip()

    if not current_top and _is_instagram_url(url):
        current_top = _build_watch_cta(current_username or current_speaker, url)
    elif not current_top and _is_article_url(url):
        current_top = _build_read_cta(url)

    return {
        "Caption Context": current_context,
        "Speaker Name": current_speaker,
        "Required Hashtags": current_hashtags,
        "Top Comment": current_top,
    }


def _save_all_workspace_speaker_names(rows: list[dict]) -> int:
    intended_updates: dict[int, str] = {}
    for row in rows:
        current_inputs = _current_row_caption_inputs(row)
        current_speaker = current_inputs["Speaker Name"]
        saved_speaker = _cell_text(row.get("Speaker Name")).strip()
        if current_speaker == saved_speaker:
            continue
        intended_updates[row["row_number"]] = current_speaker

    if intended_updates:
        if update_speaker_names_batch is None:
            raise RuntimeError("Batch speaker-name updates are not supported in this build.")
        update_speaker_names_batch(GOOGLE_SHEET_ID, intended_updates)
        for row in rows:
            row_number = row["row_number"]
            if row_number in intended_updates:
                st.session_state[_workspace_speaker_key(row)] = intended_updates[row_number]

    if intended_updates:
        refreshed_rows = {
            refreshed["row_number"]: _cell_text(refreshed.get("Speaker Name")).strip()
            for refreshed in get_all_rows(GOOGLE_SHEET_ID)
            if refreshed.get("row_number") in intended_updates
        }
        mismatched = [
            f"row {row_number}"
            for row_number, expected in intended_updates.items()
            if refreshed_rows.get(row_number, "") != expected
        ]
        if mismatched:
            raise RuntimeError(
                "Speaker name update did not persist for " + ", ".join(mismatched[:5]) + "."
            )
    return len(intended_updates)


def _dirty_workspace_speaker_rows(rows: list[dict]) -> list[dict]:
    return [
        row for row in rows
        if _cell_text(st.session_state.get(_workspace_speaker_key(row), row.get("Speaker Name", ""))).strip()
        != _cell_text(row.get("Speaker Name")).strip()
    ]


def _handle_update_all_workspace_speaker_names(rows: list[dict], rerun_tab: str = "Edit") -> None:
    try:
        updated_count = _save_all_workspace_speaker_names(rows)
    except Exception as e:
        st.session_state["workspace_error"] = f"Could not save names: {describe_error(e)}"
    else:
        st.session_state["workspace_success"] = (
            f"Updated {updated_count} speaker name(s)."
            if updated_count
            else "No name changes to save."
        )
    _rerun_workspace(rerun_tab)


def _fundraising_preset_map() -> dict[str, str]:
    presets = get_fundraising_links(GOOGLE_SHEET_ID)
    mapping: dict[str, str] = {"Custom": ""}
    for preset in presets:
        label = (preset.get("label") or "").strip()
        top_comment = (preset.get("link") or "").strip()
        if label and top_comment and label not in mapping:
            mapping[label] = top_comment
    return mapping


@st.dialog("Add link")
def _render_workspace_link_dialog(row: dict) -> None:
    row_num = row["row_number"]
    speaker_name = (row.get("Speaker Name") or "").strip()
    fundraising_presets = _fundraising_preset_map()
    source_key = _workspace_key(row, "link_source")
    link_url_key = _workspace_key(row, "link_url")
    link_comment_key = _workspace_key(row, "link_comment")

    previous_source = st.session_state.get(source_key, "Custom")
    selected_source = st.selectbox(
        "Type",
        options=list(fundraising_presets.keys()),
        key=source_key,
    )
    selected_top_comment = fundraising_presets.get(selected_source, "").strip()

    if selected_source != previous_source:
        if selected_source == "Custom":
            st.session_state.pop(link_comment_key, None)
        else:
            st.session_state[link_comment_key] = selected_top_comment

    if selected_source == "Custom":
        st.text_input(
            "Link",
            key=link_url_key,
            placeholder="https://example.com",
        )
    else:
        if link_comment_key not in st.session_state:
            st.session_state[link_comment_key] = selected_top_comment
        st.text_area(
            "Top comment",
            key=link_comment_key,
            height=180,
        )

    if st.button("Add", key=f"workspace_link_add_{row_num}", type="primary", width="stretch"):
        full_link = st.session_state.get(link_url_key, "").strip()
        if selected_source == "Custom" and not _is_https_url(full_link):
            st.session_state["workspace_error"] = f"Row {row_num}: link must start with https://"
            _rerun_workspace("Edit")

        addition = (
            _build_link_cta(full_link)
            if selected_source == "Custom"
            else st.session_state.get(link_comment_key, selected_top_comment).strip()
        )
        top_comment = _encode_top_comment(addition, pinned=(selected_source != "Custom"))
        try:
            _apply_top_comment_to_caption(row, row_num, speaker_name, top_comment)
        except Exception as e:
            st.session_state["workspace_error"] = f"Row {row_num}: could not save link CTA - {describe_error(e)}"
        else:
            st.session_state["workspace_success"] = f"Row {row_num}: link CTA saved to generated caption."
        _close_workspace_link_dialog(row)
        _rerun_workspace("Edit")

    if st.button("Cancel", key=f"workspace_link_cancel_{row_num}", width="stretch"):
        _close_workspace_link_dialog(row)
        _rerun_workspace("Edit")


@st.dialog("Update screenshot")
def _render_workspace_thumbnail_dialog(row: dict) -> None:
    row_num = row["row_number"]
    url = _cell_text(row.get("Instagram URL")).strip()
    has_media = bool(_cell_text(row.get("Media Drive Link")).strip())

    uploaded_thumbnail = st.file_uploader(
        "Replace screenshot",
        type=["png", "jpg", "jpeg", "webp", "heic", "heif"],
        accept_multiple_files=False,
        key=_workspace_key(row, "thumbnail_upload"),
        help="On iPhone this opens your photo library/files chooser. On desktop it opens the file picker.",
    )
    if uploaded_thumbnail is not None and st.button(
        "Use uploaded screenshot",
        key=f"workspace_thumbnail_upload_apply_{row_num}",
        type="primary",
        width="stretch",
    ):
        try:
            _replace_row_thumbnail_from_upload(row, uploaded_thumbnail)
        except Exception as e:
            st.session_state["workspace_error"] = f"Row {row_num}: could not replace screenshot - {describe_error(e)}"
        else:
            st.session_state["workspace_success"] = f"Row {row_num}: screenshot replaced from uploaded image."
        _close_workspace_thumbnail_dialog(row)
        _rerun_workspace("Edit")

    if _is_reel_url(url) and has_media and st.button(
        "Update screenshot (+5s)",
        key=f"workspace_thumbnail_refresh_5s_{row_num}",
        width="stretch",
        help="Replace the current screenshot with a frame taken about 5 seconds into the video.",
    ):
        _close_workspace_thumbnail_dialog(row)
        _queue_workspace_action(row_num, "refresh_thumbnail_5s")
        _rerun_workspace("Edit")

    if st.button("Cancel", key=f"workspace_thumbnail_cancel_{row_num}", width="stretch"):
        _close_workspace_thumbnail_dialog(row)
        _rerun_workspace("Edit")


def _copy_block(label: str, value: str, key: str, empty_text: str = "(none)") -> None:
    st.code(value or empty_text, language=None)
    st.markdown(
        f'<div class="workspace-plain-copy-text">{html.escape(value or empty_text)}</div>',
        unsafe_allow_html=True,
    )


def _one_line_copy_preview(label: str, value: str, key: str, empty_text: str = "(none)") -> None:
    display_text = (value or empty_text).replace("\n", " ")
    escaped_label = html.escape(label)
    clipboard_text = json.dumps(value or "")
    component_html = f"""
    <div style="margin-top:0.25rem;" id="{html.escape(key)}">
      <div style="
        position: relative;
        min-height: 2.1rem;
        height: 2.1rem;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
        border: 1px solid rgba(15,23,42,0.08);
        border-radius: 16px;
        background: #f8fafc;
        padding: 0.45rem 3.1rem 0.45rem 0.8rem;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 0.88rem;
        line-height: 1.15rem;
        color: #0f172a;
      ">{html.escape(display_text)}</div>
      <button
        onclick='navigator.clipboard.writeText({clipboard_text})'
        aria-label='Copy {escaped_label}'
        style="
          position: absolute;
          margin-top: -2.55rem;
          right: 0.55rem;
          width: 2.35rem;
          height: 2.35rem;
          border: 1px solid rgba(15,23,42,0.08);
          border-radius: 16px;
          background: white;
          color: #0f172a;
          font-size: 1rem;
          line-height: 1;
          cursor: pointer;
          box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
        "
      >⧉</button>
    </div>
    """
    st.html(component_html)


def _multiline_copy_preview(label: str, value: str, key: str, empty_text: str = "(none)") -> None:
    display_text = value or empty_text
    escaped_label = html.escape(label)
    escaped_key = html.escape(key)
    clipboard_text = json.dumps(value or "")
    component_html = f"""
    <div style="margin-top:0.5rem;" id="{escaped_key}">
      <div style="
        position: relative;
        border: 1px solid rgba(15,23,42,0.08);
        border-radius: 18px;
        background: #f8fafc;
        padding: 0.9rem 3.3rem 0.9rem 1rem;
        box-shadow: 0 8px 20px rgba(15, 23, 42, 0.04);
      ">
        <pre style="
          margin: 0;
          white-space: pre-wrap;
          word-break: break-word;
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
          font-size: 0.88rem;
          line-height: 1.35rem;
          color: #0f172a;
          max-height: 18rem;
          overflow: auto;
        ">{html.escape(display_text)}</pre>
        <button
          onclick='navigator.clipboard.writeText({clipboard_text})'
          aria-label='Copy {escaped_label}'
          style="
            position: absolute;
            top: 0.75rem;
            right: 0.75rem;
            width: 2.35rem;
            height: 2.35rem;
            border: 1px solid rgba(15,23,42,0.08);
            border-radius: 16px;
            background: white;
            color: #0f172a;
            font-size: 1rem;
            line-height: 1;
            cursor: pointer;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
          "
        >⧉</button>
      </div>
    </div>
    """
    st.html(component_html)


def _tab_copy_preview(value: str, show_plain_text: bool = True, key: str = "") -> None:
    st.code(value or "(none)", language=None)
    if show_plain_text:
        st.markdown(
            f'<div class="workspace-plain-copy-text">{html.escape(value or "(none)")}</div>',
            unsafe_allow_html=True,
        )
    else:
        preview_key = key or f"workspace_multiline_copy_{hashlib.md5((value or '').encode('utf-8')).hexdigest()[:12]}"
        _multiline_copy_preview("copy text", value or "(none)", preview_key)


def _render_slide_one_preview(
    handle: str,
    headline: str,
    background_url: str = "",
    headline_font_adjust_px: int = 0,
    background_y_adjust_px: int = 0,
) -> None:
    headline_text = (headline or "").strip()
    if not headline_text:
        return

    safe_handle = html.escape((handle or "").strip() or "@UNKNOWN")
    safe_headline = html.escape(headline_text)
    safe_background = html.escape(background_url.strip()) if background_url else ""
    headline_clamp_css = (
        f"clamp(calc(1.55rem + {headline_font_adjust_px}px), "
        f"calc(4vw + {headline_font_adjust_px}px), "
        f"calc(2.8rem + {headline_font_adjust_px}px))"
    )
    background_position = f"center {background_y_adjust_px}px"
    background_css = (
        f"background-image: url('{safe_background}'); background-size: cover; background-position: {background_position};"
        if safe_background
        else "background: #121722;"
    )
    preview_html = f"""
    <div style="margin-top: 1rem;">
      <style>
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600&display=swap');
        .workspace-preview-shell {{
          width: 100%;
          max-width: {PREVIEW_CANVAS_WIDTH_PX}px;
          margin: 0 auto;
        }}
        .workspace-preview-card {{
          width: 100%;
          border-radius: 0;
          overflow: hidden;
          box-shadow: 0 24px 80px rgba(15, 23, 42, 0.22);
        }}
        .workspace-preview-canvas {{
          width: 100%;
          aspect-ratio: 4 / 5.5;
        }}
        @media (max-width: 768px) {{
          .workspace-preview-shell {{
            width: 100vw;
            max-width: none;
            margin-left: calc(50% - 50vw);
            margin-right: calc(50% - 50vw);
          }}
        }}
        .workspace-slide-preview-copy {{
          font-family: {PREVIEW_SLIDE_FONT_FAMILY} !important;
        }}
        .workspace-slide-preview-handle {{
          font-family: {PREVIEW_SLIDE_FONT_FAMILY} !important;
          font-weight: 400 !important;
        }}
        .workspace-slide-preview-headline {{
          font-family: {PREVIEW_SLIDE_FONT_FAMILY} !important;
          font-weight: {PREVIEW_SLIDE_FONT_WEIGHT} !important;
        }}
      </style>
      <div style="font-size: 0.82rem; font-weight: 500; color: #475569; margin-bottom: 0.5rem;">
        Slide 1 preview
      </div>
      <div class="workspace-preview-shell">
        <div class="workspace-preview-card" style="background: #0f172a;">
          <div class="workspace-preview-canvas" style="
          position: relative;
          width: 100%;
          display: flex;
          flex-direction: column;
          justify-content: flex-end;
          {background_css}
        ">
          <div class="workspace-slide-preview-copy" style="
            display: flex;
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            gap: 0.8rem;
            align-self: stretch;
            padding: 78px 24px 24px 24px;
            color: white;
            font-family: {PREVIEW_SLIDE_FONT_FAMILY};
            background: linear-gradient(180deg, rgba(18, 23, 34, 0) 0%, rgba(18, 23, 34, 0.9) 36.34%, #121722 80.76%);
          ">
            <div class="workspace-slide-preview-handle" style="
              font-size: clamp(0.7rem, 1.15vw, 1rem);
              letter-spacing: 0.3em;
              line-height: 1.38;
              text-transform: uppercase;
              white-space: nowrap;
            ">{safe_handle}</div>
            <div class="workspace-slide-preview-headline" style="
              font-size: {headline_clamp_css};
              line-height: {PREVIEW_SLIDE_LINE_HEIGHT};
              letter-spacing: {PREVIEW_SLIDE_LETTER_SPACING};
            ">{safe_headline}</div>
          </div>
        </div>
      </div>
    </div>
    """
    st.html(preview_html)


def _render_text_slide_preview(
    slide_number: int,
    body_text: str,
    body_font_adjust_px: int = 0,
    include_link_cta: bool = False,
) -> None:
    content_text = (body_text or "").strip()
    if not content_text:
        return

    safe_body = html.escape(content_text)
    if slide_number == 3:
        body_clamp_css = (
            f"clamp(calc({SLIDE_THREE_FONT_MIN_REM}rem + {body_font_adjust_px}px), "
            f"calc({SLIDE_THREE_FONT_VW}vw + {body_font_adjust_px}px), "
            f"calc({SLIDE_THREE_FONT_MAX_REM}rem + {body_font_adjust_px}px))"
        )
    else:
        body_clamp_css = (
            f"clamp(calc({SLIDE_TWO_FONT_MIN_REM}rem + {body_font_adjust_px}px), "
            f"calc({SLIDE_TWO_FONT_VW}vw + {body_font_adjust_px}px), "
            f"calc({SLIDE_TWO_FONT_MAX_REM}rem + {body_font_adjust_px}px))"
        )
    cta_html = ""
    if include_link_cta:
        cta_html = """
            <div style="
              display: inline-flex;
              align-items: center;
              justify-content: center;
              margin-top: 1.2rem;
              padding: 0.4rem 0.6rem;
              border-radius: 2px;
              background: #ffffff;
              color: #121722;
              font-size: clamp(0.95rem, 2vw, 1.15rem);
              font-weight: 600;
              line-height: 1.1;
            ">Comment LINK for more</div>
        """

    preview_html = f"""
    <div style="margin-top: 1rem;">
      <style>
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700&display=swap');
        .workspace-preview-shell {{
          width: 100%;
          max-width: {PREVIEW_CANVAS_WIDTH_PX}px;
          margin: 0 auto;
        }}
        .workspace-preview-card {{
          width: 100%;
          border-radius: 0;
          overflow: hidden;
          box-shadow: 0 24px 80px rgba(15, 23, 42, 0.22);
        }}
        .workspace-preview-canvas {{
          width: 100%;
          aspect-ratio: 4 / 5.5;
        }}
        @media (max-width: 768px) {{
          .workspace-preview-shell {{
            width: 100vw;
            max-width: none;
            margin-left: calc(50% - 50vw);
            margin-right: calc(50% - 50vw);
          }}
        }}
        .workspace-text-slide-preview-copy {{
          font-family: {PREVIEW_SLIDE_FONT_FAMILY} !important;
          font-weight: {PREVIEW_SLIDE_FONT_WEIGHT} !important;
        }}
      </style>
      <div style="font-size: 0.82rem; font-weight: 600; color: #475569; margin-bottom: 0.5rem;">
        Slide {slide_number} preview
      </div>
      <div class="workspace-preview-shell">
        <div class="workspace-preview-card" style="background: #121722;">
          <div class="workspace-preview-canvas workspace-text-slide-preview-copy" style="
          padding: 28px 26px 28px 26px;
          color: #ffffff;
          background: #121722;
          font-size: {body_clamp_css};
          line-height: {PREVIEW_SLIDE_LINE_HEIGHT};
          letter-spacing: {PREVIEW_SLIDE_LETTER_SPACING};
          overflow: hidden;
          box-sizing: border-box;
        ">
          <div>{safe_body}</div>
          {cta_html}
        </div>
      </div>
    </div>
    """
    st.html(preview_html)


def _build_single_row_chatgpt_prompt(row: dict) -> str:
    return _build_chatgpt_handoff_prompt([row])


def _render_workspace_preview_control_bar(
    control_id: str,
    font_adjust_key: str,
    current_font_adjust: int,
    background_adjust_key: str | None = None,
    current_background_adjust: int = 0,
) -> None:
    with st.container():
        st.markdown('<div class="workspace-preview-controls-anchor"></div>', unsafe_allow_html=True)
        controls = [("A-", "font_down"), ("A+", "font_up")]
        if background_adjust_key is not None:
            controls.extend([("Up", "bg_up"), ("Down", "bg_down")])
        columns = st.columns(len(controls), gap="small")
        for column, (label, action) in zip(columns, controls):
            with column:
                if st.button(label, key=f"workspace_preview_{control_id}_{action}", width="stretch"):
                    if action == "font_down":
                        st.session_state[font_adjust_key] = max(-16, current_font_adjust - 2)
                    elif action == "font_up":
                        st.session_state[font_adjust_key] = min(24, current_font_adjust + 2)
                    elif action == "bg_up" and background_adjust_key is not None:
                        st.session_state[background_adjust_key] = max(-200, current_background_adjust - 48)
                    elif action == "bg_down" and background_adjust_key is not None:
                        st.session_state[background_adjust_key] = min(200, current_background_adjust + 48)
                    _rerun_workspace("Edit")


def _copy_tabs(
    row_num: int,
    generated: str,
    original_caption: str,
    transcript: str,
    username: str,
    speaker_name: str,
    top_comment: str,
    required_hashtags: str,
    media_link: str = "",
    media_type: str = "",
    source_url: str = "",
    is_instagram: bool = True,
    slide_text1: str = "",
    slide_text2: str = "",
    slide_text3: str = "",
    prompt_row: dict | None = None,
    thumbnail_link: str = "",
) -> None:
    tab_labels = ["Caption", "Original"]
    tab_labels.append("Slides")
    media_links = [link.strip() for link in (media_link or "").split(",") if link.strip()]
    if media_links:
        tab_labels.append("Media")
    content_tab_key = f"workspace_row_content_tab_{row_num}"
    current_content_tab = st.session_state.get(content_tab_key, "Caption")
    if current_content_tab not in tab_labels:
        current_content_tab = "Caption"
        st.session_state[content_tab_key] = current_content_tab
    selected_content_tab = st.segmented_control(
        "Content",
        tab_labels,
        default=current_content_tab,
        key=content_tab_key,
        label_visibility="collapsed",
        width="stretch",
    ) or "Caption"
    original_preview = _build_original_caption_preview(
        original_caption,
        username,
        top_comment,
        required_hashtags,
        is_instagram=is_instagram,
    )
    if selected_content_tab == "Caption":
        _tab_copy_preview(
            _caption_tab_value(
                generated,
                original_caption,
                username,
                top_comment,
                required_hashtags,
                is_instagram,
            )
        )
    elif selected_content_tab == "Original":
        _tab_copy_preview(original_preview)
        if is_instagram:
            st.caption("Transcript")
            _tab_copy_preview(transcript)
    elif selected_content_tab == "Slides":
        prompt_key = f"workspace_row_slides_prompt_{row_num}"
        slide_one_font_adjust_key = f"workspace_slide_preview_font_adjust_{row_num}"
        slide_one_background_adjust_key = f"workspace_slide_preview_background_adjust_{row_num}"
        slide_two_font_adjust_key = f"workspace_slide_two_preview_font_adjust_{row_num}"
        slide_three_font_adjust_key = f"workspace_slide_three_preview_font_adjust_{row_num}"
        preview_links_key = f"workspace_preview_upload_links_{row_num}"
        current_slide_one_font_adjust = int(st.session_state.get(slide_one_font_adjust_key, 0) or 0)
        current_slide_one_background_adjust = int(st.session_state.get(slide_one_background_adjust_key, 0) or 0)
        current_slide_two_font_adjust = int(st.session_state.get(slide_two_font_adjust_key, 0) or 0)
        current_slide_three_font_adjust = int(st.session_state.get(slide_three_font_adjust_key, 0) or 0)
        current_speaker_name = _cell_text(
            st.session_state.get(f"workspace_speaker_row_{row_num}", speaker_name)
        ).strip()
        slide_handle = current_speaker_name or username.strip()
        if slide_handle and slide_handle == username.strip() and not slide_handle.startswith("@"):
            slide_handle = f"@{slide_handle}"
        st.markdown('<div class="workspace-row-slides-anchor"></div>', unsafe_allow_html=True)
        if (slide_text1 or "").strip():
            _render_slide_one_preview(
                slide_handle,
                slide_text1,
                _safe_browser_image_url(thumbnail_link),
                current_slide_one_font_adjust,
                current_slide_one_background_adjust,
            )
            _render_workspace_preview_control_bar(
                f"{row_num}_slide1",
                slide_one_font_adjust_key,
                current_slide_one_font_adjust,
                slide_one_background_adjust_key,
                current_slide_one_background_adjust,
            )
        if (slide_text2 or "").strip():
            _render_text_slide_preview(2, slide_text2, current_slide_two_font_adjust)
            _render_workspace_preview_control_bar(
                f"{row_num}_slide2",
                slide_two_font_adjust_key,
                current_slide_two_font_adjust,
            )
        if (slide_text3 or "").strip():
            _render_text_slide_preview(3, slide_text3, current_slide_three_font_adjust, include_link_cta=True)
            _render_workspace_preview_control_bar(
                f"{row_num}_slide3",
                slide_three_font_adjust_key,
                current_slide_three_font_adjust,
            )
        st.code(slide_text1 or "(none)", language=None)
        st.code(slide_text2 or "(none)", language=None)
        st.code(slide_text3 or "(none)", language=None)
        if st.button("Generate prompt", key=f"workspace_row_slides_build_{row_num}", width="stretch"):
            st.session_state[prompt_key] = _build_single_row_chatgpt_prompt(prompt_row or {})
            _rerun_workspace("Edit")
        row_prompt = st.session_state.get(prompt_key, "")
        if row_prompt:
            _one_line_copy_preview("slide prompt", row_prompt, f"workspace_row_slides_prompt_preview_{row_num}")
    elif selected_content_tab == "Media" and media_links:
            _one_line_copy_preview("media", "\n".join(media_links), f"workspace_media_links_{row_num}")
            st.markdown(
                f'<div class="workspace-plain-copy-text">Drive media link{"" if len(media_links) == 1 else "s"}.</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="workspace-plain-copy-text">{html.escape(chr(10).join(media_links))}</div>',
                unsafe_allow_html=True,
            )
            if (media_type or "").strip().lower() == "reel" and media_links:
                st.link_button("Open reel in Drive", media_links[0], width="stretch")
            else:
                for index, link in enumerate(media_links, start=1):
                    label = "Open media in Drive" if len(media_links) == 1 else f"Open media {index} in Drive"
                    st.link_button(label, link, width="stretch")


def _icon_copy_button(label: str, value: str) -> None:
    escaped_label = html.escape(label)
    clipboard_text = json.dumps(value or "")
    button_html = f"""
    <button
      onclick='navigator.clipboard.writeText({clipboard_text})'
      title='Copy {escaped_label}'
      style="
        width: 100%;
        min-height: 3rem;
        border: 1px solid rgba(15,23,42,0.14);
        border-radius: 14px;
        background: white;
        color: #0f172a;
        font-size: 1.15rem;
        font-weight: 700;
        cursor: pointer;
      "
    >💬</button>
    """
    st.html(button_html)


def _copy_caption_button(value: str) -> None:
    clipboard_text = json.dumps(value or "")
    button_html = f"""
    <button
      onclick='navigator.clipboard.writeText({clipboard_text})'
      title='Copy caption'
      style="
        width: 100%;
        min-height: 3rem;
        border: 1px solid rgba(15,23,42,0.14);
        border-radius: 14px;
        background: white;
        color: #0f172a;
        font-size: 1.15rem;
        font-weight: 700;
        cursor: pointer;
      "
    >💬</button>
    """
    st.html(button_html)


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


def _process_pending_rows_from_sheet() -> int:
    pending = _run_with_sheet_quota_countdown(
        lambda: get_pending_rows(GOOGLE_SHEET_ID),
        "Processing new rows paused:",
    )
    if not pending:
        return 0

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
                existing_inputs = _current_row_caption_inputs(row)
                default_top_comment = existing_inputs["Top Comment"]
                if not default_top_comment and result["status"] == "ingested":
                    row_url = _cell_text(row.get("Instagram URL")).strip()
                    if result["media_type"] == "article":
                        default_top_comment = _build_read_cta(row_url)
                    elif _is_instagram_url(row_url):
                        default_top_comment = _build_watch_cta(result["username"], row_url)

                update_metadata(
                    GOOGLE_SHEET_ID,
                    row_num,
                    existing_inputs["Caption Context"],
                    existing_inputs["Speaker Name"],
                    existing_inputs["Required Hashtags"],
                    default_top_comment,
                    "",
                )
                if result["status"] == "ingested":
                    ingested_row = dict(row)
                    ingested_row.update(
                        {
                            "Source Username": result["username"],
                            "Media Type": result["media_type"],
                            "Photo Count": result["photo_count"],
                            "Media Drive Link": result["media_link"],
                            "Thumbnail Drive Link": result["thumbnail_link"],
                            "Original Caption": result["original_caption"],
                            "Transcript": result["transcript"],
                            "Status": result["status"],
                            "Caption Context": existing_inputs["Caption Context"],
                            "Speaker Name": existing_inputs["Speaker Name"],
                            "Required Hashtags": existing_inputs["Required Hashtags"],
                            "Top Comment": default_top_comment,
                            "Footer": "",
                        }
                    )
                    generated_caption = generate_row_caption(ingested_row)
                    if update_caption_and_metadata is not None:
                        update_caption_and_metadata(
                            GOOGLE_SHEET_ID,
                            row_num,
                            generated_caption,
                            result["status"],
                            existing_inputs["Caption Context"],
                            existing_inputs["Speaker Name"],
                            existing_inputs["Required Hashtags"],
                            default_top_comment,
                            "",
                        )
                    else:
                        update_caption(GOOGLE_SHEET_ID, row_num, generated_caption, result["status"])
            except Exception as e:
                status_box.update(label=f"Row {row_num}: error writing to sheet - {describe_error(e)}", state="error")
            else:
                if result["status"].startswith("error"):
                    status_box.update(label=f"Row {row_num}: {result['status']}", state="error")
                else:
                    action_word = "ingested + captioned"
                    display_name = f"@{result['username']}" if result["username"] and result["media_type"] != "article" else result["username"]
                    status_box.update(
                        label=(
                            f"Row {row_num}: {action_word} - {display_name} ({result['media_type']})"
                        ),
                        state="complete",
                    )
        progress.progress((i + 1) / len(pending))

    return len(pending)


def _append_url_and_get_new_row(url: str, required_hashtags: str = "") -> dict:
    cleaned_url = (url or "").strip()
    if not cleaned_url:
        raise ValueError("URL is required.")

    before_rows = get_all_rows(GOOGLE_SHEET_ID)
    before_row_numbers = {int(row.get("row_number") or 0) for row in before_rows if row.get("row_number")}
    append_link_rows(GOOGLE_SHEET_ID, [cleaned_url], required_hashtags)
    after_rows = get_all_rows(GOOGLE_SHEET_ID)

    new_rows = [
        row for row in after_rows
        if int(row.get("row_number") or 0) not in before_row_numbers
        and _cell_text(row.get("Instagram URL")).strip() == cleaned_url
    ]
    if not new_rows:
        matching_rows = [
            row for row in after_rows
            if _cell_text(row.get("Instagram URL")).strip() == cleaned_url
        ]
        if matching_rows:
            return max(matching_rows, key=lambda row: int(row.get("row_number") or 0))
        raise ValueError("Could not find the newly appended sheet row.")

    return max(new_rows, key=lambda row: int(row.get("row_number") or 0))


def _process_single_url_to_editor(url: str, required_hashtags: str = "") -> int:
    row = _append_url_and_get_new_row(url, required_hashtags)
    row_num = int(row["row_number"])

    result = _ingest_row(row)
    if result["status"] != "ingested":
        raise ValueError(result["status"])

    default_top_comment = ""
    row_url = _cell_text(row.get("Instagram URL")).strip()
    if result["media_type"] == "article":
        default_top_comment = _build_read_cta(row_url)
    elif _is_instagram_url(row_url):
        default_top_comment = _build_watch_cta(result["username"], row_url)

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
    update_metadata(
        GOOGLE_SHEET_ID,
        row_num,
        "",
        "",
        required_hashtags,
        default_top_comment,
        "",
    )

    working_row = _reload_row_from_sheet(row_num)
    media_type = _cell_text(working_row.get("Media Type")).strip().lower()
    if _is_reel_url(row_url):
        _process_post_online(working_row)
    elif media_type == "photo":
        _process_photo_post_online(working_row)
    else:
        generated_caption = generate_row_caption(working_row)
        update_caption(GOOGLE_SHEET_ID, row_num, generated_caption, "done")
        working_row = _reload_row_from_sheet(row_num)
        if not _carousel_has_required_text(
            {
                "name": _cell_text(working_row.get("name")).strip(),
                "text1": _cell_text(working_row.get("text1")).strip(),
                "text2": _cell_text(working_row.get("text2")).strip(),
                "text3": _cell_text(working_row.get("text3")).strip(),
            }
        ):
            carousel = _generate_reliable_carousel_copy(working_row, model="gpt-5.2")
            _write_specific_carousel_fields(row_num, carousel)

    return row_num


def _ingest_row(row: dict) -> dict:
    """Process one row through ingest and return sheet fields."""
    url = row["Instagram URL"].strip()
    tmp_dir = None
    try:
        if _is_article_url(url):
            article = fetch_article_source(url)
            article_source_text = (
                (article.get("source_text") or "").strip()
                or (article.get("summary_text") or "").strip()
            )
            return {
                "username": article.get("domain", ""),
                "media_type": "article",
                "photo_count": "",
                "media_link": "",
                "thumbnail_link": article.get("image_url", ""),
                "original_caption": article_source_text,
                "transcript": "",
                "status": "ingested",
            }
        if _is_reel_url(url):
            data = process_reel_url(url, include_transcript=False)
        else:
            data = process_post_url(url)
        filename_prefix = build_filename_prefix(row.get("row_number"), data.get("username", ""))
        uploaded = upload_media_bundle(data, filename_prefix=filename_prefix)
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
            "status": f"error: {describe_error(e)}",
        }
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _rerun_with_transcript(row: dict, force_remote: bool = False) -> bool:
    updated_row = _fetch_row_with_transcript(row, force_remote=force_remote)
    row_num = row["row_number"]
    caption = generate_row_caption(updated_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    return bool((updated_row.get("Transcript") or "").strip())


def _fetch_row_with_transcript(row: dict, download_media: bool = False, force_remote: bool = False) -> dict:
    url = row.get("Instagram URL", "").strip()
    if not _is_reel_url(url):
        raise ValueError("Transcript rerun is only available for reels.")

    row_num = row["row_number"]
    existing_transcript = (row.get("Transcript") or "").strip()
    if existing_transcript and not download_media and not force_remote:
        updated_row = dict(row)
        updated_row["Transcript"] = existing_transcript
        return updated_row

    if existing_transcript and download_media and not force_remote:
        _download_media_to_drive(row)
        updated_row = dict(row)
        updated_row["Transcript"] = existing_transcript
        return updated_row

    tmp_dir = None
    try:
        refreshed = process_reel_url(url, include_transcript=True)
        transcript = (refreshed.get("transcript") or "").strip()
        if download_media:
            filename_prefix = build_filename_prefix(row_num, refreshed.get("username") or row.get("Source Username", ""))
            uploaded = upload_media_bundle(refreshed, filename_prefix=filename_prefix)
            tmp_dir = uploaded["tmp_dir"]
            status_value = (row.get("Status") or "").strip() or "ingested"
            update_ingest_result(
                GOOGLE_SHEET_ID,
                row_num,
                refreshed.get("username") or row.get("Source Username", ""),
                refreshed.get("media_type") or row.get("Media Type", ""),
                refreshed.get("photo_count") or row.get("Photo Count", ""),
                uploaded.get("media_link", "") or row.get("Media Drive Link", ""),
                uploaded.get("thumbnail_link", "") or row.get("Thumbnail Drive Link", ""),
                refreshed.get("original_caption") or row.get("Original Caption", ""),
                transcript,
                status_value,
            )
        else:
            if transcript:
                update_transcript(GOOGLE_SHEET_ID, row_num, transcript)
            uploaded = {
                "media_link": row.get("Media Drive Link", ""),
                "thumbnail_link": row.get("Thumbnail Drive Link", ""),
            }

        updated_row = dict(row)
        updated_row["Transcript"] = transcript
        updated_row["Source Username"] = refreshed.get("username") or updated_row.get("Source Username", "")
        updated_row["Original Caption"] = refreshed.get("original_caption") or updated_row.get("Original Caption", "")
        updated_row["Media Type"] = refreshed.get("media_type") or updated_row.get("Media Type", "")
        updated_row["Photo Count"] = refreshed.get("photo_count") or updated_row.get("Photo Count", "")
        updated_row["Media Drive Link"] = uploaded.get("media_link", "") or updated_row.get("Media Drive Link", "")
        updated_row["Thumbnail Drive Link"] = uploaded.get("thumbnail_link", "") or updated_row.get("Thumbnail Drive Link", "")
        return updated_row
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


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
        filename_prefix = build_filename_prefix(row.get("row_number"), data.get("username", ""))
        uploaded = upload_media_bundle(data, filename_prefix=filename_prefix)
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

    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured.")
    response = _get_client().chat.completions.create(
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
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    _write_carousel_fields(row_num, updated_row)


def _row_is_photo_post(row: dict) -> bool:
    url = _cell_text(row.get("Instagram URL")).strip()
    return _is_instagram_url(url) and not _is_reel_url(url)


def _generate_caption_for_row(row: dict) -> None:
    row_num = row["row_number"]
    current_inputs = _current_row_caption_inputs(row)
    update_metadata(
        GOOGLE_SHEET_ID,
        row_num,
        current_inputs["Caption Context"],
        current_inputs["Speaker Name"],
        current_inputs["Required Hashtags"],
        current_inputs["Top Comment"],
        "",
    )
    updated_row = dict(row)
    updated_row.update(current_inputs)
    caption = generate_row_caption(updated_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    if _row_is_photo_post(updated_row):
        _write_carousel_fields(row_num, updated_row)


def _write_specific_carousel_fields(row_number: int, carousel: dict[str, str]) -> None:
    if update_carousel_fields is None:
        return
    update_carousel_fields(
        GOOGLE_SHEET_ID,
        row_number,
        carousel.get("name", ""),
        carousel.get("text1", ""),
        carousel.get("text2", ""),
        carousel.get("text3", ""),
    )


def _carousel_has_required_text(carousel: dict[str, str]) -> bool:
    return bool(
        _cell_text(carousel.get("text1")).strip()
        and _cell_text(carousel.get("text2")).strip()
        and _cell_text(carousel.get("text3")).strip()
    )


def _generate_reliable_carousel_copy(row: dict, model: str = "gpt-5.2") -> dict[str, str]:
    carousel = generate_carousel_copy_with_model(row, model=model)
    if _carousel_has_required_text(carousel):
        return carousel

    batch_results = generate_batch_carousel_copy_with_model([row], model=model)
    batch_carousel = batch_results.get(int(row.get("row_number") or 0), {})
    if _carousel_has_required_text(batch_carousel):
        return batch_carousel

    raise ValueError("Slide generation returned incomplete text.")


def _verify_carousel_fields_saved(row_number: int) -> dict[str, str]:
    rows = get_all_rows(GOOGLE_SHEET_ID)
    saved_row = next((item for item in rows if int(item.get("row_number") or 0) == row_number), None)
    if not saved_row:
        raise ValueError("Processed row could not be reloaded from the sheet.")
    saved_carousel = {
        "name": _cell_text(saved_row.get("name")).strip(),
        "text1": _cell_text(saved_row.get("text1")).strip(),
        "text2": _cell_text(saved_row.get("text2")).strip(),
        "text3": _cell_text(saved_row.get("text3")).strip(),
    }
    if not _carousel_has_required_text(saved_carousel):
        raise ValueError("Slide fields were not saved to the sheet.")
    return saved_carousel


def _reload_row_from_sheet(row_number: int) -> dict:
    rows = get_all_rows(GOOGLE_SHEET_ID)
    reloaded = next((item for item in rows if int(item.get("row_number") or 0) == row_number), None)
    if not reloaded:
        raise ValueError("Processed row could not be reloaded from the sheet.")
    return reloaded


def _process_post_online(row: dict) -> None:
    row_num = row["row_number"]
    has_media = bool(_cell_text(row.get("Media Drive Link")).strip())
    existing_transcript = _cell_text(row.get("Transcript")).strip()
    updated_row = _fetch_row_with_transcript(
        row,
        download_media=not has_media,
        force_remote=not bool(existing_transcript),
    )
    current_inputs = _current_row_caption_inputs(updated_row)
    update_metadata(
        GOOGLE_SHEET_ID,
        row_num,
        current_inputs["Caption Context"],
        current_inputs["Speaker Name"],
        current_inputs["Required Hashtags"],
        current_inputs["Top Comment"],
        "",
    )
    updated_row.update(current_inputs)

    existing_caption = _cell_text(updated_row.get("Generated Caption")).strip()
    caption = existing_caption or generate_row_caption(updated_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    if not existing_caption:
        update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    updated_row["Generated Caption"] = caption
    updated_row["Status"] = next_status

    existing_carousel = {
        "name": _cell_text(updated_row.get("name")).strip(),
        "text1": _cell_text(updated_row.get("text1")).strip(),
        "text2": _cell_text(updated_row.get("text2")).strip(),
        "text3": _cell_text(updated_row.get("text3")).strip(),
    }
    if _carousel_has_required_text(existing_carousel):
        return

    carousel = _generate_reliable_carousel_copy(updated_row, model="gpt-5.2")
    _write_specific_carousel_fields(row_num, carousel)
    _verify_carousel_fields_saved(row_num)
    st.session_state.pop(f"workspace_preview_upload_links_{row_num}", None)


def _process_photo_post_online(row: dict) -> None:
    row_num = row["row_number"]
    working_row = dict(row)
    if not _cell_text(working_row.get("Media Drive Link")).strip():
        _download_media_to_drive(working_row)
        working_row = _reload_row_from_sheet(row_num)

    current_inputs = _current_row_caption_inputs(working_row)
    update_metadata(
        GOOGLE_SHEET_ID,
        row_num,
        current_inputs["Caption Context"],
        current_inputs["Speaker Name"],
        current_inputs["Required Hashtags"],
        current_inputs["Top Comment"],
        "",
    )
    working_row.update(current_inputs)

    if not _cell_text(working_row.get("Transcript")).strip() and not _cell_text(working_row.get("Caption Context")).strip():
        extracted_text = _extract_image_text(working_row)
        update_caption_context(GOOGLE_SHEET_ID, row_num, extracted_text)
        update_transcript(GOOGLE_SHEET_ID, row_num, extracted_text)
        working_row["Caption Context"] = extracted_text
        working_row["Transcript"] = extracted_text

    existing_caption = _cell_text(working_row.get("Generated Caption")).strip()
    caption = existing_caption or generate_row_caption(working_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    if not existing_caption:
        update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    working_row["Generated Caption"] = caption
    working_row["Status"] = next_status

    existing_carousel = {
        "name": _cell_text(working_row.get("name")).strip(),
        "text1": _cell_text(working_row.get("text1")).strip(),
        "text2": _cell_text(working_row.get("text2")).strip(),
        "text3": _cell_text(working_row.get("text3")).strip(),
    }
    if _carousel_has_required_text(existing_carousel):
        return

    carousel = _generate_reliable_carousel_copy(working_row, model="gpt-5.2")
    _write_specific_carousel_fields(row_num, carousel)
    _verify_carousel_fields_saved(row_num)


def _queue_workspace_action(row_number: int, action: str) -> None:
    queue = st.session_state.setdefault("workspace_action_queue", [])
    queue.append({"row_number": row_number, "action": action})


def _rerun_workspace(tab: str | None = None) -> None:
    if tab:
        if tab in {"Edit", "Grid"}:
            tab = "Home"
        st.session_state["_workspace_pending_tab"] = tab
    st.rerun()


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
            _rerun_workspace("Edit")
        return

    try:
        if action == "process_post":
            with st.spinner(f"Processing row {row_number}..."):
                row_url = _cell_text(row.get("Instagram URL")).strip()
                if _is_reel_url(row_url):
                    _process_post_online(row)
                    success_message = f"Row {row_number}: processed with transcript, caption, and slide copy."
                else:
                    _process_photo_post_online(row)
                    success_message = f"Row {row_number}: processed with caption and slide copy."
            st.session_state["workspace_success"] = success_message
        elif action == "transcript":
            with st.spinner(f"Refreshing row {row_number} with transcript..."):
                transcript_found = _rerun_with_transcript(row, force_remote=True)
            if transcript_found:
                st.session_state["workspace_success"] = f"Row {row_number}: transcript rerun complete."
            else:
                st.session_state["workspace_success"] = (
                    f"Row {row_number}: no transcript was available, so the caption was generated from existing source text."
                )
        elif action == "generate_caption":
            with st.spinner(f"Generating caption for row {row_number}..."):
                _generate_caption_for_row(row)
            st.session_state["workspace_success"] = f"Row {row_number}: caption generated."
        elif action == "image_text":
            with st.spinner(f"Extracting image text for row {row_number}..."):
                _redo_caption_from_image_text(row)
            st.session_state["workspace_success"] = f"Row {row_number}: caption regenerated from image text."
        elif action == "refresh_thumbnail_5s":
            with st.spinner(f"Updating screenshot for row {row_number}..."):
                _refresh_row_thumbnail_from_video(row, offset_seconds=5.0)
            st.session_state["workspace_success"] = f"Row {row_number}: screenshot updated from 5 seconds into the video."
        else:
            raise ValueError(f"Unknown action: {action}")
        _mark_workspace_action_complete(row_number, action)
    except Exception as e:
        st.session_state["workspace_error"] = f"Row {row_number}: {describe_error(e)}"

    _rerun_workspace("Edit")


def _delete_workspace_row(row: dict) -> None:
    row_number = row["row_number"]
    delete_sheet_row(GOOGLE_SHEET_ID, row_number)
    pending_transcribe_resets = st.session_state.get("workspace_transcribe_reset_rows", [])
    if pending_transcribe_resets:
        st.session_state["workspace_transcribe_reset_rows"] = [
            pending for pending in pending_transcribe_resets if pending != _workspace_key(row, "transcribe")
        ]
    _clear_workspace_row_state(row)


def _write_carousel_fields(row_number: int, row: dict) -> None:
    if update_carousel_fields is None:
        return
    carousel = generate_carousel_copy(row)
    update_carousel_fields(
        GOOGLE_SHEET_ID,
        row_number,
        carousel.get("name", ""),
        carousel.get("text1", ""),
        carousel.get("text2", ""),
        carousel.get("text3", ""),
    )


def _row_ready_for_chatgpt(row: dict) -> bool:
    if not _cell_text(row.get("Instagram URL")).strip():
        return False
    status = _cell_text(row.get("Status")).strip().lower()
    if status.startswith("error") or status == "slides":
        return False
    media_type = _cell_text(row.get("Media Type")).strip().lower()
    transcript = _cell_text(row.get("Transcript")).strip()
    original_caption = _cell_text(row.get("Original Caption")).strip()
    caption_context = _cell_text(row.get("Caption Context")).strip()
    if media_type == "article":
        return bool(original_caption or caption_context)
    return bool(transcript or original_caption or caption_context)


def _chatgpt_ready_rows(sheet_id: str) -> list[dict]:
    return [row for row in get_all_rows(sheet_id) if _row_ready_for_chatgpt(row)]


def _ready_rows_from_loaded_rows(rows: list[dict]) -> list[dict]:
    return [row for row in rows if _row_ready_for_chatgpt(row)]


def _build_chatgpt_handoff_prompt(rows: list[dict]) -> str:
    blocks: list[str] = []
    for row in rows:
        row_num = row["row_number"]
        username = _cell_text(row.get("Source Username")).strip() or "unknown"
        media_type = _cell_text(row.get("Media Type")).strip().lower() or "post"
        required_hashtags = _cell_text(row.get("Required Hashtags")).strip()
        transcript = _cell_text(row.get("Transcript")).strip()
        original_caption = _cell_text(row.get("Original Caption")).strip()
        caption_context = _cell_text(row.get("Caption Context")).strip()
        blocks.append(
            "\n".join(
                [
                    f"ROW {row_num}",
                    f"username: {username}",
                    f"media_type: {media_type}",
                    f"required_hashtags: {required_hashtags or '(none)'}",
                    f"transcript:\n{transcript or '(none)'}",
                    f"original_caption:\n{original_caption or '(none)'}",
                    f"caption_context:\n{caption_context or '(none)'}",
                ]
            )
        )

    instructions = (
        "Return ONLY valid JSON as an array.\n\n"
        "Each object must include:\n"
        "* row_number\n"
        "* name\n"
        "* text1\n"
        "* text2\n"
        "* text3\n\n"
        "Rules:\n"
        "* Keep row_number exactly the same numeric value shown in the row block\n"
        "* No markdown\n"
        "* No commentary outside JSON\n"
        "* Use plain straight double quotes for all JSON keys and string values — no smart quotes, no escaped quotes inside key names\n"
        "* name = short lowercase account username (no @ symbol)\n"
        "* text1 = strongest opening carousel slide under 350 chars\n"
        "* text2 and text3 = under 900 chars each\n"
        "* No em dashes\n"
        "* No speculation\n"
        "* Avoid repetitive phrasing across fields\n"
        "Style priority:\n"
        "* Write like a viral political news account creating Instagram carousel slides\n"
        "* Sound natural, conversational, and punchy\n"
        "* Prioritize emotional framing, political stakes, accusations, numbers, and consequences\n"
        "* Use direct quotes naturally when they strengthen the writing\n"
        "* Avoid robotic transition phrases\n"
        '* Never say "the speaker," "the clip," "the transcript," "the video," "the comments," "the argument," "the warning," or "the line said"\n'
        "* Do not over explain the source material\n"
        "* Make #text1, #text2, and #text3 feel like three carousel slides\n"
        "* Put the most important accusation, statistic, conflict, or consequence into #text1\n"
        "* #text1 should feel like the strongest opening carousel slide, not just a short hook\n"
        "* Front load critical information into #text1 whenever possible\n"
        "* Use #text2 to expand the core conflict with context, quotes, or stakes\n"
        "* Use #text3 to focus on consequences, reactions, fallout, or additional details\n"
        "* Make each text field feel like a standalone Instagram carousel slide\n"
        "* Prioritize specificity over vagueness\n"
        "* Include numbers, names, and direct quotes whenever they strengthen the writing\n"
        "* Use emotionally charged but factual framing\n"
        "* Avoid filler phrases and weak transitions\n"
        "* Do not artificially shorten strong explanations just to save space\n"
        "* Avoid generic summaries\n\n"
        "Quote guidance:\n"
        "* Use the person's name when provided\n"
        "* If no name is provided, write around the facts naturally\n"
        "* Prefer short direct quotes when they are strong\n"
        "* Do not force quotes into awkward sentences\n"
        '* Never write "the quote said" or "the line said"\n\n'
        "Output format example:\n"
        "[\n"
        "  {\n"
        '    "row_number": 1,\n'
        '    "name": "nowthis",\n'
        '    "text1": "\\"We could abolish medical debt 10 times over.\\"",\n'
        '    "text2": "He compared military spending with healthcare costs and argued billions are being diverted away from public needs while families still drown in debt and coverage gaps. The attack centered on lobbying money, Medicaid cuts, and the claim that Washington keeps funding war while basic healthcare needs go unmet.",\n'
        '    "text3": "The fallout is political as much as financial. The carousel ties insurance lobbying, federal spending priorities, and Medicaid pressure to the daily reality facing working Americans who are still buried in debt and losing coverage."\n'
        "  }\n"
        "]\n"
    )
    return instructions + "\n\n" + "\n\n---\n\n".join(blocks)


_SLIDE_KEYS = ["row_number", "name", "text1", "text2", "text3", "generated_caption"]


def _normalize_slide_paste(text: str) -> str:
    """Rebuild messy slide paste as valid JSON using known field names as anchors."""
    text = text.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
    raw = text.strip().lstrip("[").rstrip("]").strip()
    blocks = re.split(r"}\s*,\s*{", raw)
    key_pat = '"(' + "|".join(re.escape(k) for k in _SLIDE_KEYS) + r')"\s*:\s*'
    out: list[dict] = []
    for block in blocks:
        matches = list(re.finditer(key_pat, block))
        if not matches:
            continue
        item: dict = {}
        for i, m in enumerate(matches):
            key = m.group(1)
            val_start = m.end()
            val_end = matches[i + 1].start() if i + 1 < len(matches) else len(block)
            raw_val = block[val_start:val_end].strip().rstrip(",}] ").strip()
            if key == "row_number":
                num = re.search(r"\d+", raw_val)
                if num:
                    item["row_number"] = int(num.group())
            else:
                if raw_val.startswith('"'):
                    raw_val = raw_val[1:]
                if raw_val.endswith('"'):
                    raw_val = raw_val[:-1]
                item[key] = raw_val
        if item:
            out.append(item)
    if not out:
        raise ValueError("No slide items found.")
    return json.dumps(out)


def _extract_json_payload(raw_text: str):
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("Paste a JSON result first.")

    def _strip_comments(candidate: str) -> str:
        without_block_comments = re.sub(r"/\*[\s\S]*?\*/", "", candidate)
        return re.sub(r"(?m)^\s*//.*$", "", without_block_comments)

    def _extract_block(candidate: str) -> str:
        candidate = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate.strip(), flags=re.IGNORECASE | re.MULTILINE)
        candidate = _strip_comments(candidate)
        match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", candidate)
        return match.group(1) if match else candidate

    def _escape_string_newlines(s: str) -> str:
        result = []
        in_string = False
        i = 0
        while i < len(s):
            c = s[i]
            if not in_string:
                if c == '"':
                    in_string = True
                result.append(c)
            else:
                if c == '\\':
                    result.append(c)
                    i += 1
                    if i < len(s):
                        result.append(s[i])
                elif c == '"':
                    in_string = False
                    result.append(c)
                elif c == '\n':
                    result.append('\\n')
                elif c == '\r':
                    result.append('\\r')
                elif c == '\t':
                    result.append('\\t')
                else:
                    result.append(c)
            i += 1
        return ''.join(result)

    def _repair_jsonish(candidate: str) -> str:
        repaired = _escape_string_newlines(candidate.strip())
        repaired = repaired.replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")
        repaired = _strip_comments(repaired)
        repaired = re.sub(r",\s*([}\]])", r"\1", repaired)
        repaired = re.sub(
            r'([{\[,]\s*)(#?[A-Za-z_][A-Za-z0-9_#]*)(\s*:)',
            lambda m: f'{m.group(1)}"{m.group(2)}"{m.group(3)}',
            repaired,
        )
        repaired = re.sub(
            r'(?m)^(\s*)(#?[A-Za-z_][A-Za-z0-9_#]*)(\s*:)',
            lambda m: f'{m.group(1)}"{m.group(2)}"{m.group(3)}',
            repaired,
        )
        repaired = re.sub(
            r'([^\s{\[,])(\s*\n\s*)(?=(?:"?#?[A-Za-z_][A-Za-z0-9_#]*"|#?[A-Za-z_][A-Za-z0-9_#]*)\s*:)',
            r"\1,\2",
            repaired,
        )
        repaired = re.sub(r"}\s*\n\s*{", "},\n{", repaired)
        if repaired.startswith("{") and repaired.endswith("}") and re.search(r"}\s*,\s*{", repaired):
            repaired = f"[{repaired}]"
        return repaired

    def _parse_by_known_keys(candidate: str) -> list:
        known = ["row_number", "name", "text1", "text2", "text3", "generated_caption"]
        key_pat = '"(' + "|".join(re.escape(k) for k in known) + r')"\s*:\s*'
        raw = candidate.strip().lstrip("[").rstrip("]")
        blocks = re.split(r"}\s*,\s*{", raw)
        items = []
        for block in blocks:
            matches = list(re.finditer(key_pat, block))
            if not matches:
                continue
            item: dict = {}
            for i, m in enumerate(matches):
                key = m.group(1)
                val_start = m.end()
                val_end = matches[i + 1].start() if i + 1 < len(matches) else len(block)
                raw_val = block[val_start:val_end].strip().rstrip(",").strip()
                if key == "row_number":
                    num = re.search(r"\d+", raw_val)
                    if num:
                        item["row_number"] = int(num.group())
                else:
                    if raw_val.startswith('"'):
                        raw_val = raw_val[1:]
                    if raw_val.endswith('"'):
                        raw_val = raw_val[:-1]
                    item[key] = raw_val
            if item:
                items.append(item)
        if not items:
            raise ValueError("No items found by key anchoring.")
        return items

    def _parse_linewise_payload(candidate: str):
        lines = [line.rstrip() for line in candidate.splitlines() if line.strip()]
        if not lines or not any(":" in line for line in lines):
            raise ValueError("No linewise payload to parse.")

        items: list[dict] = []
        current: dict[str, object] = {}
        for raw_line in lines:
            line = raw_line.strip()
            if line.startswith("- "):
                if current:
                    items.append(current)
                    current = {}
                line = line[2:].strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip().strip("\"'")
            value = value.strip().rstrip(",")
            if not key:
                continue
            if not value:
                parsed_value = ""
            else:
                normalized = value.replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")
                try:
                    parsed_value = ast.literal_eval(normalized)
                except Exception:
                    parsed_value = normalized.strip("\"'")
            current[key] = parsed_value

        if current:
            items.append(current)
        if not items:
            raise ValueError("No linewise payload to parse.")
        return items if len(items) > 1 else items[0]

    text_block = _extract_block(text)
    try:
        return json.loads(text_block)
    except json.JSONDecodeError:
        try:
            return json.loads(_normalize_slide_paste(text_block))
        except Exception:
            pass
        repaired = _repair_jsonish(text_block)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            pythonish = re.sub(r"\btrue\b", "True", repaired, flags=re.IGNORECASE)
            pythonish = re.sub(r"\bfalse\b", "False", pythonish, flags=re.IGNORECASE)
            pythonish = re.sub(r"\bnull\b", "None", pythonish, flags=re.IGNORECASE)
            try:
                return ast.literal_eval(pythonish)
            except Exception as exc:
                try:
                    return _parse_linewise_payload(text_block)
                except Exception:
                    raise ValueError(
                        "Slide results must be valid JSON or near-JSON with quoted keys."
                    ) from exc


def _apply_chatgpt_handoff_results(sheet_id: str, raw_text: str) -> tuple[int, list[str]]:
    _QUOTES = '"“”\'‘’ '
    payload = _extract_json_payload(raw_text)
    items = payload if isinstance(payload, list) else [payload]
    rows = get_all_rows(sheet_id)
    row_map = {int(row["row_number"]): row for row in rows if row.get("row_number")}
    updated_count = 0
    issues: list[str] = []

    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            issues.append(f"Item {index}: result is not an object.")
            continue
        item = {k.strip().strip(_QUOTES): v for k, v in item.items()}
        row_number = item.get("row_number")
        if row_number is None:
            found_keys = ", ".join(list(item.keys())[:6]) or "(none)"
            issues.append(f"Item {index}: missing row_number (found keys: {found_keys}).")
            continue
        try:
            row_number = int(row_number)
        except Exception:
            issues.append(f"Item {index}: row_number {row_number!r} is not numeric.")
            continue
        row = row_map.get(row_number)
        if row is None:
            issues.append(f"Item {index}: row {row_number} was not found in the sheet.")
            continue

        raw_name = _cell_text(item.get("name")).strip()
        name = ("@" + raw_name if raw_name and not raw_name.startswith("@") and " " not in raw_name else raw_name)
        text1 = _cell_text(item.get("text1")).strip()
        text2 = _cell_text(item.get("text2")).strip()
        text3 = _cell_text(item.get("text3")).strip()

        if not (name or text1 or text2 or text3):
            issues.append(
                f"Item {index} / row {row_number}: no name, text1, text2, or text3 values were provided."
            )
            continue

        if update_carousel_fields is not None:
            update_carousel_fields(sheet_id, row_number, name, text1, text2, text3)
        updated_count += 1

    return updated_count, issues


def _run_home_mode(mode: str, urls: list[str], org_hashtag: str) -> tuple[str, list[dict]]:
    results = []
    tag_value = ORG_HASHTAG_MAP.get(org_hashtag, "")

    for url in urls:
        if mode == "Generate headline":
            source = _fetch_link_data(url)
            source_text = source.get("source_text", "").strip()
            if not source_text:
                raise ValueError(f"{url}: could not extract source text.")
            footer_username = source.get("username", "") if source.get("is_instagram", False) else ""
            final_caption = _build_footered_caption(source_text, footer_username)
            if not source.get("is_instagram", False):
                final_caption = _build_footered_caption(
                    f"{source_text}\n\n{_build_read_cta(source['url'])}",
                    "",
                )
            results.append(
                {
                    "url": source["url"],
                    "username": source.get("username", ""),
                    "display_name": source.get("display_name", ""),
                    "is_instagram": source.get("is_instagram", False),
                    "headlines": _generate_headlines(source_text),
                    "caption": final_caption,
                    "source_caption": source_text,
                }
            )
        elif mode == "Caption this":
            source = _fetch_link_data(url)
            row = {
                "Instagram URL": source["url"],
                "Source Username": (
                    source.get("username", "")
                    if source.get("is_instagram", False)
                    else source.get("display_name", "")
                ),
                "Media Type": "" if source.get("is_instagram", False) else "article",
                "Original Caption": source.get("source_text", "").strip(),
                "Transcript": "",
                "Caption Context": "",
                "Speaker Name": "",
                "Required Hashtags": tag_value,
                "Top Comment": (
                    _build_watch_cta(source.get("username", ""), source["url"])
                    if source.get("is_instagram", False)
                    else _build_read_cta(source["url"])
                ),
            }
            if not row["Original Caption"]:
                raise ValueError(f"{url}: could not extract source text.")
            caption = generate_row_caption(row)
            results.append(
                {
                    "url": source["url"],
                    "username": source.get("username", ""),
                    "display_name": source.get("display_name", ""),
                    "is_instagram": source.get("is_instagram", False),
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
inject_styles("workspace")
st.title("Workspace")

if not require_auth():
    st.stop()

_process_next_workspace_action()

success_message = st.session_state.pop("workspace_success", "")
error_message = st.session_state.pop("workspace_error", "")
if success_message:
    st.success(success_message)
if error_message:
    st.error(error_message)

pending_tab = st.session_state.pop("_workspace_pending_tab", None)
if pending_tab:
    if pending_tab in {"Edit", "Grid"}:
        pending_tab = "Home"
    st.session_state["workspace_active_tab"] = pending_tab
elif "workspace_active_tab" not in st.session_state:
    st.session_state["workspace_active_tab"] = "Home"
elif st.session_state["workspace_active_tab"] in {"Edit", "Grid"}:
    st.session_state["workspace_active_tab"] = "Home"

section_tabs = st.tabs(["Home", "Actions", "Slides", "Data"])

workspace_rows_error = ""
workspace_rows: list[dict] = []
try:
    workspace_rows = _run_with_sheet_quota_countdown(
        lambda: get_all_rows(GOOGLE_SHEET_ID),
        "Loading workspace paused:",
    )
except Exception as e:
    workspace_rows_error = describe_error(e)

with section_tabs[1]:
    st.markdown('<div class="workspace-action-anchor"></div>', unsafe_allow_html=True)
    home_notice = st.session_state.pop("workspace_home_notice", "")

    mode_help = {
        "Add to sheet": "Add an Instagram post or article link to the sheet so it can be processed into the editor.",
        "Process this": "Add the link as a new sheet row, download media, ingest metadata, generate the transcript/caption, and build slide text in one shot.",
        "Generate headline": "Pull source text from an Instagram post or article link, then return three headline options plus a footered caption.",
        "Caption this": "Generate a caption directly from an Instagram post or article link using the selected hashtag preset.",
        "Download media": "Download the media and upload it to Drive without adding a row first.",
    }
    link_area = st.container()
    settings_area = st.container()
    button_area = st.container()
    results_area = st.container()

    mode = st.session_state.get("workspace_home_mode", "Generate headline")
    org_hashtag = st.session_state.get("workspace_org_hashtag", "")
    with settings_area:
        mode = st.selectbox(
            "Action",
            MODE_OPTIONS,
            index=MODE_OPTIONS.index(mode) if mode in MODE_OPTIONS else 0,
            key="workspace_home_mode",
        )
        if mode in mode_help:
            st.caption(mode_help.get(mode, ""))

        if _mode_uses_org_hashtag(mode):
            org_hashtag = st.selectbox(
                "Apply organization hashtag",
                ORG_HASHTAG_OPTIONS,
                index=ORG_HASHTAG_OPTIONS.index(org_hashtag) if org_hashtag in ORG_HASHTAG_OPTIONS else 0,
                key="workspace_org_hashtag",
            )
            selected_hashtag = ORG_HASHTAG_MAP.get(org_hashtag, "")
        else:
            selected_hashtag = ""

    with link_area:
        links = _normalize_home_links(_ensure_home_links())
        link_label = "Instagram Link" if mode == "Download media" else "Link"
        link_placeholder = (
            "https://www.instagram.com/p/... or /reel/..."
            if mode == "Download media"
            else "https://www.instagram.com/... or https://example.com/article"
        )
        links[0] = st.text_input(
            link_label,
            value=links[0],
            placeholder=link_placeholder,
            key="workspace_home_link_0",
        )
        normalized_links = _normalize_home_links(links)
        st.session_state["workspace_home_links"] = normalized_links
        if normalized_links != links:
            _rerun_workspace("Actions")

    with button_area:
        submitted = st.button(_action_label(mode), type="primary", width="stretch")
        if st.button("Clear", width="stretch", key="workspace_home_clear"):
            st.session_state.pop("workspace_home_results", None)
            st.session_state.pop("workspace_home_notice", None)
            _reset_home_links_on_next_render()
            _rerun_workspace("Actions")
        if submitted:
            links_to_process = _clean_home_links()
            if not links_to_process:
                st.warning(f"Enter at least one {link_label.lower()}.")
            elif mode == "Add to sheet":
                try:
                    append_link_rows(
                        GOOGLE_SHEET_ID,
                        links_to_process,
                        selected_hashtag,
                    )
                except Exception as e:
                    st.error(f"Could not add links to sheet: {describe_error(e)}")
                else:
                    st.session_state["workspace_home_notice"] = f"Added {len(links_to_process)} link(s) to the sheet."
                    _reset_home_links_on_next_render()
                    _rerun_workspace("Actions")
            elif mode == "Process this":
                if len(links_to_process) != 1:
                    st.warning("Process this handles one link at a time.")
                else:
                    with st.spinner("Processing link end-to-end..."):
                        try:
                            row_number = _process_single_url_to_editor(links_to_process[0], selected_hashtag)
                        except Exception as e:
                            st.error(f"Process this failed: {describe_error(e)}")
                        else:
                            st.session_state["workspace_home_notice"] = (
                                f"Processed row {row_number}: ingest, caption, and slide text complete."
                            )
                            st.session_state["workspace_selected_row_num"] = row_number
                            st.query_params["workspace_row"] = str(row_number)
                            _reset_home_links_on_next_render()
                            _rerun_workspace("Home")
            else:
                with st.spinner(f"{mode} in progress..."):
                    try:
                        tag_value, results = _run_home_mode(mode, links_to_process, org_hashtag)
                    except Exception as e:
                        st.error(f"{mode} failed: {describe_error(e)}")
                    else:
                        st.session_state["workspace_home_results"] = {
                            "mode": mode,
                            "required_hashtag": tag_value,
                            "items": results,
                        }
                        st.session_state["workspace_home_notice"] = f"{mode} finished for {len(results)} link(s)."
                        _reset_home_links_on_next_render()
                        _rerun_workspace("Actions")

    with results_area:
        home_results = st.session_state.get("workspace_home_results")
        if home_results and home_results.get("mode") == "Generate headline":
            for idx, item in enumerate(home_results.get("items", []), start=1):
                st.caption(f"Result {idx}")
                display_name = item.get("username") or item.get("display_name") or "unknown"
                st.write(f"@{display_name}" if item.get("is_instagram", True) else display_name)
                open_label = "Open Instagram link ↗" if item.get("is_instagram", True) else "Open source link ↗"
                st.markdown(f"[{open_label}]({item['url']})")
                headline_tabs = st.tabs(["Headline 1", "Headline 2", "Headline 3", "Caption"])
                for tab_idx, headline in enumerate(item.get("headlines", [])[:3]):
                    with headline_tabs[tab_idx]:
                        _tab_copy_preview(headline or "(none)")
                with headline_tabs[3]:
                    _tab_copy_preview(item.get("caption", "") or "(none)")

        if home_results and home_results.get("mode") == "Caption this":
            for idx, item in enumerate(home_results.get("items", []), start=1):
                st.caption(f"Caption {idx}")
                display_name = item.get("username") or item.get("display_name") or "unknown"
                st.write(f"@{display_name}" if item.get("is_instagram", True) else display_name)
                open_label = "Open Instagram link ↗" if item.get("is_instagram", True) else "Open source link ↗"
                st.markdown(f"[{open_label}]({item['url']})")
                _copy_block("caption", item.get("caption", ""), f"workspace_home_caption_only_{idx}")

        if home_results and home_results.get("mode") == "Download media":
            for idx, item in enumerate(home_results.get("items", []), start=1):
                st.caption(f"Download {idx}")
                st.write(f"@{item.get('username') or 'unknown'} · {item.get('media_type') or 'unknown'}")
                st.markdown(f"[Open Instagram link ↗]({item['url']})")
                if item.get("media_link"):
                    st.write(f"Media link(s): {item['media_link']}")
                if item.get("thumbnail_link"):
                    st.write(f"Thumbnail: {item['thumbnail_link']}")
        if home_notice:
            st.caption(home_notice)

with section_tabs[2]:
    st.markdown('<div class="workspace-slides-anchor"></div>', unsafe_allow_html=True)
    slides_notice = st.session_state.pop("workspace_slides_notice", "")
    slides_prompt = st.session_state.get("workspace_slides_prompt", "")

    if workspace_rows_error:
        st.error(f"Could not load slide-ready rows: {workspace_rows_error}")
        ready_rows = []
    else:
        ready_rows = _ready_rows_from_loaded_rows(workspace_rows)

    ready_count = len(ready_rows)
    row_word = "row" if ready_count == 1 else "rows"
    if ready_count:
        st.caption(f"{ready_count} {row_word} ready for slides.")
    else:
        st.info("No rows are ready for slides yet.")

    if slides_notice:
        st.caption(slides_notice)

    pasted_results = st.text_area(
        "Paste slide results",
        key="workspace_slides_results",
        height=100,
        placeholder='[{"row_number":2,"name":"...","text1":"...","text2":"...","text3":"..."}]',
    )
    if st.button("Apply slide results", key="workspace_slides_apply", type="primary", width="stretch"):
        try:
            updated_count, issues = _apply_chatgpt_handoff_results(GOOGLE_SHEET_ID, pasted_results)
        except Exception as e:
            st.error(f"Could not apply slide results: {describe_error(e)}")
        else:
            if updated_count:
                message = f"Applied slide results to {updated_count} row(s)."
                if issues:
                    message += f" Skipped {len(issues)} item(s): " + " | ".join(issues[:3])
                st.session_state["workspace_success"] = message
            else:
                st.session_state["workspace_error"] = (
                    "No valid slide results were found to apply."
                    + (f" {' | '.join(issues[:3])}" if issues else "")
                )
            _rerun_workspace("Slides")

    if st.button("Generate slides prompt", key="workspace_slides_build_prompt", type="primary", width="stretch"):
        if not ready_rows:
            st.warning("No rows are ready for slides yet.")
        else:
            st.session_state["workspace_slides_prompt"] = _build_chatgpt_handoff_prompt(ready_rows)
            st.session_state["workspace_slides_notice"] = f"Built slides prompt for {ready_count} {row_word}."
            _rerun_workspace("Slides")

    if slides_prompt:
        _tab_copy_preview(slides_prompt, show_plain_text=False, key="workspace_slides_prompt_copy")

with section_tabs[0]:
    if workspace_rows_error:
        st.error(f"Could not load rows: {workspace_rows_error}")
        pending_edit_rows = []
        editor_rows = []
    else:
        pending_edit_rows = [
            r for r in workspace_rows
            if not r.get("Status", "").strip() and r.get("Instagram URL", "").strip()
        ]
        editor_rows = _sort_editor_rows([r for r in workspace_rows if _is_editable_row(r)])

    if pending_edit_rows:
        row_word = "row" if len(pending_edit_rows) == 1 else "rows"
        st.info(f"{len(pending_edit_rows)} new {row_word} found.")
        if st.button("Process for editing", key="workspace_edit_process_pending", type="primary", width="stretch"):
            try:
                processed_count = _process_pending_rows_from_sheet()
            except Exception as e:
                st.error(f"Could not process new rows: {describe_error(e)}")
            else:
                if processed_count:
                    st.session_state["workspace_success"] = f"Processed {processed_count} new row(s) for editing."
                else:
                    st.session_state["workspace_success"] = "No new rows to process."
                _rerun_workspace("Edit")

    dialog_row_number = st.session_state.get("workspace_link_dialog_row")
    if dialog_row_number is not None:
        dialog_row = next((row for row in editor_rows if row.get("row_number") == dialog_row_number), None)
        if dialog_row is None:
            st.session_state.pop("workspace_link_dialog_row", None)
        else:
            _render_workspace_link_dialog(dialog_row)

    thumbnail_dialog_row_number = st.session_state.get("workspace_thumbnail_dialog_row")
    if thumbnail_dialog_row_number is not None:
        thumbnail_dialog_row = next((row for row in editor_rows if row.get("row_number") == thumbnail_dialog_row_number), None)
        if thumbnail_dialog_row is None:
            st.session_state.pop("workspace_thumbnail_dialog_row", None)
        else:
            _render_workspace_thumbnail_dialog(thumbnail_dialog_row)

    if not editor_rows:
        st.info("No rows yet. Add a link on Actions or process new rows on Data.")
    else:
        query_row = str(st.query_params.get("workspace_row", "") or "")
        if query_row and st.session_state.get("workspace_target_row") != query_row:
            st.session_state["workspace_target_row"] = query_row
        row_numbers = [row["row_number"] for row in editor_rows]
        current_selected = st.session_state.get("workspace_selected_row_num", row_numbers[0])
        if query_row:
            try:
                current_selected = int(query_row)
            except Exception:
                current_selected = row_numbers[0]
        if current_selected not in row_numbers:
            current_selected = row_numbers[0]
        st.session_state["workspace_selected_row_num"] = current_selected
        if st.button(
            "Refresh results",
            key="workspace_refresh_editor_rows",
            width="stretch",
            help="Reload the current editor rows from the sheet and look for new results.",
        ):
            _rerun_workspace("Edit")
        _render_editor_grid(editor_rows)
        current_index = row_numbers.index(current_selected)
        selected_row = editor_rows[current_index]
        st.caption(
            f"Showing row {current_index + 1} of {len(editor_rows)}. "
            "Rows stay here until you delete them from the sheet."
        )
        for row in [selected_row]:
            _sync_workspace_row_state(row)
            row_num = row["row_number"]
            speaker_key = _workspace_speaker_key(row)
            hashtags_key = _workspace_key(row, "hashtags")
            top_key = _workspace_key(row, "top")
            context_key = _workspace_key(row, "context")
            warning_key = _workspace_key(row, "transcript_warning")
            transcribe_key = _workspace_key(row, "transcribe")
            menu_nonce_key = _workspace_key(row, "menu_nonce")
            username = _cell_text(row.get("Source Username")).strip()
            url = _cell_text(row.get("Instagram URL")).strip()
            is_instagram = _is_instagram_url(url)
            is_article = _is_article_url(url)
            media_type = _cell_text(row.get("Media Type")).strip().lower()
            generated = _cell_text(row.get("Generated Caption")).strip()
            original_caption = _cell_text(row.get("Original Caption")).strip()
            transcript = _cell_text(row.get("Transcript")).strip()
            speaker_name = _cell_text(row.get("Speaker Name"))
            status = _cell_text(row.get("Status")).strip()

            row_container = st.container()
            with row_container:
                st.markdown(
                    f'<span id="workspace-row-{row_num}" class="workspace-list-row-anchor"></span>'
                    '<div class="workspace-edit-main-anchor"></div>',
                    unsafe_allow_html=True,
                )
                top_left, top_right = st.columns([0.9, 1.1], vertical_alignment="top")
                with top_left:
                    thumb_link = _cell_text(row.get("Thumbnail Drive Link")).strip()
                    fallback_image_path = _row_fallback_image_path(media_type)
                    if thumb_link:
                        image_url = _safe_image_url(thumb_link)
                        if image_url and _remote_image_usable(image_url):
                            st.image(image_url, width="stretch")
                        else:
                            _render_dark_media_placeholder("Preview unavailable")
                    elif fallback_image_path:
                        _render_dark_media_placeholder("Preview unavailable")
                    elif is_article:
                        st.info("Article link")
                        if original_caption:
                            st.caption(original_caption[:260] + ("..." if len(original_caption) > 260 else ""))
                    else:
                        _render_dark_media_placeholder("Thumbnail pending")

                with top_right:
                    menu_label = "Photo run" if not _is_reel_url(url) else "Process post"
                    schedule_suffix = (row.get("Scheduled Time", "") or "").strip()
                    status_line = f"Row {row_num} · {media_type or 'pending'} · {status or 'blank'}"
                    if schedule_suffix:
                        status_line = f"{status_line} · {schedule_suffix}"
                    st.markdown(
                        f'<div class="workspace-status-line">{status_line}</div>',
                        unsafe_allow_html=True,
                    )
                    if username:
                        st.markdown(f"#### @{username}" if is_instagram else f"#### {username}")
                    else:
                        st.markdown(f"#### Row {row_num}")

                    st.text_input(
                        "Speaker Name",
                        value=speaker_name,
                        key=speaker_key,
                        placeholder="Enter name",
                        label_visibility="collapsed",
                    )
                    if url:
                        menu_nonce = st.session_state.get(menu_nonce_key, 0)
                        menu_label_with_nonce = f"Actions{chr(0x200B) * menu_nonce}"
                        with st.popover(menu_label_with_nonce, use_container_width=True):
                            st.link_button(
                                "Open in Instagram" if is_instagram else "Open source link",
                                url,
                                width="stretch",
                            )
                            primary_action = "process_post" if is_instagram else "image_text"
                            primary_help = (
                                "Transcribe, generate the caption, and generate slide copy."
                                if _is_reel_url(url)
                                else "Use available post text and image text to generate the caption and slide copy."
                            )
                            if is_instagram and st.button(
                                menu_label,
                                key=f"workspace_menu_primary_{row_num}",
                                disabled=not url,
                                width="stretch",
                                help=primary_help,
                            ):
                                if primary_action == "process_post" and not transcript:
                                    try:
                                        warning = _check_reel_transcript_risk(row)
                                    except Exception as e:
                                        st.session_state["workspace_error"] = f"Row {row_num}: could not check reel size - {describe_error(e)}"
                                        _close_workspace_menu(row)
                                        _rerun_workspace("Edit")
                                    if warning:
                                        st.session_state[warning_key] = warning
                                        _close_workspace_menu(row)
                                        _rerun_workspace("Edit")
                                _close_workspace_menu(row)
                                _queue_workspace_action(row_num, primary_action)
                                _rerun_workspace("Edit")
                            if st.button(
                                "Generate caption",
                                key=f"workspace_menu_generate_{row_num}",
                                width="stretch",
                                help="Generate a caption for this row from its existing source text, transcript, and context.",
                            ):
                                _close_workspace_menu(row)
                                _queue_workspace_action(row_num, "generate_caption")
                                _rerun_workspace("Edit")
                            if st.button(
                                "Update screenshot",
                                key=f"workspace_menu_thumbnail_open_{row_num}",
                                width="stretch",
                            ):
                                _close_workspace_menu(row)
                                st.session_state["workspace_thumbnail_dialog_row"] = row_num
                                _rerun_workspace("Edit")
                            skip_label = "Unskip" if status.strip().lower() == "skipped" else "Skip"
                            if st.button(
                                skip_label,
                                key=f"workspace_menu_skip_{row_num}",
                                width="stretch",
                            ):
                                next_status = _default_editor_status(row) if status.strip().lower() == "skipped" else "skipped"
                                update_status(GOOGLE_SHEET_ID, row_num, next_status)
                                if next_status == "skipped":
                                    if str(st.query_params.get("workspace_row", "") or "") == str(row_num):
                                        st.query_params.pop("workspace_row", None)
                                    if st.session_state.get("workspace_target_row") == str(row_num):
                                        st.session_state.pop("workspace_target_row", None)
                                _close_workspace_menu(row)
                                st.session_state["workspace_success"] = (
                                    f"Row {row_num}: moved back into the main edit list."
                                    if next_status != "skipped"
                                    else f"Row {row_num}: skipped and moved to the bottom."
                                )
                                _rerun_workspace("Edit")
                            if st.button("Add link", key=f"workspace_link_open_{row_num}", width="stretch"):
                                _close_workspace_menu(row)
                                st.session_state["workspace_link_dialog_row"] = row_num
                                _rerun_workspace("Edit")
                            if st.button(
                                "Delete row",
                                key=f"workspace_menu_delete_{row_num}",
                                width="stretch",
                            ):
                                try:
                                    _delete_workspace_row(row)
                                except Exception as e:
                                    st.session_state["workspace_error"] = f"Row {row_num}: could not delete row - {describe_error(e)}"
                                else:
                                    st.session_state["workspace_success"] = f"Row {row_num}: deleted from the sheet."
                                _close_workspace_menu(row)
                                _rerun_workspace("Edit")
                            if st.button(
                                "Close",
                                key=f"workspace_menu_close_{row_num}",
                                width="stretch",
                            ):
                                _close_workspace_menu(row)
                                _rerun_workspace("Edit")

                    transcript_warning = st.session_state.get(warning_key)
                    if transcript_warning:
                        size_label = _format_bytes(transcript_warning["size_bytes"])
                        threshold_label = _format_bytes(transcript_warning["threshold_bytes"])
                        st.warning(
                            f"This reel is {size_label}, which is over the {threshold_label} transcript warning limit. "
                            "Transcription may cost more than usual."
                        )
                        if st.button(
                            "Process post anyway",
                            key=f"workspace_warning_transcribe_{row_num}",
                            type="primary",
                            width="stretch",
                        ):
                            st.session_state.pop(warning_key, None)
                            _queue_workspace_action(row_num, "process_post")
                            _rerun_workspace("Edit")

                    _copy_tabs(
                        row_num,
                        generated,
                        original_caption,
                        transcript,
                        username,
                        speaker_name,
                        _decode_top_comment(st.session_state.get(top_key, row.get("Top Comment", "")).strip())[0],
                        st.session_state.get(hashtags_key, row.get("Required Hashtags", "")).strip(),
                        row.get("Media Drive Link", ""),
                        media_type,
                        url,
                        is_instagram,
                        _cell_text(row.get("text1")).strip(),
                        _cell_text(row.get("text2")).strip(),
                        _cell_text(row.get("text3")).strip(),
                        row,
                        _cell_text(row.get("Thumbnail Drive Link")).strip(),
                    )

            st.divider()

        _scroll_to_editor_row(str(selected_row["row_number"]))

        queue = st.session_state.get("workspace_action_queue", [])
        if queue:
            st.markdown(
                f'<div class="workspace-action-note">{len(queue)} queued action(s) waiting to run.</div>',
                unsafe_allow_html=True,
            )

        dirty_name_rows = _dirty_workspace_speaker_rows(editor_rows)
        st.caption(f"{len(dirty_name_rows)} unsaved name(s).")
        if st.button(
            "Update all names",
            key="workspace_update_all_names_bottom",
            type="primary",
            width="stretch",
            help="Save all edited speaker names to the sheet.",
        ):
            _handle_update_all_workspace_speaker_names(editor_rows)

with section_tabs[3]:
    st.caption("Data view for the Google Sheet plus batch ingest.")

    if workspace_rows_error:
        st.error(f"Could not load sheet: {workspace_rows_error}")
        all_rows = []
    else:
        all_rows = workspace_rows

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
            width="stretch",
            hide_index=True,
            column_config={
                "Instagram URL": st.column_config.LinkColumn("Instagram URL"),
                "Generated Caption": st.column_config.TextColumn("Generated Caption", width="large"),
            },
        )
    else:
        st.info("No rows in sheet yet.")

    if st.button("Process new rows", type="primary", width="stretch", key="workspace_process_rows"):
        try:
            processed_count = _process_pending_rows_from_sheet()
        except Exception as e:
            st.error(f"Could not process new rows: {describe_error(e)}")
        else:
            if not processed_count:
                st.info("No new rows to process.")
            else:
                st.success(f"Done. Ingested {processed_count} row(s).")
                _rerun_workspace("Data")
