"""Unified workspace shell for the next UI redesign."""

from datetime import datetime, time as dt_time, timedelta
import hashlib
import os
import re
import shutil
import sys
import time
import json
from urllib.parse import parse_qs, quote, urlparse
import html
import requests
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openai
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from article_source import fetch_article_source
from config import DEFAULT_POST_FOOTER, GOOGLE_SHEET_ID, OPENAI_API_KEY
from ingest_helpers import upload_media_bundle
from pipeline_caption import generate_row_caption, _strip_top_comment_paragraphs
from post_scraper import process_url as process_post_url
from reel_scraper import process_url as process_reel_url
import sheets as sheet_ops
from utils.auth import require_auth
from utils.error_labels import describe_error
from utils.styles import inject as inject_styles

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
TRANSCRIPT_SIZE_WARNING_BYTES = 100 * 1024 * 1024
EDITOR_INITIAL_RENDER_LIMIT = 12
client = openai.OpenAI(api_key=OPENAI_API_KEY, timeout=45.0, max_retries=1)
PINNED_TOP_COMMENT_PREFIX = "[[TOP]] "

get_all_rows = sheet_ops.get_all_rows
get_pending_rows = sheet_ops.get_pending_rows
update_caption = sheet_ops.update_caption
update_caption_and_metadata = getattr(sheet_ops, "update_caption_and_metadata", None)
update_caption_context = sheet_ops.update_caption_context
update_ingest_result = sheet_ops.update_ingest_result
update_metadata = sheet_ops.update_metadata
update_scheduled_times = sheet_ops.update_scheduled_times
update_transcript = sheet_ops.update_transcript
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


def _workspace_row_state_keys_for_token(token: str) -> list[str]:
    return [
        f"workspace_speaker_{token}",
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
    ]


def _workspace_key(row: dict, name: str) -> str:
    return f"workspace_{name}_{_row_state_token(row)}"


def _workspace_row_state_keys(row: dict) -> list[str]:
    return _workspace_row_state_keys_for_token(_row_state_token(row))


def _sync_workspace_row_state(row: dict) -> None:
    identity_key = _workspace_stable_row_key(row, "identity")
    token_key = _workspace_stable_row_key(row, "state_token")
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
        for token in tokens_to_clear:
            for key in _workspace_row_state_keys_for_token(token):
                st.session_state.pop(key, None)
    st.session_state[identity_key] = current_identity
    st.session_state[token_key] = current_token


def _clear_workspace_row_state(row: dict) -> None:
    identity_key = _workspace_stable_row_key(row, "identity")
    token_key = _workspace_stable_row_key(row, "state_token")
    previous_token = st.session_state.get(token_key)
    tokens_to_clear = {_row_state_token(row)}
    if previous_token:
        tokens_to_clear.add(previous_token)
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
        "Generate headline": "Generate",
        "Caption this": "Caption",
        "Download media": "Download",
    }.get(mode, "Add")


def _mode_uses_org_hashtag(mode: str) -> bool:
    return mode in {"Add to sheet", "Caption this"}


def _clean_home_links() -> list[str]:
    return [link.strip() for link in st.session_state.get("workspace_home_links", []) if link.strip()]


def _row_is_dirty(row: dict) -> bool:
    speaker_key = _workspace_key(row, "speaker")
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
    def row_sort_key(row: dict) -> tuple[int, int]:
        has_caption = bool((row.get("Generated Caption") or "").strip())
        has_transcript = bool((row.get("Transcript") or "").strip())
        is_skipped = (row.get("Status") or "").strip().lower() == "skipped"
        if is_skipped:
            group = 3
        elif not has_caption:
            group = 0
        elif has_caption and not has_transcript:
            group = 1
        elif has_caption and has_transcript:
            group = 2
        else:
            group = 3
        return group, row.get("row_number", 0)

    return sorted(
        rows,
        key=row_sort_key,
    )


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
        return _drive_image_url(thumb_link) or thumb_link
    return ""


def _visible_rows_with_target(rows: list[dict], limit: int, target_row_number: str = "") -> list[dict]:
    visible_rows = rows[:limit]
    if target_row_number:
        target_row = next((row for row in rows if str(row.get("row_number", "")) == target_row_number), None)
        if target_row and all(row.get("row_number") != target_row.get("row_number") for row in visible_rows):
            visible_rows = [*visible_rows, target_row]
    return visible_rows


def _render_editor_grid(editor_rows: list[dict]) -> None:
    cards = []
    for row in editor_rows:
        row_num = row.get("row_number")
        username = _cell_text(row.get("Source Username")).strip().lstrip("@")
        media_type = _cell_text(row.get("Media Type")).strip().lower() or "post"
        image_url = _grid_preview_url(row)
        badge_html = "".join(
            f'<span class="workspace-grid-badge" title="{html.escape(title)}">{html.escape(label)}</span>'
            for label, title in _grid_badges(row)
        )
        label = f"@{username}" if username else f"Row {row_num}"
        href = f"?workspace_row={row_num}#workspace-row-{row_num}"
        if image_url:
            media_html = f'<img src="{html.escape(image_url)}" alt="{html.escape(label)}" loading="lazy" decoding="async">'
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
    candidate = _drive_image_url(raw_value) or _cell_text(raw_value).strip()
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


def _apply_top_comment_to_caption(
    row: dict,
    row_num: int,
    speaker_name: str,
    top_comment: str,
) -> None:
    current_context = st.session_state.get(_workspace_key(row, "context"), row.get("Caption Context", "")).strip()
    current_speaker = st.session_state.get(_workspace_key(row, "speaker"), speaker_name).strip()
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
        _workspace_key(row, "speaker"),
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
    updated_count = 0
    for row in rows:
        speaker_key = _workspace_key(row, "speaker")
        current_speaker = _cell_text(st.session_state.get(speaker_key, row.get("Speaker Name", ""))).strip()
        saved_speaker = _cell_text(row.get("Speaker Name")).strip()
        if current_speaker == saved_speaker:
            continue
        current_context = _cell_text(st.session_state.get(_workspace_key(row, "context"), row.get("Caption Context", ""))).strip()
        current_hashtags = _cell_text(st.session_state.get(_workspace_key(row, "hashtags"), row.get("Required Hashtags", ""))).strip()
        current_top = _cell_text(st.session_state.get(_workspace_key(row, "top"), row.get("Top Comment", ""))).strip()
        update_metadata(
            GOOGLE_SHEET_ID,
            row["row_number"],
            current_context,
            current_speaker,
            current_hashtags,
            current_top,
            "",
        )
        updated_count += 1
    return updated_count


def _dirty_workspace_speaker_rows(rows: list[dict]) -> list[dict]:
    return [
        row for row in rows
        if _cell_text(st.session_state.get(_workspace_key(row, "speaker"), row.get("Speaker Name", ""))).strip()
        != _cell_text(row.get("Speaker Name")).strip()
    ]


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


def _copy_block(label: str, value: str, key: str, empty_text: str = "(none)") -> None:
    display_text = value or empty_text
    escaped_label = html.escape(label)
    clipboard_text = json.dumps(value or "")
    component_html = f"""
    <div style="margin-top:0.25rem;" id="{html.escape(key)}">
      <div style="
        min-height: 3.25rem;
        white-space: pre-wrap;
        border: 1px solid rgba(15,23,42,0.08);
        border-radius: 16px;
        background: #f8fafc;
        padding: 0.8rem 0.9rem;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 0.88rem;
        line-height: 1.32;
        color: #0f172a;
      ">{html.escape(display_text)}</div>
      <button
        onclick='navigator.clipboard.writeText({clipboard_text})'
        style="
          width: 100%;
          margin-top: 0.5rem;
          border: 1px solid rgba(15,23,42,0.14);
          border-radius: 12px;
          background: white;
          color: #0f172a;
          padding: 0.55rem 0.8rem;
          font-size: 0.92rem;
          font-weight: 600;
        cursor: pointer;
        "
      >Copy {escaped_label}</button>
    </div>
    """
    st.html(component_html)


def _tab_copy_preview(value: str) -> None:
    st.code(value or "(none)", language=None)
    st.markdown(
        f'<div class="workspace-plain-copy-text">{html.escape(value or "(none)")}</div>',
        unsafe_allow_html=True,
    )


def _copy_tabs(
    row_num: int,
    generated: str,
    original_caption: str,
    transcript: str,
    username: str,
    top_comment: str,
    required_hashtags: str,
    media_link: str = "",
    media_type: str = "",
    source_url: str = "",
    is_instagram: bool = True,
) -> None:
    tab_labels = ["Caption", "Original caption"]
    if is_instagram:
        tab_labels.append("Transcript")
    media_links = [link.strip() for link in (media_link or "").split(",") if link.strip()]
    if media_links:
        tab_labels.append("Media")
    text_tabs = st.tabs(tab_labels)
    original_preview = _build_original_caption_preview(
        original_caption,
        username,
        top_comment,
        required_hashtags,
        is_instagram=is_instagram,
    )
    with text_tabs[0]:
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
    with text_tabs[1]:
        _tab_copy_preview(original_preview)
    next_tab_index = 2
    if is_instagram:
        with text_tabs[next_tab_index]:
            _tab_copy_preview(transcript)
        next_tab_index += 1
    if media_links:
        with text_tabs[next_tab_index]:
            st.markdown(
                f'<div class="workspace-plain-copy-text">Drive media link{"" if len(media_links) == 1 else "s"}.</div>',
                unsafe_allow_html=True,
            )
            st.code("\n".join(media_links), language=None)
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
                if result["status"] == "ingested" and result["media_type"] == "article":
                    existing_inputs = _current_row_caption_inputs(row)
                    article_row = dict(row)
                    article_row.update(
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
                            "Top Comment": existing_inputs["Top Comment"],
                            "Footer": "",
                        }
                    )
                    generated_caption = generate_row_caption(article_row)
                    if update_caption_and_metadata is not None:
                        update_caption_and_metadata(
                            GOOGLE_SHEET_ID,
                            row_num,
                            generated_caption,
                            result["status"],
                            existing_inputs["Caption Context"],
                            existing_inputs["Speaker Name"],
                            existing_inputs["Required Hashtags"],
                            existing_inputs["Top Comment"],
                            "",
                        )
                    else:
                        update_metadata(
                            GOOGLE_SHEET_ID,
                            row_num,
                            existing_inputs["Caption Context"],
                            existing_inputs["Speaker Name"],
                            existing_inputs["Required Hashtags"],
                            existing_inputs["Top Comment"],
                            "",
                        )
                        update_caption(GOOGLE_SHEET_ID, row_num, generated_caption, result["status"])
            except Exception as e:
                status_box.update(label=f"Row {row_num}: error writing to sheet - {describe_error(e)}", state="error")
            else:
                if result["status"].startswith("error"):
                    status_box.update(label=f"Row {row_num}: {result['status']}", state="error")
                else:
                    action_word = "ingested + captioned" if result["media_type"] == "article" else "ingested"
                    display_name = f"@{result['username']}" if result["username"] and result["media_type"] != "article" else result["username"]
                    status_box.update(
                        label=f"Row {row_num}: {action_word} - {display_name} ({result['media_type']})",
                        state="complete",
                    )
        progress.progress((i + 1) / len(pending))

    return len(pending)


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
            "status": f"error: {describe_error(e)}",
        }
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _rerun_with_transcript(row: dict, force_remote: bool = False) -> None:
    updated_row = _fetch_row_with_transcript(row, force_remote=force_remote)
    row_num = row["row_number"]
    caption = generate_row_caption(updated_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)


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
        if not transcript:
            raise ValueError("Apify did not return a transcript for this reel.")

        if download_media:
            uploaded = upload_media_bundle(refreshed)
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
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)


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
        if action == "transcript":
            with st.spinner(f"Refreshing row {row_number} with transcript..."):
                _rerun_with_transcript(row, force_remote=True)
            st.session_state["workspace_success"] = f"Row {row_number}: transcript rerun complete."
        elif action == "generate_caption":
            with st.spinner(f"Generating caption for row {row_number}..."):
                _generate_caption_for_row(row)
            st.session_state["workspace_success"] = f"Row {row_number}: caption generated."
        elif action == "image_text":
            with st.spinner(f"Extracting image text for row {row_number}..."):
                _redo_caption_from_image_text(row)
            st.session_state["workspace_success"] = f"Row {row_number}: caption regenerated from image text."
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
                "Source Username": source.get("username", "") if source.get("is_instagram", False) else "",
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

active_tab = st.radio(
    "Workspace section",
    ["Home", "Actions", "Data"],
    horizontal=True,
    key="workspace_active_tab",
    label_visibility="collapsed",
)

if active_tab == "Actions":
    home_notice = st.session_state.pop("workspace_home_notice", "")

    mode_help = {
        "Add to sheet": "Add an Instagram post or article link to the sheet so it can be processed into the editor.",
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
        clear_col, action_col = st.columns([1, 3])
        with clear_col:
            if st.button("Clear", width="stretch", key="workspace_home_clear"):
                st.session_state.pop("workspace_home_results", None)
                st.session_state.pop("workspace_home_notice", None)
                _reset_home_links_on_next_render()
                _rerun_workspace("Actions")
        with action_col:
            submitted = st.button(_action_label(mode), type="primary", width="stretch")
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
                        st.code(headline or "(none)", language=None)
                with headline_tabs[3]:
                    st.code(item.get("caption", "") or "(none)", language=None)

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
if active_tab == "Home":
    default_day, default_time = _schedule_day_defaults()
    default_hour, default_minute, default_suffix = _time_parts(default_time)
    st.session_state.setdefault("workspace_schedule_day", default_day)
    st.session_state.setdefault("workspace_schedule_hour", default_hour)
    st.session_state.setdefault("workspace_schedule_minute", default_minute)
    st.session_state.setdefault("workspace_schedule_suffix", default_suffix)

    schedule_apply_requested = False

    try:
        pending_edit_rows = _run_with_sheet_quota_countdown(
            lambda: get_pending_rows(GOOGLE_SHEET_ID),
            "Checking for new rows paused:",
        )
    except Exception as e:
        st.error(f"Could not check for new rows: {describe_error(e)}")
        pending_edit_rows = []

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
    try:
        editor_rows = _sort_editor_rows(_run_with_sheet_quota_countdown(
            lambda: [
                r for r in get_all_rows(GOOGLE_SHEET_ID)
                if _is_editable_row(r)
            ],
            "Loading editor rows paused:",
        ))
    except Exception as e:
        st.error(f"Could not load edit rows: {describe_error(e)}")
        editor_rows = []

    dialog_row_number = st.session_state.get("workspace_link_dialog_row")
    if dialog_row_number is not None:
        dialog_row = next((row for row in editor_rows if row.get("row_number") == dialog_row_number), None)
        if dialog_row is None:
            st.session_state.pop("workspace_link_dialog_row", None)
        else:
            _render_workspace_link_dialog(dialog_row)

    last_scheduled_times = _persisted_last_scheduled_time_labels(editor_rows)

    if not editor_rows:
        st.info("No rows yet. Add a link on Actions or process new rows on Data.")
    else:

        query_row = str(st.query_params.get("workspace_row", "") or "")
        if query_row and st.session_state.get("workspace_target_row") != query_row:
            st.session_state["workspace_target_row"] = query_row
        _render_editor_grid(editor_rows)
        show_full_list = st.checkbox(
            "Show full list",
            key="workspace_show_full_list",
        )
        visible_editor_rows = editor_rows if show_full_list else _visible_rows_with_target(
            editor_rows,
            EDITOR_INITIAL_RENDER_LIMIT,
            query_row,
        )
        if len(visible_editor_rows) < len(editor_rows):
            st.caption(f"Showing {len(visible_editor_rows)} of {len(editor_rows)} rows. Rows stay here until you delete them from the sheet.")
        else:
            st.caption("Rows stay here until you delete them from the sheet.")
        for row in visible_editor_rows:
            _sync_workspace_row_state(row)
            row_num = row["row_number"]
            speaker_key = _workspace_key(row, "speaker")
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
                    if thumb_link:
                        image_url = _safe_image_url(thumb_link)
                        if image_url:
                            st.image(image_url, width="stretch")
                        else:
                            st.info("Thumbnail link is unavailable.")
                    elif is_article:
                        st.info("Article link")
                        if original_caption:
                            st.caption(original_caption[:260] + ("..." if len(original_caption) > 260 else ""))
                    else:
                        st.info("Thumbnail will appear here after ingest.")

                with top_right:
                    menu_label = "Photo run" if not _is_reel_url(url) else "Transcribe"
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
                    )
                    if _is_reel_url(url):
                        pending_transcribe_resets = st.session_state.setdefault("workspace_transcribe_reset_rows", [])
                        if transcribe_key in pending_transcribe_resets:
                            st.session_state.pop(transcribe_key, None)
                            st.session_state["workspace_transcribe_reset_rows"] = [
                                pending for pending in pending_transcribe_resets if pending != transcribe_key
                            ]
                        st.checkbox(
                            "Check to transcribe",
                            value=bool(st.session_state.get(transcribe_key, False)),
                            key=transcribe_key,
                        )
                    if url:
                        st.link_button("Open in Instagram" if is_instagram else "Open source link", url, width="stretch")
                        menu_nonce = st.session_state.get(menu_nonce_key, 0)
                        menu_label_with_nonce = f"Actions{chr(0x200B) * menu_nonce}"
                        with st.popover(menu_label_with_nonce, use_container_width=True):
                            primary_action = "transcript" if _is_reel_url(url) else "image_text"
                            primary_help = "Fetch transcript and regenerate caption." if _is_reel_url(url) else "Extract text from images and regenerate caption."
                            if is_instagram and st.button(
                                menu_label,
                                key=f"workspace_menu_primary_{row_num}",
                                disabled=not url,
                                width="stretch",
                                help=primary_help,
                            ):
                                if primary_action == "transcript" and not transcript:
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
                            dirty_name_count = len(_dirty_workspace_speaker_rows(editor_rows))
                            if st.button(
                                "Update all names",
                                key=f"workspace_menu_update_names_{row_num}",
                                width="stretch",
                                disabled=dirty_name_count == 0,
                                help="Save all edited speaker names to the sheet.",
                            ):
                                try:
                                    updated_count = _save_all_workspace_speaker_names(editor_rows)
                                except Exception as e:
                                    st.session_state["workspace_error"] = f"Could not save names: {describe_error(e)}"
                                else:
                                    st.session_state["workspace_success"] = (
                                        f"Updated {updated_count} speaker name(s)."
                                        if updated_count
                                        else "No name changes to save."
                                    )
                                _close_workspace_menu(row)
                                _rerun_workspace("Edit")
                            if url and st.button("Add Watch", key=f"workspace_watch_add_{row_num}", width="stretch"):
                                top_comment = _build_watch_cta(username or speaker_name, url)
                                try:
                                    _apply_top_comment_to_caption(row, row_num, speaker_name, top_comment)
                                except Exception as e:
                                    st.session_state["workspace_error"] = f"Row {row_num}: could not save watch CTA - {describe_error(e)}"
                                else:
                                    st.session_state["workspace_success"] = f"Row {row_num}: watch CTA saved to generated caption."
                                _close_workspace_menu(row)
                                _rerun_workspace("Edit")
                            skip_label = "Unskip" if status.strip().lower() == "skipped" else "Skip"
                            if st.button(
                                skip_label,
                                key=f"workspace_menu_skip_{row_num}",
                                width="stretch",
                            ):
                                next_status = _default_editor_status(row) if status.strip().lower() == "skipped" else "skipped"
                                update_status(GOOGLE_SHEET_ID, row_num, next_status)
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

                    transcript_warning = st.session_state.get(warning_key)
                    if transcript_warning:
                        size_label = _format_bytes(transcript_warning["size_bytes"])
                        threshold_label = _format_bytes(transcript_warning["threshold_bytes"])
                        st.warning(
                            f"This reel is {size_label}, which is over the {threshold_label} transcript warning limit. "
                            "Transcription may cost more than usual."
                        )
                        if st.button(
                            "Transcribe anyway",
                            key=f"workspace_warning_transcribe_{row_num}",
                            type="primary",
                            width="stretch",
                        ):
                            st.session_state.pop(warning_key, None)
                            _queue_workspace_action(row_num, "transcript")
                            _rerun_workspace("Edit")

                    st.markdown('<div class="workspace-section-label workspace-content-tabs">Content</div>', unsafe_allow_html=True)
                    _copy_tabs(
                        row_num,
                        generated,
                        original_caption,
                        transcript,
                        username,
                        _decode_top_comment(st.session_state.get(top_key, row.get("Top Comment", "")).strip())[0],
                        st.session_state.get(hashtags_key, row.get("Required Hashtags", "")).strip(),
                        row.get("Media Drive Link", ""),
                        media_type,
                        url,
                        is_instagram,
                    )

            st.divider()

        _scroll_to_editor_row(query_row)

        queue = st.session_state.get("workspace_action_queue", [])
        if queue:
            st.markdown(
                f'<div class="workspace-action-note">{len(queue)} queued action(s) waiting to run.</div>',
                unsafe_allow_html=True,
            )

        dirty_name_rows = _dirty_workspace_speaker_rows(editor_rows)
        if dirty_name_rows:
            st.caption(f"{len(dirty_name_rows)} unsaved name(s).")

    with st.expander("Set times", expanded=False):
        st.markdown('<div class="workspace-schedule-anchor"></div>', unsafe_allow_html=True)
        schedule_cols = st.columns([1, 0.7, 0.7, 0.7, 0.4], vertical_alignment="bottom")
        with schedule_cols[0]:
            st.selectbox(
                "Day",
                WEEKDAY_OPTIONS,
                key="workspace_schedule_day",
            )
        with schedule_cols[1]:
            st.selectbox(
                "Hour",
                list(range(1, 13)),
                key="workspace_schedule_hour",
            )
        with schedule_cols[2]:
            st.selectbox(
                "Minute",
                list(range(60)),
                key="workspace_schedule_minute",
                format_func=lambda value: f"{value:02d}",
            )
        with schedule_cols[3]:
            st.selectbox(
                "AM/PM",
                ["am", "pm"],
                key="workspace_schedule_suffix",
            )
        with schedule_cols[4]:
            schedule_apply_requested = st.button("Set", key="workspace_schedule_set", type="primary", width="stretch")

        if schedule_apply_requested:
            start_day = st.session_state.get("workspace_schedule_day", default_day)
            start_time = _time_from_parts(
                int(st.session_state.get("workspace_schedule_hour", default_hour)),
                int(st.session_state.get("workspace_schedule_minute", default_minute)),
                st.session_state.get("workspace_schedule_suffix", default_suffix),
            )
            schedule_rows = sorted(editor_rows, key=lambda row: row.get("row_number", 0))
            assignments = _build_schedule_labels(schedule_rows, start_day, start_time)
            try:
                update_scheduled_times(GOOGLE_SHEET_ID, assignments)
                if assignments:
                    latest_entry = list(assignments.values())[-1]
                    existing_entries = get_last_scheduled_times(GOOGLE_SHEET_ID)
                    update_last_scheduled_times(GOOGLE_SHEET_ID, [latest_entry, *existing_entries][:3])
            except Exception as e:
                st.session_state["workspace_error"] = f"Could not save schedule: {describe_error(e)}"
            else:
                row_word = "row" if len(assignments) == 1 else "rows"
                st.session_state["workspace_success"] = f"Updated schedule for {len(assignments)} {row_word}."
            _rerun_workspace("Edit")

    if last_scheduled_times:
        schedule_summary = " · ".join(last_scheduled_times)
        st.markdown(
            f'<div class="workspace-plain-copy-text">Last scheduled entries: {html.escape(schedule_summary)}</div>',
            unsafe_allow_html=True,
        )

if active_tab == "Data":
    st.caption("Data view for the Google Sheet plus batch ingest.")

    try:
        all_rows = _run_with_sheet_quota_countdown(
            lambda: get_all_rows(GOOGLE_SHEET_ID),
            "Loading rows paused:",
        )
    except Exception as e:
        st.error(f"Could not load sheet: {describe_error(e)}")
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
