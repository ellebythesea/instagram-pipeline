"""Google Sheets helper — read rows and write pipeline results.

Column order is fixed per spec. Headers are used for reads (get_all_records),
column letter ranges are used for writes since ranges are inherently positional.

Sheet layout:
  A  Instagram URL      B  Required Hashtags  C  Source Username
  D  Generated Caption  E  Media Type         F  Photo Count
  G  Media Drive Link   H  Thumbnail Drive Link
  I  Original Caption   J  Transcript         K  Top Comment
  L  Speaker Name       M  Footer             N  Status
  O  Caption Context    P  Scheduled Time     Q  #name
  R  #text1             S  #text2             T  #text3
  U  Slide CTA          V  text4             W  text5
  X  text6
"""

import json
import os
import random
import time
from json import JSONDecodeError

import gspread
from google.oauth2.service_account import Credentials

from config import GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_WORKSHEET_NAME

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

_EXPECTED_HEADERS = [
    "Instagram URL",
    "Required Hashtags",
    "Source Username",
    "Generated Caption",
    "Media Type",
    "Photo Count",
    "Media Drive Link",
    "Thumbnail Drive Link",
    "Original Caption",
    "Transcript",
    "Top Comment",
    "Speaker Name",
    "Footer",
    "Status",
    "Caption Context",
    "Scheduled Time",
    "name",
    "text1",
    "text2",
    "text3",
    "Slide CTA",
    "text4",
    "text5",
    "text6",
]

_headers_checked: set[tuple[str, str]] = set()
_client: gspread.Client | None = None
_workbooks: dict[str, gspread.Spreadsheet] = {}
_worksheets: dict[tuple[str, str], gspread.Worksheet] = {}
_rows_cache: dict[tuple[str, str], tuple[float, list[dict]]] = {}
_ROWS_CACHE_TTL_SECONDS = 20.0
_METADATA_SHEET_TITLE = "__workspace_meta__"
_LAST_SCHEDULED_TIMES_KEY = "last_scheduled_times"
_SLIDE_CTA_OPTIONS_KEY = "slide_cta_options"
_FUNDRAISING_SHEET_TITLE = "fundraising"


def _get_client() -> gspread.Client:
    global _client
    if _client is not None:
        return _client

    creds_src = GOOGLE_SERVICE_ACCOUNT_JSON
    if not creds_src:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not configured. Add it to "
            ".streamlit/local_secrets.toml, set it as an environment variable, "
            "or set it to a service-account JSON file path."
        )
    if os.path.isfile(creds_src):
        creds = Credentials.from_service_account_file(creds_src, scopes=_SCOPES)
    else:
        try:
            creds_info = json.loads(creds_src)
        except JSONDecodeError as exc:
            raise RuntimeError(
                "GOOGLE_SERVICE_ACCOUNT_JSON must be either a valid service-account "
                "JSON object or a path to a service-account JSON file."
            ) from exc
        creds = Credentials.from_service_account_info(creds_info, scopes=_SCOPES)
    _client = gspread.authorize(creds)
    return _client


def _workbook(sheet_id: str):
    cleaned_sheet_id = (sheet_id or "").strip()
    if not cleaned_sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID is not configured.")
    if cleaned_sheet_id not in _workbooks:
        _workbooks[cleaned_sheet_id] = _with_backoff(_get_client().open_by_key, cleaned_sheet_id)
    return _workbooks[cleaned_sheet_id]


def _named_worksheet(sheet_id: str, title: str) -> gspread.Worksheet:
    cleaned_sheet_id = (sheet_id or "").strip()
    cleaned_title = (title or "").strip()
    cache_key = (cleaned_sheet_id, cleaned_title)
    if cache_key not in _worksheets:
        _worksheets[cache_key] = _workbook(cleaned_sheet_id).worksheet(cleaned_title)
    return _worksheets[cache_key]


def _with_backoff(fn, *args, **kwargs):
    delay = 1.0
    for attempt in range(5):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            message = str(e)
            if "Exceeded in a metric read request" not in message and "429" not in message:
                raise
            if attempt == 4:
                raise
            time.sleep(delay + random.uniform(0, 0.5))
            delay = min(delay * 2, 8.0)


def _worksheet(sheet_id: str) -> gspread.Worksheet:
    cache_key = (sheet_id, "__main__")
    if cache_key in _worksheets:
        return _worksheets[cache_key]

    workbook = _workbook(sheet_id)
    configured_title = (GOOGLE_WORKSHEET_NAME or "").strip()
    expected_headers = {"Instagram URL", "Status"}

    if configured_title:
        try:
            ws = _named_worksheet(sheet_id, configured_title)
            headers = {h.strip() for h in ws.row_values(1) if h.strip()}
            if expected_headers.issubset(headers):
                _ensure_headers(sheet_id, ws)
                _worksheets[cache_key] = ws
                return ws
        except gspread.WorksheetNotFound:
            pass

    for ws in workbook.worksheets():
        headers = {h.strip() for h in ws.row_values(1) if h.strip()}
        if expected_headers.issubset(headers):
            _ensure_headers(sheet_id, ws)
            _worksheets[cache_key] = ws
            return ws

    if configured_title:
        raise RuntimeError(
            f"Worksheet '{configured_title}' was not found or does not contain the expected pipeline headers."
        )

    ws = workbook.sheet1
    _ensure_headers(sheet_id, ws)
    _worksheets[cache_key] = ws
    return ws


def _metadata_worksheet(sheet_id: str) -> gspread.Worksheet:
    workbook = _workbook(sheet_id)
    try:
        ws = _named_worksheet(sheet_id, _METADATA_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = workbook.add_worksheet(title=_METADATA_SHEET_TITLE, rows=10, cols=2)
        _worksheets[(sheet_id, _METADATA_SHEET_TITLE)] = ws
        _with_backoff(ws.update, "A1:B1", [["key", "value"]])
    return ws


def _optional_worksheet(sheet_id: str, title: str) -> gspread.Worksheet | None:
    try:
        return _named_worksheet(sheet_id, title)
    except gspread.WorksheetNotFound:
        return None


def _ensure_headers(sheet_id: str, ws: gspread.Worksheet) -> None:
    """Restore the expected header row if it is missing or incorrect."""
    cache_key = (sheet_id, ws.title)
    if cache_key in _headers_checked:
        return
    current = _with_backoff(ws.row_values, 1)
    normalized = current[:len(_EXPECTED_HEADERS)]
    if normalized != _EXPECTED_HEADERS:
        _with_backoff(ws.update, "A1:X1", [_EXPECTED_HEADERS])
    _headers_checked.add(cache_key)


def _invalidate_rows_cache(sheet_id: str) -> None:
    stale_keys = [key for key in _rows_cache if key[0] == sheet_id]
    for key in stale_keys:
        _rows_cache.pop(key, None)


def _get_cached_rows(sheet_id: str, tab_name: str) -> list[dict] | None:
    cached = _rows_cache.get((sheet_id, tab_name))
    if not cached:
        return None
    cached_at, rows = cached
    if time.monotonic() - cached_at > _ROWS_CACHE_TTL_SECONDS:
        _rows_cache.pop((sheet_id, tab_name), None)
        return None
    return [row.copy() for row in rows]


def _set_cached_rows(sheet_id: str, tab_name: str, rows: list[dict]) -> None:
    _rows_cache[(sheet_id, tab_name)] = (time.monotonic(), [row.copy() for row in rows])


def get_all_rows(sheet_id: str) -> list[dict]:
    """Return all data rows as dicts keyed by header name, plus row_number."""
    cached = _get_cached_rows(sheet_id, "posts")
    if cached is not None:
        return cached
    ws = _worksheet(sheet_id)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for i, r in enumerate(records):
        r["row_number"] = i + 2  # header is row 1
    _set_cached_rows(sheet_id, "posts", records)
    return [row.copy() for row in records]


def get_pending_rows(sheet_id: str) -> list[dict]:
    """Rows where Status is empty and URL is present."""
    return [
        r for r in get_all_rows(sheet_id)
        if not r.get("Status", "").strip() and r.get("Instagram URL", "").strip()
    ]


def get_ingested_rows(sheet_id: str) -> list[dict]:
    """Rows where Status is 'ingested'."""
    return [
        r for r in get_all_rows(sheet_id)
        if r.get("Status", "").strip().lower() == "ingested"
    ]


def append_link_rows(sheet_id: str, urls: list[str], required_hashtags: str = "") -> None:
    """Append new rows with Instagram URL and optional required hashtags."""
    cleaned_urls = [url.strip() for url in urls if url.strip()]
    if not cleaned_urls:
        return

    ws = _worksheet(sheet_id)
    rows = []
    for url in cleaned_urls:
        row = [""] * len(_EXPECTED_HEADERS)
        row[0] = url
        row[1] = required_hashtags.strip()
        rows.append(row)
    _with_backoff(ws.append_rows, rows, value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def append_generated_post_rows(sheet_id: str, rows: list[dict]) -> None:
    """Append pre-generated rows to the main posts tab."""
    cleaned_rows = [row for row in rows if (row.get("url") or "").strip()]
    if not cleaned_rows:
        return

    values = []
    for source in cleaned_rows:
        row = [""] * len(_EXPECTED_HEADERS)
        row[0] = source.get("url", "").strip()
        row[2] = source.get("source_username", "").strip()
        row[3] = source.get("caption", "").strip()
        row[4] = source.get("media_type", "").strip()
        row[8] = source.get("original_caption", "").strip()
        row[13] = source.get("status", "").strip()
        row[14] = source.get("caption_context", "").strip()
        row[16] = source.get("name", "").strip()
        row[17] = source.get("text1", "").strip()
        row[18] = source.get("text2", "").strip()
        row[19] = source.get("text3", "").strip()
        row[20] = source.get("slide_cta", "").strip()
        row[21] = source.get("text4", "").strip()
        row[22] = source.get("text5", "").strip()
        row[23] = source.get("text6", "").strip()
        values.append(row)

    ws = _worksheet(sheet_id)
    _with_backoff(ws.append_rows, values, value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def update_ingest_result(
    sheet_id: str,
    row_number: int,
    username: str,
    media_type: str,
    photo_count: int,
    media_link: str,
    thumbnail_link: str,
    original_caption: str,
    transcript: str,
    status: str,
) -> None:
    """Write ingest results to cols C and E-J, default name to Q, and status to N."""
    ws = _worksheet(sheet_id)
    cleaned_username = (username or "").strip()
    default_name = cleaned_username if not cleaned_username or cleaned_username.startswith("@") else f"@{cleaned_username}"
    _with_backoff(
        ws.batch_update,
        [
            {"range": f"C{row_number}", "values": [[username]]},
            {
                "range": f"E{row_number}:J{row_number}",
                "values": [[
                    media_type,
                    str(photo_count) if photo_count else "",
                    media_link,
                    thumbnail_link,
                    original_caption,
                    transcript,
                ]],
            },
            {"range": f"N{row_number}", "values": [[status]]},
            {"range": f"Q{row_number}", "values": [[default_name]]},
        ],
    )
    _invalidate_rows_cache(sheet_id)


def update_caption(sheet_id: str, row_number: int, caption: str, status: str) -> None:
    """Write generated caption to col D and status to col N."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"D{row_number}", [[caption]])
    _with_backoff(ws.update, f"N{row_number}", [[status]])
    _invalidate_rows_cache(sheet_id)


def update_caption_and_metadata(
    sheet_id: str,
    row_number: int,
    caption: str,
    status: str,
    caption_context: str,
    speaker_name: str,
    hashtags: str,
    top_comment: str,
    footer: str,
) -> None:
    """Write generated caption, status, and editor metadata after one worksheet lookup."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        [
            {"range": f"D{row_number}", "values": [[caption]]},
            {"range": f"B{row_number}", "values": [[hashtags]]},
            {"range": f"K{row_number}:M{row_number}", "values": [[top_comment, speaker_name, footer]]},
            {"range": f"N{row_number}:O{row_number}", "values": [[status, caption_context]]},
        ],
    )
    _invalidate_rows_cache(sheet_id)


def update_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write status to col N for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"N{row_number}", [[status]])
    _invalidate_rows_cache(sheet_id)


def update_transcript(sheet_id: str, row_number: int, transcript: str) -> None:
    """Write transcript to col J for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"J{row_number}", [[transcript]])
    _invalidate_rows_cache(sheet_id)


def update_thumbnail_link(sheet_id: str, row_number: int, thumbnail_link: str) -> None:
    """Write thumbnail drive link to col H for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"H{row_number}", [[thumbnail_link]])
    _invalidate_rows_cache(sheet_id)


def update_caption_context(sheet_id: str, row_number: int, caption_context: str) -> None:
    """Write caption context to col O for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"O{row_number}", [[caption_context]])
    _invalidate_rows_cache(sheet_id)


def update_scheduled_times(sheet_id: str, assignments: dict[int, str]) -> None:
    """Write scheduled time values to col P for multiple rows."""
    if not assignments:
        return
    ws = _worksheet(sheet_id)
    for row_number, scheduled_time in assignments.items():
        _with_backoff(ws.update, f"P{row_number}", [[scheduled_time]])
    _invalidate_rows_cache(sheet_id)


def update_carousel_fields(
    sheet_id: str,
    row_number: int,
    name: str,
    text1: str,
    text2: str,
    text3: str,
) -> None:
    """Write Figma/Google Sync carousel fields to cols Q-T and set status to 'slides'."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        [
            {"range": f"N{row_number}", "values": [["slides"]]},
            {"range": f"Q{row_number}:T{row_number}", "values": [[name, text1, text2, text3]]},
        ],
    )
    _invalidate_rows_cache(sheet_id)


def get_last_scheduled_times(sheet_id: str) -> list[str]:
    """Return the last saved workspace scheduled times from metadata."""
    ws = _metadata_worksheet(sheet_id)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for record in records:
        key = (record.get("key", "") or "").strip()
        if key not in {"last_scheduled_time", _LAST_SCHEDULED_TIMES_KEY}:
            continue
        raw_value = (record.get("value", "") or "").strip()
        if not raw_value:
            return []
        if key == "last_scheduled_time":
            return [raw_value]
        try:
            values = json.loads(raw_value)
        except json.JSONDecodeError:
            return [raw_value]
        if isinstance(values, list):
            return [str(value).strip() for value in values if str(value).strip()]
        return [str(values).strip()] if str(values).strip() else []
    return []


def update_last_scheduled_times(sheet_id: str, scheduled_times: list[str]) -> None:
    """Persist the last assigned workspace scheduled times in metadata."""
    ws = _metadata_worksheet(sheet_id)
    payload = json.dumps([value.strip() for value in scheduled_times if value.strip()])
    records = _with_backoff(ws.get_all_records, default_blank="")
    for index, record in enumerate(records, start=2):
        key = (record.get("key", "") or "").strip()
        if key in {"last_scheduled_time", _LAST_SCHEDULED_TIMES_KEY}:
            _with_backoff(ws.update, f"A{index}:B{index}", [[_LAST_SCHEDULED_TIMES_KEY, payload]])
            return
    _with_backoff(ws.append_row, [_LAST_SCHEDULED_TIMES_KEY, payload], value_input_option="USER_ENTERED")


def get_slide_cta_options(sheet_id: str) -> dict[str, str]:
    """Return saved slide 3 CTA choices keyed by sheet row number."""
    ws = _metadata_worksheet(sheet_id)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for record in records:
        key = (record.get("key", "") or "").strip()
        if key != _SLIDE_CTA_OPTIONS_KEY:
            continue
        raw_value = (record.get("value", "") or "").strip()
        if not raw_value:
            return {}
        try:
            values = json.loads(raw_value)
        except json.JSONDecodeError:
            return {}
        if not isinstance(values, dict):
            return {}
        return {
            str(row_number).strip(): str(option).strip()
            for row_number, option in values.items()
            if str(row_number).strip() and str(option).strip()
        }
    return {}


def update_slide_cta_option(sheet_id: str, row_number: int, option: str) -> None:
    """Persist a row's selected slide CTA in column U of the main sheet."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"U{row_number}", [[(option or "").strip()]])


def get_fundraising_links(sheet_id: str) -> list[dict[str, str]]:
    """Return fundraising top-comment presets from the optional worksheet.

    Expected layout is two columns:
      A: label/name
      B: full top comment text
    The first row may optionally be a header row.
    """
    ws = _optional_worksheet(sheet_id, _FUNDRAISING_SHEET_TITLE)
    if ws is None:
        return []

    values = _with_backoff(ws.get_all_values)
    if not values:
        return []

    presets: list[dict[str, str]] = []
    for index, row in enumerate(values):
        label = row[0].strip() if len(row) > 0 else ""
        link = row[1].strip() if len(row) > 1 else ""
        if not label and not link:
            continue
        if index == 0 and label.lower() in {"name", "label", "fundraising", "preset"} and link.lower() in {"link", "url", "comment", "top comment"}:
            continue
        if not label or not link:
            continue
        presets.append({"label": label, "link": link})
    return presets


def update_metadata(
    sheet_id: str,
    row_number: int,
    caption_context: str,
    speaker_name: str,
    hashtags: str,
    top_comment: str,
    footer: str,
) -> None:
    """Write user metadata to cols B and K-M, and caption context to O."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        [
            {"range": f"B{row_number}", "values": [[hashtags]]},
            {"range": f"K{row_number}:M{row_number}", "values": [[top_comment, speaker_name, footer]]},
            {"range": f"O{row_number}", "values": [[caption_context]]},
        ],
    )
    _invalidate_rows_cache(sheet_id)


def update_speaker_names_batch(sheet_id: str, updates: dict[int, str]) -> None:
    """Write multiple speaker names to column L in one batch."""
    if not updates:
        return
    ws = _worksheet(sheet_id)
    requests = [
        {"range": f"L{row_number}", "values": [[speaker_name]]}
        for row_number, speaker_name in sorted(updates.items())
    ]
    _with_backoff(ws.batch_update, requests)
    _invalidate_rows_cache(sheet_id)


def delete_row(sheet_id: str, row_number: int) -> None:
    """Delete a single sheet row by absolute row number."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.delete_rows, row_number)
    _invalidate_rows_cache(sheet_id)


# ---------------------------------------------------------------------------
# monitors tab helpers
# ---------------------------------------------------------------------------

def get_monitor_rows(sheet_id: str) -> list[dict]:
    """Return all rows from the monitors tab."""
    cached = _get_cached_rows(sheet_id, "monitors")
    if cached is not None:
        return cached
    ws = _named_worksheet(sheet_id, "monitors")
    values = _with_backoff(ws.get_all_values)
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for i, row in enumerate(values[1:], start=2):
        record = {headers[j]: (row[j].strip() if j < len(row) else "") for j in range(len(headers))}
        record["row_number"] = i
        rows.append(record)
    _set_cached_rows(sheet_id, "monitors", rows)
    return rows


def get_open_monitor_rows(sheet_id: str) -> list[dict]:
    """Return rows from the monitors tab where status is 'open'."""
    return [r for r in get_monitor_rows(sheet_id) if r.get("status", "").strip().lower() == "open"]


def update_monitor_summary(sheet_id: str, row_number: int, summary: str, last_checked: str) -> None:
    """Write summary and last checked date to the monitors tab by header name."""
    ws = _named_worksheet(sheet_id, "monitors")
    header_row = _with_backoff(ws.row_values, 1)
    normalized = {h.strip().lower(): i for i, h in enumerate(header_row) if h.strip()}
    last_index = normalized.get("last")
    summary_index = normalized.get("summary")
    if last_index is None:
        raise RuntimeError("monitors tab is missing a 'last' column.")
    if summary_index is None:
        raise RuntimeError("monitors tab is missing a 'summary' column.")
    last_col = chr(ord("A") + last_index)
    summary_col = chr(ord("A") + summary_index)
    _with_backoff(ws.batch_update, [
        {"range": f"{last_col}{row_number}", "values": [[last_checked]]},
        {"range": f"{summary_col}{row_number}", "values": [[summary]]},
    ])
    _invalidate_rows_cache(sheet_id)


# ---------------------------------------------------------------------------
# substack tab helpers
# ---------------------------------------------------------------------------

def get_substack_rows(sheet_id: str) -> list[dict]:
    """Return all rows from the substack tab."""
    cached = _get_cached_rows(sheet_id, "substack")
    if cached is not None:
        return cached
    ws = _named_worksheet(sheet_id, "substack")
    values = _with_backoff(ws.get_all_values)
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for i, row in enumerate(values[1:], start=2):
        record = {headers[j]: (row[j].strip() if j < len(row) else "") for j in range(len(headers))}
        record["row_number"] = i
        rows.append(record)
    _set_cached_rows(sheet_id, "substack", rows)
    return [row.copy() for row in rows]


def get_open_substack_rows(sheet_id: str) -> list[dict]:
    """Return rows from the substack tab where status is 'open'."""
    return [r for r in get_substack_rows(sheet_id) if r.get("status", "").strip().lower() == "open"]


def update_substack_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write status to col C of the substack tab."""
    ws = _named_worksheet(sheet_id, "substack")
    _with_backoff(ws.update, f"C{row_number}", [[status]])
    _invalidate_rows_cache(sheet_id)


def update_substack_article(sheet_id: str, row_number: int, article: str) -> None:
    """Write article body to col B of the substack tab."""
    ws = _named_worksheet(sheet_id, "substack")
    _with_backoff(ws.update, f"B{row_number}", [[article]])
    _invalidate_rows_cache(sheet_id)


def append_substack_row(sheet_id: str, url: str) -> None:
    """Append a new row to the substack tab with the given URL and status open."""
    ws = _named_worksheet(sheet_id, "substack")
    _with_backoff(ws.append_row, [url, "", "open", ""], value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def _ensure_substack_post_headers(ws) -> None:
    cache_key = ("substack_posts_headers", ws.id)
    if cache_key in _headers_checked:
        return
    values = _with_backoff(ws.get_all_values)
    headers = values[0] if values else []
    expected_suffix = ["slide_prompt", "slide_input", "post_type", "topics", "text4", "text5", "text6"]
    if len(headers) >= 15 and headers[8:15] == expected_suffix:
        _headers_checked.add(cache_key)
        return
    _with_backoff(ws.update, "I1:O1", [expected_suffix])
    _headers_checked.add(cache_key)


def append_substack_post_rows(sheet_id: str, rows: list[dict]) -> None:
    """Append rows to substack_posts tab.

    Each dict must have keys: url, angle, caption, text1, text2, text3, cta, status.
    Newer sheets may also include slide_prompt, slide_input, post_type, topics, and text4-text6.
    """
    if not rows:
        return
    ws = _named_worksheet(sheet_id, "substack_posts")
    _ensure_substack_post_headers(ws)
    values = [
        [
            r.get("url", ""),
            r.get("angle", ""),
            r.get("caption", ""),
            r.get("text1", ""),
            r.get("text2", ""),
            r.get("text3", ""),
            r.get("cta", ""),
            r.get("status", ""),
            r.get("slide_prompt", ""),
            r.get("slide_input", ""),
            r.get("post_type", ""),
            r.get("topics", ""),
            r.get("text4", ""),
            r.get("text5", ""),
            r.get("text6", ""),
        ]
        for r in rows
    ]
    _with_backoff(ws.append_rows, values, value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def get_substack_post_rows(sheet_id: str) -> list[dict]:
    """Return all rows from the substack_posts tab."""
    cached = _get_cached_rows(sheet_id, "substack_posts")
    if cached is not None:
        return cached
    ws = _named_worksheet(sheet_id, "substack_posts")
    _ensure_substack_post_headers(ws)
    values = _with_backoff(ws.get_all_values)
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for i, row in enumerate(values[1:], start=2):
        record = {headers[j]: (row[j].strip() if j < len(row) else "") for j in range(len(headers))}
        record["row_number"] = i
        rows.append(record)
    _set_cached_rows(sheet_id, "substack_posts", rows)
    return [row.copy() for row in rows]


def update_substack_post_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write status to col H of the substack_posts tab."""
    ws = _named_worksheet(sheet_id, "substack_posts")
    _with_backoff(ws.update, f"H{row_number}", [[status]])
    _invalidate_rows_cache(sheet_id)


def update_substack_post_slides_and_status(
    sheet_id: str,
    row_number: int,
    text1: str,
    text2: str,
    text3: str,
    text4: str,
    text5: str,
    text6: str,
    status: str,
) -> None:
    """Write slide text and status to a substack_posts row."""
    ws = _named_worksheet(sheet_id, "substack_posts")
    _with_backoff(
        ws.batch_update,
        [
            {"range": f"D{row_number}:F{row_number}", "values": [[text1, text2, text3]]},
            {"range": f"H{row_number}", "values": [[status]]},
            {"range": f"M{row_number}:O{row_number}", "values": [[text4, text5, text6]]},
        ],
    )
    _invalidate_rows_cache(sheet_id)
