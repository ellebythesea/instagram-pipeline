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
_SUBSTACK_SHEET_TITLE = "substack"
_SUBSTACK_HEADERS = [
    "url",
    "name",
    "article",
    "topic breakdown",
    "status",
    "instagram url",
    "monitoring status",
    "last comment retrieved",
    "summary",
]
_SUBSTACK_LEGACY_HEADERS_WITH_NAME = [
    "url",
    "name",
    "article",
    "status",
    "instagram url",
    "monitoring status",
    "last comment retrieved",
    "summary",
]
_SUBSTACK_LEGACY_HEADERS_NO_NAME = [
    "url",
    "article",
    "status",
    "instagram url",
    "monitoring status",
    "last comment retrieved",
    "summary",
]


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
        row[1] = source.get("required_hashtags", "").strip()
        row[2] = source.get("source_username", "").strip()
        row[3] = source.get("caption", "").strip()
        row[4] = source.get("media_type", "").strip()
        row[7] = source.get("thumbnail_link", "").strip()
        row[8] = source.get("original_caption", "").strip()
        row[9] = source.get("transcript", "").strip()
        row[10] = source.get("top_comment", "").strip()
        row[11] = source.get("speaker_name", "").strip()
        row[12] = source.get("footer", "").strip()
        row[13] = source.get("status", "").strip()
        row[14] = source.get("caption_context", "").strip()
        row[15] = source.get("scheduled_time", "").strip()
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


def append_manual_post_row(sheet_id: str, row_data: dict) -> None:
    """Append a manually created row to the posts tab (no URL required)."""
    row = [""] * len(_EXPECTED_HEADERS)
    row[0] = (row_data.get("url") or "").strip()
    row[2] = (row_data.get("source_username") or "").strip()
    row[3] = (row_data.get("caption") or "").strip()
    row[4] = (row_data.get("media_type") or "").strip()
    row[5] = str(row_data.get("photo_count") or "").strip()
    row[6] = (row_data.get("media_link") or "").strip()
    row[7] = (row_data.get("thumbnail_link") or "").strip()
    row[8] = (row_data.get("original_caption") or "").strip()
    row[9] = (row_data.get("transcript") or "").strip()
    row[10] = (row_data.get("top_comment") or "").strip()
    row[11] = (row_data.get("speaker_name") or "").strip()
    row[13] = (row_data.get("status") or "").strip()
    row[14] = (row_data.get("caption_context") or "").strip()
    row[16] = (row_data.get("name") or "").strip()
    row[17] = (row_data.get("text1") or "").strip()
    row[18] = (row_data.get("text2") or "").strip()
    row[19] = (row_data.get("text3") or "").strip()
    row[20] = (row_data.get("slide_cta") or "").strip()
    row[21] = (row_data.get("text4") or "").strip()
    row[22] = (row_data.get("text5") or "").strip()
    row[23] = (row_data.get("text6") or "").strip()
    ws = _worksheet(sheet_id)
    _with_backoff(ws.append_row, row, value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def update_generated_post_slides_and_status(
    sheet_id: str,
    row_number: int,
    name: str,
    text1: str,
    text2: str,
    text3: str,
    text4: str,
    text5: str,
    text6: str,
    status: str,
) -> None:
    """Write generated post slide fields and status to the main posts tab."""
    ws = _worksheet(sheet_id)
    _update_row_fields_by_headers(
        ws,
        row_number,
        {
            "name": name,
            "text1": text1,
            "text2": text2,
            "text3": text3,
            "text4": text4,
            "text5": text5,
            "text6": text6,
            "status": status,
        },
    )
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
    text4: str = "",
    text5: str = "",
    text6: str = "",
) -> None:
    """Write carousel fields to cols Q-X and set status to 'slides'."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        [
            {"range": f"N{row_number}", "values": [["slides"]]},
            {
                "range": f"Q{row_number}:X{row_number}",
                "values": [[name, text1, text2, text3, "", text4, text5, text6]],
            },
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
    _update_row_fields_by_headers(ws, row_number, {"last": last_checked, "summary": summary})
    _invalidate_rows_cache(sheet_id)


# ---------------------------------------------------------------------------
# substack tab helpers
# ---------------------------------------------------------------------------


def _ensure_substack_headers(ws) -> None:
    cache_key = ("substack_headers", ws.id)
    if cache_key in _headers_checked:
        return
    values = _with_backoff(ws.get_all_values)
    headers = [header.strip() for header in values[0]] if values else []
    normalized = [header.lower() for header in headers if header]
    expected = [header.lower() for header in _SUBSTACK_HEADERS]
    base_required = ["url", "article", "status"]
    if "name" in normalized:
        upgradeable_required = ["url", "name", "article", "status"]
    else:
        upgradeable_required = base_required

    if not headers:
        _with_backoff(ws.update, "A1:I1", [_SUBSTACK_HEADERS])
        _headers_checked.add(cache_key)
        return

    def _rewrite_substack_rows(rows: list[list[str]]) -> None:
        _with_backoff(ws.clear)
        _with_backoff(ws.update, f"A1:I{len(rows)}", rows)

    def _looks_like_shifted_expected_layout(data_rows: list[list[str]]) -> bool:
        checked = 0
        shifted_matches = 0
        status_values = {"open", "closed", "ingested", "posts created"}
        for row in data_rows:
            if not any((cell or "").strip() for cell in row):
                continue
            padded = row + [""] * (len(_SUBSTACK_HEADERS) - len(row))
            topic_breakdown_value = padded[3].strip().lower()
            status_value = padded[4].strip()
            instagram_url_value = padded[5].strip().lower()
            monitoring_status_value = padded[6].strip()
            if not any([topic_breakdown_value, status_value, instagram_url_value, monitoring_status_value]):
                continue
            checked += 1
            if (
                topic_breakdown_value in status_values
                and (not status_value or status_value.startswith("http"))
                and instagram_url_value in {"", "open", "closed"}
                and (not monitoring_status_value or monitoring_status_value.startswith("20"))
            ):
                shifted_matches += 1
        return checked > 0 and shifted_matches >= max(1, checked // 2)

    if normalized == [header.lower() for header in _SUBSTACK_LEGACY_HEADERS_WITH_NAME]:
        migrated_rows = [_SUBSTACK_HEADERS]
        for row in values[1:]:
            padded = row + [""] * (len(_SUBSTACK_LEGACY_HEADERS_WITH_NAME) - len(row))
            migrated_rows.append([
                padded[0].strip(),
                padded[1].strip(),
                padded[2].strip(),
                "",
                padded[3].strip(),
                padded[4].strip(),
                padded[5].strip(),
                padded[6].strip(),
                padded[7].strip(),
            ])
        _rewrite_substack_rows(migrated_rows)
        _headers_checked.add(cache_key)
        return

    if normalized == [header.lower() for header in _SUBSTACK_LEGACY_HEADERS_NO_NAME]:
        migrated_rows = [_SUBSTACK_HEADERS]
        for row in values[1:]:
            padded = row + [""] * (len(_SUBSTACK_LEGACY_HEADERS_NO_NAME) - len(row))
            migrated_rows.append([
                padded[0].strip(),
                "",
                padded[1].strip(),
                "",
                padded[2].strip(),
                padded[3].strip(),
                padded[4].strip(),
                padded[5].strip(),
                padded[6].strip(),
            ])
        _rewrite_substack_rows(migrated_rows)
        _headers_checked.add(cache_key)
        return

    if normalized == expected and _looks_like_shifted_expected_layout(values[1:]):
        migrated_rows = [_SUBSTACK_HEADERS]
        for row in values[1:]:
            padded = row + [""] * (len(_SUBSTACK_HEADERS) - len(row))
            migrated_rows.append([
                padded[0].strip(),
                padded[1].strip(),
                padded[2].strip(),
                "",
                padded[3].strip(),
                padded[4].strip(),
                padded[5].strip(),
                padded[6].strip(),
                padded[7].strip(),
            ])
        _rewrite_substack_rows(migrated_rows)
        _headers_checked.add(cache_key)
        return

    if all(header in normalized for header in upgradeable_required) and not all(
        header in normalized for header in expected
    ):
        header_index = {header.lower(): idx for idx, header in enumerate(headers)}
        migrated_rows = [_SUBSTACK_HEADERS]
        for row in values[1:]:
            migrated_record = {header: "" for header in _SUBSTACK_HEADERS}
            for source_header, idx in header_index.items():
                if source_header in migrated_record and idx < len(row):
                    migrated_record[source_header] = row[idx].strip()
            migrated_rows.append([migrated_record.get(header, "") for header in _SUBSTACK_HEADERS])
        _rewrite_substack_rows(migrated_rows)
        _headers_checked.add(cache_key)
        return

    missing_required = [header for header in base_required if header not in normalized]
    if missing_required:
        raise RuntimeError(
            "substack tab is missing required header(s): " + ", ".join(missing_required)
        )
    _headers_checked.add(cache_key)


def _substack_header_map(ws) -> dict[str, int]:
    _ensure_substack_headers(ws)
    headers = [header.strip() for header in _with_backoff(ws.row_values, 1)]
    return {
        header: index + 1
        for index, header in enumerate(headers)
        if header
    }

def get_substack_rows(sheet_id: str) -> list[dict]:
    """Return all rows from the substack tab."""
    cached = _get_cached_rows(sheet_id, "substack")
    if cached is not None:
        return cached
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    _ensure_substack_headers(ws)
    values = _with_backoff(ws.get_all_values)
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for i, row in enumerate(values[1:], start=2):
        record = {headers[j]: (row[j].strip() if j < len(row) else "") for j in range(len(headers))}
        article_value = (record.get("article") or "").strip().lower()
        status_value = (record.get("status") or "").strip().lower()
        if article_value in {"open", "closed", "ingested", "posts created"} and status_value in {"", "open", "closed", "ingested", "posts created"}:
            record["article"] = ""
        record["row_number"] = i
        rows.append(record)
    _set_cached_rows(sheet_id, "substack", rows)
    return [row.copy() for row in rows]


def get_open_substack_rows(sheet_id: str) -> list[dict]:
    """Return rows from the substack tab where status is 'open'."""
    return [r for r in get_substack_rows(sheet_id) if r.get("status", "").strip().lower() == "open"]


def update_substack_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write article workflow status to the substack tab."""
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    _update_row_fields_by_headers(ws, row_number, {"status": status})
    _invalidate_rows_cache(sheet_id)


def update_substack_article(sheet_id: str, row_number: int, article: str) -> None:
    """Write article body to the substack tab."""
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    _update_row_fields_by_headers(ws, row_number, {"article": article})
    _invalidate_rows_cache(sheet_id)


def update_substack_topic_breakdown(sheet_id: str, row_number: int, topic_breakdown: str) -> None:
    """Write persisted topic breakdown JSON/text to the substack tab."""
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    _update_row_fields_by_headers(ws, row_number, {"topic breakdown": topic_breakdown})
    _invalidate_rows_cache(sheet_id)


def append_substack_row(sheet_id: str, url: str) -> None:
    """Append a new row to the substack tab with default article and monitoring states."""
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    header_map = _substack_header_map(ws)
    ordered_headers = [header for header, _ in sorted(header_map.items(), key=lambda item: item[1])]
    row = {
        "url": (url or "").strip(),
        "name": "",
        "article": "",
        "topic breakdown": "",
        "status": "open",
        "instagram url": "",
        "monitoring status": "closed",
        "last comment retrieved": "",
        "summary": "",
    }
    _with_backoff(
        ws.append_row,
        [row.get(header, "") for header in ordered_headers],
        value_input_option="USER_ENTERED",
    )
    _invalidate_rows_cache(sheet_id)


def get_open_comment_monitor_rows(sheet_id: str) -> list[dict]:
    """Return open comment-monitor rows from the merged substack sheet plus legacy monitors rows."""
    merged_rows: list[dict] = []
    seen_urls: set[str] = set()

    for row in get_substack_rows(sheet_id):
        instagram_url = row.get("instagram url", "").strip()
        monitoring_status = row.get("monitoring status", "").strip().lower()
        if monitoring_status != "open" or not instagram_url:
            continue
        seen_urls.add(instagram_url)
        substack_url = row.get("url", "").strip()
        merged_rows.append(
            {
                "source": "substack",
                "row_number": row["row_number"],
                "label": substack_url or instagram_url,
                "url": instagram_url,
                "substack_url": substack_url,
                "summary": row.get("summary", "").strip(),
                "last_checked": row.get("last comment retrieved", "").strip(),
            }
        )

    legacy_ws = _optional_worksheet(sheet_id, "monitors")
    if legacy_ws is None:
        return merged_rows

    for row in get_open_monitor_rows(sheet_id):
        url = row.get("url", "").strip()
        if not url or url in seen_urls:
            continue
        merged_rows.append(
            {
                "source": "monitors",
                "row_number": row["row_number"],
                "label": row.get("label", "").strip() or row.get("substack url", "").strip() or url,
                "url": url,
                "substack_url": row.get("substack url", "").strip(),
                "summary": row.get("summary", "").strip(),
                "last_checked": row.get("last", "").strip(),
            }
        )

    return merged_rows


def update_comment_monitor_summary(
    sheet_id: str,
    source: str,
    row_number: int,
    summary: str,
    last_checked: str,
) -> None:
    """Write monitoring summary and last-checked timestamp to the correct sheet."""
    normalized_source = (source or "").strip().lower()
    if normalized_source == "substack":
        ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
        _update_row_fields_by_headers(
            ws,
            row_number,
            {"last comment retrieved": last_checked, "summary": summary},
        )
        _invalidate_rows_cache(sheet_id)
        return
    update_monitor_summary(sheet_id, row_number, summary, last_checked)


def update_comment_monitor_last_checked(
    sheet_id: str,
    source: str,
    row_number: int,
    last_checked: str,
) -> None:
    """Write only the last-checked timestamp to the correct monitoring row."""
    normalized_source = (source or "").strip().lower()
    if normalized_source == "substack":
        ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
        _update_row_fields_by_headers(ws, row_number, {"last comment retrieved": last_checked})
        _invalidate_rows_cache(sheet_id)
        return
    ws = _named_worksheet(sheet_id, "monitors")
    _update_row_fields_by_headers(ws, row_number, {"last": last_checked})
    _invalidate_rows_cache(sheet_id)


def _ensure_substack_post_headers(ws) -> None:
    cache_key = ("substack_posts_headers", ws.id)
    if cache_key in _headers_checked:
        return
    values = _with_backoff(ws.get_all_values)
    headers = values[0] if values else []
    expected_headers = [
        "url",
        "angle",
        "caption",
        "text1",
        "text2",
        "text3",
        "text4",
        "text5",
        "text6",
        "cta",
        "status",
        "slide_prompt",
        "slide_input",
        "post_type",
        "topics",
    ]
    if not headers:
        _with_backoff(ws.update, "A1:O1", [expected_headers])
        _headers_checked.add(cache_key)
        return
    missing_headers = [header for header in expected_headers if header not in headers]
    if missing_headers:
        raise RuntimeError(
            "substack_posts is missing required header(s): " + ", ".join(missing_headers)
        )
    _headers_checked.add(cache_key)


def _column_letter(index: int) -> str:
    result = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _update_row_fields_by_headers(
    ws: gspread.Worksheet,
    row_number: int,
    field_values: dict[str, str],
) -> None:
    headers = [header.strip() for header in _with_backoff(ws.row_values, 1)]
    normalized = {
        header.lower(): index + 1
        for index, header in enumerate(headers)
        if header
    }
    missing_headers = [
        field for field in field_values
        if field.strip().lower() not in normalized
    ]
    if missing_headers:
        raise RuntimeError(
            f"{ws.title} tab is missing required header(s): {', '.join(missing_headers)}."
        )
    requests = [
        {
            "range": f"{_column_letter(normalized[field.strip().lower()])}{row_number}",
            "values": [[value]],
        }
        for field, value in field_values.items()
    ]
    _with_backoff(ws.batch_update, requests)


def _substack_post_header_map(ws) -> dict[str, int]:
    _ensure_substack_post_headers(ws)
    headers = [header.strip() for header in _with_backoff(ws.row_values, 1)]
    return {
        header: index + 1
        for index, header in enumerate(headers)
        if header
    }


def append_substack_post_rows(sheet_id: str, rows: list[dict]) -> None:
    """Append rows to substack_posts tab.

    Each dict must have keys: url, angle, caption, text1, text2, text3, cta, status.
    Newer sheets may also include text4-text6, slide_prompt, slide_input, post_type, and topics.
    """
    if not rows:
        return
    ws = _named_worksheet(sheet_id, "substack_posts")
    header_map = _substack_post_header_map(ws)
    ordered_headers = [header for header, _ in sorted(header_map.items(), key=lambda item: item[1])]
    values = [
        [r.get(header, "") for header in ordered_headers]
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
    """Write status to the status column of the substack_posts tab."""
    ws = _named_worksheet(sheet_id, "substack_posts")
    header_map = _substack_post_header_map(ws)
    status_column = _column_letter(header_map["status"])
    _with_backoff(ws.update, f"{status_column}{row_number}", [[status]])
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
    header_map = _substack_post_header_map(ws)
    updates = []
    for key, value in {
        "text1": text1,
        "text2": text2,
        "text3": text3,
        "text4": text4,
        "text5": text5,
        "text6": text6,
        "status": status,
    }.items():
        column = _column_letter(header_map[key])
        updates.append({"range": f"{column}{row_number}", "values": [[value]]})
    _with_backoff(
        ws.batch_update,
        updates,
    )
    _invalidate_rows_cache(sheet_id)
