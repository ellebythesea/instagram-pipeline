"""Google Sheets helper — read rows and write pipeline results.

Rows are read and written by header name so users can reorder columns in the
worksheet without breaking the app.
"""

import json
import os
import random
import time
from json import JSONDecodeError

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

from config import GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_WORKSHEET_NAME

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

_EXPECTED_HEADERS = [
    "Instagram URL",
    "Source Username",
    "Generated Caption",
    "Media Type",
    "Photo Count",
    "Media Drive Link",
    "Thumbnail Drive Link",
    "Original Caption",
    "Transcript",
    "Top Comment",
    "Required Hashtags",
    "Speaker Name",
    "Footer",
    "Status",
    "Caption Context",
    "Scheduled Time",
]

_ROWS_CACHE_TTL_SECONDS = 10
_rows_cache: dict[str, tuple[float, list[dict]]] = {}
_headers_checked: set[tuple[str, str]] = set()
_METADATA_SHEET_TITLE = "__workspace_meta__"
_LAST_SCHEDULED_TIMES_KEY = "last_scheduled_times"
_FUNDRAISING_SHEET_TITLE = "fundraising"


def _get_client() -> gspread.Client:
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
    return gspread.authorize(creds)


def _workbook(sheet_id: str):
    return _get_client().open_by_key(sheet_id)


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
    workbook = _workbook(sheet_id)

    if GOOGLE_WORKSHEET_NAME:
        ws = workbook.worksheet(GOOGLE_WORKSHEET_NAME)
        _ensure_headers(sheet_id, ws)
        return ws

    expected_headers = {"Instagram URL", "Status"}
    for ws in workbook.worksheets():
        headers = {h.strip() for h in ws.row_values(1) if h.strip()}
        if expected_headers.issubset(headers):
            _ensure_headers(sheet_id, ws)
            return ws

    ws = workbook.sheet1
    _ensure_headers(sheet_id, ws)
    return ws


def _metadata_worksheet(sheet_id: str) -> gspread.Worksheet:
    workbook = _workbook(sheet_id)
    try:
        ws = workbook.worksheet(_METADATA_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = workbook.add_worksheet(title=_METADATA_SHEET_TITLE, rows=10, cols=2)
        _with_backoff(ws.update, "A1:B1", [["key", "value"]])
    return ws


def _optional_worksheet(sheet_id: str, title: str) -> gspread.Worksheet | None:
    workbook = _workbook(sheet_id)
    try:
        return workbook.worksheet(title)
    except gspread.WorksheetNotFound:
        return None


def _ensure_headers(sheet_id: str, ws: gspread.Worksheet) -> None:
    """Ensure the worksheet has the required headers without forcing their order."""
    cache_key = (sheet_id, ws.title)
    if cache_key in _headers_checked:
        return
    current = _with_backoff(ws.row_values, 1)
    normalized = [value.strip() for value in current if value.strip()]
    if not normalized:
        _with_backoff(ws.update, "A1:P1", [_EXPECTED_HEADERS])
        _headers_checked.add(cache_key)
        return

    missing_headers = [header for header in _EXPECTED_HEADERS if header not in normalized]
    if missing_headers:
        raise RuntimeError(
            f"Worksheet '{ws.title}' is missing required header(s): {', '.join(missing_headers)}"
        )
    _headers_checked.add(cache_key)


def _header_map(ws: gspread.Worksheet) -> dict[str, int]:
    headers = _with_backoff(ws.row_values, 1)
    mapping: dict[str, int] = {}
    for index, header in enumerate(headers, start=1):
        cleaned = header.strip()
        if cleaned:
            mapping[cleaned] = index
    missing = [header for header in _EXPECTED_HEADERS if header not in mapping]
    if missing:
        raise RuntimeError(
            f"Worksheet '{ws.title}' is missing required header(s): {', '.join(missing)}"
        )
    return mapping


def _header_cell(ws: gspread.Worksheet, row_number: int, header: str) -> str:
    header_index = _header_map(ws)[header]
    return rowcol_to_a1(row_number, header_index)


def _invalidate_rows_cache(sheet_id: str) -> None:
    _rows_cache.pop(sheet_id, None)


def get_all_rows(sheet_id: str) -> list[dict]:
    """Return all data rows as dicts keyed by header name, plus row_number."""
    cached = _rows_cache.get(sheet_id)
    now = time.time()
    if cached and now - cached[0] < _ROWS_CACHE_TTL_SECONDS:
        return [dict(r) for r in cached[1]]

    ws = _worksheet(sheet_id)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for i, r in enumerate(records):
        r["row_number"] = i + 2  # header is row 1
    _rows_cache[sheet_id] = (now, [dict(r) for r in records])
    return records


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
    header_map = _header_map(ws)
    width = max(header_map.values())
    rows = []
    for url in cleaned_urls:
        row = [""] * width
        row[header_map["Instagram URL"] - 1] = url
        row[header_map["Required Hashtags"] - 1] = required_hashtags.strip()
        rows.append(row)
    _with_backoff(ws.append_rows, rows, value_input_option="USER_ENTERED")
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
    """Write ingest results by header name."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        [
            {
                "range": _header_cell(ws, row_number, "Source Username"),
                "values": [[username]],
            },
            {
                "range": _header_cell(ws, row_number, "Media Type"),
                "values": [[media_type]],
            },
            {
                "range": _header_cell(ws, row_number, "Photo Count"),
                "values": [[str(photo_count) if photo_count else ""]],
            },
            {
                "range": _header_cell(ws, row_number, "Media Drive Link"),
                "values": [[media_link]],
            },
            {
                "range": _header_cell(ws, row_number, "Thumbnail Drive Link"),
                "values": [[thumbnail_link]],
            },
            {
                "range": _header_cell(ws, row_number, "Original Caption"),
                "values": [[original_caption]],
            },
            {
                "range": _header_cell(ws, row_number, "Transcript"),
                "values": [[transcript]],
            },
            {
                "range": _header_cell(ws, row_number, "Status"),
                "values": [[status]],
            },
        ],
    )
    _invalidate_rows_cache(sheet_id)


def update_caption(sheet_id: str, row_number: int, caption: str, status: str) -> None:
    """Write generated caption and status by header name."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        [
            {"range": _header_cell(ws, row_number, "Generated Caption"), "values": [[caption]]},
            {"range": _header_cell(ws, row_number, "Status"), "values": [[status]]},
        ],
    )
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
            {"range": _header_cell(ws, row_number, "Generated Caption"), "values": [[caption]]},
            {"range": _header_cell(ws, row_number, "Top Comment"), "values": [[top_comment]]},
            {"range": _header_cell(ws, row_number, "Required Hashtags"), "values": [[hashtags]]},
            {"range": _header_cell(ws, row_number, "Speaker Name"), "values": [[speaker_name]]},
            {"range": _header_cell(ws, row_number, "Footer"), "values": [[footer]]},
            {"range": _header_cell(ws, row_number, "Status"), "values": [[status]]},
            {"range": _header_cell(ws, row_number, "Caption Context"), "values": [[caption_context]]},
        ],
    )
    _invalidate_rows_cache(sheet_id)


def update_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write status for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, _header_cell(ws, row_number, "Status"), [[status]])
    _invalidate_rows_cache(sheet_id)


def update_transcript(sheet_id: str, row_number: int, transcript: str) -> None:
    """Write transcript for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, _header_cell(ws, row_number, "Transcript"), [[transcript]])
    _invalidate_rows_cache(sheet_id)


def update_caption_context(sheet_id: str, row_number: int, caption_context: str) -> None:
    """Write caption context for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, _header_cell(ws, row_number, "Caption Context"), [[caption_context]])
    _invalidate_rows_cache(sheet_id)


def update_scheduled_times(sheet_id: str, assignments: dict[int, str]) -> None:
    """Write scheduled time values for multiple rows."""
    if not assignments:
        return
    ws = _worksheet(sheet_id)
    for row_number, scheduled_time in assignments.items():
        _with_backoff(ws.update, _header_cell(ws, row_number, "Scheduled Time"), [[scheduled_time]])
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
    """Write user metadata and caption context by header name."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        [
            {"range": _header_cell(ws, row_number, "Top Comment"), "values": [[top_comment]]},
            {"range": _header_cell(ws, row_number, "Required Hashtags"), "values": [[hashtags]]},
            {"range": _header_cell(ws, row_number, "Speaker Name"), "values": [[speaker_name]]},
            {"range": _header_cell(ws, row_number, "Footer"), "values": [[footer]]},
            {"range": _header_cell(ws, row_number, "Caption Context"), "values": [[caption_context]]},
        ],
    )
    _invalidate_rows_cache(sheet_id)


def delete_row(sheet_id: str, row_number: int) -> None:
    """Delete a single sheet row by absolute row number."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.delete_rows, row_number)
    _invalidate_rows_cache(sheet_id)
