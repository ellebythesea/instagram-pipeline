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
]

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
    """Restore the expected header row if it is missing or incorrect."""
    cache_key = (sheet_id, ws.title)
    if cache_key in _headers_checked:
        return
    current = _with_backoff(ws.row_values, 1)
    normalized = current[:len(_EXPECTED_HEADERS)]
    if normalized != _EXPECTED_HEADERS:
        _with_backoff(ws.update, "A1:T1", [_EXPECTED_HEADERS])
    _headers_checked.add(cache_key)


def _invalidate_rows_cache(sheet_id: str) -> None:
    return None


def get_all_rows(sheet_id: str) -> list[dict]:
    """Return all data rows as dicts keyed by header name, plus row_number."""
    ws = _worksheet(sheet_id)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for i, r in enumerate(records):
        r["row_number"] = i + 2  # header is row 1
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
    rows = []
    for url in cleaned_urls:
        row = [""] * len(_EXPECTED_HEADERS)
        row[0] = url
        row[1] = required_hashtags.strip()
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
    """Write ingest results to cols C and E-J, and status to N."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"C{row_number}", [[username]])
    _with_backoff(
        ws.update,
        f"E{row_number}:J{row_number}",
        [[
            media_type,
            str(photo_count) if photo_count else "",
            media_link,
            thumbnail_link,
            original_caption,
            transcript,
        ]],
    )
    _with_backoff(ws.update, f"N{row_number}", [[status]])
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
    """Write Figma/Google Sync carousel fields to cols Q-T."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.update,
        f"Q{row_number}:T{row_number}",
        [[name, text1, text2, text3]],
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


def delete_row(sheet_id: str, row_number: int) -> None:
    """Delete a single sheet row by absolute row number."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.delete_rows, row_number)
    _invalidate_rows_cache(sheet_id)
