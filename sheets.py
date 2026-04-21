"""Google Sheets helper — read rows and write pipeline results.

Column order is fixed per spec. Headers are used for reads (get_all_records),
column letter ranges are used for writes since ranges are inherently positional.

Sheet layout:
  A  Instagram URL      B  Source Username    C  Generated Caption
  D  Media Type         E  Photo Count        F  Media Drive Link
  G  Thumbnail Drive Link
  H  Original Caption   I  Transcript         J  Top Comment
  K  Speaker Name       L  Required Hashtags  M  Footer
  N  Status             O  Caption Context
"""

import json
import os
import random
import time

import gspread
from google.oauth2.service_account import Credentials

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
    "Speaker Name",
    "Required Hashtags",
    "Footer",
    "Status",
    "Caption Context",
]

_ROWS_CACHE_TTL_SECONDS = 10
_rows_cache: dict[str, tuple[float, list[dict]]] = {}
_headers_checked: set[tuple[str, str]] = set()


def _get_client() -> gspread.Client:
    creds_src = GOOGLE_SERVICE_ACCOUNT_JSON
    if os.path.isfile(creds_src):
        creds = Credentials.from_service_account_file(creds_src, scopes=_SCOPES)
    else:
        creds = Credentials.from_service_account_info(json.loads(creds_src), scopes=_SCOPES)
    return gspread.authorize(creds)


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
    workbook = _get_client().open_by_key(sheet_id)

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


def _ensure_headers(sheet_id: str, ws: gspread.Worksheet) -> None:
    """Restore the expected header row if it is missing or incorrect."""
    cache_key = (sheet_id, ws.title)
    if cache_key in _headers_checked:
        return
    current = _with_backoff(ws.row_values, 1)
    normalized = current[:len(_EXPECTED_HEADERS)]
    if normalized == _EXPECTED_HEADERS:
        _headers_checked.add(cache_key)
        return
    _with_backoff(ws.update, "A1:O1", [_EXPECTED_HEADERS])
    _headers_checked.add(cache_key)


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
    """Write ingest results to cols B and D-I, and status to N."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"B{row_number}", [[username]])
    _with_backoff(
        ws.update,
        f"D{row_number}:I{row_number}",
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
    """Write generated caption to col C and status to col N."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"C{row_number}", [[caption]])
    _with_backoff(ws.update, f"N{row_number}", [[status]])
    _invalidate_rows_cache(sheet_id)


def update_transcript(sheet_id: str, row_number: int, transcript: str) -> None:
    """Write transcript to col I for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"I{row_number}", [[transcript]])
    _invalidate_rows_cache(sheet_id)


def update_caption_context(sheet_id: str, row_number: int, caption_context: str) -> None:
    """Write caption context to col O for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.update, f"O{row_number}", [[caption_context]])
    _invalidate_rows_cache(sheet_id)


def update_metadata(
    sheet_id: str,
    row_number: int,
    caption_context: str,
    speaker_name: str,
    hashtags: str,
    top_comment: str,
    footer: str,
) -> None:
    """Write user metadata to cols J-M and caption context to O."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.update,
        f"J{row_number}:M{row_number}",
        [[top_comment, speaker_name, hashtags, footer]],
    )
    _with_backoff(ws.update, f"O{row_number}", [[caption_context]])
    _invalidate_rows_cache(sheet_id)
