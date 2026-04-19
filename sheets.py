"""Google Sheets helper — read rows and write pipeline results.

Column order is fixed per spec. Headers are used for reads (get_all_records),
column letter ranges are used for writes since ranges are inherently positional.

Sheet layout:
  A  Instagram URL      B  Media Type         C  Photo Count
  D  Media Drive Link   E  Thumbnail Drive Link
  F  Original Caption   G  Transcript         H  Speaker Name
  I  Required Hashtags  J  Top Comment        K  Footer
  L  Source Username
  M  Generated Caption  N  Status
"""

import json
import os

import gspread
from google.oauth2.service_account import Credentials

from config import GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_WORKSHEET_NAME

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

_EXPECTED_HEADERS = [
    "Instagram URL",
    "Media Type",
    "Photo Count",
    "Media Drive Link",
    "Thumbnail Drive Link",
    "Original Caption",
    "Transcript",
    "Speaker Name",
    "Required Hashtags",
    "Top Comment",
    "Footer",
    "Source Username",
    "Generated Caption",
    "Status",
]


def _get_client() -> gspread.Client:
    creds_src = GOOGLE_SERVICE_ACCOUNT_JSON
    if os.path.isfile(creds_src):
        creds = Credentials.from_service_account_file(creds_src, scopes=_SCOPES)
    else:
        creds = Credentials.from_service_account_info(json.loads(creds_src), scopes=_SCOPES)
    return gspread.authorize(creds)


def _worksheet(sheet_id: str) -> gspread.Worksheet:
    workbook = _get_client().open_by_key(sheet_id)

    if GOOGLE_WORKSHEET_NAME:
        ws = workbook.worksheet(GOOGLE_WORKSHEET_NAME)
        _ensure_headers(ws)
        return ws

    expected_headers = {"Instagram URL", "Status"}
    for ws in workbook.worksheets():
        headers = {h.strip() for h in ws.row_values(1) if h.strip()}
        if expected_headers.issubset(headers):
            _ensure_headers(ws)
            return ws

    ws = workbook.sheet1
    _ensure_headers(ws)
    return ws


def _ensure_headers(ws: gspread.Worksheet) -> None:
    """Restore the expected header row if it is missing or incorrect."""
    current = ws.row_values(1)
    normalized = current[:len(_EXPECTED_HEADERS)]
    if normalized == _EXPECTED_HEADERS:
        return
    ws.update("A1:N1", [_EXPECTED_HEADERS])


def get_all_rows(sheet_id: str) -> list[dict]:
    """Return all data rows as dicts keyed by header name, plus row_number."""
    ws = _worksheet(sheet_id)
    records = ws.get_all_records(default_blank="")
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
    """Write ingest results to cols B–G, username to L, and status to N."""
    ws = _worksheet(sheet_id)
    ws.update(
        f"B{row_number}:G{row_number}",
        [[media_type, str(photo_count) if photo_count else "",
          media_link, thumbnail_link, original_caption, transcript]],
    )
    ws.update(f"L{row_number}", [[username]])
    ws.update(f"N{row_number}", [[status]])


def update_caption(sheet_id: str, row_number: int, caption: str, status: str) -> None:
    """Write generated caption to col M and status to col N."""
    ws = _worksheet(sheet_id)
    ws.update(f"M{row_number}:N{row_number}", [[caption, status]])


def update_metadata(
    sheet_id: str,
    row_number: int,
    speaker_name: str,
    hashtags: str,
    top_comment: str,
    footer: str,
) -> None:
    """Write user metadata to cols H–K."""
    ws = _worksheet(sheet_id)
    ws.update(
        f"H{row_number}:K{row_number}",
        [[speaker_name, hashtags, top_comment, footer]],
    )
