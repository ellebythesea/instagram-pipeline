"""Google Sheets helper — read rows and write pipeline results.

Column order is fixed per spec. Headers are used for reads (get_all_records),
column letter ranges are used for writes since ranges are inherently positional.

Sheet layout:
  A  Instagram URL      B  Source Username    C  Media Type
  D  Photo Count        E  Media Drive Link   F  Thumbnail Drive Link
  G  Original Caption   H  Transcript         I  Speaker Name
  J  Required Hashtags  K  Top Comment        L  Footer
  M  Generated Caption  N  Status
"""

import json
import os

import gspread
from google.oauth2.service_account import Credentials

from config import GOOGLE_SERVICE_ACCOUNT_JSON

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


def _get_client() -> gspread.Client:
    creds_src = GOOGLE_SERVICE_ACCOUNT_JSON
    if os.path.isfile(creds_src):
        creds = Credentials.from_service_account_file(creds_src, scopes=_SCOPES)
    else:
        creds = Credentials.from_service_account_info(json.loads(creds_src), scopes=_SCOPES)
    return gspread.authorize(creds)


def _worksheet(sheet_id: str) -> gspread.Worksheet:
    return _get_client().open_by_key(sheet_id).sheet1


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
    """Write ingest results to cols B–H and status to col N."""
    ws = _worksheet(sheet_id)
    ws.update(
        f"B{row_number}:H{row_number}",
        [[username, media_type, str(photo_count) if photo_count else "",
          media_link, thumbnail_link, original_caption, transcript]],
    )
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
    """Write user metadata to cols I–L."""
    ws = _worksheet(sheet_id)
    ws.update(
        f"I{row_number}:L{row_number}",
        [[speaker_name, hashtags, top_comment, footer]],
    )
