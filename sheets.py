"""Google Sheets helper — read pending rows and write results back."""

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


def get_pending_rows(sheet_id: str) -> list[dict]:
    """Return rows where Status (col I) is blank, including their 1-based row numbers."""
    ws = _get_client().open_by_key(sheet_id).sheet1
    all_values = ws.get_all_values()

    pending = []
    for idx, row in enumerate(all_values[1:], start=2):  # row 1 is header
        padded = (row + [""] * 9)[:9]
        if not padded[8].strip() and padded[0].strip():  # col I empty, col A has URL
            pending.append({
                "row_number": idx,
                "url": padded[0].strip(),
                "speaker_name": padded[1].strip(),
                "required_hashtags": padded[2].strip(),
                "top_comment": padded[3].strip(),
            })
    return pending


def update_row(
    sheet_id: str,
    row_number: int,
    username: str,
    drive_link: str,
    transcript: str,
    caption: str,
    status: str,
) -> None:
    """Write results to columns E–I of the specified row."""
    ws = _get_client().open_by_key(sheet_id).sheet1
    ws.update(
        f"E{row_number}:I{row_number}",
        [[username, drive_link, transcript, caption, status]],
    )
