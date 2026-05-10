#!/usr/bin/env python3
"""Debug Google Sheets and Drive access for the configured service account.

Usage:
    .venv/bin/python scripts/debug_google_access.py
"""

from __future__ import annotations

import json
import os
import sys
from json import JSONDecodeError
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import GOOGLE_DRIVE_FOLDER_ID, GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEET_ID, GOOGLE_WORKSHEET_NAME  # noqa: E402
from drive import _get_service  # noqa: E402


_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


def _load_service_account_credentials() -> tuple[Credentials, dict]:
    creds_src = GOOGLE_SERVICE_ACCOUNT_JSON
    if not creds_src:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not configured.")

    if os.path.isfile(creds_src):
        with open(creds_src, "r", encoding="utf-8") as handle:
            info = json.load(handle)
        creds = Credentials.from_service_account_file(creds_src, scopes=_SCOPES)
        return creds, info

    try:
        info = json.loads(creds_src)
    except JSONDecodeError as exc:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.") from exc
    creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
    return creds, info


def _print_header(title: str) -> None:
    print(f"\n== {title} ==")


def main() -> int:
    try:
        creds, info = _load_service_account_credentials()
    except Exception as exc:
        print(f"Service account load failed: {exc}")
        return 1

    client_email = (info.get("client_email") or "").strip()
    project_id = (info.get("project_id") or "").strip()

    _print_header("Service Account")
    print(f"client_email: {client_email or '(missing)'}")
    print(f"project_id: {project_id or '(missing)'}")

    _print_header("Configured Targets")
    print(f"GOOGLE_SHEET_ID: {GOOGLE_SHEET_ID or '(missing)'}")
    print(f"GOOGLE_WORKSHEET_NAME: {GOOGLE_WORKSHEET_NAME or '(auto)'}")
    print(f"GOOGLE_DRIVE_FOLDER_ID: {GOOGLE_DRIVE_FOLDER_ID or '(missing)'}")

    _print_header("Sheets Check")
    try:
        gc = gspread.authorize(creds)
        workbook = gc.open_by_key(GOOGLE_SHEET_ID)
        print(f"Workbook: OK ({workbook.title})")
        if GOOGLE_WORKSHEET_NAME:
            worksheet = workbook.worksheet(GOOGLE_WORKSHEET_NAME)
            print(f"Worksheet: OK ({worksheet.title})")
        else:
            worksheet = workbook.sheet1
            print(f"Worksheet: OK ({worksheet.title})")
    except Exception as exc:
        print(f"Sheets access failed: {exc}")

    _print_header("Drive Check")
    try:
        service = _get_service()
        metadata = (
            service.files()
            .get(
                fileId=GOOGLE_DRIVE_FOLDER_ID,
                fields="id,name,mimeType,parents,driveId",
                supportsAllDrives=True,
            )
            .execute()
        )
        print(f"Drive folder: OK ({metadata.get('name', '')})")
        print(f"mimeType: {metadata.get('mimeType', '')}")
        if metadata.get("driveId"):
            print(f"shared_drive_id: {metadata.get('driveId')}")
    except Exception as exc:
        print(f"Drive access failed: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
