#!/usr/bin/env python3
"""Check whether the current Drive OAuth token can still access the target folder.

Usage:
    .venv/bin/python scripts/check_drive_oauth.py

Exit codes:
    0 = Drive auth is working
    1 = Drive auth/config failed
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import GOOGLE_DRIVE_FOLDER_ID  # noqa: E402
from drive import _get_service  # noqa: E402


def main() -> int:
    print("Checking Google Drive OAuth access...")
    print(f"GOOGLE_DRIVE_FOLDER_ID={GOOGLE_DRIVE_FOLDER_ID or '(missing)'}")

    if not GOOGLE_DRIVE_FOLDER_ID:
        print("FAILED: GOOGLE_DRIVE_FOLDER_ID is missing.")
        return 1

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
    except Exception as exc:
        print(f"FAILED: {exc}")
        return 1

    print("OK")
    print(f"folder_name={metadata.get('name', '')}")
    print(f"mime_type={metadata.get('mimeType', '')}")
    if metadata.get("driveId"):
        print(f"shared_drive_id={metadata.get('driveId')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
