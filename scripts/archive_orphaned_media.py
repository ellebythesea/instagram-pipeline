#!/usr/bin/env python3
"""Archive orphaned local media without running transcription.

Usage:
    .venv/bin/python scripts/archive_orphaned_media.py

Optional:
    .venv/bin/python scripts/archive_orphaned_media.py --dry-run
    .venv/bin/python scripts/archive_orphaned_media.py --media-dir "/path/to/instagram pipeline media"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import GOOGLE_SHEET_ID  # noqa: E402
from drive import _get_service  # noqa: E402
from sheets import get_all_rows  # noqa: E402
from scripts.local_transcribe_reels import _archive_orphaned_media, _default_media_dir  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive orphaned local originals, segment folders, and screenshots.")
    parser.add_argument(
        "--media-dir",
        default=None,
        help="Path to your locally synced Drive media folder. Defaults to auto-detecting common Google Drive locations.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which orphaned local items would be archived without moving them.",
    )
    args = parser.parse_args()

    media_root = Path(args.media_dir).expanduser() if args.media_dir else _default_media_dir()
    if not media_root.exists():
        raise FileNotFoundError(f"Media directory does not exist: {media_root}")

    print(f"Using media directory: {media_root}")
    rows = get_all_rows(GOOGLE_SHEET_ID)
    service = _get_service()
    _archive_orphaned_media(media_root, rows, service, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
