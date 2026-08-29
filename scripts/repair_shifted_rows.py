#!/usr/bin/env python3
"""Repair rows written by an out-of-date checkout after the columns were moved.

Before the app read column positions from the sheet's header row, every write
went to a fixed letter. Running an older checkout against a sheet whose columns
have since been reordered puts each value in whatever column now occupies that
letter — the caption into Source Username, the thumbnail link into Media Drive
Link, and so on.

That is a permutation, not a loss: every value is still on the row, just under
the wrong header. Reading the row as though the sheet were still in the
original order recovers what each field was meant to be, and writing it back
through the current header map puts it where it belongs.

Only run this on rows actually written by an old checkout. A row written by a
current one is already correct, and this would shuffle it.

    python scripts/repair_shifted_rows.py 12 13 14        # show what it would do
    python scripts/repair_shifted_rows.py 12 13 14 --apply
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sheets
from config import GOOGLE_SHEET_ID


def _canonical_reading(raw: list[str]) -> dict[str, str]:
    """What each field was meant to be, read in the sheet's original order."""
    return {
        header: (raw[position] if position < len(raw) else "")
        for position, header in enumerate(sheets._EXPECTED_HEADERS)
    }


_STATUS_WORDS = {"ingested", "done", "slides", "skipped", "reel", "reels", "reel lines"}


def _looks_like_status(value: str) -> bool:
    text = " ".join((value or "").split()).lower()
    return bool(text) and (
        text in _STATUS_WORDS
        or text.startswith("error:")
        or text.startswith("needs source")
    )


def _looks_shifted(raw: list[str], columns: dict[str, int]) -> bool:
    """Whether this row reads like one written against the original column order.

    The status is the tell. A row written by an old checkout has it at the
    position Status used to occupy, and whatever field now lives there holds it
    instead. A row written correctly has it under Status and not there.
    """
    canonical_status = sheets._EXPECTED_HEADERS.index("Status")
    if columns["Status"] - 1 == canonical_status:
        return False  # the sheet is in its original order; nothing can be shifted
    at_old = raw[canonical_status] if canonical_status < len(raw) else ""
    at_new = raw[columns["Status"] - 1] if columns["Status"] - 1 < len(raw) else ""
    return _looks_like_status(at_old) and not _looks_like_status(at_new)


def _shorten(value: str, width: int = 34) -> str:
    text = " ".join((value or "").split())
    return (text[: width - 1] + "…") if len(text) > width else text


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rows", nargs="+", type=int, help="sheet row numbers to repair")
    parser.add_argument("--apply", action="store_true", help="write the repair (otherwise dry run)")
    parser.add_argument(
        "--force",
        action="store_true",
        help="repair a row even where it does not look shifted (rarely right)",
    )
    args = parser.parse_args()

    ws = sheets._worksheet(GOOGLE_SHEET_ID)
    columns = sheets._posts_columns(GOOGLE_SHEET_ID, ws)

    for row_number in args.rows:
        raw = sheets._with_backoff(ws.row_values, row_number)
        if not _looks_shifted(raw, columns) and not args.force:
            print(f"\nRow {row_number}: does not look shifted — skipped.")
            print("   Its status is already under Status. Repairing it would move "
                  "every field. Pass --force only if you are certain.")
            continue
        intended = _canonical_reading(raw)
        moves = []
        for header, index in columns.items():
            current = raw[index - 1] if index - 1 < len(raw) else ""
            if (current or "").strip() != (intended[header] or "").strip():
                moves.append((header, current, intended[header]))

        print(f"\nRow {row_number}: {len(moves)} field(s) to move")
        for header, current, wanted in moves:
            print(f"   {header:<22} {_shorten(current)!r:<38} -> {_shorten(wanted)!r}")
        if not moves:
            print("   already correct — nothing to do")
            continue

        if args.apply:
            sheets._with_backoff(
                ws.batch_update,
                sheets._posts_updates(columns, row_number, intended),
            )
            print("   written")

    if not args.apply:
        print("\nDry run. Re-run with --apply to write these changes.")
    else:
        sheets._invalidate_rows_cache(GOOGLE_SHEET_ID)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
