#!/usr/bin/env python3
"""Blur the cover of article rows that are still carrying a sharp one.

An article's lead image is somebody else's press photo, so ingest blurs it by
default. Rows ingested before that default existed — and rows where the blur
gave way to a download or upload failure — still show the sharp photo, and
nothing about the row says which. This walks them and blurs what is left.

Two shapes of row need repairing, and they need different handling:

  * The thumbnail is a Drive link. The sharp image is already uploaded, so the
    fix is to fetch it back, blur it, and point the row at the blurred copy.
  * The thumbnail is still the outlet's own image URL, which is what a failed
    download leaves behind. There is no Drive copy at all, so the row goes
    through the same path ingest uses, hotlink-tolerant Referer included.

Either way the sharp version is remembered against the row first, so the
editor's Unblur button restores it. A row that is already blurred is skipped.

    python scripts/blur_article_covers.py                 # show what it would do
    python scripts/blur_article_covers.py --apply
    python scripts/blur_article_covers.py --apply --rows 12 13
"""
import argparse
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    GOOGLE_SHEET_ID,
)
from drive import download_drive_file, get_or_create_subfolder, upload_to_drive
from ingest_helpers import article_thumbnail_link, blur_image_file
from sheets import (
    get_all_rows,
    get_original_thumbnails,
    save_original_thumbnail,
    update_thumbnail_link,
)


def _text(value) -> str:
    return str(value or "").strip()


def _is_drive_link(link: str) -> bool:
    return "drive.google.com" in link or "docs.google.com" in link


def _already_blurred(row: dict, originals: dict) -> bool:
    """True when this row's cover has already been through a blur.

    A remembered original is the reliable marker — it is only written when
    something replaced the sharp version. The filename check catches rows
    blurred by an ingest whose bookkeeping write did not land.
    """
    if str(row["row_number"]) in originals:
        return True
    return _text(row.get("Thumbnail Drive Link")).lower().endswith("_blur.jpg")


def _blur_drive_thumbnail(row: dict, thumb_link: str) -> str:
    """Re-blur a cover that is already on Drive, and hand back the new link."""
    row_num = row["row_number"]
    screenshots_folder_id = get_or_create_subfolder(
        GOOGLE_DRIVE_FOLDER_ID, GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER
    )
    tmp_dir = tempfile.mkdtemp(prefix="blur_backfill_")
    try:
        src_path = os.path.join(tmp_dir, "thumb_src")
        download_drive_file(thumb_link, src_path)
        out_path = blur_image_file(src_path, os.path.join(tmp_dir, "thumb_blur.jpg"))
        upload_name = f"row_{row_num}_thumb_blur.jpg"
        blurred_link = upload_to_drive(out_path, upload_name, screenshots_folder_id)
        # Remembered only once the blurred copy exists, so a failure part way
        # through does not leave the row pointing at a sharp image it now
        # believes is the blurred one.
        save_original_thumbnail(GOOGLE_SHEET_ID, int(row_num), thumb_link)
        return blurred_link
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _blur_hotlinked_thumbnail(row: dict, thumb_link: str) -> tuple[str, str]:
    """Take a cover that never reached Drive through the ingest path."""
    row_num = row["row_number"]
    return article_thumbnail_link(
        thumb_link,
        row_num,
        _text(row.get("Source Username")),
        page_url=_text(row.get("Instagram URL")),
        remember_original=lambda original: save_original_thumbnail(
            GOOGLE_SHEET_ID, int(row_num), original
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apply", action="store_true", help="Write the changes; otherwise just report.")
    parser.add_argument("--rows", nargs="*", type=int, default=None, help="Limit to these row numbers.")
    args = parser.parse_args()

    rows = get_all_rows(GOOGLE_SHEET_ID)
    originals = get_original_thumbnails(GOOGLE_SHEET_ID)
    wanted = set(args.rows) if args.rows else None

    candidates = []
    for row in rows:
        if _text(row.get("Media Type")).lower() != "article":
            continue
        if wanted is not None and int(row["row_number"]) not in wanted:
            continue
        if not _text(row.get("Thumbnail Drive Link")):
            continue
        if _already_blurred(row, originals):
            continue
        candidates.append(row)

    if not candidates:
        print("No article rows are carrying a sharp cover.")
        return

    print(f"{len(candidates)} article row(s) with a sharp cover:")
    for row in candidates:
        link = _text(row.get("Thumbnail Drive Link"))
        shape = "drive" if _is_drive_link(link) else "outlet hotlink"
        print(f"  row {row['row_number']}  ({shape})  {link[:80]}")

    if not args.apply:
        print("\nDry run. Re-run with --apply to blur these.")
        return

    blurred = 0
    for row in candidates:
        row_num = row["row_number"]
        link = _text(row.get("Thumbnail Drive Link"))
        try:
            if _is_drive_link(link):
                new_link = _blur_drive_thumbnail(row, link)
                note = ""
            else:
                new_link, note = _blur_hotlinked_thumbnail(row, link)
            if not new_link or new_link == link:
                print(f"  row {row_num}: unchanged — {note or 'nothing came back'}")
                continue
            update_thumbnail_link(GOOGLE_SHEET_ID, int(row_num), new_link)
            blurred += 1
            print(f"  row {row_num}: blurred{f' ({note})' if note else ''}")
        except Exception as exc:  # noqa: BLE001 - one bad row should not stop the rest
            print(f"  row {row_num}: failed — {exc}")

    print(f"\nBlurred {blurred} of {len(candidates)} row(s).")


if __name__ == "__main__":
    main()
