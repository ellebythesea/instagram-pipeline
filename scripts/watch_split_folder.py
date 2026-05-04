#!/usr/bin/env python3
"""Watch a local folder and auto-split new videos into one-minute segments.

Usage:
    .venv/bin/python scripts/watch_split_folder.py

Optional:
    .venv/bin/python scripts/watch_split_folder.py "/path/to/folder"
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from split_video_minutes import VIDEO_SUFFIXES, default_split_dir, output_dir_for_video, split_video_file


POLL_SECONDS = 5
STABLE_SECONDS = 10


def _video_files(folder: Path) -> list[Path]:
    return sorted(
        path for path in folder.iterdir()
        if path.is_file() and path.suffix.lower() in VIDEO_SUFFIXES
    )


def watch_folder(folder: Path) -> int:
    if not folder.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Not a folder: {folder}")

    print(f"Watching {folder}")
    seen_sizes: dict[Path, tuple[int, float]] = {}

    while True:
        current_files = set(_video_files(folder))
        current_time = time.time()

        for video_path in current_files:
            output_dir = output_dir_for_video(folder, video_path)
            if output_dir.exists() and any(output_dir.glob("*.mp4")):
                continue

            stat = video_path.stat()
            size = stat.st_size
            previous = seen_sizes.get(video_path)
            if previous is None or previous[0] != size:
                seen_sizes[video_path] = (size, current_time)
                continue

            stable_since = previous[1]
            if current_time - stable_since < STABLE_SECONDS:
                continue

            try:
                split_video_file(video_path, folder)
            except Exception as exc:
                print(f"Failed to split {video_path.name}: {exc}")
            finally:
                seen_sizes.pop(video_path, None)

        stale = [path for path in seen_sizes if path not in current_files]
        for path in stale:
            seen_sizes.pop(path, None)

        time.sleep(POLL_SECONDS)


def main() -> int:
    target = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else default_split_dir()
    watch_folder(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
