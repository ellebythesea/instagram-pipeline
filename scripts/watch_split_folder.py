#!/usr/bin/env python3
"""Watch a local folder and auto-split new videos into one-minute segments.

Usage:
    .venv/bin/python scripts/watch_split_folder.py

Optional:
    .venv/bin/python scripts/watch_split_folder.py "/path/to/folder"
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from split_video_minutes import VIDEO_SUFFIXES, default_split_dir, output_dir_for_video, split_video_file


POLL_SECONDS = 5
STABLE_SECONDS = 10
DEFAULT_IDLE_POLLS = 2
FAILURE_RETRY_SECONDS = 120


def _video_files(folder: Path) -> list[Path]:
    return sorted(
        path for path in folder.iterdir()
        if path.is_file() and path.suffix.lower() in VIDEO_SUFFIXES
    )


def _needs_split(folder: Path, video_path: Path) -> bool:
    output_dir = output_dir_for_video(folder, video_path)
    return not (output_dir.exists() and any(output_dir.glob("*.mp4")))


def _split_existing_unsplit_videos(folder: Path) -> int:
    processed = 0
    for video_path in _video_files(folder):
        if not _needs_split(folder, video_path):
            continue
        try:
            split_video_file(video_path, folder)
        except Exception as exc:
            print(f"Failed to split {video_path.name}: {exc}")
        else:
            processed += 1
    return processed


def watch_folder(folder: Path, stop_when_idle: bool = False, idle_polls: int = DEFAULT_IDLE_POLLS) -> int:
    if not folder.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Not a folder: {folder}")

    print(f"Watching {folder}")
    processed_total = 0
    startup_processed = _split_existing_unsplit_videos(folder)
    processed_total += startup_processed
    if startup_processed:
        print(f"Startup pass split {startup_processed} existing video(s).")
    else:
        print("Startup pass found no unsplit existing videos.")
    seen_sizes: dict[Path, tuple[int, float]] = {}
    failed_at: dict[Path, float] = {}
    idle_cycles = 0

    while True:
        current_files = set(_video_files(folder))
        current_time = time.time()
        processed_this_cycle = 0

        for video_path in current_files:
            if not _needs_split(folder, video_path):
                failed_at.pop(video_path, None)
                continue

            last_failure = failed_at.get(video_path)
            if last_failure is not None and current_time - last_failure < FAILURE_RETRY_SECONDS:
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
                processed_this_cycle += 1
                failed_at.pop(video_path, None)
            except Exception as exc:
                print(f"Failed to split {video_path.name}: {exc}")
                failed_at[video_path] = current_time
            finally:
                seen_sizes.pop(video_path, None)

        stale = [path for path in seen_sizes if path not in current_files]
        for path in stale:
            seen_sizes.pop(path, None)
        for path in [p for p in failed_at if p not in current_files]:
            failed_at.pop(path, None)

        processed_total += processed_this_cycle
        if stop_when_idle:
            unsplit_remaining = any(_needs_split(folder, video_path) for video_path in current_files)
            if not unsplit_remaining and not seen_sizes:
                idle_cycles += 1
            else:
                idle_cycles = 0
            if idle_cycles >= max(1, idle_polls):
                print(f"Folder is idle. Stopping watcher after splitting {processed_total} video(s).")
                return processed_total

        time.sleep(POLL_SECONDS)


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch a folder and split videos into one-minute segments.")
    parser.add_argument("folder", nargs="?", default="", help="Optional folder to watch.")
    parser.add_argument(
        "--until-idle",
        action="store_true",
        help="Stop automatically after the folder has no remaining unsplit videos for a couple of polls.",
    )
    args = parser.parse_args()
    target = Path(args.folder).expanduser() if args.folder else default_split_dir()
    watch_folder(target, stop_when_idle=args.until_idle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
