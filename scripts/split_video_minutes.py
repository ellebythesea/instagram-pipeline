#!/usr/bin/env python3
"""Split locally downloaded videos into exact one-minute 4:6 segments.

Usage:
    .venv/bin/python scripts/split_video_minutes.py

Optional:
    .venv/bin/python scripts/split_video_minutes.py "/path/to/folder"
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


SPLIT_DIR_SUFFIX = (
    Path("_apps")
    / "vioo instagram pipeline"
    / "instagram pipeline media"
    / "splits"
)

SPLIT_DIR_CANDIDATES = [
    Path.home()
    / "Library"
    / "CloudStorage"
    / "GoogleDrive-voteinorout@gmail.com"
    / "My Drive"
    / SPLIT_DIR_SUFFIX,
    Path("/Users/lisa")
    / "Library"
    / "CloudStorage"
    / "GoogleDrive-voteinorout@gmail.com"
    / "My Drive"
    / SPLIT_DIR_SUFFIX,
    Path("/Users/lisamollica")
    / "Library"
    / "CloudStorage"
    / "GoogleDrive-voteinorout@gmail.com"
    / "My Drive"
    / SPLIT_DIR_SUFFIX,
]


def default_split_dir() -> Path:
    for candidate in SPLIT_DIR_CANDIDATES:
        if candidate.exists():
            return candidate

    cloud_storage = Path.home() / "Library" / "CloudStorage"
    if cloud_storage.exists():
        matches = sorted(cloud_storage.glob(f"GoogleDrive-*/My Drive/{SPLIT_DIR_SUFFIX}"))
        if matches:
            return matches[0]

    return SPLIT_DIR_CANDIDATES[0]

VIDEO_SUFFIXES = {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"}
TARGET_ASPECT_RATIO = "2/3"
NUMBER_WORDS = [
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
    "twenty",
    "twenty_one",
    "twenty_two",
    "twenty_three",
    "twenty_four",
    "twenty_five",
    "twenty_six",
    "twenty_seven",
    "twenty_eight",
    "twenty_nine",
    "thirty",
    "thirty_one",
    "thirty_two",
    "thirty_three",
    "thirty_four",
    "thirty_five",
    "thirty_six",
    "thirty_seven",
    "thirty_eight",
    "thirty_nine",
    "forty",
    "forty_one",
    "forty_two",
    "forty_three",
    "forty_four",
    "forty_five",
    "forty_six",
    "forty_seven",
    "forty_eight",
    "forty_nine",
    "fifty",
    "fifty_one",
    "fifty_two",
    "fifty_three",
    "fifty_four",
    "fifty_five",
    "fifty_six",
    "fifty_seven",
    "fifty_eight",
    "fifty_nine",
    "sixty",
]


def _segment_name(index: int) -> str:
    if 0 <= index < len(NUMBER_WORDS):
        return NUMBER_WORDS[index]
    return f"{index + 1:02d}"


def _video_files(folder: Path) -> list[Path]:
    return sorted(
        path for path in folder.iterdir()
        if path.is_file() and path.suffix.lower() in VIDEO_SUFFIXES
    )


def _run_ffmpeg(input_path: Path, output_dir: Path) -> list[Path]:
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError("ffmpeg is not installed or not on PATH.")

    crop_width = f"if(gte(iw/ih,{TARGET_ASPECT_RATIO}),trunc(ih*{TARGET_ASPECT_RATIO}/2)*2,iw)"
    crop_height = f"if(gte(iw/ih,{TARGET_ASPECT_RATIO}),ih,trunc(iw/({TARGET_ASPECT_RATIO})/2)*2)"
    video_filter = (
        f"crop={crop_width}:{crop_height}:(iw-ow)/2:(ih-oh)/2,"
        "scale=trunc(iw/2)*2:trunc(ih/2)*2"
    )
    tmp_pattern = output_dir / "segment_%03d.mp4"
    command = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(input_path),
        "-vf",
        video_filter,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-force_key_frames",
        "expr:gte(t,n_forced*60)",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-f",
        "segment",
        "-segment_time",
        "60",
        "-reset_timestamps",
        "1",
        str(tmp_pattern),
    ]
    subprocess.run(command, check=True)
    return sorted(output_dir.glob("segment_*.mp4"))


def _rename_segments(output_dir: Path, segments: list[Path]) -> list[Path]:
    renamed: list[Path] = []
    for index, segment_path in enumerate(segments):
        target = output_dir / f"{_segment_name(index)}.mp4"
        segment_path.rename(target)
        renamed.append(target)
    return renamed


def output_dir_for_video(folder: Path, video_path: Path) -> Path:
    return folder / f"{video_path.stem}_segments"


def split_video_file(video_path: Path, base_folder: Path | None = None) -> int:
    folder = base_folder or video_path.parent
    output_dir = output_dir_for_video(folder, video_path)
    if output_dir.exists() and any(output_dir.glob("*.mp4")):
        print(f"Skipping {video_path.name}: segments already exist in {output_dir.name}")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        segments = _run_ffmpeg(video_path, output_dir)
        renamed = _rename_segments(output_dir, segments)
    except Exception:
        shutil.rmtree(output_dir, ignore_errors=True)
        raise

    print(f"Split {video_path.name} into {len(renamed)} segment(s) in {output_dir.name}")
    return len(renamed)


def split_folder(folder: Path) -> int:
    if not folder.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Not a folder: {folder}")

    videos = _video_files(folder)
    if not videos:
        print(f"No video files found in {folder}")
        return 0

    processed = 0
    for video_path in videos:
        processed += 1 if split_video_file(video_path, folder) else 0

    if not processed:
        print("Nothing new to split.")
    return processed


def main() -> int:
    target = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else default_split_dir()
    print(f"Using split folder: {target}")
    split_folder(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
