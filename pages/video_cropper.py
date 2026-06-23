"""Video cropper — center-crop uploads to common social aspect ratios."""

import os
import subprocess
import tempfile

import streamlit as st


def _ffmpeg_path() -> str:
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return "ffmpeg"


def _ffprobe_path() -> str:
    try:
        import imageio_ffmpeg
        exe = imageio_ffmpeg.get_ffmpeg_exe()
        probe = exe.replace("ffmpeg", "ffprobe")
        if os.path.exists(probe):
            return probe
    except Exception:
        pass
    return "ffprobe"


def _video_dimensions(path: str) -> tuple[int, int]:
    result = subprocess.run(
        [
            _ffprobe_path(), "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            path,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        parts = result.stdout.strip().split(",")
        if len(parts) == 2:
            try:
                return int(parts[0]), int(parts[1])
            except ValueError:
                pass
    raise RuntimeError(f"Could not read video dimensions:\n{result.stderr}")


def _crop_video(input_path: str, output_path: str, ratio_w: int, ratio_h: int) -> None:
    w, h = _video_dimensions(input_path)
    target = ratio_w / ratio_h
    current = w / h

    if current > target:
        new_w = int(h * ratio_w / ratio_h)
        new_h = h
    else:
        new_w = w
        new_h = int(w * ratio_h / ratio_w)

    # make dimensions even (required by most codecs)
    new_w = new_w - (new_w % 2)
    new_h = new_h - (new_h % 2)

    x = (w - new_w) // 2
    y = (h - new_h) // 2

    cmd = [
        _ffmpeg_path(), "-y", "-i", input_path,
        "-vf", f"crop={new_w}:{new_h}:{x}:{y}",
        "-c:v", "libx264", "-crf", "18", "-preset", "fast",
        "-c:a", "copy",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode(errors="replace"))


RATIOS = {
    "4:5  (Instagram portrait)": (4, 5),
    "9:16 (Stories / Reels)":    (9, 16),
    "1:1  (Square)":             (1, 1),
    "16:9 (Landscape)":          (16, 9),
}

st.set_page_config(page_title="Video Cropper", layout="centered")
st.title("Video Cropper")
st.caption("Center-crop a video to a standard social aspect ratio.")

uploaded = st.file_uploader("Upload video", type=["mp4", "mov", "avi", "mkv", "m4v"])
ratio_label = st.radio("Crop to", list(RATIOS.keys()), horizontal=True)

if uploaded and st.button("Crop video", type="primary"):
    rw, rh = RATIOS[ratio_label]
    suffix = os.path.splitext(uploaded.name)[-1] or ".mp4"

    with (
        tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as src_f,
        tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as out_f,
    ):
        src_path = src_f.name
        out_path = out_f.name

    try:
        with open(src_path, "wb") as f:
            f.write(uploaded.read())

        with st.spinner(f"Cropping to {rw}:{rh}…"):
            _crop_video(src_path, out_path, rw, rh)

        with open(out_path, "rb") as f:
            video_bytes = f.read()

        st.success("Done!")
        st.video(video_bytes)

        base = os.path.splitext(uploaded.name)[0]
        st.download_button(
            label="Download cropped video",
            data=video_bytes,
            file_name=f"{base}_{rw}x{rh}.mp4",
            mime="video/mp4",
        )
    except Exception as e:
        st.error(f"Crop failed: {e}")
    finally:
        for p in (src_path, out_path):
            try:
                os.unlink(p)
            except Exception:
                pass
