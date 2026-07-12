"""Montage Generator page: overlay up to four people (duotone cutouts) on a background."""

import hashlib
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from PIL import Image, ImageOps
from rembg import remove

from utils.auth import require_auth
from utils.styles import inject as inject_styles

# Matches PREVIEW_EXPORT_WIDTH_PX / PREVIEW_EXPORT_HEIGHT_PX in pages/workspace.py,
# the export size used by the existing post preview/screenshot flow.
CANVAS_WIDTH_PX = 1080
CANVAS_HEIGHT_PX = 1350

MAX_PERSON_DIMENSION = 1200
DEFAULT_BACKGROUND_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "assets", "default_background.png"
)
SHADOW_COLOR = "#004B9F"
HIGHLIGHT_COLOR = "#E9E9EE"
CONTRAST = 2.0
SLOT_NUMBERS = (1, 2, 3, 4)
SLOT_CENTER_FRACTIONS = {1: 0.18, 2: 0.40, 3: 0.60, 4: 0.82}
SECOND_PERSON_OFFSET_PX = 0.35 * CANVAS_WIDTH_PX
MOVE_STEP_PX = 40
ZOOM_STEP = 0.05


def _cap_max_dimension(image: Image.Image, max_dim: int) -> Image.Image:
    width, height = image.size
    largest = max(width, height)
    if largest <= max_dim:
        return image
    scale = max_dim / largest
    return image.resize((max(1, round(width * scale)), max(1, round(height * scale))), Image.LANCZOS)


def _apply_duotone(image: Image.Image, shadow_hex: str, highlight_hex: str) -> Image.Image:
    rgba = image.convert("RGBA")
    alpha = rgba.split()[3]
    gray = rgba.convert("L")
    gray = ImageOps.autocontrast(gray, cutoff=1)
    # Gamma curve anchored at true black/white (not the photo's own average) so midtones
    # get crushed toward the shadow color regardless of how bright the source photo is.
    gray = gray.point(lambda p: min(255, max(0, round(255 * (p / 255) ** CONTRAST))))
    colorized = ImageOps.colorize(gray, black=shadow_hex, white=highlight_hex).convert("RGBA")
    colorized.putalpha(alpha)
    return colorized


@st.cache_data(show_spinner="Removing background...")
def _process_person_image(image_bytes: bytes, shadow_hex: str, highlight_hex: str) -> bytes:
    image = Image.open(io.BytesIO(image_bytes))
    image = ImageOps.exif_transpose(image)
    image = _cap_max_dimension(image.convert("RGB"), MAX_PERSON_DIMENSION)
    cutout = remove(image)
    duotoned = _apply_duotone(cutout, shadow_hex, highlight_hex)
    buffer = io.BytesIO()
    duotoned.save(buffer, format="PNG")
    return buffer.getvalue()


def _uploaded_file_id(uploaded_file, raw_bytes: bytes) -> str:
    file_id = getattr(uploaded_file, "file_id", None)
    if file_id:
        return file_id
    return hashlib.md5(raw_bytes).hexdigest()


def _default_transform(person_width: int, person_height: int, center_x: float) -> dict:
    scale = CANVAS_HEIGHT_PX / person_height if person_height else 1.0
    scaled_width = person_width * scale
    return {
        "x": round(center_x - scaled_width / 2),
        "y": 0,
        "scale": round(scale, 4),
        "width": person_width,
        "height": person_height,
    }


def _transform_center_x(transform: dict) -> float:
    return transform["x"] + (transform["width"] * transform["scale"]) / 2


def _load_background(uploaded_file) -> Image.Image | None:
    if uploaded_file is not None:
        return Image.open(uploaded_file).convert("RGBA")
    if os.path.exists(DEFAULT_BACKGROUND_PATH):
        return Image.open(DEFAULT_BACKGROUND_PATH).convert("RGBA")
    return None


def _fit_background(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    return ImageOps.fit(image.convert("RGB"), size, method=Image.LANCZOS, centering=(0.5, 0.5)).convert("RGBA")


def _composite_layer(canvas: Image.Image, person_image: Image.Image, transform: dict) -> Image.Image:
    scale = transform["scale"]
    new_size = (max(1, round(person_image.width * scale)), max(1, round(person_image.height * scale)))
    scaled = person_image.resize(new_size, Image.LANCZOS)
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    layer.paste(scaled, (round(transform["x"]), round(transform["y"])), scaled)
    return Image.alpha_composite(canvas, layer)


st.set_page_config(page_title="Montage Generator", page_icon="🖼️", layout="centered")
inject_styles()
st.title("Montage Generator")
st.caption(
    "Overlay up to four people (background removed, blue duotone) on a background, "
    "positioned with buttons, and export as PNG."
)

if not require_auth():
    st.stop()

if "montage_transforms" not in st.session_state:
    st.session_state.montage_transforms = {}
if "montage_file_ids" not in st.session_state:
    st.session_state.montage_file_ids = {}

bg_upload = st.file_uploader(
    "Use a different background (optional)",
    type=["png", "jpg", "jpeg", "webp"],
    key="montage_bg_upload",
)

st.subheader("People")
person_images: dict[int, Image.Image] = {}
person_filenames: dict[int, str] = {}
newly_uploaded_slots: set[int] = set()
for slot in SLOT_NUMBERS:
    uploaded = st.file_uploader(
        f"Person {slot}",
        type=["png", "jpg", "jpeg", "webp"],
        key=f"montage_person_upload_{slot}",
    )
    if uploaded is None:
        st.session_state.montage_file_ids.pop(slot, None)
        st.session_state.montage_transforms.pop(slot, None)
        continue

    raw_bytes = uploaded.getvalue()
    file_id = _uploaded_file_id(uploaded, raw_bytes)
    if st.session_state.montage_file_ids.get(slot) != file_id:
        original = ImageOps.exif_transpose(Image.open(io.BytesIO(raw_bytes)))
        capped = _cap_max_dimension(original, MAX_PERSON_DIMENSION)
        st.session_state.montage_file_ids[slot] = file_id
        st.session_state.montage_transforms[slot] = _default_transform(
            capped.width, capped.height, CANVAS_WIDTH_PX * SLOT_CENTER_FRACTIONS[slot]
        )
        newly_uploaded_slots.add(slot)

    processed_bytes = _process_person_image(raw_bytes, SHADOW_COLOR, HIGHLIGHT_COLOR)
    person_images[slot] = Image.open(io.BytesIO(processed_bytes)).convert("RGBA")
    person_filenames[slot] = uploaded.name

# When a second person joins, default them further right of the first
# instead of the fixed per-slot fraction, so a two-person montage starts centered.
filled_slots_now = sorted(person_images.keys())
if len(filled_slots_now) == 2 and filled_slots_now[1] in newly_uploaded_slots:
    first_slot, second_slot = filled_slots_now
    first_transform = st.session_state.montage_transforms[first_slot]
    second_transform = st.session_state.montage_transforms[second_slot]
    new_center_x = _transform_center_x(first_transform) + SECOND_PERSON_OFFSET_PX
    scaled_width_second = second_transform["width"] * second_transform["scale"]
    second_transform["x"] = round(new_center_x - scaled_width_second / 2)

st.subheader("Preview")
st.caption("Layer order: Person 1 is placed first (back) through Person 4 last (front).")

filled_slots = filled_slots_now
selected_slot = None
if filled_slots:
    labels = [f"{slot}-{person_filenames[slot]}" for slot in filled_slots]
    current_label = st.session_state.get("montage_selected_person")
    index = labels.index(current_label) if current_label in labels else 0
    selected_label = st.selectbox("Select person to move", labels, index=index, key="montage_selected_person")
    selected_slot = filled_slots[labels.index(selected_label)]
else:
    st.info("Upload at least one person photo above to begin positioning.")

preview_placeholder = st.empty()

if selected_slot is not None:
    transform = st.session_state.montage_transforms[selected_slot]

    move_cols = st.columns(4)
    if move_cols[0].button("⬆️", key="montage_move_up", width="stretch"):
        transform["y"] -= MOVE_STEP_PX
    if move_cols[1].button("⬇️", key="montage_move_down", width="stretch"):
        transform["y"] += MOVE_STEP_PX
    if move_cols[2].button("⬅️", key="montage_move_left", width="stretch"):
        transform["x"] -= MOVE_STEP_PX
    if move_cols[3].button("➡️", key="montage_move_right", width="stretch"):
        transform["x"] += MOVE_STEP_PX

    zoom_cols = st.columns(2)
    if zoom_cols[0].button("➕", key="montage_zoom_bigger", width="stretch"):
        transform["scale"] = min(5.0, transform["scale"] + ZOOM_STEP)
    if zoom_cols[1].button("➖", key="montage_zoom_smaller", width="stretch"):
        transform["scale"] = max(0.05, transform["scale"] - ZOOM_STEP)

background = _load_background(bg_upload)
if background is None:
    with preview_placeholder:
        st.warning(
            f"No background found. Add your default at `assets/default_background.png`, "
            "or upload one above to get started."
        )
else:
    canvas = _fit_background(background, (CANVAS_WIDTH_PX, CANVAS_HEIGHT_PX))
    for slot in SLOT_NUMBERS:
        person_image = person_images.get(slot)
        if person_image is None:
            continue
        canvas = _composite_layer(canvas, person_image, st.session_state.montage_transforms[slot])

    with preview_placeholder:
        st.image(canvas, width="stretch")

    output_buffer = io.BytesIO()
    canvas.save(output_buffer, format="PNG")
    st.download_button(
        "Download montage PNG",
        data=output_buffer.getvalue(),
        file_name="montage.png",
        mime="image/png",
        width="stretch",
    )
