"""Shared caption-generation helpers for the sheet workflow."""

import json
import re

import openai

from config import DEFAULT_POST_FOOTER, OPENAI_API_KEY

client = openai.OpenAI(api_key=OPENAI_API_KEY, timeout=45.0, max_retries=1)
PINNED_TOP_COMMENT_PREFIX = "[[TOP]] "

SYS_PROMPT = (
    "You are a sharp political analyst. Rewrite the source material into a short, clear social post "
    "under 1300 characters using exactly two simple paragraphs.\n\n"
    "The first paragraph must be 250 characters or fewer and serve as the most important summary. "
    "You may naturally weave in a few important hashtags when they improve the post, especially "
    "for major names or core subjects. Keep those woven hashtags focused and preferably in the "
    "first paragraph. Do not force required hashtags into the prose because they will be appended "
    "separately after the footer. Keep the total number of hashtags across the final post to five "
    "or fewer.\n\n"
    "The second paragraph should add context using verified facts, dates, and numbers when relevant. "
    "Include direct quotes when available. Verify names and quotes carefully. "
    "Do not refer to the source as a transcript, clip, speech, interview, or video unless that is explicitly certain. "
    "Do not write phrases like during his speech, in the transcript, in this clip, or in the video. "
    "Write as if you are describing the underlying event or claim directly. "
    "Avoid speculation, flourish, links, or references to Trump's current office status."
)


def _extract_hashtags(text: str) -> list[str]:
    return re.findall(r"#[A-Za-z0-9_]+", text or "")


def _unique_hashtags_in_order(text: str) -> list[str]:
    seen = set()
    ordered = []
    for tag in _extract_hashtags(text):
        lowered = tag.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(tag)
    return ordered


def _remove_disallowed_hashtags(text: str, allowed_tags: set[str]) -> str:
    def repl(match: re.Match) -> str:
        tag = match.group(0)
        return tag if tag.lower() in allowed_tags else ""

    cleaned = re.sub(r"#[A-Za-z0-9_]+", repl, text or "")
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n[ \t]+\n", "\n\n", cleaned)
    cleaned = re.sub(r" +([,.;:!?])", r"\1", cleaned)
    return cleaned.strip()


def _finalize_required_hashtags(caption: str, required_hashtags: str) -> tuple[str, list[str]]:
    required = _unique_hashtags_in_order(required_hashtags)[:5]
    existing = _unique_hashtags_in_order(caption)

    allowed = list(required)
    allowed_lower = {tag.lower() for tag in allowed}
    for tag in existing:
        lowered = tag.lower()
        if lowered in allowed_lower:
            continue
        if len(allowed) >= 5:
            break
        allowed.append(tag)
        allowed_lower.add(lowered)

    caption = _remove_disallowed_hashtags(caption, allowed_lower)
    missing_required = [tag for tag in required if tag.lower() not in {t.lower() for t in _extract_hashtags(caption)}]
    remaining_slots = max(0, 5 - len({tag.lower() for tag in _extract_hashtags(caption)}))
    return caption, missing_required[:remaining_slots]


def _strip_top_comment_paragraphs(text: str, top_comment: str) -> str:
    text = (text or "").strip()
    top_comment = (top_comment or "").strip()
    if not text:
        return text

    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    cleaned: list[str] = []
    top_lower = top_comment.lower()

    for paragraph in paragraphs:
        normalized = paragraph.strip()
        lowered = normalized.lower()
        if top_comment and lowered == top_lower:
            continue
        if lowered.startswith("comment link (on instagram)"):
            continue
        cleaned.append(normalized)

    return "\n\n".join(cleaned).strip()


def _decode_top_comment(value: str) -> tuple[str, bool]:
    cleaned = (value or "").strip()
    if cleaned.startswith(PINNED_TOP_COMMENT_PREFIX):
        return cleaned[len(PINNED_TOP_COMMENT_PREFIX):].strip(), True
    return cleaned, False


def _row_source_text(row: dict) -> tuple[str, str, str]:
    transcript = row.get("Transcript", "").strip()
    original_caption = row.get("Original Caption", "").strip()
    caption_context = row.get("Caption Context", "").strip()
    content = transcript or original_caption or caption_context
    if not content:
        raise ValueError("No transcript, original caption, or caption context available")
    return transcript, original_caption, caption_context


def generate_row_caption(row: dict) -> str:
    """Generate a final caption string for one sheet row."""
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured.")

    transcript, original_caption, caption_context = _row_source_text(row)

    user_parts = []
    if transcript:
        user_parts.append(f"TRANSCRIPT:\n{transcript}")
    if original_caption:
        user_parts.append(f"ORIGINAL INSTAGRAM CAPTION:\n{original_caption}")
    if caption_context:
        user_parts.append(
            "ADDITIONAL CONTEXT FROM EDITOR:\n"
            f"{caption_context}\n"
            "Use this to fill in missing context, but do not present uncertain claims as facts."
        )
    if not user_parts:
        user_parts.append(f"SOURCE TEXT:\n{content}")

    if row.get("Speaker Name", "").strip():
        user_parts.append(
            f"The person featured here is: {row['Speaker Name'].strip()}. "
            "Mention their name once, then refer to them with he, she, or they. "
            "If gender is unclear, use they. Do not repeat their name multiple times."
        )

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYS_PROMPT},
            {"role": "user", "content": "\n\n".join(user_parts)},
        ],
        max_tokens=600,
        temperature=0.35,
    )
    caption = response.choices[0].message.content.strip()

    top_comment, pin_top_comment = _decode_top_comment(row.get("Top Comment", ""))
    if top_comment:
        caption = _strip_top_comment_paragraphs(caption, top_comment)
        original_caption = _strip_top_comment_paragraphs(original_caption, top_comment)
    if top_comment:
        if pin_top_comment:
            caption = f"{top_comment}\n\n{caption}"
        else:
            caption = f"{caption}\n\n{top_comment}"

    media_type = (row.get("Media Type", "") or "").strip().lower()

    if original_caption and media_type != "article":
        caption = f"{caption}\n\n--\n\n{original_caption}"

    required_hashtags = row.get("Required Hashtags", "").strip()
    appended_required = []
    if required_hashtags:
        caption, appended_required = _finalize_required_hashtags(caption, required_hashtags)

    username = row.get("Source Username", "").strip().lstrip("@")
    footer_parts = []
    if media_type != "article" and username and username.lower() != "unknown":
        footer_parts.append(f"Follow @{username} for more.")

    footer = DEFAULT_POST_FOOTER.strip()
    if footer:
        footer_parts.append(footer)

    if appended_required:
        footer_parts.append(" ".join(appended_required))

    if footer_parts:
        caption = f"{caption}\n\n{' '.join(footer_parts)}"

    return caption


def generate_carousel_copy(row: dict) -> dict[str, str]:
    return generate_carousel_copy_with_model(row, model="gpt-4o")


def generate_carousel_copy_with_model(row: dict, model: str = "gpt-4o") -> dict[str, str]:
    """Generate Figma/Google Sync carousel fields."""
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured.")

    transcript, original_caption, caption_context = _row_source_text(row)
    speaker_name = row.get("Speaker Name", "").strip()
    username = row.get("Source Username", "").strip()
    media_type = (row.get("Media Type", "") or "").strip().lower()
    if speaker_name:
        display_name = speaker_name
    else:
        display_name = f"@{username.lstrip('@')}" if media_type != "article" and username else username

    user_parts = []
    if transcript:
        user_parts.append(f"TRANSCRIPT:\n{transcript}")
    if original_caption:
        user_parts.append(f"ORIGINAL SOURCE TEXT:\n{original_caption}")
    if caption_context:
        user_parts.append(f"ADDITIONAL CONTEXT:\n{caption_context}")

    if row.get("Speaker Name", "").strip():
        user_parts.append(f"Featured person: {row['Speaker Name'].strip()}")

    prompt = (
        "Create three clickbait carousel slides as JSON with keys "
        "\"name\", \"text1\", \"text2\", \"text3\".\n"
        "- name: short username/domain label only\n"
        "- text1: strong hook, max 150 characters\n"
        "- text2: max 300 characters\n"
        "- text3: max 300 characters\n"
        "- no hashtags\n"
        "- no intro labels like Slide 1\n"
        "- keep each field as plain text only\n"
        f"- use this label for name when possible: {display_name or 'unknown'}"
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You write concise viral political carousel copy and return valid JSON only."},
            {"role": "user", "content": prompt + "\n\n" + "\n\n".join(user_parts)},
        ],
        max_tokens=500,
        temperature=0.45,
    )
    raw = response.choices[0].message.content.strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    payload = json.loads(match.group(0) if match else raw)

    return {
        "name": (payload.get("name") or display_name or "").strip(),
        "text1": (payload.get("text1") or "").strip()[:150],
        "text2": (payload.get("text2") or "").strip()[:300],
        "text3": (payload.get("text3") or "").strip()[:300],
    }
