"""Shared caption-generation helpers for the sheet workflow."""

import re

import openai

from config import DEFAULT_POST_FOOTER, OPENAI_API_KEY

client = openai.OpenAI(api_key=OPENAI_API_KEY)

SYS_PROMPT = (
    "You are a sharp political analyst. Rewrite the transcript into a short, clear social post "
    "under 1300 characters using exactly two simple paragraphs.\n\n"
    "The first paragraph must be 250 characters or fewer and serve as the most important summary. "
    "You may naturally weave in a few important hashtags when they improve the post, especially "
    "for major names or core subjects. Keep those woven hashtags focused and preferably in the "
    "first paragraph. Do not force required hashtags into the prose because they will be appended "
    "separately after the footer. Keep the total number of hashtags across the final post to five "
    "or fewer.\n\n"
    "The second paragraph should add context using verified facts, dates, and numbers when relevant. "
    "Include direct quotes from the transcript when available. Verify names and quotes carefully. "
    "Avoid speculation, flourish, links, or references to Trump's current office status."
)


def _extract_hashtags(text: str) -> list[str]:
    return re.findall(r"#[A-Za-z0-9_]+", text or "")


def _collect_required_hashtags(caption: str, required_hashtags: str) -> list[str]:
    existing = {tag.lower() for tag in _extract_hashtags(caption)}
    remaining_slots = max(0, 5 - len(existing))
    if remaining_slots == 0:
        return []

    requested = []
    for tag in _extract_hashtags(required_hashtags):
        if tag.lower() in existing:
            continue
        if tag.lower() in {t.lower() for t in requested}:
            continue
        requested.append(tag)
        if len(requested) >= remaining_slots:
            break

    return requested


def generate_row_caption(row: dict) -> str:
    """Generate a final caption string for one sheet row."""
    transcript = row.get("Transcript", "").strip()
    original_caption = row.get("Original Caption", "").strip()
    caption_context = row.get("Caption Context", "").strip()

    content = transcript or original_caption or caption_context
    if not content:
        raise ValueError("No transcript, original caption, or caption context available")

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
            f"The speaker in this transcript is: {row['Speaker Name'].strip()}. Reference them by name."
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

    if row.get("Top Comment", "").strip():
        caption = f"{row['Top Comment'].strip()}\n\n{caption}"

    if original_caption:
        caption = f"{caption}\n\n--\n\n{original_caption}"

    username = row.get("Source Username", "").strip().lstrip("@")
    footer_parts = []
    if username and username.lower() != "unknown":
        footer_parts.append(f"Follow @{username} for more.")

    footer = DEFAULT_POST_FOOTER.strip()
    if footer:
        footer_parts.append(footer)

    required_hashtags = row.get("Required Hashtags", "").strip()
    appended_required = _collect_required_hashtags(caption, required_hashtags) if required_hashtags else []
    if appended_required:
        footer_parts.append(" ".join(appended_required))

    if footer_parts:
        caption = f"{caption}\n\n{' '.join(footer_parts)}"

    return caption
