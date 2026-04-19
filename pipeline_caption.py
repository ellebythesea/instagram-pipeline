"""Shared caption-generation helpers for the sheet workflow."""

import openai

from config import DEFAULT_POST_FOOTER, OPENAI_API_KEY

client = openai.OpenAI(api_key=OPENAI_API_KEY)

SYS_PROMPT = (
    "You are a sharp political analyst. Rewrite the transcript into a short, clear social post "
    "under 1300 characters using exactly two simple paragraphs.\n\n"
    "The first paragraph must be 250 characters or fewer and serve as the most important summary. "
    "It must include all hashtags. Use 3 to 5 relevant hashtags total. Prioritize the main people "
    "the post is about, then include one single word subject hashtag that helps with trending news "
    "discovery, followed by any remaining relevant tags. Replace the normal word or phrase in the "
    "sentence with the hashtag version, for example use #DonaldTrump in the sentence instead of "
    "writing the name normally. Do not add a separate hashtag only line at the end.\n\n"
    "The second paragraph should add context using verified facts, dates, and numbers when relevant. "
    "Include direct quotes from the transcript when available. Verify names and quotes carefully. "
    "Any hashtag used in the caption body counts toward the total of 3 to 5 hashtags. Avoid "
    "speculation, flourish, links, or references to Trump's current office status."
)


def generate_row_caption(row: dict) -> str:
    """Generate a final caption string for one sheet row."""
    content = row.get("Transcript", "").strip() or row.get("Original Caption", "").strip()
    if not content:
        raise ValueError("No transcript or original caption available")

    user_parts = [f"TRANSCRIPT:\n{content}"]
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

    username = row.get("Source Username", "").strip().lstrip("@")
    footer_parts = []
    if username and username.lower() != "unknown":
        footer_parts.append(f"Follow @{username} for more.")

    footer = DEFAULT_POST_FOOTER.strip()
    if footer:
        footer_parts.append(footer)
    if footer_parts:
        caption = f"{caption}\n\n{' '.join(footer_parts)}"

    required_hashtags = row.get("Required Hashtags", "").strip()
    if required_hashtags:
        caption = f"{caption}\n\n{required_hashtags}"

    return caption
