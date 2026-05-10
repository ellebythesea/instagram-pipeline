"""Shared caption-generation helpers for the sheet workflow."""

import ast
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


def _completion_limit_arg(model: str, token_limit: int) -> dict:
    normalized = (model or "").strip().lower()
    if normalized.startswith("gpt-5") or normalized.startswith("o"):
        return {"max_completion_tokens": token_limit}
    return {"max_tokens": token_limit}


def _parse_jsonish_payload(raw_text: str):
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("Model returned an empty response.")

    def _strip_comments(candidate: str) -> str:
        without_block_comments = re.sub(r"/\*[\s\S]*?\*/", "", candidate)
        return re.sub(r"(?m)^\s*//.*$", "", without_block_comments)

    def _extract_block(candidate: str) -> str:
        candidate = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate.strip(), flags=re.IGNORECASE | re.MULTILINE)
        candidate = _strip_comments(candidate)
        match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", candidate)
        return match.group(1) if match else candidate

    def _escape_string_newlines(value: str) -> str:
        result = []
        in_string = False
        i = 0
        while i < len(value):
            char = value[i]
            if not in_string:
                if char == '"':
                    in_string = True
                result.append(char)
            else:
                if char == "\\":
                    result.append(char)
                    i += 1
                    if i < len(value):
                        result.append(value[i])
                elif char == '"':
                    in_string = False
                    result.append(char)
                elif char == "\n":
                    result.append("\\n")
                elif char == "\r":
                    result.append("\\r")
                elif char == "\t":
                    result.append("\\t")
                else:
                    result.append(char)
            i += 1
        return "".join(result)

    def _repair_jsonish(candidate: str) -> str:
        repaired = _escape_string_newlines(candidate.strip())
        repaired = repaired.replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")
        repaired = _strip_comments(repaired)
        repaired = re.sub(r",\s*([}\]])", r"\1", repaired)
        repaired = re.sub(
            r'([{\[,]\s*)(#?[A-Za-z_][A-Za-z0-9_#]*)(\s*:)',
            lambda match: f'{match.group(1)}"{match.group(2)}"{match.group(3)}',
            repaired,
        )
        repaired = re.sub(
            r'(?m)^(\s*)(#?[A-Za-z_][A-Za-z0-9_#]*)(\s*:)',
            lambda match: f'{match.group(1)}"{match.group(2)}"{match.group(3)}',
            repaired,
        )
        repaired = re.sub(r"}\s*\n\s*{", "},\n{", repaired)
        if repaired.startswith("{") and repaired.endswith("}") and re.search(r"}\s*,\s*{", repaired):
            repaired = f"[{repaired}]"
        return repaired

    block = _extract_block(text)
    try:
        return json.loads(block)
    except json.JSONDecodeError:
        repaired = _repair_jsonish(block)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            pythonish = re.sub(r"\btrue\b", "True", repaired, flags=re.IGNORECASE)
            pythonish = re.sub(r"\bfalse\b", "False", pythonish, flags=re.IGNORECASE)
            pythonish = re.sub(r"\bnull\b", "None", pythonish, flags=re.IGNORECASE)
            return ast.literal_eval(pythonish)


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
        **_completion_limit_arg("gpt-4o", 600),
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


def _carousel_display_name(row: dict) -> str:
    speaker_name = row.get("Speaker Name", "").strip()
    username = row.get("Source Username", "").strip()
    media_type = (row.get("Media Type", "") or "").strip().lower()
    if speaker_name:
        return speaker_name
    if media_type != "article" and username:
        return f"@{username.lstrip('@')}"
    return username


def _carousel_slide_prompt_instructions(include_row_numbers: bool) -> str:
    header = (
        "Return ONLY valid JSON as an array.\n\n"
        "Each object must include:\n"
        "* row_number\n"
        "* name\n"
        "* text1\n"
        "* text2\n"
        "* text3\n\n"
        if include_row_numbers else
        "Return ONLY valid JSON as an object.\n\n"
        "The object must include:\n"
        "* name\n"
        "* text1\n"
        "* text2\n"
        "* text3\n\n"
    )
    return (
        header +
        "Rules:\n"
        + ("* Keep row_number exactly the same numeric value shown in the row block\n" if include_row_numbers else "")
        + "* No markdown\n"
        "* No commentary outside JSON\n"
        "* Use plain straight double quotes for all JSON keys and string values — no smart quotes, no escaped quotes inside key names\n"
        "* name = short lowercase account username (no @ symbol)\n"
        "* text1 = strongest opening carousel slide under 250 chars\n"
        "* text2 and text3 = under 900 chars each\n"
        "* No em dashes\n"
        "* No speculation\n"
        "* Avoid repetitive phrasing across fields\n"
        "Style priority:\n"
        "* Write like a viral political news account creating Instagram carousel slides\n"
        "* Sound natural, conversational, and punchy\n"
        "* Prioritize emotional framing, political stakes, accusations, numbers, and consequences\n"
        "* Use direct quotes naturally when they strengthen the writing\n"
        "* Avoid robotic transition phrases\n"
        '* Never say "the speaker," "the clip," "the transcript," "the video," "the comments," "the argument," "the warning," or "the line said"\n'
        "* Do not over explain the source material\n"
        "* Make #text1, #text2, and #text3 feel like three carousel slides\n"
        "* Put the most important accusation, statistic, conflict, or consequence into #text1\n"
        "* #text1 should feel like the strongest opening carousel slide, not just a short hook\n"
        "* Front load critical information into #text1 whenever possible\n"
        "* Use #text2 to expand the core conflict with context, quotes, or stakes\n"
        "* Use #text3 to focus on consequences, reactions, fallout, or additional details\n"
        "* Make each text field feel like a standalone Instagram carousel slide\n"
        "* Prioritize specificity over vagueness\n"
        "* Include numbers, names, and direct quotes whenever they strengthen the writing\n"
        "* Use emotionally charged but factual framing\n"
        "* Avoid filler phrases and weak transitions\n"
        "* Do not artificially shorten strong explanations just to save space\n"
        "* Avoid generic summaries\n\n"
        "Quote guidance:\n"
        "* Use the person's name when provided\n"
        "* If no name is provided, write around the facts naturally\n"
        "* Prefer short direct quotes when they are strong\n"
        "* Do not force quotes into awkward sentences\n"
        '* Never write "the quote said" or "the line said"\n'
    )


def generate_carousel_copy_with_model(row: dict, model: str = "gpt-4o") -> dict[str, str]:
    """Generate Figma/Google Sync carousel fields."""
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured.")

    transcript, original_caption, caption_context = _row_source_text(row)
    display_name = _carousel_display_name(row)

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
        _carousel_slide_prompt_instructions(include_row_numbers=False)
        + f"\n* Use this label for name when possible: {display_name or 'unknown'}"
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You write concise viral political carousel copy and return valid JSON only."},
            {"role": "user", "content": prompt + "\n\n" + "\n\n".join(user_parts)},
        ],
        **_completion_limit_arg(model, 500),
        temperature=0.45,
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content.strip()
    payload = _parse_jsonish_payload(raw)

    return {
        "name": (payload.get("name") or display_name or "").strip(),
        "text1": (payload.get("text1") or "").strip()[:350],
        "text2": (payload.get("text2") or "").strip()[:900],
        "text3": (payload.get("text3") or "").strip()[:900],
    }


def generate_batch_carousel_copy_with_model(rows: list[dict], model: str = "gpt-5.2") -> dict[int, dict[str, str]]:
    """Generate carousel fields for multiple rows in one API call."""
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured.")
    if not rows:
        return {}

    blocks: list[str] = []
    display_names: dict[int, str] = {}
    for row in rows:
        row_number = int(row.get("row_number") or 0)
        if row_number <= 0:
            continue
        transcript, original_caption, caption_context = _row_source_text(row)
        display_name = _carousel_display_name(row)
        display_names[row_number] = display_name
        blocks.append(
            "\n".join(
                [
                    f"ROW {row_number}",
                    f"display_name: {display_name or 'unknown'}",
                    f"username: {(row.get('Source Username') or '').strip() or 'unknown'}",
                    f"media_type: {(row.get('Media Type') or '').strip().lower() or 'post'}",
                    f"speaker_name: {(row.get('Speaker Name') or '').strip() or '(none)'}",
                    f"generated_caption:\n{(row.get('Generated Caption') or '').strip() or '(none)'}",
                    f"transcript:\n{transcript or '(none)'}",
                    f"original_caption:\n{original_caption or '(none)'}",
                    f"caption_context:\n{caption_context or '(none)'}",
                ]
            )
        )

    if not blocks:
        return {}

    prompt = _carousel_slide_prompt_instructions(include_row_numbers=True)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "You write concise viral political carousel copy and return valid JSON only.",
            },
            {"role": "user", "content": prompt + "\n\n" + "\n\n---\n\n".join(blocks)},
        ],
        **_completion_limit_arg(model, max(900, min(4000, 450 * len(blocks)))),
        temperature=0.45,
    )
    raw = response.choices[0].message.content.strip()
    payload = _parse_jsonish_payload(raw)
    items = payload if isinstance(payload, list) else [payload]

    results: dict[int, dict[str, str]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            row_number = int(item.get("row_number") or 0)
        except Exception:
            continue
        if row_number <= 0:
            continue
        results[row_number] = {
            "name": (item.get("name") or display_names.get(row_number) or "").strip(),
            "text1": (item.get("text1") or "").strip()[:350],
            "text2": (item.get("text2") or "").strip()[:900],
            "text3": (item.get("text3") or "").strip()[:900],
        }
    return results
