"""Shared caption-generation helpers for the sheet workflow."""

import ast
import json
import re

import openai

from config import DEFAULT_POST_FOOTER, OPENAI_API_KEY

PINNED_TOP_COMMENT_PREFIX = "[[TOP]] "

SLIDE_BODY_FONT_MIN_REM = 1.4
SLIDE_BODY_FONT_CQW = 5.5
SLIDE_BODY_FONT_MAX_REM = 2.7

SYS_PROMPT = (
    "You are a sharp political analyst. Write a new short, clear social post "
    "under 1300 characters using exactly two simple paragraphs based on the source material provided. "
    "Do not reproduce or rewrite the original caption — use it only as reference for facts and context.\n\n"
    "Never write the caption in first person. Do not use I, me, my, mine, we, us, our, or ours "
    "unless they appear inside a short direct quote from the source. The narration of the caption "
    "must stay in third person and describe the person or event from the outside. "
    "Even if the source material is written in first person, your generated caption must be fully in third person — "
    "never echo or adopt the speaker's voice as your own. "
    "If the speaker cannot be clearly identified, skip naming them and describe the content or information directly.\n\n"
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


_client: openai.OpenAI | None = None


def _get_client() -> openai.OpenAI:
    global _client
    if _client is None:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        _client = openai.OpenAI(api_key=OPENAI_API_KEY, timeout=45.0, max_retries=1)
    return _client


def _single_paragraph_slide_text(value: str, limit: int) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())[:limit].strip()


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


def _row_media_type(row: dict) -> str:
    return (row.get("Media Type") or "").strip().lower()


def _row_url(row: dict) -> str:
    return (row.get("Instagram URL") or "").strip().lower()


def row_requires_transcript(row: dict) -> bool:
    media_type = _row_media_type(row)
    if media_type == "reel":
        return True
    url = _row_url(row)
    return "/reel/" in url or "/reels/" in url


def row_ready_for_caption(row: dict) -> bool:
    transcript = (row.get("Transcript") or "").strip()
    original_caption = (row.get("Original Caption") or "").strip()
    caption_context = (row.get("Caption Context") or "").strip()
    if row_requires_transcript(row):
        return bool(transcript)
    return bool(transcript or original_caption or caption_context)


def _row_source_text(row: dict) -> tuple[str, str, str]:
    transcript = row.get("Transcript", "").strip()
    original_caption = row.get("Original Caption", "").strip()
    caption_context = row.get("Caption Context", "").strip()
    if row_requires_transcript(row) and not transcript:
        raise ValueError("Reels require a transcript before caption generation.")
    if not row_ready_for_caption(row):
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
        user_parts.append(
            "ORIGINAL INSTAGRAM CAPTION (for reference and context only — do not reproduce or rewrite this):\n"
            f"{original_caption}"
        )
    if caption_context:
        user_parts.append(
            "ADDITIONAL CONTEXT FROM EDITOR:\n"
            f"{caption_context}\n"
            "Use this to fill in missing context, but do not present uncertain claims as facts."
        )
    if not user_parts:
        user_parts.append(f"SOURCE TEXT:\n{content}")

    speaker_name = row.get("Speaker Name", "").strip()
    username = row.get("Source Username", "").strip().lstrip("@")
    if speaker_name:
        user_parts.append(
            f"The person featured here is: {speaker_name}. "
            "Mention their name once, then refer to them with he, she, or they. "
            "If gender is unclear, use they. Do not repeat their name multiple times."
        )
    elif username and username.lower() not in ("unknown", ""):
        user_parts.append(
            f"This content is from the Instagram account @{username}. "
            "If you can identify the speaker or subject from the source text, refer to them by name. "
            "If you cannot, do not guess — just describe the content or information directly and factually."
        )

    response = _get_client().chat.completions.create(
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


def carousel_slide_rules() -> str:
    """Canonical slide generation rules shared by all carousel prompt builders."""
    return (
        "* name = short lowercase account username, no @ symbol\n"
        "* text1 = strongest opening slide under 150 chars\n"
        "* text2 = target 450 to 650 chars. Use the full space when the source supports it. Only go shorter if the source is genuinely thin\n"
        "* text3 = target 450 to 650 chars. Use the full space when the source supports it. Only go shorter if the source is genuinely thin\n"
        "* If the source material is too thin, write the strongest accurate version without padding\n"
        "* No em dashes, emojis, hashtags, paragraph breaks, or newline characters inside text fields\n"
        "* Every text field must be a single continuous paragraph with no line breaks or paragraph spacing\n"
        "* Do not use escaped newline characters like \\n, \\r, or unicode line separators\n"
        "* Collapse all whitespace into normal single spaces before returning JSON\n"
        "* No speculation or invented framing\n"
        "* Never include hashtags in slide text\n"
        "* Never use phrases like 'the speaker,' 'the clip,' 'the transcript,' 'the video,' or 'the creator said'\n"
        "* Never repeat the same fact, quote, setup, accusation, or disclaimer across text1, text2, and text3\n\n"

        "Slide structure:\n"
        "* quote = the single best verbatim pull-quote from the source. Under 120 characters. No quotation marks, no attribution. This is the large-format display quote shown on slide 1. If no strong verbatim quote exists, write the most charged paraphrase in the speaker's voice.\n"
        "* text1 = slide 1 body text. This is the supporting context that appears alongside the quote. Do NOT repeat the quote in text1. Write it as the hook/framing that gives the quote meaning — the setup, the stakes, or the consequence. Under 150 chars, single paragraph.\n"
        "* text2 = quote heavy. Use the strongest exchanges, pushback, direct lines, new facts, verified context, names, dates, numbers, contradictions, or legal details only\n"
        "* text3 = broader context, stakes, political backdrop, public reaction, fallout, unanswered questions, public consequences, policy stakes, legal implications, or next steps\n"
        "* Assume the viewer already read previous slides. Do not restate information\n"
        "* Each field should feel like a complete standalone carousel slide\n"
        "* Prioritize numbers, names, dates, direct quotes, charges, rulings, dollar amounts, and locations over generic summaries\n"
        "* Use emotionally charged but factual framing only\n"
        "* Avoid obvious framing like 'this matters because,' 'why this matters,' or 'the reason this is important'\n"
        "* Every slide must add a new concrete detail, quote, context point, or consequence\n\n"

        "Style priority:\n"
        "* Write like a viral political news account creating Instagram carousel slides\n"
        "* Sound natural, conversational, punchy, emotionally charged, and factual\n"
        "* Prioritize specificity: names, numbers, accusations, stakes, consequences, and strong quotes\n"
        "* Avoid robotic transitions, filler phrases, generic summaries, and over explaining\n"
        "* Expand beyond the transcript and caption only when reliable context materially improves the carousel\n\n"

        "Quote guidance:\n"
        "* Pull direct quotes from the transcript first before writing anything in your own words\n"
        "* Each slide must contain at least one direct quote from the transcript if one is available\n"
        "* Short punchy quotes are preferred over paraphrasing\n"
        "* Do not invent, paraphrase as a quote, or attribute anything not said verbatim in the transcript\n"
        "* If no strong quote exists for a slide, write around verified facts without fabricating one\n"
        "* Use the person's name when provided\n"
        "* Prioritize text1 and text2 for direct quotes\n"
        "* text3 may use fewer quotes when context explains the stakes more naturally\n"
        "* If the transcript appears to be from a notable public figure — politician, executive, celebrity, "
        "activist, expert, or anyone whose direct words carry weight — maximize direct quotes across all "
        "slides. Pack each slide with as much verbatim content from the transcript as the character limit "
        "allows. Use their exact words rather than paraphrases whenever possible.\n"
    )


def _carousel_slide_prompt_instructions(include_row_numbers: bool) -> str:
    header = (
        "Return ONLY valid JSON as an array.\n\n"
        "Each object must include:\n"
        "* row_number\n"
        "* name\n"
        "* quote\n"
        "* text1\n"
        "* text2\n"
        "* text3\n\n"
        if include_row_numbers else
        "Return ONLY valid JSON as an object.\n\n"
        "The object must include:\n"
        "* name\n"
        "* quote\n"
        "* text1\n"
        "* text2\n"
        "* text3\n\n"
    )
    return (
        header
        + "Rules:\n"
        + ("* Keep row_number exactly the same numeric value shown in the row block\n" if include_row_numbers else "")
        + "* Return only valid JSON. No markdown or commentary outside JSON\n"
        + "* Use plain straight double quotes only. No smart quotes\n"
        + carousel_slide_rules()
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

    response = _get_client().chat.completions.create(
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
        "quote": (payload.get("quote") or "").strip().strip('"').strip("'").strip().rstrip("."),
        "text1": _single_paragraph_slide_text(payload.get("text1") or "", 350),
        "text2": _single_paragraph_slide_text(payload.get("text2") or "", 900),
        "text3": _single_paragraph_slide_text(payload.get("text3") or "", 900),
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

    response = _get_client().chat.completions.create(
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
            "quote": (item.get("quote") or "").strip().strip('"').strip("'").strip().rstrip("."),
            "text1": _single_paragraph_slide_text(item.get("text1") or "", 350),
            "text2": _single_paragraph_slide_text(item.get("text2") or "", 900),
            "text3": _single_paragraph_slide_text(item.get("text3") or "", 900),
        }
    return results
