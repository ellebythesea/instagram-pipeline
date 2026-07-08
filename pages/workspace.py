"""Unified workspace shell for the next UI redesign."""
from datetime import datetime, time as dt_time, timedelta
import ast
import hashlib
import json
import html
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from urllib.parse import parse_qs, quote, urlparse
import requests
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openai
import streamlit as st
from config import (
    APIFY_API_TOKEN,
    APIFY_POST_ACTOR_ID,
    DEFAULT_POST_FOOTER,
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    GOOGLE_SHEET_ID,
    OPENAI_API_KEY,
    SECRET_MANAGER_PROJECT_ID,
    SECRET_MANAGER_SECRET_NAMES,
    SERPER_API_KEY,
    _secret_manager_client,
)
from caption import transcribe_video
from drive import (
    _get_service,
    copy_drive_file_to_folder,
    download_drive_file,
    extract_drive_file_id,
    get_drive_file_metadata,
    get_or_create_subfolder,
    upload_to_drive,
)
from ingest_helpers import _compact_post_date, build_filename_prefix, upload_media_bundle
import pipeline_caption as pipeline_caption_ops
from post_scraper import process_url as process_post_url
from reel_scraper import process_url as process_reel_url
import sheets as sheet_ops
from utils.auth import require_auth
from utils.error_labels import describe_error
from utils.styles import inject as inject_styles

generate_row_caption = pipeline_caption_ops.generate_row_caption
row_ready_for_caption = pipeline_caption_ops.row_ready_for_caption
_strip_top_comment_paragraphs = pipeline_caption_ops._strip_top_comment_paragraphs
generate_carousel_copy_with_model = getattr(
    pipeline_caption_ops,
    "generate_carousel_copy_with_model",
    lambda row, model="gpt-4o": pipeline_caption_ops.generate_carousel_copy(row),
)
generate_carousel_copy = getattr(
    pipeline_caption_ops,
    "generate_carousel_copy",
    lambda _row: {"name": "", "text1": "", "text2": "", "text3": ""},
)
generate_batch_carousel_copy_with_model = getattr(
    pipeline_caption_ops,
    "generate_batch_carousel_copy_with_model",
    lambda rows, model="gpt-5.2": {},
)

MODE_OPTIONS = [
    "Create a Post",
    "Crop Video",
    "Generate headline",
]

# ---------------------------------------------------------------------------
# Video crop helpers
# ---------------------------------------------------------------------------

def _crop_ffmpeg_path() -> str:
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return "ffmpeg"


def _crop_ffprobe_path() -> str:
    try:
        import imageio_ffmpeg
        exe = imageio_ffmpeg.get_ffmpeg_exe()
        probe = exe.replace("ffmpeg", "ffprobe")
        if os.path.exists(probe):
            return probe
    except Exception:
        pass
    return "ffprobe"


def _crop_video_to_bytes(src_path: str, ratio_w: int, ratio_h: int) -> bytes:
    result = subprocess.run(
        [
            _crop_ffprobe_path(), "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            src_path,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"Could not read video dimensions: {result.stderr}")
    parts = result.stdout.strip().split(",")
    w, h = int(parts[0]), int(parts[1])

    target = ratio_w / ratio_h
    if w / h > target:
        new_w = int(h * ratio_w / ratio_h)
        new_h = h
    else:
        new_w = w
        new_h = int(w * ratio_h / ratio_w)
    new_w -= new_w % 2
    new_h -= new_h % 2
    x = (w - new_w) // 2
    y = (h - new_h) // 2

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as out_f:
        out_path = out_f.name
    try:
        cmd = [
            _crop_ffmpeg_path(), "-y", "-i", src_path,
            "-vf", f"crop={new_w}:{new_h}:{x}:{y}",
            "-c:v", "libx264", "-crf", "18", "-preset", "fast",
            "-c:a", "copy",
            out_path,
        ]
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode(errors="replace"))
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        try:
            os.unlink(out_path)
        except Exception:
            pass

def _fit_video_to_bytes(src_path: str) -> bytes:
    """Scale video to fit within 4:5, padding sides/top with black bars."""
    result = subprocess.run(
        [
            _crop_ffprobe_path(), "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            src_path,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"Could not read video dimensions: {result.stderr}")
    parts = result.stdout.strip().split(",")
    w, h = int(parts[0]), int(parts[1])

    if w / h > 4 / 5:
        out_w = w - (w % 2)
        out_h = int(out_w * 5 / 4)
        out_h -= out_h % 2
    else:
        out_h = h - (h % 2)
        out_w = int(out_h * 4 / 5)
        out_w -= out_w % 2

    vf = (
        f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,"
        f"pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black"
    )
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as out_f:
        out_path = out_f.name
    try:
        proc = subprocess.run(
            [
                _crop_ffmpeg_path(), "-y", "-i", src_path,
                "-vf", vf,
                "-c:v", "libx264", "-crf", "18", "-preset", "fast",
                "-c:a", "copy",
                out_path,
            ],
            capture_output=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode(errors="replace"))
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        try:
            os.unlink(out_path)
        except Exception:
            pass


ORG_HASHTAG_OPTIONS = [
    "",
    "Good Influence",
    "American Experiment Project",
]

ORG_HASHTAG_MAP = {
    "Good Influence": "#usapolitics",
    "American Experiment Project": "#usa",
}

EDITABLE_STATUSES = {"ingested", "done", "slides"}
TRANSCRIPT_SIZE_WARNING_BYTES = 100 * 1024 * 1024
EDITOR_INITIAL_RENDER_LIMIT = 12
WORKSPACE_SLIDES_BATCH_SIZE = 4
INSTAGRAM_CANVAS_WIDTH_PX = 1080
INSTAGRAM_CANVAS_HEIGHT_PX = 1485
_SUBSTACK_PROMOTE_META_PREFIX = "SUBSTACK_PROMOTE_META:"
PREVIEW_EXPORT_WIDTH_PX = 1080
PREVIEW_EXPORT_HEIGHT_PX = 1350
PREVIEW_EXPORT_SCALE = PREVIEW_EXPORT_HEIGHT_PX / INSTAGRAM_CANVAS_HEIGHT_PX
PREVIEW_EXPORT_FONT_SCALE = 0.92
PREVIEW_CANVAS_WIDTH_PX = 420
PREVIEW_CANVAS_HEIGHT_PX = round(
    PREVIEW_CANVAS_WIDTH_PX * INSTAGRAM_CANVAS_HEIGHT_PX / INSTAGRAM_CANVAS_WIDTH_PX
)
PREVIEW_SLIDE_FONT_FAMILY = "'Poppins', sans-serif"
PREVIEW_SLIDE_FONT_WEIGHT = 500
PREVIEW_SLIDE_LETTER_SPACING = "0.01em"
PREVIEW_SLIDE_LINE_HEIGHT = "1.26"
SLIDE_BODY_FONT_MIN_REM = pipeline_caption_ops.SLIDE_BODY_FONT_MIN_REM
SLIDE_BODY_FONT_CQW = pipeline_caption_ops.SLIDE_BODY_FONT_CQW
SLIDE_BODY_FONT_MAX_REM = pipeline_caption_ops.SLIDE_BODY_FONT_MAX_REM
PREVIEW_UPLOAD_SUBFOLDER = "previews"
PINNED_TOP_COMMENT_PREFIX = "[[TOP]] "

_client: openai.OpenAI | None = None
_pd_module = None
VOTER_GUIDE_PROMPT_TEMPLATE = textwrap.dedent(
    """\
    You are generating a "living voter guide" article for a Substack series called Vote In Or Out. Each article covers one race or, when appropriate, a small set of clearly related races. Your job is to research the elections using web search and produce a complete article ready to publish.

    INPUT:
    - Candidates: [CANDIDATE_LIST]
    - Resolved races:
    [RACE_SCOPE]
    - Donation URL: [DONATION_LINK]

    STEP 1: RESOLVE THE ELECTIONS

    Before writing anything, use web search to figure out:
    1. What office these candidates are currently running for, in what jurisdiction, in what cycle.
    2. Which candidates belong in the same active race, and whether the pasted list spans more than one election.
    3. What are the exact election dates for each race you cover?
    4. What is today's date (for the "last updated" stamp)?

    If you cannot confidently resolve at least one clear race from the candidate list, stop and report back: "I could not resolve a clear set of races for [candidate list]. Please verify the names or specify the exact contests."

    Once resolved, internally fill in:
    - Candidate list: [CANDIDATE_LIST]
    - Race scope: [RACE_SCOPE]

    STEP 2: WRITE THE ARTICLE

    Research the candidates and races using web search. Pull from a mix of mainstream news, local journalism, neutral reference sources (Ballotpedia, Wikipedia), and prediction markets if available. Cite specific sources for every factual claim, especially numbers, quotes, and polling data.

    If the candidates fall into one race, compare them directly. If they fall into multiple races, organize the article by race and discuss all of the races you resolved from the candidate list. Be smart about grouping: do not force unrelated candidates into one comparison table, and do not ignore a clearly separate election if it is part of the pasted list.

    Identify the three to five issues that most define the difference between the candidates or races you are covering. Do not pad with generic policy categories. If a race is really about one or two big fault lines (loyalty to a party leader, a foreign policy split, a generational divide), let that show in which issues you choose. If there is a real controversy, scandal, viral moment, or unusually prominent talking point shaping a race, include it with clear sourcing and explain why it matters without sensationalizing it.

    Write the article in the voice of The Atlantic: confident, accessible, narrative-driven, not breathless or partisan. Lead with what makes this race interesting beyond the names on the ballot. Treat the reader as a smart adult who has not been following closely. The article should feel informative first, not persuasive, and should help the reader understand the candidates, the stakes, and the most-discussed flashpoints in the race.

    If a donation URL is provided, do not let it shape your analysis, tone, issue selection, or candidate framing.

    If a usable donation URL is present, add a strong donation call to action near the top of the article. Make the case for why donations materially help the relevant candidate or effort compete and succeed, and include the exact donation URL.

    Do not use em dashes anywhere in the article.

    Use this exact structure and section order:

    - TITLE in this format:
      * if one race: "[candidate last names joined with 'vs.'] | [Race Name] | [Election Date]"
      * if multiple races: "[shared jurisdiction or topic] voter guide | [Election year]"
    - Date stamp: "Last updated: [today's date]"
    - Opening hook: 2-3 short paragraphs framing why this race or set of races matters beyond the local context. End with a sentence pointing at the relevant election date or dates.
    - "Who Are These Candidates?" section: one paragraph per candidate covering background, experience, and the brand they have built. Include education, career, prior runs for office, and the core argument each candidate is making about themselves.
    - "The [Three / Four / Five] Issues That Define This Race" section:
      * if one race: pick the right number based on the race
      * if multiple races: either organize issues across the elections or use race-by-race subsections, whichever is clearer
      * for each issue or race subsection, explain where the candidates stand, with direct quotes where possible
    - "The Money: Who's Funding What" section: campaign finance breakdown, major super PAC spending, notable donors, and any unusual funding dynamics (foreign lobbying, dark money, deepfake ads, etc.). If multiple races are covered, separate the analysis clearly by race.
    - "Where the Race Stands Right Now" section: most recent polling, prediction market odds, and a sober assessment of momentum. Note margins of error and undecided voter percentages. If multiple races are covered, give a concise update for each.
    - "What You Can Do Right Now" section: practical voter actions split into three subgroups:
      * "If you live in [the relevant district/state]" with polling place lookup URL, registration verification URL, ID requirements, and registration deadline status.
      * "If you're watching from elsewhere" with results tracking URL and context on the general election.
      * "If you want to go deeper" with campaign websites and recommended local journalism.
    - "What People Are Getting Wrong" section: 3-5 common pieces of misinformation or misleading framings, each with a brief correction. Be fair to both sides; do not only debunk one candidate's critics.
    - "Read More" section: 5-8 sources, each with the publication name in bold, article title in quotes, a one-sentence description of what the piece offers, and the full URL written out on its own line.
    - Closing line in italics: "Something missing? Something wrong? Drop it in the comments. This article is updated daily based on what readers add."
    - AI disclaimer in italics, placed as the final element of the article: "This guide was researched and written with the assistance of AI, guided and reviewed by a human editor. It can make mistakes. If you spot something wrong, missing, or outdated, leave a comment below and it will be reviewed and updated. This is a living document. [Read about our methodology here.](https://voteinorout.substack.com/p/we-are-building-our-voter-guides)"

    After the article, output a "Tags" block with five Substack tags optimized for search and discovery. Use the names of both candidates, the race name, the election cycle, and one issue-based tag.

    CONSTRAINTS:
    - Cite every factual claim, statistic, quote, and poll number to a specific source from your web search results.
    - Stay scrupulously neutral in tone. Each candidate's arguments should be presented as they would present them. Save critical assessment for the "What People Are Getting Wrong" section, and balance it across the field.
    - When covering controversies or talking points, explain them as reported facts and competing interpretations, not as settled proof unless the sourcing clearly supports that.
    - If polling, funding, or other numbers conflict across sources, note the discrepancy rather than picking one.
    - Do not invent endorsements, quotes, vote counts, or polling data. If you cannot find a fact, omit it.
    - Keep the total article length between 1,800 and 2,500 words.
    - No em dashes.

    STEP 3: REPORT

    At the very top of your output, before the article itself, include a one-line "Resolved race" note so the human editor can verify you picked the right contest:

    Resolved races: [RACE_SCOPE]

    Then output the article, then the Tags block.
    """
)
VOTER_GUIDE_RESOLUTION_QUERIES = [
    ("search", '"{name}" running for office election opponent'),
    ("search", '"{name}" election race opponent'),
    ("search", 'site:ballotpedia.org "{name}" election'),
    ("search", 'site:wikipedia.org "{name}" election'),
    ("news", '"{name}" campaign election opponent'),
]
SUBSTACK_CANDIDATE_ARTICLE_PROMPT_TEMPLATE = textwrap.dedent(
    """\
    Return ONLY valid JSON as an array.
    Each object must include:
    * row_number
    * name
    * text1
    * text2
    * text3
    * generated_caption

    Rules:
    * Keep row_number exactly as 1
    * No markdown
    * No commentary outside JSON
    * Use plain straight double quotes for all JSON keys and string values, no smart quotes, no escaped quotes inside key names
    * name = "voteinorout"
    * text1 = strongest opening carousel slide under 350 chars. Lead with the most emotionally compelling verified quote, allegation, consequence, contradiction, or fact. Write it like a viral news headline — prioritize emotion, conflict, consequences, and curiosity over explanation. text1 must make the viewer urgently want to read slide 2.
    * text2 and text3 = under 900 chars each
    * generated_caption = Instagram caption body under 900 chars before the standard footer/hashtags are appended in-app
    * No em dashes
    * No paragraph breaks in text1, text2, or text3. Keep each slide text field to one paragraph with no newline characters or escaped newline sequences like \\n or \\n\\n
    * No speculation
    * Avoid repetitive phrasing across fields
    * Never include hashtags in slide text
    * Do not include hashtags in generated_caption
    * Do not write the standard footer in generated_caption because the app appends it separately

    Style priority:
    * Write like a viral political news account creating Instagram carousel slides
    * Sound natural, conversational, and punchy
    * Prioritize emotional framing, political stakes, accusations, numbers, and consequences
    * Use direct quotes from the article naturally when they strengthen the writing
    * Avoid robotic transition phrases
    * Do not over-explain the article
    * Front-load critical information into text1 whenever possible
    * Prioritize specificity over vagueness
    * Include numbers, names, and direct quotes whenever they strengthen the writing
    * Use emotionally charged but factual framing
    * Avoid filler phrases and weak transitions
    * Avoid generic summaries

    Slide-by-slide guidance:
    * These slides are promoting a full written article, not replacing it. They should feel like a sharp Instagram teaser for a deeper Substack piece.
    * text1 = stop-scrolling opener built from the most dramatic angle in the article. Make it explicit that this post is based on a breakdown article we created about the race. Pull the most surprising number, most loaded conflict, or highest-stakes framing from the piece. Identify the main candidates by name when possible.
    * text2 = "here's what the full article gets into." Summarize the central conflict, money, stakes, and defining contrast from the written piece. Make it clear this is drawn from a larger article.
    * text3 = election date and latest polling. Include the exact election date, polling numbers with source if mentioned in the article, and any prediction market odds. End with: Comment LINK and I'll DM you the full article.
    * generated_caption = a concise, informative Instagram caption summarizing the article's key findings. It should mention that we created a breakdown article for this election, note that the article will be updated as comments come in, and briefly mention any major controversy or talking point if it is central to the race. Keep it neutral and informative rather than persuasive.

    Article to base the carousel on:
    [ARTICLE]

    Article URL if available (for reference only, do not include in slide text unless explicitly relevant):
    [SUBSTACK_URL]

    Output format example:
    [
      {
        "row_number": 1,
        "name": "voteinorout",
        "text1": "[clickbait opener under 350 chars]",
        "text2": "[summary of article key points under 900 chars]",
        "text3": "[dates and polls under 900 chars]. Comment LINK and I'll DM you the full article.",
        "generated_caption": "[informative caption body under 900 chars]"
      }
    ]
    """
)

ELECTION_POST_PROMPT_TEMPLATE = textwrap.dedent(
    """\
    Return ONLY valid JSON — a single object with no markdown and no commentary outside the JSON.

    Use web search to research this race before writing. Pull from Ballotpedia, local news, major outlets, and campaign sites. Cite facts and figures only if you can verify them.

    Race:
    [RACE_INFO]

    Today's date: [TODAY]

    Your output must be a single JSON object with these exact keys:
    name, quote, text1, text2, text3, text4, text5, text6, generated_caption, source_url

    Rules:
    * No markdown
    * No em dashes
    * No paragraph breaks or escaped newlines (no \\n) inside any field — each field is one unbroken paragraph
    * No hashtags anywhere
    * Straight double quotes only — no smart quotes, no escaped quotes inside values
    * No speculation; only verified, sourced facts

    Field instructions:

    name — Short label for the election. Format: "[Jurisdiction] [Office]" (e.g. "Colorado Senate", "NY-21 Congressional", "Georgia Governor"). Under 40 chars. This is the headline/speaker label shown on the post.

    quote — The single sharpest tension or decision voters face in this race. Not a slogan. Make it specific to these two candidates and this moment — what is the real choice being made? Under 120 chars. No attribution. No nested quotes. Should feel non-templated.

    text1 — Introduces the race and reinforces the tension from the quote. Name both candidates. Set the stakes. Under 150 chars.

    text2, text3, text4, text5 — Each slide covers one major issue voters are weighing in this race. These should be the meatiest slides. For each:
    * Start with the issue name (e.g. "Abortion:", "Immigration:", "Economy:")
    * Go deep — don't just say candidates disagree; show HOW they disagree with specifics
    * Include at least one hard number, dollar figure, vote record, polling stat, or direct quote — search for real figures
    * Use sources: legislation voted on, fundraising data, polling margins, campaign finance numbers, verified quotes from debates or ads
    * The goal is that a voter who reads this slide knows something concrete, not just a vibe
    * Up to 800 chars each. One paragraph, no newlines.

    text6 — Election logistics: the exact election date, registration or mail-in ballot deadline if known, and where voters can look up polling place or registration status. Under 400 chars.

    generated_caption — Concise Instagram caption naming the candidates and race, mentioning this is a breakdown carousel, and noting one or two key contrasts. Under 900 chars. No standard footer (appended separately by the app). No hashtags.

    source_url — The single best URL for a voter wanting to understand this race. Prefer Ballotpedia, a local news race overview, or a major outlet's dedicated race page. Raw URL only.

    Output example (keys only — fill in real content):
    {
      "name": "...",
      "quote": "...",
      "text1": "...",
      "text2": "...",
      "text3": "...",
      "text4": "...",
      "text5": "...",
      "text6": "...",
      "generated_caption": "...",
      "source_url": "..."
    }
    """
)


def _get_client() -> openai.OpenAI:
    global _client
    if _client is None:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        _client = openai.OpenAI(api_key=OPENAI_API_KEY, timeout=45.0, max_retries=1)
    return _client


def _get_pandas():
    global _pd_module
    if _pd_module is None:
        import pandas as pd
        _pd_module = pd
    return _pd_module


def _get_apify_client_class():
    from apify_client import ApifyClient
    return ApifyClient


def _fetch_article_source_data(url: str) -> dict:
    from article_source import fetch_article_source
    return fetch_article_source(url)


def _today_eastern_label() -> str:
    now = datetime.now(ZoneInfo("America/New_York"))
    return f"{now.strftime('%B')} {now.day}, {now.year}"


def _now_eastern() -> datetime:
    return datetime.now(ZoneInfo("America/New_York"))


def _format_eastern_timestamp(value: datetime) -> str:
    eastern = value.astimezone(ZoneInfo("America/New_York"))
    return eastern.strftime("%Y-%m-%d %I:%M %p ET")


def _parse_candidate_comment_timestamp(value) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 1_000_000_000_000:
            numeric /= 1000.0
        try:
            return datetime.fromtimestamp(numeric, tz=ZoneInfo("UTC"))
        except Exception:
            return None
    pd = _get_pandas()
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _load_open_candidate_comment_rows() -> list[dict]:
    rows: list[dict] = []
    for row in sheet_ops.get_open_comment_monitor_rows(GOOGLE_SHEET_ID):
        last_raw = (row.get("last_checked") or "").strip()
        rows.append(
            {
                "row_number": row["row_number"],
                "source": row.get("source", "substack"),
                "label": (row.get("label") or "").strip(),
                "url": (row.get("url") or "").strip(),
                "last_checked_raw": last_raw,
                "last_checked_at": _parse_candidate_comment_timestamp(last_raw),
                "substack": (row.get("substack_url") or "").strip(),
                "summary": (row.get("summary") or "").strip(),
            }
        )
    return rows


def _extract_comment_records(items: list[dict], since: datetime | None) -> list[dict]:
    collected: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    def visit_comment(payload: dict) -> None:
        if not isinstance(payload, dict):
            return
        text = ""
        for key in ("text", "comment", "content", "commentText"):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate.strip():
                text = candidate.strip()
                break
        timestamp = None
        for key in ("timestamp", "createdAt", "created_at", "time"):
            timestamp = _parse_candidate_comment_timestamp(payload.get(key))
            if timestamp is not None:
                break
        if since is not None and timestamp is not None and timestamp <= since:
            return
        username = ""
        for key in ("ownerUsername", "username", "userName", "authorUsername"):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate.strip():
                username = candidate.strip().lstrip("@")
                break
        if not username:
            owner = payload.get("owner")
            if isinstance(owner, dict):
                for key in ("username", "userName"):
                    candidate = owner.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        username = candidate.strip().lstrip("@")
                        break
        fingerprint = (
            text.lower(),
            username.lower(),
            timestamp.isoformat() if timestamp is not None else "",
        )
        if text and fingerprint not in seen:
            seen.add(fingerprint)
            collected.append(
                {
                    "text": text,
                    "username": username,
                    "timestamp": timestamp.isoformat() if timestamp is not None else "",
                }
            )

    def walk(payload) -> None:
        if isinstance(payload, dict):
            visit_comment(payload)
            for key in ("comments", "latestComments", "latest_comments", "items"):
                nested = payload.get(key)
                if isinstance(nested, list):
                    for child in nested:
                        walk(child)
        elif isinstance(payload, list):
            for child in payload:
                walk(child)

    walk(items)
    return collected


def _fetch_candidate_comments_since(url: str, since: datetime | None) -> list[dict]:
    if not APIFY_API_TOKEN:
        raise RuntimeError("APIFY_API_TOKEN is not configured.")

    apify_client = _get_apify_client_class()(APIFY_API_TOKEN)
    actor = apify_client.actor(APIFY_POST_ACTOR_ID)
    run = None
    last_error = None
    candidate_inputs = [
        {
            "directUrls": [url],
            "resultsType": "comments",
            "resultsLimit": 50,
        },
        {
            "directUrls": [url],
            "scrapeType": "comments",
            "resultsLimit": 50,
        },
    ]
    for run_input in candidate_inputs:
        try:
            run = actor.call(run_input=run_input, timeout_secs=300)
            break
        except Exception as exc:
            last_error = exc
    if run is None:
        raise last_error or RuntimeError("Apify comments run did not start.")
    if run.get("status") != "SUCCEEDED":
        raise RuntimeError(f"Comments actor failed: {run.get('status') or 'unknown status'}")

    items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
    return _extract_comment_records(items, since)


def _is_link_request_comment(comment: str) -> bool:
    lowered = comment.lower()
    return "link" in lowered and not any(
        phrase in lowered
        for phrase in (
            "missing link",
            "source link",
            "link to source",
            "where is the source",
        )
    )


def _empty_comment_groups() -> dict[str, list[dict]]:
    return {
        "What About": [],
        "Missing": [],
        "Biased": [],
        "Wrong": [],
        "Controversies": [],
    }


def _fallback_issue_comment_examples(comments: list[dict]) -> dict[str, list[dict]]:
    groups = _empty_comment_groups()
    for comment in comments:
        text = (comment.get("text") or "").strip()
        lowered = text.lower()
        if not text or _is_link_request_comment(text):
            continue
        if "what about" in lowered:
            groups["What About"].append(comment)
        if any(marker in lowered for marker in (
            "missing",
            "you missed",
            "you left out",
            "left out",
            "forgot",
            "doesn't mention",
            "doesnt mention",
            "should mention",
            "should include",
            "needs to include",
            "no mention of",
        )):
            groups["Missing"].append(comment)
        if any(marker in lowered for marker in ("biased", "bias", "unfair")):
            groups["Biased"].append(comment)
        if any(marker in lowered for marker in (
            "wrong",
            "incorrect",
            "inaccurate",
            "misleading",
            "not true",
            "false",
            "source?",
            "where's the source",
            "where is the source",
            "proof?",
        )):
            groups["Wrong"].append(comment)
        if any(marker in lowered for marker in (
            "controvers",
            "scandal",
            "lawsuit",
            "corruption",
            "fraud",
            "investigation",
            "indict",
            "ethics",
            "criminal",
            "cover up",
            "cover-up",
            "allegation",
            "accusation",
        )):
            groups["Controversies"].append(comment)
    return {label: entries for label, entries in groups.items() if entries}


def _summarize_candidate_comments(comments: list[dict]) -> dict[str, list[dict]]:
    cleaned_comments = []
    for comment in comments:
        text = (comment.get("text") or "").strip()
        if not text:
            continue
        cleaned_comments.append(
            {
                "text": text,
                "username": (comment.get("username") or "").strip(),
                "timestamp": (comment.get("timestamp") or "").strip(),
            }
        )
    if not cleaned_comments:
        return {}
    candidate_comments = [comment for comment in cleaned_comments if not _is_link_request_comment(comment["text"])]
    if not candidate_comments:
        return {}

    numbered_comments = []
    for index, comment in enumerate(candidate_comments[:80], start=1):
        username = f"@{comment['username']}" if comment.get("username") else "(unknown user)"
        numbered_comments.append(f"{index}. {username}: {comment['text']}")

    prompt = (
        "Review these Instagram comments and classify every qualifying comment into one or more of these headings: "
        "What About, Missing, Biased, Wrong, Controversies. "
        "Phrases like 'what about', 'you missed', and 'why didn't you mention' are strong signals, but use judgment "
        "and do not require exact wording. Ignore requests that are only asking for a link. "
        "Use Controversies for comments asking about scandals, allegations, investigations, corruption, lawsuits, ethics issues, or other controversies around the person. "
        "Do not rewrite, shorten, or paraphrase the comments. Choose the exact comment numbers from the list. "
        "Return all qualifying comments from this list, not just a sample. A comment may appear in more than one heading if needed. "
        "Return JSON only in this format: "
        "{\"groups\": {\"What About\": [1, 4], \"Missing\": [2], \"Biased\": [3], \"Wrong\": [5, 6], \"Controversies\": [7]}}. "
        "Use empty arrays for headings with no matches. Return all five headings every time."
    )
    joined_comments = "\n".join(numbered_comments)
    response = _get_client().chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Comments to review:\n{joined_comments}"},
        ],
        max_completion_tokens=220,
        temperature=0.2,
    )
    summary = (response.choices[0].message.content or "").strip()
    if not summary:
        raise ValueError("OpenAI returned an empty comments summary.")
    try:
        payload = _extract_json_object(summary)
    except Exception:
        return _fallback_issue_comment_examples(candidate_comments)
    raw_groups = payload.get("groups") or {}
    grouped_comments = _empty_comment_groups()
    for label in grouped_comments:
        raw_numbers = raw_groups.get(label) or []
        seen_indexes: set[int] = set()
        for value in raw_numbers:
            try:
                index = int(value)
            except Exception:
                continue
            if index < 1 or index > len(candidate_comments) or index in seen_indexes:
                continue
            seen_indexes.add(index)
            grouped_comments[label].append(candidate_comments[index - 1])
    grouped_comments = {label: entries for label, entries in grouped_comments.items() if entries}
    return grouped_comments or _fallback_issue_comment_examples(candidate_comments)


def _update_candidate_last_checked(source: str, row_number: int, checked_at: datetime) -> None:
    sheet_ops.update_comment_monitor_last_checked(
        GOOGLE_SHEET_ID,
        source,
        row_number,
        checked_at.isoformat(timespec="seconds"),
    )


def _extract_json_object(raw_text: str) -> dict:
    cleaned = (raw_text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    pythonish = re.sub(r"\btrue\b", "True", cleaned, flags=re.IGNORECASE)
    pythonish = re.sub(r"\bfalse\b", "False", pythonish, flags=re.IGNORECASE)
    pythonish = re.sub(r"\bnull\b", "None", pythonish, flags=re.IGNORECASE)
    try:
        result = ast.literal_eval(pythonish)
        if isinstance(result, dict):
            return result
    except Exception:
        pass
    # Last resort: extract the first {...} block and retry
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    return json.loads(cleaned)  # re-raise original error


def _serper_search(query: str, *, num: int = 8, news: bool = False) -> list[dict]:
    if not SERPER_API_KEY:
        raise RuntimeError("SERPER_API_KEY is not configured.")
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    payload = {"q": query, "num": num, "gl": "us", "hl": "en"}
    endpoint = "https://google.serper.dev/news" if news else "https://google.serper.dev/search"
    response = requests.post(
        endpoint,
        json=payload,
        headers=headers,
        timeout=20,
    )
    response.raise_for_status()
    body = response.json()
    items = body.get("news" if news else "organic", []) or []
    normalized: list[dict] = []
    for item in items:
        normalized.append(
            {
                "title": _cell_text(item.get("title")).strip(),
                "url": _cell_text(item.get("link")).strip(),
                "snippet": _cell_text(item.get("snippet")).strip(),
                "source": _cell_text(item.get("source")).strip(),
                "date": _cell_text(item.get("date")).strip(),
                "query": query,
                "search_type": "news" if news else "search",
            }
        )
    return normalized


def _collect_candidate_research(candidate_name: str) -> list[dict]:
    seen_urls: set[str] = set()
    collected: list[dict] = []
    for search_type, query_template in VOTER_GUIDE_RESOLUTION_QUERIES:
        query = query_template.format(name=candidate_name.strip())
        results = _serper_search(query, news=search_type == "news")
        for item in results:
            url = item.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            collected.append(item)
    return collected[:18]


def _resolve_candidate_comparison(candidate_names: list[str]) -> dict:
    cleaned_names = [name.strip() for name in candidate_names if name.strip()]
    if len(cleaned_names) < 2:
        raise ValueError("Enter two candidate names.")

    search_results: list[dict] = []
    for candidate_name in cleaned_names:
        candidate_results = _collect_candidate_research(candidate_name)
        if not candidate_results:
            raise RuntimeError(f"No search results found for {candidate_name}.")
        for item in candidate_results:
            tagged_item = dict(item)
            tagged_item["input_candidate"] = candidate_name
            search_results.append(tagged_item)

    sources_json = json.dumps(search_results, ensure_ascii=True)
    response = _get_client().chat.completions.create(
        model="gpt-5.2",
        messages=[
            {
                "role": "system",
                "content": (
                    "You resolve active election races from search results. "
                    "Use only the supplied search results. "
                    "Prefer Ballotpedia, official campaign sites, major local news, major national news, and Wikipedia. "
                    "If the race is ambiguous, say so instead of guessing. "
                    "Return JSON only."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Resolve the active election races represented by these candidates.\n\n"
                    f"Candidates: {', '.join(cleaned_names)}\n"
                    f"Today's date: {_today_eastern_label()}\n\n"
                    "Return a JSON object with these keys:\n"
                    "- could_not_resolve: boolean\n"
                    "- candidate_names: array of strings\n"
                    "- race_groups: array of objects, each with keys candidate_names, office, jurisdiction, cycle, race_name, election_date, resolution_basis\n"
                    "- office: string\n"
                    "- jurisdiction: string\n"
                    "- cycle: string\n"
                    "- race_name: string\n"
                    "- election_date: string\n"
                    "- resolution_basis: string\n"
                    "- ambiguity_note: string\n"
                    "- active_races: array of strings\n"
                    "- source_urls: array of strings\n\n"
                    "If the candidates span multiple races, group them into the correct races instead of forcing one shared race.\n"
                    "If you can resolve at least one race clearly, set could_not_resolve to false.\n"
                    "Only set could_not_resolve to true if you cannot confidently resolve any clear race from the list.\n\n"
                    "Search results:\n"
                    f"{sources_json}"
                ),
            },
        ],
        max_completion_tokens=1200,
        temperature=0,
    )
    resolved = _extract_json_object(response.choices[0].message.content or "")
    if not isinstance(resolved.get("race_groups"), list):
        resolved["race_groups"] = []
    resolved["today_date"] = _today_eastern_label()
    resolved["search_results"] = search_results
    return resolved


def _extract_candidate_names_from_input(raw_input: str) -> list[str]:
    cleaned_input = _cell_text(raw_input).strip()
    if not cleaned_input:
        return []

    fallback_names = [
        part.strip(" -•\t,;")
        for part in re.split(r"[\n,;]+", cleaned_input)
        if part.strip(" -•\t,;")
    ]
    if len(fallback_names) >= 2:
        fallback_names = list(dict.fromkeys(fallback_names))

    response = _get_client().chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "Extract candidate names from messy pasted election text. "
                    "Return JSON only in the form {\"candidate_names\": [\"Name 1\", \"Name 2\"]}. "
                    "Keep only person names. Preserve order of appearance. "
                    "Do not include party labels, offices, URLs, bullets, or commentary."
                ),
            },
            {
                "role": "user",
                "content": cleaned_input,
            },
        ],
        max_completion_tokens=300,
        temperature=0,
    )
    try:
        payload = _extract_json_object(response.choices[0].message.content or "")
        candidate_names = [
            _cell_text(name).strip()
            for name in (payload.get("candidate_names") or [])
            if _cell_text(name).strip()
        ]
        candidate_names = list(dict.fromkeys(candidate_names))
        if len(candidate_names) >= 2:
            return candidate_names
    except Exception:
        pass

    if len(fallback_names) >= 2:
        return fallback_names
    raise ValueError("Could not identify at least two candidate names from that input.")


def _build_candidate_prompt(candidate_result: dict, donation_link: str = "") -> str:
    cleaned_link = _extract_first_url(donation_link)
    candidate_names = [
        _cell_text(name).strip()
        for name in (candidate_result.get("candidate_names") or [])
        if _cell_text(name).strip()
    ]
    candidate_list = ", ".join(candidate_names)
    race_groups = candidate_result.get("race_groups") or []
    if race_groups:
        race_scope_lines = []
        for group in race_groups:
            group_names = ", ".join(
                _cell_text(name).strip()
                for name in (group.get("candidate_names") or [])
                if _cell_text(name).strip()
            )
            race_name = _cell_text(group.get("race_name")).strip()
            election_date = _cell_text(group.get("election_date")).strip()
            office = _cell_text(group.get("office")).strip()
            label_parts = [part for part in [race_name, office, election_date] if part]
            race_scope_lines.append(f"- {group_names}: {' | '.join(label_parts)}".strip())
        race_scope = "\n".join(race_scope_lines)
    else:
        race_name = _cell_text(candidate_result.get("race_name")).strip()
        election_date = _cell_text(candidate_result.get("election_date")).strip()
        race_scope = f"- {candidate_list}: {' | '.join([part for part in [race_name, election_date] if part])}".strip()

    prompt = (
        VOTER_GUIDE_PROMPT_TEMPLATE
        .replace("[CANDIDATE_LIST]", candidate_list)
        .replace("[RACE_SCOPE]", race_scope)
    )
    if cleaned_link:
        return prompt.replace("[DONATION_LINK]", cleaned_link)
    return prompt.replace("[DONATION_LINK]", "(none provided)")


def _build_substack_candidate_article_prompt(article_body: str, substack_url: str) -> str:
    return (
        SUBSTACK_CANDIDATE_ARTICLE_PROMPT_TEMPLATE
        .replace("[ARTICLE]", article_body.strip())
        .replace("[SUBSTACK_URL]", substack_url.strip())
    )


def _build_candidate_article_footer(substack_url: str) -> str:
    cleaned_url = substack_url.strip()
    if cleaned_url:
        return (
            "This post is pulled from a longer Vote In Or Out article.\n\n"
            f"Comment LINK and I'll DM you the full article: {cleaned_url}\n\n"
            "Full guide updated daily based on your comments."
        )
    return (
        "This post is pulled from a longer Vote In Or Out article.\n\n"
        "Comment LINK and I'll DM you the full article.\n\n"
        "Full guide updated daily based on your comments."
    )


def _build_candidate_article_caption(caption_body: str, required_hashtags: str = "") -> str:
    cleaned_body = _cell_text(caption_body).strip()
    article_note = (
        "We created this article to break down the election, and we'll keep updating it as comments come in."
    )
    link_note = "Comment LINK and I'll DM you the full article."
    if article_note.lower() not in cleaned_body.lower():
        cleaned_body = f"{cleaned_body}\n\n{article_note}" if cleaned_body else article_note
    if link_note.lower() not in cleaned_body.lower():
        cleaned_body = f"{cleaned_body}\n\n{link_note}" if cleaned_body else link_note
    return _build_footered_caption(cleaned_body, "", required_hashtags.strip())


def _save_candidate_article_assets(row: dict, generated_payload: dict) -> str:
    row_num = int(row.get("row_number") or 0)
    if not row_num:
        raise ValueError("Candidate article row is missing a row number.")

    caption_text = _build_candidate_article_caption(
        _cell_text(generated_payload.get("generated_caption")).strip(),
        _cell_text(row.get("Required Hashtags")).strip(),
    )
    update_caption(GOOGLE_SHEET_ID, row_num, caption_text, "done")
    _write_specific_carousel_fields(
        row_num,
        {
            "name": "vote in or out substack",
            "text1": _cell_text(generated_payload.get("text1")).strip(),
            "text2": _cell_text(generated_payload.get("text2")).strip(),
            "text3": _cell_text(generated_payload.get("text3")).strip(),
        },
    )
    if update_speaker_names_batch is not None:
        update_speaker_names_batch(GOOGLE_SHEET_ID, {row_num: "vote in or out substack"})
        speaker_key = _workspace_speaker_key(row)
        st.session_state[speaker_key] = "vote in or out substack"
    _verify_carousel_fields_saved(row_num)
    st.session_state.pop(f"workspace_preview_upload_links_{row_num}", None)
    return caption_text


def _is_candidate_article_row(row: dict) -> bool:
    media_type = _cell_text(row.get("Media Type")).strip().lower()
    slide_name = _cell_text(row.get("name")).strip().lower()
    generated_caption = _cell_text(row.get("Generated Caption")).strip().lower()
    return (
        media_type == "article"
        and (
            slide_name == "vote in or out"
            or "we created this article to break down the election" in generated_caption
        )
    )


_INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\u200e\u200f\u2060\ufeff]")


def _strip_invisible_chars(text: str) -> str:
    """Drop zero-width/invisible Unicode chars (e.g. WORD JOINER) some captions use to block auto-linking."""
    return _INVISIBLE_CHARS_RE.sub("", text or "")


def _extract_first_url(value: str) -> str:
    text = _strip_invisible_chars(_cell_text(value).strip())
    if not text:
        return ""
    match = re.search(r"https?://\S+", text)
    return match.group(0).rstrip(".,);]") if match else text


def _copy_button_html(label: str, value: str, key: str, primary: bool = False) -> str:
    clipboard_text = json.dumps(value or "")
    escaped_key = html.escape(key)
    escaped_label = html.escape(label)
    background = "#111827" if primary else "#ffffff"
    color = "#ffffff" if primary else "#0f172a"
    border = "#111827" if primary else "rgba(15,23,42,0.08)"
    return f"""
    <div id="{escaped_key}" style="margin-top:0.35rem;">
      <button
        onclick='navigator.clipboard.writeText({clipboard_text})'
        aria-label='{escaped_label}'
        style="
          width: 100%;
          min-height: 3rem;
          border: 1px solid {border};
          border-radius: 14px;
          background: {background};
          color: {color};
          font-size: 0.96rem;
          font-weight: 600;
          line-height: 1.2;
          cursor: pointer;
          box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
        "
      >{escaped_label}</button>
    </div>
    """


def _render_candidate_output_card(title: str, value: str, copy_key: str) -> None:
    with st.container():
        st.markdown('<div class="workspace-candidate-output-anchor"></div>', unsafe_allow_html=True)
        st.markdown(f"**{title}**")
        _multiline_copy_preview(f"Copy {title}", value, copy_key)


def _call_openai_candidate_article(article_body: str, substack_url: str) -> dict:
    prompt = _build_substack_candidate_article_prompt(article_body, substack_url)
    response = _get_client().chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": "Generate the carousel JSON."},
        ],
        max_completion_tokens=2000,
        temperature=0.4,
    )
    raw_text = _cell_text(response.choices[0].message.content).strip()
    if not raw_text:
        raise ValueError("OpenAI returned an empty response.")

    parsed = _extract_json_payload(raw_text)
    items = parsed if isinstance(parsed, list) else [parsed]
    if not items or not isinstance(items[0], dict):
        raise ValueError("OpenAI response did not contain the expected JSON array.")

    first = items[0]
    result = {
        "row_number": int(first.get("row_number", 1)),
        "name": _cell_text(first.get("name")).strip(),
        "text1": _cell_text(first.get("text1")).strip(),
        "text2": _cell_text(first.get("text2")).strip(),
        "text3": _cell_text(first.get("text3")).strip(),
        "generated_caption": _cell_text(first.get("generated_caption")).strip(),
        "raw_response": raw_text,
    }
    if not result["text1"] or not result["text2"] or not result["text3"] or not result["generated_caption"]:
        raise ValueError("OpenAI response was missing one or more slide fields.")
    return result


def _build_election_post_prompt(candidate_result: dict) -> str:
    """Build the ChatGPT prompt for a 6-slide election post from a resolved candidate comparison."""
    race_groups = candidate_result.get("race_groups") or []
    candidate_names = [
        _cell_text(name).strip()
        for name in (candidate_result.get("candidate_names") or [])
        if _cell_text(name).strip()
    ]

    race_lines = []
    for group in race_groups:
        group_names = ", ".join(
            _cell_text(n).strip() for n in (group.get("candidate_names") or [])
            if _cell_text(n).strip()
        )
        parts = [
            _cell_text(group.get("race_name")).strip(),
            _cell_text(group.get("office")).strip(),
            _cell_text(group.get("jurisdiction")).strip(),
            _cell_text(group.get("election_date")).strip(),
        ]
        race_lines.append(f"- {group_names}: {' | '.join(p for p in parts if p)}")
    if not race_lines:
        race_name = _cell_text(candidate_result.get("race_name")).strip()
        election_date = _cell_text(candidate_result.get("election_date")).strip()
        race_lines.append(
            f"- {', '.join(candidate_names)}: {' | '.join(p for p in [race_name, election_date] if p)}"
        )
    race_info = "\n".join(race_lines)

    return (
        ELECTION_POST_PROMPT_TEMPLATE
        .replace("[RACE_INFO]", race_info)
        .replace("[TODAY]", _today_eastern_label())
    )


get_all_rows = sheet_ops.get_all_rows
get_pending_rows = sheet_ops.get_pending_rows
update_caption = sheet_ops.update_caption
update_caption_and_metadata = getattr(sheet_ops, "update_caption_and_metadata", None)
update_caption_context = sheet_ops.update_caption_context
update_ingest_result = sheet_ops.update_ingest_result
update_metadata = sheet_ops.update_metadata
update_speaker_names_batch = getattr(sheet_ops, "update_speaker_names_batch", None)
update_scheduled_times = sheet_ops.update_scheduled_times
update_transcript = sheet_ops.update_transcript
update_thumbnail_link = getattr(sheet_ops, "update_thumbnail_link", None)
update_carousel_fields = getattr(sheet_ops, "update_carousel_fields", None)
update_quote = getattr(sheet_ops, "update_quote", None)
delete_sheet_row = sheet_ops.delete_row
get_fundraising_links = getattr(sheet_ops, "get_fundraising_links", lambda _sheet_id: [])
get_slide_cta_options = getattr(sheet_ops, "get_slide_cta_options", lambda _sheet_id: {})
update_slide_cta_option = getattr(sheet_ops, "update_slide_cta_option", lambda _sheet_id, _row_number, _option: None)
get_original_thumbnails = getattr(sheet_ops, "get_original_thumbnails", lambda _sheet_id: {})
save_original_thumbnail = getattr(sheet_ops, "save_original_thumbnail", lambda _sheet_id, _row_number, _link: None)
clear_original_thumbnail = getattr(sheet_ops, "clear_original_thumbnail", lambda _sheet_id, _row_number: None)
shift_original_thumbnails_after_delete = getattr(sheet_ops, "shift_original_thumbnails_after_delete", lambda _sheet_id, _row_number: None)
if hasattr(sheet_ops, "get_last_scheduled_times"):
    get_last_scheduled_times = sheet_ops.get_last_scheduled_times
else:
    def get_last_scheduled_times(sheet_id: str) -> list[str]:
        if hasattr(sheet_ops, "get_last_scheduled_time"):
            value = sheet_ops.get_last_scheduled_time(sheet_id)
            return [value] if value else []
        return []

if hasattr(sheet_ops, "update_last_scheduled_times"):
    update_last_scheduled_times = sheet_ops.update_last_scheduled_times
else:
    def update_last_scheduled_times(sheet_id: str, scheduled_times: list[str]) -> None:
        if hasattr(sheet_ops, "update_last_scheduled_time") and scheduled_times:
            sheet_ops.update_last_scheduled_time(sheet_id, scheduled_times[-1])


def append_link_rows(sheet_id: str, urls: list[str], required_hashtags: str = "") -> None:
    if hasattr(sheet_ops, "append_link_rows"):
        sheet_ops.append_link_rows(sheet_id, urls, required_hashtags)
        return

    cleaned_urls = [url.strip() for url in urls if url.strip()]
    if not cleaned_urls:
        return

    ws = sheet_ops._worksheet(sheet_id)
    rows = []
    for url in cleaned_urls:
        row = [""] * len(sheet_ops._EXPECTED_HEADERS)
        row[0] = url
        row[10] = required_hashtags.strip()
        rows.append(row)
    sheet_ops._with_backoff(ws.append_rows, rows, value_input_option="USER_ENTERED")
    sheet_ops._invalidate_rows_cache(sheet_id)


def update_status(sheet_id: str, row_number: int, status: str) -> None:
    if hasattr(sheet_ops, "update_status"):
        sheet_ops.update_status(sheet_id, row_number, status)
        return

    ws = sheet_ops._worksheet(sheet_id)
    sheet_ops._with_backoff(ws.update, f"N{row_number}", [[status]])
    sheet_ops._invalidate_rows_cache(sheet_id)


def append_generated_post_rows(sheet_id: str, rows: list[dict]) -> None:
    if hasattr(sheet_ops, "append_generated_post_rows"):
        sheet_ops.append_generated_post_rows(sheet_id, rows)
        return
    raise RuntimeError("append_generated_post_rows is not available.")


def append_manual_post_row(sheet_id: str, row_data: dict) -> None:
    if hasattr(sheet_ops, "append_manual_post_row"):
        sheet_ops.append_manual_post_row(sheet_id, row_data)
        return
    raise RuntimeError("append_manual_post_row is not available.")


def update_generated_post_slides_and_status(
    sheet_id: str,
    row_number: int,
    name: str,
    text1: str,
    text2: str,
    text3: str,
    text4: str,
    text5: str,
    text6: str,
    status: str,
) -> None:
    if hasattr(sheet_ops, "update_generated_post_slides_and_status"):
        sheet_ops.update_generated_post_slides_and_status(
            sheet_id,
            row_number,
            name,
            text1,
            text2,
            text3,
            text4,
            text5,
            text6,
            status,
        )
        return
    raise RuntimeError("update_generated_post_slides_and_status is not available.")


def _is_reel_url(url: str) -> bool:
    lowered = (url or "").lower()
    return "/reel/" in lowered or "/reels/" in lowered


def _cell_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _is_instagram_url(url: str) -> bool:
    return "instagram.com/" in (url or "").lower()


def _is_article_url(url: str) -> bool:
    return _is_https_url(url) and not _is_instagram_url(url)


def _is_substack_url(url: str) -> bool:
    lowered = (url or "").lower()
    return "substack.com/" in lowered


def _format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    units = ["B", "KB", "MB", "GB"]
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    return f"{value:.1f} {unit}"


def _get_remote_file_size(url: str) -> int:
    try:
        response = requests.head(url, allow_redirects=True, timeout=20)
        response.raise_for_status()
        content_length = response.headers.get("Content-Length") or response.headers.get("content-length")
        if content_length:
            return int(content_length)
    except Exception:
        pass

    response = requests.get(url, allow_redirects=True, timeout=20, stream=True)
    response.raise_for_status()
    content_length = response.headers.get("Content-Length") or response.headers.get("content-length")
    if content_length:
        return int(content_length)
    raise ValueError("Could not determine reel file size.")


def _check_reel_transcript_risk(row: dict) -> dict | None:
    url = row.get("Instagram URL", "").strip()
    if not _is_reel_url(url):
        return None

    preview = process_reel_url(url, include_transcript=False)
    media_urls = preview.get("media_urls") or []
    if not media_urls:
        raise ValueError("Could not find the reel video URL for size check.")

    size_bytes = _get_remote_file_size(media_urls[0])
    if size_bytes <= TRANSCRIPT_SIZE_WARNING_BYTES:
        return None

    return {
        "size_bytes": size_bytes,
        "threshold_bytes": TRANSCRIPT_SIZE_WARNING_BYTES,
    }


def _ensure_home_links() -> list[str]:
    if st.session_state.pop("_workspace_reset_home_links", False):
        for key in list(st.session_state.keys()):
            if key.startswith("workspace_home_link_"):
                st.session_state.pop(key, None)
        st.session_state["workspace_home_links"] = [""]

    links = st.session_state.setdefault("workspace_home_links", [""])
    if not links:
        links.append("")
    return links


def _reset_home_links_on_next_render() -> None:
    st.session_state["_workspace_reset_home_links"] = True
    st.session_state["workspace_home_links"] = [""]


def _open_workspace_home_action_dialog(mode: str) -> None:
    st.session_state["workspace_home_action_dialog"] = mode


def _close_workspace_home_action_dialog(clear_inputs: bool = False) -> None:
    st.session_state.pop("workspace_home_action_dialog", None)
    if clear_inputs:
        st.session_state.pop("workspace_home_dialog_link", None)
        st.session_state.pop("workspace_home_dialog_org_hashtag", None)
        st.session_state.pop("workspace_home_candidate_article_step", None)
        st.session_state.pop("workspace_home_candidate_article_body", None)
        st.session_state.pop("workspace_home_candidate_article_error", None)
        st.session_state.pop("workspace_home_candidate_article_result", None)
        st.session_state.pop("workspace_home_candidate_article_generating", None)
        st.session_state.pop("workspace_home_create_post_prompt", None)
        st.session_state.pop("workspace_home_create_post_speaker", None)
        st.session_state.pop("workspace_home_create_post_link", None)


def _dismiss_workspace_home_action_dialog() -> None:
    _close_workspace_home_action_dialog(clear_inputs=True)


def _open_video_post_dialog() -> None:
    st.session_state["workspace_video_post_dialog"] = True


def _close_video_post_dialog(clear_inputs: bool = False) -> None:
    st.session_state.pop("workspace_video_post_dialog", None)
    if clear_inputs:
        st.session_state.pop("workspace_video_post_upload", None)
        st.session_state.pop("workspace_video_post_speaker", None)


def _dismiss_video_post_dialog() -> None:
    _close_video_post_dialog(clear_inputs=True)


def _open_election_post_dialog() -> None:
    st.session_state["workspace_election_post_dialog"] = True


def _close_election_post_dialog(clear_inputs: bool = False) -> None:
    st.session_state.pop("workspace_election_post_dialog", None)
    if clear_inputs:
        st.session_state.pop("workspace_election_post_candidates", None)
        st.session_state.pop("workspace_election_post_result", None)
        st.session_state.pop("workspace_election_post_error", None)
        st.session_state.pop("workspace_election_post_resolved", None)


def _dismiss_election_post_dialog() -> None:
    _close_election_post_dialog(clear_inputs=True)


def _open_workspace_slides_dialog() -> None:
    st.session_state["workspace_slides_dialog"] = True


def _close_workspace_slides_dialog() -> None:
    st.session_state.pop("workspace_slides_dialog", None)


def _open_workspace_post_slides_dialog(row_number: int) -> None:
    if st.session_state.get("workspace_post_slides_dialog_row") != row_number:
        st.session_state.pop("workspace_post_slides_results", None)
    st.session_state["workspace_post_slides_dialog_row"] = row_number


def _close_workspace_post_slides_dialog(clear_inputs: bool = False) -> None:
    st.session_state.pop("workspace_post_slides_dialog_row", None)
    if clear_inputs:
        st.session_state.pop("workspace_post_slides_results", None)


def _open_workspace_generic_slides_dialog(row_number: int) -> None:
    if st.session_state.get("workspace_generic_slides_dialog_row") != row_number:
        st.session_state.pop("workspace_generic_slides_results", None)
    st.session_state["workspace_generic_slides_dialog_row"] = row_number


def _close_workspace_generic_slides_dialog(clear_inputs: bool = False) -> None:
    st.session_state.pop("workspace_generic_slides_dialog_row", None)
    if clear_inputs:
        st.session_state.pop("workspace_generic_slides_results", None)


def _dismiss_workspace_generic_slides_dialog() -> None:
    _close_workspace_generic_slides_dialog(clear_inputs=True)


def _workspace_home_link_label(mode: str) -> str:
    if mode == "Process as Candidate Article":
        return "Substack URL"
    return "Link"


def _workspace_home_link_placeholder(mode: str) -> str:
    if mode == "Process as Candidate Article":
        return "https://yourpublication.substack.com/p/race-name"
    return "https://www.instagram.com/... or https://example.com/article"


def _open_workspace_slide_action_dialog(row_number: int, action: str) -> None:
    st.session_state["workspace_slide_action_dialog"] = {
        "row_number": row_number,
        "action": action,
    }


def _close_workspace_slide_action_dialog(clear_inputs: bool = False) -> None:
    st.session_state.pop("workspace_slide_action_dialog", None)
    if clear_inputs:
        st.session_state.pop("workspace_slide_dialog_context", None)
        st.session_state.pop("workspace_slide_dialog_value", None)


def _dismiss_workspace_slide_action_dialog() -> None:
    _close_workspace_slide_action_dialog(clear_inputs=True)


def _open_workspace_candidate_article_dialog(row_number: int) -> None:
    st.session_state["workspace_candidate_article_dialog_row"] = row_number


def _close_workspace_candidate_article_dialog(clear_inputs: bool = False) -> None:
    st.session_state.pop("workspace_candidate_article_dialog_row", None)
    if clear_inputs:
        st.session_state.pop("workspace_row_candidate_article_body", None)
        st.session_state.pop("workspace_row_candidate_article_error", None)
        st.session_state.pop("workspace_row_candidate_article_result", None)
        st.session_state.pop("workspace_row_candidate_article_generating", None)


def _dismiss_workspace_candidate_article_dialog() -> None:
    _close_workspace_candidate_article_dialog(clear_inputs=True)


def _run_workspace_home_action(mode: str, link_value: str, org_hashtag: str = "") -> None:
    cleaned_link = (link_value or "").strip()
    if not cleaned_link:
        st.warning(f"Enter at least one {_workspace_home_link_label(mode).lower()}.")
        return

    links_to_process = [cleaned_link]
    st.session_state["workspace_home_links"] = _normalize_home_links(links_to_process)
    st.session_state["workspace_org_hashtag"] = org_hashtag
    selected_hashtag = ORG_HASHTAG_MAP.get(org_hashtag, "")

    if mode == "Process this":
        with st.spinner("Processing link end-to-end..."):
            try:
                row_number = _process_single_url_to_editor(links_to_process[0], selected_hashtag)
            except Exception as e:
                st.error(f"Process this failed: {describe_error(e)}")
                return
        st.session_state["workspace_home_notice"] = (
            f"Processed row {row_number}: ingest, caption, and slide text complete."
        )
        st.session_state["workspace_selected_row_num"] = row_number
        st.query_params["workspace_row"] = str(row_number)
        _close_workspace_home_action_dialog(clear_inputs=True)
        _reset_home_links_on_next_render()
        _rerun_workspace("Home")

    with st.spinner(f"{mode} in progress..."):
        try:
            tag_value, results = _run_home_mode(mode, links_to_process, org_hashtag)
        except Exception as e:
            st.error(f"{mode} failed: {describe_error(e)}")
            return

    st.session_state["workspace_home_results"] = {
        "mode": mode,
        "required_hashtag": tag_value,
        "items": results,
    }
    st.session_state["workspace_home_notice"] = f"{mode} finished for {len(results)} link(s)."
    _close_workspace_home_action_dialog(clear_inputs=True)
    _reset_home_links_on_next_render()
    _rerun_workspace("Home")


def _mark_transcribe_checkbox_for_reset(row: dict) -> None:
    transcribe_key = _workspace_key(row, "transcribe")
    pending = st.session_state.setdefault("workspace_transcribe_reset_rows", [])
    if transcribe_key not in pending:
        pending.append(transcribe_key)


def _workspace_row_identity(row: dict) -> str:
    return "||".join([
        _cell_text(row.get("Instagram URL")).strip(),
        _cell_text(row.get("Media Type")).strip(),
        _cell_text(row.get("Source Username")).strip(),
    ])


def _row_state_token(row: dict) -> str:
    identity = _workspace_row_identity(row) or str(row.get("row_number", ""))
    return hashlib.md5(identity.encode("utf-8")).hexdigest()[:12]


def _workspace_stable_row_key(row: dict, name: str) -> str:
    return f"workspace_{name}_row_{row.get('row_number', '')}"


def _workspace_speaker_key(row: dict) -> str:
    return _workspace_stable_row_key(row, "speaker")


def _workspace_row_state_keys_for_token(token: str) -> list[str]:
    return [
        f"workspace_hashtags_{token}",
        f"workspace_top_{token}",
        f"workspace_context_{token}",
        f"workspace_transcript_warning_{token}",
        f"workspace_transcribe_{token}",
        f"workspace_link_editor_open_{token}",
        f"workspace_link_source_{token}",
        f"workspace_link_url_{token}",
        f"workspace_link_display_{token}",
        f"workspace_link_comment_{token}",
        f"workspace_menu_nonce_{token}",
        f"workspace_thumbnail_upload_{token}",
        f"workspace_slide_three_cta_{token}",
    ]


def _workspace_key(row: dict, name: str) -> str:
    return f"workspace_{name}_{_row_state_token(row)}"


def _workspace_row_state_keys(row: dict) -> list[str]:
    return _workspace_row_state_keys_for_token(_row_state_token(row))


def _clear_row_num_keyed_state(row_num: int) -> None:
    """Clear all UI state that is keyed by row position rather than content identity."""
    for key in [
        f"workspace_row_content_tab_{row_num}",
        f"workspace_row_slides_prompt_{row_num}",
        f"workspace_slide_preview_font_adjust_{row_num}",
        f"workspace_slide_preview_background_adjust_{row_num}",
        f"workspace_slide_preview_fit_mode_{row_num}",
        f"workspace_slide_two_preview_font_adjust_{row_num}",
        f"workspace_slide_two_cta_row_{row_num}",
        f"workspace_slide_three_preview_font_adjust_{row_num}",
        f"workspace_slide_three_cta_row_{row_num}",
        f"workspace_slide_four_preview_font_adjust_{row_num}",
        f"workspace_slide_five_preview_font_adjust_{row_num}",
        f"workspace_slide_six_preview_font_adjust_{row_num}",
        f"workspace_slide_merge_row_{row_num}",
        f"workspace_slide_merge_original_t3_{row_num}",
        f"workspace_slide_quote_show_{row_num}",
        f"workspace_slide_quote_font_adjust_{row_num}",
        f"workspace_preview_upload_links_{row_num}",
        f"workspace_quote_picker_{row_num}",
        f"workspace_quote_options_{row_num}",
    ]:
        st.session_state.pop(key, None)
    st.session_state.get("workspace_original_thumbnails", {}).pop(str(row_num), None)


def _sync_workspace_row_state(row: dict) -> None:
    identity_key = _workspace_stable_row_key(row, "identity")
    token_key = _workspace_stable_row_key(row, "state_token")
    speaker_key = _workspace_speaker_key(row)
    current_identity = _workspace_row_identity(row)
    current_token = _row_state_token(row)
    previous_identity = st.session_state.get(identity_key)
    previous_token = st.session_state.get(token_key)
    if previous_identity == current_identity:
        return
    tokens_to_clear = {current_token}
    if previous_token:
        tokens_to_clear.add(previous_token)
    if previous_identity is not None or previous_token is not None:
        for token in tokens_to_clear:
            for key in _workspace_row_state_keys_for_token(token):
                st.session_state.pop(key, None)
        _clear_row_num_keyed_state(row["row_number"])
    st.session_state[speaker_key] = _cell_text(row.get("Speaker Name")).strip()
    st.session_state[identity_key] = current_identity
    st.session_state[token_key] = current_token


def _clear_workspace_row_state(row: dict) -> None:
    identity_key = _workspace_stable_row_key(row, "identity")
    token_key = _workspace_stable_row_key(row, "state_token")
    speaker_key = _workspace_speaker_key(row)
    previous_token = st.session_state.get(token_key)
    tokens_to_clear = {_row_state_token(row)}
    if previous_token:
        tokens_to_clear.add(previous_token)
    st.session_state.pop(speaker_key, None)
    for token in tokens_to_clear:
        for key in _workspace_row_state_keys_for_token(token):
            st.session_state.pop(key, None)
    _clear_row_num_keyed_state(row["row_number"])
    st.session_state.pop(identity_key, None)
    st.session_state.pop(token_key, None)


def _normalize_home_links(links: list[str]) -> list[str]:
    first = ""
    for link in links:
        if (link or "").strip():
            first = link
            break
    return [first]


def _remove_home_link(index: int) -> None:
    links = st.session_state.get("workspace_home_links", [""])
    next_links = [link for i, link in enumerate(links) if i != index]
    st.session_state["workspace_home_links"] = _normalize_home_links(next_links or [""])


def _action_label(mode: str) -> str:
    return {
        "Process this": "Process",
        "Generate headline": "Generate",
        "Caption this": "Caption",
        "Process as Candidate Article": "Process as Candidate Article",
    }.get(mode, "Add")


def _mode_uses_org_hashtag(mode: str) -> bool:
    return mode in {"Caption this"}


def _clean_home_links() -> list[str]:
    return [link.strip() for link in st.session_state.get("workspace_home_links", []) if link.strip()]


def _row_is_dirty(row: dict) -> bool:
    speaker_key = _workspace_speaker_key(row)
    hashtags_key = _workspace_key(row, "hashtags")
    top_key = _workspace_key(row, "top")
    context_key = _workspace_key(row, "context")
    return any(
        [
            _cell_text(st.session_state.get(speaker_key, row.get("Speaker Name", ""))).strip()
            != _cell_text(row.get("Speaker Name")).strip(),
            _cell_text(st.session_state.get(hashtags_key, row.get("Required Hashtags", ""))).strip()
            != _cell_text(row.get("Required Hashtags")).strip(),
            _cell_text(st.session_state.get(top_key, row.get("Top Comment", ""))).strip()
            != _cell_text(row.get("Top Comment")).strip(),
            _cell_text(st.session_state.get(context_key, row.get("Caption Context", ""))).strip()
            != _cell_text(row.get("Caption Context")).strip(),
        ]
    )


def _is_editable_row(row: dict) -> bool:
    status = _cell_text(row.get("Status")).strip().lower()

    # Rows with a known editable status are always shown, even without an
    # Instagram URL (manually-created posts from "Create a Post" have no URL).
    if status in EDITABLE_STATUSES:
        return True

    # Rows without a URL and without an editable status are hidden.
    if not _cell_text(row.get("Instagram URL")).strip():
        return False

    # Some rows may already be effectively ingested even if the status field
    # is not one of the editor-specific values yet.
    return any(
        _cell_text(row.get(field, "")).strip()
        for field in [
            "Source Username",
            "Media Type",
            "Media Drive Link",
            "Thumbnail Drive Link",
            "Original Caption",
            "Transcript",
            "Generated Caption",
        ]
    )


def _default_editor_status(row: dict) -> str:
    generated_caption = (row.get("Generated Caption") or "").strip()
    return "done" if generated_caption else "ingested"


def _sort_editor_rows(rows: list[dict]) -> list[dict]:
    def sort_key(row):
        is_skipped = _cell_text(row.get("Status")).strip().lower() == "skipped"
        return (1 if is_skipped else 0, row.get("row_number", 0))

    return sorted(rows, key=sort_key)


def _row_has_slide_text(row: dict) -> bool:
    return bool(
        _cell_text(row.get("text1")).strip()
        and _cell_text(row.get("text2")).strip()
        and _cell_text(row.get("text3")).strip()
    )


def _grid_badges(row: dict) -> list[tuple[str, str]]:
    badges = []
    media_type = _cell_text(row.get("Media Type")).strip().lower()
    status = _cell_text(row.get("Status")).strip().lower()
    if _cell_text(row.get("Generated Caption")).strip():
        badges.append(("C", "Has caption"))
    if _cell_text(row.get("Transcript")).strip():
        badges.append(("T", "Transcribed"))
    if _row_has_slide_text(row):
        badges.append(("S", "Slide text complete"))
    if status == "skipped":
        badges.append(("Skip", "Skipped"))
    try:
        photo_count = int(row.get("Photo Count") or 0)
    except Exception:
        photo_count = 0
    if media_type == "photo" and photo_count > 1:
        badges.append(("P+", "Photo carousel"))
    return badges


def _grid_preview_url(row: dict) -> str:
    thumb_link = _cell_text(row.get("Thumbnail Drive Link")).strip()
    if thumb_link:
        return _drive_image_url(thumb_link) or thumb_link
    return ""


def _visible_rows_with_target(rows: list[dict], limit: int, target_row_number: str = "") -> list[dict]:
    visible_rows = rows[:limit]
    if target_row_number:
        target_row = next((row for row in rows if str(row.get("row_number", "")) == target_row_number), None)
        is_skipped = _cell_text((target_row or {}).get("Status")).strip().lower() == "skipped"
        if target_row and not is_skipped and all(row.get("row_number") != target_row.get("row_number") for row in visible_rows):
            visible_rows = [*visible_rows, target_row]
    return visible_rows


def _render_editor_grid(editor_rows: list[dict], selected_row_num: int | None = None) -> None:
    cards = []
    for i, row in enumerate(editor_rows):
        row_num = row.get("row_number")
        username = _cell_text(row.get("Source Username")).strip().lstrip("@")
        media_type = _cell_text(row.get("Media Type")).strip().lower() or "post"
        image_url = _grid_preview_url(row)
        selected_class = " workspace-grid-card-selected" if row_num == selected_row_num else ""
        badge_html = "".join(
            f'<span class="workspace-grid-badge" title="{html.escape(title)}">{html.escape(label)}</span>'
            for label, title in _grid_badges(row)
        )
        label = f"@{username}" if username and media_type != "article" else (username or f"Row {row_num}")
        if i == 0:
            href = "?"
            extra_attrs = ' title="Refresh workspace"'
        else:
            href = f"?workspace_row={row_num}#workspace-row-{row_num}"
            extra_attrs = ""
        if image_url:
            media_html = f'<img src="{html.escape(image_url)}" alt="{html.escape(label)}" loading="lazy" decoding="async">'
        else:
            media_html = (
                '<div class="workspace-grid-placeholder">'
                f'{html.escape(label)}<br>{html.escape(media_type)}'
                '</div>'
            )
        cards.append(
            f"""
            <a class="workspace-grid-card{selected_class}" href="{html.escape(href)}"{extra_attrs}>
              {media_html}
              <div class="workspace-grid-badges">{badge_html}</div>
              <div class="workspace-grid-meta">{html.escape(label)} · {html.escape(media_type)}</div>
            </a>
            """
        )
    grid_html = "".join(cards)
    st.html(f'<div class="workspace-grid">{grid_html}</div>')


def _scroll_to_element(element_id: str, block: str = "center") -> None:
    if not element_id:
        return
    script = f"""
    <script>
    const targetId = {json.dumps(element_id)};
    function scrollToTarget(attempt) {{
      const target = window.parent.document.getElementById(targetId);
      if (target) {{
        target.scrollIntoView({{ behavior: "smooth", block: {json.dumps(block)} }});
        return;
      }}
      if (attempt < 20) {{
        window.setTimeout(() => scrollToTarget(attempt + 1), 100);
      }}
    }}
    window.setTimeout(() => scrollToTarget(0), 100);
    </script>
    """
    st.html(script)


def _scroll_to_editor_row(row_number: str) -> None:
    if not row_number:
        return
    _scroll_to_element(f"workspace-row-{row_number}", block="start")


WEEKDAY_OPTIONS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
EASTERN_TZ = ZoneInfo("America/New_York")


def _schedule_day_defaults() -> tuple[str, dt_time]:
    now = datetime.now(EASTERN_TZ)
    return WEEKDAY_OPTIONS[now.weekday()], now.time().replace(second=0, microsecond=0)


def _next_schedule_slot(current_dt: datetime, rollover_minute: int) -> datetime:
    next_dt = current_dt + timedelta(hours=1)
    if next_dt.date() != current_dt.date():
        next_dt = next_dt.replace(hour=9, minute=rollover_minute, second=0, microsecond=0)
    elif next_dt.hour < 9:
        next_dt = next_dt.replace(hour=9, minute=rollover_minute, second=0, microsecond=0)
    return next_dt


def _format_schedule_time(value: dt_time) -> str:
    hour = value.hour % 12 or 12
    suffix = "am" if value.hour < 12 else "pm"
    return f"{hour}:{value.minute:02d}{suffix}"


def _time_parts(value: dt_time) -> tuple[int, int, str]:
    hour = value.hour % 12 or 12
    suffix = "am" if value.hour < 12 else "pm"
    return hour, value.minute, suffix


def _time_from_parts(hour: int, minute: int, suffix: str) -> dt_time:
    normalized_hour = hour % 12
    if suffix == "pm":
        normalized_hour += 12
    return dt_time(normalized_hour, minute)


def _build_schedule_labels(rows: list[dict], start_day: str, start_time: dt_time) -> dict[int, str]:
    if not rows:
        return {}

    start_index = WEEKDAY_OPTIONS.index(start_day)
    anchor = datetime(2026, 1, 5 + start_index, start_time.hour, start_time.minute)
    current = anchor
    labels: dict[int, str] = {}
    rollover_minute = start_time.minute
    for row in rows:
        current = _next_schedule_slot(current, rollover_minute)
        labels[row["row_number"]] = f"{WEEKDAY_OPTIONS[current.weekday()]} {_format_schedule_time(current.time())}"
    return labels


def _last_scheduled_time_labels(rows: list[dict]) -> list[str]:
    scheduled_rows = sorted(
        [
            row for row in rows
            if (row.get("Scheduled Time", "") or "").strip()
        ],
        key=lambda row: row.get("row_number", 0),
    )
    if not scheduled_rows:
        return []
    return [
        (row.get("Scheduled Time", "") or "").strip()
        for row in scheduled_rows[-3:]
        if (row.get("Scheduled Time", "") or "").strip()
    ]


def _persisted_last_scheduled_time_labels(rows: list[dict]) -> list[str]:
    try:
        persisted = get_last_scheduled_times(GOOGLE_SHEET_ID)
        if persisted:
            return persisted
    except Exception:
        pass
    row_labels = _last_scheduled_time_labels(rows)
    return row_labels[-1:] if row_labels else []


def _fetch_post_data(url: str) -> dict:
    if _is_reel_url(url):
        return process_reel_url(url, include_transcript=False)
    return process_post_url(url)


def _fetch_link_data(url: str) -> dict:
    if _is_instagram_url(url):
        post = _fetch_post_data(url)
        return {
            "url": url,
            "username": post.get("username", ""),
            "source_text": (post.get("original_caption") or "").strip(),
            "is_instagram": True,
        }

    article = _fetch_article_source_data(url)
    article_source_text = (
        (article.get("source_text") or "").strip()
        or (article.get("summary_text") or "").strip()
    )
    return {
        "url": article.get("url", url),
        "username": "",
        "display_name": article.get("domain", ""),
        "source_text": article_source_text,
        "is_instagram": False,
    }


def _generate_headlines(source_text: str) -> list[str]:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured.")
    response = _get_client().chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": (
                    "You write short, salacious, attention-grabbing political headlines. "
                    "Return exactly 3 distinct headline options. Keep each under 12 words. "
                    "Do not use hashtags. Do not use quotation marks unless essential. "
                    "Do not add labels or extra explanation. Put each headline on its own line."
                ),
            },
            {
                "role": "user",
                "content": f"Write a headline from this Instagram caption:\n\n{source_text}",
            },
        ],
        max_tokens=60,
        temperature=0.9,
    )
    raw_lines = response.choices[0].message.content.strip().splitlines()
    headlines = []
    for line in raw_lines:
        cleaned = line.strip().lstrip("-*0123456789. ").replace("#", "")
        if cleaned:
            headlines.append(cleaned)
        if len(headlines) == 3:
            break
    return headlines


def _build_footered_caption(caption_body: str, username: str, required_hashtags: str = "") -> str:
    footer_parts = []
    cleaned_username = (username or "").strip().lstrip("@")
    if cleaned_username and cleaned_username.lower() != "unknown":
        footer_parts.append(f"Follow @{cleaned_username} for more.")
    footer_parts.append(
        "Help this information get to more voters. 🇺🇸 "
        "A well-informed electorate is a prerequisite to Democracy. - Thomas Jefferson"
    )
    if required_hashtags.strip():
        footer_parts.append(required_hashtags.strip())
    return f"{caption_body.strip()}\n\n{' '.join(footer_parts)}"


def _build_original_caption_preview(
    original_caption: str,
    username: str,
    top_comment: str = "",
    required_hashtags: str = "",
    is_instagram: bool = True,
) -> str:
    top_comment, _ = _decode_top_comment(top_comment)
    original_with_username = (original_caption or "").strip()
    cleaned_username = (username or "").strip().lstrip("@")
    if is_instagram and cleaned_username and original_with_username:
        original_with_username = f"@{cleaned_username}: {original_with_username}"
    original_preview = original_with_username
    if original_preview and (top_comment or "").strip():
        original_preview = f"{original_preview}\n\n{top_comment.strip()}"
    elif (top_comment or "").strip():
        original_preview = top_comment.strip()
    footer_username = username if is_instagram else ""
    return (
        _build_footered_caption(original_preview, footer_username, required_hashtags)
        if original_preview
        else ""
    )


def _ensure_required_hashtags_text(value: str, required_hashtags: str) -> str:
    caption = (value or "").strip()
    required = re.findall(r"#\w+", required_hashtags or "")
    if not caption or not required:
        return caption
    existing = {tag.lower() for tag in re.findall(r"#\w+", caption)}
    missing = [tag for tag in required if tag.lower() not in existing]
    if missing:
        caption = f"{caption}\n\n{' '.join(missing)}"
    return caption


def _caption_tab_value(
    generated: str,
    original_caption: str,
    username: str,
    top_comment: str,
    required_hashtags: str,
    is_instagram: bool,
) -> str:
    generated = (generated or "").strip()
    if generated:
        return _ensure_required_hashtags_text(generated, required_hashtags)
    return _build_original_caption_preview(
        original_caption,
        username,
        top_comment,
        required_hashtags,
        is_instagram=is_instagram,
    )


def _drive_image_url(drive_link: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", drive_link or "")
    if m:
        return f"https://drive.google.com/thumbnail?id={m.group(1)}&sz=w1200"
    parsed = urlparse(drive_link or "")
    file_id = parse_qs(parsed.query).get("id", [""])[0]
    if file_id:
        return f"https://drive.google.com/thumbnail?id={file_id}&sz=w1200"
    return ""


def _upload_article_thumbnail(image_url: str, row_number: int | str | None, username: str) -> str:
    image_url = _cell_text(image_url).strip()
    if not image_url:
        return ""

    tmp_dir = tempfile.mkdtemp(prefix="article_thumb_")
    try:
        screenshots_folder_id = get_or_create_subfolder(
            GOOGLE_DRIVE_FOLDER_ID,
            GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
        )
        parsed = urlparse(image_url)
        ext = os.path.splitext(parsed.path or "")[1].lower() or ".jpg"
        if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            ext = ".jpg"
        filename_prefix = build_filename_prefix(row_number, username)
        filename = f"{filename_prefix}article_{row_number or 'thumb'}_thumb{ext}"
        local_path = os.path.join(tmp_dir, filename)
        response = requests.get(
            image_url,
            allow_redirects=True,
            timeout=60,
            stream=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
                ),
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                "Referer": "https://www.google.com/",
            },
        )
        response.raise_for_status()
        with open(local_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)
        return upload_to_drive(local_path, filename, screenshots_folder_id)
    except Exception:
        return image_url
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _safe_image_url(raw_value: str) -> str:
    candidate = _drive_view_url(raw_value) or _drive_image_url(raw_value) or _cell_text(raw_value).strip()
    return candidate if _is_https_url(candidate) else ""


def _drive_view_url(drive_link: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)/", drive_link or "")
    if m:
        return f"https://drive.google.com/uc?export=view&id={m.group(1)}"
    parsed = urlparse(drive_link or "")
    file_id = parse_qs(parsed.query).get("id", [""])[0]
    if file_id:
        return f"https://drive.google.com/uc?export=view&id={file_id}"
    return ""


def _safe_browser_image_url(raw_value: str) -> str:
    candidate = _drive_view_url(raw_value) or _drive_image_url(raw_value) or _cell_text(raw_value).strip()
    return candidate if _is_https_url(candidate) else ""


def _ffmpeg_filter_value(value: str) -> str:
    return (
        (value or "")
        .replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\\'")
        .replace(",", "\\,")
    )


def _preview_font_path(bold: bool = False) -> str:
    candidates = (
        [
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/System/Library/Fonts/Supplemental/Helvetica Bold.ttf",
            "/Library/Fonts/Arial Bold.ttf",
        ]
        if bold else
        [
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/System/Library/Fonts/Supplemental/Helvetica.ttf",
            "/Library/Fonts/Arial.ttf",
        ]
    )
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return ""


def _preview_ffmpeg_path() -> str:
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError("ffmpeg is not installed or not on PATH.")
    return ffmpeg_path


def _write_preview_text_file(tmp_dir: str, filename: str, value: str, wrap_width: int) -> str:
    path = os.path.join(tmp_dir, filename)
    wrapped_lines: list[str] = []
    for raw_line in (value or "").splitlines() or [""]:
        cleaned_line = raw_line.strip()
        if not cleaned_line:
            wrapped_lines.append("")
            continue
        wrapped_lines.extend(textwrap.wrap(cleaned_line, width=wrap_width, break_long_words=False) or [""])
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(wrapped_lines).strip())
    return path


def _download_preview_background(url: str, tmp_dir: str) -> str:
    if not _is_https_url(url):
        return ""
    output_path = os.path.join(tmp_dir, "preview_background.jpg")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    with open(output_path, "wb") as handle:
        handle.write(response.content)
    return output_path


def _preview_folder_base_name(username: str, media_link: str, row_num: int) -> tuple[str, str]:
    cleaned_username = re.sub(r"[^A-Za-z0-9._-]+", "_", (username or "").strip().lstrip("@")).strip("._-")
    if media_link:
        try:
            metadata = get_drive_file_metadata(media_link)
            filename = (metadata.get("name") or "").strip()
            stem = os.path.splitext(filename)[0]
            match = re.match(r"(?P<username>[A-Za-z0-9._-]+)_(?P<date>\d{6})_", stem)
            if match:
                matched_username = (match.group("username") or "").strip("._-")
                matched_date = (match.group("date") or "").strip()
                return f"{matched_username}_{matched_date}", filename
            date_match = re.search(r"(\d{6})", stem)
            if cleaned_username and date_match:
                return f"{cleaned_username}_{date_match.group(1)}", filename
            if stem:
                return stem, filename
            return filename or f"{cleaned_username or 'row'}_{row_num}", filename
        except Exception:
            pass
    fallback = f"{cleaned_username or 'row'}_{row_num}"
    return fallback, ""


def _ffprobe_path() -> str:
    ffprobe_path = shutil.which("ffprobe")
    if not ffprobe_path:
        ffprobe_path = _crop_ffprobe_path()
    return ffprobe_path


def _video_duration_seconds(path: str) -> float:
    command = [
        _ffprobe_path(),
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    duration_text = (result.stdout or "").strip()
    return float(duration_text) if duration_text else 0.0


def _refresh_row_thumbnail_from_video(row: dict, offset_seconds: float = 5.0) -> str:
    if update_thumbnail_link is None:
        raise RuntimeError("Thumbnail link updates are not supported in this build.")

    media_links = [link.strip() for link in (_cell_text(row.get("Media Drive Link")) or "").split(",") if link.strip()]
    if not media_links:
        raise ValueError("This row does not have a Drive video link yet.")

    media_link = media_links[0]
    metadata = get_drive_file_metadata(media_link)
    filename = (metadata.get("name") or "").strip()
    if not filename:
        raise ValueError("Could not determine the video filename from Drive.")

    row_num = row["row_number"]
    tmp_dir = tempfile.mkdtemp(prefix="workspace_thumb_")
    try:
        local_video_path = os.path.join(tmp_dir, filename or f"row_{row_num}.mp4")
        download_drive_file(media_link, local_video_path)

        duration_seconds = 0.0
        try:
            duration_seconds = _video_duration_seconds(local_video_path)
        except Exception:
            duration_seconds = 0.0
        capture_seconds = offset_seconds
        if duration_seconds > 0:
            capture_seconds = min(offset_seconds, max(0.0, duration_seconds - 0.25))

        screenshots_folder_id = get_or_create_subfolder(
            GOOGLE_DRIVE_FOLDER_ID,
            GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
        )
        stem, _ext = os.path.splitext(filename)
        screenshot_name = f"{stem}_thumb_{int(round(capture_seconds))}s.jpg"
        screenshot_path = os.path.join(tmp_dir, screenshot_name)
        command = [
            shutil.which("ffmpeg") or "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            f"{capture_seconds:.3f}",
            "-i",
            local_video_path,
            "-frames:v",
            "1",
            screenshot_path,
        ]
        subprocess.run(command, check=True)
        thumbnail_link = upload_to_drive(screenshot_path, screenshot_name, screenshots_folder_id)
        update_thumbnail_link(GOOGLE_SHEET_ID, row_num, thumbnail_link)
        clear_original_thumbnail(GOOGLE_SHEET_ID, row_num)
        st.session_state.get("workspace_original_thumbnails", {}).pop(str(row_num), None)
        return thumbnail_link
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _replace_row_thumbnail_from_upload(row: dict, uploaded_file) -> str:
    if update_thumbnail_link is None:
        raise RuntimeError("Thumbnail link updates are not supported in this build.")

    row_num = row["row_number"]
    screenshots_folder_id = get_or_create_subfolder(
        GOOGLE_DRIVE_FOLDER_ID,
        GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    )

    media_links = [link.strip() for link in (_cell_text(row.get("Media Drive Link")) or "").split(",") if link.strip()]
    screenshot_stem = f"row_{row_num}_thumb"
    if media_links:
        try:
            metadata = get_drive_file_metadata(media_links[0])
            filename = (metadata.get("name") or "").strip()
            if filename:
                screenshot_stem = f"{os.path.splitext(filename)[0]}_thumb"
        except Exception:
            pass

    source_name = getattr(uploaded_file, "name", "") or ""
    ext = os.path.splitext(source_name)[1].lower() or ".jpg"
    screenshot_name = f"{screenshot_stem}{ext}"

    tmp_dir = tempfile.mkdtemp(prefix="workspace_thumb_upload_")
    try:
        screenshot_path = os.path.join(tmp_dir, screenshot_name)
        with open(screenshot_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        thumbnail_link = upload_to_drive(screenshot_path, screenshot_name, screenshots_folder_id)
        update_thumbnail_link(GOOGLE_SHEET_ID, row_num, thumbnail_link)
        clear_original_thumbnail(GOOGLE_SHEET_ID, row_num)
        st.session_state.get("workspace_original_thumbnails", {}).pop(str(row_num), None)
        return thumbnail_link
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _blur_row_thumbnail(row: dict, sigma: int = 10) -> str:
    """Download the thumbnail, apply Gaussian blur, re-upload, update sheet."""
    if update_thumbnail_link is None:
        raise RuntimeError("Thumbnail link updates are not supported in this build.")

    thumb_link = _cell_text(row.get("Thumbnail Drive Link")).strip()
    if not thumb_link:
        raise RuntimeError("No thumbnail link on this row.")

    # Persist original before overwriting so Unblur can restore it.
    save_original_thumbnail(GOOGLE_SHEET_ID, row["row_number"], thumb_link)
    st.session_state.setdefault("workspace_original_thumbnails", {})[str(row["row_number"])] = thumb_link

    row_num = row["row_number"]
    screenshots_folder_id = get_or_create_subfolder(
        GOOGLE_DRIVE_FOLDER_ID,
        GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER,
    )

    tmp_dir = tempfile.mkdtemp(prefix="workspace_blur_")
    try:
        src_path = os.path.join(tmp_dir, "thumb_src.jpg")
        download_drive_file(thumb_link, src_path)

        out_path = os.path.join(tmp_dir, "thumb_blur.jpg")
        ffmpeg = _crop_ffmpeg_path()
        cmd = [
            ffmpeg, "-y", "-i", src_path,
            "-vf", f"gblur=sigma={sigma}",
            "-q:v", "2",
            out_path,
        ]
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode(errors="replace"))

        media_links = [lnk.strip() for lnk in (_cell_text(row.get("Media Drive Link")) or "").split(",") if lnk.strip()]
        screenshot_stem = f"row_{row_num}_thumb_blur"
        if media_links:
            try:
                metadata = get_drive_file_metadata(media_links[0])
                filename = (metadata.get("name") or "").strip()
                if filename:
                    screenshot_stem = f"{os.path.splitext(filename)[0]}_thumb_blur"
            except Exception:
                pass

        upload_name = f"{screenshot_stem}.jpg"
        thumbnail_link = upload_to_drive(out_path, upload_name, screenshots_folder_id)
        update_thumbnail_link(GOOGLE_SHEET_ID, row_num, thumbnail_link)
        return thumbnail_link
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _unblur_row_thumbnail(row: dict) -> str:
    """Restore the pre-blur thumbnail and remove the stored original link."""
    if update_thumbnail_link is None:
        raise RuntimeError("Thumbnail link updates are not supported in this build.")

    row_num = row["row_number"]
    originals = get_original_thumbnails(GOOGLE_SHEET_ID)
    original_link = originals.get(str(row_num), "").strip()
    if not original_link:
        raise RuntimeError("No original thumbnail stored for this row.")

    update_thumbnail_link(GOOGLE_SHEET_ID, row_num, original_link)
    clear_original_thumbnail(GOOGLE_SHEET_ID, row_num)

    # Refresh session-state cache so the button toggles immediately.
    if "workspace_original_thumbnails" in st.session_state:
        st.session_state["workspace_original_thumbnails"].pop(str(row_num), None)

    return original_link


def _render_slide_one_png(
    output_path: str,
    tmp_dir: str,
    handle_text: str,
    headline: str,
    background_url: str,
    headline_font_adjust_px: int = 0,
    background_y_adjust_px: int = 0,
) -> None:
    ffmpeg_path = _preview_ffmpeg_path()
    handle_file = _write_preview_text_file(tmp_dir, "slide1_handle.txt", (handle_text or "@UNKNOWN").upper(), 40)
    headline_file = _write_preview_text_file(tmp_dir, "slide1_headline.txt", headline, 24)
    bold_font = _preview_font_path(bold=True)
    regular_font = _preview_font_path(bold=False) or bold_font
    background_path = _download_preview_background(background_url, tmp_dir)
    handle_font_clause = f":fontfile='{_ffmpeg_filter_value(regular_font)}'" if regular_font else ""
    headline_font_clause = f":fontfile='{_ffmpeg_filter_value(bold_font)}'" if bold_font else ""
    font_size = max(8, round((96 + int(headline_font_adjust_px)) * PREVIEW_EXPORT_FONT_SCALE))
    y_offset = int(background_y_adjust_px)
    overlay_y = round(720 * PREVIEW_EXPORT_SCALE)
    overlay_h = round(900 * PREVIEW_EXPORT_SCALE)
    handle_y = round(1000 * PREVIEW_EXPORT_SCALE)
    headline_y = round(1080 * PREVIEW_EXPORT_SCALE)
    handle_font_size = max(26, round(30 * PREVIEW_EXPORT_FONT_SCALE))
    line_spacing = max(15, round(18 * PREVIEW_EXPORT_FONT_SCALE))
    y_shift = round(y_offset * PREVIEW_EXPORT_SCALE)
    y_pad_expr = f"(oh-ih)/2{y_shift:+d}"

    if background_path:
        input_args = ["-loop", "1", "-i", background_path]
        filter_graph = (
            f"[0:v]scale={PREVIEW_EXPORT_WIDTH_PX}:{PREVIEW_EXPORT_HEIGHT_PX}:force_original_aspect_ratio=decrease,"
            f"pad={PREVIEW_EXPORT_WIDTH_PX}:{PREVIEW_EXPORT_HEIGHT_PX}:(ow-iw)/2:{y_pad_expr}:color=0x121722,"
            f"drawbox=x=0:y={overlay_y}:w={PREVIEW_EXPORT_WIDTH_PX}:h={overlay_h}:color=0x121722@0.90:t=fill,"
            f"drawtext=textfile='{_ffmpeg_filter_value(handle_file)}'{handle_font_clause}:"
            f"fontcolor=white:fontsize={handle_font_size}:line_spacing=8:x=74:y={handle_y},"
            f"drawtext=textfile='{_ffmpeg_filter_value(headline_file)}'{headline_font_clause}:"
            f"fontcolor=white:fontsize={font_size}:line_spacing={line_spacing}:x=72:y={headline_y}"
        )
    else:
        input_args = ["-f", "lavfi", "-i", f"color=c=#121722:s={PREVIEW_EXPORT_WIDTH_PX}x{PREVIEW_EXPORT_HEIGHT_PX}:d=1"]
        filter_graph = (
            f"drawtext=textfile='{_ffmpeg_filter_value(handle_file)}'{handle_font_clause}:"
            f"fontcolor=white:fontsize={handle_font_size}:line_spacing=8:x=74:y={handle_y},"
            f"drawtext=textfile='{_ffmpeg_filter_value(headline_file)}'{headline_font_clause}:"
            f"fontcolor=white:fontsize={font_size}:line_spacing={line_spacing}:x=72:y={headline_y}"
        )

    command = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        *input_args,
        "-frames:v",
        "1",
        "-vf",
        filter_graph,
        output_path,
    ]
    subprocess.run(command, check=True)


def _render_text_slide_png(
    output_path: str,
    tmp_dir: str,
    body_text: str,
    font_adjust_px: int = 0,
    include_link_cta: bool = False,
    link_cta_target: str = "more",
    link_cta_text: str = "",
) -> None:
    ffmpeg_path = _preview_ffmpeg_path()
    body_file = _write_preview_text_file(tmp_dir, os.path.basename(output_path) + ".txt", body_text, 26)
    cta_file = ""
    bold_font = _preview_font_path(bold=True)
    body_font_clause = f":fontfile='{_ffmpeg_filter_value(bold_font)}'" if bold_font else ""
    body_font_size = max(52, round((74 + int(font_adjust_px)) * PREVIEW_EXPORT_FONT_SCALE))
    body_y = round(78 * PREVIEW_EXPORT_SCALE)
    body_line_spacing = max(14, round(16 * PREVIEW_EXPORT_FONT_SCALE))

    filter_parts = [
        f"drawtext=textfile='{_ffmpeg_filter_value(body_file)}'{body_font_clause}:fontcolor=white:fontsize={body_font_size}:"
        f"line_spacing={body_line_spacing}:x=62:y={body_y}"
    ]
    if include_link_cta:
        cta_value = (link_cta_text or "").strip() or _slide_three_cta_text(link_cta_target, "")
        cta_file = _write_preview_text_file(tmp_dir, "slide3_cta.txt", cta_value, 28)
        cta_box_y = round(1380 * PREVIEW_EXPORT_SCALE)
        cta_box_h = round(88 * PREVIEW_EXPORT_SCALE)
        cta_text_y = round(1405 * PREVIEW_EXPORT_SCALE)
        cta_font_size = max(32, round(36 * PREVIEW_EXPORT_FONT_SCALE))
        filter_parts.extend(
            [
                f"drawbox=x=62:y={cta_box_y}:w=470:h={cta_box_h}:color=white@1.0:t=fill",
                f"drawtext=textfile='{_ffmpeg_filter_value(cta_file)}'{body_font_clause}:fontcolor=#121722:fontsize={cta_font_size}:"
                f"line_spacing=8:x=90:y={cta_text_y}",
            ]
        )

    command = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"color=c=#121722:s={PREVIEW_EXPORT_WIDTH_PX}x{PREVIEW_EXPORT_HEIGHT_PX}:d=1",
        "-frames:v",
        "1",
        "-vf",
        ",".join(filter_parts),
        output_path,
    ]
    subprocess.run(command, check=True)


def _upload_preview_pngs(
    row_num: int,
    username: str,
    handle_text: str,
    slide_text1: str,
    slide_text2: str,
    slide_text3: str,
    slide_text4: str,
    slide_text5: str,
    slide_text6: str,
    background_url: str,
    media_link: str = "",
    preview_folder_id: str = "",
    folder_base_name: str = "",
    source_filename: str = "",
    include_source_video: bool = True,
    slide_one_font_adjust: int = 0,
    slide_one_background_adjust: int = 0,
    slide_two_font_adjust: int = 0,
    slide_three_font_adjust: int = 0,
    slide_three_cta_target: str = "more",
    slide_three_cta_text: str = "",
) -> list[dict[str, str]]:
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not configured.")

    tmp_dir = tempfile.mkdtemp(prefix="workspace_previews_")
    uploaded: list[dict[str, str]] = []
    if not preview_folder_id or not folder_base_name:
        preview_folder_id, folder_base_name, resolved_source_filename = _ensure_preview_folder(
            row_num,
            username,
            handle_text,
            media_link,
        )
        if not source_filename:
            source_filename = resolved_source_filename
    safe_handle = (handle_text or username or f"row_{row_num}").strip()

    try:
        slides_to_render: list[tuple[str, callable, dict]] = []
        last_cta_slide_number = 3
        for candidate_slide_number, candidate_text in (
            (6, slide_text6),
            (5, slide_text5),
            (4, slide_text4),
            (3, slide_text3),
        ):
            if (candidate_text or "").strip():
                last_cta_slide_number = candidate_slide_number
                break
        if (slide_text1 or "").strip():
            slides_to_render.append(
                (
                    "slide1",
                    _render_slide_one_png,
                    {
                        "handle_text": safe_handle,
                        "headline": slide_text1,
                        "background_url": background_url,
                        "headline_font_adjust_px": slide_one_font_adjust,
                        "background_y_adjust_px": slide_one_background_adjust,
                    },
                )
            )
        if (slide_text2 or "").strip():
            slides_to_render.append(
                (
                    "slide2",
                    _render_text_slide_png,
                    {
                        "body_text": slide_text2,
                        "font_adjust_px": slide_two_font_adjust,
                        "include_link_cta": False,
                    },
                )
            )
        if (slide_text3 or "").strip():
            slides_to_render.append(
                (
                    "slide3",
                    _render_text_slide_png,
                    {
                        "body_text": slide_text3,
                        "font_adjust_px": slide_three_font_adjust,
                        "include_link_cta": last_cta_slide_number == 3,
                        "link_cta_target": slide_three_cta_target,
                        "link_cta_text": slide_three_cta_text,
                    },
                )
            )
        if (slide_text4 or "").strip():
            slides_to_render.append(
                (
                    "slide4",
                    _render_text_slide_png,
                    {
                        "body_text": slide_text4,
                        "font_adjust_px": slide_three_font_adjust,
                        "include_link_cta": last_cta_slide_number == 4,
                        "link_cta_target": slide_three_cta_target,
                        "link_cta_text": slide_three_cta_text,
                    },
                )
            )
        if (slide_text5 or "").strip():
            slides_to_render.append(
                (
                    "slide5",
                    _render_text_slide_png,
                    {
                        "body_text": slide_text5,
                        "font_adjust_px": slide_three_font_adjust,
                        "include_link_cta": last_cta_slide_number == 5,
                        "link_cta_target": slide_three_cta_target,
                        "link_cta_text": slide_three_cta_text,
                    },
                )
            )
        if (slide_text6 or "").strip():
            slides_to_render.append(
                (
                    "slide6",
                    _render_text_slide_png,
                    {
                        "body_text": slide_text6,
                        "font_adjust_px": slide_three_font_adjust,
                        "include_link_cta": last_cta_slide_number == 6,
                        "link_cta_target": slide_three_cta_target,
                        "link_cta_text": slide_three_cta_text,
                    },
                )
            )
        if not slides_to_render:
            raise ValueError("No slide preview text is available to export.")

        if media_link and include_source_video:
            copied_media_link = _copy_source_video_into_preview_folder(media_link, preview_folder_id, source_filename)
            uploaded.append(
                {
                    "label": "Source video",
                    "link": copied_media_link,
                }
            )

        for suffix, renderer, kwargs in slides_to_render:
            output_filename = f"{folder_base_name}_{suffix}.png"
            output_path = os.path.join(tmp_dir, output_filename)
            renderer(output_path=output_path, tmp_dir=tmp_dir, **kwargs)
            uploaded.append(
                {
                    "label": suffix.replace("slide", "Slide "),
                    "link": upload_to_drive(output_path, output_filename, preview_folder_id, overwrite=True),
                }
            )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return uploaded


def _segment_name(index: int) -> str:
    words = [
        "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
        "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
        "eighteen", "nineteen", "twenty", "twenty_one", "twenty_two", "twenty_three",
        "twenty_four", "twenty_five", "twenty_six", "twenty_seven", "twenty_eight",
        "twenty_nine", "thirty", "thirty_one", "thirty_two", "thirty_three",
        "thirty_four", "thirty_five", "thirty_six", "thirty_seven", "thirty_eight",
        "thirty_nine", "forty", "forty_one", "forty_two", "forty_three", "forty_four",
        "forty_five", "forty_six", "forty_seven", "forty_eight", "forty_nine", "fifty",
        "fifty_one", "fifty_two", "fifty_three", "fifty_four", "fifty_five", "fifty_six",
        "fifty_seven", "fifty_eight", "fifty_nine", "sixty",
    ]
    if 0 <= index < len(words):
        return words[index]
    return f"{index + 1:02d}"


def _ensure_preview_folder(row_num: int, username: str, handle_text: str, media_link: str) -> tuple[str, str, str]:
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not configured.")
    folder_base_name, source_filename = _preview_folder_base_name(username or handle_text, media_link, row_num)
    preview_folder_id = get_or_create_subfolder(GOOGLE_DRIVE_FOLDER_ID, folder_base_name)
    return preview_folder_id, folder_base_name, source_filename


def _copy_source_video_into_preview_folder(media_link: str, preview_folder_id: str, source_filename: str) -> str:
    if not media_link:
        return ""
    return copy_drive_file_to_folder(media_link, preview_folder_id, source_filename)


def _split_video_to_folder(local_video_path: str, output_dir: str, mode: str = "fill") -> list[str]:
    if mode == "fit":
        pad_w = "if(gte(iw/ih\\,4/5)\\,iw\\,trunc(ih*(4/5)/2)*2)"
        pad_h = "if(gte(iw/ih\\,4/5)\\,trunc(iw*(5/4)/2)*2\\,ih)"
        video_filter = f"pad={pad_w}:{pad_h}:(ow-iw)/2:(oh-ih)/2:black,scale=trunc(iw/2)*2:trunc(ih/2)*2"
    else:
        crop_width = "if(gte(iw/ih\\,4/5)\\,trunc(ih*(4/5)/2)*2\\,iw)"
        crop_height = "if(gte(iw/ih\\,4/5)\\,ih\\,trunc(iw/(4/5)/2)*2)"
        video_filter = f"crop={crop_width}:{crop_height}:(iw-ow)/2:(ih-oh)/2,scale=trunc(iw/2)*2:trunc(ih/2)*2"
    duration = _video_duration_seconds(local_video_path)
    if duration <= 0:
        raise RuntimeError("Could not determine video duration for splitting.")

    ffmpeg_path = _crop_ffmpeg_path()
    outputs: list[str] = []
    start_seconds = 0.0
    segment_index = 0
    while start_seconds < duration - 0.01:
        clip_duration = min(60.0, duration - start_seconds)
        suffix = "_fit" if mode == "fit" else ""
        output_path = os.path.join(output_dir, f"{_segment_name(segment_index)}{suffix}.mp4")
        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-ss",
            f"{start_seconds:.3f}",
            "-i",
            local_video_path,
            "-t",
            f"{clip_duration:.3f}",
            "-vf",
            video_filter,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            output_path,
        ]
        proc = subprocess.run(command, capture_output=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode(errors="replace"))
        outputs.append(output_path)
        start_seconds += 60.0
        segment_index += 1
    return outputs


def _upload_split_videos(media_link: str, preview_folder_id: str, mode: str = "fill") -> list[dict[str, str]]:
    if not media_link:
        return []
    metadata = get_drive_file_metadata(media_link)
    filename = (metadata.get("name") or "").strip()
    if not filename:
        raise ValueError("Could not determine the source video filename from Drive.")

    tmp_dir = tempfile.mkdtemp(prefix="workspace_splits_")
    try:
        local_video_path = os.path.join(tmp_dir, filename)
        download_drive_file(media_link, local_video_path)
        split_dir = os.path.join(tmp_dir, "segments")
        os.makedirs(split_dir, exist_ok=True)
        segment_paths = _split_video_to_folder(local_video_path, split_dir, mode=mode)
        uploaded: list[dict[str, str]] = []
        for segment_path in segment_paths:
            segment_filename = os.path.basename(segment_path)
            uploaded.append(
                {
                    "label": f"Split {os.path.splitext(segment_filename)[0]}",
                    "link": upload_to_drive(segment_path, segment_filename, preview_folder_id),
                }
            )
        return uploaded
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


_SLIDE_JSON_KEYS = {"name", "text1", "text2", "text3", "text4", "text5", "text6", "slide_cta", "caption", "generated_caption"}


def _build_create_post_slide_prompt() -> str:
    """Return the full slide + caption prompt with a blank placeholder row."""
    row_block = "\n".join([
        "ROW new",
        "username: (fill in)",
        "media_type: post",
        "speaker_name: (none)",
        "transcript:",
        "(paste your content, transcript, or talking points here)",
        "original_caption:",
        "(none)",
        "caption_context:",
        "(none)",
    ])

    caption_instructions = (
        "For each object also include a \"generated_caption\" field. "
        "Write a short, clear social post under 1300 characters using exactly two simple paragraphs.\n\n"
        "Never write in first person. Do not use I, me, my, mine, we, us, our, or ours "
        "unless inside a direct quote. Stay in third person. "
        "If the source material is written in first person, rewrite it entirely in third person — "
        "never echo or adopt the speaker's voice as your own. "
        "If the speaker cannot be clearly identified, skip naming them and describe the content or information directly.\n\n"
        "The first paragraph must be 250 characters or fewer and serve as the most important summary. "
        "It must include all hashtags. Use 3 to 5 relevant hashtags total. "
        "Prioritize hashtags for the main people the post is about, then one single-word subject hashtag "
        "for trending news discovery, then any remaining relevant tags. "
        "Replace the normal word or phrase in the sentence with the hashtag version — for example use "
        "#DonaldTrump in the sentence instead of writing the name normally. "
        "Do not add a separate hashtag-only line at the end.\n\n"
        "The second paragraph adds context using verified facts, dates, and numbers when relevant. "
        "Include direct quotes from the transcript when available. Verify names and quotes carefully. "
        "Any hashtag in the caption body counts toward the 3–5 total. "
        "Avoid speculation, flourish, links, or references to Trump's current office status.\n\n"
        "Do not refer to the source as a transcript, clip, speech, interview, or video unless explicitly certain. "
        "Write as if describing the underlying event or claim directly.\n"
    )

    instructions = (
        "Return ONLY valid JSON as an array. No markdown, no commentary outside JSON.\n\n"
        "Each object must include: row_number, name, quote, text1, text2, text3, generated_caption\n\n"
        "Mandatory research step before writing:\n"
        "* For every row with a current event, public figure, legal case, government action, investigation, company, or breaking news claim, search online for reliable context before writing.\n"
        "* Use search to verify names, dates, charges, court rulings, dollar amounts, locations, and status of claims.\n"
        "* Prefer primary sources, Reuters, AP, local public radio, court records, official statements, and reputable outlets.\n"
        "* Do not add unverified claims. If context cannot be verified, stay close to the supplied transcript and caption.\n"
        "* Never cite sources in the JSON output. Use research only to improve accuracy and context.\n\n"
        "Rules:\n"
        "* Keep row_number exactly as shown\n"
        "* No markdown, no commentary outside JSON\n"
        "* Plain straight double quotes only, no smart quotes\n"
        + pipeline_caption_ops.carousel_slide_rules()
        + "Caption rules:\n"
        + caption_instructions
        + "\nQuality check before final output:\n"
        "* Confirm every object has exactly row_number, name, quote, text1, text2, text3, generated_caption\n"
        "* Confirm character limits are respected\n"
        "* Confirm text is not too short when more verified context exists\n"
        "* Confirm no field repeats another field\n"
        "* Confirm no hashtags, em dashes, smart quotes, markdown, newlines, or source citations appear in slide fields\n"
        "* Confirm every quote is verbatim from supplied text\n\n"
        "Output format example:\n"
        "[\n"
        "  {\n"
        '    "row_number": "new",\n'
        '    "name": "nowthis",\n'
        '    "quote": "We could abolish medical debt 10 times over.",\n'
        '    "text1": "The line frames the central contrast: billions flowing into military spending while families still face unpaid medical bills, coverage gaps, and debt that can follow them for years.",\n'
        '    "text2": "The argument connects military funding, healthcare costs, Medicaid pressure, and lobbying money into one political charge: Washington keeps finding money for war while ordinary people are told basic care is too expensive.",\n'
        '    "text3": "The fallout is political as much as financial. The carousel should leave viewers with the real stakes: who benefits from federal spending choices, who absorbs the cost, and why healthcare debt remains unresolved.",\n'
        '    "generated_caption": "#BernieSanders says the U.S. could abolish medical debt 10 times over with what it spends on the military. The contrast is stark and personal for millions of Americans still carrying unpaid bills.\\n\\nSanders made the argument during a Senate speech, pointing to Medicaid cuts and rising premiums as Congress approved another round of defense spending. \\"We keep finding money for war,\\" he said, \\"while people can\'t afford insulin.\\""\n'
        "  }\n"
        "]\n"
    )
    return instructions + "\n\n" + row_block


def _parse_slide_json(prompt: str) -> dict | None:
    """Return a slide dict if prompt is valid slide JSON (object or array), else None."""
    stripped = (prompt or "").strip()
    starts_json = stripped.startswith("{") or stripped.startswith("[")
    ends_json = stripped.endswith("}") or stripped.endswith("]")
    if not (starts_json and ends_json):
        return None
    try:
        data = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        try:
            normalized = _normalize_slide_paste(stripped)
            data = json.loads(normalized)
        except Exception:
            return None
    if isinstance(data, list):
        data = data[0] if data else None
    if not isinstance(data, dict) or not any(k in data for k in _SLIDE_JSON_KEYS):
        return None
    return data


def _create_post_from_prompt(prompt: str, custom_link: str, uploaded_file, speaker_name: str = "") -> int:
    """Append a new row from a manual prompt, upload media if provided, and generate a caption."""
    media_link = ""
    thumbnail_link = ""
    media_type = ""
    transcript = ""

    slide_data = _parse_slide_json(prompt)
    top_comment = _encode_top_comment(_build_link_cta(custom_link), pinned=False) if custom_link else ""

    # Derive default name from link domain (e.g. "newyorktimes.com") for non-Instagram URLs.
    link_domain = ""
    if custom_link and not _is_instagram_url(custom_link):
        try:
            _parsed = urlparse(custom_link.strip())
            _host = _parsed.netloc or _parsed.path
            link_domain = _host.removeprefix("www.").split("/")[0].strip().lower()
        except Exception:
            pass

    if uploaded_file is not None:
        file_name = uploaded_file.name or "upload"
        ext = os.path.splitext(file_name)[-1].lower()
        is_video = ext in {".mp4", ".mov"}
        media_type = "reel" if is_video else "photo"
        if not GOOGLE_DRIVE_FOLDER_ID:
            raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID is not configured.")
        tmp_dir = tempfile.mkdtemp(prefix="workspace_create_post_")
        try:
            local_path = os.path.join(tmp_dir, file_name)
            with open(local_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            media_link = upload_to_drive(local_path, file_name, GOOGLE_DRIVE_FOLDER_ID)
            if not is_video:
                thumbnail_link = media_link
            else:
                transcript = prompt
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if link_domain and not media_type:
        media_type = "article"

    if slide_data:
        caption_context = slide_data.get("generated_caption") or slide_data.get("caption") or ""
        _row_name = slide_data.get("name", "").strip() or link_domain
        append_manual_post_row(GOOGLE_SHEET_ID, {
            "url": custom_link,
            "caption_context": caption_context,
            "original_caption": caption_context,
            "transcript": transcript,
            "source_username": _row_name,
            "speaker_name": speaker_name,
            "media_type": media_type,
            "media_link": media_link,
            "thumbnail_link": thumbnail_link,
            "top_comment": top_comment,
            "status": "ingested",
            "name": _row_name,
            "quote": (slide_data.get("quote") or "").strip().strip('"').strip("'").rstrip("."),
            "text1": slide_data.get("text1", ""),
            "text2": slide_data.get("text2", ""),
            "text3": slide_data.get("text3", ""),
            "text4": slide_data.get("text4", ""),
            "text5": slide_data.get("text5", ""),
            "text6": slide_data.get("text6", ""),
            "slide_cta": slide_data.get("slide_cta", ""),
        })
    else:
        append_manual_post_row(GOOGLE_SHEET_ID, {
            "url": custom_link,
            "caption_context": prompt,
            "original_caption": prompt,
            "transcript": transcript,
            "source_username": link_domain,
            "speaker_name": speaker_name,
            "media_type": media_type,
            "media_link": media_link,
            "thumbnail_link": thumbnail_link,
            "top_comment": top_comment,
            "status": "ingested",
            "name": link_domain,
        })

    all_rows = _run_with_sheet_quota_countdown(
        lambda: get_all_rows(GOOGLE_SHEET_ID),
        "Create post paused (sheet quota):",
    )
    if not all_rows:
        raise RuntimeError("Could not retrieve rows after creating post.")
    new_row = all_rows[-1]
    row_num = new_row["row_number"]

    try:
        if row_ready_for_caption(new_row):
            caption = generate_row_caption(new_row)
            update_caption(GOOGLE_SHEET_ID, row_num, caption, "done")
        else:
            update_status(GOOGLE_SHEET_ID, row_num, "done")
    except Exception:
        update_status(GOOGLE_SHEET_ID, row_num, "ingested")

    return row_num


def _preview_folder_has_splits(folder_id: str) -> bool:
    """Return True if the preview folder already contains any mp4 files."""
    service = _get_service()
    query = f"'{folder_id}' in parents and trashed = false and mimeType = 'video/mp4'"
    try:
        result = (
            service.files()
            .list(
                q=query,
                fields="files(id)",
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        return bool(result.get("files"))
    except Exception:
        return False


def _is_https_url(value: str) -> bool:
    parsed = urlparse((value or "").strip())
    return parsed.scheme == "https" and bool(parsed.netloc)


def _clean_public_url(link: str) -> str:
    link = _strip_invisible_chars((link or "").strip())
    parsed = urlparse(link)
    if not parsed.scheme or not parsed.netloc:
        return link
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _build_link_cta(link: str) -> str:
    return f"Comment LINK (on instagram) and we will DM you the link to {_clean_public_url(link)}"


def _build_read_cta(link: str) -> str:
    return f"Comment LINK (on instagram) and we will DM you the link to {_clean_public_url(link)}"


def _build_watch_cta(username: str, link: str) -> str:
    cleaned_username = (username or "").strip().lstrip("@")
    cleaned_link = _clean_public_url(link)
    destination = f"@{cleaned_username} {cleaned_link}" if cleaned_username else cleaned_link
    return f"Comment LINK (on instagram) and we will DM you the link to {destination}"


def _slide_three_cta_text(option: str, top_comment: str) -> str:
    normalized = (option or "more").strip().lower()
    cta_text_by_option = {
        "article": "Say LINK for the article",
        "substack": "Say LINK for the Substack",
        "petition": "Say LINK for the petition",
        "video": "Say LINK for the video",
        "more": "Say LINK for more",
        "custom link": "Say LINK for more",
    }
    if normalized in cta_text_by_option:
        return cta_text_by_option[normalized]
    # Non-standard value — treat as custom button text stored directly in the column
    cleaned = (option or "").strip()
    return cleaned if cleaned else "Say LINK for more"


def _save_slide_three_cta_choice(row_number: int, state_key: str, option: str) -> None:
    st.session_state[state_key] = option
    update_slide_cta_option(GOOGLE_SHEET_ID, row_number, option)


def _append_top_comment(existing: str, addition: str) -> str:
    existing = (existing or "").strip()
    addition = (addition or "").strip()
    if not existing:
        return addition
    if not addition or addition in existing.split("\n\n"):
        return existing
    return f"{existing}\n\n{addition}"


def _encode_top_comment(value: str, pinned: bool = False) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    return f"{PINNED_TOP_COMMENT_PREFIX}{cleaned}" if pinned else cleaned


def _decode_top_comment(value: str) -> tuple[str, bool]:
    cleaned = (value or "").strip()
    if cleaned.startswith(PINNED_TOP_COMMENT_PREFIX):
        return cleaned[len(PINNED_TOP_COMMENT_PREFIX):].strip(), True
    return cleaned, False


def _close_workspace_menu(row: dict) -> None:
    nonce_key = _workspace_key(row, "menu_nonce")
    st.session_state[nonce_key] = st.session_state.get(nonce_key, 0) + 1
    st.session_state[_workspace_key(row, "link_editor_open")] = False
    st.session_state.pop("workspace_link_dialog_row", None)


def _close_workspace_link_dialog(row: dict) -> None:
    st.session_state.pop("workspace_link_dialog_row", None)
    st.session_state.pop(_workspace_key(row, "link_source"), None)
    st.session_state.pop(_workspace_key(row, "link_url"), None)
    st.session_state.pop(_workspace_key(row, "link_comment"), None)


def _close_workspace_thumbnail_dialog(row: dict) -> None:
    st.session_state.pop("workspace_thumbnail_dialog_row", None)
    st.session_state.pop(_workspace_key(row, "thumbnail_upload"), None)
    st.session_state.pop(_workspace_key(row, "thumbnail_upload_token"), None)


def _dismiss_workspace_link_dialog() -> None:
    st.session_state.pop("workspace_link_dialog_row", None)


def _dismiss_workspace_thumbnail_dialog() -> None:
    st.session_state.pop("workspace_thumbnail_dialog_row", None)


def _apply_top_comment_to_caption(
    row: dict,
    row_num: int,
    speaker_name: str,
    top_comment: str,
) -> bool:
    """Save the Top Comment and, if a caption already exists, splice the CTA into it.

    Returns True if the generated caption itself was updated, False if only
    the Top Comment field was saved (e.g. no caption exists yet to update).
    """
    current_context = st.session_state.get(_workspace_key(row, "context"), row.get("Caption Context", "")).strip()
    current_speaker = st.session_state.get(_workspace_speaker_key(row), speaker_name).strip()
    current_hashtags = st.session_state.get(_workspace_key(row, "hashtags"), row.get("Required Hashtags", "")).strip()
    updated_row = dict(row)
    updated_row["Caption Context"] = current_context
    updated_row["Speaker Name"] = current_speaker
    updated_row["Required Hashtags"] = current_hashtags
    updated_row["Top Comment"] = top_comment
    current_status = (row.get("Status") or "").strip() or "done"
    existing_caption = (row.get("Generated Caption") or "").strip()
    previous_top_comment = (row.get("Top Comment") or "").strip()
    clean_top_comment, pin_top_comment = _decode_top_comment(top_comment)
    if existing_caption:
        caption = existing_caption
        for removable in (previous_top_comment, top_comment):
            removable_text, _ = _decode_top_comment(removable)
            if removable_text:
                caption = _strip_top_comment_paragraphs(caption, removable_text)

        media_type = (row.get("Media Type") or "").strip().lower()
        if media_type != "article" and "\n\n--\n\n" in caption:
            before_divider, after_divider = caption.split("\n\n--\n\n", 1)
            before_divider = before_divider.strip()
            after_divider = after_divider.strip()
            if clean_top_comment:
                if pin_top_comment:
                    before_divider = f"{clean_top_comment}\n\n{before_divider}".strip()
                else:
                    before_divider = f"{before_divider}\n\n{clean_top_comment}".strip()
            caption = f"{before_divider}\n\n--\n\n{after_divider}".strip()
        elif clean_top_comment:
            footer_text = DEFAULT_POST_FOOTER.strip()
            if footer_text and footer_text in caption:
                body, _, trailing = caption.rpartition(footer_text)
                body = body.strip()
                trailing = trailing.strip()
                if pin_top_comment:
                    body = f"{clean_top_comment}\n\n{body}".strip()
                else:
                    body = f"{body}\n\n{clean_top_comment}".strip()
                caption = f"{body}\n\n{footer_text}{trailing}".strip()
            else:
                if pin_top_comment:
                    caption = f"{clean_top_comment}\n\n{caption}".strip()
                else:
                    caption = f"{caption}\n\n{clean_top_comment}".strip()
        caption = _ensure_required_hashtags_text(caption, current_hashtags)
    else:
        caption = ""

    if caption and update_caption_and_metadata is not None:
        update_caption_and_metadata(
            GOOGLE_SHEET_ID,
            row_num,
            caption,
            current_status,
            current_context,
            current_speaker,
            current_hashtags,
            top_comment,
            "",
        )
    else:
        update_metadata(
            GOOGLE_SHEET_ID,
            row_num,
            current_context,
            current_speaker,
            current_hashtags,
            top_comment,
            "",
        )
        if caption:
            update_caption(GOOGLE_SHEET_ID, row_num, caption, current_status)
    st.session_state[_workspace_key(row, "top")] = top_comment
    return bool(caption)


def _current_row_caption_inputs(row: dict) -> dict:
    current_context = st.session_state.get(
        _workspace_key(row, "context"),
        row.get("Caption Context", ""),
    ).strip()
    if not current_context and _is_article_url((row.get("Instagram URL") or "").strip()):
        current_context = _cell_text(row.get("Original Caption")).strip()
    current_speaker = st.session_state.get(
        _workspace_speaker_key(row),
        row.get("Speaker Name", ""),
    ).strip()
    current_hashtags = st.session_state.get(
        _workspace_key(row, "hashtags"),
        row.get("Required Hashtags", ""),
    ).strip()
    current_top = st.session_state.get(
        _workspace_key(row, "top"),
        row.get("Top Comment", ""),
    ).strip()
    current_top, _ = _decode_top_comment(current_top)
    url = (row.get("Instagram URL") or "").strip()
    current_username = (row.get("Source Username") or "").strip()

    if not current_top and _is_instagram_url(url):
        current_top = _build_watch_cta(current_username or current_speaker, url)
    elif not current_top and _is_article_url(url):
        current_top = _build_read_cta(url)

    return {
        "Caption Context": current_context,
        "Speaker Name": current_speaker,
        "Required Hashtags": current_hashtags,
        "Top Comment": current_top,
    }


def _handle_speaker_name_change(row: dict) -> None:
    speaker_key = _workspace_speaker_key(row)
    new_name = _cell_text(st.session_state.get(speaker_key, "")).strip()
    saved_name = _cell_text(row.get("Speaker Name")).strip()
    if new_name == saved_name:
        return
    try:
        if update_speaker_names_batch is None:
            raise RuntimeError("Batch speaker-name updates are not supported in this build.")
        update_speaker_names_batch(GOOGLE_SHEET_ID, {row["row_number"]: new_name})
    except Exception as e:
        st.session_state["workspace_error"] = f"Could not save name: {describe_error(e)}"
    else:
        st.session_state["workspace_success"] = "Saved speaker name."
    _rerun_workspace("Edit")


def _fundraising_preset_map() -> dict[str, str]:
    presets = get_fundraising_links(GOOGLE_SHEET_ID)
    mapping: dict[str, str] = {"Custom": ""}
    for preset in presets:
        label = (preset.get("label") or "").strip()
        top_comment = (preset.get("link") or "").strip()
        if label and top_comment and label not in mapping:
            mapping[label] = top_comment
    return mapping


@st.dialog("Update Instagram cookies")
def _render_cookies_dialog() -> None:
    st.markdown(
        "**How to get a fresh cookies file (desktop Chrome):**\n"
        "1. Install the **Get cookies.txt LOCALLY** Chrome extension\n"
        "2. Go to instagram.com and make sure you're logged in\n"
        "3. Click the extension → Export\n"
        "4. Open the downloaded file, select all, copy\n"
        "5. Paste below and save"
    )
    cookies_text = st.text_area(
        "Cookies file contents",
        height=200,
        placeholder="# Netscape HTTP Cookie File\n# https://curl.se/rfc/cookie_spec.html\n...",
        label_visibility="collapsed",
    )
    if st.button("Save to Secret Manager", type="primary", use_container_width=True):
        if not cookies_text.strip():
            st.error("Paste the cookies file contents before saving.")
        elif "instagram.com" not in cookies_text:
            st.error("This doesn't look like an instagram.com cookies file.")
        elif not SECRET_MANAGER_PROJECT_ID:
            st.error("SECRET_MANAGER_PROJECT_ID is not configured.")
        else:
            try:
                from google.cloud import secretmanager as _sm
                client = _secret_manager_client()
                if client is None:
                    st.error("Could not connect to Secret Manager. Check service account credentials.")
                else:
                    secret_name = SECRET_MANAGER_SECRET_NAMES.get("INSTAGRAM_COOKIES", "instagram-cookies")
                    if isinstance(secret_name, tuple):
                        secret_name = secret_name[0]
                    parent = f"projects/{SECRET_MANAGER_PROJECT_ID}/secrets/{secret_name}"
                    payload = _sm.SecretPayload(data=cookies_text.strip().encode("utf-8"))
                    client.add_secret_version(request={"parent": parent, "payload": payload})
                    st.success("Saved. New session will be used on the next ingest.")
            except Exception as e:
                st.error(f"Failed: {e}")


@st.dialog("Video Post", width="large", on_dismiss=_dismiss_video_post_dialog)
def _render_video_post_dialog() -> None:
    uploaded = st.file_uploader(
        "Upload video",
        type=["mp4", "mov"],
        key="workspace_video_post_upload",
        accept_multiple_files=False,
    )
    speaker_name = st.text_input(
        "Speaker name",
        key="workspace_video_post_speaker",
        placeholder="e.g. Bernie Sanders",
    ).strip()

    if st.button(
        "Create Post",
        key="workspace_video_post_submit",
        type="primary",
        width="stretch",
        disabled=not (uploaded and speaker_name),
    ):
        if not GOOGLE_DRIVE_FOLDER_ID:
            st.error("GOOGLE_DRIVE_FOLDER_ID is not configured.")
            return
        tmp_dir = tempfile.mkdtemp(prefix="workspace_video_post_")
        try:
            suffix = os.path.splitext(uploaded.name)[-1].lower() or ".mp4"
            src_path = os.path.join(tmp_dir, f"source{suffix}")
            with open(src_path, "wb") as f:
                f.write(uploaded.getbuffer())

            with st.spinner("Transcribing…"):
                transcript = (transcribe_video(src_path) or "").strip()

            file_name = uploaded.name or f"video{suffix}"
            with st.spinner("Uploading to Drive…"):
                media_link = upload_to_drive(src_path, file_name, GOOGLE_DRIVE_FOLDER_ID)

            thumbnail_link = ""
            try:
                duration_s = _video_duration_seconds(src_path)
                capture_s = min(5.0, max(0.0, duration_s - 0.25)) if duration_s > 0 else 0.0
                thumb_name = f"{os.path.splitext(file_name)[0]}_thumb_{int(round(capture_s))}s.jpg"
                thumb_path = os.path.join(tmp_dir, thumb_name)
                subprocess.run(
                    [
                        _crop_ffmpeg_path(), "-hide_banner", "-loglevel", "error",
                        "-y", "-ss", f"{capture_s:.3f}", "-i", src_path,
                        "-frames:v", "1", thumb_path,
                    ],
                    check=True,
                )
                screenshots_folder_id = get_or_create_subfolder(
                    GOOGLE_DRIVE_FOLDER_ID, GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER
                )
                thumbnail_link = upload_to_drive(thumb_path, thumb_name, screenshots_folder_id)
            except Exception:
                pass

            append_manual_post_row(GOOGLE_SHEET_ID, {
                "url": "",
                "caption_context": transcript,
                "original_caption": "",
                "transcript": transcript,
                "source_username": speaker_name,
                "speaker_name": speaker_name,
                "media_type": "reel",
                "media_link": media_link,
                "thumbnail_link": thumbnail_link,
                "top_comment": "",
                "status": "ingested",
                "name": speaker_name,
            })

            all_rows = _run_with_sheet_quota_countdown(
                lambda: get_all_rows(GOOGLE_SHEET_ID),
                "Video post paused (sheet quota):",
            )
            new_row = all_rows[-1] if all_rows else None
            row_num = new_row["row_number"] if new_row else None

            if row_num and new_row:
                with st.spinner("Generating caption…"):
                    try:
                        if row_ready_for_caption(new_row):
                            caption = generate_row_caption(new_row)
                            update_caption(GOOGLE_SHEET_ID, row_num, caption, "done")
                        else:
                            update_status(GOOGLE_SHEET_ID, row_num, "done")
                    except Exception:
                        update_status(GOOGLE_SHEET_ID, row_num, "done")

            if row_num and media_link:
                with st.spinner("Cropping and splitting video…"):
                    username_clean = re.sub(r"[^\w\-]", "_", speaker_name.lower())
                    preview_folder_id, _, _ = _ensure_preview_folder(
                        row_num, username_clean, speaker_name, media_link
                    )
                    seg_dir = os.path.join(tmp_dir, "segments")
                    os.makedirs(seg_dir, exist_ok=True)
                    segment_paths = _split_video_to_folder(src_path, seg_dir, mode="fill")
                    for seg_path in segment_paths:
                        upload_to_drive(seg_path, os.path.basename(seg_path), preview_folder_id)

            _close_video_post_dialog(clear_inputs=True)
            if row_num:
                st.session_state["workspace_home_notice"] = f"Video post created as row {row_num}."
                st.session_state["workspace_selected_row_num"] = row_num
                st.query_params["workspace_row"] = str(row_num)
            _rerun_workspace("Home")

        except Exception as e:
            st.error(f"Could not create video post: {describe_error(e)}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if st.button("Cancel", key="workspace_video_post_cancel", width="stretch"):
        _close_video_post_dialog(clear_inputs=True)
        _rerun_workspace("Home")


@st.dialog("Election Post", width="large", on_dismiss=_dismiss_election_post_dialog)
def _render_election_post_dialog() -> None:
    candidate_input = st.text_input(
        "Candidates",
        key="workspace_election_post_candidates",
        placeholder="e.g. Jane Smith, John Doe",
    ).strip()

    if st.button(
        "Build Prompt",
        key="workspace_election_post_build",
        type="primary",
        width="stretch",
        disabled=not candidate_input,
    ):
        try:
            with st.spinner("Resolving race…"):
                candidate_names = _extract_candidate_names_from_input(candidate_input)
                candidate_result = _resolve_candidate_comparison(candidate_names)
            if candidate_result.get("could_not_resolve"):
                st.session_state["workspace_election_post_error"] = (
                    "Could not resolve a clear election from those names. "
                    "Try adding the office or state (e.g. 'Jane Smith Colorado Senate')."
                )
            else:
                st.session_state["workspace_election_post_resolved"] = candidate_result
        except Exception as e:
            st.session_state["workspace_election_post_error"] = describe_error(e)
        st.rerun()

    error = st.session_state.pop("workspace_election_post_error", None)
    if error:
        st.error(error)

    resolved = st.session_state.get("workspace_election_post_resolved")
    if resolved:
        race_groups = resolved.get("race_groups") or []
        if race_groups:
            st.markdown("**Resolved race:**")
            for group in race_groups:
                g_names = ", ".join(
                    _cell_text(n).strip() for n in (group.get("candidate_names") or [])
                    if _cell_text(n).strip()
                )
                parts = [
                    _cell_text(group.get("race_name")).strip(),
                    _cell_text(group.get("election_date")).strip(),
                ]
                st.markdown(f"- {g_names}: {', '.join(p for p in parts if p)}")
        elif not resolved.get("could_not_resolve"):
            names_str = ", ".join(
                _cell_text(n).strip() for n in (resolved.get("candidate_names") or [])
                if _cell_text(n).strip()
            )
            st.markdown(
                f"**Resolved:** {names_str}, "
                f"{resolved.get('race_name', '')}, "
                f"{resolved.get('election_date', '')}"
            )

        json_paste = st.text_area(
            "Paste JSON result from ChatGPT",
            key="workspace_election_post_json_paste",
            height=150,
            placeholder='{"name": "...", "quote": "...", "text1": "...", ...}',
        ).strip()
        if st.button(
            "Create Post",
            key="workspace_election_post_create",
            type="primary",
            width="stretch",
            disabled=not json_paste,
        ):
            try:
                slide_data = _parse_slide_json(json_paste)
                if not slide_data:
                    st.error("That doesn't look like valid slide JSON. Paste the full JSON object from ChatGPT.")
                else:
                    source_url = _cell_text(slide_data.get("source_url", "")).strip()
                    with st.spinner("Creating post…"):
                        row_num = _create_post_from_prompt(json_paste, source_url, None)
                    _close_election_post_dialog(clear_inputs=True)
                    st.session_state["workspace_home_notice"] = f"Election post created as row {row_num}."
                    st.session_state["workspace_selected_row_num"] = row_num
                    st.query_params["workspace_row"] = str(row_num)
                    _rerun_workspace("Home")
            except Exception as e:
                st.error(f"Could not create post: {describe_error(e)}")

        prompt_text = _build_election_post_prompt(resolved)
        st.caption("Copy this prompt into ChatGPT:")
        st.code(prompt_text, language=None)

    if st.button("Cancel", key="workspace_election_post_cancel", width="stretch"):
        _close_election_post_dialog(clear_inputs=True)
        _rerun_workspace("Home")


@st.dialog("Workspace action", on_dismiss=_dismiss_workspace_home_action_dialog)
def _render_workspace_home_action_dialog() -> None:
    mode = st.session_state.get("workspace_home_action_dialog", "").strip()
    if not mode:
        return

    mode_help = {
        "Process this": "Add the link as a new sheet row, download media, ingest metadata, generate the transcript/caption, and build slide text in one shot.",
        "Generate headline": "Pull source text from an Instagram post or article link, then return three headline options plus a footered caption.",
        "Caption this": "Generate a caption directly from an Instagram post or article link using the selected hashtag preset.",
        "Process as Candidate Article": "Paste the full article body to generate article-based slides and a caption footer.",
        "Create a Post": "Write a prompt and optionally attach a link or media file to create a new post row with a generated caption.",
    }

    st.caption(mode)
    if mode in mode_help:
        st.caption(mode_help[mode])

    selected_org_hashtag = ""
    if mode not in {"Create a Post", "Crop Video"}:
        default_link = _clean_home_links()[0] if _clean_home_links() else ""
        if "workspace_home_dialog_link" not in st.session_state:
            st.session_state["workspace_home_dialog_link"] = default_link
        if "workspace_home_dialog_org_hashtag" not in st.session_state:
            st.session_state["workspace_home_dialog_org_hashtag"] = st.session_state.get("workspace_org_hashtag", "")

        st.text_input(
            _workspace_home_link_label(mode),
            key="workspace_home_dialog_link",
            placeholder=_workspace_home_link_placeholder(mode),
        )

        if _mode_uses_org_hashtag(mode):
            selected_org_hashtag = st.selectbox(
                "Apply organization hashtag",
                ORG_HASHTAG_OPTIONS,
                index=(
                    ORG_HASHTAG_OPTIONS.index(st.session_state["workspace_home_dialog_org_hashtag"])
                    if st.session_state["workspace_home_dialog_org_hashtag"] in ORG_HASHTAG_OPTIONS
                    else 0
                ),
                key="workspace_home_dialog_org_hashtag",
            )

    if mode == "Process as Candidate Article":
        step = int(st.session_state.get("workspace_home_candidate_article_step", 1) or 1)
        substack_url = _cell_text(st.session_state.get("workspace_home_dialog_link", "")).strip()

        if st.button(
            "Process as Candidate Article",
            key="workspace_home_candidate_article_start",
            type="primary",
            width="stretch",
            disabled=not substack_url,
        ):
            st.session_state["workspace_home_candidate_article_step"] = 2
            st.session_state.pop("workspace_home_candidate_article_error", None)
            st.session_state.pop("workspace_home_candidate_article_result", None)
            _rerun_workspace("Home")

        step = int(st.session_state.get("workspace_home_candidate_article_step", 1) or 1)
        if step >= 2:
            st.divider()
            st.caption("Since we can't read the article directly from the Substack URL, paste the full article text here so we can generate the Instagram caption from it.")
            article_body = st.text_area(
                "Paste the article body",
                key="workspace_home_candidate_article_body",
                height=420,
            ).strip()
            generate_disabled = (
                not substack_url
                or not article_body
                or bool(st.session_state.get("workspace_home_candidate_article_generating"))
            )
            if st.button(
                "Generate Caption",
                key="workspace_home_candidate_article_generate",
                type="primary",
                width="stretch",
                disabled=generate_disabled,
            ):
                st.session_state["workspace_home_candidate_article_generating"] = True
                st.session_state.pop("workspace_home_candidate_article_error", None)
                try:
                    with st.spinner("Generating caption..."):
                        generated_payload = _call_openai_candidate_article(article_body, substack_url)
                except Exception as e:
                    st.session_state["workspace_home_candidate_article_error"] = (
                        "Could not generate the caption. "
                        f"{describe_error(e)}"
                    )
                    st.session_state.pop("workspace_home_candidate_article_result", None)
                    st.session_state["workspace_home_candidate_article_step"] = 2
                else:
                    generated_payload["substack_url"] = substack_url
                    st.session_state["workspace_home_candidate_article_result"] = generated_payload
                    st.session_state["workspace_home_candidate_article_step"] = 3
                finally:
                    st.session_state["workspace_home_candidate_article_generating"] = False
                _rerun_workspace("Home")

            candidate_article_error = _cell_text(
                st.session_state.get("workspace_home_candidate_article_error", "")
            ).strip()
            candidate_article_result = st.session_state.get("workspace_home_candidate_article_result")
            if candidate_article_error:
                st.error(candidate_article_error)
                if st.button(
                    "Retry",
                    key="workspace_home_candidate_article_retry",
                    width="stretch",
                    disabled=not substack_url or not article_body,
                ):
                    st.session_state["workspace_home_candidate_article_generating"] = True
                    st.session_state.pop("workspace_home_candidate_article_error", None)
                    try:
                        with st.spinner("Generating caption..."):
                            generated_payload = _call_openai_candidate_article(article_body, substack_url)
                    except Exception as e:
                        st.session_state["workspace_home_candidate_article_error"] = (
                            "Could not generate the caption. "
                            f"{describe_error(e)}"
                        )
                        st.session_state.pop("workspace_home_candidate_article_result", None)
                        st.session_state["workspace_home_candidate_article_step"] = 2
                    else:
                        generated_payload["substack_url"] = substack_url
                        st.session_state["workspace_home_candidate_article_result"] = generated_payload
                        st.session_state["workspace_home_candidate_article_step"] = 3
                    finally:
                        st.session_state["workspace_home_candidate_article_generating"] = False
                    _rerun_workspace("Home")

            if candidate_article_result:
                st.divider()
                st.markdown("**Generated Output**")
                text1 = _cell_text(candidate_article_result.get("text1")).strip()
                text2 = _cell_text(candidate_article_result.get("text2")).strip()
                text3 = _cell_text(candidate_article_result.get("text3")).strip()
                caption_text = _build_candidate_article_caption(
                    _cell_text(candidate_article_result.get("generated_caption")).strip()
                )
                copy_all_text = (
                    f"SLIDE 1:\n{text1}\n\n"
                    f"SLIDE 2:\n{text2}\n\n"
                    f"SLIDE 3:\n{text3}\n\n"
                    f"CAPTION:\n{caption_text}"
                )
                _render_candidate_output_card("Slide 1", text1, "workspace_home_candidate_article_slide1")
                _render_candidate_output_card("Slide 2", text2, "workspace_home_candidate_article_slide2")
                _render_candidate_output_card("Slide 3", text3, "workspace_home_candidate_article_slide3")
                _render_candidate_output_card("Generated Caption", caption_text, "workspace_home_candidate_article_caption")
                st.html(_copy_button_html("Copy All", copy_all_text, "workspace_home_candidate_article_copy_all", primary=True))
    elif mode == "Crop Video":
        uploaded = st.file_uploader(
            "Upload video",
            type=["mp4", "mov", "avi", "mkv", "m4v"],
            key="workspace_crop_video_upload",
        )
        mode_label = st.radio(
            "Format",
            ["Crop to 4:5 (fill frame)", "Fit into 4:5 (letterbox)"],
            horizontal=True,
            key="workspace_crop_video_mode",
        )
        if uploaded and st.button("Process video", key="workspace_crop_video_submit", type="primary", width="stretch"):
            split_mode = "fit" if "Fit" in mode_label else "fill"
            suffix = os.path.splitext(uploaded.name)[-1] or ".mp4"
            tmp_dir = tempfile.mkdtemp(prefix="workspace_crop_")
            try:
                src_path = os.path.join(tmp_dir, f"source{suffix}")
                with open(src_path, "wb") as f:
                    f.write(uploaded.read())
                seg_dir = os.path.join(tmp_dir, "segments")
                os.makedirs(seg_dir, exist_ok=True)
                label = "Cropping" if split_mode == "fill" else "Fitting"
                with st.spinner(f"{label} and splitting into 60-second segments…"):
                    segment_paths = _split_video_to_folder(src_path, seg_dir, mode=split_mode)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                folder_name = f"{'crop' if split_mode == 'fill' else 'fit'}_{ts}"
                folder_id = get_or_create_subfolder(
                    get_or_create_subfolder(GOOGLE_DRIVE_FOLDER_ID, "Cropped Videos"),
                    folder_name,
                )
                links = []
                for seg_path in segment_paths:
                    seg_filename = os.path.basename(seg_path)
                    link = upload_to_drive(seg_path, seg_filename, folder_id)
                    links.append((seg_filename, link))
                st.success(f"Uploaded {len(links)} segment{'s' if len(links) != 1 else ''}:")
                for name, link in links:
                    st.markdown(f"- [{name}]({link})")
            except Exception as e:
                st.error(f"Failed: {describe_error(e)}")
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

    elif mode == "Create a Post":
        prompt = st.text_area(
            "Post prompt",
            key="workspace_home_create_post_prompt",
            height=150,
            placeholder="Write your post content, talking points, or key message…",
        ).strip()
        st.text_input(
            "Speaker name (optional)",
            key="workspace_home_create_post_speaker",
            placeholder="e.g. Bernie Sanders",
        )
        st.text_input(
            "Link (optional)",
            key="workspace_home_create_post_link",
            placeholder="https://…",
        )
        uploaded_media = st.file_uploader(
            "Photo or video (optional)",
            type=["mp4", "mov", "png", "jpg", "jpeg", "webp", "heic"],
            accept_multiple_files=False,
            key="workspace_home_create_post_media",
        )
        if st.button(
            "Create Post",
            key="workspace_home_create_post_submit",
            type="primary",
            width="stretch",
            disabled=not prompt,
        ):
            custom_link = _cell_text(st.session_state.get("workspace_home_create_post_link", "")).strip()
            speaker_name = _cell_text(st.session_state.get("workspace_home_create_post_speaker", "")).strip()
            try:
                with st.spinner("Creating post…"):
                    row_num = _create_post_from_prompt(prompt, custom_link, uploaded_media, speaker_name)
            except Exception as e:
                st.error(f"Could not create post: {describe_error(e)}")
            else:
                st.session_state["workspace_home_notice"] = f"Post created as row {row_num}."
                st.session_state["workspace_selected_row_num"] = row_num
                st.query_params["workspace_row"] = str(row_num)
                _close_workspace_home_action_dialog(clear_inputs=True)
                _rerun_workspace("Home")
        st.divider()
        st.caption("Slide prompt — copy, fill in your content, then paste the JSON result back into the prompt above:")
        st.code(_build_create_post_slide_prompt(), language=None)
    elif st.button(_action_label(mode), key=f"workspace_home_dialog_submit_{mode}", type="primary", width="stretch"):
        _run_workspace_home_action(
            mode,
            st.session_state.get("workspace_home_dialog_link", ""),
            selected_org_hashtag,
        )

    if st.button("Cancel", key=f"workspace_home_dialog_cancel_{mode}", width="stretch"):
        _close_workspace_home_action_dialog(clear_inputs=True)
        _rerun_workspace("Home")


@st.dialog("Process as Candidate Article", width="large", on_dismiss=_dismiss_workspace_candidate_article_dialog)
def _render_workspace_candidate_article_dialog(row: dict) -> None:
    row_num = row.get("row_number")
    substack_url = _cell_text(row.get("Instagram URL")).strip()

    st.caption("Paste the full article text here to generate the article-based slides and caption footer.")
    article_body = st.text_area(
        "Paste the full article",
        key="workspace_row_candidate_article_body",
        height=420,
    ).strip()
    generate_disabled = (
        not article_body
        or bool(st.session_state.get("workspace_row_candidate_article_generating"))
    )
    if st.button(
        "Generate Article Assets",
        key=f"workspace_row_candidate_article_generate_{row_num}",
        type="primary",
        width="stretch",
        disabled=generate_disabled,
    ):
        st.session_state["workspace_row_candidate_article_generating"] = True
        st.session_state.pop("workspace_row_candidate_article_error", None)
        try:
            with st.spinner("Generating article assets..."):
                generated_payload = _call_openai_candidate_article(article_body, substack_url)
                caption_text = _save_candidate_article_assets(row, generated_payload)
        except Exception as e:
            st.session_state["workspace_row_candidate_article_error"] = (
                "Could not generate the article assets. "
                f"{describe_error(e)}"
            )
            st.session_state.pop("workspace_row_candidate_article_result", None)
        else:
            generated_payload["substack_url"] = substack_url
            generated_payload["final_caption"] = caption_text
            st.session_state["workspace_row_candidate_article_result"] = generated_payload
            st.session_state["workspace_success"] = (
                f"Row {row_num}: article caption and slides saved to the sheet."
            )
        finally:
            st.session_state["workspace_row_candidate_article_generating"] = False
        _rerun_workspace("Home")

    candidate_article_error = _cell_text(
        st.session_state.get("workspace_row_candidate_article_error", "")
    ).strip()
    candidate_article_result = st.session_state.get("workspace_row_candidate_article_result")
    if candidate_article_error:
        st.error(candidate_article_error)
        if st.button(
            "Retry",
            key=f"workspace_row_candidate_article_retry_{row_num}",
            width="stretch",
            disabled=not article_body,
        ):
            st.session_state["workspace_row_candidate_article_generating"] = True
            st.session_state.pop("workspace_row_candidate_article_error", None)
            try:
                with st.spinner("Generating article assets..."):
                    generated_payload = _call_openai_candidate_article(article_body, substack_url)
                    caption_text = _save_candidate_article_assets(row, generated_payload)
            except Exception as e:
                st.session_state["workspace_row_candidate_article_error"] = (
                    "Could not generate the article assets. "
                    f"{describe_error(e)}"
                )
                st.session_state.pop("workspace_row_candidate_article_result", None)
            else:
                generated_payload["substack_url"] = substack_url
                generated_payload["final_caption"] = caption_text
                st.session_state["workspace_row_candidate_article_result"] = generated_payload
                st.session_state["workspace_success"] = (
                    f"Row {row_num}: article caption and slides saved to the sheet."
                )
            finally:
                st.session_state["workspace_row_candidate_article_generating"] = False
            _rerun_workspace("Home")

    if candidate_article_result:
        st.divider()
        st.markdown("**Generated Output**")
        text1 = _cell_text(candidate_article_result.get("text1")).strip()
        text2 = _cell_text(candidate_article_result.get("text2")).strip()
        text3 = _cell_text(candidate_article_result.get("text3")).strip()
        caption_text = _cell_text(candidate_article_result.get("final_caption")).strip() or _build_candidate_article_caption(
            _cell_text(candidate_article_result.get("generated_caption")).strip(),
            _cell_text(row.get("Required Hashtags")).strip(),
        )
        copy_all_text = (
            f"SLIDE 1:\n{text1}\n\n"
            f"SLIDE 2:\n{text2}\n\n"
            f"SLIDE 3:\n{text3}\n\n"
            f"CAPTION:\n{caption_text}"
        )
        _render_candidate_output_card("Slide 1", text1, f"workspace_row_candidate_article_slide1_{row_num}")
        _render_candidate_output_card("Slide 2", text2, f"workspace_row_candidate_article_slide2_{row_num}")
        _render_candidate_output_card("Slide 3", text3, f"workspace_row_candidate_article_slide3_{row_num}")
        _render_candidate_output_card("Generated Caption", caption_text, f"workspace_row_candidate_article_caption_{row_num}")
        st.html(_copy_button_html("Copy All", copy_all_text, f"workspace_row_candidate_article_copy_all_{row_num}", primary=True))

    if st.button("Close", key=f"workspace_row_candidate_article_close_{row_num}", width="stretch"):
        _close_workspace_candidate_article_dialog(clear_inputs=True)
        _rerun_workspace("Home")


@st.dialog("Slides", width="large", on_dismiss=_close_workspace_slides_dialog)
def _render_workspace_slides_dialog(workspace_rows: list[dict], workspace_rows_error: str) -> None:
    slides_notice = st.session_state.pop("workspace_slides_notice", "")

    if workspace_rows_error:
        st.error(f"Could not load slide-ready rows: {workspace_rows_error}")
        ready_rows = []
    else:
        ready_rows = _ready_rows_from_loaded_rows(workspace_rows)

    ready_count = len(ready_rows)
    batched_ready_rows = ready_rows[:WORKSPACE_SLIDES_BATCH_SIZE]
    remaining_count = max(ready_count - len(batched_ready_rows), 0)
    row_word = "row" if ready_count == 1 else "rows"
    if ready_count:
        st.caption(
            f"{ready_count} {row_word} ready for slides. "
            f"Showing the next {len(batched_ready_rows)} row(s) in this batch."
        )
        if remaining_count:
            st.caption(f"{remaining_count} more row(s) will be available after you finish this batch.")
    else:
        st.info("No rows are ready for slides yet.")

    if slides_notice:
        st.caption(slides_notice)

    pasted_results = st.text_area(
        "Paste slide results",
        key="workspace_slides_results",
        height=100,
        placeholder='[{"row_number":2,"name":"...","text1":"...","text2":"...","text3":"..."}]',
    )
    if st.button("Apply slide results", key="workspace_slides_apply", type="primary", width="stretch"):
        try:
            updated_count, issues = _apply_chatgpt_handoff_results(GOOGLE_SHEET_ID, pasted_results)
        except Exception as e:
            st.error(f"Could not apply slide results: {describe_error(e)}")
        else:
            if updated_count:
                message = f"Applied slide results to {updated_count} row(s)."
                if issues:
                    message += f" Skipped {len(issues)} item(s): " + " | ".join(issues[:3])
                st.session_state["workspace_success"] = message
            else:
                st.session_state["workspace_error"] = (
                    "No valid slide results were found to apply."
                    + (f" {' | '.join(issues[:3])}" if issues else "")
                )
            st.session_state.pop("workspace_slides_results", None)
            _rerun_workspace("Home")

    if batched_ready_rows:
        st.caption("Slide prompt")
        st.code(_build_chatgpt_handoff_prompt(batched_ready_rows), language=None)

    if st.button("Close", key="workspace_slides_close", width="stretch"):
        _close_workspace_slides_dialog()
        _rerun_workspace("Home")


@st.dialog("Slides for this post", width="large", on_dismiss=_close_workspace_post_slides_dialog)
def _render_workspace_post_slides_dialog(row: dict) -> None:
    row_num = row["row_number"]
    st.caption(f"Row {row_num}. Pasted results will be applied to this post.")

    pasted_results = st.text_area(
        "Paste slide results",
        key="workspace_post_slides_results",
        height=100,
        placeholder='[{"row_number":2,"name":"...","text1":"...","text2":"...","text3":"..."}]',
    )
    if st.button(
        "Apply to this post",
        key=f"workspace_post_slides_apply_{row_num}",
        type="primary",
        width="stretch",
        disabled=not pasted_results.strip(),
    ):
        try:
            updated_count, issues = _apply_slide_result_to_specific_row(row_num, pasted_results)
            success_message = f"Row {row_num}: slide result applied."
        except Exception as e:
            st.error(f"Could not apply slide result: {describe_error(e)}")
        else:
            if updated_count:
                message = success_message
                if issues:
                    message += f" {' | '.join(issues[:3])}"
                st.session_state["workspace_success"] = message
            else:
                st.session_state["workspace_error"] = (
                    f"Row {row_num}: no valid slide result was found."
                    + (f" {' | '.join(issues[:3])}" if issues else "")
                )
            st.session_state.pop("workspace_post_slides_results", None)
            _close_workspace_post_slides_dialog(clear_inputs=True)
            _rerun_workspace("Edit")

    st.caption("Slide prompt")
    st.code(_build_chatgpt_handoff_prompt([row]), language=None)

    if st.button("Close", key=f"workspace_post_slides_close_{row_num}", width="stretch"):
        _close_workspace_post_slides_dialog(clear_inputs=True)
        _rerun_workspace("Edit")


@st.dialog("Make generic", width="large", on_dismiss=_dismiss_workspace_generic_slides_dialog)
def _render_workspace_generic_slides_dialog(row: dict) -> None:
    row_num = row["row_number"]
    st.caption(
        f"Row {row_num}. Creates a new source-agnostic post — the original is left unchanged. "
        "Paste the result below and click Create new post."
    )

    pasted_results = st.text_area(
        "Paste slide results",
        key="workspace_generic_slides_results",
        height=100,
        placeholder='[{"row_number":2,"name":"...","text1":"...","text2":"...","text3":"...","generated_caption":"..."}]',
    )
    if st.button(
        "Create new post",
        key=f"workspace_generic_slides_apply_{row_num}",
        type="primary",
        width="stretch",
        disabled=not pasted_results.strip(),
    ):
        try:
            new_row_num = _create_generic_post_from_result(row, pasted_results)
        except Exception as e:
            st.error(f"Could not create generic post: {describe_error(e)}")
        else:
            st.session_state["workspace_success"] = f"Generic post created as row {new_row_num}."
            st.session_state["workspace_selected_row_num"] = new_row_num
            st.query_params["workspace_row"] = str(new_row_num)
            st.session_state.pop("workspace_generic_slides_results", None)
            _close_workspace_generic_slides_dialog(clear_inputs=True)
            _rerun_workspace("Edit")

    st.caption("Generic slides prompt")
    st.code(_build_generic_chatgpt_prompt(row), language=None)

    if st.button("Close", key=f"workspace_generic_slides_close_{row_num}", width="stretch"):
        _close_workspace_generic_slides_dialog(clear_inputs=True)
        _rerun_workspace("Edit")


@st.dialog("Slide action", on_dismiss=_dismiss_workspace_slide_action_dialog)
def _render_workspace_slide_action_dialog(row: dict) -> None:
    dialog_state = st.session_state.get("workspace_slide_action_dialog") or {}
    action = (dialog_state.get("action") or "").strip()
    row_num = row["row_number"]
    if not action:
        return

    prompt_key = f"workspace_row_slides_prompt_{row_num}"
    raw_top_comment = st.session_state.get(_workspace_key(row, "top"), row.get("Top Comment", "")).strip()
    clean_top_comment, pinned_top_comment = _decode_top_comment(raw_top_comment)
    current_speaker_for_dialog = _cell_text(
        st.session_state.get(_workspace_speaker_key(row), row.get("Speaker Name", ""))
    ).strip()
    current_values = {
        "prompt": st.session_state.get(prompt_key, "") or _build_single_row_chatgpt_prompt(row),
        "text1": _cell_text(row.get("text1")).strip(),
        "text2": _cell_text(row.get("text2")).strip(),
        "text3": _cell_text(row.get("text3")).strip(),
        "text4": _cell_text(row.get("text4")).strip(),
        "text5": _cell_text(row.get("text5")).strip(),
        "text6": _cell_text(row.get("text6")).strip(),
        "caption": _cell_text(row.get("Generated Caption")).strip(),
        "custom_link": _cell_text(row.get("Slide CTA")).strip() or "Say LINK for more",
        "speaker": current_speaker_for_dialog,
        "quote": _cell_text(row.get("quote")).strip(),
    }
    dialog_labels = {
        "prompt": "Generate prompt",
        "text1": "Edit text 1",
        "text2": "Edit text 2",
        "text3": "Edit text 3",
        "text4": "Edit text 4",
        "text5": "Edit text 5",
        "text6": "Edit text 6",
        "caption": "Edit caption",
        "custom_link": "Edit custom link",
        "speaker": "Update name",
        "quote": "Edit quote",
    }
    if action not in current_values:
        st.session_state["workspace_error"] = f"Row {row_num}: unknown slide action {action}."
        _close_workspace_slide_action_dialog(clear_inputs=True)
        _rerun_workspace("Edit")
        return

    context_key = f"{row_num}:{action}"
    if st.session_state.get("workspace_slide_dialog_context") != context_key:
        st.session_state["workspace_slide_dialog_context"] = context_key
        st.session_state["workspace_slide_dialog_value"] = current_values[action]

    st.caption(dialog_labels[action])
    if action == "speaker":
        st.text_input(
            dialog_labels[action],
            key="workspace_slide_dialog_value",
            placeholder="Add context (e.g. speaker name)",
            label_visibility="collapsed",
        )
    else:
        st.text_area(
            dialog_labels[action],
            key="workspace_slide_dialog_value",
            height=240,
            label_visibility="collapsed",
        )

    if st.button("Save", key=f"workspace_slide_dialog_save_{context_key}", type="primary", width="stretch"):
        edited_value = st.session_state.get("workspace_slide_dialog_value", "").strip()
        try:
            if action == "prompt":
                st.session_state[prompt_key] = edited_value
                st.session_state["workspace_success"] = f"Row {row_num}: slide prompt saved."
            elif action in {"text1", "text2", "text3", "text4", "text5", "text6"}:
                if update_carousel_fields is None:
                    raise RuntimeError("Carousel field updates are not supported in this build.")
                name = _cell_text(row.get("name")).strip()
                text1 = _single_paragraph_slide_text(edited_value if action == "text1" else row.get("text1"))
                text2 = _single_paragraph_slide_text(edited_value if action == "text2" else row.get("text2"))
                text3 = _single_paragraph_slide_text(edited_value if action == "text3" else row.get("text3"))
                text4 = _single_paragraph_slide_text(edited_value if action == "text4" else row.get("text4"))
                text5 = _single_paragraph_slide_text(edited_value if action == "text5" else row.get("text5"))
                text6 = _single_paragraph_slide_text(edited_value if action == "text6" else row.get("text6"))
                update_carousel_fields(GOOGLE_SHEET_ID, row_num, name, text1, text2, text3, text4, text5, text6)
                st.session_state["workspace_success"] = f"Row {row_num}: {dialog_labels[action].lower()} saved."
            elif action == "quote":
                if update_quote is None:
                    raise RuntimeError("Quote updates are not supported in this build.")
                update_quote(GOOGLE_SHEET_ID, row_num, edited_value)
                st.session_state["workspace_success"] = f"Row {row_num}: quote saved."
            elif action == "caption":
                current_status = _cell_text(row.get("Status")).strip() or _default_editor_status(row)
                update_caption(GOOGLE_SHEET_ID, row_num, edited_value, current_status)
                st.session_state["workspace_success"] = f"Row {row_num}: caption saved."
            elif action == "custom_link":
                update_slide_cta_option(GOOGLE_SHEET_ID, row_num, edited_value)
                st.session_state[f"workspace_slide_three_cta_row_{row_num}"] = edited_value
                st.session_state["workspace_success"] = f"Row {row_num}: slide button text saved."
            elif action == "speaker":
                if update_speaker_names_batch is None:
                    raise RuntimeError("Speaker name updates are not supported in this build.")
                update_speaker_names_batch(GOOGLE_SHEET_ID, {row_num: edited_value})
                st.session_state[_workspace_speaker_key(row)] = edited_value
                st.session_state["workspace_success"] = f"Row {row_num}: name saved."
        except Exception as e:
            st.session_state["workspace_error"] = f"Row {row_num}: could not save {dialog_labels[action].lower()} - {describe_error(e)}"
        _close_workspace_slide_action_dialog(clear_inputs=True)
        _rerun_workspace("Edit")

    if st.button("Cancel", key=f"workspace_slide_dialog_cancel_{context_key}", width="stretch"):
        _close_workspace_slide_action_dialog(clear_inputs=True)
        _rerun_workspace("Edit")


@st.dialog("Add link", on_dismiss=_dismiss_workspace_link_dialog)
def _render_workspace_link_dialog(row: dict) -> None:
    row_num = row["row_number"]
    speaker_name = (row.get("Speaker Name") or "").strip()
    fundraising_presets = _fundraising_preset_map()
    source_key = _workspace_key(row, "link_source")
    link_url_key = _workspace_key(row, "link_url")
    link_comment_key = _workspace_key(row, "link_comment")

    previous_source = st.session_state.get(source_key, "Custom")
    selected_source = st.selectbox(
        "Type",
        options=list(fundraising_presets.keys()),
        key=source_key,
    )
    selected_top_comment = fundraising_presets.get(selected_source, "").strip()

    if selected_source != previous_source:
        if selected_source == "Custom":
            st.session_state.pop(link_comment_key, None)
        else:
            st.session_state[link_comment_key] = selected_top_comment

    if selected_source == "Custom":
        st.text_input(
            "Link",
            key=link_url_key,
            placeholder="https://example.com",
        )
    else:
        if link_comment_key not in st.session_state:
            st.session_state[link_comment_key] = selected_top_comment
        st.text_area(
            "Top comment",
            key=link_comment_key,
            height=180,
        )

    if st.button("Add", key=f"workspace_link_add_{row_num}", type="primary", width="stretch"):
        full_link = st.session_state.get(link_url_key, "").strip()
        if selected_source == "Custom" and not _is_https_url(full_link):
            st.session_state["workspace_error"] = f"Row {row_num}: link must start with https://"
            _rerun_workspace("Edit")

        addition = (
            _build_link_cta(full_link)
            if selected_source == "Custom"
            else st.session_state.get(link_comment_key, selected_top_comment).strip()
        )
        top_comment = _encode_top_comment(addition, pinned=False)
        try:
            caption_updated = _apply_top_comment_to_caption(row, row_num, speaker_name, top_comment)
        except Exception as e:
            st.session_state["workspace_error"] = f"Row {row_num}: could not save link CTA - {describe_error(e)}"
        else:
            if caption_updated:
                st.session_state["workspace_success"] = f"Row {row_num}: link CTA saved to generated caption."
            else:
                st.session_state["workspace_success"] = (
                    f"Row {row_num}: link CTA saved. No caption exists yet, "
                    "so it will be included the next time one is generated."
                )
        _close_workspace_link_dialog(row)
        _rerun_workspace("Edit")

    if st.button("Cancel", key=f"workspace_link_cancel_{row_num}", width="stretch"):
        _close_workspace_link_dialog(row)
        _rerun_workspace("Edit")


@st.dialog("Update screenshot", on_dismiss=_dismiss_workspace_thumbnail_dialog)
def _render_workspace_thumbnail_dialog(row: dict) -> None:
    row_num = row["row_number"]
    url = _cell_text(row.get("Instagram URL")).strip()
    has_media = bool(_cell_text(row.get("Media Drive Link")).strip())

    uploaded_thumbnail = st.file_uploader(
        "Replace screenshot",
        type=["png", "jpg", "jpeg", "webp", "heic", "heif"],
        accept_multiple_files=False,
        key=_workspace_key(row, "thumbnail_upload"),
        help="On iPhone this opens your photo library/files chooser. On desktop it opens the file picker.",
    )
    if uploaded_thumbnail is not None:
        upload_token_key = _workspace_key(row, "thumbnail_upload_token")
        upload_token = "|".join(
            [
                getattr(uploaded_thumbnail, "name", "") or "",
                str(getattr(uploaded_thumbnail, "size", "") or ""),
                getattr(uploaded_thumbnail, "type", "") or "",
            ]
        )
        if st.session_state.get(upload_token_key) != upload_token:
            st.session_state[upload_token_key] = upload_token
            try:
                _replace_row_thumbnail_from_upload(row, uploaded_thumbnail)
            except Exception as e:
                st.session_state["workspace_error"] = f"Row {row_num}: could not replace screenshot - {describe_error(e)}"
            else:
                st.session_state["workspace_success"] = f"Row {row_num}: screenshot replaced from uploaded image."
            _close_workspace_thumbnail_dialog(row)
            _rerun_workspace("Edit")

    if _is_reel_url(url) and has_media and st.button(
        "Update screenshot (+5s)",
        key=f"workspace_thumbnail_refresh_5s_{row_num}",
        width="stretch",
        help="Replace the current screenshot with a frame taken about 5 seconds into the video.",
    ):
        _close_workspace_thumbnail_dialog(row)
        _queue_workspace_action(row_num, "refresh_thumbnail_5s")
        _rerun_workspace("Edit")

    if st.button("Cancel", key=f"workspace_thumbnail_cancel_{row_num}", width="stretch"):
        _close_workspace_thumbnail_dialog(row)
        _rerun_workspace("Edit")


def _copy_block(label: str, value: str, key: str, empty_text: str = "(none)") -> None:
    st.code(value or empty_text, language=None)
    st.markdown(
        f'<div class="workspace-plain-copy-text">{html.escape(value or empty_text)}</div>',
        unsafe_allow_html=True,
    )


def _one_line_copy_preview(label: str, value: str, key: str, empty_text: str = "(none)") -> None:
    display_text = (value or empty_text).replace("\n", " ")
    escaped_label = html.escape(label)
    clipboard_text = json.dumps(value or "")
    component_html = f"""
    <div style="margin-top:0.25rem;" id="{html.escape(key)}">
      <div style="
        position: relative;
        min-height: 2.1rem;
        height: 2.1rem;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
        border: 1px solid rgba(15,23,42,0.08);
        border-radius: 16px;
        background: #f8fafc;
        padding: 0.45rem 3.1rem 0.45rem 0.8rem;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 0.88rem;
        line-height: 1.15rem;
        color: #0f172a;
      ">{html.escape(display_text)}</div>
      <button
        onclick='navigator.clipboard.writeText({clipboard_text})'
        aria-label='Copy {escaped_label}'
        style="
          position: absolute;
          margin-top: -2.55rem;
          right: 0.55rem;
          width: 2.35rem;
          height: 2.35rem;
          border: 1px solid rgba(15,23,42,0.08);
          border-radius: 16px;
          background: white;
          color: #0f172a;
          font-size: 1rem;
          line-height: 1;
          cursor: pointer;
          box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
        "
      >⧉</button>
    </div>
    """
    st.html(component_html)


def _multiline_copy_preview(label: str, value: str, key: str, empty_text: str = "(none)") -> None:
    display_text = value or empty_text
    escaped_label = html.escape(label)
    escaped_key = html.escape(key)
    clipboard_text = json.dumps(value or "")
    component_html = f"""
    <div style="margin-top:0.5rem;" id="{escaped_key}">
      <div style="
        position: relative;
        border: 1px solid rgba(15,23,42,0.08);
        border-radius: 18px;
        background: #f8fafc;
        padding: 0.9rem 3.3rem 0.9rem 1rem;
        box-shadow: 0 8px 20px rgba(15, 23, 42, 0.04);
      ">
        <pre style="
          margin: 0;
          white-space: pre-wrap;
          word-break: break-word;
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
          font-size: 0.88rem;
          line-height: 1.35rem;
          color: #0f172a;
          max-height: 18rem;
          overflow: auto;
        ">{html.escape(display_text)}</pre>
        <button
          onclick='navigator.clipboard.writeText({clipboard_text})'
          aria-label='Copy {escaped_label}'
          style="
            position: absolute;
            top: 0.75rem;
            right: 0.75rem;
            width: 2.35rem;
            height: 2.35rem;
            border: 1px solid rgba(15,23,42,0.08);
            border-radius: 16px;
            background: white;
            color: #0f172a;
            font-size: 1rem;
            line-height: 1;
            cursor: pointer;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
          "
        >⧉</button>
      </div>
    </div>
    """
    st.html(component_html)


def _tab_copy_preview(value: str, show_plain_text: bool = True, key: str = "") -> None:
    st.code(value or "(none)", language=None)
    if show_plain_text:
        st.markdown(
            f'<div class="workspace-plain-copy-text">{html.escape(value or "(none)")}</div>',
            unsafe_allow_html=True,
        )
    else:
        preview_key = key or f"workspace_multiline_copy_{hashlib.md5((value or '').encode('utf-8')).hexdigest()[:12]}"
        _multiline_copy_preview("copy text", value or "(none)", preview_key)


def _render_slide_one_preview(
    handle: str,
    headline: str,
    background_url: str = "",
    headline_font_adjust_px: int = 0,
    background_y_adjust_px: int = 0,
    fit_to_top: bool = False,
    quote: str = "",
    quote_font_adjust_px: int = 0,
) -> None:
    headline_text = (headline or "").strip()
    if not headline_text:
        return

    safe_handle = html.escape((handle or "").strip() or "@UNKNOWN")
    safe_headline = html.escape(headline_text)
    safe_background = html.escape(background_url.strip()) if background_url else ""
    headline_clamp_css = (
        f"clamp(calc(0.9rem + {headline_font_adjust_px}px), "
        f"calc(3.6cqw + {headline_font_adjust_px}px), "
        f"calc(1.125rem + {headline_font_adjust_px}px))"
    )
    background_position = f"center {background_y_adjust_px}px"
    background_size = "contain" if fit_to_top else "cover"
    background_repeat = "no-repeat" if fit_to_top else "repeat"
    if fit_to_top:
        background_position = f"center top"
    background_css = (
        f"background-image: url('{safe_background}'); background-size: {background_size}; "
        f"background-repeat: {background_repeat}; background-position: {background_position};"
        if safe_background
        else "background: #121722;"
    )
    quote_html = ""
    if (quote or "").strip():
        safe_quote = html.escape(quote.strip())
        quote_html = f"""<div style="
  font-family: 'Bebas Neue', sans-serif;
  font-size: clamp(2rem, calc(10.2cqw + {quote_font_adjust_px}px), 7rem);
  font-weight: 400;
  line-height: 85%;
  color: #FFF;
  text-transform: uppercase;
  margin-bottom: 0;
">{safe_quote}</div>"""
    preview_html = f"""
    <div style="margin-top: 1rem;">
      <style>
        @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600&display=swap');
        .workspace-preview-shell {{
          width: 100%;
          max-width: {PREVIEW_CANVAS_WIDTH_PX}px;
          margin: 0 auto;
          container-type: inline-size;
        }}
        .workspace-preview-card {{
          width: 100%;
          border-radius: 0;
          overflow: hidden;
          box-shadow: 0 24px 80px rgba(15, 23, 42, 0.22);
        }}
        .workspace-preview-canvas {{
          width: 100%;
          aspect-ratio: 4 / 5.5;
        }}
        @media (max-width: 768px) {{
          .workspace-preview-shell {{
            width: 100vw;
            max-width: none;
            margin-left: calc(50% - 50vw);
            margin-right: calc(50% - 50vw);
          }}
        }}
        .workspace-slide-preview-copy {{
          font-family: {PREVIEW_SLIDE_FONT_FAMILY} !important;
        }}
        .workspace-slide-preview-handle {{
          font-family: {PREVIEW_SLIDE_FONT_FAMILY} !important;
          font-weight: 400 !important;
        }}
        .workspace-slide-preview-headline {{
          font-family: {PREVIEW_SLIDE_FONT_FAMILY} !important;
          font-weight: {PREVIEW_SLIDE_FONT_WEIGHT} !important;
        }}
      </style>
      <div style="font-size: 0.82rem; font-weight: 500; color: #475569; margin-bottom: 0.5rem;">
        Slide 1 preview
      </div>
      <div class="workspace-preview-shell">
        <div class="workspace-preview-card" style="background: #0f172a;">
          <div class="workspace-preview-canvas" style="
          position: relative;
          width: 100%;
          display: flex;
          flex-direction: column;
          justify-content: flex-end;
          {background_css}
        ">
          <div class="workspace-slide-preview-copy" style="
            display: flex;
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            gap: 0.8rem;
            align-self: stretch;
            padding: 78px 24px 24px 24px;
            color: white;
            font-family: {PREVIEW_SLIDE_FONT_FAMILY};
            background: linear-gradient(180deg, rgba(18, 23, 34, 0) 0%, rgba(18, 23, 34, 0.9) 36.34%, #121722 80.76%);
          ">
            <div class="workspace-slide-preview-handle" style="
              font-size: clamp(0.7rem, 2.7cqw, 1rem);
              letter-spacing: 0.3em;
              line-height: 1.38;
              text-transform: uppercase;
              white-space: nowrap;
            ">{safe_handle}</div>
            {quote_html}
            <div class="workspace-slide-preview-headline" style="
              font-size: {headline_clamp_css};
              line-height: {PREVIEW_SLIDE_LINE_HEIGHT};
              letter-spacing: {PREVIEW_SLIDE_LETTER_SPACING};
            ">{safe_headline}</div>
          </div>
        </div>
      </div>
    </div>
    """
    st.html(preview_html)


def _render_text_slide_preview(
    slide_number: int,
    body_text: str,
    body_font_adjust_px: int = 0,
    include_link_cta: bool = False,
    link_cta_target: str = "more",
    link_cta_text: str = "",
) -> None:
    content_text = (body_text or "").strip()
    if not content_text:
        return

    safe_body = html.escape(content_text)
    body_clamp_css = (
        f"clamp(calc({SLIDE_BODY_FONT_MIN_REM}rem + {body_font_adjust_px}px), "
        f"calc({SLIDE_BODY_FONT_CQW}cqw + {body_font_adjust_px}px), "
        f"calc({SLIDE_BODY_FONT_MAX_REM}rem + {body_font_adjust_px}px))"
    )
    cta_html = ""
    if include_link_cta:
        cta_value = (link_cta_text or "").strip() or _slide_three_cta_text(link_cta_target, "")
        cta_html = """
            <div style="
              display: inline-flex;
              align-items: center;
              justify-content: center;
              margin-top: 1.2rem;
              padding: 0.4rem 0.6rem;
              border-radius: 2px;
              background: #ffffff;
              color: #121722;
              font-size: clamp(0.95rem, 2vw, 1.15rem);
              font-weight: 600;
              line-height: 1.1;
            ">""" + html.escape(cta_value) + """</div>
        """

    preview_html = f"""
    <div style="margin-top: 1rem;">
      <style>
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700&display=swap');
        .workspace-preview-shell {{
          width: 100%;
          max-width: {PREVIEW_CANVAS_WIDTH_PX}px;
          margin: 0 auto;
          container-type: inline-size;
        }}
        .workspace-preview-card {{
          width: 100%;
          border-radius: 0;
          overflow: hidden;
          box-shadow: 0 24px 80px rgba(15, 23, 42, 0.22);
        }}
        .workspace-preview-canvas {{
          width: 100%;
          aspect-ratio: 4 / 5.5;
        }}
        @media (max-width: 768px) {{
          .workspace-preview-shell {{
            width: 100vw;
            max-width: none;
            margin-left: calc(50% - 50vw);
            margin-right: calc(50% - 50vw);
          }}
        }}
        .workspace-text-slide-preview-copy {{
          font-family: {PREVIEW_SLIDE_FONT_FAMILY} !important;
          font-weight: {PREVIEW_SLIDE_FONT_WEIGHT} !important;
        }}
      </style>
      <div style="font-size: 0.82rem; font-weight: 600; color: #475569; margin-bottom: 0.5rem;">
        Slide {slide_number} preview
      </div>
      <div class="workspace-preview-shell">
        <div class="workspace-preview-card" style="background: #121722;">
          <div class="workspace-preview-canvas workspace-text-slide-preview-copy" style="
          padding: 28px 26px 28px 26px;
          color: #ffffff;
          background: #121722;
          font-size: {body_clamp_css};
          line-height: {PREVIEW_SLIDE_LINE_HEIGHT};
          letter-spacing: {PREVIEW_SLIDE_LETTER_SPACING};
          overflow: hidden;
          box-sizing: border-box;
        ">
          <div style="white-space: pre-wrap;">{safe_body}</div>
          {cta_html}
        </div>
      </div>
    </div>
    """
    st.html(preview_html)


def _build_single_row_chatgpt_prompt(row: dict) -> str:
    return _build_chatgpt_handoff_prompt([row])


def _render_workspace_preview_control_bar(
    control_id: str,
    font_adjust_key: str,
    current_font_adjust: int,
    background_adjust_key: str | None = None,
    current_background_adjust: int = 0,
    fit_toggle_key: str | None = None,
    fit_toggle_current: bool = False,
) -> None:
    anchor_id = f"workspace-preview-ctrl-{control_id}"
    with st.container():
        st.markdown(f'<div id="{anchor_id}" class="workspace-preview-controls-anchor"></div>', unsafe_allow_html=True)
        controls = [("A-", "font_down"), ("A+", "font_up")]
        if background_adjust_key is not None:
            controls.extend([("Up", "bg_up"), ("Down", "bg_down")])
        if fit_toggle_key is not None:
            controls.append(("Fill" if fit_toggle_current else "Fit", "fit_toggle"))
        columns = st.columns(len(controls), gap="small")
        for column, (label, action) in zip(columns, controls):
            with column:
                if st.button(label, key=f"workspace_preview_{control_id}_{action}", width="stretch"):
                    if action == "font_down":
                        st.session_state[font_adjust_key] = max(-80, current_font_adjust - 2)
                    elif action == "font_up":
                        st.session_state[font_adjust_key] = min(200, current_font_adjust + 2)
                    elif action == "bg_up" and background_adjust_key is not None:
                        st.session_state[background_adjust_key] = max(-1200, current_background_adjust - 48)
                    elif action == "bg_down" and background_adjust_key is not None:
                        st.session_state[background_adjust_key] = min(1200, current_background_adjust + 48)
                    elif action == "fit_toggle" and fit_toggle_key is not None:
                        st.session_state[fit_toggle_key] = not fit_toggle_current
                    st.session_state["workspace_preview_scroll_target"] = anchor_id
                    _rerun_workspace("Edit")


def _copy_tabs(
    row_num: int,
    generated: str,
    original_caption: str,
    transcript: str,
    username: str,
    speaker_name: str,
    top_comment: str,
    required_hashtags: str,
    media_link: str = "",
    media_type: str = "",
    source_url: str = "",
    is_instagram: bool = True,
    slide_text1: str = "",
    slide_text2: str = "",
    slide_text3: str = "",
    slide_text4: str = "",
    slide_text5: str = "",
    slide_text6: str = "",
    prompt_row: dict | None = None,
    thumbnail_link: str = "",
    slide_cta_options: dict[str, str] | None = None,
) -> None:
    tab_labels = ["Slides", "Caption", "Original"]
    content_tab_key = f"workspace_row_content_tab_{row_num}"
    if content_tab_key not in st.session_state or st.session_state[content_tab_key] not in tab_labels:
        st.session_state[content_tab_key] = "Slides"
    selected_content_tab = st.segmented_control(
        "Content",
        tab_labels,
        key=content_tab_key,
        label_visibility="collapsed",
        width="stretch",
    ) or "Slides"
    original_preview = _build_original_caption_preview(
        original_caption,
        username,
        top_comment,
        required_hashtags,
        is_instagram=is_instagram,
    )
    if selected_content_tab == "Caption":
        _tab_copy_preview(
            _caption_tab_value(
                generated,
                original_caption,
                username,
                top_comment,
                required_hashtags,
                is_instagram,
            )
        )
        st.caption("Comment CTA")
        st.code(top_comment or "(none)", language=None)
        if st.button("Add custom link", key=f"workspace_caption_link_open_{row_num}", width="stretch"):
            st.session_state["workspace_link_dialog_row"] = row_num
            _rerun_workspace("Edit")
        if prompt_row:
            st.markdown("<div style='padding-top:100px'></div>", unsafe_allow_html=True)
            if st.button(
                "Delete row",
                key=f"workspace_caption_delete_{row_num}",
                width="stretch",
            ):
                try:
                    _delete_workspace_row(prompt_row)
                except Exception as e:
                    st.session_state["workspace_error"] = f"Row {row_num}: could not delete row - {describe_error(e)}"
                else:
                    st.session_state["workspace_success"] = f"Row {row_num}: deleted from the sheet."
                _rerun_workspace("Edit")
    elif selected_content_tab == "Original":
        _tab_copy_preview(original_preview)
        if is_instagram:
            st.caption("Transcript")
            _tab_copy_preview(transcript)
    elif selected_content_tab == "Slides":
        prompt_key = f"workspace_row_slides_prompt_{row_num}"
        if not st.session_state.get(prompt_key):
            base_row = prompt_row or {}
            effective_row = {
                **base_row,
                "Speaker Name": st.session_state.get(_workspace_speaker_key(base_row), base_row.get("Speaker Name", "")),
                "Caption Context": st.session_state.get(_workspace_key(base_row, "context"), base_row.get("Caption Context", "")),
            }
            st.session_state[prompt_key] = _build_single_row_chatgpt_prompt(effective_row)
        slide_one_font_adjust_key = f"workspace_slide_preview_font_adjust_{row_num}"
        slide_one_background_adjust_key = f"workspace_slide_preview_background_adjust_{row_num}"
        slide_one_fit_toggle_key = f"workspace_slide_preview_fit_mode_{row_num}"
        slide_two_font_adjust_key = f"workspace_slide_two_preview_font_adjust_{row_num}"
        slide_two_cta_key = f"workspace_slide_two_cta_row_{row_num}"
        slide_three_font_adjust_key = f"workspace_slide_three_preview_font_adjust_{row_num}"
        slide_three_cta_key = f"workspace_slide_three_cta_row_{row_num}"
        slide_four_font_adjust_key = f"workspace_slide_four_preview_font_adjust_{row_num}"
        slide_five_font_adjust_key = f"workspace_slide_five_preview_font_adjust_{row_num}"
        slide_six_font_adjust_key = f"workspace_slide_six_preview_font_adjust_{row_num}"
        slide_merge_key = f"workspace_slide_merge_row_{row_num}"
        slide_merge_original_key = f"workspace_slide_merge_original_t3_{row_num}"
        slide_quote_show_key = f"workspace_slide_quote_show_{row_num}"
        slide_quote_font_adjust_key = f"workspace_slide_quote_font_adjust_{row_num}"
        preview_links_key = f"workspace_preview_upload_links_{row_num}"
        default_slide_one_fit_mode = _is_candidate_article_row(prompt_row or {})
        slide_quote = _cell_text((prompt_row or {}).get("quote", "")).strip()
        _raw_s1_font = st.session_state.get(slide_one_font_adjust_key)
        current_slide_one_font_adjust = 2 if _raw_s1_font is None else int(_raw_s1_font)
        current_slide_one_background_adjust = int(st.session_state.get(slide_one_background_adjust_key, 0) or 0)
        current_slide_one_fit_mode = bool(
            st.session_state.get(slide_one_fit_toggle_key, default_slide_one_fit_mode)
        )
        _raw_s2_font = st.session_state.get(slide_two_font_adjust_key)
        current_slide_two_font_adjust = -2 if _raw_s2_font is None else int(_raw_s2_font)
        current_slide_two_cta = _cell_text(
            st.session_state.get(slide_two_cta_key, "hidden")
        ).strip().lower() or "hidden"
        if current_slide_two_cta not in {"more", "article", "petition", "video", "custom link", "hidden"}:
            current_slide_two_cta = "hidden"
            st.session_state[slide_two_cta_key] = current_slide_two_cta
        _raw_s3_font = st.session_state.get(slide_three_font_adjust_key)
        current_slide_three_font_adjust = -2 if _raw_s3_font is None else int(_raw_s3_font)
        _raw_s4_font = st.session_state.get(slide_four_font_adjust_key)
        current_slide_four_font_adjust = -2 if _raw_s4_font is None else int(_raw_s4_font)
        _raw_s5_font = st.session_state.get(slide_five_font_adjust_key)
        current_slide_five_font_adjust = -2 if _raw_s5_font is None else int(_raw_s5_font)
        _raw_s6_font = st.session_state.get(slide_six_font_adjust_key)
        current_slide_six_font_adjust = -2 if _raw_s6_font is None else int(_raw_s6_font)
        _known_cta_options = {"more", "article", "substack", "petition", "video", "custom link", "hidden"}
        _is_article = _is_article_url(source_url)
        default_slide_three_cta_option = "article" if (_is_article or _is_candidate_article_row(prompt_row or {})) else "hidden"
        raw_sheet_cta = _cell_text((prompt_row or {}).get("Slide CTA")).strip()
        default_slide_three_cta = raw_sheet_cta or default_slide_three_cta_option
        current_slide_three_cta = _cell_text(
            st.session_state.get(slide_three_cta_key, default_slide_three_cta)
        ).strip() or default_slide_three_cta
        if current_slide_three_cta.lower() not in _known_cta_options and not current_slide_three_cta:
            current_slide_three_cta = default_slide_three_cta
            st.session_state[slide_three_cta_key] = current_slide_three_cta
        current_speaker_name = _cell_text(
            st.session_state.get(f"workspace_speaker_row_{row_num}", speaker_name)
        ).strip()
        if _is_article:
            _article_domain = urlparse(source_url).netloc.lower().removeprefix("www.") if source_url else ""
            slide_handle = current_speaker_name.lstrip("@") or _article_domain
        else:
            if current_speaker_name:
                slide_handle = current_speaker_name
            else:
                u = username.strip().lstrip("@")
                slide_handle = f"@{u}" if u else ""
        last_cta_slide_number = 3
        for candidate_slide_number, candidate_text in (
            (6, slide_text6),
            (5, slide_text5),
            (4, slide_text4),
            (3, slide_text3),
        ):
            if (candidate_text or "").strip():
                last_cta_slide_number = candidate_slide_number
                break
        st.markdown('<div class="workspace-row-slides-anchor"></div>', unsafe_allow_html=True)
        if (slide_text1 or "").strip():
            _render_slide_one_preview(
                slide_handle,
                slide_text1,
                _drive_image_url(thumbnail_link) or thumbnail_link,
                current_slide_one_font_adjust,
                current_slide_one_background_adjust,
                current_slide_one_fit_mode,
                quote=slide_quote if st.session_state.get(slide_quote_show_key, True) else "",
                quote_font_adjust_px=int(st.session_state.get(slide_quote_font_adjust_key, 0) or 0),
            )
            current_quote_show = st.session_state.get(slide_quote_show_key, True)
            current_quote_font_adjust = int(st.session_state.get(slide_quote_font_adjust_key, 0) or 0)
            anchor_id = f"workspace-preview-ctrl-{row_num}_slide1"
            with st.container():
                st.markdown(f'<div id="{anchor_id}" class="workspace-preview-controls-anchor workspace-slide1-ctrl-anchor"></div>', unsafe_allow_html=True)
                _s1_col_count = 12 if slide_quote else 11
                s1_cols = st.columns(_s1_col_count, gap="small")
                with s1_cols[0]:
                    if st.button("Q-", key=f"workspace_quote_font_down_{row_num}", width="stretch"):
                        st.session_state[slide_quote_font_adjust_key] = max(-40, current_quote_font_adjust - 4)
                        st.session_state["workspace_preview_scroll_target"] = anchor_id
                        _rerun_workspace("Edit")
                with s1_cols[1]:
                    if st.button("Q+", key=f"workspace_quote_font_up_{row_num}", width="stretch"):
                        st.session_state[slide_quote_font_adjust_key] = min(40, current_quote_font_adjust + 4)
                        st.session_state["workspace_preview_scroll_target"] = anchor_id
                        _rerun_workspace("Edit")
                with s1_cols[2]:
                    if st.button("↑", key=f"workspace_preview_{row_num}_slide1_bg_up", width="stretch"):
                        st.session_state[slide_one_background_adjust_key] = max(-1200, current_slide_one_background_adjust - 48)
                        st.session_state["workspace_preview_scroll_target"] = anchor_id
                        _rerun_workspace("Edit")
                with s1_cols[3]:
                    if st.button("↓", key=f"workspace_preview_{row_num}_slide1_bg_down", width="stretch"):
                        st.session_state[slide_one_background_adjust_key] = min(1200, current_slide_one_background_adjust + 48)
                        st.session_state["workspace_preview_scroll_target"] = anchor_id
                        _rerun_workspace("Edit")
                with s1_cols[4]:
                    if st.button("A-", key=f"workspace_preview_{row_num}_slide1_font_down", width="stretch"):
                        st.session_state[slide_one_font_adjust_key] = max(-80, current_slide_one_font_adjust - 2)
                        st.session_state["workspace_preview_scroll_target"] = anchor_id
                        _rerun_workspace("Edit")
                with s1_cols[5]:
                    if st.button("A+", key=f"workspace_preview_{row_num}_slide1_font_up", width="stretch"):
                        st.session_state[slide_one_font_adjust_key] = min(200, current_slide_one_font_adjust + 2)
                        st.session_state["workspace_preview_scroll_target"] = anchor_id
                        _rerun_workspace("Edit")
                if slide_quote:
                    with s1_cols[6]:
                        if st.button("Quote", key=f"workspace_quote_edit_{row_num}", width="stretch"):
                            try:
                                _opts = _generate_quote_options_for_row(prompt_row or {})
                                st.session_state[f"workspace_quote_options_{row_num}"] = _opts
                                st.session_state[f"workspace_quote_picker_{row_num}"] = True
                            except Exception as _qe:
                                st.session_state["workspace_error"] = f"Row {row_num}: could not generate quotes — {describe_error(_qe)}"
                            _rerun_workspace("Edit")
                    with s1_cols[7]:
                        fit_label = "Fill" if current_slide_one_fit_mode else "Fit"
                        if st.button(fit_label, key=f"workspace_preview_{row_num}_slide1_fit_toggle", width="stretch"):
                            st.session_state[slide_one_fit_toggle_key] = not current_slide_one_fit_mode
                            st.session_state["workspace_preview_scroll_target"] = anchor_id
                            _rerun_workspace("Edit")
                    with s1_cols[8]:
                        hide_label = "Hide" if current_quote_show else "Show"
                        if st.button(hide_label, key=f"workspace_quote_toggle_{row_num}", width="stretch"):
                            slide_name = _cell_text((prompt_row or {}).get("name", "")).strip()
                            if current_quote_show:
                                merged_text1 = (slide_quote.strip() + " " + (slide_text1 or "").strip()).strip()
                                _write_specific_carousel_fields(row_num, {
                                    "name": slide_name,
                                    "text1": merged_text1, "text2": slide_text2,
                                    "text3": slide_text3, "text4": slide_text4,
                                    "text5": slide_text5, "text6": slide_text6,
                                })
                            else:
                                current_text1 = (slide_text1 or "").strip()
                                quote_prefix = slide_quote.strip() + " "
                                restored_text1 = current_text1[len(quote_prefix):] if current_text1.startswith(quote_prefix) else current_text1
                                _write_specific_carousel_fields(row_num, {
                                    "name": slide_name,
                                    "text1": restored_text1, "text2": slide_text2,
                                    "text3": slide_text3, "text4": slide_text4,
                                    "text5": slide_text5, "text6": slide_text6,
                                })
                            st.session_state[slide_quote_show_key] = not current_quote_show
                            _rerun_workspace("Edit")
                    with s1_cols[9]:
                        if st.button("Edit", key=f"workspace_quote_edit_btn_{row_num}", width="stretch"):
                            _open_workspace_slide_action_dialog(row_num, "quote")
                            _rerun_workspace("Edit")
                    with s1_cols[10]:
                        if st.button("Edit Text 1", key=f"workspace_inline_edit_text1_{row_num}", width="stretch"):
                            _open_workspace_slide_action_dialog(row_num, "text1")
                            _rerun_workspace("Edit")
                    with s1_cols[11]:
                        _is_blurred = bool(st.session_state.get("workspace_original_thumbnails", {}).get(str(row_num)))
                        _blur_label = "Unblur" if _is_blurred else "Blur"
                        if st.button(_blur_label, key=f"workspace_blur_thumb_{row_num}", width="stretch"):
                            try:
                                if _is_blurred:
                                    with st.spinner("Restoring original…"):
                                        _unblur_row_thumbnail(row)
                                    st.session_state["workspace_success"] = f"Row {row_num}: original thumbnail restored."
                                else:
                                    with st.spinner("Blurring thumbnail…"):
                                        _blur_row_thumbnail(row)
                                    st.session_state["workspace_success"] = f"Row {row_num}: thumbnail blurred."
                            except Exception as _be:
                                st.session_state["workspace_error"] = f"Row {row_num}: {'unblur' if _is_blurred else 'blur'} failed — {describe_error(_be)}"
                            _rerun_workspace("Edit")
                else:
                    with s1_cols[6]:
                        if st.button("Quote", key=f"workspace_quote_edit_{row_num}", width="stretch"):
                            try:
                                _opts = _generate_quote_options_for_row(prompt_row or {})
                                st.session_state[f"workspace_quote_options_{row_num}"] = _opts
                                st.session_state[f"workspace_quote_picker_{row_num}"] = True
                            except Exception as _qe:
                                st.session_state["workspace_error"] = f"Row {row_num}: could not generate quotes — {describe_error(_qe)}"
                            _rerun_workspace("Edit")
                    with s1_cols[7]:
                        fit_label = "Fill" if current_slide_one_fit_mode else "Fit"
                        if st.button(fit_label, key=f"workspace_preview_{row_num}_slide1_fit_toggle", width="stretch"):
                            st.session_state[slide_one_fit_toggle_key] = not current_slide_one_fit_mode
                            st.session_state["workspace_preview_scroll_target"] = anchor_id
                            _rerun_workspace("Edit")
                    with s1_cols[8]:
                        if st.button("Edit", key=f"workspace_quote_edit_btn_{row_num}", width="stretch"):
                            _open_workspace_slide_action_dialog(row_num, "quote")
                            _rerun_workspace("Edit")
                    with s1_cols[9]:
                        if st.button("Edit Text 1", key=f"workspace_inline_edit_text1_{row_num}", width="stretch"):
                            _open_workspace_slide_action_dialog(row_num, "text1")
                            _rerun_workspace("Edit")
                    with s1_cols[10]:
                        _is_blurred = bool(st.session_state.get("workspace_original_thumbnails", {}).get(str(row_num)))
                        _blur_label = "Unblur" if _is_blurred else "Blur"
                        if st.button(_blur_label, key=f"workspace_blur_thumb_{row_num}", width="stretch"):
                            try:
                                if _is_blurred:
                                    with st.spinner("Restoring original…"):
                                        _unblur_row_thumbnail(row)
                                    st.session_state["workspace_success"] = f"Row {row_num}: original thumbnail restored."
                                else:
                                    with st.spinner("Blurring thumbnail…"):
                                        _blur_row_thumbnail(row)
                                    st.session_state["workspace_success"] = f"Row {row_num}: thumbnail blurred."
                            except Exception as _be:
                                st.session_state["workspace_error"] = f"Row {row_num}: {'unblur' if _is_blurred else 'blur'} failed — {describe_error(_be)}"
                            _rerun_workspace("Edit")
            if st.session_state.get(f"workspace_quote_picker_{row_num}"):
                _quote_options = st.session_state.get(f"workspace_quote_options_{row_num}", [])
                _quote_sel = st.selectbox(
                    "Pick a quote",
                    _quote_options,
                    key=f"workspace_quote_select_{row_num}",
                    label_visibility="collapsed",
                )
                _qcols = st.columns(2, gap="small")
                with _qcols[0]:
                    if st.button("Use this", key=f"workspace_quote_use_{row_num}", type="primary"):
                        if update_quote is not None:
                            update_quote(GOOGLE_SHEET_ID, row_num, _quote_sel)
                        st.session_state.pop(f"workspace_quote_picker_{row_num}", None)
                        st.session_state.pop(f"workspace_quote_options_{row_num}", None)
                        st.session_state["workspace_success"] = f"Row {row_num}: quote saved."
                        _rerun_workspace("Edit")
                with _qcols[1]:
                    if st.button("Cancel", key=f"workspace_quote_cancel_{row_num}"):
                        st.session_state.pop(f"workspace_quote_picker_{row_num}", None)
                        st.session_state.pop(f"workspace_quote_options_{row_num}", None)
                        _rerun_workspace("Edit")
        if (slide_text2 or "").strip():
            _render_text_slide_preview(
                2,
                slide_text2,
                current_slide_two_font_adjust,
                include_link_cta=current_slide_two_cta != "hidden",
                link_cta_target=current_slide_two_cta,
                link_cta_text=_slide_three_cta_text(current_slide_two_cta, top_comment),
            )
            with st.container():
                st.markdown('<div class="workspace-slide2-ctrl-anchor"></div>', unsafe_allow_html=True)
                _s2_cols = st.columns(3, gap="small")
                with _s2_cols[0]:
                    if st.button("A-", key=f"workspace_preview_{row_num}_slide2_font_down", width="stretch"):
                        st.session_state[slide_two_font_adjust_key] = max(-80, current_slide_two_font_adjust - 2)
                        _rerun_workspace("Edit")
                with _s2_cols[1]:
                    if st.button("A+", key=f"workspace_preview_{row_num}_slide2_font_up", width="stretch"):
                        st.session_state[slide_two_font_adjust_key] = min(200, current_slide_two_font_adjust + 2)
                        _rerun_workspace("Edit")
                with _s2_cols[2]:
                    if st.button("Edit Text 2", key=f"workspace_inline_edit_text2_{row_num}", width="stretch"):
                        _open_workspace_slide_action_dialog(row_num, "text2")
                        _rerun_workspace("Edit")
            with st.popover("Slide 2 actions", use_container_width=True):
                if st.button("More link", key=f"workspace_row_slides_s2cta_more_{row_num}", width="stretch"):
                    st.session_state[slide_two_cta_key] = "more"
                    _rerun_workspace("Edit")
                if st.button("Video link", key=f"workspace_row_slides_s2cta_video_{row_num}", width="stretch"):
                    st.session_state[slide_two_cta_key] = "video"
                    _rerun_workspace("Edit")
                if st.button("Article link", key=f"workspace_row_slides_s2cta_article_{row_num}", width="stretch"):
                    st.session_state[slide_two_cta_key] = "article"
                    _rerun_workspace("Edit")
                if st.button("Petition link", key=f"workspace_row_slides_s2cta_petition_{row_num}", width="stretch"):
                    st.session_state[slide_two_cta_key] = "petition"
                    _rerun_workspace("Edit")
                if st.button("Link", key=f"workspace_row_slides_s2cta_custom_{row_num}", width="stretch"):
                    st.session_state[slide_two_cta_key] = "custom link"
                    _open_workspace_slide_action_dialog(row_num, "custom_link")
                    _rerun_workspace("Edit")
                if st.button("Hide link", key=f"workspace_row_slides_s2cta_hidden_{row_num}", width="stretch"):
                    st.session_state[slide_two_cta_key] = "hidden"
                    _rerun_workspace("Edit")
        if (slide_text3 or "").strip():
            _render_text_slide_preview(
                3,
                slide_text3,
                current_slide_three_font_adjust,
                include_link_cta=last_cta_slide_number == 3 and current_slide_three_cta != "hidden",
                link_cta_target=current_slide_three_cta,
                link_cta_text=_slide_three_cta_text(current_slide_three_cta, top_comment),
            )
            with st.container():
                st.markdown('<div class="workspace-slide3-ctrl-anchor"></div>', unsafe_allow_html=True)
                _s3_has_drive = bool(media_link)
                _s3_cols = st.columns(3 + (1 if _s3_has_drive else 0), gap="small")
                with _s3_cols[0]:
                    if st.button("A-", key=f"workspace_preview_{row_num}_slide3_font_down", width="stretch"):
                        st.session_state[slide_three_font_adjust_key] = max(-80, current_slide_three_font_adjust - 2)
                        _rerun_workspace("Edit")
                with _s3_cols[1]:
                    if st.button("A+", key=f"workspace_preview_{row_num}_slide3_font_up", width="stretch"):
                        st.session_state[slide_three_font_adjust_key] = min(200, current_slide_three_font_adjust + 2)
                        _rerun_workspace("Edit")
                with _s3_cols[2]:
                    if st.button("Edit Text 3", key=f"workspace_inline_edit_text3_{row_num}", width="stretch"):
                        _open_workspace_slide_action_dialog(row_num, "text3")
                        _rerun_workspace("Edit")
                if _s3_has_drive:
                    with _s3_cols[3]:
                        _drive_label = "Open Reel in Drive" if media_type.lower() == "reel" else "Open in Drive"
                        st.link_button(_drive_label, media_link, width="stretch")
        if (slide_text4 or "").strip():
            _render_text_slide_preview(
                4,
                slide_text4,
                current_slide_four_font_adjust,
                include_link_cta=last_cta_slide_number == 4 and current_slide_three_cta != "hidden",
                link_cta_target=current_slide_three_cta,
                link_cta_text=_slide_three_cta_text(current_slide_three_cta, top_comment),
            )
            _render_workspace_preview_control_bar(
                f"{row_num}_slide4",
                slide_four_font_adjust_key,
                current_slide_four_font_adjust,
            )
        if (slide_text5 or "").strip():
            _render_text_slide_preview(
                5,
                slide_text5,
                current_slide_five_font_adjust,
                include_link_cta=last_cta_slide_number == 5 and current_slide_three_cta != "hidden",
                link_cta_target=current_slide_three_cta,
                link_cta_text=_slide_three_cta_text(current_slide_three_cta, top_comment),
            )
            _render_workspace_preview_control_bar(
                f"{row_num}_slide5",
                slide_five_font_adjust_key,
                current_slide_five_font_adjust,
            )
        if (slide_text6 or "").strip():
            _render_text_slide_preview(
                6,
                slide_text6,
                current_slide_six_font_adjust,
                include_link_cta=last_cta_slide_number == 6 and current_slide_three_cta != "hidden",
                link_cta_target=current_slide_three_cta,
                link_cta_text=_slide_three_cta_text(current_slide_three_cta, top_comment),
            )
            _render_workspace_preview_control_bar(
                f"{row_num}_slide6",
                slide_six_font_adjust_key,
                current_slide_six_font_adjust,
            )
        with st.popover("Slide actions", use_container_width=True):
            if st.button("Generate prompt", key=f"workspace_row_slides_build_{row_num}", width="stretch"):
                base_row = prompt_row or {}
                effective_row = {
                    **base_row,
                    "Speaker Name": st.session_state.get(_workspace_speaker_key(base_row), base_row.get("Speaker Name", "")),
                    "Caption Context": st.session_state.get(_workspace_key(base_row, "context"), base_row.get("Caption Context", "")),
                }
                st.session_state[prompt_key] = _build_single_row_chatgpt_prompt(effective_row)
                _open_workspace_slide_action_dialog(row_num, "prompt")
                _rerun_workspace("Edit")
            if st.button("Generate quote", key=f"workspace_row_slides_gen_quote_{row_num}", width="stretch"):
                try:
                    generated_q = _generate_quote_for_row(prompt_row or {})
                    if update_quote is not None:
                        update_quote(GOOGLE_SHEET_ID, row_num, generated_q)
                    st.session_state["workspace_success"] = f"Row {row_num}: quote generated."
                except Exception as e:
                    st.session_state["workspace_error"] = f"Could not generate quote: {describe_error(e)}"
                _rerun_workspace("Edit")
            slides_merged = st.session_state.get(slide_merge_key, False)
            if slides_merged:
                if st.button("Break slides", key=f"workspace_row_slides_break_{row_num}", width="stretch"):
                    original_t3 = st.session_state.get(slide_merge_original_key, "")
                    slide_name = _cell_text((prompt_row or {}).get("name", "")).strip()
                    _write_specific_carousel_fields(row_num, {
                        "name": slide_name,
                        "text1": slide_text1, "text2": slide_text2,
                        "text3": original_t3,
                        "text4": slide_text4, "text5": slide_text5, "text6": slide_text6,
                    })
                    st.session_state.pop(slide_merge_key, None)
                    st.session_state.pop(slide_merge_original_key, None)
                    _rerun_workspace("Edit")
            elif (slide_text2 or "").strip() and (slide_text3 or "").strip():
                if st.button("Merge slides 2+3", key=f"workspace_row_slides_merge_{row_num}", width="stretch"):
                    original_t3 = (slide_text3 or "").strip()
                    merged_t3 = (slide_text2 or "").strip() + "\n\n" + original_t3
                    slide_name = _cell_text((prompt_row or {}).get("name", "")).strip()
                    _write_specific_carousel_fields(row_num, {
                        "name": slide_name,
                        "text1": slide_text1, "text2": slide_text2,
                        "text3": merged_t3,
                        "text4": slide_text4, "text5": slide_text5, "text6": slide_text6,
                    })
                    st.session_state[slide_merge_key] = True
                    st.session_state[slide_merge_original_key] = original_t3
                    _rerun_workspace("Edit")
            if (slide_text4 or "").strip() and st.button("Edit text 4", key=f"workspace_row_slides_edit_text4_{row_num}", width="stretch"):
                _open_workspace_slide_action_dialog(row_num, "text4")
                _rerun_workspace("Edit")
            if (slide_text5 or "").strip() and st.button("Edit text 5", key=f"workspace_row_slides_edit_text5_{row_num}", width="stretch"):
                _open_workspace_slide_action_dialog(row_num, "text5")
                _rerun_workspace("Edit")
            if (slide_text6 or "").strip() and st.button("Edit text 6", key=f"workspace_row_slides_edit_text6_{row_num}", width="stretch"):
                _open_workspace_slide_action_dialog(row_num, "text6")
                _rerun_workspace("Edit")
            if st.button("Link", key=f"workspace_row_slides_cta_custom_{row_num}", width="stretch"):
                _save_slide_three_cta_choice(row_num, slide_three_cta_key, "custom link")
                _open_workspace_slide_action_dialog(row_num, "custom_link")
                _rerun_workspace("Edit")
            if st.button("More link", key=f"workspace_row_slides_cta_more_{row_num}", width="stretch"):
                _save_slide_three_cta_choice(row_num, slide_three_cta_key, "more")
                _rerun_workspace("Edit")
            if st.button("Video link", key=f"workspace_row_slides_cta_video_{row_num}", width="stretch"):
                _save_slide_three_cta_choice(row_num, slide_three_cta_key, "video")
                _rerun_workspace("Edit")
            if st.button("Article link", key=f"workspace_row_slides_cta_article_{row_num}", width="stretch"):
                _save_slide_three_cta_choice(row_num, slide_three_cta_key, "article")
                _rerun_workspace("Edit")
            if st.button("Substack link", key=f"workspace_row_slides_cta_substack_{row_num}", width="stretch"):
                _save_slide_three_cta_choice(row_num, slide_three_cta_key, "substack")
                _rerun_workspace("Edit")
            if st.button("Update name", key=f"workspace_row_slides_edit_speaker_{row_num}", width="stretch"):
                _open_workspace_slide_action_dialog(row_num, "speaker")
                _rerun_workspace("Edit")
            if st.button("Hide link", key=f"workspace_row_slides_cta_hidden_{row_num}", width="stretch"):
                _save_slide_three_cta_choice(row_num, slide_three_cta_key, "hidden")
                _rerun_workspace("Edit")
        if (slide_text3 or "").strip():
            _tab_copy_preview(
                _caption_tab_value(
                    generated,
                    original_caption,
                    username,
                    top_comment,
                    required_hashtags,
                    is_instagram,
                ) or "(none)"
            )
            st.caption("Custom link text")
            st.code(top_comment or "(none)", language=None)
        if (slide_text1 or "").strip() and prompt_row:
            st.markdown("<div style='padding-top:100px'></div>", unsafe_allow_html=True)
            if st.button(
                "Delete row",
                key=f"workspace_slides_delete_{row_num}",
                width="stretch",
            ):
                try:
                    _delete_workspace_row(prompt_row)
                except Exception as e:
                    st.session_state["workspace_error"] = f"Row {row_num}: could not delete row - {describe_error(e)}"
                else:
                    st.session_state["workspace_success"] = f"Row {row_num}: deleted from the sheet."
                _rerun_workspace("Edit")


def _icon_copy_button(label: str, value: str) -> None:
    escaped_label = html.escape(label)
    clipboard_text = json.dumps(value or "")
    button_html = f"""
    <button
      onclick='navigator.clipboard.writeText({clipboard_text})'
      title='Copy {escaped_label}'
      style="
        width: 100%;
        min-height: 3rem;
        border: 1px solid rgba(15,23,42,0.14);
        border-radius: 14px;
        background: white;
        color: #0f172a;
        font-size: 1.15rem;
        font-weight: 700;
        cursor: pointer;
      "
    >💬</button>
    """
    st.html(button_html)


def _copy_caption_button(value: str) -> None:
    clipboard_text = json.dumps(value or "")
    button_html = f"""
    <button
      onclick='navigator.clipboard.writeText({clipboard_text})'
      title='Copy caption'
      style="
        width: 100%;
        min-height: 3rem;
        border: 1px solid rgba(15,23,42,0.14);
        border-radius: 14px;
        background: white;
        color: #0f172a;
        font-size: 1.15rem;
        font-weight: 700;
        cursor: pointer;
      "
    >💬</button>
    """
    st.html(button_html)


def _move_selected_row(editor_rows: list[dict], step: int) -> None:
    if not editor_rows:
        return
    row_numbers = [row["row_number"] for row in editor_rows]
    current = st.session_state.get("workspace_selected_row_num", row_numbers[0])
    if current not in row_numbers:
        current = row_numbers[0]
    current_index = row_numbers.index(current)
    next_index = max(0, min(len(row_numbers) - 1, current_index + step))
    st.session_state["workspace_selected_row_num"] = row_numbers[next_index]


def _is_sheets_read_quota_error(error: Exception) -> bool:
    message = str(error)
    return (
        "Quota exceeded for quota metric 'Read requests'" in message
        or "Read requests per minute per user" in message
        or "Exceeded in a metric read request" in message
    )


def _run_with_sheet_quota_countdown(fn, waiting_label: str):
    while True:
        try:
            return fn()
        except Exception as e:
            if not _is_sheets_read_quota_error(e):
                raise
            countdown = st.empty()
            for remaining in range(60, 0, -1):
                countdown.warning(f"{waiting_label} Sheets read quota hit. Retrying in {remaining}s.")
                time.sleep(1)
            countdown.empty()


def _transcribe_reel_from_drive(row: dict) -> str | None:
    """Download reel from Drive and transcribe with Whisper. Returns transcript text or None."""
    media_link = _cell_text(row.get("Media Drive Link")).strip()
    if not media_link:
        return None
    row_num = row["row_number"]
    tmp_dir = tempfile.mkdtemp(prefix="workspace_transcribe_")
    try:
        try:
            metadata = get_drive_file_metadata(media_link)
            filename = metadata.get("name") or f"row_{row_num}.mp4"
        except Exception:
            filename = f"row_{row_num}.mp4"
        local_path = os.path.join(tmp_dir, filename)
        download_drive_file(media_link, local_path)
        return transcribe_video(local_path)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


SAFE_DELETE_SUBFOLDER = "safe_for_deletion"


def _cleanup_orphaned_preview_folders(all_rows: list[dict]) -> int:
    """Move orphaned Drive files and preview subfolders into safe_for_deletion."""
    if not GOOGLE_DRIVE_FOLDER_ID:
        return 0
    service = _get_service()

    # Collect all active Drive file IDs from every media link in the sheet.
    active_file_ids: set[str] = set()
    for row in all_rows:
        for link in _cell_text(row.get("Media Drive Link")).split(","):
            link = link.strip()
            if link:
                fid = extract_drive_file_id(link)
                if fid:
                    active_file_ids.add(fid)
        for link in _cell_text(row.get("Thumbnail Drive Link")).split(","):
            link = link.strip()
            if link:
                fid = extract_drive_file_id(link)
                if fid:
                    active_file_ids.add(fid)

    # Compute expected preview subfolder names from active rows.
    expected_folder_names: set[str] = set()
    for row in all_rows:
        media_link = _cell_text(row.get("Media Drive Link")).strip().split(",")[0].strip()
        if not media_link:
            continue
        username = _cell_text(row.get("Source Username")).strip().lstrip("@")
        handle_text = _cell_text(row.get("Speaker Name")).strip()
        try:
            folder_name, _ = _preview_folder_base_name(username or handle_text, media_link, row["row_number"])
            expected_folder_names.add(folder_name)
        except Exception:
            pass

    # List all items directly in the main Drive folder (files and subfolders).
    query = (
        f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents and "
        "trashed = false"
    )
    result = service.files().list(
        q=query,
        fields="files(id,name,mimeType)",
        pageSize=1000,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    all_items = result.get("files", [])

    orphan_files: list[dict] = []
    orphan_folders: list[dict] = []
    for item in all_items:
        if item["name"] == SAFE_DELETE_SUBFOLDER:
            continue
        if item["mimeType"] == "application/vnd.google-apps.folder":
            if item["name"] not in expected_folder_names:
                orphan_folders.append(item)
        else:
            if item["id"] not in active_file_ids:
                orphan_files.append(item)

    if not orphan_files and not orphan_folders:
        return 0

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_root_id = get_or_create_subfolder(GOOGLE_DRIVE_FOLDER_ID, SAFE_DELETE_SUBFOLDER)
    archive_folder_id = get_or_create_subfolder(safe_root_id, timestamp)

    moved = 0
    for item in orphan_files + orphan_folders:
        try:
            service.files().update(
                fileId=item["id"],
                addParents=archive_folder_id,
                removeParents=GOOGLE_DRIVE_FOLDER_ID,
                fields="id,parents",
                supportsAllDrives=True,
            ).execute()
            kind = "folder" if item["mimeType"] == "application/vnd.google-apps.folder" else "file"
            st.write(f"Moved to safe_for_deletion ({kind}): {item['name']}")
            moved += 1
        except Exception as e:
            st.warning(f"Could not move '{item['name']}': {describe_error(e)}")
    return moved


def _run_all_steps() -> None:
    """Ingest new rows, transcribe untranscribed reels, and split newly ingested reel videos."""
    # Capture pending rows before ingesting so we know which ones are new
    try:
        pending_before = _run_with_sheet_quota_countdown(
            lambda: get_pending_rows(GOOGLE_SHEET_ID),
            "Run all paused (sheet quota):",
        )
        pending_row_nums = {r["row_number"] for r in pending_before}
    except Exception:
        pending_row_nums = set()

    # Step 1: Ingest + auto-caption new rows
    with st.status("Step 1: Ingesting new rows…", expanded=True) as s:
        try:
            processed = _process_pending_rows_from_sheet()
            s.update(label=f"Step 1: Ingested {processed} new row(s)", state="complete")
        except Exception as e:
            s.update(label=f"Step 1 error: {describe_error(e)}", state="error")

    # Reload sheet after ingest
    try:
        all_rows = _run_with_sheet_quota_countdown(
            lambda: get_all_rows(GOOGLE_SHEET_ID),
            "Run all paused (sheet quota):",
        )
    except Exception as e:
        st.error(f"Could not reload sheet after ingest: {describe_error(e)}")
        return

    # Step 2: Transcribe untranscribed reels directly with Whisper
    untranscribed = [
        r for r in all_rows
        if r.get("Media Type", "").strip().lower() == "reel"
        and not r.get("Transcript", "").strip()
        and r.get("Media Drive Link", "").strip()
    ]
    if untranscribed:
        with st.status(f"Step 2: Transcribing {len(untranscribed)} reel(s) with Whisper…", expanded=True) as s2:
            succeeded = 0
            for i, row in enumerate(untranscribed, 1):
                row_num = row["row_number"]
                username = _cell_text(row.get("Source Username")).strip() or f"row {row_num}"
                s2.update(label=f"Step 2: Transcribing {i}/{len(untranscribed)} — {username} (row {row_num})…")
                try:
                    st.write(f"Downloading row {row_num}…")
                    transcript = _transcribe_reel_from_drive(row)
                    if transcript:
                        st.write(f"Row {row_num}: transcript received ({len(transcript)} chars), saving…")
                        update_transcript(GOOGLE_SHEET_ID, row_num, transcript)
                        updated_row = dict(row)
                        updated_row["Transcript"] = transcript
                        caption = generate_row_caption(updated_row)
                        next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
                        update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
                        st.write(f"Row {row_num}: done.")
                        succeeded += 1
                    else:
                        st.warning(f"Row {row_num}: Whisper returned no transcript")
                except Exception as e:
                    st.warning(f"Row {row_num}: {describe_error(e)}")
            s2.update(label=f"Step 2: Transcribed {succeeded}/{len(untranscribed)} reel(s)", state="complete")

    # Step 3: Split and upload all reels with a media link into their preview folders
    reels_to_split = [
        r for r in all_rows
        if r.get("Media Type", "").strip().lower() == "reel"
        and r.get("Media Drive Link", "").strip()
    ]
    if reels_to_split:
        with st.status(f"Step 3: Splitting {len(reels_to_split)} reel(s)…", expanded=True) as s3:
            split_succeeded = 0
            for i, row in enumerate(reels_to_split, 1):
                row_num = row["row_number"]
                username = _cell_text(row.get("Source Username")).strip() or f"row {row_num}"
                s3.update(label=f"Step 3: Splitting {i}/{len(reels_to_split)} — {username} (row {row_num})…")
                try:
                    media_link = _cell_text(row.get("Media Drive Link")).strip().split(",")[0].strip()
                    username_clean = _cell_text(row.get("Source Username")).strip().lstrip("@")
                    handle_text = _cell_text(row.get("Speaker Name")).strip()
                    preview_folder_id, _, _ = _ensure_preview_folder(row_num, username_clean, handle_text, media_link)
                    if _preview_folder_has_splits(preview_folder_id):
                        st.write(f"Row {row_num}: already split, skipping.")
                        split_succeeded += 1
                        continue
                    st.write(f"Row {row_num}: downloading and splitting…")
                    _upload_split_videos(media_link, preview_folder_id, mode="fill")
                    st.write(f"Row {row_num}: done.")
                    split_succeeded += 1
                except Exception as e:
                    st.warning(f"Row {row_num}: {describe_error(e)}")
            s3.update(label=f"Step 3: Split {split_succeeded}/{len(reels_to_split)} reel(s)", state="complete")

    # Step 4: Move orphaned files and preview folders in Drive to safe_for_deletion
    with st.status("Step 4: Cleaning up orphaned Drive files and folders…", expanded=True) as s4:
        try:
            trashed = _cleanup_orphaned_preview_folders(all_rows)
            s4.update(label=f"Step 4: Moved {trashed} orphaned item(s) to safe_for_deletion", state="complete")
        except Exception as e:
            s4.update(label=f"Step 4 cleanup error: {describe_error(e)}", state="error")

    st.session_state["workspace_success"] = "Run all complete."


def _process_pending_rows_from_sheet() -> int:
    pending = _run_with_sheet_quota_countdown(
        lambda: get_pending_rows(GOOGLE_SHEET_ID),
        "Processing new rows paused:",
    )
    if not pending:
        return 0

    progress = st.progress(0)
    for i, row in enumerate(pending):
        row_num = row["row_number"]
        label = row["Instagram URL"][:60]
        with st.status(f"Row {row_num}: {label}", expanded=False) as status_box:
            result = _ingest_row(row)
            try:
                update_ingest_result(
                    GOOGLE_SHEET_ID,
                    row_num,
                    result["username"],
                    result["media_type"],
                    result["photo_count"],
                    result["media_link"],
                    result["thumbnail_link"],
                    result["original_caption"],
                    result["transcript"],
                    result["status"],
                )
                existing_inputs = _current_row_caption_inputs(row)
                sheet_top_comment = _cell_text(row.get("Top Comment", "")).strip()
                row_url = _cell_text(row.get("Instagram URL")).strip()
                if result["status"] == "ingested":
                    if result["media_type"] == "article" and not existing_inputs["Caption Context"].strip():
                        existing_inputs["Caption Context"] = result["original_caption"]
                    if result["media_type"] == "article":
                        default_top_comment = _build_read_cta(row_url) if not sheet_top_comment else sheet_top_comment
                    elif _is_instagram_url(row_url):
                        default_top_comment = _build_watch_cta(result["username"], row_url)
                    else:
                        default_top_comment = sheet_top_comment or existing_inputs["Top Comment"]
                else:
                    default_top_comment = sheet_top_comment or existing_inputs["Top Comment"]

                update_metadata(
                    GOOGLE_SHEET_ID,
                    row_num,
                    existing_inputs["Caption Context"],
                    existing_inputs["Speaker Name"],
                    existing_inputs["Required Hashtags"],
                    default_top_comment,
                    "",
                )
                if result["status"] == "ingested":
                    ingested_row = dict(row)
                    ingested_row.update(
                        {
                            "Source Username": result["username"],
                            "Media Type": result["media_type"],
                            "Photo Count": result["photo_count"],
                            "Media Drive Link": result["media_link"],
                            "Thumbnail Drive Link": result["thumbnail_link"],
                            "Original Caption": result["original_caption"],
                            "Transcript": result["transcript"],
                            "Status": result["status"],
                            "Caption Context": existing_inputs["Caption Context"],
                            "Speaker Name": existing_inputs["Speaker Name"],
                            "Required Hashtags": existing_inputs["Required Hashtags"],
                            "Top Comment": default_top_comment,
                            "Footer": "",
                        }
                    )
                    if result["media_type"] == "photo":
                        ingested_row = _ensure_photo_post_source_text(ingested_row)
                    if row_ready_for_caption(ingested_row):
                        generated_caption = generate_row_caption(ingested_row)
                        if update_caption_and_metadata is not None:
                            update_caption_and_metadata(
                                GOOGLE_SHEET_ID,
                                row_num,
                                generated_caption,
                                result["status"],
                                existing_inputs["Caption Context"],
                                existing_inputs["Speaker Name"],
                                existing_inputs["Required Hashtags"],
                                default_top_comment,
                                "",
                            )
                        else:
                            update_caption(GOOGLE_SHEET_ID, row_num, generated_caption, result["status"])
            except Exception as e:
                status_box.update(label=f"Row {row_num}: error writing to sheet - {describe_error(e)}", state="error")
            else:
                if result["status"].startswith("error"):
                    status_box.update(label=f"Row {row_num}: {result['status']}", state="error")
                else:
                    action_word = "ingested + captioned" if row_ready_for_caption(ingested_row) else "ingested"
                    display_name = f"@{result['username']}" if result["username"] and result["media_type"] != "article" else result["username"]
                    status_box.update(
                        label=(
                            f"Row {row_num}: {action_word} - {display_name} ({result['media_type']})"
                        ),
                        state="complete",
                    )
        progress.progress((i + 1) / len(pending))

    return len(pending)


def _append_url_and_get_new_row(url: str, required_hashtags: str = "") -> dict:
    cleaned_url = (url or "").strip()
    if not cleaned_url:
        raise ValueError("URL is required.")

    before_rows = get_all_rows(GOOGLE_SHEET_ID)
    before_row_numbers = {int(row.get("row_number") or 0) for row in before_rows if row.get("row_number")}
    append_link_rows(GOOGLE_SHEET_ID, [cleaned_url], required_hashtags)
    after_rows = get_all_rows(GOOGLE_SHEET_ID)

    new_rows = [
        row for row in after_rows
        if int(row.get("row_number") or 0) not in before_row_numbers
        and _cell_text(row.get("Instagram URL")).strip() == cleaned_url
    ]
    if not new_rows:
        matching_rows = [
            row for row in after_rows
            if _cell_text(row.get("Instagram URL")).strip() == cleaned_url
        ]
        if matching_rows:
            return max(matching_rows, key=lambda row: int(row.get("row_number") or 0))
        raise ValueError("Could not find the newly appended sheet row.")

    return max(new_rows, key=lambda row: int(row.get("row_number") or 0))


def _process_single_url_to_editor(url: str, required_hashtags: str = "") -> int:
    row = _append_url_and_get_new_row(url, required_hashtags)
    row_num = int(row["row_number"])

    result = _ingest_row(row)
    if result["status"] != "ingested":
        raise ValueError(result["status"])

    default_top_comment = ""
    row_url = _cell_text(row.get("Instagram URL")).strip()
    if result["media_type"] == "article":
        default_top_comment = _build_read_cta(row_url)
    elif _is_instagram_url(row_url):
        default_top_comment = _build_watch_cta(result["username"], row_url)

    update_ingest_result(
        GOOGLE_SHEET_ID,
        row_num,
        result["username"],
        result["media_type"],
        result["photo_count"],
        result["media_link"],
        result["thumbnail_link"],
        result["original_caption"],
        result["transcript"],
        result["status"],
    )
    update_metadata(
        GOOGLE_SHEET_ID,
        row_num,
        "",
        "",
        required_hashtags,
        default_top_comment,
        "",
    )

    working_row = _reload_row_from_sheet(row_num)
    media_type = _cell_text(working_row.get("Media Type")).strip().lower()
    if _is_reel_url(row_url):
        _process_post_online(working_row)
    elif media_type == "photo":
        _process_photo_post_online(working_row)
    else:
        generated_caption = generate_row_caption(working_row)
        update_caption(GOOGLE_SHEET_ID, row_num, generated_caption, "done")
        working_row = _reload_row_from_sheet(row_num)
        if not _carousel_has_required_text(
            {
                "name": _cell_text(working_row.get("name")).strip(),
                "text1": _cell_text(working_row.get("text1")).strip(),
                "text2": _cell_text(working_row.get("text2")).strip(),
                "text3": _cell_text(working_row.get("text3")).strip(),
            }
        ):
            carousel = _generate_reliable_carousel_copy(working_row, model="gpt-5.2")
            _write_specific_carousel_fields(row_num, carousel)

    return row_num


def _ingest_row(row: dict) -> dict:
    """Process one row through ingest and return sheet fields."""
    url = row["Instagram URL"].strip()
    tmp_dir = None
    try:
        if _is_article_url(url):
            article = _fetch_article_source_data(url)
            article_source_text = (
                (article.get("source_text") or "").strip()
                or (article.get("summary_text") or "").strip()
            )
            article_username = article.get("domain", "")
            return {
                "username": article_username,
                "media_type": "article",
                "photo_count": "",
                "media_link": "",
                "thumbnail_link": _upload_article_thumbnail(article.get("image_url", ""), row.get("row_number"), article_username),
                "original_caption": article_source_text,
                "transcript": article_source_text,
                "status": "ingested",
            }
        if _is_reel_url(url):
            data = process_reel_url(url, include_transcript=False)
        else:
            data = process_post_url(url)
        filename_prefix = build_filename_prefix(row.get("row_number"), data.get("username", ""))
        uploaded = upload_media_bundle(data, filename_prefix=filename_prefix)
        tmp_dir = uploaded["tmp_dir"]

        return {
            "username": data["username"],
            "media_type": data["media_type"],
            "photo_count": data["photo_count"],
            "media_link": uploaded["media_link"],
            "thumbnail_link": uploaded["thumbnail_link"],
            "original_caption": data["original_caption"],
            "transcript": data["transcript"],
            "status": "ingested",
        }
    except Exception as e:
        return {
            "username": "",
            "media_type": "",
            "photo_count": "",
            "media_link": "",
            "thumbnail_link": "",
            "original_caption": "",
            "transcript": "",
            "status": f"error: {describe_error(e)}",
        }
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _rerun_with_transcript(row: dict, force_remote: bool = False) -> bool:
    updated_row = _fetch_row_with_transcript(row, force_remote=force_remote)
    if not (updated_row.get("Transcript") or "").strip():
        return False
    row_num = row["row_number"]
    caption = generate_row_caption(updated_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    return True


def _fetch_row_with_transcript(row: dict, download_media: bool = False, force_remote: bool = False) -> dict:
    url = row.get("Instagram URL", "").strip()
    if not _is_reel_url(url):
        raise ValueError("Transcript rerun is only available for reels.")

    row_num = row["row_number"]
    existing_transcript = (row.get("Transcript") or "").strip()
    if existing_transcript and not download_media and not force_remote:
        updated_row = dict(row)
        updated_row["Transcript"] = existing_transcript
        return updated_row

    if existing_transcript and download_media and not force_remote:
        _download_media_to_drive(row)
        updated_row = dict(row)
        updated_row["Transcript"] = existing_transcript
        return updated_row

    tmp_dir = None
    try:
        refreshed = process_reel_url(url, include_transcript=True)
        transcript = (refreshed.get("transcript") or "").strip()
        uploaded = {
            "media_link": row.get("Media Drive Link", ""),
            "thumbnail_link": row.get("Thumbnail Drive Link", ""),
        }
        if download_media:
            filename_prefix = build_filename_prefix(row_num, refreshed.get("username") or row.get("Source Username", ""))
            uploaded = upload_media_bundle(refreshed, filename_prefix=filename_prefix)
            tmp_dir = uploaded["tmp_dir"]
            media_row_for_whisper = dict(row)
            media_row_for_whisper["Media Drive Link"] = uploaded.get("media_link", "") or row.get("Media Drive Link", "")
            media_row_for_whisper["Thumbnail Drive Link"] = uploaded.get("thumbnail_link", "") or row.get("Thumbnail Drive Link", "")
            if not transcript:
                transcript = (_transcribe_reel_from_drive(media_row_for_whisper) or "").strip()
            status_value = (row.get("Status") or "").strip() or "ingested"
            update_ingest_result(
                GOOGLE_SHEET_ID,
                row_num,
                refreshed.get("username") or row.get("Source Username", ""),
                refreshed.get("media_type") or row.get("Media Type", ""),
                refreshed.get("photo_count") or row.get("Photo Count", ""),
                uploaded.get("media_link", "") or row.get("Media Drive Link", ""),
                uploaded.get("thumbnail_link", "") or row.get("Thumbnail Drive Link", ""),
                refreshed.get("original_caption") or row.get("Original Caption", ""),
                transcript,
                status_value,
            )
        else:
            if not transcript:
                transcript = (_transcribe_reel_from_drive(row) or "").strip()
            if transcript:
                update_transcript(GOOGLE_SHEET_ID, row_num, transcript)

        updated_row = dict(row)
        updated_row["Transcript"] = transcript
        updated_row["Source Username"] = refreshed.get("username") or updated_row.get("Source Username", "")
        updated_row["Original Caption"] = refreshed.get("original_caption") or updated_row.get("Original Caption", "")
        updated_row["Media Type"] = refreshed.get("media_type") or updated_row.get("Media Type", "")
        updated_row["Photo Count"] = refreshed.get("photo_count") or updated_row.get("Photo Count", "")
        updated_row["Media Drive Link"] = uploaded.get("media_link", "") or updated_row.get("Media Drive Link", "")
        updated_row["Thumbnail Drive Link"] = uploaded.get("thumbnail_link", "") or updated_row.get("Thumbnail Drive Link", "")
        return updated_row
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _download_media_to_drive(row: dict) -> None:
    url = row.get("Instagram URL", "").strip()
    if not url:
        raise ValueError("This row does not have an Instagram URL.")

    tmp_dir = None
    try:
        if _is_reel_url(url):
            data = process_reel_url(url, include_transcript=False)
        else:
            data = process_post_url(url)
        filename_prefix = build_filename_prefix(row.get("row_number"), data.get("username", ""))
        uploaded = upload_media_bundle(data, filename_prefix=filename_prefix)
        tmp_dir = uploaded["tmp_dir"]
        update_ingest_result(
            GOOGLE_SHEET_ID,
            row["row_number"],
            data["username"],
            data["media_type"],
            data["photo_count"],
            uploaded["media_link"],
            uploaded["thumbnail_link"],
            data["original_caption"] or row.get("Original Caption", ""),
            row.get("Transcript", ""),
            row.get("Status", "") or "ingested",
        )
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _extract_image_text(row: dict) -> str:
    media_type = (row.get("Media Type", "") or "").strip().lower()
    if media_type != "photo":
        raise ValueError("Image text extraction is only available for photo or carousel posts.")

    links = [link.strip() for link in (row.get("Media Drive Link", "") or "").split(",") if link.strip()]
    if not links:
        raise ValueError("This row does not have image media links in Drive yet.")

    url = (row.get("Instagram URL") or "").strip()
    image_indexes = list(range(len(links)))
    if url:
        try:
            latest = process_post_url(url)
            media_kinds = latest.get("media_kinds") or []
            filtered_indexes = [i for i, kind in enumerate(media_kinds[: len(links)]) if kind == "image"]
            if filtered_indexes:
                image_indexes = filtered_indexes
        except Exception:
            pass

    image_links = [links[i] for i in image_indexes if i < len(links)]
    if not image_links:
        raise ValueError("This row does not have any image slides available for OCR.")

    content = [{
        "type": "text",
        "text": "Extract all readable text from these images. Return plain text only, in reading order. No labels or commentary.",
    }]
    for link in image_links[:10]:
        view_url = _drive_view_url(link)
        if view_url:
            content.append({"type": "image_url", "image_url": {"url": view_url}})

    if len(content) == 1:
        raise ValueError("Could not build image URLs for OCR.")

    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured.")
    response = _get_client().chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": content}],
        max_tokens=800,
        temperature=0,
    )
    text = response.choices[0].message.content.strip()
    if not text:
        raise ValueError("No text found in the images.")
    return text


def _ensure_photo_post_source_text(row: dict) -> dict:
    working_row = dict(row)
    if not _row_is_photo_post(working_row):
        return working_row
    if _cell_text(working_row.get("Transcript")).strip() or _cell_text(working_row.get("Caption Context")).strip():
        return working_row

    row_num = working_row["row_number"]
    if not _cell_text(working_row.get("Media Drive Link")).strip():
        _download_media_to_drive(working_row)
        working_row = _reload_row_from_sheet(row_num)

    extracted_text = _extract_image_text(working_row)
    update_caption_context(GOOGLE_SHEET_ID, row_num, extracted_text)
    update_transcript(GOOGLE_SHEET_ID, row_num, extracted_text)
    working_row["Caption Context"] = extracted_text
    working_row["Transcript"] = extracted_text
    return working_row


def _redo_caption_from_image_text(row: dict) -> None:
    extracted_text = _extract_image_text(row)
    row_num = row["row_number"]
    update_caption_context(GOOGLE_SHEET_ID, row_num, extracted_text)
    update_transcript(GOOGLE_SHEET_ID, row_num, extracted_text)

    updated_row = dict(row)
    updated_row["Caption Context"] = extracted_text
    updated_row["Transcript"] = extracted_text
    caption = generate_row_caption(updated_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    _write_carousel_fields(row_num, updated_row)


def _row_is_photo_post(row: dict) -> bool:
    url = _cell_text(row.get("Instagram URL")).strip()
    return _is_instagram_url(url) and not _is_reel_url(url)


def _generate_caption_for_row(row: dict) -> None:
    working_row = _ensure_photo_post_source_text(row)
    row_num = working_row["row_number"]
    current_inputs = _current_row_caption_inputs(working_row)
    update_metadata(
        GOOGLE_SHEET_ID,
        row_num,
        current_inputs["Caption Context"],
        current_inputs["Speaker Name"],
        current_inputs["Required Hashtags"],
        current_inputs["Top Comment"],
        "",
    )
    updated_row = dict(working_row)
    updated_row.update(current_inputs)
    caption = generate_row_caption(updated_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    if _row_is_photo_post(updated_row):
        _write_carousel_fields(row_num, updated_row)


def _generate_quote_options_for_row(row: dict) -> list[str]:
    transcript = _cell_text(row.get("Transcript")).strip()
    original_caption = _cell_text(row.get("Original Caption")).strip()
    generated = _cell_text(row.get("Generated Caption")).strip()
    source_text = transcript or original_caption
    if not source_text:
        raise ValueError("No transcript or caption available to generate quotes from.")
    source = f"{source_text}\n\n---\n\nCaption:\n{generated}" if generated else source_text
    response = _get_client().chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": (
                    "You generate pull quotes and punchy headlines for social media graphics. "
                    "Return EXACTLY 10 options, one per line, numbered 1–10. "
                    "Options 1–7 must be verbatim pull quotes lifted directly from the source text — "
                    "real sentences or phrases spoken or written in the source, unchanged. "
                    "Options 8–10 must be salacious, attention-grabbing rewritten headlines (not verbatim). "
                    "Each option must be under 120 characters. "
                    "No quotation marks, no attribution, no extra commentary — just the numbered list."
                ),
            },
            {"role": "user", "content": source},
        ],
        max_tokens=700,
        temperature=0.85,
    )
    raw = response.choices[0].message.content.strip()
    options: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^\d+[.)]\s*", "", line).strip().strip('"').strip("'").strip().rstrip(".")
        if line:
            options.append(line)
    return options[:10]


def _generate_quote_for_row(row: dict) -> str:
    """Extract the single best pull-quote from the row's transcript or caption."""
    transcript = _cell_text(row.get("Transcript")).strip()
    original_caption = _cell_text(row.get("Original Caption")).strip()
    source = transcript or original_caption
    if not source:
        raise ValueError("No transcript or caption available to generate a quote from.")
    client = _get_client()
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are extracting a single pull-quote from source material for a social media graphic. "
                    "Return ONLY the verbatim quote text — no quotation marks, no attribution, no explanation. "
                    "Pick the most punchy, surprising, or emotionally resonant direct quote from the source. "
                    "It should be short enough to display as large text: ideally under 120 characters. "
                    "If the source has no strong direct quotes, write a tight paraphrase in the speaker's voice."
                ),
            },
            {"role": "user", "content": source},
        ],
        max_tokens=120,
        temperature=0.3,
    )
    return response.choices[0].message.content.strip().strip('"').strip("'").strip().rstrip(".")


def _write_specific_carousel_fields(row_number: int, carousel: dict[str, str]) -> None:
    if update_carousel_fields is None:
        return
    update_carousel_fields(
        GOOGLE_SHEET_ID,
        row_number,
        carousel.get("name", ""),
        _single_paragraph_slide_text(carousel.get("text1")),
        _single_paragraph_slide_text(carousel.get("text2")),
        _single_paragraph_slide_text(carousel.get("text3")),
        _single_paragraph_slide_text(carousel.get("text4")),
        _single_paragraph_slide_text(carousel.get("text5")),
        _single_paragraph_slide_text(carousel.get("text6")),
    )
    quote = (carousel.get("quote") or "").strip().strip('"').strip("'").rstrip(".")
    if quote and update_quote is not None:
        update_quote(GOOGLE_SHEET_ID, row_number, quote)


def _carousel_has_required_text(carousel: dict[str, str]) -> bool:
    return bool(
        _cell_text(carousel.get("text1")).strip()
        and _cell_text(carousel.get("text2")).strip()
        and _cell_text(carousel.get("text3")).strip()
    )


def _generate_reliable_carousel_copy(row: dict, model: str = "gpt-5.2") -> dict[str, str]:
    carousel = generate_carousel_copy_with_model(row, model=model)
    if _carousel_has_required_text(carousel):
        return carousel

    batch_results = generate_batch_carousel_copy_with_model([row], model=model)
    batch_carousel = batch_results.get(int(row.get("row_number") or 0), {})
    if _carousel_has_required_text(batch_carousel):
        return batch_carousel

    raise ValueError("Slide generation returned incomplete text.")


def _verify_carousel_fields_saved(row_number: int) -> dict[str, str]:
    rows = get_all_rows(GOOGLE_SHEET_ID)
    saved_row = next((item for item in rows if int(item.get("row_number") or 0) == row_number), None)
    if not saved_row:
        raise ValueError("Processed row could not be reloaded from the sheet.")
    saved_carousel = {
        "name": _cell_text(saved_row.get("name")).strip(),
        "text1": _cell_text(saved_row.get("text1")).strip(),
        "text2": _cell_text(saved_row.get("text2")).strip(),
        "text3": _cell_text(saved_row.get("text3")).strip(),
    }
    if not _carousel_has_required_text(saved_carousel):
        raise ValueError("Slide fields were not saved to the sheet.")
    return saved_carousel


def _reload_row_from_sheet(row_number: int) -> dict:
    rows = get_all_rows(GOOGLE_SHEET_ID)
    reloaded = next((item for item in rows if int(item.get("row_number") or 0) == row_number), None)
    if not reloaded:
        raise ValueError("Processed row could not be reloaded from the sheet.")
    return reloaded


def _process_post_online(row: dict) -> None:
    row_num = row["row_number"]
    has_media = bool(_cell_text(row.get("Media Drive Link")).strip())
    existing_transcript = _cell_text(row.get("Transcript")).strip()
    updated_row = _fetch_row_with_transcript(
        row,
        download_media=not has_media,
        force_remote=not bool(existing_transcript),
    )
    current_inputs = _current_row_caption_inputs(updated_row)
    update_metadata(
        GOOGLE_SHEET_ID,
        row_num,
        current_inputs["Caption Context"],
        current_inputs["Speaker Name"],
        current_inputs["Required Hashtags"],
        current_inputs["Top Comment"],
        "",
    )
    updated_row.update(current_inputs)

    existing_caption = _cell_text(updated_row.get("Generated Caption")).strip()
    caption = existing_caption or generate_row_caption(updated_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    if not existing_caption:
        update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    updated_row["Generated Caption"] = caption
    updated_row["Status"] = next_status

    existing_carousel = {
        "name": _cell_text(updated_row.get("name")).strip(),
        "text1": _cell_text(updated_row.get("text1")).strip(),
        "text2": _cell_text(updated_row.get("text2")).strip(),
        "text3": _cell_text(updated_row.get("text3")).strip(),
    }
    if _carousel_has_required_text(existing_carousel):
        return

    carousel = _generate_reliable_carousel_copy(updated_row, model="gpt-5.2")
    _write_specific_carousel_fields(row_num, carousel)
    _verify_carousel_fields_saved(row_num)
    st.session_state.pop(f"workspace_preview_upload_links_{row_num}", None)


def _process_photo_post_online(row: dict) -> None:
    working_row = _ensure_photo_post_source_text(row)
    row_num = working_row["row_number"]

    current_inputs = _current_row_caption_inputs(working_row)
    update_metadata(
        GOOGLE_SHEET_ID,
        row_num,
        current_inputs["Caption Context"],
        current_inputs["Speaker Name"],
        current_inputs["Required Hashtags"],
        current_inputs["Top Comment"],
        "",
    )
    working_row.update(current_inputs)

    existing_caption = _cell_text(working_row.get("Generated Caption")).strip()
    caption = existing_caption or generate_row_caption(working_row)
    next_status = "skipped" if (row.get("Status", "") or "").strip().lower() == "skipped" else "done"
    if not existing_caption:
        update_caption(GOOGLE_SHEET_ID, row_num, caption, next_status)
    working_row["Generated Caption"] = caption
    working_row["Status"] = next_status

    existing_carousel = {
        "name": _cell_text(working_row.get("name")).strip(),
        "text1": _cell_text(working_row.get("text1")).strip(),
        "text2": _cell_text(working_row.get("text2")).strip(),
        "text3": _cell_text(working_row.get("text3")).strip(),
    }
    if _carousel_has_required_text(existing_carousel):
        return

    carousel = _generate_reliable_carousel_copy(working_row, model="gpt-5.2")
    _write_specific_carousel_fields(row_num, carousel)
    _verify_carousel_fields_saved(row_num)


def _queue_workspace_action(row_number: int, action: str) -> None:
    queue = st.session_state.setdefault("workspace_action_queue", [])
    queue.append({"row_number": row_number, "action": action})


def _rerun_workspace(tab: str | None = None) -> None:
    if tab:
        if tab in {"Edit", "Grid"}:
            tab = "Home"
        st.session_state["_workspace_pending_tab"] = tab
    st.rerun()


def _substack_promote_context(
    url: str,
    focus_topic: str,
    context_request: str = "",
    article_topics: list[str] | None = None,
) -> str:
    cleaned_focus_topic = _cell_text(focus_topic).strip()
    payload = {
        "source": "substack_promote",
        "url": _cell_text(url).strip(),
        "focus_topic": cleaned_focus_topic,
        "angle": cleaned_focus_topic,
        "context_request": _cell_text(context_request).strip(),
        "article_topics": [
            _cell_text(topic).strip()
            for topic in (article_topics or [])
            if _cell_text(topic).strip()
        ],
    }
    return f"{_SUBSTACK_PROMOTE_META_PREFIX}{json.dumps(payload, separators=(',', ':'))}"


def _parse_substack_promote_context(value: str) -> dict:
    raw = _cell_text(value).strip()
    if not raw.startswith(_SUBSTACK_PROMOTE_META_PREFIX):
        return {}
    try:
        payload = json.loads(raw[len(_SUBSTACK_PROMOTE_META_PREFIX):])
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _substack_topic_breakdown_prompt() -> str:
    return (
        "You are preparing a reusable topic breakdown for Vote In Or Out.\n\n"
        "Read the full article all the way through and identify the 10 most salacious, clickbait-worthy, interesting topics a reader would care about.\n"
        "Do not just copy the first few nouns or phrases from the opening lines.\n"
        "Look for conflict, scandal, stakes, named people, named events, named policies, surprising claims, sharp contrasts, legal fights, election drama, campaign weaknesses, controversies, money, corruption, rights, power, and anything else in the article that would genuinely make someone want to click.\n"
        "Prefer proper names, named events, named institutions, named offices, named policies, accusations, fights, rulings, scandals, and concrete controversies over abstract summaries.\n"
        "Return EXACTLY 10 topic strings in rank order from most interesting/clickable to least.\n"
        "Each string must be 1 to 5 words.\n"
        "Use concrete article topics, not vague labels.\n"
        "Good examples: \"Zohran Mamdani\", \"Project 2025\", \"California governor race\", \"abortion rights\", \"Supreme Court ruling\", \"ICE raids\", \"union vote\", \"candidate flip-flop\", \"donor money\", \"ethics probe\".\n"
        "Bad examples: \"emergencies\", \"political evolution\", \"cognitive biases\", \"article overview\", \"politics\", \"news\", \"voter information\".\n"
        "Return valid JSON when possible. Preferred format: an array of strings. Also acceptable: an object with a \"topics\" array.\n"
        "No duplicates. No numbering. No markdown. No commentary outside JSON."
    )


def _normalize_substack_topics(raw_topics: object) -> list[str]:
    if not isinstance(raw_topics, list):
        return []
    seen: set[str] = set()
    cleaned: list[str] = []
    for raw_topic in raw_topics:
        topic = _single_paragraph_slide_text(raw_topic).strip(" ,.;:-")
        if not topic:
            continue
        if len(topic.split()) > 5:
            continue
        topic_key = topic.lower()
        if topic_key in seen:
            continue
        seen.add(topic_key)
        cleaned.append(topic)
    return cleaned[:15]


def _substack_topic_options(raw_topics: object) -> list[str]:
    normalized = _normalize_substack_topics(raw_topics)
    if not normalized:
        return []
    return ["High-level overview", *normalized]


def _extract_substack_topics_from_model_output(raw_value: str) -> list[str]:
    raw = _cell_text(raw_value).strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        try:
            payload = _extract_json_payload(raw)
        except Exception:
            payload = None

    if isinstance(payload, list):
        topics = _normalize_substack_topics(payload)
        if topics:
            return topics
    if isinstance(payload, dict):
        for key in ("topics", "topic_breakdown", "topicBreakdown", "items"):
            topics = _normalize_substack_topics(payload.get(key))
            if topics:
                return topics

    lines = []
    for part in re.split(r"[\n|,]", raw):
        cleaned = re.sub(r"^\s*[-*0-9.)]+\s*", "", part).strip()
        if cleaned:
            lines.append(cleaned)
    return _normalize_substack_topics(lines)


def _parse_substack_topic_breakdown(raw_value: str) -> list[str]:
    return _extract_substack_topics_from_model_output(raw_value)


def _build_substack_slide_handoff(
    focus_topic: str,
    context_request: str,
    article_topics: list[str],
    article_body: str,
    substack_url: str,
) -> tuple[str, str]:
    focus_topic = _cell_text(focus_topic).strip()
    context_request = _cell_text(context_request).strip()
    article_topics = _normalize_substack_topics(article_topics)
    prompt = (
        "Return ONLY valid JSON as an array. No markdown, no commentary outside JSON.\n\n"
        "Each object must include exactly: row_number, name, text1, text2, text3, text4, text5, text6\n\n"
        "Create a 6-slide Instagram carousel for Vote In Or Out promoting a Substack election article.\n"
        "Keep row_number exactly as shown in the input.\n"
        "Use plain language, no hashtags, no citations, no markdown, and no newline characters inside values.\n"
        "Each slide should be self-contained and specific.\n"
        "text1 is the strongest opening slide under 350 characters. Lead with the most emotionally compelling verified quote, allegation, consequence, contradiction, or fact. Write it like a viral news headline — prioritize emotion, conflict, consequences, and curiosity over explanation. text1 must make the viewer urgently want to read slide 2.\n"
        "text2, text3, text4, and text5 are semi-longer explainer slides, usually 500 to 800 characters each.\n"
        "text6 is the closing slide under 500 characters. It should point people to the full article without adding a URL.\n"
        "Every text2-text5 slide must include at least one concrete piece of data from the article: a date, number, office, jurisdiction, candidate name, quote, poll, vote margin, dollar amount, legal status, or other specific fact.\n"
        "Do not write generic summary slides. Pull details directly from the article and distribute them across the six slides.\n"
        "No em dashes, emojis, hashtags, paragraph breaks, or newline characters inside text fields.\n"
        "Collapse all whitespace into normal single spaces before returning JSON.\n"
        "No speculation or invented framing.\n"
        "Never repeat the same fact, quote, setup, accusation, or disclaimer across slides.\n\n"
    )
    prompt += (
        "Focus the carousel on the selected article topic.\n"
        "Use the extra user context only as direction, not as a source of new facts.\n"
        'On the final slide, say the full article covers this topic and more, and name at least two other article topics when possible.\n'
        'Set the "name" field to "vote in or out substack".\n'
    )

    slide_input = (
        "ROW [ROW_NUMBER]\n"
        f"Substack URL: {substack_url}\n"
        f"Focus topic: {focus_topic or '(infer from article)'}\n"
        f"Article topics: {', '.join(article_topics) if article_topics else '(infer from article)'}\n"
        f"Extra context from user: {context_request or '(none)'}\n\n"
        "Slide requirement: focus on the selected topic, use concrete article data points across slides 2 through 5, and explicitly say the full article covers this topic and more on slide 6.\n\n"
        + f"Article:\n{article_body}"
    )
    return prompt.strip(), slide_input.strip()


def _format_substack_slide_prompt(slide_prompt: str, slide_input: str, row_number: int) -> str:
    input_block = (slide_input or "").replace("[ROW_NUMBER]", str(row_number))
    return f"{slide_prompt.strip()}\n\n{input_block.strip()}"


def _format_substack_slide_prompt_preview(slide_prompt: str, slide_input: str, row_number: int) -> str:
    input_block = (slide_input or "").replace("[ROW_NUMBER]", str(row_number)).strip()
    article_marker = "\n\nArticle:\n"
    if article_marker in input_block:
        input_block = input_block.split(article_marker, 1)[0].rstrip()
    return f"{slide_prompt.strip()}\n\n{input_block}".strip()


def _substack_caption_footer(substack_url: str) -> str:
    return (
        f"Comment LINK (on instagram) and we will DM you the link to {substack_url}\n\n"
        "Help this information get to more voters. 🇺🇸 "
        "A well-informed electorate is a prerequisite to Democracy.—Thomas Jefferson"
    )


def _ensure_substack_caption_footer(caption: str, substack_url: str) -> str:
    footer = _substack_caption_footer(substack_url)
    cleaned = _cell_text(caption).strip()
    existing_cta = re.search(r"\bComment\s+\w+\s+\(on instagram\)", cleaned)
    if existing_cta:
        cleaned = cleaned[:existing_cta.start()].strip()
    body_parts = [part.strip() for part in re.split(r"\n\s*\n", cleaned) if part.strip()]
    if len(body_parts) > 2:
        cleaned = "\n\n".join([body_parts[0], " ".join(body_parts[1:])])
    return f"{cleaned}\n\n{footer}".strip()


def _generate_substack_caption_from_slides(
    substack_url: str,
    article_body: str,
    focus_topic: str,
    context_request: str,
    article_topics: list[str],
    slides: dict,
) -> str:
    slide_lines = []
    for slide_key in ("text1", "text2", "text3", "text4", "text5", "text6"):
        slide_text = _cell_text(slides.get(slide_key)).strip()
        if slide_text:
            slide_lines.append(f"{slide_key.upper()}: {slide_text}")
    slide_summary = "\n".join(slide_lines)
    response = _get_client().chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are writing an Instagram caption for Vote In Or Out that promotes a Substack article after the slide copy is finalized.\n"
                    "Use the finalized slides as the primary guide for the caption's angle and summary.\n"
                    "Use the article only to verify facts and add one or two concrete details.\n"
                    "Write in third person. Do not use I, me, my, we, us, our, or ours outside of a short direct quote from the source.\n"
                    "Write exactly two short paragraphs before the required CTA/footer.\n"
                    "The first paragraph should summarize the main point clearly and specifically.\n"
                    "The second paragraph should add concrete context and make clear the full article covers this topic and more.\n"
                    "No hashtags, no emojis, no bullet points, no markdown, no links in the body.\n"
                    "End with the exact required CTA/footer provided by the user."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Focus topic: {focus_topic}\n"
                    f"Article topics: {', '.join(_normalize_substack_topics(article_topics)) or '(infer from article)'}\n"
                    f"Extra context from user: {context_request or '(none)'}\n\n"
                    f"Finalized slides:\n{slide_summary}\n\n"
                    f"Article:\n{article_body}\n\n"
                    f"Required CTA/footer:\n{_substack_caption_footer(substack_url)}"
                ),
            },
        ],
        max_tokens=700,
    )
    return _ensure_substack_caption_footer(response.choices[0].message.content or "", substack_url)


def _substack_slide_result(raw_text: str, fallback_row_number: int) -> dict:
    payload = _extract_json_payload(raw_text)
    items = payload if isinstance(payload, list) else [payload]
    dict_items = [item for item in items if isinstance(item, dict)]
    if not dict_items:
        raise ValueError("Paste one JSON object or an array containing one slide result.")

    selected = None
    for item in dict_items:
        try:
            if int(item.get("row_number")) == int(fallback_row_number):
                selected = item
                break
        except Exception:
            continue
    if selected is None:
        selected = dict_items[0]

    return {
        "name": "vote in or out substack",
        "text1": _single_paragraph_slide_text(selected.get("text1")),
        "text2": _single_paragraph_slide_text(selected.get("text2")),
        "text3": _single_paragraph_slide_text(selected.get("text3")),
        "text4": _single_paragraph_slide_text(selected.get("text4")),
        "text5": _single_paragraph_slide_text(selected.get("text5")),
        "text6": _single_paragraph_slide_text(selected.get("text6")),
    }


def _mark_workspace_action_complete(row_number: int, action: str) -> None:
    completed = st.session_state.setdefault("workspace_action_completed", {})
    completed[f"{row_number}:{action}"] = True


def _is_workspace_action_complete(row_number: int, action: str) -> bool:
    completed = st.session_state.setdefault("workspace_action_completed", {})
    return bool(completed.get(f"{row_number}:{action}"))


def _process_next_workspace_action(for_row_number: int | None = None) -> None:
    queue = st.session_state.setdefault("workspace_action_queue", [])
    if not queue:
        return
    if for_row_number is not None and queue[0]["row_number"] != for_row_number:
        return

    current = queue.pop(0)
    row_number = current["row_number"]
    action = current["action"]

    rows = _run_with_sheet_quota_countdown(
        lambda: get_all_rows(GOOGLE_SHEET_ID),
        "Queued edit action paused:",
    )
    row = next((r for r in rows if r.get("row_number") == row_number), None)
    if not row:
        st.session_state[f"workspace_row_error_{row_number}"] = f"Row {row_number}: row not found in sheet."
        if queue:
            _rerun_workspace("Edit")
        return

    try:
        if action == "process_post":
            row_url = _cell_text(row.get("Instagram URL")).strip()
            is_reel = _is_reel_url(row_url)
            with st.status(
                f"Transcribing and generating caption for row {row_number}…" if is_reel
                else f"Generating caption for row {row_number}…",
                expanded=True,
            ) as _s:
                if is_reel:
                    st.write("Downloading reel and transcribing with Whisper…")
                    _process_post_online(row)
                    success_message = f"Row {row_number}: processed with transcript, caption, and slide copy."
                else:
                    st.write("Extracting post text and generating caption…")
                    _process_photo_post_online(row)
                    success_message = f"Row {row_number}: processed with caption and slide copy."
                _s.update(label=success_message, state="complete")
        elif action == "transcript":
            with st.status(f"Refreshing transcript for row {row_number}…", expanded=True) as _s:
                st.write("Fetching transcript from Whisper…")
                transcript_found = _rerun_with_transcript(row, force_remote=True)
                if transcript_found:
                    success_message = f"Row {row_number}: transcript rerun complete."
                else:
                    success_message = f"Row {row_number}: no transcript found."
                _s.update(label=success_message, state="complete")
        elif action == "generate_caption":
            with st.status(f"Generating caption for row {row_number}…", expanded=True) as _s:
                st.write("Generating caption…")
                _generate_caption_for_row(row)
                success_message = f"Row {row_number}: caption generated."
                _s.update(label=success_message, state="complete")
        elif action == "image_text":
            with st.status(f"Extracting image text for row {row_number}…", expanded=True) as _s:
                st.write("Reading image text and regenerating caption…")
                _redo_caption_from_image_text(row)
                success_message = f"Row {row_number}: caption regenerated from image text."
                _s.update(label=success_message, state="complete")
        elif action == "refresh_thumbnail_5s":
            with st.status(f"Updating screenshot for row {row_number}…", expanded=True) as _s:
                st.write("Extracting frame at 5 seconds…")
                _refresh_row_thumbnail_from_video(row, offset_seconds=5.0)
                success_message = f"Row {row_number}: screenshot updated from 5 seconds into the video."
                _s.update(label=success_message, state="complete")
        elif action == "split_video_fit":
            with st.status(f"Splitting video for row {row_number}…", expanded=True) as _s:
                media_links = [
                    lnk.strip()
                    for lnk in _cell_text(row.get("Media Drive Link")).split(",")
                    if lnk.strip()
                ]
                if not media_links:
                    raise ValueError("No media link found for this row.")
                media_link = media_links[0]
                username = _cell_text(row.get("Source Username")).strip().lstrip("@")
                handle_text = _cell_text(row.get("Speaker Name")).strip()
                st.write("Downloading video and splitting into segments…")
                preview_folder_id, _, _ = _ensure_preview_folder(row_number, username, handle_text, media_link)
                _upload_split_videos(media_link, preview_folder_id, mode="fit")
                success_message = f"Row {row_number}: video scaled to fit and uploaded to Drive."
                _s.update(label=success_message, state="complete")
        else:
            raise ValueError(f"Unknown action: {action}")
        _mark_workspace_action_complete(row_number, action)
        st.session_state[f"workspace_row_success_{row_number}"] = success_message
    except Exception as e:
        st.session_state[f"workspace_row_error_{row_number}"] = f"Row {row_number}: {describe_error(e)}"

    _rerun_workspace("Edit")


def _delete_workspace_row(row: dict) -> None:
    row_number = row["row_number"]
    delete_sheet_row(GOOGLE_SHEET_ID, row_number)
    pending_transcribe_resets = st.session_state.get("workspace_transcribe_reset_rows", [])
    if pending_transcribe_resets:
        st.session_state["workspace_transcribe_reset_rows"] = [
            pending for pending in pending_transcribe_resets if pending != _workspace_key(row, "transcribe")
        ]
    # Re-key the blur map: remove the deleted row's entry and shift all higher row numbers
    # down by 1 so blur state stays aligned with the sheet after rows renumber.
    try:
        shift_original_thumbnails_after_delete(GOOGLE_SHEET_ID, row_number)
    except Exception:
        pass
    session_thumbs = st.session_state.get("workspace_original_thumbnails", {})
    session_thumbs.pop(str(row_number), None)
    st.session_state["workspace_original_thumbnails"] = {
        str(int(k) - 1) if k.isdigit() and int(k) > row_number else k: v
        for k, v in session_thumbs.items()
    }
    _clear_workspace_row_state(row)


def _write_carousel_fields(row_number: int, row: dict) -> None:
    if update_carousel_fields is None:
        return
    carousel = generate_carousel_copy(row)
    update_carousel_fields(
        GOOGLE_SHEET_ID,
        row_number,
        carousel.get("name", ""),
        carousel.get("text1", ""),
        carousel.get("text2", ""),
        carousel.get("text3", ""),
        carousel.get("text4", ""),
        carousel.get("text5", ""),
        carousel.get("text6", ""),
    )
    if carousel.get("quote") and update_quote is not None:
        _clean_quote = (carousel["quote"] or "").strip().strip('"').strip("'").rstrip(".")
        if _clean_quote:
            update_quote(GOOGLE_SHEET_ID, row_number, _clean_quote)


def _row_ready_for_chatgpt(row: dict) -> bool:
    status = _cell_text(row.get("Status")).strip().lower()
    if status.startswith("error") or status == "slides":
        return False
    slide_fields = (
        "text1",
        "text2",
        "text3",
        "text4",
        "text5",
        "text6",
    )
    if any(_cell_text(row.get(field)).strip() for field in slide_fields):
        return False
    normalized_row = dict(row)
    normalized_row["Instagram URL"] = _cell_text(row.get("Instagram URL")).strip()
    normalized_row["Media Type"] = _cell_text(row.get("Media Type")).strip()
    normalized_row["Transcript"] = _cell_text(row.get("Transcript")).strip()
    normalized_row["Original Caption"] = _cell_text(row.get("Original Caption")).strip()
    normalized_row["Caption Context"] = _cell_text(row.get("Caption Context")).strip()
    return row_ready_for_caption(normalized_row)


def _chatgpt_ready_rows(sheet_id: str) -> list[dict]:
    return [row for row in get_all_rows(sheet_id) if _row_ready_for_chatgpt(row)]


def _ready_rows_from_loaded_rows(rows: list[dict]) -> list[dict]:
    return [row for row in rows if _row_ready_for_chatgpt(row)]


def _build_chatgpt_handoff_prompt(rows: list[dict]) -> str:
    blocks: list[str] = []
    for row in rows:
        working_row = _ensure_photo_post_source_text(row)
        row_num = working_row["row_number"]
        username = _cell_text(working_row.get("Source Username")).strip() or "unknown"
        media_type = _cell_text(working_row.get("Media Type")).strip().lower() or "post"
        transcript = _cell_text(working_row.get("Transcript")).strip()
        original_caption = _cell_text(working_row.get("Original Caption")).strip()
        caption_context = _cell_text(working_row.get("Caption Context")).strip()
        speaker_name = _cell_text(working_row.get("Speaker Name")).strip()
        blocks.append(
            "\n".join(
                [
                    f"ROW {row_num}",
                    f"username: {username}",
                    f"media_type: {media_type}",
                    f"speaker_name: {speaker_name or '(none)'}",
                    f"transcript:\n{transcript or '(none)'}",
                    f"original_caption:\n{original_caption or '(none)'}",
                    f"caption_context:\n{caption_context or '(none)'}",
                ]
            )
        )

    instructions = (
        "Return ONLY valid JSON as an array. No markdown, no commentary outside JSON.\n\n"

        "Each object must include: row_number, name, quote, text1, text2, text3\n\n"

        "Mandatory research step before writing:\n"
        "* For every row with a current event, public figure, legal case, government action, investigation, company, or breaking news claim, search online for reliable context before writing.\n"
        "* Use search to verify names, dates, charges, court rulings, dollar amounts, locations, and status of claims.\n"
        "* Prefer primary sources, Reuters, AP, local public radio, court records, official statements, and reputable outlets.\n"
        "* Do not add unverified claims. If context cannot be verified, stay close to the supplied transcript and caption.\n"
        "* Never cite sources in the JSON output. Use research only to improve accuracy and context.\n\n"

        + "Rules:\n"
        "* Keep row_number exactly as shown\n"
        "* No markdown, no commentary outside JSON\n"
        "* Plain straight double quotes only, no smart quotes\n"
        + pipeline_caption_ops.carousel_slide_rules()
        + "Quality check before final output:\n"
        "* Confirm every object has exactly row_number, name, quote, text1, text2, text3\n"
        "* Confirm character limits are respected\n"
        "* Confirm text is not too short when more verified context exists\n"
        "* Confirm no field repeats another field\n"
        "* Confirm no hashtags, em dashes, smart quotes, markdown, newlines, or source citations appear\n"
        "* Confirm every quote is verbatim from supplied text\n\n"

        "Output format example:\n"
        "[\n"
        "  {\n"
        '    "row_number": 1,\n'
        '    "name": "nowthis",\n'
        '    "quote": "We could abolish medical debt 10 times over.",\n'
        '    "text1": "The line frames the central contrast: billions flowing into military spending while families still face unpaid medical bills, coverage gaps, and debt that can follow them for years.",\n'
        '    "text2": "The argument connects military funding, healthcare costs, Medicaid pressure, and lobbying money into one political charge: Washington keeps finding money for war while ordinary people are told basic care is too expensive. The strongest details should be names, dollar amounts, dates, and direct claims from the source material.",\n'
        '    "text3": "The fallout is political as much as financial. The carousel should leave viewers with the real stakes: who benefits from federal spending choices, who absorbs the cost, and why healthcare debt remains unresolved even when Congress approves massive spending elsewhere."\n'
        "  }\n"
        "]\n"
    )
    return instructions + "\n\n" + "\n\n---\n\n".join(blocks)


def _build_generic_chatgpt_prompt(row: dict) -> str:
    """Build a source-agnostic slides + caption prompt: no speaker, extended research, neutral caption, no CTA."""
    working_row = _ensure_photo_post_source_text(row)
    row_num = working_row["row_number"]
    username = _cell_text(working_row.get("Source Username")).strip() or "unknown"
    media_type = _cell_text(working_row.get("Media Type")).strip().lower() or "post"
    transcript = _cell_text(working_row.get("Transcript")).strip()
    original_caption = _cell_text(working_row.get("Original Caption")).strip()
    caption_context = _cell_text(working_row.get("Caption Context")).strip()
    hashtags = _cell_text(working_row.get("Required Hashtags")).strip()

    row_block = "\n".join([
        "ROW new",
        f"username: {username}",
        f"media_type: {media_type}",
        "speaker_name: (none — do not attribute to any speaker)",
        f"transcript:\n{transcript or '(none)'}",
        f"original_caption:\n{original_caption or '(none)'}",
        f"caption_context:\n{caption_context or '(none)'}",
    ])

    hashtag_note = (
        f"\nRequired hashtags to include in the caption: {hashtags}\n"
        if hashtags else ""
    )

    instructions = (
        "You are creating a standalone, source-agnostic informative carousel post.\n\n"
        "CRITICAL: This post must NOT mention, credit, quote, or attribute anything to the original speaker "
        "or the source of the content below. Do not name the speaker. Do not reference the clip, interview, "
        "speech, or original post in any way.\n\n"
        "Instead: identify the underlying topic or main person/subject the content is ABOUT, "
        "and write the post as if it is original research on that topic.\n\n"
        "Mandatory extended research step before writing:\n"
        "* Identify the core topic or main person of interest from the content below.\n"
        "* Search online extensively for additional facts, data, dates, numbers, context, and recent "
        "developments on this topic.\n"
        "* Pull in verified statistics, timelines, key figures, and relevant background that "
        "strengthens the post.\n"
        "* Prefer primary sources, Reuters, AP, government records, court documents, and reputable outlets.\n"
        "* Do not add unverified claims. If context cannot be verified, stay close to the supplied content.\n"
        "* Never cite sources in the JSON output. Use research only to improve accuracy and depth.\n\n"
        "Return ONLY valid JSON as an array. No markdown, no commentary outside JSON.\n\n"
        "Each object must include: row_number, name, quote, text1, text2, text3, generated_caption\n\n"
        "Rules:\n"
        "* Keep row_number exactly as shown\n"
        "* No markdown, no commentary outside JSON\n"
        "* Plain straight double quotes only, no smart quotes\n"
        + pipeline_caption_ops.carousel_slide_rules()
        + hashtag_note
        + "\nCaption rules:\n"
        "Write a neutral, third-person informative caption under 1300 characters using exactly two simple "
        "paragraphs.\n\n"
        "Never write in first person. Do not use I, me, my, mine, we, us, our, or ours unless inside "
        "a verified direct quote from a named public source. Stay in third person.\n\n"
        "The first paragraph must be 250 characters or fewer and serve as the most important summary. "
        "It must include all required hashtags plus 3 to 5 relevant hashtags total. "
        "Prioritize hashtags for the main subject or topic, then subject-area hashtags for discovery. "
        "Replace the normal word or phrase in the sentence with the hashtag version. "
        "Do not add a separate hashtag-only line at the end.\n\n"
        "The second paragraph adds context using verified facts, dates, and numbers. "
        "Do not refer to any transcript, clip, speech, interview, or video. "
        "Write as if describing the underlying topic directly.\n\n"
        "Do NOT include any call to action asking readers to comment or DM for a link. "
        "Do not include any line about 'Comment LINK', 'Say LINK', 'DM', or any link-retrieval "
        "instructions.\n\n"
        "\nQuality check before final output:\n"
        "* Confirm no reference to the original speaker appears anywhere in the output\n"
        "* Confirm no reference to a clip, transcript, speech, interview, or video\n"
        "* Confirm the post reads as original research on the topic, not a summary of someone's content\n"
        "* Confirm no call-to-action about commenting, DMing, or retrieving a link\n"
        "* Confirm every object has exactly row_number, name, quote, text1, text2, text3, generated_caption\n"
        "* Confirm character limits are respected\n"
        "* Confirm no hashtags, em dashes, smart quotes, markdown, or newlines in slide fields\n\n"
    )
    return instructions + "\n\n" + row_block


def _create_generic_post_from_result(original_row: dict, raw_text: str) -> int:
    """Append a new source-agnostic post row from a generic slide result; leaves the original row unchanged."""
    payload = _extract_json_payload(raw_text)
    items = payload if isinstance(payload, list) else [payload]
    dict_items = [item for item in items if isinstance(item, dict)]
    if not dict_items:
        raise ValueError("Paste one JSON object or an array containing one slide result.")

    original_row_num = original_row.get("row_number")
    selected = None
    for item in dict_items:
        try:
            if int(item.get("row_number")) == int(original_row_num):
                selected = item
                break
        except Exception:
            continue
    if selected is None:
        selected = dict_items[0]

    raw_name = _cell_text(selected.get("name")).strip()
    if not (raw_name or selected.get("text1") or selected.get("text2") or selected.get("text3")):
        raise ValueError("No name or slide text values were found in the pasted result.")

    caption = _cell_text(selected.get("generated_caption")).strip()
    # Fallback JSON parsers return raw escape sequences — normalize \n to real newlines
    caption = caption.replace("\\n", "\n")
    if caption:
        footer = DEFAULT_POST_FOOTER.strip()
        if footer:
            caption = f"{caption}\n\n{footer}"

    append_manual_post_row(GOOGLE_SHEET_ID, {
        "url": "",
        "source_username": "",
        "caption_context": caption,
        "original_caption": caption,
        "media_type": "",
        "media_link": "",
        "thumbnail_link": "",
        "speaker_name": "",
        "status": "ingested",
        "name": raw_name,
        "text1": _single_paragraph_slide_text(selected.get("text1")),
        "text2": _single_paragraph_slide_text(selected.get("text2")),
        "text3": _single_paragraph_slide_text(selected.get("text3")),
        "text4": _single_paragraph_slide_text(selected.get("text4")),
        "text5": _single_paragraph_slide_text(selected.get("text5")),
        "text6": _single_paragraph_slide_text(selected.get("text6")),
    })

    all_rows = _run_with_sheet_quota_countdown(
        lambda: get_all_rows(GOOGLE_SHEET_ID),
        "Create generic post paused (sheet quota):",
    )
    if not all_rows:
        raise RuntimeError("Could not retrieve rows after creating generic post.")
    new_row_num = all_rows[-1]["row_number"]

    if caption:
        update_caption(GOOGLE_SHEET_ID, new_row_num, caption, "done")
    else:
        update_status(GOOGLE_SHEET_ID, new_row_num, "done")

    return new_row_num


_SLIDE_KEYS = ["row_number", "name", "quote", "text1", "text2", "text3", "text4", "text5", "text6", "generated_caption"]


def _normalize_slide_paste(text: str) -> str:
    """Rebuild messy slide paste as valid JSON using known field names as anchors."""
    text = text.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
    raw = text.strip().lstrip("[").rstrip("]").strip()
    blocks = re.split(r"}\s*,\s*{", raw)
    key_pat = '"(' + "|".join(re.escape(k) for k in _SLIDE_KEYS) + r')"\s*:\s*'
    out: list[dict] = []
    for block in blocks:
        matches = list(re.finditer(key_pat, block))
        if not matches:
            continue
        item: dict = {}
        for i, m in enumerate(matches):
            key = m.group(1)
            val_start = m.end()
            val_end = matches[i + 1].start() if i + 1 < len(matches) else len(block)
            raw_val = block[val_start:val_end].strip().rstrip(",}] ").strip()
            if key == "row_number":
                num = re.search(r"\d+", raw_val)
                if num:
                    item["row_number"] = int(num.group())
            else:
                if raw_val.startswith('"'):
                    raw_val = raw_val[1:]
                if raw_val.endswith('"'):
                    raw_val = raw_val[:-1]
                item[key] = raw_val
        if item:
            out.append(item)
    if not out:
        raise ValueError("No slide items found.")
    return json.dumps(out)


def _extract_json_payload(raw_text: str):
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("Paste a JSON result first.")

    def _strip_comments(candidate: str) -> str:
        without_block_comments = re.sub(r"/\*[\s\S]*?\*/", "", candidate)
        return re.sub(r"(?m)^\s*//.*$", "", without_block_comments)

    def _extract_block(candidate: str) -> str:
        candidate = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate.strip(), flags=re.IGNORECASE | re.MULTILINE)
        candidate = _strip_comments(candidate)
        match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", candidate)
        return match.group(1) if match else candidate

    def _escape_string_newlines(s: str) -> str:
        result = []
        in_string = False
        i = 0
        while i < len(s):
            c = s[i]
            if not in_string:
                if c == '"':
                    in_string = True
                result.append(c)
            else:
                if c == '\\':
                    result.append(c)
                    i += 1
                    if i < len(s):
                        result.append(s[i])
                elif c == '"':
                    in_string = False
                    result.append(c)
                elif c == '\n':
                    result.append('\\n')
                elif c == '\r':
                    result.append('\\r')
                elif c == '\t':
                    result.append('\\t')
                else:
                    result.append(c)
            i += 1
        return ''.join(result)

    def _repair_jsonish(candidate: str) -> str:
        repaired = _escape_string_newlines(candidate.strip())
        repaired = repaired.replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")
        repaired = _strip_comments(repaired)
        repaired = re.sub(r",\s*([}\]])", r"\1", repaired)
        repaired = re.sub(
            r'([{\[,]\s*)(#?[A-Za-z_][A-Za-z0-9_#]*)(\s*:)',
            lambda m: f'{m.group(1)}"{m.group(2)}"{m.group(3)}',
            repaired,
        )
        repaired = re.sub(
            r'(?m)^(\s*)(#?[A-Za-z_][A-Za-z0-9_#]*)(\s*:)',
            lambda m: f'{m.group(1)}"{m.group(2)}"{m.group(3)}',
            repaired,
        )
        repaired = re.sub(
            r'([^\s{\[,])(\s*\n\s*)(?=(?:"?#?[A-Za-z_][A-Za-z0-9_#]*"|#?[A-Za-z_][A-Za-z0-9_#]*)\s*:)',
            r"\1,\2",
            repaired,
        )
        repaired = re.sub(r"}\s*\n\s*{", "},\n{", repaired)
        if repaired.startswith("{") and repaired.endswith("}") and re.search(r"}\s*,\s*{", repaired):
            repaired = f"[{repaired}]"
        return repaired

    def _parse_by_known_keys(candidate: str) -> list:
        known = ["row_number", "name", "quote", "text1", "text2", "text3", "text4", "text5", "text6", "generated_caption"]
        key_pat = '"(' + "|".join(re.escape(k) for k in known) + r')"\s*:\s*'
        raw = candidate.strip().lstrip("[").rstrip("]")
        blocks = re.split(r"}\s*,\s*{", raw)
        items = []
        for block in blocks:
            matches = list(re.finditer(key_pat, block))
            if not matches:
                continue
            item: dict = {}
            for i, m in enumerate(matches):
                key = m.group(1)
                val_start = m.end()
                val_end = matches[i + 1].start() if i + 1 < len(matches) else len(block)
                raw_val = block[val_start:val_end].strip().rstrip(",").strip()
                if key == "row_number":
                    num = re.search(r"\d+", raw_val)
                    if num:
                        item["row_number"] = int(num.group())
                else:
                    if raw_val.startswith('"'):
                        raw_val = raw_val[1:]
                    if raw_val.endswith('"'):
                        raw_val = raw_val[:-1]
                    item[key] = raw_val
            if item:
                items.append(item)
        if not items:
            raise ValueError("No items found by key anchoring.")
        return items

    def _parse_linewise_payload(candidate: str):
        lines = [line.rstrip() for line in candidate.splitlines() if line.strip()]
        if not lines or not any(":" in line for line in lines):
            raise ValueError("No linewise payload to parse.")

        items: list[dict] = []
        current: dict[str, object] = {}
        for raw_line in lines:
            line = raw_line.strip()
            if line.startswith("- "):
                if current:
                    items.append(current)
                    current = {}
                line = line[2:].strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip().strip("\"'")
            value = value.strip().rstrip(",")
            if not key:
                continue
            if not value:
                parsed_value = ""
            else:
                normalized = value.replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")
                try:
                    parsed_value = ast.literal_eval(normalized)
                except Exception:
                    parsed_value = normalized.strip("\"'")
            current[key] = parsed_value

        if current:
            items.append(current)
        if not items:
            raise ValueError("No linewise payload to parse.")
        return items if len(items) > 1 else items[0]

    text_block = _extract_block(text)
    try:
        return json.loads(text_block)
    except json.JSONDecodeError:
        try:
            return json.loads(_normalize_slide_paste(text_block))
        except Exception:
            pass
        repaired = _repair_jsonish(text_block)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            pythonish = re.sub(r"\btrue\b", "True", repaired, flags=re.IGNORECASE)
            pythonish = re.sub(r"\bfalse\b", "False", pythonish, flags=re.IGNORECASE)
            pythonish = re.sub(r"\bnull\b", "None", pythonish, flags=re.IGNORECASE)
            try:
                return ast.literal_eval(pythonish)
            except Exception as exc:
                try:
                    return _parse_linewise_payload(text_block)
                except Exception:
                    raise ValueError(
                        "Slide results must be valid JSON or near-JSON with quoted keys."
                    ) from exc


def _single_row_slide_result_json(raw_text: str, row_number: int) -> str:
    payload = _extract_json_payload(raw_text)
    items = payload if isinstance(payload, list) else [payload]
    dict_items = [item for item in items if isinstance(item, dict)]
    if not dict_items:
        raise ValueError("Paste one JSON object or an array containing one slide result.")

    selected = None
    for item in dict_items:
        try:
            if int(item.get("row_number")) == int(row_number):
                selected = item
                break
        except Exception:
            continue
    if selected is None:
        selected = dict_items[0]

    selected = dict(selected)
    selected["row_number"] = row_number
    return json.dumps([selected])


def _apply_slide_result_to_specific_row(row_number: int, raw_text: str) -> tuple[int, list[str]]:
    payload = _extract_json_payload(raw_text)
    items = payload if isinstance(payload, list) else [payload]
    dict_items = [item for item in items if isinstance(item, dict)]
    if not dict_items:
        raise ValueError("Paste one JSON object or an array containing one slide result.")

    selected = None
    for item in dict_items:
        try:
            if int(item.get("row_number")) == int(row_number):
                selected = item
                break
        except Exception:
            continue
    if selected is None:
        selected = dict_items[0]

    raw_name = _cell_text(selected.get("name")).strip()
    carousel = {
        "name": ("@" + raw_name if raw_name and not raw_name.startswith("@") and " " not in raw_name else raw_name),
        "quote": _cell_text(selected.get("quote")).strip().strip('"').strip("'").strip().rstrip("."),
        "text1": _single_paragraph_slide_text(selected.get("text1")),
        "text2": _single_paragraph_slide_text(selected.get("text2")),
        "text3": _single_paragraph_slide_text(selected.get("text3")),
        "text4": _single_paragraph_slide_text(selected.get("text4")),
        "text5": _single_paragraph_slide_text(selected.get("text5")),
        "text6": _single_paragraph_slide_text(selected.get("text6")),
    }
    if not (
        carousel["name"]
        or carousel["text1"]
        or carousel["text2"]
        or carousel["text3"]
        or carousel["text4"]
        or carousel["text5"]
        or carousel["text6"]
    ):
        return 0, [f"Row {row_number}: no name or slide text values were provided."]

    _write_specific_carousel_fields(row_number, carousel)
    return 1, []


def _single_paragraph_slide_text(value: str) -> str:
    text = _cell_text(value).strip()
    # Collapse runs of non-newline whitespace but preserve intentional line breaks
    return re.sub(r"[^\S\n]+", " ", text).strip()


def _apply_chatgpt_handoff_results(sheet_id: str, raw_text: str) -> tuple[int, list[str]]:
    _QUOTES = '"“”\'‘’ '
    payload = _extract_json_payload(raw_text)
    items = payload if isinstance(payload, list) else [payload]
    rows = get_all_rows(sheet_id)
    row_map = {int(row["row_number"]): row for row in rows if row.get("row_number")}
    updated_count = 0
    issues: list[str] = []

    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            issues.append(f"Item {index}: result is not an object.")
            continue
        item = {k.strip().strip(_QUOTES): v for k, v in item.items()}
        row_number = item.get("row_number")
        if row_number is None:
            found_keys = ", ".join(list(item.keys())[:6]) or "(none)"
            issues.append(f"Item {index}: missing row_number (found keys: {found_keys}).")
            continue
        try:
            row_number = int(row_number)
        except Exception:
            issues.append(f"Item {index}: row_number {row_number!r} is not numeric.")
            continue
        row = row_map.get(row_number)
        if row is None:
            issues.append(f"Item {index}: row {row_number} was not found in the sheet.")
            continue

        raw_name = _cell_text(item.get("name")).strip()
        name = ("@" + raw_name if raw_name and not raw_name.startswith("@") and " " not in raw_name else raw_name)
        text1 = _single_paragraph_slide_text(item.get("text1"))
        text2 = _single_paragraph_slide_text(item.get("text2"))
        text3 = _single_paragraph_slide_text(item.get("text3"))
        text4 = _single_paragraph_slide_text(item.get("text4"))
        text5 = _single_paragraph_slide_text(item.get("text5"))
        text6 = _single_paragraph_slide_text(item.get("text6"))

        if not (name or text1 or text2 or text3 or text4 or text5 or text6):
            issues.append(
                f"Item {index} / row {row_number}: no name or slide text values were provided."
            )
            continue

        quote = _cell_text(item.get("quote")).strip().strip('"').strip("'").strip().rstrip(".")
        if update_carousel_fields is not None:
            update_carousel_fields(sheet_id, row_number, name, text1, text2, text3, text4, text5, text6)
        if quote and update_quote is not None:
            update_quote(sheet_id, row_number, quote)
        updated_count += 1

    return updated_count, issues


def _run_home_mode(mode: str, urls: list[str], org_hashtag: str) -> tuple[str, list[dict]]:
    results = []
    tag_value = ORG_HASHTAG_MAP.get(org_hashtag, "")

    for url in urls:
        if mode == "Generate headline":
            source = _fetch_link_data(url)
            source_text = source.get("source_text", "").strip()
            if not source_text:
                raise ValueError(f"{url}: could not extract source text.")
            footer_username = source.get("username", "") if source.get("is_instagram", False) else ""
            final_caption = _build_footered_caption(source_text, footer_username)
            if not source.get("is_instagram", False):
                final_caption = _build_footered_caption(
                    f"{source_text}\n\n{_build_read_cta(source['url'])}",
                    "",
                )
            results.append(
                {
                    "url": source["url"],
                    "username": source.get("username", ""),
                    "display_name": source.get("display_name", ""),
                    "is_instagram": source.get("is_instagram", False),
                    "headlines": _generate_headlines(source_text),
                    "caption": final_caption,
                    "source_caption": source_text,
                }
            )
        elif mode == "Caption this":
            source = _fetch_link_data(url)
            row = {
                "Instagram URL": source["url"],
                "Source Username": (
                    source.get("username", "")
                    if source.get("is_instagram", False)
                    else source.get("display_name", "")
                ),
                "Media Type": "" if source.get("is_instagram", False) else "article",
                "Original Caption": source.get("source_text", "").strip(),
                "Transcript": "" if source.get("is_instagram", False) else source.get("source_text", "").strip(),
                "Caption Context": "" if source.get("is_instagram", False) else source.get("source_text", "").strip(),
                "Speaker Name": "",
                "Required Hashtags": tag_value,
                "Top Comment": (
                    _build_watch_cta(source.get("username", ""), source["url"])
                    if source.get("is_instagram", False)
                    else _build_read_cta(source["url"])
                ),
            }
            if not row["Original Caption"]:
                raise ValueError(f"{url}: could not extract source text.")
            caption = generate_row_caption(row)
            results.append(
                {
                    "url": source["url"],
                    "username": source.get("username", ""),
                    "display_name": source.get("display_name", ""),
                    "is_instagram": source.get("is_instagram", False),
                    "caption": caption,
                    "source_caption": row["Original Caption"],
                }
            )
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    return tag_value, results


st.set_page_config(page_title="Workspace", page_icon="🏠", layout="wide")
inject_styles("workspace")
st.title("Workspace")

if not require_auth():
    st.stop()

pending_tab = st.session_state.pop("_workspace_pending_tab", None)
if pending_tab:
    if pending_tab in {"Edit", "Grid", "Actions", "Slides"}:
        pending_tab = "Home"
    elif pending_tab == "Data":
        pending_tab = "Substack"
    st.session_state["workspace_active_tab"] = pending_tab
elif "workspace_active_tab" not in st.session_state:
    st.session_state["workspace_active_tab"] = "Home"
elif st.session_state["workspace_active_tab"] in {"Edit", "Grid", "Actions", "Slides"}:
    st.session_state["workspace_active_tab"] = "Home"
elif st.session_state["workspace_active_tab"] == "Data":
    st.session_state["workspace_active_tab"] = "Substack"

active_section_tab = st.segmented_control(
    "Workspace section",
    ["Home", "Substack"],
    key="workspace_active_tab",
    label_visibility="collapsed",
    width="stretch",
) or "Home"

workspace_rows_error = ""
workspace_rows: list[dict] = []
home_notice = st.session_state.pop("workspace_home_notice", "")
slide_cta_options: dict[str, str] = {}

# Load original-thumbnail map once per session; blur/unblur update it in-place.
if "workspace_original_thumbnails" not in st.session_state:
    try:
        st.session_state["workspace_original_thumbnails"] = get_original_thumbnails(GOOGLE_SHEET_ID)
    except Exception:
        st.session_state["workspace_original_thumbnails"] = {}

if active_section_tab == "Home":
    try:
        workspace_rows = _run_with_sheet_quota_countdown(
            lambda: get_all_rows(GOOGLE_SHEET_ID),
            "Loading workspace paused:",
        )
    except Exception as e:
        workspace_rows_error = describe_error(e)

    st.markdown('<div class="workspace-action-anchor"></div>', unsafe_allow_html=True)
    if st.session_state.get("workspace_home_action_dialog"):
        _render_workspace_home_action_dialog()
    if st.session_state.get("workspace_slides_dialog"):
        _render_workspace_slides_dialog(workspace_rows, workspace_rows_error)
    if st.session_state.get("workspace_video_post_dialog"):
        _render_video_post_dialog()
    if st.session_state.get("workspace_election_post_dialog"):
        _render_election_post_dialog()

    with st.popover("App actions", use_container_width=True):
        if st.button(
            "⚡ Run all",
            key="workspace_run_all",
            width="stretch",
            help="Ingest new rows, transcribe untranscribed reels, and split newly ingested videos.",
            type="primary",
        ):
            st.session_state["workspace_run_all_pending"] = True
            st.rerun()

        if st.button(
            "Refresh results",
            key="workspace_refresh_editor_rows",
            width="stretch",
            help="Reload the current editor rows from the sheet and look for new results.",
        ):
            _rerun_workspace("Home")

        if st.button("Slides", key="workspace_open_slides_dialog", width="stretch"):
            _open_workspace_slides_dialog()
            _rerun_workspace("Home")

        for action_mode in MODE_OPTIONS:
            if st.button(action_mode, key=f"workspace_home_action_{action_mode}", width="stretch"):
                _open_workspace_home_action_dialog(action_mode)
                _rerun_workspace("Home")

        if st.button("Video Post", key="workspace_open_video_post_dialog", width="stretch"):
            _open_video_post_dialog()
            _rerun_workspace("Home")

        if st.button("Election Post", key="workspace_open_election_post_dialog", width="stretch"):
            _open_election_post_dialog()
            _rerun_workspace("Home")

        if st.button("Update cookies", key="workspace_open_cookies_dialog", width="stretch"):
            _render_cookies_dialog()

    # Show success/error feedback below the action buttons
    success_message = st.session_state.pop("workspace_success", "")
    error_message = st.session_state.pop("workspace_error", "")
    if success_message:
        st.success(success_message)
    if error_message:
        st.error(error_message)

    # Run All executes here so status output appears below the buttons
    if st.session_state.pop("workspace_run_all_pending", False):
        _run_all_steps()
        _rerun_workspace("Home")


    if home_notice:
        st.caption(home_notice)

    home_results = st.session_state.get("workspace_home_results")
    if home_results and home_results.get("mode") == "Generate headline":
        for idx, item in enumerate(home_results.get("items", []), start=1):
            st.caption(f"Result {idx}")
            display_name = item.get("username") or item.get("display_name") or "unknown"
            st.write(f"@{display_name}" if item.get("is_instagram", True) else display_name)
            open_label = "Open Instagram link ↗" if item.get("is_instagram", True) else "Open source link ↗"
            st.markdown(f"[{open_label}]({item['url']})")
            headline_tabs = st.tabs(["Headline 1", "Headline 2", "Headline 3", "Caption"])
            for tab_idx, headline in enumerate(item.get("headlines", [])[:3]):
                with headline_tabs[tab_idx]:
                    _tab_copy_preview(headline or "(none)")
            with headline_tabs[3]:
                _tab_copy_preview(item.get("caption", "") or "(none)")

    if home_results and home_results.get("mode") == "Caption this":
        for idx, item in enumerate(home_results.get("items", []), start=1):
            st.caption(f"Caption {idx}")
            display_name = item.get("username") or item.get("display_name") or "unknown"
            st.write(f"@{display_name}" if item.get("is_instagram", True) else display_name)
            open_label = "Open Instagram link ↗" if item.get("is_instagram", True) else "Open source link ↗"
            st.markdown(f"[{open_label}]({item['url']})")
            _copy_block("caption", item.get("caption", ""), f"workspace_home_caption_only_{idx}")

    if home_results and st.button("Clear results", width="stretch", key="workspace_home_clear"):
        st.session_state.pop("workspace_home_results", None)
        st.session_state.pop("workspace_home_notice", None)
        _reset_home_links_on_next_render()
        _rerun_workspace("Home")

    if workspace_rows_error:
        st.error(f"Could not load rows: {workspace_rows_error}")
        pending_edit_rows = []
        editor_rows = []
    else:
        pending_edit_rows = [
            r for r in workspace_rows
            if not r.get("Status", "").strip() and r.get("Instagram URL", "").strip()
        ]
        editor_rows = _sort_editor_rows([r for r in workspace_rows if _is_editable_row(r)])

    if pending_edit_rows:
        row_word = "row" if len(pending_edit_rows) == 1 else "rows"
        st.info(f"{len(pending_edit_rows)} new {row_word} found.")
        if st.button("Process for editing", key="workspace_edit_process_pending", type="primary", width="stretch"):
            try:
                processed_count = _process_pending_rows_from_sheet()
            except Exception as e:
                st.error(f"Could not process new rows: {describe_error(e)}")
            else:
                if processed_count:
                    st.session_state["workspace_success"] = f"Processed {processed_count} new row(s) for editing."
                else:
                    st.session_state["workspace_success"] = "No new rows to process."
                _rerun_workspace("Edit")

    dialog_row_number = st.session_state.get("workspace_link_dialog_row")
    if dialog_row_number is not None:
        dialog_row = next((row for row in editor_rows if row.get("row_number") == dialog_row_number), None)
        if dialog_row is None:
            st.session_state.pop("workspace_link_dialog_row", None)
        else:
            _render_workspace_link_dialog(dialog_row)

    thumbnail_dialog_row_number = st.session_state.get("workspace_thumbnail_dialog_row")
    if thumbnail_dialog_row_number is not None:
        thumbnail_dialog_row = next((row for row in editor_rows if row.get("row_number") == thumbnail_dialog_row_number), None)
        if thumbnail_dialog_row is None:
            st.session_state.pop("workspace_thumbnail_dialog_row", None)
        else:
            _render_workspace_thumbnail_dialog(thumbnail_dialog_row)

    post_slides_dialog_row_number = st.session_state.get("workspace_post_slides_dialog_row")
    if post_slides_dialog_row_number is not None:
        post_slides_dialog_row = next((row for row in editor_rows if row.get("row_number") == post_slides_dialog_row_number), None)
        if post_slides_dialog_row is None:
            _close_workspace_post_slides_dialog(clear_inputs=True)
        else:
            _render_workspace_post_slides_dialog(post_slides_dialog_row)

    generic_slides_dialog_row_number = st.session_state.get("workspace_generic_slides_dialog_row")
    if generic_slides_dialog_row_number is not None:
        generic_slides_dialog_row = next((row for row in editor_rows if row.get("row_number") == generic_slides_dialog_row_number), None)
        if generic_slides_dialog_row is None:
            _close_workspace_generic_slides_dialog(clear_inputs=True)
        else:
            _render_workspace_generic_slides_dialog(generic_slides_dialog_row)

    slide_dialog_state = st.session_state.get("workspace_slide_action_dialog") or {}
    slide_dialog_row_number = slide_dialog_state.get("row_number")
    if slide_dialog_row_number is not None:
        slide_dialog_row = next((row for row in editor_rows if row.get("row_number") == slide_dialog_row_number), None)
        if slide_dialog_row is None:
            _close_workspace_slide_action_dialog(clear_inputs=True)
        else:
            _render_workspace_slide_action_dialog(slide_dialog_row)

    candidate_article_dialog_row_number = st.session_state.get("workspace_candidate_article_dialog_row")
    if candidate_article_dialog_row_number is not None:
        candidate_article_dialog_row = next((row for row in editor_rows if row.get("row_number") == candidate_article_dialog_row_number), None)
        if candidate_article_dialog_row is None:
            _close_workspace_candidate_article_dialog(clear_inputs=True)
        else:
            _render_workspace_candidate_article_dialog(candidate_article_dialog_row)

    if workspace_rows_error:
        pass
    elif not editor_rows:
        st.info("No rows yet. Use the Home actions menu to create work, or use Substack > Guides to build a voter-guide prompt.")
    else:
        query_row = str(st.query_params.get("workspace_row", "") or "")
        if query_row and st.session_state.get("workspace_target_row") != query_row:
            st.session_state["workspace_target_row"] = query_row
        row_numbers = [row["row_number"] for row in editor_rows]
        current_selected = st.session_state.get("workspace_selected_row_num", row_numbers[0])
        if query_row:
            try:
                current_selected = int(query_row)
            except Exception:
                current_selected = row_numbers[0]
        if current_selected not in row_numbers:
            current_selected = row_numbers[0]
        st.session_state["workspace_selected_row_num"] = current_selected
        _render_editor_grid(editor_rows, current_selected)
        current_index = row_numbers.index(current_selected)
        selected_row = editor_rows[current_index]
        preview_scroll_target = st.session_state.pop("workspace_preview_scroll_target", None)
        if preview_scroll_target:
            _scroll_to_element(preview_scroll_target)
        else:
            _scroll_to_editor_row(str(selected_row["row_number"]))
        for row in [selected_row]:
            _sync_workspace_row_state(row)
            row_num = row["row_number"]
            speaker_key = _workspace_speaker_key(row)
            hashtags_key = _workspace_key(row, "hashtags")
            top_key = _workspace_key(row, "top")
            context_key = _workspace_key(row, "context")
            warning_key = _workspace_key(row, "transcript_warning")
            transcribe_key = _workspace_key(row, "transcribe")
            menu_nonce_key = _workspace_key(row, "menu_nonce")
            username = _cell_text(row.get("Source Username")).strip()
            url = _cell_text(row.get("Instagram URL")).strip()
            is_instagram = _is_instagram_url(url)
            is_article = _is_article_url(url)
            media_type = _cell_text(row.get("Media Type")).strip().lower()
            generated = _cell_text(row.get("Generated Caption")).strip()
            original_caption = _cell_text(row.get("Original Caption")).strip()
            transcript = _cell_text(row.get("Transcript")).strip()
            speaker_name = _cell_text(row.get("Speaker Name"))
            status = _cell_text(row.get("Status")).strip()

            row_container = st.container()
            with row_container:
                st.markdown(
                    f'<span id="workspace-row-{row_num}" class="workspace-list-row-anchor"></span>'
                    '<div class="workspace-edit-main-anchor"></div>',
                    unsafe_allow_html=True,
                )
                top_left, top_right = st.columns([0.9, 1.1], vertical_alignment="top")
                with top_left:
                    thumb_link = _cell_text(row.get("Thumbnail Drive Link")).strip()
                    if thumb_link:
                        image_url = _grid_preview_url(row)
                        if image_url:
                            st.image(image_url, width="stretch")
                        else:
                            st.info("Thumbnail link is unavailable.")
                    elif is_article:
                        st.info("Article link")
                        if original_caption:
                            st.caption(original_caption[:260] + ("..." if len(original_caption) > 260 else ""))
                    else:
                        st.info("Thumbnail will appear here after ingest.")

                with top_right:
                    is_reel = _is_reel_url(url)
                    is_photo_post = is_instagram and not is_reel
                    menu_label = "Process this post" if is_photo_post else "Process post"
                    schedule_suffix = (row.get("Scheduled Time", "") or "").strip()
                    status_line = f"Row {row_num} · {media_type or 'pending'} · {status or 'blank'}"
                    if schedule_suffix:
                        status_line = f"{status_line} · {schedule_suffix}"
                    st.markdown(
                        f'<div class="workspace-status-line">{status_line}</div>',
                        unsafe_allow_html=True,
                    )
                    if username:
                        st.markdown(f"#### @{username}" if is_instagram else f"#### {username}")
                    else:
                        st.markdown(f"#### Row {row_num}")

                    if speaker_key not in st.session_state:
                        st.session_state[speaker_key] = speaker_name
                    st.text_input(
                        "Speaker name",
                        key=speaker_key,
                        placeholder="Speaker name",
                        label_visibility="collapsed",
                        on_change=_handle_speaker_name_change,
                        args=(row,),
                    )
                    media_links = [
                        link.strip()
                        for link in _cell_text(row.get("Media Drive Link")).split(",")
                        if link.strip()
                    ]
                    menu_nonce = st.session_state.get(menu_nonce_key, 0)
                    menu_label_with_nonce = f"Post actions{chr(0x200B) * menu_nonce}"
                    with st.popover(menu_label_with_nonce, use_container_width=True):
                        st.text_input(
                            "Add context",
                            key=context_key,
                            placeholder="Add context",
                            label_visibility="collapsed",
                        )
                        if is_article or media_type == "article":
                            st.link_button(
                                "Open article link",
                                url or "#",
                                width="stretch",
                                disabled=not url,
                            )
                        elif url:
                            st.link_button(
                                "Open in Instagram" if is_instagram else "Open source link",
                                url,
                                width="stretch",
                            )
                        if media_links:
                            st.link_button(
                                "Open reel in Drive" if is_reel else "Open media in Drive",
                                media_links[0],
                                width="stretch",
                            )
                        if st.button(
                            "Slides",
                            key=f"workspace_menu_post_slides_{row_num}",
                            width="stretch",
                        ):
                            _close_workspace_menu(row)
                            _open_workspace_post_slides_dialog(row_num)
                            _rerun_workspace("Edit")
                        if st.button("Add link", key=f"workspace_link_open_{row_num}", width="stretch"):
                            _close_workspace_menu(row)
                            st.session_state["workspace_link_dialog_row"] = row_num
                            _rerun_workspace("Edit")
                        if st.button(
                            "Make generic",
                            key=f"workspace_menu_generic_slides_{row_num}",
                            width="stretch",
                            help="Build a source-agnostic slides prompt — strips speaker, adds research directive, neutralizes caption, removes CTA",
                        ):
                            _close_workspace_menu(row)
                            _open_workspace_generic_slides_dialog(row_num)
                            _rerun_workspace("Edit")
                        if st.button(
                            "Update screenshot",
                            key=f"workspace_menu_thumbnail_open_{row_num}",
                            width="stretch",
                        ):
                            _close_workspace_menu(row)
                            st.session_state["workspace_thumbnail_dialog_row"] = row_num
                            _rerun_workspace("Edit")
                        if is_reel and media_links and st.button(
                            "Crop video to fit",
                            key=f"workspace_menu_crop_video_fit_{row_num}",
                            width="stretch",
                            help="Scale the original video to fit the 4:5 canvas with black bars and upload segments to Drive.",
                        ):
                            _close_workspace_menu(row)
                            _queue_workspace_action(row_num, "split_video_fit")
                            _rerun_workspace("Edit")
                        primary_action = "process_post" if is_instagram else "image_text"
                        primary_help = (
                            "Transcribe, generate the caption, and generate slide copy."
                            if _is_reel_url(url)
                            else "Use available post text and image text to generate the caption and slide copy."
                        )
                        if st.button("Edit caption", key=f"workspace_menu_edit_caption_{row_num}", width="stretch"):
                            _close_workspace_menu(row)
                            _open_workspace_slide_action_dialog(row_num, "caption")
                            _rerun_workspace("Edit")
                        if is_instagram and st.button(
                            menu_label,
                            key=f"workspace_menu_primary_{row_num}",
                            disabled=not url,
                            width="stretch",
                            help=primary_help,
                        ):
                            if primary_action == "process_post" and not transcript:
                                try:
                                    warning = _check_reel_transcript_risk(row)
                                except Exception as e:
                                    st.session_state["workspace_error"] = f"Row {row_num}: could not check reel size - {describe_error(e)}"
                                    _close_workspace_menu(row)
                                    _rerun_workspace("Edit")
                                if warning:
                                    st.session_state[warning_key] = warning
                                    _close_workspace_menu(row)
                                    _rerun_workspace("Edit")
                            _close_workspace_menu(row)
                            _queue_workspace_action(row_num, primary_action)
                            _rerun_workspace("Edit")
                        if is_article and _is_substack_url(url) and st.button(
                            "Process as Candidate Article",
                            key=f"workspace_menu_candidate_article_{row_num}",
                            width="stretch",
                            help="Generate a three-slide carousel and footer from this Substack article row.",
                        ):
                            _close_workspace_menu(row)
                            _open_workspace_candidate_article_dialog(row_num)
                            _rerun_workspace("Edit")
                        skip_label = "Unskip" if status.strip().lower() == "skipped" else "Skip"
                        if st.button(
                            skip_label,
                            key=f"workspace_menu_skip_{row_num}",
                            width="stretch",
                        ):
                            next_status = _default_editor_status(row) if status.strip().lower() == "skipped" else "skipped"
                            update_status(GOOGLE_SHEET_ID, row_num, next_status)
                            if next_status == "skipped":
                                if str(st.query_params.get("workspace_row", "") or "") == str(row_num):
                                    st.query_params.pop("workspace_row", None)
                                if st.session_state.get("workspace_target_row") == str(row_num):
                                    st.session_state.pop("workspace_target_row", None)
                            _close_workspace_menu(row)
                            st.session_state["workspace_success"] = (
                                f"Row {row_num}: moved back into the main edit list."
                                if next_status != "skipped"
                                else f"Row {row_num}: skipped and moved to the bottom."
                            )
                            _rerun_workspace("Edit")
                        if st.button(
                            "Delete row",
                            key=f"workspace_menu_delete_{row_num}",
                            width="stretch",
                        ):
                            try:
                                _delete_workspace_row(row)
                            except Exception as e:
                                st.session_state["workspace_error"] = f"Row {row_num}: could not delete row - {describe_error(e)}"
                            else:
                                st.session_state["workspace_success"] = f"Row {row_num}: deleted from the sheet."
                            _close_workspace_menu(row)
                            _rerun_workspace("Edit")
                    transcript_warning = st.session_state.get(warning_key)
                    if transcript_warning:
                        size_label = _format_bytes(transcript_warning["size_bytes"])
                        threshold_label = _format_bytes(transcript_warning["threshold_bytes"])
                        st.warning(
                            f"This reel is {size_label}, which is over the {threshold_label} transcript warning limit. "
                            "Transcription may cost more than usual."
                        )
                        if st.button(
                            "Process post anyway",
                            key=f"workspace_warning_transcribe_{row_num}",
                            type="primary",
                            width="stretch",
                        ):
                            st.session_state.pop(warning_key, None)
                            _queue_workspace_action(row_num, "process_post")
                            _rerun_workspace("Edit")

                    row_success = st.session_state.pop(f"workspace_row_success_{row_num}", "")
                    row_error = st.session_state.pop(f"workspace_row_error_{row_num}", "")
                    if row_success:
                        st.success(row_success)
                    if row_error:
                        st.error(row_error)
                    _process_next_workspace_action(for_row_number=row_num)

                    _copy_tabs(
                        row_num,
                        generated,
                        original_caption,
                        transcript,
                        username,
                        speaker_name,
                        _decode_top_comment(st.session_state.get(top_key, row.get("Top Comment", "")).strip())[0],
                        st.session_state.get(hashtags_key, row.get("Required Hashtags", "")).strip(),
                        row.get("Media Drive Link", ""),
                        media_type,
                        url,
                        is_instagram,
                        _cell_text(row.get("text1")).strip(),
                        _cell_text(row.get("text2")).strip(),
                        _cell_text(row.get("text3")).strip(),
                        _cell_text(row.get("text4")).strip(),
                        _cell_text(row.get("text5")).strip(),
                        _cell_text(row.get("text6")).strip(),
                        row,
                        _cell_text(row.get("Thumbnail Drive Link")).strip(),
                        slide_cta_options,
                    )

        queue = st.session_state.get("workspace_action_queue", [])
        if queue:
            st.markdown(
                f'<div class="workspace-action-note">{len(queue)} queued action(s) waiting to run.</div>',
                unsafe_allow_html=True,
            )


if active_section_tab == "Substack":
    substack_section = st.segmented_control(
        "Substack section",
        ["Promote", "Monitors", "Guides"],
        key="workspace_substack_section",
        label_visibility="collapsed",
        width="stretch",
    ) or "Promote"

    # ── Promote ───────────────────────────────────────────────────────────
    if substack_section == "Promote":
        st.caption("Generate Instagram posts to drive traffic to your Substack articles.")
        _sb_open_rows = [
            row for row in sheet_ops.get_substack_rows(GOOGLE_SHEET_ID)
            if _cell_text(row.get("status")).strip().lower() == "open"
        ]

        if not _sb_open_rows:
            st.info("No open Substack articles. Paste a URL below to add one.")
            _sb_new_url = st.text_input("Substack article URL", key="ws_sb_new_url")
            if st.button("Add Article", type="primary", key="ws_sb_add"):
                if _sb_new_url.strip():
                    sheet_ops.append_substack_row(GOOGLE_SHEET_ID, _sb_new_url.strip())
                    st.success("Article added.")
                    _rerun_workspace("Substack")
                else:
                    st.warning("Enter a URL first.")
        else:
            _sb_options = sorted(
                _sb_open_rows,
                key=lambda row: int(row.get("row_number") or 0),
                reverse=True,
            )
            _sb_row = st.selectbox(
                "Select article",
                _sb_options,
                key="ws_sb_select_row",
                format_func=lambda row: (
                    (row.get("name") or "").strip()
                    or (row.get("url") or "").strip()[:80]
                    or f"Row {row.get('row_number', '')}"
                ),
            )
            _sb_url = _sb_row["url"]
            _sb_row_number = _sb_row["row_number"]

            _sb_fetch_key = f"ws_sb_fetched_{_sb_url}"
            _sb_fetch_meta_key = f"ws_sb_fetched_meta_{_sb_url}"
            _sb_topics_key = f"ws_sb_article_topics_{_sb_url}"
            _sb_stored_topics = _parse_substack_topic_breakdown(_sb_row.get("topic breakdown", ""))
            if _sb_stored_topics and _sb_topics_key not in st.session_state:
                st.session_state[_sb_topics_key] = _sb_stored_topics
            _sb_article_body = _sb_row.get("article", "").strip() or st.session_state.get(_sb_fetch_key, "").strip()
            if _sb_article_body:
                st.caption("Topic generation uses only the article text saved in column C.")
            else:
                st.caption("Column C is blank. Topic generation needs article text in column C.")

            if st.button("Generate Topic Breakdown", type="primary", key="ws_sb_gen_topics"):
                try:
                    if not _sb_article_body:
                        raise RuntimeError("Column C is blank for this article. Add article text there first.")

                    _sb_resp = _get_client().chat.completions.create(
                        model="gpt-4o",
                        messages=[
                            {"role": "system", "content": _substack_topic_breakdown_prompt()},
                            {"role": "user", "content": f"Article:\n\n{_sb_article_body}"},
                        ],
                        max_tokens=500,
                    )
                    _sb_raw = (_sb_resp.choices[0].message.content or "").strip()
                    _sb_raw = _sb_raw.lstrip("```json").lstrip("```").rstrip("```").strip()
                    _sb_topics = _extract_substack_topics_from_model_output(_sb_raw)
                    if not _sb_topics:
                        raise ValueError("No valid topics returned.")
                    st.session_state[_sb_topics_key] = _sb_topics
                    sheet_ops.update_substack_topic_breakdown(
                        GOOGLE_SHEET_ID,
                        _sb_row_number,
                        json.dumps(_sb_topics, ensure_ascii=True),
                    )
                except Exception as _sb_err:
                    st.error(f"Failed to generate topic breakdown: {_sb_err}")

            _sb_article_topics = _substack_topic_options(st.session_state.get(_sb_topics_key, []))
            if _sb_article_topics:
                st.markdown("**Article topic breakdown**")
                st.caption("Pick one short topic to focus the promotional post.")
                _sb_focus_topic = st.selectbox(
                    "Focus topic",
                    _sb_article_topics,
                    key=f"ws_sb_focus_topic_{_sb_url}",
                )
                _sb_context_request = st.text_area(
                    "Context to emphasize",
                    key=f"ws_sb_context_{_sb_url}",
                    height=110,
                    placeholder="Add what angle or context you want emphasized in the post.",
                )
                st.caption("Topics found: " + ", ".join(_sb_article_topics))
                if st.button("Create Promote Draft", type="primary", key="ws_sb_create"):
                    try:
                        _sb_fetch_meta = st.session_state.get(_sb_fetch_meta_key) or {}
                        append_generated_post_rows(
                            GOOGLE_SHEET_ID,
                            [
                                {
                                    "url": _sb_url,
                                    "source_username": "voteinorout",
                                    "caption": "",
                                    "media_type": "article",
                                    "thumbnail_link": _cell_text(_sb_fetch_meta.get("image_url")).strip(),
                                    "original_caption": _sb_article_body,
                                    "transcript": _sb_article_body,
                                    "caption_context": _substack_promote_context(
                                        _sb_url,
                                        _sb_focus_topic,
                                        _sb_context_request,
                                        _sb_article_topics,
                                    ),
                                    "name": "vote in or out substack",
                                    "text1": "",
                                    "text2": "",
                                    "text3": "",
                                    "text4": "",
                                    "text5": "",
                                    "text6": "",
                                    "slide_cta": "Save link for Substack",
                                    "status": "slide prompt ready",
                                }
                            ],
                        )
                    except Exception as _sb_create_err:
                        st.error(f"Could not create promote draft: {describe_error(_sb_create_err)}")
                    else:
                        st.success("Promote draft created in the posts tab.")
                        _rerun_workspace("Substack")
            elif _sb_stored_topics:
                st.info("Stored topic breakdown found but could not be displayed cleanly. Regenerate it once to refresh the saved format.")

            _sb_all_posts = get_all_rows(GOOGLE_SHEET_ID)
            _sb_generated = []
            for _sb_post_row in _sb_all_posts:
                if _cell_text(_sb_post_row.get("Instagram URL")).strip() != _sb_url:
                    continue
                _sb_meta = _parse_substack_promote_context(_sb_post_row.get("Caption Context", ""))
                if _sb_meta.get("source") != "substack_promote":
                    continue
                _sb_generated.append((_sb_post_row, _sb_meta))
            if _sb_generated:
                st.markdown("---")
                st.markdown("### Generated Posts")
                for _sb_post, _sb_meta in _sb_generated:
                    _sb_post_status = _cell_text(_sb_post.get("Status")).strip().lower()
                    _sb_post_row_number = int(_sb_post.get("row_number") or 0)
                    _sb_focus_topic = _cell_text(_sb_meta.get("focus_topic") or _sb_meta.get("angle")).strip()
                    with st.expander(f"{(_sb_focus_topic or '(no topic)')[:80]} — {_sb_post_status}", expanded=False):
                        if _cell_text(_sb_meta.get("context_request")).strip():
                            st.markdown(f"**Context:** {_cell_text(_sb_meta.get('context_request')).strip()}")
                        _sb_meta_topics = _normalize_substack_topics(
                            _sb_meta.get("article_topics") or _sb_meta.get("topics") or []
                        )
                        if _sb_meta_topics:
                            st.markdown(f"**Article topics:** {', '.join(_sb_meta_topics)}")
                        st.markdown(f"**CTA:** {_sb_post.get('Slide CTA', '')}")
                        st.markdown("**Caption**")
                        st.code(_sb_post.get("Generated Caption", ""), language=None)
                        if any(_sb_post.get(f"text{i}") for i in range(1, 7)):
                            for _sb_slide_num in range(1, 7):
                                st.markdown(f"**Slide {_sb_slide_num}**")
                                st.code(_sb_post.get(f"text{_sb_slide_num}", ""), language=None)
                        if _sb_post_status not in {"row created", "posted"}:
                            _sb_slide_results = st.text_area(
                                "Paste slide results",
                                key=f"ws_sb_slide_results_{_sb_post_row_number}",
                                height=110,
                                placeholder=(
                                    f'[{{"row_number":{_sb_post_row_number},"name":"...",'
                                    '"text1":"...","text2":"...","text3":"...",'
                                    '"text4":"...","text5":"...","text6":"..."}}]'
                                ),
                            )
                            if st.button(
                                "Save slide results",
                                type="primary",
                                key=f"ws_sb_create_row_{_sb_post_row_number}",
                            ):
                                try:
                                    _sb_slides = _substack_slide_result(
                                        _sb_slide_results,
                                        _sb_post_row_number,
                                    )
                                    if not all(_sb_slides[f"text{i}"] for i in range(1, 7)):
                                        raise ValueError("Slide result must include text1 through text6.")
                                    update_generated_post_slides_and_status(
                                        GOOGLE_SHEET_ID,
                                        _sb_post_row_number,
                                        _sb_slides["name"],
                                        _sb_slides["text1"],
                                        _sb_slides["text2"],
                                        _sb_slides["text3"],
                                        _sb_slides["text4"],
                                        _sb_slides["text5"],
                                        _sb_slides["text6"],
                                        "slides",
                                    )
                                    _sb_caption = _generate_substack_caption_from_slides(
                                        _sb_url,
                                        _cell_text(_sb_post.get("Original Caption")).strip() or _sb_article_body,
                                        _sb_focus_topic,
                                        _cell_text(_sb_meta.get("context_request")).strip(),
                                        _sb_meta_topics,
                                        _sb_slides,
                                    )
                                    update_caption(GOOGLE_SHEET_ID, _sb_post_row_number, _sb_caption, "slides")
                                    st.session_state.pop(f"ws_sb_slide_results_{_sb_post_row_number}", None)
                                except Exception as _sb_apply_err:
                                    st.error(f"Could not save slide results: {describe_error(_sb_apply_err)}")
                                else:
                                    st.success("Slide results and caption saved to the posts tab.")
                                    _rerun_workspace("Substack")
                        _sb_prompt, _sb_input = _build_substack_slide_handoff(
                            _sb_focus_topic,
                            _cell_text(_sb_meta.get("context_request")).strip(),
                            _sb_meta_topics,
                            _cell_text(_sb_post.get("Original Caption")).strip() or _sb_article_body,
                            _sb_url,
                        )
                        if _sb_post_status != "posted":
                            st.markdown("**Slide prompt**")
                            st.code(
                                _format_substack_slide_prompt_preview(
                                    _sb_prompt,
                                    _sb_input,
                                    _sb_post_row_number,
                                ),
                                language=None,
                            )
                        if _sb_post_status != "posted":
                            if st.button("Mark as Posted", key=f"ws_sb_posted_{_sb_post['row_number']}"):
                                update_status(GOOGLE_SHEET_ID, _sb_post["row_number"], "posted")
                                _rerun_workspace("Substack")

            st.markdown("---")
            st.link_button("Open Substack Link", _sb_url, width="stretch")

    # ── Monitors ──────────────────────────────────────────────────────────
    if substack_section == "Monitors":
        st.caption("Watch Instagram comments on your election guide posts.")
        commentary_entries = st.session_state.setdefault("workspace_candidate_commentary_entries", [])
        open_comment_rows: list[dict] = []
        open_comment_rows_error = ""
        try:
            open_comment_rows = _load_open_candidate_comment_rows()
        except Exception as e:
            open_comment_rows_error = describe_error(e)

        selected_rows: list[dict] = []
        if open_comment_rows_error:
            st.error(open_comment_rows_error)
        elif not open_comment_rows:
            st.caption('No rows with monitoring status "open" and an `instagram url` were found in the Substack sheet.')
        else:
            selector_default_rows = []
            for row in open_comment_rows:
                selector_default_rows.append(
                    {
                        "Check": False,
                        "Summary": row.get("summary") or "",
                        "Instagram": row["url"],
                        "Substack": row.get("substack") or "",
                        "_row_number": row["row_number"],
                    }
                )
            selector_df = _get_pandas().DataFrame(selector_default_rows)
            edited_selector_df = st.data_editor(
                selector_df,
                hide_index=True,
                width="stretch",
                key="workspace_candidate_comments_selector",
                column_config={
                    "Check": st.column_config.CheckboxColumn("Check", default=False),
                    "Summary": st.column_config.TextColumn("Summary", disabled=True),
                    "Instagram": st.column_config.LinkColumn("Instagram", disabled=True),
                    "Substack": st.column_config.LinkColumn("Substack", disabled=True),
                    "_row_number": None,
                },
                disabled=["Summary", "Instagram", "Substack", "_row_number"],
            )
            selected_row_numbers = {
                int(row["_row_number"])
                for row in edited_selector_df.to_dict("records")
                if row.get("Check") and row.get("_row_number") not in (None, "")
            }
            selected_rows = [
                row for row in open_comment_rows
                if row["row_number"] in selected_row_numbers
            ]

        comments_action_left, comments_action_right = st.columns(2)
        with comments_action_left:
            if st.button(
                "Check for New Comments",
                type="primary",
                width="stretch",
                key="workspace_candidate_comments_check",
            ):
                checked_at = _now_eastern()
                new_entries: list[dict] = []
                if open_comment_rows_error:
                    st.error(open_comment_rows_error)
                elif not open_comment_rows:
                    st.info('No rows with monitoring status "open" and an `instagram url` were found in the Substack sheet.')
                elif not selected_rows:
                    st.warning("Select at least one open row before checking for new comments.")
                else:
                    with st.spinner("Checking selected posts for new comments..."):
                        for candidate_row in selected_rows:
                            url = candidate_row["url"]
                            checked_label = _format_eastern_timestamp(checked_at)
                            try:
                                comments = _fetch_candidate_comments_since(
                                    url,
                                    candidate_row.get("last_checked_at"),
                                )
                                summary = _summarize_candidate_comments(comments)
                                _update_candidate_last_checked(
                                    candidate_row.get("source", "substack"),
                                    candidate_row["row_number"],
                                    checked_at,
                                )
                                new_entries.append(
                                    {
                                        "label": candidate_row.get("label") or candidate_row.get("substack") or url,
                                        "url": url,
                                        "checked_at": checked_label,
                                        "summary_groups": summary,
                                        "error": "",
                                    }
                                )
                            except Exception as e:
                                new_entries.append(
                                    {
                                        "label": candidate_row.get("label") or candidate_row.get("substack") or url,
                                        "url": url,
                                        "checked_at": checked_label,
                                        "summary_groups": {},
                                        "error": describe_error(e),
                                    }
                                )
                    if new_entries:
                        st.session_state["workspace_candidate_commentary_entries"] = new_entries + commentary_entries
                        commentary_entries = st.session_state["workspace_candidate_commentary_entries"]
        with comments_action_right:
            if st.button(
                "Clear Commentary",
                width="stretch",
                key="workspace_candidate_comments_clear",
            ):
                st.session_state["workspace_candidate_commentary_entries"] = []
                commentary_entries = []

        if not commentary_entries:
            st.caption("No comment summaries yet.")
        else:
            for entry in commentary_entries:
                st.markdown(f"**{entry['url']}**")
                st.caption(f"{entry.get('label') or entry['url']} last checked {entry['checked_at']}")
                if entry.get("error"):
                    st.error(entry["error"])
                else:
                    groups = entry.get("summary_groups") or {}
                    if not groups:
                        st.caption("No missing, biased, wrong, or controversy-comment patterns found.")
                    else:
                        for heading in ("What About", "Missing", "Biased", "Wrong", "Controversies"):
                            comments = groups.get(heading) or []
                            if not comments:
                                continue
                            st.markdown(f"**{heading}**")
                            for comment in comments:
                                username = (comment.get("username") or "").strip()
                                text = (comment.get("text") or "").strip()
                                if not text:
                                    continue
                                prefix = f"@{username}: " if username else ""
                                st.markdown(f"- {prefix}{text}")

    # ── Guides ────────────────────────────────────────────────────────────
    if substack_section == "Guides":
        st.caption("Generate a Substack article prompt for a race by entering the candidates you want compared.")

        fundraising_presets = _fundraising_preset_map()
        candidate_input_blob = st.text_area(
            "Candidates",
            key="workspace_candidate_names_blob",
            placeholder="Paste candidate names, one per line or comma-separated",
            height=140,
        ).strip()

        donation_options = ["No link"] + [option for option in fundraising_presets.keys() if option != "Custom"]
        donation_source = st.selectbox(
            "Donation link",
            options=donation_options,
            key="workspace_candidate_donation_source",
        )
        donation_link = "" if donation_source == "No link" else fundraising_presets.get(donation_source, "").strip()

        action_left, action_right = st.columns(2)
        with action_left:
            if st.button(
                "Process prompt",
                type="primary",
                width="stretch",
                disabled=not candidate_input_blob,
                key="workspace_candidate_research",
            ):
                try:
                    with st.spinner("Resolving shared race..."):
                        candidate_names = _extract_candidate_names_from_input(candidate_input_blob)
                        st.session_state["workspace_candidate_names_parsed"] = candidate_names
                        st.session_state["workspace_candidate_result"] = _resolve_candidate_comparison(
                            candidate_names
                        )
                except Exception as e:
                    st.session_state["workspace_candidate_error"] = describe_error(e)
                    st.session_state.pop("workspace_candidate_result", None)
                    st.session_state.pop("workspace_candidate_names_parsed", None)
                else:
                    st.session_state.pop("workspace_candidate_error", None)
                    _rerun_workspace("Substack")
        with action_right:
            if st.button("Clear", width="stretch", key="workspace_candidate_clear"):
                st.session_state.pop("workspace_candidate_result", None)
                st.session_state.pop("workspace_candidate_error", None)
                st.session_state.pop("workspace_candidate_names_blob", None)
                st.session_state.pop("workspace_candidate_names_parsed", None)
                st.session_state.pop("workspace_candidate_donation_source", None)
                _rerun_workspace("Substack")

        candidate_error = st.session_state.get("workspace_candidate_error", "")
        if candidate_error:
            st.error(candidate_error)

        parsed_candidate_names = st.session_state.get("workspace_candidate_names_parsed") or []
        if parsed_candidate_names:
            st.caption(f"Parsed candidates: {', '.join(parsed_candidate_names)}")

        candidate_result = st.session_state.get("workspace_candidate_result")
        if candidate_result:
            if candidate_result.get("could_not_resolve"):
                st.warning(
                    "I could not resolve a clear set of races for these candidates. "
                    "Verify the names or specify the exact contests."
                )
                active_races = candidate_result.get("active_races") or []
                if active_races:
                    st.caption(f"Active races found: {', '.join(active_races)}")
                if candidate_result.get("ambiguity_note"):
                    st.caption(candidate_result["ambiguity_note"])
            else:
                race_groups = candidate_result.get("race_groups") or []
                if race_groups:
                    st.markdown("**Resolved races:**")
                    for group in race_groups:
                        group_names = ", ".join(
                            _cell_text(name).strip()
                            for name in (group.get("candidate_names") or [])
                            if _cell_text(name).strip()
                        )
                        race_label_parts = [
                            _cell_text(group.get("race_name")).strip(),
                            _cell_text(group.get("election_date")).strip(),
                        ]
                        st.markdown(f"- {group_names}: {', '.join(part for part in race_label_parts if part)}")
                else:
                    resolved_names = [
                        _cell_text(name).strip()
                        for name in (candidate_result.get("candidate_names") or [])
                        if _cell_text(name).strip()
                    ]
                    st.markdown(
                        (
                            f"**Resolved race:** {', '.join(resolved_names) or ', '.join(parsed_candidate_names)}, "
                            f"{candidate_result.get('race_name', '')}, "
                            f"{candidate_result.get('election_date', '')}"
                        )
                    )
                prompt_text = _build_candidate_prompt(candidate_result, donation_link=donation_link)
                st.subheader("Substack prompt")
                st.code(prompt_text, language=None)
