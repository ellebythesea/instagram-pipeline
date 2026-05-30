"""Substack article post generator and comment monitor."""

import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openai
import streamlit as st
from apify_client import ApifyClient

from article_source import fetch_article_source
from config import (
    APIFY_API_TOKEN,
    APIFY_POST_ACTOR_ID,
    DEFAULT_POST_FOOTER,
    GOOGLE_SHEET_ID,
    OPENAI_API_KEY,
)
import sheets as sheet_ops
from utils.auth import require_auth
from utils.styles import inject as inject_styles

st.set_page_config(page_title="Substack", page_icon="📝", layout="wide")
inject_styles()
st.title("Substack")

if not require_auth():
    st.stop()

_openai_client: openai.OpenAI | None = None
_SUBSTACK_PROMOTE_META_PREFIX = "SUBSTACK_PROMOTE_META:"


def _get_openai_client() -> openai.OpenAI:
    global _openai_client
    if _openai_client is None:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        _openai_client = openai.OpenAI(api_key=OPENAI_API_KEY, timeout=60.0, max_retries=1)
    return _openai_client


def _substack_promote_context(
    url: str,
    focus_topic: str,
    context_request: str = "",
    article_topics: list[str] | None = None,
) -> str:
    cleaned_focus_topic = (focus_topic or "").strip()
    payload = {
        "source": "substack_promote",
        "url": (url or "").strip(),
        "focus_topic": cleaned_focus_topic,
        "angle": cleaned_focus_topic,
        "context_request": (context_request or "").strip(),
        "article_topics": [topic.strip() for topic in (article_topics or []) if topic.strip()],
    }
    return f"{_SUBSTACK_PROMOTE_META_PREFIX}{json.dumps(payload, separators=(',', ':'))}"


def _parse_substack_promote_context(value: str) -> dict:
    raw = (value or "").strip()
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
        "Read the full article all the way through and identify the most interesting larger themes it covers.\n"
        "Do not just copy the first few nouns or phrases from the opening lines.\n"
        "Look for what actually matters in the piece: major events, conflicts, notable people, elections, voting, democracy, women's rights, policy fights, court decisions, campaign dynamics, and other political topics of interest that are genuinely present in the article.\n"
        "Prioritize themes a reader could realistically choose as the focus of a promotional post.\n"
        "Return EXACTLY 15 topic strings in rank order from most interesting/useful to least.\n"
        "Each string must be 1 to 3 words.\n"
        "Use concrete article topics, not vague labels.\n"
        "Prefer themes over generic summary words.\n"
        "Good examples: \"tariffs\", \"swing voters\", \"polling gap\", \"abortion rights\", \"ballot access\", \"housing costs\", \"court ruling\", \"union vote\".\n"
        "Bad examples: \"introduction\", \"article overview\", \"politics\", \"news\", \"economy\" unless the article is specifically about the economy as a theme.\n"
        "No duplicates. No numbering. No markdown. No commentary outside JSON."
    )


def _normalize_substack_topics(raw_topics: object) -> list[str]:
    seen: set[str] = {"high-level overview"}
    cleaned: list[str] = ["High-level overview"]
    if not isinstance(raw_topics, list):
        return cleaned
    for raw_topic in raw_topics:
        topic = " ".join(str(raw_topic or "").split()).strip(" ,.;:-")
        if not topic:
            continue
        topic_key = topic.lower()
        if topic_key in seen:
            continue
        seen.add(topic_key)
        cleaned.append(topic)
    return cleaned[:15]


def _substack_slide_handoff(
    focus_topic: str,
    context_request: str,
    article_topics: list[str],
    article_body: str,
    substack_url: str,
) -> str:
    topics_line = ", ".join(_normalize_substack_topics(article_topics)) or "(infer from article)"
    return (
        "Return ONLY valid JSON as an array. No markdown, no commentary outside JSON.\n\n"
        "Each object must include exactly: row_number, name, text1, text2, text3, text4, text5, text6\n\n"
        "Create a 6-slide Instagram carousel for Vote In Or Out promoting a Substack election article.\n"
        "Keep row_number exactly as shown in the input.\n"
        "Use plain language, no hashtags, no citations, no markdown, and no newline characters inside values.\n"
        "Each slide should be self-contained and specific.\n"
        'Set the "name" field to "@voteinorout".\n'
        "text1 is the strongest opening slide under 350 characters.\n"
        "text2, text3, text4, and text5 are semi-longer explainer slides, usually 500 to 800 characters each.\n"
        "text6 is the closing slide under 500 characters. It should point people to the full article without adding a URL.\n"
        "Every text2-text5 slide must include at least one concrete piece of data from the article: a date, number, office, jurisdiction, candidate name, quote, poll, vote margin, dollar amount, legal status, or other specific fact.\n"
        "Do not write generic summary slides. Pull details directly from the article and distribute them across the six slides.\n"
        "Focus the carousel on the selected article topic.\n"
        "Use the extra user context only as direction, not as a source of new facts.\n"
        'On the final slide, say the full article covers this topic and more, and name at least two other article topics when possible.\n'
        "No em dashes, emojis, hashtags, paragraph breaks, or newline characters inside text fields.\n"
        "Collapse all whitespace into normal single spaces before returning JSON.\n"
        "No speculation or invented framing.\n"
        "Never repeat the same fact, quote, setup, accusation, or disclaimer across slides.\n\n"
        "ROW [ROW_NUMBER]\n"
        f"Substack URL: {substack_url}\n"
        f"Focus topic: {(focus_topic or '').strip() or '(infer from article)'}\n"
        f"Article topics: {topics_line}\n"
        f"Extra context from user: {(context_request or '').strip() or '(none)'}\n\n"
        "Slide requirement: focus on the selected topic, use concrete article data points across slides 2 through 5, and explicitly say the full article covers this topic and more on slide 6.\n\n"
        f"Article:\n{article_body}"
    ).strip()


def _substack_caption_footer(substack_url: str) -> str:
    return (
        f"Comment LINK (on instagram) and we will DM you the link to {substack_url}\n\n"
        "Help this information get to more voters. 🇺🇸 "
        "A well-informed electorate is a prerequisite to Democracy.—Thomas Jefferson"
    )


def _ensure_substack_caption_footer(caption: str, substack_url: str) -> str:
    footer = _substack_caption_footer(substack_url)
    cleaned = (caption or "").strip()
    existing_cta = re.search(r"\bComment\s+\w+\s+\(on instagram\)", cleaned)
    if existing_cta:
        cleaned = cleaned[:existing_cta.start()].strip()
    body_parts = [part.strip() for part in re.split(r"\n\s*\n", cleaned) if part.strip()]
    if len(body_parts) > 2:
        cleaned = "\n\n".join([body_parts[0], " ".join(body_parts[1:])])
    return f"{cleaned}\n\n{footer}".strip()


def _extract_json_payload(raw_text: str):
    raw = (raw_text or "").strip()
    if not raw:
        raise ValueError("Paste the model output first.")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", raw)
        if not match:
            raise ValueError("Could not find JSON in pasted text.")
        return json.loads(match.group(1))


def _single_paragraph_slide_text(value: object) -> str:
    return " ".join(str(value or "").split())


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
        "name": _single_paragraph_slide_text(selected.get("name")),
        "text1": _single_paragraph_slide_text(selected.get("text1")),
        "text2": _single_paragraph_slide_text(selected.get("text2")),
        "text3": _single_paragraph_slide_text(selected.get("text3")),
        "text4": _single_paragraph_slide_text(selected.get("text4")),
        "text5": _single_paragraph_slide_text(selected.get("text5")),
        "text6": _single_paragraph_slide_text(selected.get("text6")),
    }


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
        slide_text = (slides.get(slide_key) or "").strip()
        if slide_text:
            slide_lines.append(f"{slide_key.upper()}: {slide_text}")
    slide_summary = "\n".join(slide_lines)
    response = _get_openai_client().chat.completions.create(
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
                    f"Focus topic: {(focus_topic or '').strip()}\n"
                    f"Article topics: {', '.join(_normalize_substack_topics(article_topics)) or '(infer from article)'}\n"
                    f"Extra context from user: {(context_request or '').strip() or '(none)'}\n\n"
                    f"Finalized slides:\n{slide_summary}\n\n"
                    f"Article:\n{article_body}\n\n"
                    f"Required CTA/footer:\n{_substack_caption_footer(substack_url)}"
                ),
            },
        ],
        max_tokens=700,
    )
    return _ensure_substack_caption_footer(response.choices[0].message.content or "", substack_url)


articles_tab, comments_tab = st.tabs(["Articles", "Comments"])


# ---------------------------------------------------------------------------
# Articles tab
# ---------------------------------------------------------------------------

with articles_tab:
    open_rows = sheet_ops.get_open_substack_rows(GOOGLE_SHEET_ID)

    if not open_rows:
        st.info("No open Substack articles. Paste a URL below to add one.")
        new_url = st.text_input("Substack article URL")
        if st.button("Add Article", type="primary"):
            if new_url.strip():
                sheet_ops.append_substack_row(GOOGLE_SHEET_ID, new_url.strip())
                st.success("Article added.")
            else:
                st.warning("Enter a URL first.")
    else:
        options = sorted(
            open_rows,
            key=lambda row: int(row.get("row_number") or 0),
            reverse=True,
        )
        selected_row = st.selectbox(
            "Select article",
            options,
            format_func=lambda row: (
                (row.get("name") or "").strip()
                or (row.get("url") or "").strip()[:80]
                or f"Row {row.get('row_number', '')}"
            ),
        )
        substack_url = selected_row["url"]
        row_number = selected_row["row_number"]

        fetch_key = f"substack_fetched_{substack_url}"
        fetch_meta_key = f"substack_fetch_meta_{substack_url}"
        article_body = selected_row.get("article", "").strip() or st.session_state.get(fetch_key, "").strip()
        topics_key = f"substack_topics_{substack_url}"
        if article_body:
            st.caption("The latest article text will be fetched from the Substack link when you generate the topic breakdown.")
        else:
            st.caption("Article text will be fetched from the Substack link when you generate the topic breakdown.")

        if st.button("Generate Topic Breakdown", type="primary"):
            try:
                fetched_article_body = ""
                try:
                    with st.spinner("Fetching article text…"):
                        result = fetch_article_source(substack_url)
                    fetched_article_body = (result.get("source_text") or "").strip()
                    st.session_state[fetch_meta_key] = result
                except Exception:
                    fetched_article_body = ""

                if fetched_article_body:
                    article_body = fetched_article_body
                    st.session_state[fetch_key] = article_body
                    sheet_ops.update_substack_article(GOOGLE_SHEET_ID, row_number, article_body)

                if not article_body:
                    raise RuntimeError("Could not fetch article text from this Substack link.")

                resp = _get_openai_client().chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": _substack_topic_breakdown_prompt()},
                        {"role": "user", "content": f"Article:\n\n{article_body}"},
                    ],
                    max_tokens=500,
                )
                raw = (resp.choices[0].message.content or "").strip()
                raw = raw.lstrip("```json").lstrip("```").rstrip("```").strip()
                topic_breakdown = _normalize_substack_topics(json.loads(raw))
                if not topic_breakdown:
                    raise ValueError("No valid topics returned.")
                st.session_state[topics_key] = topic_breakdown
            except Exception as e:
                st.error(f"Failed to generate topic breakdown: {e}")

        topic_breakdown = _normalize_substack_topics(st.session_state.get(topics_key, []))
        if topic_breakdown:
            st.markdown("**Article topic breakdown**")
            focus_topic = st.selectbox(
                "Focus topic",
                topic_breakdown,
                key=f"substack_focus_topic_{substack_url}",
            )
            context_request = st.text_area(
                "Context to emphasize",
                key=f"substack_context_{substack_url}",
                height=110,
                placeholder="Add what angle or context you want emphasized in the post.",
            )
            st.caption("Topics found: " + ", ".join(topic_breakdown))

            if st.button("Create Promote Draft", type="primary"):
                try:
                    fetch_meta = st.session_state.get(fetch_meta_key) or {}
                    sheet_ops.append_generated_post_rows(
                        GOOGLE_SHEET_ID,
                        [
                            {
                                "url": substack_url,
                                "source_username": "voteinorout",
                                "caption": "",
                                "media_type": "article",
                                "thumbnail_link": (fetch_meta.get("image_url") or "").strip(),
                                "original_caption": article_body,
                                "transcript": article_body,
                                "caption_context": _substack_promote_context(
                                    substack_url,
                                    focus_topic,
                                    context_request,
                                    topic_breakdown,
                                ),
                                "name": "@voteinorout",
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
                except Exception as e:
                    st.error(f"Could not create promote draft: {e}")
                else:
                    sheet_ops.update_substack_status(GOOGLE_SHEET_ID, row_number, "posts created")
                    st.success("Promote draft created in the posts tab.")

        # ── Generated Posts view ──────────────────────────────────────────
        all_post_rows = sheet_ops.get_all_rows(GOOGLE_SHEET_ID)
        generated = []
        for r in all_post_rows:
            if (r.get("Instagram URL") or "").strip() != substack_url:
                continue
            meta = _parse_substack_promote_context(r.get("Caption Context", ""))
            if meta.get("source") != "substack_promote":
                continue
            generated.append({"row_number": r.get("row_number"), "row": r, "meta": meta})

        if generated:
            st.markdown("---")
            st.markdown("### Generated Posts")
            for post in generated:
                row = post["row"]
                meta = post["meta"]
                status = row.get("Status", "")
                focus_topic = (meta.get("focus_topic") or meta.get("angle") or "").strip()
                with st.expander(f"{(focus_topic or '(no topic)')[:80]} — {status}", expanded=False):
                    context_request = (meta.get("context_request") or "").strip()
                    if context_request:
                        st.markdown(f"**Context:** {context_request}")
                    article_topics = _normalize_substack_topics(meta.get("article_topics") or meta.get("topics") or [])
                    if article_topics:
                        st.markdown(f"**Article topics:** {', '.join(article_topics)}")
                    st.markdown(f"**CTA:** {row.get('Slide CTA', '')}")
                    st.markdown("**Caption**")
                    st.code(row.get("Generated Caption", ""), language=None)
                    for slide_num in range(1, 7):
                        st.markdown(f"**Slide {slide_num}**")
                        st.code(row.get(f"text{slide_num}", ""), language=None)
                    if status != "posted":
                        slide_results = st.text_area(
                            "Paste slide results",
                            key=f"substack_slide_results_{row['row_number']}",
                            height=110,
                            placeholder=(
                                f'[{{"row_number":{row["row_number"]},"name":"...",'
                                '"text1":"...","text2":"...","text3":"...",'
                                '"text4":"...","text5":"...","text6":"..."}}]'
                            ),
                        )
                        if st.button(
                            "Save slide results",
                            type="primary",
                            key=f"substack_save_slides_{row['row_number']}",
                        ):
                            try:
                                slides = _substack_slide_result(slide_results, int(row["row_number"]))
                                if not all(slides[f"text{i}"] for i in range(1, 7)):
                                    raise ValueError("Slide result must include text1 through text6.")
                                sheet_ops.update_generated_post_slides_and_status(
                                    GOOGLE_SHEET_ID,
                                    int(row["row_number"]),
                                    slides["name"],
                                    slides["text1"],
                                    slides["text2"],
                                    slides["text3"],
                                    slides["text4"],
                                    slides["text5"],
                                    slides["text6"],
                                    "slides",
                                )
                                caption = _generate_substack_caption_from_slides(
                                    substack_url,
                                    (row.get("Original Caption") or "").strip() or article_body,
                                    focus_topic,
                                    context_request,
                                    article_topics,
                                    slides,
                                )
                                sheet_ops.update_caption(GOOGLE_SHEET_ID, int(row["row_number"]), caption, "slides")
                                st.session_state.pop(f"substack_slide_results_{row['row_number']}", None)
                            except Exception as e:
                                st.error(f"Could not save slide results: {e}")
                            else:
                                st.success("Slide results and caption saved to the posts tab.")
                        st.markdown("**Slide prompt**")
                        st.code(
                            _substack_slide_handoff(
                                focus_topic,
                                context_request,
                                article_topics,
                                (row.get("Original Caption") or "").strip() or article_body,
                                substack_url,
                            ).replace("[ROW_NUMBER]", str(row["row_number"])),
                            language=None,
                        )
                        if st.button(
                            "Mark as Posted",
                            key=f"substack_mark_posted_{row['row_number']}",
                        ):
                            sheet_ops.update_status(GOOGLE_SHEET_ID, row["row_number"], "posted")

        st.markdown("---")
        st.link_button("Open Substack Link", substack_url)


# ---------------------------------------------------------------------------
# Comments tab
# ---------------------------------------------------------------------------

with comments_tab:
    col1, col2 = st.columns([1, 1])
    with col1:
        check_btn = st.button("Check for New Comments", type="primary")
    with col2:
        if st.button("Clear"):
            st.session_state.pop("monitor_summaries", None)

    summaries: list[dict] = st.session_state.get("monitor_summaries", [])

    if check_btn:
        if not APIFY_API_TOKEN:
            st.error("APIFY_API_TOKEN is not configured.")
        else:
            monitor_rows = sheet_ops.get_open_comment_monitor_rows(GOOGLE_SHEET_ID)
            if not monitor_rows:
                st.info("No open monitoring rows found. Set `monitoring status` to `open` and fill in `instagram url` on the Substack sheet.")
            else:
                new_summaries = []
                apify_client = ApifyClient(APIFY_API_TOKEN)
                actor = apify_client.actor(APIFY_POST_ACTOR_ID)
                now_str = datetime.now(timezone.utc).isoformat(timespec="seconds")

                for row in monitor_rows:
                    url = row.get("url", "").strip()
                    label = row.get("label", "").strip() or row.get("substack_url", "").strip() or url
                    source = row.get("source", "substack")
                    row_number = row["row_number"]

                    try:
                        run_input = {
                            "directUrls": [url],
                            "resultsType": "comments",
                            "resultsLimit": 200,
                        }
                        run = actor.call(run_input=run_input)
                        items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())

                        comment_texts = []
                        for item in items:
                            for comment in item.get("latestComments", item.get("comments", [])):
                                text = (comment.get("text") or "").strip()
                                if text:
                                    comment_texts.append(text)

                        if not comment_texts:
                            summary_text = "(No comments found)"
                        else:
                            joined = "\n".join(f"- {c}" for c in comment_texts[:150])
                            summary_resp = _get_openai_client().chat.completions.create(
                                model="gpt-4o",
                                messages=[
                                    {
                                        "role": "system",
                                        "content": (
                                            "Summarize what people are repeatedly asking, pushing back on, or saying is missing or biased. "
                                            "Focus on patterns across comments, not individual opinions. Keep it under 150 words."
                                        ),
                                    },
                                    {"role": "user", "content": joined},
                                ],
                                max_tokens=250,
                            )
                            summary_text = (summary_resp.choices[0].message.content or "").strip()

                        sheet_ops.update_comment_monitor_summary(
                            GOOGLE_SHEET_ID,
                            source,
                            row_number,
                            summary_text,
                            now_str,
                        )
                        new_summaries.append({"label": label, "url": url, "summary": summary_text})

                    except Exception as e:
                        st.error(f"{label}: {e}")
                        new_summaries.append({"label": label, "url": url, "summary": f"Error: {e}"})

                st.session_state["monitor_summaries"] = new_summaries + summaries

    for entry in st.session_state.get("monitor_summaries", []):
        st.markdown(f"**{entry['label']}**")
        st.caption(entry["url"])
        st.write(entry["summary"])
        st.divider()
