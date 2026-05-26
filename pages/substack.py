"""Substack article post generator and comment monitor."""

import json
import os
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
import pipeline_caption as pipeline_caption_ops
import sheets as sheet_ops
from utils.auth import require_auth
from utils.styles import inject as inject_styles

st.set_page_config(page_title="Substack", page_icon="📝", layout="wide")
inject_styles()
st.title("Substack")

if not require_auth():
    st.stop()

_openai_client: openai.OpenAI | None = None


def _get_openai_client() -> openai.OpenAI:
    global _openai_client
    if _openai_client is None:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        _openai_client = openai.OpenAI(api_key=OPENAI_API_KEY, timeout=60.0, max_retries=1)
    return _openai_client


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
                st.rerun()
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
            format_func=lambda row: f"Row {row.get('row_number', '')}: {row.get('url', '')[:80]}",
        )
        substack_url = selected_row["url"]
        row_number = selected_row["row_number"]

        # ── Article body ──────────────────────────────────────────────────
        article_body = selected_row.get("article", "").strip()

        if article_body:
            st.text_area("Article body", value=article_body, height=120, disabled=True)
        else:
            fetch_key = f"substack_fetched_{substack_url}"
            if fetch_key not in st.session_state:
                try:
                    with st.spinner("Fetching article body…"):
                        result = fetch_article_source(substack_url)
                        st.session_state[fetch_key] = result.get("source_text", "")
                except Exception as e:
                    st.session_state[fetch_key] = ""
                    st.error(f"Could not auto-fetch article: {e}. Paste the body below.")

            prefill = st.session_state.get(fetch_key, "")
            edited = st.text_area(
                "Article body",
                value=prefill,
                height=140,
                key=f"substack_article_edit_{substack_url}",
            )
            if st.button("Save Article Body"):
                body_to_save = edited.strip()
                if body_to_save:
                    sheet_ops.update_substack_article(GOOGLE_SHEET_ID, row_number, body_to_save)
                    st.success("Saved.")
                    st.rerun()
                else:
                    st.warning("Article body is empty.")
            article_body = edited.strip()

        # ── Idea generation ───────────────────────────────────────────────
        topics = st.text_input("Topics or angles to cover (optional)", key="substack_topics")
        ideas_key = f"substack_ideas_{substack_url}"

        if st.button("Generate Post Ideas", type="primary", disabled=not article_body):
            user_msg = f"Article:\n\n{article_body}"
            if topics.strip():
                user_msg += f"\n\nPrioritize these angles: {topics.strip()}"
            try:
                resp = _get_openai_client().chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are a social media strategist for a political news Instagram account called Vote In Or Out.\n\n"
                                "Given this Substack article, generate exactly 5 post ideas. Each idea should tease a specific fact, "
                                "quote, or angle from the article and feel like clickbait that makes someone want to read the full piece.\n\n"
                                "Return ONLY valid JSON as an array of 5 objects. Each must have:\n"
                                '- "angle": one sentence describing the post angle\n'
                                '- "hook": the strongest opening line for that post, under 100 characters\n\n'
                                "No markdown. No commentary outside JSON."
                            ),
                        },
                        {"role": "user", "content": user_msg},
                    ],
                    max_tokens=800,
                )
                raw = (resp.choices[0].message.content or "").strip()
                raw = raw.lstrip("```json").lstrip("```").rstrip("```").strip()
                ideas = json.loads(raw)
                st.session_state[ideas_key] = ideas
            except Exception as e:
                st.error(f"Failed to generate ideas: {e}")

        ideas = st.session_state.get(ideas_key)
        if ideas:
            st.markdown("**Select ideas to turn into posts:**")
            selected_ideas = []
            for i, idea in enumerate(ideas):
                checked = st.checkbox(
                    idea.get("hook", f"Idea {i + 1}"),
                    key=f"substack_idea_{substack_url}_{i}",
                )
                st.caption(idea.get("angle", ""))
                if checked:
                    selected_ideas.append(idea)

            if st.button("Create Selected Posts", type="primary", disabled=not selected_ideas):
                created = 0
                post_rows = []
                progress = st.progress(0, text="Generating posts…")
                for idx, idea in enumerate(selected_ideas):
                    angle = idea.get("angle", "")
                    try:
                        caption_resp = _get_openai_client().chat.completions.create(
                            model="gpt-4o",
                            messages=[
                                {
                                    "role": "system",
                                    "content": (
                                        "You are writing an Instagram caption for Vote In Or Out, a political news account.\n"
                                        "This post promotes a Substack article. Write a caption that:\n"
                                        "- Opens with the specific angle provided\n"
                                        "- Teases what the full article covers without giving everything away\n"
                                        f'- Ends with exactly this line: "Comment LINK (on instagram) and we will DM you the link to {substack_url}"\n'
                                        "- Appends the footer below the caption on a new line\n"
                                        "Keep it under 1300 characters. No hashtags unless they appear in the source material."
                                    ),
                                },
                                {
                                    "role": "user",
                                    "content": (
                                        f"Angle: {angle}\n\n"
                                        f"Article:\n{article_body}\n\n"
                                        f"Footer:\n{DEFAULT_POST_FOOTER}"
                                    ),
                                },
                            ],
                            max_tokens=600,
                        )
                        caption = (caption_resp.choices[0].message.content or "").strip()

                        slide_row = {
                            "Transcript": "",
                            "Original Caption": article_body,
                            "Caption Context": f"This post is promoting a Substack article. Angle: {angle}",
                            "Speaker Name": "",
                            "Source Username": "voteinorout",
                            "Required Hashtags": "",
                        }
                        slides = pipeline_caption_ops.generate_carousel_copy_with_model(
                            slide_row, model="gpt-4o"
                        )

                        post_rows.append(
                            {
                                "url": substack_url,
                                "angle": angle,
                                "caption": caption,
                                "text1": slides.get("text1", ""),
                                "text2": slides.get("text2", ""),
                                "text3": slides.get("text3", ""),
                                "cta": "Save link for Substack",
                                "status": "generated",
                            }
                        )
                        created += 1
                    except Exception as e:
                        st.error(f"Failed to create post for angle '{angle[:60]}': {e}")
                    progress.progress(
                        (idx + 1) / len(selected_ideas),
                        text=f"Generated {idx + 1} of {len(selected_ideas)}…",
                    )

                progress.empty()

                if post_rows:
                    sheet_ops.append_substack_post_rows(GOOGLE_SHEET_ID, post_rows)
                    sheet_ops.append_link_rows(GOOGLE_SHEET_ID, [substack_url] * len(post_rows))
                    sheet_ops.update_substack_status(GOOGLE_SHEET_ID, row_number, "posts created")
                    st.success(
                        f"{created} post{'s' if created != 1 else ''} created and added to your main Posts sheet."
                    )
                    st.session_state.pop(ideas_key, None)
                    st.rerun()

        # ── Generated Posts view ──────────────────────────────────────────
        all_post_rows = sheet_ops.get_substack_post_rows(GOOGLE_SHEET_ID)
        generated = [r for r in all_post_rows if r.get("url", "") == substack_url]

        if generated:
            st.markdown("---")
            st.markdown("### Generated Posts")
            for post in generated:
                status = post.get("status", "")
                angle_label = post.get("angle", "(no angle)")
                with st.expander(f"{angle_label[:80]} — {status}", expanded=False):
                    st.markdown(f"**CTA:** {post.get('cta', '')}")
                    st.markdown("**Caption**")
                    st.code(post.get("caption", ""), language=None)
                    st.markdown("**Slide 1**")
                    st.code(post.get("text1", ""), language=None)
                    st.markdown("**Slide 2**")
                    st.code(post.get("text2", ""), language=None)
                    st.markdown("**Slide 3**")
                    st.code(post.get("text3", ""), language=None)
                    if status != "posted":
                        if st.button(
                            "Mark as Posted",
                            key=f"substack_mark_posted_{post['row_number']}",
                        ):
                            sheet_ops.update_substack_post_status(
                                GOOGLE_SHEET_ID, post["row_number"], "posted"
                            )
                            st.rerun()


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
            st.rerun()

    summaries: list[dict] = st.session_state.get("monitor_summaries", [])

    if check_btn:
        if not APIFY_API_TOKEN:
            st.error("APIFY_API_TOKEN is not configured.")
        else:
            monitor_rows = sheet_ops.get_open_monitor_rows(GOOGLE_SHEET_ID)
            if not monitor_rows:
                st.info("No open monitor rows found.")
            else:
                new_summaries = []
                apify_client = ApifyClient(APIFY_API_TOKEN)
                actor = apify_client.actor(APIFY_POST_ACTOR_ID)
                now_str = datetime.now(timezone.utc).isoformat(timespec="seconds")

                for row in monitor_rows:
                    url = row.get("url", "").strip()
                    label = row.get("label", "").strip() or url
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

                        sheet_ops.update_monitor_summary(GOOGLE_SHEET_ID, row_number, summary_text, now_str)
                        new_summaries.append({"label": label, "url": url, "summary": summary_text})

                    except Exception as e:
                        st.error(f"{label}: {e}")
                        new_summaries.append({"label": label, "url": url, "summary": f"Error: {e}"})

                st.session_state["monitor_summaries"] = new_summaries + summaries
                st.rerun()

    for entry in st.session_state.get("monitor_summaries", []):
        st.markdown(f"**{entry['label']}**")
        st.caption(entry["url"])
        st.write(entry["summary"])
        st.divider()
