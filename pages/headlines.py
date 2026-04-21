"""Headline generator page."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import openai
import streamlit as st

from config import APP_PASSWORD, OPENAI_API_KEY


def _check_password() -> bool:
    if not APP_PASSWORD:
        return True
    if st.session_state.get("authenticated"):
        return True
    pwd = st.text_input("Password", type="password")
    if pwd:
        if pwd == APP_PASSWORD:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False


def _fetch_post_data(url: str) -> dict:
    if "/reel/" in url.lower() or "/reels/" in url.lower():
        from reel_scraper import process_url
        return process_url(url, include_transcript=False)
    else:
        from post_scraper import process_url
        return process_url(url)


def _generate_headlines(source_text: str) -> list[str]:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
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


def _generate_caption_from_caption(source_text: str) -> str:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": (
                    "You write sharp political Instagram captions from an existing Instagram caption only. "
                    "Return exactly two short paragraphs, no hashtags, no labels, and no quotation marks unless essential. "
                    "Do not mention transcription or missing audio. Keep it concise, punchy, and readable."
                ),
            },
            {
                "role": "user",
                "content": f"Write a caption from this Instagram caption:\n\n{source_text}",
            },
        ],
        max_tokens=220,
        temperature=0.5,
    )
    return response.choices[0].message.content.strip()


def _build_footered_caption(caption_body: str, username: str) -> str:
    footer_parts = []
    cleaned_username = (username or "").strip().lstrip("@")
    if cleaned_username and cleaned_username.lower() != "unknown":
        footer_parts.append(f"Follow @{cleaned_username} for more.")
    footer_parts.append(
        "Help this information get to more voters. 🇺🇸 "
        "A well-informed electorate is a prerequisite to Democracy. - Thomas Jefferson"
    )
    return f"{caption_body.strip()}\n\n{' '.join(footer_parts)}"


st.set_page_config(page_title="Headlines", page_icon="🗞️", layout="centered")
st.title("Headlines")
st.caption("Paste an Instagram URL and get three salacious headlines plus a caption based only on the post caption.")

if not _check_password():
    st.stop()

with st.form("headline_form"):
    ig_url = st.text_input("Instagram URL", placeholder="https://www.instagram.com/p/... or /reel/...")
    submitted = st.form_submit_button("Generate Headlines", type="primary", use_container_width=True)

if submitted:
    if not ig_url.strip():
        st.warning("Please enter an Instagram URL.")
        st.stop()

    url = ig_url.strip()
    with st.status("Fetching Instagram data from Apify...", expanded=True) as status:
        try:
            post = _fetch_post_data(url)
        except Exception as e:
            st.error(f"Could not fetch Instagram data from Apify: {e}")
            st.stop()
        status.update(label="Instagram data fetched", state="complete")

    original_caption = (post.get("original_caption") or "").strip()
    source_text = original_caption

    if not source_text:
        st.error("Apify did not return an Instagram caption for this URL.")
        st.stop()

    with st.status("Generating headlines and caption...", expanded=True) as status:
        headlines = _generate_headlines(source_text)
        caption_body = _generate_caption_from_caption(source_text)
        final_caption = _build_footered_caption(caption_body, post.get("username", ""))
        status.update(label="Headlines and caption ready", state="complete")

    st.divider()
    st.subheader("Headline options")
    for idx, headline in enumerate(headlines, start=1):
        st.caption(f"Option {idx}")
        st.code(headline, language=None)

    st.subheader("Caption")
    st.code(final_caption, language=None)

    with st.expander("Source caption"):
        st.write(original_caption or "(none)")
