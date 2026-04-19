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
    else:
        from post_scraper import process_url
    return process_url(url)


def _generate_headline(source_text: str) -> str:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": (
                    "You write short, salacious, attention-grabbing political headlines. "
                    "Return one headline only. Keep it under 12 words. "
                    "Do not use hashtags. Do not use quotation marks unless essential. "
                    "Do not add labels, bullets, or extra explanation."
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
    return response.choices[0].message.content.strip().replace("#", "")


st.set_page_config(page_title="Headlines", page_icon="🗞️", layout="centered")
st.title("Headlines")
st.caption("Paste an Instagram URL and get a short salacious headline based on the post caption.")

if not _check_password():
    st.stop()

with st.form("headline_form"):
    ig_url = st.text_input("Instagram URL", placeholder="https://www.instagram.com/p/... or /reel/...")
    submitted = st.form_submit_button("Generate Headline", type="primary", use_container_width=True)

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
    transcript = (post.get("transcript") or "").strip()
    source_text = original_caption or transcript

    if not source_text:
        st.error("Apify did not return a caption or transcript for this URL.")
        st.stop()

    with st.status("Generating headline...", expanded=True) as status:
        headline = _generate_headline(source_text)
        status.update(label="Headline ready", state="complete")

    st.divider()
    st.subheader("Headline")
    st.code(headline, language=None)

    with st.expander("Source caption"):
        st.write(original_caption or "(none)")
    if transcript:
        with st.expander("Transcript fallback"):
            st.write(transcript)
