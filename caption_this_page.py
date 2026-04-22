"""Streamlit page: paste an Instagram link and generate a caption."""

import streamlit as st

from caption import generate_caption
from utils.auth import require_auth
from utils.styles import inject as inject_styles


st.set_page_config(page_title="Caption this", page_icon="📸", layout="centered")
inject_styles()

st.title("Caption this")
st.caption("Paste a reel or post link, enter the speaker's name, and get a ready-to-post caption.")

if not require_auth():
    st.stop()

with st.form("caption_form"):
    ig_url = st.text_input("Instagram URL", placeholder="https://www.instagram.com/reel/...")
    speaker = st.text_input("Speaker name", placeholder="e.g. Alexandria Ocasio-Cortez")
    extra = st.text_area("Additional prompt (optional)", placeholder="Focus on the economic angle...", height=80)
    submitted = st.form_submit_button("Generate Caption", type="primary", use_container_width=True)

if submitted:
    if not ig_url.strip():
        st.warning("Please enter an Instagram URL.")
        st.stop()

    url = ig_url.strip()

    with st.status("Fetching post data from Apify...", expanded=True) as status:
        try:
            if "/reel/" in url.lower() or "/reels/" in url.lower():
                from reel_scraper import process_url
            else:
                from post_scraper import process_url
            post = process_url(url)
        except Exception as e:
            st.error(f"Could not fetch Instagram data from Apify: {e}")
            st.stop()
        status.update(label="Instagram data fetched", state="complete")

    transcript = (post.get("transcript") or "").strip()
    original_caption = (post.get("original_caption") or "").strip()
    source_text = transcript or original_caption

    if not source_text:
        st.error("Apify did not return a transcript or original caption for this URL.")
        st.stop()

    with st.status("Generating caption...", expanded=True) as status:
        caption_body = generate_caption(source_text, speaker_name=speaker.strip(), extra_prompt=extra.strip())
        status.update(label="Caption ready", state="complete")

    footer_parts = []
    if original_caption:
        footer_parts.append(original_caption)
    username = (post.get("username") or "").strip()
    if username and username != "unknown":
        footer_parts.append(f"@{username.lstrip('@')}")

    footer = "\n".join(footer_parts) if footer_parts else ""

    full_caption = caption_body
    if footer:
        full_caption = f"{caption_body}\n\n---\n{footer}"

    st.divider()
    st.subheader("Your Caption")
    st.text_area("Copy this caption", value=full_caption, height=300, label_visibility="collapsed")

    with st.expander("Transcript"):
        st.write(transcript or "(none from Apify)")
    with st.expander("Original Instagram caption"):
        st.write(original_caption or "(none)")
    with st.expander("Username"):
        st.write(f"@{username}" if username and username != "unknown" else "(unknown)")
