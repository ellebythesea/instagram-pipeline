# app.py
"""Streamlit app: paste an Instagram link, get a caption back."""

import os
import shutil
import streamlit as st

from config import APP_PASSWORD
from instagram import download_instagram_post
from caption import transcribe_video, generate_caption


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def check_password() -> bool:
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


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Instagram Caption Generator", page_icon="📸", layout="centered")

st.title("Instagram Caption Generator")
st.caption("Paste a reel or post link, enter the speaker's name, and get a ready-to-post caption.")

if not check_password():
    st.stop()

# Form
with st.form("caption_form"):
    ig_url = st.text_input("Instagram URL", placeholder="https://www.instagram.com/reel/...")
    speaker = st.text_input("Speaker name", placeholder="e.g. Alexandria Ocasio-Cortez")
    extra = st.text_area("Additional prompt (optional)", placeholder="Focus on the economic angle...", height=80)
    submitted = st.form_submit_button("Generate Caption", type="primary", use_container_width=True)

if submitted:
    if not ig_url.strip():
        st.warning("Please enter an Instagram URL.")
        st.stop()

    # Step 1 -- download
    with st.status("Downloading video...", expanded=True) as status:
        post = download_instagram_post(ig_url.strip())
        if post is None:
            st.error(
                "Could not download the video. Make sure the URL is a public Instagram reel or post "
                "and that the link is correct."
            )
            st.stop()
        status.update(label="Video downloaded", state="complete")

    try:
        # Step 2 -- transcribe
        with st.status("Transcribing audio...", expanded=True) as status:
            transcript = transcribe_video(post.video_path)
            if not transcript:
                st.error("Transcription failed. The video may not contain audible speech.")
                st.stop()
            status.update(label="Transcription complete", state="complete")

        # Step 3 -- generate caption
        with st.status("Generating caption...", expanded=True) as status:
            caption_body = generate_caption(transcript, speaker_name=speaker.strip(), extra_prompt=extra.strip())
            status.update(label="Caption ready", state="complete")

        # Step 4 -- build footer
        footer_parts = []
        if post.original_caption:
            footer_parts.append(post.original_caption.strip())
        if post.username and post.username != "unknown":
            footer_parts.append(f"@{post.username.lstrip('@')}")

        footer = "\n".join(footer_parts) if footer_parts else ""

        full_caption = caption_body
        if footer:
            full_caption = f"{caption_body}\n\n---\n{footer}"

        # Display
        st.divider()
        st.subheader("Your Caption")
        st.text_area("Copy this caption", value=full_caption, height=300, label_visibility="collapsed")

        # Also show pieces in expanders for transparency
        with st.expander("Transcript"):
            st.write(transcript)
        with st.expander("Original Instagram caption"):
            st.write(post.original_caption or "(none)")
        with st.expander("Username"):
            st.write(f"@{post.username}" if post.username != "unknown" else "(unknown)")

    finally:
        # Cleanup downloaded video
        try:
            parent = os.path.dirname(post.video_path)
            shutil.rmtree(parent, ignore_errors=True)
        except Exception:
            pass
