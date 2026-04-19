"""Streamlit entrypoint with explicit sidebar page order."""

import streamlit as st


navigation = st.navigation(
    [
        st.Page("pages/instagram_pipeline.py", title="Instagram Pipeline", icon="📋"),
        st.Page("pages/post_editor.py", title="Post Editor", icon="✏️"),
        st.Page("pages/headlines.py", title="Headline Generator", icon="🗞️"),
        st.Page("pages/reel_downloader.py", title="Reel Downloader", icon="🎞️"),
        st.Page("caption_this_page.py", title="Caption This", icon="📸"),
    ]
)

navigation.run()
