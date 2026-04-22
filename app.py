"""Streamlit entrypoint with explicit sidebar page order."""

import streamlit as st


navigation = st.navigation(
    [
        st.Page("pages/workspace.py", title="Workspace", icon="🏠", default=True),
        st.Page("pages/instagram_pipeline.py", title="Instagram pipeline", icon="📋"),
        st.Page("pages/post_editor.py", title="Post editor", icon="✏️"),
        st.Page("pages/headlines.py", title="Headline generator", icon="🗞️"),
        st.Page("pages/reel_downloader.py", title="Media downloader", icon="🎞️"),
        st.Page("caption_this_page.py", title="Caption this", icon="📸"),
    ]
)

navigation.run()
