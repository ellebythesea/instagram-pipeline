"""Streamlit entrypoint for the workspace UI."""

import streamlit as st

pg = st.navigation([
    st.Page("pages/workspace.py", title="Workspace", icon="🏠"),
    st.Page("pages/instagram_pipeline.py", title="Instagram Pipeline", icon="📋"),
    st.Page("pages/post_editor.py", title="Post Editor", icon="✏️"),
    st.Page("pages/headlines.py", title="Headlines", icon="🗞️"),
    st.Page("pages/reel_downloader.py", title="Media Downloader", icon="🎞️"),
])
pg.run()
