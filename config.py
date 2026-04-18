# config.py
import os
import streamlit as st


def _get_secret(key: str, default: str = "") -> str:
    """Read a secret from Streamlit Cloud secrets first, then env vars."""
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)


OPENAI_API_KEY = _get_secret("OPENAI_API_KEY")
SERPER_API_KEY = _get_secret("SERPER_API_KEY")
APP_PASSWORD = _get_secret("APP_PASSWORD")

# Audio preprocessing
TRIM_SILENCE = _get_secret("TRIM_SILENCE", "false").lower() in {"1", "true", "yes"}
AUDIO_SAMPLE_RATE = int(_get_secret("AUDIO_SAMPLE_RATE", "16000"))
AUDIO_CHANNELS = int(_get_secret("AUDIO_CHANNELS", "1"))
AUDIO_BITRATE = _get_secret("AUDIO_BITRATE", "32k")

# Caption generation
CAPTION_SPLIT_THRESHOLD = int(_get_secret("CAPTION_SPLIT_THRESHOLD", "400"))

# Instagram Pipeline (batch Google Sheets workflow)
ANTHROPIC_API_KEY = _get_secret("ANTHROPIC_API_KEY")
APIFY_API_TOKEN = _get_secret("APIFY_API_TOKEN")
APIFY_ACTOR_ID = _get_secret("APIFY_ACTOR_ID", "apify/instagram-scraper")
GOOGLE_SHEET_ID = _get_secret("GOOGLE_SHEET_ID")
GOOGLE_DRIVE_FOLDER_ID = _get_secret("GOOGLE_DRIVE_FOLDER_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = _get_secret("GOOGLE_SERVICE_ACCOUNT_JSON")
