# config.py
from __future__ import annotations

import os
from pathlib import Path


try:
    import tomllib
except ImportError:  # pragma: no cover
    tomllib = None

try:
    import streamlit as st
except ImportError:  # Local scripts may not have Streamlit installed.
    st = None

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


REPO_ROOT = Path(__file__).resolve().parent

if load_dotenv:
    load_dotenv(REPO_ROOT / ".env")


def _load_local_toml_secrets() -> dict[str, str]:
    if tomllib is None:
        return {}

    secrets_paths = [
        REPO_ROOT / ".streamlit" / "secrets.toml",
        REPO_ROOT / ".streamlit" / "local_secrets.toml",
    ]
    for path in secrets_paths:
        if path.exists():
            with path.open("rb") as handle:
                data = tomllib.load(handle)
            return {str(key): value for key, value in data.items()}
    return {}


LOCAL_TOML_SECRETS = _load_local_toml_secrets()


def _get_secret(key: str, default: str = "") -> str:
    """Read a secret from Streamlit, then local files, then env vars."""
    if st is not None:
        try:
            return st.secrets[key]
        except Exception:
            pass

    if key in LOCAL_TOML_SECRETS:
        return LOCAL_TOML_SECRETS[key]

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
DEFAULT_POST_FOOTER = _get_secret(
    "DEFAULT_POST_FOOTER",
    "Help this information get to more voters. 🇺🇸 A well-informed electorate is a prerequisite to Democracy. - Thomas Jefferson",
)

# Instagram Pipeline (batch Google Sheets workflow)
APIFY_API_TOKEN = _get_secret("APIFY_API_TOKEN")
APIFY_REEL_ACTOR_ID = _get_secret("APIFY_REEL_ACTOR_ID", "xMc5Ga1oCONPmWJIa")
APIFY_POST_ACTOR_ID = _get_secret("APIFY_POST_ACTOR_ID", "apify/instagram-scraper")
GOOGLE_SHEET_ID = _get_secret("GOOGLE_SHEET_ID")
GOOGLE_WORKSHEET_NAME = _get_secret("GOOGLE_WORKSHEET_NAME")
GOOGLE_DRIVE_FOLDER_ID = _get_secret("GOOGLE_DRIVE_FOLDER_ID")
GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER = _get_secret("GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER", "screenshots")
GOOGLE_OAUTH_CLIENT_JSON = _get_secret("GOOGLE_OAUTH_CLIENT_JSON")
GOOGLE_OAUTH_TOKEN_JSON = _get_secret("GOOGLE_OAUTH_TOKEN_JSON")


def _get_google_credentials_json() -> str:
    """Accept credentials as raw JSON or as a base64-encoded string."""
    raw = _get_secret("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        return raw
    b64 = _get_secret("GOOGLE_CREDENTIALS_BASE64")
    if b64:
        import base64

        return base64.b64decode(b64).decode()
    return ""


GOOGLE_SERVICE_ACCOUNT_JSON = _get_google_credentials_json()
