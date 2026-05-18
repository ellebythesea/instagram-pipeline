# config.py
from __future__ import annotations

import base64
import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


try:
    import tomllib
except ImportError:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore
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

try:
    from google.cloud import secretmanager
except ImportError:  # pragma: no cover
    secretmanager = None

try:
    from google.oauth2 import service_account
except ImportError:  # pragma: no cover
    service_account = None


REPO_ROOT = Path(__file__).resolve().parent

if load_dotenv:
    load_dotenv(REPO_ROOT / ".env")


def _load_local_toml_secrets() -> dict[str, Any]:
    secrets_paths = [
        REPO_ROOT / ".streamlit" / "secrets.toml",
        REPO_ROOT / ".streamlit" / "local_secrets.toml",
    ]
    for path in secrets_paths:
        if path.exists():
            if tomllib is not None:
                with path.open("rb") as handle:
                    data = tomllib.load(handle)
                return {str(key): value for key, value in data.items()}
            text = path.read_text()
            return {
                match.group("key"): match.group("value")
                for match in re.finditer(
                    r'(?m)^\s*(?P<key>[A-Z0-9_]+)\s*=\s*"(?P<value>.*)"\s*$',
                    text,
                )
            }
    return {}


LOCAL_TOML_SECRETS = _load_local_toml_secrets()


def _runtime_secret(key: str, default: Any = "") -> Any:
    """Read from Streamlit, then local TOML, then environment variables."""
    if st is not None:
        try:
            return st.secrets[key]
        except Exception:
            pass

    if key in LOCAL_TOML_SECRETS:
        return LOCAL_TOML_SECRETS[key]

    return os.getenv(key, default)


def _decode_service_account_json(raw_json: str, b64_json: str) -> str:
    if raw_json:
        return raw_json
    if b64_json:
        return base64.b64decode(b64_json).decode()
    return ""


BOOTSTRAP_SERVICE_ACCOUNT_JSON = _decode_service_account_json(
    str(_runtime_secret("GOOGLE_SERVICE_ACCOUNT_JSON", "") or ""),
    str(_runtime_secret("GOOGLE_CREDENTIALS_BASE64", "") or ""),
)


SECRET_MANAGER_PROJECT_ID = (
    str(_runtime_secret("SECRET_MANAGER_PROJECT_ID", "") or "")
    or str(_runtime_secret("GOOGLE_CLOUD_PROJECT", "") or "")
    or os.getenv("GOOGLE_CLOUD_PROJECT", "")
)

if not SECRET_MANAGER_PROJECT_ID and BOOTSTRAP_SERVICE_ACCOUNT_JSON:
    try:
        SECRET_MANAGER_PROJECT_ID = json.loads(BOOTSTRAP_SERVICE_ACCOUNT_JSON).get("project_id", "")
    except Exception:
        SECRET_MANAGER_PROJECT_ID = ""


SECRET_MANAGER_SECRET_NAMES: dict[str, str | tuple[str, ...]] = {
    "OPENAI_API_KEY": str(_runtime_secret("SECRET_MANAGER_OPENAI_API_KEY_NAME", "openai-api-key") or "openai-api-key"),
    "ANTHROPIC_API_KEY": str(_runtime_secret("SECRET_MANAGER_ANTHROPIC_API_KEY_NAME", "anthropic-api-key") or "anthropic-api-key"),
    "SERPER_API_KEY": str(_runtime_secret("SECRET_MANAGER_SERPER_API_KEY_NAME", "serper-id") or "serper-id"),
    "APP_PASSWORD": str(_runtime_secret("SECRET_MANAGER_APP_PASSWORD_NAME", "password") or "password"),
    "APIFY_API_TOKEN": str(_runtime_secret("SECRET_MANAGER_APIFY_API_TOKEN_NAME", "apify-api") or "apify-api"),
    "GOOGLE_SHEET_ID": str(_runtime_secret("SECRET_MANAGER_GOOGLE_SHEET_ID_NAME", "google-sheet-id") or "google-sheet-id"),
    "GOOGLE_WORKSHEET_NAME": str(_runtime_secret("SECRET_MANAGER_GOOGLE_WORKSHEET_NAME", "google-worksheet-name") or "google-worksheet-name"),
    "GOOGLE_DRIVE_FOLDER_ID": str(_runtime_secret("SECRET_MANAGER_GOOGLE_DRIVE_FOLDER_ID_NAME", "google-folder-id") or "google-folder-id"),
    "GOOGLE_OAUTH_CLIENT_JSON": str(_runtime_secret("SECRET_MANAGER_GOOGLE_OAUTH_CLIENT_JSON_NAME", "google-oauth-id") or "google-oauth-id"),
    "GOOGLE_OAUTH_TOKEN_JSON": str(_runtime_secret("SECRET_MANAGER_GOOGLE_OAUTH_TOKEN_JSON_NAME", "google-oauth-token") or "google-oauth-token"),
    "GOOGLE_SERVICE_ACCOUNT_JSON": (
        str(_runtime_secret("SECRET_MANAGER_GOOGLE_SERVICE_ACCOUNT_JSON_NAME", "google-service-account-json") or "google-service-account-json"),
        "google-service-account",
    ),
    "GOOGLE_CREDENTIALS_BASE64": str(_runtime_secret("SECRET_MANAGER_GOOGLE_CREDENTIALS_BASE64_NAME", "google-service-account") or "google-service-account"),
    "GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER": str(_runtime_secret("SECRET_MANAGER_GOOGLE_DRIVE_SCREENSHOTS_SUBFOLDER_NAME", "google-screenshots-subfolder") or "google-screenshots-subfolder"),
    "APIFY_REEL_ACTOR_ID": str(_runtime_secret("SECRET_MANAGER_APIFY_REEL_ACTOR_ID_NAME", "apify-reel-actor-id") or "apify-reel-actor-id"),
    "APIFY_POST_ACTOR_ID": str(_runtime_secret("SECRET_MANAGER_APIFY_POST_ACTOR_ID_NAME", "apify-post-actor-id") or "apify-post-actor-id"),
}


def _truthy_env_flag(key: str) -> bool:
    return str(_runtime_secret(key, "") or "").strip().lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def _secret_manager_client():
    if secretmanager is None:
        return None

    if BOOTSTRAP_SERVICE_ACCOUNT_JSON and service_account is not None:
        try:
            info = json.loads(BOOTSTRAP_SERVICE_ACCOUNT_JSON)
            credentials = service_account.Credentials.from_service_account_info(info)
            return secretmanager.SecretManagerServiceClient(credentials=credentials)
        except Exception:
            pass

    try:
        return secretmanager.SecretManagerServiceClient()
    except Exception:
        return None


@lru_cache(maxsize=None)
def _secret_manager_value(secret_name: str) -> str:
    if not secret_name or not SECRET_MANAGER_PROJECT_ID:
        return ""

    client = _secret_manager_client()
    if client is None:
        return ""

    try:
        resource = f"projects/{SECRET_MANAGER_PROJECT_ID}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": resource})
        return response.payload.data.decode("utf-8")
    except Exception:
        return ""


def _get_secret(key: str, default: str = "") -> str:
    """Read a secret from Secret Manager first, then runtime secrets."""
    if key in {"GOOGLE_OAUTH_CLIENT_JSON", "GOOGLE_OAUTH_TOKEN_JSON"}:
        runtime_value = _runtime_secret(key, default)
        if runtime_value not in (None, ""):
            return str(runtime_value)

    secret_names = SECRET_MANAGER_SECRET_NAMES.get(key, "")
    if isinstance(secret_names, str):
        secret_names = (secret_names,) if secret_names else ()
    for secret_name in secret_names:
        value = _secret_manager_value(secret_name)
        if value:
            return value

    value = _runtime_secret(key, default)
    if value is None:
        return default
    return str(value)


OPENAI_API_KEY = _get_secret("OPENAI_API_KEY")
ANTHROPIC_API_KEY = _get_secret("ANTHROPIC_API_KEY")
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
    """Accept credentials as raw JSON or a base64-encoded JSON blob."""
    if BOOTSTRAP_SERVICE_ACCOUNT_JSON:
        return BOOTSTRAP_SERVICE_ACCOUNT_JSON
    raw = _get_secret("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        return raw
    b64 = _get_secret("GOOGLE_CREDENTIALS_BASE64")
    if b64:
        return base64.b64decode(b64).decode()
    return ""


GOOGLE_SERVICE_ACCOUNT_JSON = _get_google_credentials_json()


google_service_account_client_email = ""
google_service_account_json_loaded = "no"
google_service_account_json_valid = "no"
if GOOGLE_SERVICE_ACCOUNT_JSON:
    google_service_account_json_loaded = "yes"
    try:
        google_service_account_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        google_service_account_client_email = str(google_service_account_info.get("client_email", "") or "")
        google_service_account_json_valid = "yes"
    except Exception:
        google_service_account_client_email = "(parse failed)"

openai_secret_name = SECRET_MANAGER_SECRET_NAMES.get("OPENAI_API_KEY", "")
if isinstance(openai_secret_name, tuple):
    openai_secret_name = ",".join(openai_secret_name)
openai_secret_raw = ""
if isinstance(SECRET_MANAGER_SECRET_NAMES.get("OPENAI_API_KEY", ""), str):
    openai_secret_raw = _secret_manager_value(str(SECRET_MANAGER_SECRET_NAMES["OPENAI_API_KEY"]))
print(
    "[config] "
    f"secret_manager_project_id={SECRET_MANAGER_PROJECT_ID or '(missing)'} "
    f"google_service_account_json_loaded={google_service_account_json_loaded} "
    f"google_service_account_json_valid={google_service_account_json_valid} "
    f"google_service_account_client_email={google_service_account_client_email or '(missing)'} "
    f"openai_secret_name={openai_secret_name or '(missing)'} "
    f"openai_secret_found={'yes' if openai_secret_raw else 'no'} "
    f"openai_api_key_loaded={'yes' if bool(OPENAI_API_KEY) else 'no'}"
)
