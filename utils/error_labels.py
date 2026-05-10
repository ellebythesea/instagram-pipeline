"""Small helpers for turning raw provider exceptions into useful labels."""

from __future__ import annotations

import json


def _exception_name(error: Exception) -> str:
    cls = error.__class__
    return f"{cls.__module__}.{cls.__name__}".lower()


def _status_code(error: Exception) -> int | None:
    status = getattr(error, "status_code", None) or getattr(error, "code", None)
    response = getattr(error, "response", None)
    if status is None and response is not None:
        status = getattr(response, "status_code", None)
    try:
        return int(status) if status is not None else None
    except Exception:
        return None


def _response_text(error: Exception) -> str:
    response = getattr(error, "response", None)
    if response is None:
        return ""
    text = getattr(response, "text", "") or ""
    if text:
        return str(text)
    try:
        return json.dumps(response.json())
    except Exception:
        return ""


def _message(error: Exception) -> str:
    parts = [str(error or "").strip(), _response_text(error).strip()]
    return " ".join(part for part in parts if part).strip()


def describe_error(error: Exception) -> str:
    message = _message(error)
    lowered = message.lower()
    exc_name = _exception_name(error)
    status = _status_code(error)

    if "OPENAI_API_KEY is not configured" in message:
        return "OpenAI auth failed. OPENAI_API_KEY is missing from Streamlit secrets."
    if "APIFY_API_TOKEN is not configured" in message:
        return "Apify auth failed. APIFY_API_TOKEN is missing from Streamlit secrets."
    if "GOOGLE_SERVICE_ACCOUNT_JSON is not configured" in message:
        return "Google auth failed. GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_CREDENTIALS_BASE64 is missing from Streamlit secrets."
    if "GOOGLE_OAUTH_TOKEN_JSON is not configured" in message:
        return "Google Drive auth failed. GOOGLE_OAUTH_TOKEN_JSON is missing for personal My Drive uploads."
    if "GOOGLE_OAUTH_TOKEN_JSON is malformed" in message or "GOOGLE_OAUTH_TOKEN_JSON is not valid JSON" in message:
        return "Google Drive auth failed. GOOGLE_OAUTH_TOKEN_JSON is malformed."
    if "Google OAuth refresh failed" in message:
        return "Google Drive auth failed. Refresh GOOGLE_OAUTH_TOKEN_JSON."
    if "no audio stream to transcribe" in lowered:
        return "Local media file has no audio track, so there is nothing to transcribe."
    if "tuple index out of range" in lowered:
        return (
            "Local transcription backend failed with an internal tuple index error. "
            "The script will retry from extracted audio when possible; rerun with --debug if it still fails."
        )
    if "ffmpeg could not extract audio" in lowered:
        return f"Local media decode failed. ffmpeg could not extract audio from the video. Raw error: {message}"
    if "could not decode the video directly or from extracted audio" in lowered:
        return f"Local media decode failed. The video may be unsupported, corrupt, or partially synced. Raw error: {message}"
    if status == 403 and "for url:" in lowered:
        if any(token in lowered for token in ["please enable js", "ad blocker", "captcha", "paywall", "forbidden"]):
            return "Article access blocked or paywalled (403). Open the link manually or use another source."
        return "Article access forbidden (403). Open the link manually or use another source."
    if any(token in lowered for token in ["article request timed out", "read timed out", "connect timeout", "connection timed out"]):
        return "Article request timed out. Open the link manually or use another source."
    if any(token in lowered for token in ["please enable js", "disable any ad blocker", "captcha-delivery.com"]):
        return "Article access blocked by a bot check. Open the link manually or use another source."

    if "openai" in exc_name:
        if status == 401 or any(token in lowered for token in ["incorrect api key", "invalid_api_key", "unauthorized"]):
            return "OpenAI auth failed. Check OPENAI_API_KEY in Streamlit secrets."
        if status == 429 or any(token in lowered for token in ["rate limit", "quota", "insufficient_quota"]):
            return "OpenAI quota/rate limit hit. Check project billing/limits or retry later."
        if status and status >= 500:
            return "OpenAI service error. Retry in a minute."
        return f"OpenAI API error. Check OPENAI_API_KEY and request limits. Raw error: {message}"

    if "apify" in exc_name or "apify" in lowered:
        if status in {401, 403} or any(token in lowered for token in ["unauthorized", "authentication failed", "token is not valid", "invalid token"]):
            return "Apify auth failed. Check APIFY_API_TOKEN in Streamlit secrets."
        if status == 429 or "rate limit" in lowered or "quota" in lowered:
            return "Apify quota/rate limit hit. Check Apify account usage or retry later."
        if any(token in lowered for token in ["590 upstream502", "upstream502", "proxy responded with 590"]):
            return "Transient Apify/Instagram proxy error. Retry in a minute."
        if "rotating session" in lowered or "session error" in lowered:
            return "Transient Apify session error. Retry in a minute."
        return f"Apify API error. Check APIFY_API_TOKEN and actor settings. Raw error: {message}"

    if "google" in exc_name or "gspread" in exc_name or "googleapi" in exc_name:
        if "worksheetnotfound" in exc_name:
            return "Google Sheets worksheet not found. Check GOOGLE_WORKSHEET_NAME."
        if "malformederror" in exc_name or "service account info was not in the expected format" in lowered:
            return "Google service account credentials are malformed. Check GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_CREDENTIALS_BASE64."
        if any(token in lowered for token in ["invalid_grant", "reauth", "refresh token", "token has been expired", "authorized user"]):
            return (
                "Google auth failed. Check GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_CREDENTIALS_BASE64, "
                "and make sure the Sheet/Drive folder is shared with the service account."
            )
        if status in {401, 403} or any(token in lowered for token in ["permission denied", "forbidden", "unauthorized", "insufficient authentication scopes"]):
            return (
                "Google auth/permission failed. Check GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_CREDENTIALS_BASE64, "
                "and make sure the Sheet/Drive folder is shared with the service account."
            )
        if status == 404 or "not found" in lowered:
            return "Google Sheet or Drive file not found. Check GOOGLE_SHEET_ID, GOOGLE_DRIVE_FOLDER_ID, and worksheet name."
        if status == 429 or "quota exceeded" in lowered or "read requests per minute" in lowered:
            return "Google Sheets quota/rate limit hit. Wait a minute and retry."
        if "service accounts do not have storage quota" in lowered:
            return "Google Drive upload failed because the service account has no personal Drive quota. Use a shared drive or a shared folder it can access."
        return f"Google API error. Check Google secrets, sharing permissions, and quotas. Raw error: {message}"

    if any(token in lowered for token in ["590 upstream502", "upstream502", "proxy responded with 590"]):
        return f"Transient Apify/Instagram proxy error. Retry in a minute. Raw error: {message}"

    if "rotating session" in lowered or "session error" in lowered:
        return f"Transient Apify session error. Retry in a minute. Raw error: {message}"

    if any(token in lowered for token in ["unauthorized", "authentication failed", "token is not valid", "invalid token"]):
        if "apify" in lowered:
            return f"Apify auth failed. Check APIFY_API_TOKEN. Raw error: {message}"

    if any(token in lowered for token in ["incorrect api key", "invalid_api_key", "401", "unauthorized"]) and "openai" in lowered:
        return f"OpenAI auth failed. Check OPENAI_API_KEY. Raw error: {message}"

    if any(token in lowered for token in ["invalid_grant", "token has been expired", "reauth", "refresh", "authorized user"]) and "google" in lowered:
        return (
            "Google auth failed. Check GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_CREDENTIALS_BASE64, "
            f"and sharing permissions for the service account. Raw error: {message}"
        )

    if "service accounts do not have storage quota" in lowered:
        return "Google Drive upload failed because the service account has no personal Drive quota. Use a shared drive or a shared folder it can access."

    if "quota exceeded" in lowered and "sheets.googleapis.com" in lowered:
        return f"Google Sheets quota exceeded. Wait and retry. Raw error: {message}"

    return message or error.__class__.__name__
