"""Small helpers for turning raw provider exceptions into useful labels."""

from __future__ import annotations


def describe_error(error: Exception) -> str:
    message = str(error or "").strip()
    lowered = message.lower()

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
        return f"Google OAuth refresh failed. Check GOOGLE_OAUTH_TOKEN_JSON. Raw error: {message}"

    if "service accounts do not have storage quota" in lowered:
        return "Google Drive upload failed because the app is using a service account without Drive quota. Use the OAuth token flow or a shared drive."

    if "quota exceeded" in lowered and "sheets.googleapis.com" in lowered:
        return f"Google Sheets quota exceeded. Wait and retry. Raw error: {message}"

    return message or error.__class__.__name__
