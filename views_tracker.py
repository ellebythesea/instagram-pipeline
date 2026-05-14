#!/usr/bin/env python3
"""Fetch Instagram post metrics and write them to the views tracker Google Sheet.

Usage:
  python views_tracker.py          # fetch metrics for all rows with a Link but no Views
  python views_tracker.py --setup  # write the header row (run once on a blank sheet)

Secrets come from Google Secret Manager (via config.py):
  - instagram-access-token   → INSTAGRAM_ACCESS_TOKEN
  - views-tracker-sheet-id   → VIEWS_TRACKER_SHEET_ID
  - google-service-account   → GOOGLE_SERVICE_ACCOUNT_JSON
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date

import gspread
import requests
from google.oauth2.service_account import Credentials

from config import GOOGLE_SERVICE_ACCOUNT_JSON, INSTAGRAM_ACCESS_TOKEN, VIEWS_TRACKER_SHEET_ID

_GRAPH_BASE = "https://graph.facebook.com/v19.0"
_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# Column order for the sheet. Link is the only column you fill in manually.
HEADERS = [
    "Date Posted",
    "Link",
    "Username",
    "Content Type",
    "Caption",
    "Views",          # plays (Reels) or video_views (Video)
    "Reach",
    "Impressions",
    "Likes",
    "Comments",
    "Shares",
    "Saves",
    "Follows",
    "Total Interactions",
    "Engagement Rate",
    "Date Pulled",
]


# ---------------------------------------------------------------------------
# Sheets helpers
# ---------------------------------------------------------------------------

def _sheets_client() -> gspread.Client:
    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), scopes=_SCOPES
    )
    return gspread.authorize(creds)


def _check_secrets() -> None:
    if not INSTAGRAM_ACCESS_TOKEN:
        raise RuntimeError("INSTAGRAM_ACCESS_TOKEN is not configured — add 'instagram-access-token' to Secret Manager.")
    if not VIEWS_TRACKER_SHEET_ID:
        raise RuntimeError("VIEWS_TRACKER_SHEET_ID is not configured — add 'views-tracker-sheet-id' to Secret Manager.")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not configured.")


# ---------------------------------------------------------------------------
# Instagram Graph API helpers
# ---------------------------------------------------------------------------

def _ig_get(path: str, params: dict) -> dict:
    params = {**params, "access_token": INSTAGRAM_ACCESS_TOKEN}
    resp = requests.get(f"{_GRAPH_BASE}/{path.lstrip('/')}", params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def _ig_user_id() -> str:
    # /me returns the Facebook user ID; we need the linked Instagram Business Account ID.
    data = _ig_get("me/accounts", {"fields": "id,instagram_business_account"})
    for page in data.get("data", []):
        ig_id = (page.get("instagram_business_account") or {}).get("id")
        if ig_id:
            return ig_id
    raise RuntimeError(
        "No Instagram Business Account found. Make sure the access token is for a "
        "Facebook Page that has a linked Instagram Business or Creator account."
    )


def _normalize_url(url: str) -> str:
    return url.rstrip("/").split("?")[0].lower()


def _find_media_id(user_id: str, target_url: str) -> str | None:
    normalized = _normalize_url(target_url)
    params: dict = {"fields": "id,permalink", "limit": 100}
    path = f"{user_id}/media"
    while True:
        data = _ig_get(path, params)
        for item in data.get("data", []):
            if _normalize_url(item.get("permalink", "")) == normalized:
                return item["id"]
        after = data.get("paging", {}).get("cursors", {}).get("after")
        if not after:
            break
        params["after"] = after
        time.sleep(0.3)
    return None


def _insight_metrics(media_type: str, product_type: str) -> list[str]:
    if product_type == "REELS":
        return ["plays", "reach", "saved", "shares", "follows", "total_interactions", "impressions"]
    if media_type == "VIDEO":
        return ["impressions", "reach", "saved", "video_views", "shares", "total_interactions"]
    if media_type == "CAROUSEL_ALBUM":
        return ["impressions", "reach", "saved", "shares",
                "carousel_album_impressions", "carousel_album_reach",
                "carousel_album_saved", "carousel_album_video_views"]
    return ["impressions", "reach", "saved", "shares"]


def _parse_insight_value(item: dict) -> int:
    if "values" in item and item["values"]:
        return int(item["values"][0].get("value", 0) or 0)
    return int(item.get("value", 0) or 0)


def _fetch_insights(media_id: str, media_type: str, product_type: str) -> dict[str, int]:
    metrics = _insight_metrics(media_type, product_type)
    try:
        data = _ig_get(f"{media_id}/insights", {"metric": ",".join(metrics)})
        return {item["name"]: _parse_insight_value(item) for item in data.get("data", [])}
    except Exception as exc:
        print(f"    insights error: {exc}")
        return {}


def _fetch_post_metrics(user_id: str, link: str) -> dict | None:
    media_id = _find_media_id(user_id, link)
    if not media_id:
        print(f"    Could not find media ID for {link}")
        return None

    fields_data = _ig_get(media_id, {
        "fields": "timestamp,media_type,media_product_type,like_count,comments_count,username,caption",
    })
    media_type = fields_data.get("media_type", "")
    product_type = fields_data.get("media_product_type", "")
    insights = _fetch_insights(media_id, media_type, product_type)

    likes = int(fields_data.get("like_count") or insights.get("likes", 0))
    comments = int(fields_data.get("comments_count") or insights.get("comments", 0))
    shares = insights.get("shares", 0)
    saves = insights.get("saved", 0)
    reach = insights.get("reach", 0)
    impressions = (
        insights.get("impressions")
        or insights.get("carousel_album_impressions", 0)
    )
    views = insights.get("plays") or insights.get("video_views") or insights.get("carousel_album_video_views", 0)
    follows = insights.get("follows", 0)
    total_interactions = insights.get("total_interactions") or (likes + comments + shares + saves)
    engagement = round((likes + comments + shares + saves) / reach * 100, 2) if reach else 0

    ts = fields_data.get("timestamp", "")
    caption = (fields_data.get("caption") or "").strip()

    return {
        "Date Posted": ts[:10] if ts else "",
        "Username": fields_data.get("username", ""),
        "Content Type": product_type or media_type,
        "Caption": caption,
        "Views": views,
        "Reach": reach,
        "Impressions": impressions,
        "Likes": likes,
        "Comments": comments,
        "Shares": shares,
        "Saves": saves,
        "Follows": follows,
        "Total Interactions": total_interactions,
        "Engagement Rate": f"{engagement}%",
        "Date Pulled": date.today().isoformat(),
    }


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def setup_sheet() -> None:
    _check_secrets()
    print("Connecting to Google Sheets…")
    ws = _sheets_client().open_by_key(VIEWS_TRACKER_SHEET_ID).sheet1
    existing = ws.row_values(1)
    if existing:
        print(f"Sheet already has headers: {existing}")
        print("No changes made. Delete row 1 and re-run --setup if you want to reset.")
        return
    ws.append_row(HEADERS, value_input_option="RAW")
    # Freeze the header row
    ws.freeze(rows=1)
    print(f"Headers written: {HEADERS}")


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def run() -> None:
    _check_secrets()

    print("Connecting to Google Sheets…")
    ws = _sheets_client().open_by_key(VIEWS_TRACKER_SHEET_ID).sheet1

    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Sheet has no header row. Run: python views_tracker.py --setup")

    def col(name: str) -> int:
        return headers.index(name) + 1

    link_col = col("Link")
    views_col = col("Views")

    all_rows = ws.get_all_values()
    pending = [
        (sheet_row, row)
        for sheet_row, row in enumerate(all_rows[1:], start=2)
        if (row[link_col - 1] if len(row) >= link_col else "").strip()
        and not (row[views_col - 1] if len(row) >= views_col else "").strip()
    ]

    if not pending:
        print("No rows to update.")
        return

    print(f"Fetching metrics for {len(pending)} row(s)…")
    user_id = _ig_user_id()

    for sheet_row, row in pending:
        link = row[link_col - 1].strip()
        print(f"  Row {sheet_row}: {link}")
        try:
            metrics = _fetch_post_metrics(user_id, link)
        except Exception as exc:
            print(f"    Error: {exc}")
            continue
        if not metrics:
            continue
        for col_name, value in metrics.items():
            if col_name in headers:
                ws.update_cell(sheet_row, col(col_name), str(value) if value else "")
        time.sleep(0.5)

    print("Done.")


if __name__ == "__main__":
    if "--setup" in sys.argv:
        setup_sheet()
    else:
        run()
