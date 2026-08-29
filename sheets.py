"""Google Sheets helper — read rows and write pipeline results.

Column order is read off the sheet's own header row, so a column can be moved
without a code change. _EXPECTED_HEADERS is the order a fresh sheet is built in
and the fallback for a header the sheet does not carry
so duplicate or misnamed sheet headers cannot corrupt field mapping.

Sheet layout:
  A  Instagram URL      B  Required Hashtags  C  Source Username
  D  Generated Caption  E  Media Type         F  Photo Count
  G  Media Drive Link   H  Thumbnail Drive Link
  I  Original Caption   J  Transcript         K  Top Comment
  L  Speaker Name       M  Footer             N  Status
  O  Caption Context    P  Scheduled Time     Q  #name
  R  #text1             S  #text2             T  #text3
  U  Slide CTA          V  text4             W  text5
  X  text6
  Y  quote
  Z  text7             AA  text8
"""

import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from json import JSONDecodeError
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials

from config import GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_WORKSHEET_NAME

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

_log = logging.getLogger(__name__)

# Transient Sheets API failures: quota pushback (429) and Google-side outages
# (500/502/503/504) plus request timeouts. These are safe to retry; anything
# else (401/403/404, WorksheetNotFound, bad input) is a real error and is raised
# on the first attempt.
_RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
_RETRYABLE_MESSAGE_FRAGMENTS = (
    "exceeded in a metric read request",
    "the service is currently unavailable",
    "internal error encountered",
    "backend error",
    "try again later",
)
_RETRY_ATTEMPTS = 6
_RETRY_BASE_DELAY_SECONDS = 1.0
_RETRY_MAX_DELAY_SECONDS = 32.0
# (connect, read) timeout so a stalled request fails fast enough to be retried
# instead of hanging the run.
_REQUEST_TIMEOUT_SECONDS = (10.0, 60.0)
_STATUS_IN_MESSAGE_RE = re.compile(r"\[(\d{3})\]")

_EXPECTED_HEADERS = [
    "Instagram URL",
    "Required Hashtags",
    "Source Username",
    "Generated Caption",
    "Media Type",
    "Photo Count",
    "Media Drive Link",
    "Thumbnail Drive Link",
    "Original Caption",
    "Transcript",
    "Top Comment",
    "Speaker Name",
    "Footer",
    "Status",
    "Caption Context",
    "Scheduled Time",
    "name",
    "text1",
    "text2",
    "text3",
    "Slide CTA",
    "text4",
    "text5",
    "text6",
    "quote",
    "text7",
    "text8",
    "Reel Drive Link",
]

_headers_checked: set[tuple[str, str]] = set()
_client: gspread.Client | None = None
_workbooks: dict[str, gspread.Spreadsheet] = {}
_worksheets: dict[tuple[str, str], gspread.Worksheet] = {}
_rows_cache: dict[tuple[str, str], tuple[float, list[dict]]] = {}
_ROWS_CACHE_TTL_SECONDS = 120.0
_METADATA_SHEET_TITLE = "__workspace_meta__"
_LAST_SCHEDULED_TIMES_KEY = "last_scheduled_times"
_SLIDE_CTA_OPTIONS_KEY = "slide_cta_options"
_ORIGINAL_THUMBNAILS_KEY = "original_thumbnails"
_FUNDRAISING_SHEET_TITLE = "fundraising"
_HASHTAGS_SHEET_TITLE = "hashtags"
_DOCS_SHEET_TITLE = "docs"
_SUBSTACK_SHEET_TITLE = "substack"
_ARCHIVE_SHEET_TITLE = "Safe to Delete"
_archive_ready: set[str] = set()
_SUBSTACK_HEADERS = [
    "url",
    "name",
    "article",
    "topic breakdown",
    "status",
    "instagram url",
    "monitoring status",
    "last comment retrieved",
    "summary",
]
_SUBSTACK_LEGACY_HEADERS_WITH_NAME = [
    "url",
    "name",
    "article",
    "status",
    "instagram url",
    "monitoring status",
    "last comment retrieved",
    "summary",
]
_SUBSTACK_LEGACY_HEADERS_NO_NAME = [
    "url",
    "article",
    "status",
    "instagram url",
    "monitoring status",
    "last comment retrieved",
    "summary",
]


def _get_client() -> gspread.Client:
    global _client
    if _client is not None:
        return _client

    creds_src = GOOGLE_SERVICE_ACCOUNT_JSON
    if not creds_src:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not configured. Add it to "
            ".streamlit/local_secrets.toml, set it as an environment variable, "
            "or set it to a service-account JSON file path."
        )
    if os.path.isfile(creds_src):
        creds = Credentials.from_service_account_file(creds_src, scopes=_SCOPES)
    else:
        try:
            creds_info = json.loads(creds_src)
        except JSONDecodeError as exc:
            raise RuntimeError(
                "GOOGLE_SERVICE_ACCOUNT_JSON must be either a valid service-account "
                "JSON object or a path to a service-account JSON file."
            ) from exc
        creds = Credentials.from_service_account_info(creds_info, scopes=_SCOPES)
    _client = gspread.authorize(creds)
    # Older gspread releases have no timeout knob; without it a stalled request
    # can hang indefinitely and never reach the retry path.
    if hasattr(_client, "set_timeout"):
        _client.set_timeout(_REQUEST_TIMEOUT_SECONDS)
    return _client


def _error_status_code(exc: Exception) -> int | None:
    """Best-effort HTTP status for an exception raised by gspread/requests."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status
    # gspread's APIError exposes the API's own code, which is -1 when the error
    # body was not JSON (Google returns HTML for some 5xx responses).
    code = getattr(exc, "code", None)
    if isinstance(code, int) and code > 0:
        return code
    match = _STATUS_IN_MESSAGE_RE.search(str(exc))
    return int(match.group(1)) if match else None


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (gspread.WorksheetNotFound, gspread.SpreadsheetNotFound)):
        return False
    if isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True
    status = _error_status_code(exc)
    if status is not None:
        return status in _RETRYABLE_STATUS_CODES
    message = str(exc).lower()
    return any(fragment in message for fragment in _RETRYABLE_MESSAGE_FRAGMENTS)


def _with_backoff(fn, *args, **kwargs):
    """Call `fn`, retrying transient Sheets API failures with jittered backoff."""
    delay = _RETRY_BASE_DELAY_SECONDS
    last_attempt = _RETRY_ATTEMPTS - 1
    for attempt in range(_RETRY_ATTEMPTS):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == last_attempt or not _is_retryable(e):
                raise
            sleep_for = delay + random.uniform(0, 0.5)
            _log.warning(
                "Sheets API call %s failed (attempt %d/%d): %s — retrying in %.1fs",
                getattr(fn, "__name__", repr(fn)),
                attempt + 1,
                _RETRY_ATTEMPTS,
                e,
                sleep_for,
            )
            time.sleep(sleep_for)
            delay = min(delay * 2, _RETRY_MAX_DELAY_SECONDS)


def _workbook(sheet_id: str):
    cleaned_sheet_id = (sheet_id or "").strip()
    if not cleaned_sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID is not configured.")
    if cleaned_sheet_id not in _workbooks:
        _workbooks[cleaned_sheet_id] = _with_backoff(_get_client().open_by_key, cleaned_sheet_id)
    return _workbooks[cleaned_sheet_id]


def _named_worksheet(sheet_id: str, title: str) -> gspread.Worksheet:
    cleaned_sheet_id = (sheet_id or "").strip()
    cleaned_title = (title or "").strip()
    cache_key = (cleaned_sheet_id, cleaned_title)
    if cache_key not in _worksheets:
        _worksheets[cache_key] = _with_backoff(
            _workbook(cleaned_sheet_id).worksheet, cleaned_title
        )
    return _worksheets[cache_key]


def _worksheet(sheet_id: str) -> gspread.Worksheet:
    cache_key = (sheet_id, "__main__")
    if cache_key in _worksheets:
        return _worksheets[cache_key]

    workbook = _workbook(sheet_id)
    configured_title = (GOOGLE_WORKSHEET_NAME or "").strip()
    expected_headers = {"Instagram URL", "Status"}

    if configured_title:
        try:
            ws = _named_worksheet(sheet_id, configured_title)
            headers = {h.strip() for h in _with_backoff(ws.row_values, 1) if h.strip()}
            if expected_headers.issubset(headers):
                _ensure_headers(sheet_id, ws)
                _worksheets[cache_key] = ws
                return ws
        except gspread.WorksheetNotFound:
            pass

    for ws in _with_backoff(workbook.worksheets):
        headers = {h.strip() for h in _with_backoff(ws.row_values, 1) if h.strip()}
        if expected_headers.issubset(headers):
            _ensure_headers(sheet_id, ws)
            _worksheets[cache_key] = ws
            return ws

    if configured_title:
        raise RuntimeError(
            f"Worksheet '{configured_title}' was not found or does not contain the expected pipeline headers."
        )

    ws = _with_backoff(workbook.get_worksheet, 0)
    _ensure_headers(sheet_id, ws)
    _worksheets[cache_key] = ws
    return ws


def _metadata_worksheet(sheet_id: str) -> gspread.Worksheet:
    workbook = _workbook(sheet_id)
    try:
        ws = _named_worksheet(sheet_id, _METADATA_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = _with_backoff(
            workbook.add_worksheet, title=_METADATA_SHEET_TITLE, rows=10, cols=2
        )
        _worksheets[(sheet_id, _METADATA_SHEET_TITLE)] = ws
        _with_backoff(ws.update, "A1:B1", [["key", "value"]])
    return ws


def _optional_worksheet(sheet_id: str, title: str) -> gspread.Worksheet | None:
    try:
        return _named_worksheet(sheet_id, title)
    except gspread.WorksheetNotFound:
        return None


# Columns added after the original A-Y layout. Reads pad missing columns, so an
# older sheet still loads, but a write to Z/AA/AB fails on a grid that never
# grew past the default 26 columns - hence the widen below.
_LATE_ADDED_HEADERS = ("text7", "text8", "Reel Drive Link")


def _ensure_headers(sheet_id: str, ws: gspread.Worksheet) -> None:
    """Make room for the columns added after the original layout, and label them.

    Reads are positional and writes use direct column letters, so nothing here is
    needed to map fields. What is needed is that the grid is wide enough to write
    the later slide columns at all, and that they carry their header name so the
    sheet still reads as documentation. Only blank header cells are filled, so a
    sheet already using those columns for something else is left alone.
    """
    cache_key = (sheet_id, f"posts-columns:{ws.title}")
    if cache_key in _headers_checked:
        return
    _headers_checked.add(cache_key)
    try:
        needed = len(_EXPECTED_HEADERS)
        if ws.col_count < needed:
            _with_backoff(ws.add_cols, needed - ws.col_count)
        first_late_index = len(_EXPECTED_HEADERS) - len(_LATE_ADDED_HEADERS)
        header_row = _with_backoff(ws.row_values, 1)
        updates = []
        for offset, name in enumerate(_LATE_ADDED_HEADERS):
            index = first_late_index + offset
            current = header_row[index].strip() if index < len(header_row) else ""
            if not current:
                updates.append({"range": f"{_column_letter(index + 1)}1", "values": [[name]]})
        if updates:
            _with_backoff(ws.batch_update, updates)
    except Exception as error:
        # Never block a read or a write on housekeeping - the worst case is that
        # the two columns stay unlabeled.
        _log.warning("could not prepare the later slide columns: %s", error)



# ---------------------------------------------------------------------------
# Where each posts column actually is
#
# _EXPECTED_HEADERS gives the order a fresh sheet is built in. It is no longer
# what the code writes against: the sheet's own header row is, so a column can
# be dragged somewhere else without a matching deploy, and nothing is left
# pointing at the cell it used to occupy.
# ---------------------------------------------------------------------------

_posts_columns_cache: dict[str, tuple[float, dict[str, int]]] = {}
# Deliberately shorter than the rows cache. This is the window in which a column
# dragged somewhere else is still written to where it used to be, so it is kept
# small; reads reseed the map for free, so it rarely costs a call of its own.
_POSTS_COLUMNS_TTL_SECONDS = 30.0


# The header row is hand-maintained and was decorative until column positions
# started being read from it, so a cell may well be shortened or reworded. These
# are the forms accepted for a header besides its own name. Each belongs to
# exactly one header — an ambiguous short form ("caption", "link") is left out
# rather than resolved by guesswork.
_POSTS_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "Instagram URL": ("url", "instagram link", "post url", "link url"),
    "Required Hashtags": ("hashtags", "required tags"),
    "Source Username": ("username", "source user", "handle"),
    "Generated Caption": ("generated", "final caption"),
    "Media Type": ("media kind",),
    "Photo Count": ("photos", "photo no", "photo number"),
    "Media Drive Link": ("media", "media link", "media drive", "video", "video link"),
    "Thumbnail Drive Link": ("thumbnail", "thumbnail link", "thumbnail drive", "thumb", "screenshot"),
    "Original Caption": ("original",),
    "Top Comment": ("top comment cta", "comment cta"),
    "Speaker Name": ("speaker",),
    "Caption Context": ("context",),
    "Scheduled Time": ("scheduled", "schedule", "post time"),
    "Slide CTA": ("cta", "slide call to action"),
    "Reel Drive Link": ("reel", "reel link", "reel drive", "reel video"),
}


def _normalized_header(name: str) -> str:
    return " ".join((name or "").split()).lower()


def _posts_column_map(header_row: list[str]) -> dict[str, int]:
    """1-based column index for every expected header, read off the sheet.

    Only the late-added columns may fall back to a canonical position: those are
    the ones an older sheet genuinely lacks. Any other header missing from the
    row is refused rather than guessed. Guessing is what makes this dangerous —
    on a sheet whose columns have been reordered, a canonical position is some
    other field's cell, so a "fallback" writes one column's value over another's.
    The first of any duplicate header wins.
    """
    found: dict[str, int] = {}
    for index, name in enumerate(header_row or []):
        key = _normalized_header(name)
        if key and key not in found:
            found[key] = index + 1

    # Exact names first, across the whole row, so a header's own name always wins
    # over another header's alias for the same cell.
    taken: set[int] = set()
    columns: dict[str, int] = {}
    for header in _EXPECTED_HEADERS:
        index = found.get(_normalized_header(header))
        if index:
            columns[header] = index
            taken.add(index)

    missing: list[str] = []
    for position, header in enumerate(_EXPECTED_HEADERS):
        if header in columns:
            continue
        index = next(
            (
                found[alias]
                for alias in _POSTS_HEADER_ALIASES.get(header, ())
                if found.get(alias) and found[alias] not in taken
            ),
            0,
        )
        if index:
            columns[header] = index
            taken.add(index)
        elif header in _LATE_ADDED_HEADERS:
            columns[header] = position + 1
        else:
            missing.append(header)
    if missing:
        raise RuntimeError(
            "The posts tab header row does not name: "
            + ", ".join(missing)
            + ". Column positions are read from that row, so nothing is read or "
            "written until it is intact. The row currently reads: "
            + ", ".join(name.strip() or "(blank)" for name in (header_row or []))
        )
    return columns


def _cache_posts_columns(sheet_id: str, columns: dict[str, int]) -> None:
    _posts_columns_cache[sheet_id] = (time.monotonic(), columns)


def _posts_columns(sheet_id: str, ws: gspread.Worksheet) -> dict[str, int]:
    """The posts column map, on the same clock as the rows cache.

    get_all_rows already holds the header row and seeds this, so a write
    normally reuses what the read before it saw rather than spending a call of
    its own. Moving a column therefore takes effect on the next real read.
    """
    cached = _posts_columns_cache.get(sheet_id)
    if cached and time.monotonic() - cached[0] <= _POSTS_COLUMNS_TTL_SECONDS:
        return cached[1]
    columns = _posts_column_map(_with_backoff(ws.row_values, 1))
    _cache_posts_columns(sheet_id, columns)
    return columns


def _posts_cell(columns: dict[str, int], header: str, row_number: int) -> str:
    return f"{_column_letter(columns[header])}{row_number}"


def _posts_updates(
    columns: dict[str, int],
    row_number: int,
    fields: dict[str, str],
) -> list[dict]:
    """One single-cell range per field, for a batch_update.

    A cell at a time rather than a span: a span is only correct while the fields
    inside it stay adjacent, and the whole point of reading the header row is
    that they might not be. It is still one call however many fields there are.
    """
    return [
        {"range": _posts_cell(columns, header, row_number), "values": [[value]]}
        for header, value in fields.items()
    ]


def _posts_row_values(columns: dict[str, int], fields: dict[str, str]) -> list[str]:
    """A row laid out for append, each value under its own header."""
    row = [""] * max(columns.values())
    for header, value in fields.items():
        row[columns[header] - 1] = value
    return row



def _invalidate_rows_cache(sheet_id: str) -> None:
    stale_keys = [key for key in _rows_cache if key[0] == sheet_id]
    for key in stale_keys:
        _rows_cache.pop(key, None)


def _get_cached_rows(sheet_id: str, tab_name: str) -> list[dict] | None:
    cached = _rows_cache.get((sheet_id, tab_name))
    if not cached:
        return None
    cached_at, rows = cached
    if time.monotonic() - cached_at > _ROWS_CACHE_TTL_SECONDS:
        _rows_cache.pop((sheet_id, tab_name), None)
        return None
    return [row.copy() for row in rows]


def _set_cached_rows(sheet_id: str, tab_name: str, rows: list[dict]) -> None:
    _rows_cache[(sheet_id, tab_name)] = (time.monotonic(), [row.copy() for row in rows])


def get_all_rows(sheet_id: str) -> list[dict]:
    """Return all data rows as dicts keyed by header name, plus row_number."""
    cached = _get_cached_rows(sheet_id, "posts")
    if cached is not None:
        return cached
    ws = _worksheet(sheet_id)
    all_values = _with_backoff(ws.get_all_values)
    # The header row is already in hand here, so this is where the column map is
    # settled for everything that follows, reads and writes alike.
    if not all_values:
        _set_cached_rows(sheet_id, "posts", [])
        return []
    columns = _posts_column_map(all_values[0])
    _cache_posts_columns(sheet_id, columns)
    records = []
    for i, row in enumerate(all_values[1:]):  # skip header row
        record = {
            header: (row[index - 1] if index - 1 < len(row) else "")
            for header, index in columns.items()
        }
        record["row_number"] = i + 2
        records.append(record)
    _set_cached_rows(sheet_id, "posts", records)
    return [row.copy() for row in records]


# An article row whose page could not be read keeps this status prefix followed by
# the reason, e.g. "needs source: Article access blocked or paywalled (403)". The
# row still exists so the text can be pasted in by hand from the app.
# pages/workspace.py spells this same value out rather than importing it — see the
# note there — so keep the two in step.
NEEDS_SOURCE_PREFIX = "needs source"


# Typing this into the Status cell of a pending row forces it to be processed as a
# reel, for a video that lives at a /p/ link rather than a /reel/ one. It is consumed
# on processing: the row comes out with the normal ingested/done status.
REEL_STATUS_MARKER = "reel"
# The cell is typed by hand, so the obvious ways of writing it all count. "reels" is
# at least as natural to type as the singular, and "reel lines" names what the marker
# actually produces. Case, surrounding space, hyphens and underscores are ignored, so
# "Reels", "Reel-Lines" and " reel " are all the marker too.
# pipeline_caption.is_reel_status mirrors this for modules that cannot import sheets —
# keep the two in step.
REEL_STATUS_MARKERS = frozenset({"reel", "reels", "reel line", "reel lines", "reels lines"})
# A row flagged as a reel is finished as a Reel Lines post — ten headlines in text1
# instead of carousel slide copy. Column U carries that for the life of the row, since
# the Status marker itself is consumed as soon as the row is processed.
REEL_LINES_SLIDE_CTA = "reel lines"


def is_reel_status(value: str) -> bool:
    """Whether a Status cell is the hand-typed reel marker rather than a real status."""
    normalized = " ".join((value or "").replace("-", " ").replace("_", " ").lower().split())
    return normalized in REEL_STATUS_MARKERS


def get_pending_rows(sheet_id: str) -> list[dict]:
    """Rows where Status is empty (or the 'reel' marker) and URL is present."""
    return [
        r for r in get_all_rows(sheet_id)
        if (not r.get("Status", "").strip() or is_reel_status(r.get("Status", "")))
        and r.get("Instagram URL", "").strip()
    ]


def get_ingested_rows(sheet_id: str) -> list[dict]:
    """Rows where Status is 'ingested'."""
    return [
        r for r in get_all_rows(sheet_id)
        if r.get("Status", "").strip().lower() == "ingested"
    ]


def append_link_rows(
    sheet_id: str,
    urls: list[str],
    required_hashtags: str = "",
    top_comment: str = "",
    reel_urls: set[str] | None = None,
) -> None:
    """Append new rows with Instagram URL, optional required hashtags and top comment.

    `top_comment` goes into col K as written. A bare URL there is expanded into the
    standard "Comment LINK" CTA when the caption is generated, so the docs tab can
    carry just a link.

    Any URL also listed in `reel_urls` gets the 'reel' Status marker, so it is processed
    as a reel even when its link is not a /reel/ one.
    """
    cleaned_urls = [url.strip() for url in urls if url.strip()]
    if not cleaned_urls:
        return

    reels = {url.strip() for url in (reel_urls or set()) if url.strip()}
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    rows = []
    for url in cleaned_urls:
        fields = {
            "Instagram URL": url,
            "Required Hashtags": required_hashtags.strip(),
            "Top Comment": top_comment.strip(),
        }
        if url in reels:
            fields["Status"] = REEL_STATUS_MARKER
            fields["Slide CTA"] = REEL_LINES_SLIDE_CTA
        rows.append(_posts_row_values(columns, fields))
    _with_backoff(ws.append_rows, rows, value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def append_generated_post_rows(sheet_id: str, rows: list[dict]) -> None:
    """Append pre-generated rows to the main posts tab."""
    cleaned_rows = [row for row in rows if (row.get("url") or "").strip()]
    if not cleaned_rows:
        return

    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    keys_by_header = {
        "Instagram URL": "url",
        "Required Hashtags": "required_hashtags",
        "Source Username": "source_username",
        "Generated Caption": "caption",
        "Media Type": "media_type",
        "Thumbnail Drive Link": "thumbnail_link",
        "Original Caption": "original_caption",
        "Transcript": "transcript",
        "Top Comment": "top_comment",
        "Speaker Name": "speaker_name",
        "Footer": "footer",
        "Status": "status",
        "Caption Context": "caption_context",
        "Scheduled Time": "scheduled_time",
        "name": "name",
        "text1": "text1",
        "text2": "text2",
        "text3": "text3",
        "Slide CTA": "slide_cta",
        "text4": "text4",
        "text5": "text5",
        "text6": "text6",
        "text7": "text7",
        "text8": "text8",
    }
    values = [
        _posts_row_values(
            columns,
            {header: (source.get(key, "") or "").strip() for header, key in keys_by_header.items()},
        )
        for source in cleaned_rows
    ]

    _with_backoff(ws.append_rows, values, value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def append_manual_post_row(sheet_id: str, row_data: dict) -> None:
    """Append a manually created row to the posts tab (no URL required)."""
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    keys_by_header = {
        "Instagram URL": "url",
        "Source Username": "source_username",
        "Generated Caption": "caption",
        "Media Type": "media_type",
        "Photo Count": "photo_count",
        "Media Drive Link": "media_link",
        "Thumbnail Drive Link": "thumbnail_link",
        "Original Caption": "original_caption",
        "Transcript": "transcript",
        "Top Comment": "top_comment",
        "Speaker Name": "speaker_name",
        "Status": "status",
        "Caption Context": "caption_context",
        "name": "name",
        "text1": "text1",
        "text2": "text2",
        "text3": "text3",
        "Slide CTA": "slide_cta",
        "text4": "text4",
        "text5": "text5",
        "text6": "text6",
        "quote": "quote",
        "text7": "text7",
        "text8": "text8",
    }
    row = _posts_row_values(
        columns,
        {header: str(row_data.get(key) or "").strip() for header, key in keys_by_header.items()},
    )
    _with_backoff(ws.append_row, row, value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def update_generated_post_slides_and_status(
    sheet_id: str,
    row_number: int,
    name: str,
    text1: str,
    text2: str,
    text3: str,
    text4: str,
    text5: str,
    text6: str,
    status: str,
) -> None:
    """Write generated post slide fields and status to the main posts tab."""
    ws = _worksheet(sheet_id)
    # Slide CTA is deliberately not in here: it sits between text3 and text4 and
    # must keep whatever it holds.
    _with_backoff(
        ws.batch_update,
        _posts_updates(_posts_columns(sheet_id, ws), row_number, {
            "name": name, "text1": text1, "text2": text2, "text3": text3,
            "text4": text4, "text5": text5, "text6": text6, "Status": status,
        }),
    )
    _invalidate_rows_cache(sheet_id)


def update_ingest_result(
    sheet_id: str,
    row_number: int,
    username: str,
    media_type: str,
    photo_count: int,
    media_link: str,
    thumbnail_link: str,
    original_caption: str,
    transcript: str,
    status: str,
) -> None:
    """Write ingest results to cols C and E-J, default name to Q, and status to N."""
    ws = _worksheet(sheet_id)
    cleaned_username = (username or "").strip()
    if not cleaned_username or media_type == "article":
        default_name = cleaned_username
    elif cleaned_username.startswith("@"):
        default_name = cleaned_username
    else:
        default_name = f"@{cleaned_username}"
    _with_backoff(
        ws.batch_update,
        _posts_updates(_posts_columns(sheet_id, ws), row_number, {
            "Source Username": username,
            "Media Type": media_type,
            "Photo Count": str(photo_count) if photo_count else "",
            "Media Drive Link": media_link,
            "Thumbnail Drive Link": thumbnail_link,
            "Original Caption": original_caption,
            "Transcript": transcript,
            "Status": status,
            "name": default_name,
        }),
    )
    _invalidate_rows_cache(sheet_id)


def update_caption(sheet_id: str, row_number: int, caption: str, status: str) -> None:
    """Write the generated caption and the status for a single row."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        _posts_updates(_posts_columns(sheet_id, ws), row_number,
                       {"Generated Caption": caption, "Status": status}),
    )
    _invalidate_rows_cache(sheet_id)


def update_caption_and_metadata(
    sheet_id: str,
    row_number: int,
    caption: str,
    status: str,
    caption_context: str,
    speaker_name: str,
    hashtags: str,
    top_comment: str,
    footer: str,
) -> None:
    """Write generated caption, status, and editor metadata after one worksheet lookup."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        _posts_updates(_posts_columns(sheet_id, ws), row_number, {
            "Generated Caption": caption,
            "Required Hashtags": hashtags,
            "Top Comment": top_comment,
            "Speaker Name": speaker_name,
            "Footer": footer,
            "Status": status,
            "Caption Context": caption_context,
        }),
    )
    _invalidate_rows_cache(sheet_id)


def update_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write status to col N for a single row."""
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    _with_backoff(ws.update, _posts_cell(columns, "Status", row_number),
                  [[status]])
    _invalidate_rows_cache(sheet_id)


def update_transcript(sheet_id: str, row_number: int, transcript: str) -> None:
    """Write transcript to col J for a single row."""
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    _with_backoff(ws.update, _posts_cell(columns, "Transcript", row_number),
                  [[transcript]])
    _invalidate_rows_cache(sheet_id)


def update_thumbnail_link(sheet_id: str, row_number: int, thumbnail_link: str) -> None:
    """Write thumbnail drive link to col H for a single row."""
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    _with_backoff(ws.update, _posts_cell(columns, "Thumbnail Drive Link", row_number),
                  [[thumbnail_link]])
    _invalidate_rows_cache(sheet_id)


def update_reel_drive_link(sheet_id: str, row_number: int, reel_link: str) -> None:
    """Write the composed reel's Drive link to col AB for a single row.

    Kept apart from Media Drive Link, which stays pointed at the source video.
    This is the cropped, headlined cut, and holding it on the row is what lets
    another machine pick the post up with the reel already attached.
    """
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    _with_backoff(ws.update, _posts_cell(columns, "Reel Drive Link", row_number), [[reel_link]])
    _invalidate_rows_cache(sheet_id)


def update_caption_context(sheet_id: str, row_number: int, caption_context: str) -> None:
    """Write caption context to col O for a single row."""
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    _with_backoff(ws.update, _posts_cell(columns, "Caption Context", row_number),
                  [[caption_context]])
    _invalidate_rows_cache(sheet_id)


def update_scheduled_times(sheet_id: str, assignments: dict[int, str]) -> None:
    """Write scheduled time values to col P for multiple rows."""
    if not assignments:
        return
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    _with_backoff(
        ws.batch_update,
        [
            {"range": _posts_cell(columns, "Scheduled Time", row_number), "values": [[scheduled_time]]}
            for row_number, scheduled_time in assignments.items()
        ],
    )
    _invalidate_rows_cache(sheet_id)


def update_carousel_fields(
    sheet_id: str,
    row_number: int,
    name: str,
    text1: str,
    text2: str,
    text3: str,
    text4: str = "",
    text5: str = "",
    text6: str = "",
    text7: str | None = None,
    text8: str | None = None,
) -> None:
    """Write carousel fields to cols Q-X (and Z-AA) and set status to 'slides'.

    text7 and text8 live past the original layout, so they are only touched when
    the caller passes them. None means "leave whatever is there" — that way a
    caller that predates the extra slides cannot blank them out, while a caller
    rewriting the whole carousel passes "" to clear them.
    """
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    fields = {
        "Status": "slides",
        "name": name, "text1": text1, "text2": text2, "text3": text3,
        # The old span wrote Slide CTA blank as the cell between text3 and text4.
        # Now that each field is addressed by name, keep clearing it explicitly.
        "Slide CTA": "",
        "text4": text4, "text5": text5, "text6": text6,
    }
    if text7 is not None or text8 is not None:
        fields["text7"] = text7 or ""
        fields["text8"] = text8 or ""
    _with_backoff(ws.batch_update, _posts_updates(columns, row_number, fields))
    _invalidate_rows_cache(sheet_id)


def get_last_scheduled_times(sheet_id: str) -> list[str]:
    """Return the last saved workspace scheduled times from metadata."""
    ws = _metadata_worksheet(sheet_id)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for record in records:
        key = (record.get("key", "") or "").strip()
        if key not in {"last_scheduled_time", _LAST_SCHEDULED_TIMES_KEY}:
            continue
        raw_value = (record.get("value", "") or "").strip()
        if not raw_value:
            return []
        if key == "last_scheduled_time":
            return [raw_value]
        try:
            values = json.loads(raw_value)
        except json.JSONDecodeError:
            return [raw_value]
        if isinstance(values, list):
            return [str(value).strip() for value in values if str(value).strip()]
        return [str(values).strip()] if str(values).strip() else []
    return []


def update_last_scheduled_times(sheet_id: str, scheduled_times: list[str]) -> None:
    """Persist the last assigned workspace scheduled times in metadata."""
    ws = _metadata_worksheet(sheet_id)
    payload = json.dumps([value.strip() for value in scheduled_times if value.strip()])
    records = _with_backoff(ws.get_all_records, default_blank="")
    for index, record in enumerate(records, start=2):
        key = (record.get("key", "") or "").strip()
        if key in {"last_scheduled_time", _LAST_SCHEDULED_TIMES_KEY}:
            _with_backoff(ws.update, f"A{index}:B{index}", [[_LAST_SCHEDULED_TIMES_KEY, payload]])
            return
    _with_backoff(ws.append_row, [_LAST_SCHEDULED_TIMES_KEY, payload], value_input_option="USER_ENTERED")


def get_slide_cta_options(sheet_id: str) -> dict[str, str]:
    """Return saved slide 3 CTA choices keyed by sheet row number."""
    ws = _metadata_worksheet(sheet_id)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for record in records:
        key = (record.get("key", "") or "").strip()
        if key != _SLIDE_CTA_OPTIONS_KEY:
            continue
        raw_value = (record.get("value", "") or "").strip()
        if not raw_value:
            return {}
        try:
            values = json.loads(raw_value)
        except json.JSONDecodeError:
            return {}
        if not isinstance(values, dict):
            return {}
        return {
            str(row_number).strip(): str(option).strip()
            for row_number, option in values.items()
            if str(row_number).strip() and str(option).strip()
        }
    return {}


def update_slide_cta_option(sheet_id: str, row_number: int, option: str) -> None:
    """Persist a row's selected slide CTA in column U of the main sheet."""
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    _with_backoff(ws.update, _posts_cell(columns, "Slide CTA", row_number),
                  [[(option or "").strip()]])


def update_reel_lines_fields(
    sheet_id: str,
    row_number: int,
    name: str,
    headlines: str,
    status: str = "done",
) -> None:
    """Write a Reel Lines row: its name to Q, its headlines to R, the marker to U.

    Deliberately not update_carousel_fields, which blanks column U and forces the
    status to 'slides'. A Reel Lines row has to keep its marker, keep its ten
    headlines on separate lines, and land on the status the caller settled on.
    """
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        _posts_updates(_posts_columns(sheet_id, ws), row_number, {
            "Status": (status or "").strip(),
            "name": (name or "").strip(),
            "text1": headlines or "",
            "Slide CTA": REEL_LINES_SLIDE_CTA,
        }),
    )
    _invalidate_rows_cache(sheet_id)


def update_quote(sheet_id: str, row_number: int, quote: str) -> None:
    """Write a pull-quote to column Y of the main sheet."""
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    _with_backoff(ws.update, _posts_cell(columns, "quote", row_number),
                  [[(quote or "").strip()]])
    _invalidate_rows_cache(sheet_id)


def get_original_thumbnails(sheet_id: str) -> dict[str, str]:
    """Return saved pre-blur thumbnail links keyed by row number (as str)."""
    ws = _metadata_worksheet(sheet_id)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for record in records:
        if (record.get("key", "") or "").strip() != _ORIGINAL_THUMBNAILS_KEY:
            continue
        raw = (record.get("value", "") or "").strip()
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return {str(k): str(v) for k, v in data.items() if k and v} if isinstance(data, dict) else {}
    return {}


def _update_original_thumbnails(sheet_id: str, data: dict) -> None:
    ws = _metadata_worksheet(sheet_id)
    payload = json.dumps(data)
    records = _with_backoff(ws.get_all_records, default_blank="")
    for index, record in enumerate(records, start=2):
        if (record.get("key", "") or "").strip() == _ORIGINAL_THUMBNAILS_KEY:
            _with_backoff(ws.update, f"A{index}:B{index}", [[_ORIGINAL_THUMBNAILS_KEY, payload]])
            return
    _with_backoff(ws.append_row, [_ORIGINAL_THUMBNAILS_KEY, payload], value_input_option="USER_ENTERED")


def save_original_thumbnail(sheet_id: str, row_number: int, link: str) -> None:
    """Persist a row's pre-blur thumbnail link so Unblur can restore it."""
    data = get_original_thumbnails(sheet_id)
    data[str(row_number)] = link
    _update_original_thumbnails(sheet_id, data)


def clear_original_thumbnail(sheet_id: str, row_number: int) -> None:
    """Remove the stored pre-blur thumbnail link after Unblur is used."""
    data = get_original_thumbnails(sheet_id)
    data.pop(str(row_number), None)
    _update_original_thumbnails(sheet_id, data)


def shift_original_thumbnails_after_delete(sheet_id: str, deleted_row_number: int) -> None:
    """Re-key the blur map after a row deletion.

    Removes the deleted row's entry and shifts all higher row numbers down by 1
    so blur state stays aligned with the sheet after rows renumber.
    """
    data = get_original_thumbnails(sheet_id)
    if not data:
        # Nothing stored, so nothing to re-key: skip the read-modify-write entirely.
        return
    shifted = {}
    for k, v in data.items():
        if k == str(deleted_row_number):
            continue
        try:
            n = int(k)
        except ValueError:
            shifted[k] = v
            continue
        shifted[str(n - 1) if n > deleted_row_number else k] = v
    if shifted == data:
        return
    _update_original_thumbnails(sheet_id, shifted)


def get_fundraising_links(sheet_id: str) -> list[dict[str, str]]:
    """Return fundraising top-comment presets from the optional worksheet.

    Expected layout is two columns:
      A: label/name
      B: full top comment text
    The first row may optionally be a header row.
    """
    ws = _optional_worksheet(sheet_id, _FUNDRAISING_SHEET_TITLE)
    if ws is None:
        return []

    values = _with_backoff(ws.get_all_values)
    if not values:
        return []

    presets: list[dict[str, str]] = []
    for index, row in enumerate(values):
        label = row[0].strip() if len(row) > 0 else ""
        link = row[1].strip() if len(row) > 1 else ""
        if not label and not link:
            continue
        if index == 0 and label.lower() in {"name", "label", "fundraising", "preset"} and link.lower() in {"link", "url", "comment", "top comment"}:
            continue
        if not label or not link:
            continue
        presets.append({"label": label, "link": link})
    return presets


_LABEL_HEADER_WORDS = {"name", "label", "organization", "org", "client", "source"}


def _label_value_presets(
    sheet_id: str,
    title: str,
    value_key: str,
    value_header_words: set[str],
) -> list[dict[str, str]]:
    """Read a two-column label/value preset tab (col A label, col B value).

    The first row may optionally be a header row. Rows missing either column are
    skipped, so a half-filled row can never reach an app dropdown.
    """
    ws = _optional_worksheet(sheet_id, title)
    if ws is None:
        return []

    values = _with_backoff(ws.get_all_values)
    if not values:
        return []

    presets: list[dict[str, str]] = []
    for index, row in enumerate(values):
        label = row[0].strip() if len(row) > 0 else ""
        value = " ".join((row[1] if len(row) > 1 else "").split())
        if not label and not value:
            continue
        if (
            index == 0
            and label.lower() in _LABEL_HEADER_WORDS
            and value.lower() in value_header_words
        ):
            continue
        if not label or not value:
            continue
        presets.append({"label": label, value_key: value})
    return presets


def get_hashtag_presets(sheet_id: str) -> list[dict[str, str]]:
    """Organization hashtag presets from the optional `hashtags` worksheet.

    Column A is the client label shown in app dropdowns, column B the hashtag text
    written into Required Hashtags.
    """
    return _label_value_presets(
        sheet_id,
        _HASHTAGS_SHEET_TITLE,
        "hashtags",
        {"hashtag", "hashtags", "required hashtags", "tag", "tags"},
    )


def get_doc_presets(sheet_id: str) -> list[dict[str, str]]:
    """Client source documents from the optional `docs` worksheet.

    Expected layout is four columns:
      A: client label shown in app dropdowns
      B: Google Doc link that client's items are read from
      C: hashtag text used for links added from that document (optional)
      D: comment link used as the Top Comment for links added from that document
         (optional) - a bare URL becomes the standard "Comment LINK" CTA, and any
         other text is used as the top comment verbatim
    The first row may optionally be a header row.
    """
    ws = _optional_worksheet(sheet_id, _DOCS_SHEET_TITLE)
    if ws is None:
        return []

    values = _with_backoff(ws.get_all_values)
    if not values:
        return []

    presets: list[dict[str, str]] = []
    for index, row in enumerate(values):
        label = row[0].strip() if len(row) > 0 else ""
        url = " ".join((row[1] if len(row) > 1 else "").split())
        hashtags = " ".join((row[2] if len(row) > 2 else "").split())
        comment_link = " ".join((row[3] if len(row) > 3 else "").split())
        if not label and not url:
            continue
        if (
            index == 0
            and label.lower() in _LABEL_HEADER_WORDS
            and url.lower() in {"url", "link", "doc", "docs", "document", "google doc"}
        ):
            continue
        if not label or not url:
            continue
        presets.append(
            {
                "label": label,
                "url": url,
                "hashtags": hashtags,
                "comment_link": comment_link,
            }
        )
    return presets


def update_metadata(
    sheet_id: str,
    row_number: int,
    caption_context: str,
    speaker_name: str,
    hashtags: str,
    top_comment: str,
    footer: str,
) -> None:
    """Write user metadata to cols B and K-M, and caption context to O."""
    ws = _worksheet(sheet_id)
    _with_backoff(
        ws.batch_update,
        _posts_updates(_posts_columns(sheet_id, ws), row_number, {
            "Required Hashtags": hashtags,
            "Top Comment": top_comment,
            "Speaker Name": speaker_name,
            "Footer": footer,
            "Caption Context": caption_context,
        }),
    )
    _invalidate_rows_cache(sheet_id)


def update_speaker_names_batch(sheet_id: str, updates: dict[int, str]) -> None:
    """Write multiple speaker names to column L in one batch."""
    if not updates:
        return
    ws = _worksheet(sheet_id)
    columns = _posts_columns(sheet_id, ws)
    requests = [
        {"range": _posts_cell(columns, "Speaker Name", row_number), "values": [[speaker_name]]}
        for row_number, speaker_name in sorted(updates.items())
    ]
    _with_backoff(ws.batch_update, requests)
    _invalidate_rows_cache(sheet_id)


def delete_row(sheet_id: str, row_number: int) -> None:
    """Delete a single sheet row by absolute row number."""
    ws = _worksheet(sheet_id)
    _with_backoff(ws.delete_rows, row_number)
    _invalidate_rows_cache(sheet_id)


def _deleted_at_stamp() -> str:
    """Now, in Eastern time - the timezone the rest of the workflow is planned in."""
    stamp = datetime.now(timezone.utc)
    try:
        stamp = stamp.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        pass
    return stamp.strftime("%Y-%m-%d %H:%M:%S")


def _archive_source_headers(sheet_id: str) -> list[str]:
    """The posts headers in the order the posts tab actually holds them.

    archive_row copies a row across cell for cell, so the archive's header row
    has to match the source's order or a moved column ends up filed under its
    old neighbour's name.
    """
    columns = _posts_columns(sheet_id, _worksheet(sheet_id))
    return [header for header, _index in sorted(columns.items(), key=lambda item: item[1])]


def _archive_worksheet(sheet_id: str) -> gspread.Worksheet:
    """The "Safe to Delete" tab, created with a header row the first time it is needed."""
    if sheet_id in _archive_ready:
        # Header row already confirmed for this process, and the worksheet is cached,
        # so this costs no call at all.
        return _named_worksheet(sheet_id, _ARCHIVE_SHEET_TITLE)
    # Mirrors whatever order the posts tab is in, since archive_row copies the
    # row across verbatim; a canonical order here would mislabel a moved column.
    headers = [*_archive_source_headers(sheet_id), "Deleted At"]
    try:
        ws = _named_worksheet(sheet_id, _ARCHIVE_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = _with_backoff(
            _workbook(sheet_id).add_worksheet,
            title=_ARCHIVE_SHEET_TITLE,
            rows=200,
            cols=len(headers),
        )
        _worksheets[(sheet_id, _ARCHIVE_SHEET_TITLE)] = ws
        _with_backoff(ws.append_row, headers, value_input_option="RAW")
        _archive_ready.add(sheet_id)
        return ws
    if not any(value.strip() for value in _with_backoff(ws.row_values, 1)):
        _with_backoff(ws.append_row, headers, value_input_option="RAW")
    _archive_ready.add(sheet_id)
    return ws


def archive_row(sheet_id: str, row_number: int) -> None:
    """Move a row to the "Safe to Delete" tab, then drop it from the posts tab.

    The copy keeps the posts column order, so restoring a row is a paste back into
    the posts tab. Values go in RAW so a caption that starts with "=" stays text.
    """
    ws = _worksheet(sheet_id)
    values = _with_backoff(ws.row_values, row_number)
    # row_values trims trailing empties, so pad to the full column set before adding
    # the timestamp - otherwise it lands in whichever column happened to be last.
    width = max(_posts_columns(sheet_id, ws).values())
    padded = [*values, *[""] * (width - len(values))][:width]
    archive_ws = _archive_worksheet(sheet_id)
    # Copy first, delete second: a failed append must not lose the row.
    _with_backoff(
        archive_ws.append_row,
        [*padded, _deleted_at_stamp()],
        value_input_option="RAW",
    )
    _with_backoff(ws.delete_rows, row_number)
    _invalidate_rows_cache(sheet_id)


# ---------------------------------------------------------------------------
# monitors tab helpers
# ---------------------------------------------------------------------------

def get_monitor_rows(sheet_id: str) -> list[dict]:
    """Return all rows from the monitors tab."""
    cached = _get_cached_rows(sheet_id, "monitors")
    if cached is not None:
        return cached
    ws = _named_worksheet(sheet_id, "monitors")
    values = _with_backoff(ws.get_all_values)
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for i, row in enumerate(values[1:], start=2):
        record = {headers[j]: (row[j].strip() if j < len(row) else "") for j in range(len(headers))}
        record["row_number"] = i
        rows.append(record)
    _set_cached_rows(sheet_id, "monitors", rows)
    return rows


def get_open_monitor_rows(sheet_id: str) -> list[dict]:
    """Return rows from the monitors tab where status is 'open'."""
    return [r for r in get_monitor_rows(sheet_id) if r.get("status", "").strip().lower() == "open"]


def update_monitor_summary(sheet_id: str, row_number: int, summary: str, last_checked: str) -> None:
    """Write summary and last checked date to the monitors tab by header name."""
    ws = _named_worksheet(sheet_id, "monitors")
    _update_row_fields_by_headers(ws, row_number, {"last": last_checked, "summary": summary})
    _invalidate_rows_cache(sheet_id)


# ---------------------------------------------------------------------------
# substack tab helpers
# ---------------------------------------------------------------------------


def _ensure_substack_headers(ws) -> None:
    cache_key = ("substack_headers", ws.id)
    if cache_key in _headers_checked:
        return
    values = _with_backoff(ws.get_all_values)
    headers = [header.strip() for header in values[0]] if values else []
    normalized = [header.lower() for header in headers if header]
    expected = [header.lower() for header in _SUBSTACK_HEADERS]
    base_required = ["url", "article", "status"]
    if "name" in normalized:
        upgradeable_required = ["url", "name", "article", "status"]
    else:
        upgradeable_required = base_required

    if not headers:
        _with_backoff(ws.update, "A1:I1", [_SUBSTACK_HEADERS])
        _headers_checked.add(cache_key)
        return

    def _rewrite_substack_rows(rows: list[list[str]]) -> None:
        _with_backoff(ws.clear)
        _with_backoff(ws.update, f"A1:I{len(rows)}", rows)

    def _looks_like_shifted_expected_layout(data_rows: list[list[str]]) -> bool:
        checked = 0
        shifted_matches = 0
        status_values = {"open", "closed", "ingested", "posts created"}
        for row in data_rows:
            if not any((cell or "").strip() for cell in row):
                continue
            padded = row + [""] * (len(_SUBSTACK_HEADERS) - len(row))
            topic_breakdown_value = padded[3].strip().lower()
            status_value = padded[4].strip()
            instagram_url_value = padded[5].strip().lower()
            monitoring_status_value = padded[6].strip()
            if not any([topic_breakdown_value, status_value, instagram_url_value, monitoring_status_value]):
                continue
            checked += 1
            if (
                topic_breakdown_value in status_values
                and (not status_value or status_value.startswith("http"))
                and instagram_url_value in {"", "open", "closed"}
                and (not monitoring_status_value or monitoring_status_value.startswith("20"))
            ):
                shifted_matches += 1
        return checked > 0 and shifted_matches >= max(1, checked // 2)

    if normalized == [header.lower() for header in _SUBSTACK_LEGACY_HEADERS_WITH_NAME]:
        migrated_rows = [_SUBSTACK_HEADERS]
        for row in values[1:]:
            padded = row + [""] * (len(_SUBSTACK_LEGACY_HEADERS_WITH_NAME) - len(row))
            migrated_rows.append([
                padded[0].strip(),
                padded[1].strip(),
                padded[2].strip(),
                "",
                padded[3].strip(),
                padded[4].strip(),
                padded[5].strip(),
                padded[6].strip(),
                padded[7].strip(),
            ])
        _rewrite_substack_rows(migrated_rows)
        _headers_checked.add(cache_key)
        return

    if normalized == [header.lower() for header in _SUBSTACK_LEGACY_HEADERS_NO_NAME]:
        migrated_rows = [_SUBSTACK_HEADERS]
        for row in values[1:]:
            padded = row + [""] * (len(_SUBSTACK_LEGACY_HEADERS_NO_NAME) - len(row))
            migrated_rows.append([
                padded[0].strip(),
                "",
                padded[1].strip(),
                "",
                padded[2].strip(),
                padded[3].strip(),
                padded[4].strip(),
                padded[5].strip(),
                padded[6].strip(),
            ])
        _rewrite_substack_rows(migrated_rows)
        _headers_checked.add(cache_key)
        return

    if normalized == expected and _looks_like_shifted_expected_layout(values[1:]):
        migrated_rows = [_SUBSTACK_HEADERS]
        for row in values[1:]:
            padded = row + [""] * (len(_SUBSTACK_HEADERS) - len(row))
            migrated_rows.append([
                padded[0].strip(),
                padded[1].strip(),
                padded[2].strip(),
                "",
                padded[3].strip(),
                padded[4].strip(),
                padded[5].strip(),
                padded[6].strip(),
                padded[7].strip(),
            ])
        _rewrite_substack_rows(migrated_rows)
        _headers_checked.add(cache_key)
        return

    if all(header in normalized for header in upgradeable_required) and not all(
        header in normalized for header in expected
    ):
        header_index = {header.lower(): idx for idx, header in enumerate(headers)}
        migrated_rows = [_SUBSTACK_HEADERS]
        for row in values[1:]:
            migrated_record = {header: "" for header in _SUBSTACK_HEADERS}
            for source_header, idx in header_index.items():
                if source_header in migrated_record and idx < len(row):
                    migrated_record[source_header] = row[idx].strip()
            migrated_rows.append([migrated_record.get(header, "") for header in _SUBSTACK_HEADERS])
        _rewrite_substack_rows(migrated_rows)
        _headers_checked.add(cache_key)
        return

    missing_required = [header for header in base_required if header not in normalized]
    if missing_required:
        raise RuntimeError(
            "substack tab is missing required header(s): " + ", ".join(missing_required)
        )
    _headers_checked.add(cache_key)


def _substack_header_map(ws) -> dict[str, int]:
    _ensure_substack_headers(ws)
    headers = [header.strip() for header in _with_backoff(ws.row_values, 1)]
    return {
        header: index + 1
        for index, header in enumerate(headers)
        if header
    }

def get_substack_rows(sheet_id: str) -> list[dict]:
    """Return all rows from the substack tab."""
    cached = _get_cached_rows(sheet_id, "substack")
    if cached is not None:
        return cached
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    _ensure_substack_headers(ws)
    values = _with_backoff(ws.get_all_values)
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for i, row in enumerate(values[1:], start=2):
        record = {headers[j]: (row[j].strip() if j < len(row) else "") for j in range(len(headers))}
        article_value = (record.get("article") or "").strip().lower()
        status_value = (record.get("status") or "").strip().lower()
        if article_value in {"open", "closed", "ingested", "posts created"} and status_value in {"", "open", "closed", "ingested", "posts created"}:
            record["article"] = ""
        record["row_number"] = i
        rows.append(record)
    _set_cached_rows(sheet_id, "substack", rows)
    return [row.copy() for row in rows]


def get_open_substack_rows(sheet_id: str) -> list[dict]:
    """Return rows from the substack tab where status is 'open'."""
    return [r for r in get_substack_rows(sheet_id) if r.get("status", "").strip().lower() == "open"]


def update_substack_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write article workflow status to the substack tab."""
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    _update_row_fields_by_headers(ws, row_number, {"status": status})
    _invalidate_rows_cache(sheet_id)


def update_substack_article(sheet_id: str, row_number: int, article: str) -> None:
    """Write article body to the substack tab."""
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    _update_row_fields_by_headers(ws, row_number, {"article": article})
    _invalidate_rows_cache(sheet_id)


def update_substack_topic_breakdown(sheet_id: str, row_number: int, topic_breakdown: str) -> None:
    """Write persisted topic breakdown JSON/text to the substack tab."""
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    _update_row_fields_by_headers(ws, row_number, {"topic breakdown": topic_breakdown})
    _invalidate_rows_cache(sheet_id)


def append_substack_row(sheet_id: str, url: str) -> None:
    """Append a new row to the substack tab with default article and monitoring states."""
    ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
    header_map = _substack_header_map(ws)
    ordered_headers = [header for header, _ in sorted(header_map.items(), key=lambda item: item[1])]
    row = {
        "url": (url or "").strip(),
        "name": "",
        "article": "",
        "topic breakdown": "",
        "status": "open",
        "instagram url": "",
        "monitoring status": "closed",
        "last comment retrieved": "",
        "summary": "",
    }
    _with_backoff(
        ws.append_row,
        [row.get(header, "") for header in ordered_headers],
        value_input_option="USER_ENTERED",
    )
    _invalidate_rows_cache(sheet_id)


def get_open_comment_monitor_rows(sheet_id: str) -> list[dict]:
    """Return open comment-monitor rows from the merged substack sheet plus legacy monitors rows."""
    merged_rows: list[dict] = []
    seen_urls: set[str] = set()

    for row in get_substack_rows(sheet_id):
        instagram_url = row.get("instagram url", "").strip()
        monitoring_status = row.get("monitoring status", "").strip().lower()
        if monitoring_status != "open" or not instagram_url:
            continue
        seen_urls.add(instagram_url)
        substack_url = row.get("url", "").strip()
        merged_rows.append(
            {
                "source": "substack",
                "row_number": row["row_number"],
                "label": substack_url or instagram_url,
                "url": instagram_url,
                "substack_url": substack_url,
                "summary": row.get("summary", "").strip(),
                "last_checked": row.get("last comment retrieved", "").strip(),
            }
        )

    legacy_ws = _optional_worksheet(sheet_id, "monitors")
    if legacy_ws is None:
        return merged_rows

    for row in get_open_monitor_rows(sheet_id):
        url = row.get("url", "").strip()
        if not url or url in seen_urls:
            continue
        merged_rows.append(
            {
                "source": "monitors",
                "row_number": row["row_number"],
                "label": row.get("label", "").strip() or row.get("substack url", "").strip() or url,
                "url": url,
                "substack_url": row.get("substack url", "").strip(),
                "summary": row.get("summary", "").strip(),
                "last_checked": row.get("last", "").strip(),
            }
        )

    return merged_rows


def update_comment_monitor_summary(
    sheet_id: str,
    source: str,
    row_number: int,
    summary: str,
    last_checked: str,
) -> None:
    """Write monitoring summary and last-checked timestamp to the correct sheet."""
    normalized_source = (source or "").strip().lower()
    if normalized_source == "substack":
        ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
        _update_row_fields_by_headers(
            ws,
            row_number,
            {"last comment retrieved": last_checked, "summary": summary},
        )
        _invalidate_rows_cache(sheet_id)
        return
    update_monitor_summary(sheet_id, row_number, summary, last_checked)


def update_comment_monitor_last_checked(
    sheet_id: str,
    source: str,
    row_number: int,
    last_checked: str,
) -> None:
    """Write only the last-checked timestamp to the correct monitoring row."""
    normalized_source = (source or "").strip().lower()
    if normalized_source == "substack":
        ws = _named_worksheet(sheet_id, _SUBSTACK_SHEET_TITLE)
        _update_row_fields_by_headers(ws, row_number, {"last comment retrieved": last_checked})
        _invalidate_rows_cache(sheet_id)
        return
    ws = _named_worksheet(sheet_id, "monitors")
    _update_row_fields_by_headers(ws, row_number, {"last": last_checked})
    _invalidate_rows_cache(sheet_id)


def _ensure_substack_post_headers(ws) -> None:
    cache_key = ("substack_posts_headers", ws.id)
    if cache_key in _headers_checked:
        return
    values = _with_backoff(ws.get_all_values)
    headers = values[0] if values else []
    expected_headers = [
        "url",
        "angle",
        "caption",
        "text1",
        "text2",
        "text3",
        "text4",
        "text5",
        "text6",
        "cta",
        "status",
        "slide_prompt",
        "slide_input",
        "post_type",
        "topics",
    ]
    if not headers:
        _with_backoff(ws.update, "A1:O1", [expected_headers])
        _headers_checked.add(cache_key)
        return
    missing_headers = [header for header in expected_headers if header not in headers]
    if missing_headers:
        raise RuntimeError(
            "substack_posts is missing required header(s): " + ", ".join(missing_headers)
        )
    _headers_checked.add(cache_key)


def _column_letter(index: int) -> str:
    result = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _update_row_fields_by_headers(
    ws: gspread.Worksheet,
    row_number: int,
    field_values: dict[str, str],
) -> None:
    headers = [header.strip() for header in _with_backoff(ws.row_values, 1)]
    normalized = {
        header.lower(): index + 1
        for index, header in enumerate(headers)
        if header
    }
    missing_headers = [
        field for field in field_values
        if field.strip().lower() not in normalized
    ]
    if missing_headers:
        raise RuntimeError(
            f"{ws.title} tab is missing required header(s): {', '.join(missing_headers)}."
        )
    requests = [
        {
            "range": f"{_column_letter(normalized[field.strip().lower()])}{row_number}",
            "values": [[value]],
        }
        for field, value in field_values.items()
    ]
    _with_backoff(ws.batch_update, requests)


def _substack_post_header_map(ws) -> dict[str, int]:
    _ensure_substack_post_headers(ws)
    headers = [header.strip() for header in _with_backoff(ws.row_values, 1)]
    return {
        header: index + 1
        for index, header in enumerate(headers)
        if header
    }


def append_substack_post_rows(sheet_id: str, rows: list[dict]) -> None:
    """Append rows to substack_posts tab.

    Each dict must have keys: url, angle, caption, text1, text2, text3, cta, status.
    Newer sheets may also include text4-text6, slide_prompt, slide_input, post_type, and topics.
    """
    if not rows:
        return
    ws = _named_worksheet(sheet_id, "substack_posts")
    header_map = _substack_post_header_map(ws)
    ordered_headers = [header for header, _ in sorted(header_map.items(), key=lambda item: item[1])]
    values = [
        [r.get(header, "") for header in ordered_headers]
        for r in rows
    ]
    _with_backoff(ws.append_rows, values, value_input_option="USER_ENTERED")
    _invalidate_rows_cache(sheet_id)


def get_substack_post_rows(sheet_id: str) -> list[dict]:
    """Return all rows from the substack_posts tab."""
    cached = _get_cached_rows(sheet_id, "substack_posts")
    if cached is not None:
        return cached
    ws = _named_worksheet(sheet_id, "substack_posts")
    _ensure_substack_post_headers(ws)
    values = _with_backoff(ws.get_all_values)
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for i, row in enumerate(values[1:], start=2):
        record = {headers[j]: (row[j].strip() if j < len(row) else "") for j in range(len(headers))}
        record["row_number"] = i
        rows.append(record)
    _set_cached_rows(sheet_id, "substack_posts", rows)
    return [row.copy() for row in rows]


def update_substack_post_status(sheet_id: str, row_number: int, status: str) -> None:
    """Write status to the status column of the substack_posts tab."""
    ws = _named_worksheet(sheet_id, "substack_posts")
    header_map = _substack_post_header_map(ws)
    status_column = _column_letter(header_map["status"])
    _with_backoff(ws.update, f"{status_column}{row_number}", [[status]])
    _invalidate_rows_cache(sheet_id)


def update_substack_post_slides_and_status(
    sheet_id: str,
    row_number: int,
    text1: str,
    text2: str,
    text3: str,
    text4: str,
    text5: str,
    text6: str,
    status: str,
) -> None:
    """Write slide text and status to a substack_posts row."""
    ws = _named_worksheet(sheet_id, "substack_posts")
    header_map = _substack_post_header_map(ws)
    updates = []
    for key, value in {
        "text1": text1,
        "text2": text2,
        "text3": text3,
        "text4": text4,
        "text5": text5,
        "text6": text6,
        "status": status,
    }.items():
        column = _column_letter(header_map[key])
        updates.append({"range": f"{column}{row_number}", "values": [[value]]})
    _with_backoff(
        ws.batch_update,
        updates,
    )
    _invalidate_rows_cache(sheet_id)
