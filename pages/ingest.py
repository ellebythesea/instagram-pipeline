"""Ingest page: paste a client document, pull out its linked items, add the picked ones to the posts sheet.

No AI is involved and nothing is rewritten. Every http(s) URL in the pasted text is
located by regex and paired with the document's own wording around it, verbatim, so
nothing in a long document can be silently skipped or reworded. Links on blocked
platforms (Twitter/X, Threads, Reddit) are left out; everything else is listed.
"""

import hashlib
import html
import os
import re
import sys
from datetime import date, datetime
from urllib.parse import urlparse, urlunparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config import GOOGLE_SHEET_ID
from drive import export_google_doc_html, export_google_doc_markdown
from sheets import append_link_rows, get_all_rows, get_doc_presets, get_hashtag_presets
from utils.auth import require_auth
from utils.error_labels import describe_error
from utils.styles import inject as inject_styles

# Shorter than this, a line is a lead-in (e.g. "@aaronparnas:") rather than a headline.
HEADLINE_MIN_CHARS = 20
# Long enough for a paragraph-style item, short enough to keep a table row readable.
HEADLINE_MAX_CHARS = 300

MARKDOWN_LINK_RE = re.compile(r"\[([^\]\n]*)\]\(\s*(https?://[^\s)]+?)\s*\)")
BARE_URL_RE = re.compile(r"https?://[^\s<>()\[\]\"'\\]+")
LIST_MARKER_RE = re.compile(r"^\s*(?:[*\-+•]|\d+[.)])\s+")
MARKDOWN_ESCAPE_RE = re.compile(r"\\([\\`*_{}\[\]()#+\-.!|>~])")
# Instagram query strings (?hl=en, ?img_index=1) point at the same post.
INSTAGRAM_HOSTS = ("instagram.com",)
# Platforms that are never posted from, so their links are dropped on sight.
BLOCKED_HOSTS = ("twitter.com", "x.com", "t.co", "threads.com", "threads.net", "reddit.com")

# A highlighted run in the HTML export is a span with a non-white background colour.
HIGHLIGHT_SPAN_RE = re.compile(
    r"<span[^>]*background-color:\s*([^;\"']+)[^>]*>(.*?)</span>", re.IGNORECASE | re.DOTALL
)
HTML_TAG_RE = re.compile(r"<[^>]+>")
UNHIGHLIGHTED_COLORS = {"#ffffff", "#fff", "transparent", "white", "none", "initial", "inherit"}
# Below this length a highlighted run is a fragment (a heading, a word) and matching
# it against headlines would produce false positives.
HIGHLIGHT_MIN_CHARS = 25

# In a Google Doc's markdown export each tab starts with its title as a `# ` heading.
DOC_TAB_HEADING_RE = re.compile(r"^#\s+(\S.*)$")
# Tab titles are usually dates, which is how the newest tab is picked.
TAB_DATE_FORMATS = ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%y", "%m-%d-%Y", "%b %d %Y", "%B %d %Y")


def _host(url: str) -> str:
    return (urlparse((url or "").strip()).netloc or "").lower().removeprefix("www.")


def _is_http_url(url: str) -> bool:
    parsed = urlparse((url or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _host_matches(url: str, domains: tuple[str, ...]) -> bool:
    """True when the URL's host is one of these domains or a subdomain of one."""
    host = _host(url)
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)


def _is_instagram_url(url: str) -> bool:
    return _host_matches(url, INSTAGRAM_HOSTS)


def _is_blocked_url(url: str) -> bool:
    """Twitter/X, Threads, and Reddit links are never posted from."""
    return _host_matches(url, BLOCKED_HOSTS)


def _canonical_url(url: str) -> str:
    """Trim trailing punctuation, and drop tracking query strings from Instagram links."""
    cleaned = (url or "").strip().rstrip(").,;:\"'>]}")
    if not cleaned:
        return ""
    if _is_instagram_url(cleaned):
        parsed = urlparse(cleaned)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
    return cleaned


def _normalize_url(url: str) -> str:
    """Comparison key for a URL: no scheme/www, no trailing slash, lowercase host."""
    cleaned = _canonical_url(url)
    if not cleaned:
        return ""
    parsed = urlparse(cleaned)
    host = (parsed.netloc or "").lower().removeprefix("www.")
    path = (parsed.path or "").rstrip("/")
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{host}{path}{query}".lower()


def _unescape_markdown(text: str) -> str:
    r"""Drop backslash escapes (\[, \#, \+) so link syntax matches cleanly."""
    return MARKDOWN_ESCAPE_RE.sub(r"\1", text or "")


def _plain_text(line: str) -> str:
    """A markdown line as readable text: links become their label, markup and escapes go."""
    text = MARKDOWN_LINK_RE.sub(lambda m: m.group(1).strip(), line)
    text = BARE_URL_RE.sub("", text)
    text = MARKDOWN_ESCAPE_RE.sub(r"\1", text)
    text = LIST_MARKER_RE.sub("", text)
    text = re.sub(r"^\s*#+\s*", "", text)
    text = text.replace("**", "").replace("__", "")
    # Leftover brackets from escaped link wrappers like \[[IG](url)\].
    text = text.replace("[", " ").replace("]", " ")
    return " ".join(text.split()).strip(" :-–—")


def _text_without_links(line: str) -> str:
    """The line's own words, with every link and link label removed."""
    # Markdown links first: stripping bare URLs first would break the markdown match.
    without_links = BARE_URL_RE.sub(" ", MARKDOWN_LINK_RE.sub(" ", line))
    return _plain_text(without_links)


def _line_links(line: str) -> list[tuple[str, str]]:
    """(label, url) for every link on a line, markdown links first then bare URLs."""
    links: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in MARKDOWN_LINK_RE.finditer(line):
        label, url = match.group(1).strip(), match.group(2).strip()
        if url not in seen:
            seen.add(url)
            links.append((_plain_text(label), url))
    without_markdown = MARKDOWN_LINK_RE.sub(" ", line)
    for match in BARE_URL_RE.finditer(without_markdown):
        url = match.group(0)
        if url not in seen:
            seen.add(url)
            links.append(("", url))
    return links


def _is_lead_in(text: str) -> bool:
    """True for a line that introduces a link rather than titling it (e.g. "@aaronparnas:")."""
    if not text:
        return True
    return len(text) < HEADLINE_MIN_CHARS or len(text.split()) < 3


def _is_section_heading(line: str, has_links: bool) -> bool:
    """Markdown heading, or a standalone bold line that is not a list item."""
    if has_links:
        return False
    stripped = line.strip()
    if not stripped or LIST_MARKER_RE.match(line):
        return False
    if stripped.startswith("#"):
        return True
    return bool(re.fullmatch(r"\*\*.+\*\*", stripped))


def _parse_link_candidates(text: str) -> list[dict]:
    """Every distinct link in the document, paired with the document's own wording.

    headline is the document's text for the item, copied as written. note holds a
    lead-in like "@aaronparnas" when the link sits on its own line under a headline.
    """
    candidates: list[dict] = []
    seen: set[str] = set()
    heading = ""
    recent: list[str] = []

    for raw_line in _unescape_markdown(text).splitlines():
        links = _line_links(raw_line)
        own_text = _text_without_links(raw_line)

        if not links:
            if _is_section_heading(raw_line, has_links=False):
                heading = own_text
                recent = []
            elif own_text:
                recent = [*recent, own_text][-3:]
            continue

        # A line carrying its own headline keeps it; a bare "@handle: [IG]" line
        # borrows the nearest headline-shaped line above it (its parent bullet).
        if not _is_lead_in(own_text):
            headline, note = own_text, ""
        else:
            parent = next((line for line in reversed(recent) if not _is_lead_in(line)), "")
            headline, note = (parent or heading), own_text

        for label, url in links:
            canonical = _canonical_url(url)
            key = _normalize_url(canonical)
            if not canonical or not _is_http_url(canonical) or not key or key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "url": canonical,
                    "label": label,
                    "headline": (headline or label or canonical)[:HEADLINE_MAX_CHARS],
                    "note": note[:HEADLINE_MAX_CHARS],
                }
            )
        if own_text:
            recent = [*recent, own_text][-3:]

    return candidates


def _extract_items(text: str) -> tuple[list[dict], int]:
    """Return (items, skipped_count) for a pasted document.

    Every link is listed with the document's own wording, except links on the
    blocked platforms. Nothing is summarized, reworded, or sent anywhere.
    """
    items: list[dict] = []
    skipped = 0
    for candidate in _parse_link_candidates(text):
        if _is_blocked_url(candidate["url"]):
            skipped += 1
            continue
        items.append(
            {
                "headline": candidate["headline"],
                "description": candidate["note"],
                "url": candidate["url"],
            }
        )
    return items, skipped


def _split_doc_tabs(markdown: str) -> list[dict]:
    """Split a Google Doc markdown export into its tabs.

    Each `# ` heading starts a tab and its text is everything up to the next one.
    Anything before the first heading becomes a leading "(start of document)" entry
    so no content is ever hidden.
    """
    lines = (markdown or "").splitlines()
    starts = [(i, DOC_TAB_HEADING_RE.match(line)) for i, line in enumerate(lines)]
    headings = [(i, match.group(1).strip()) for i, match in starts if match]
    if not headings:
        return [{"title": "(whole document)", "text": markdown or ""}]

    tabs: list[dict] = []
    preamble = "\n".join(lines[: headings[0][0]]).strip()
    if preamble:
        tabs.append({"title": "(start of document)", "text": preamble})

    bounds = [i for i, _ in headings] + [len(lines)]
    for position, (start, title) in enumerate(headings):
        body = "\n".join(lines[start + 1 : bounds[position + 1]]).strip()
        tabs.append({"title": _plain_text(title) or title, "text": body})
    return tabs


def _tab_date(title: str) -> date | None:
    """The date in a tab title like "8/14/26", if there is one."""
    match = re.search(r"\d{1,4}[/-]\d{1,2}([/-]\d{2,4})?", title or "")
    if match:
        candidate = match.group(0)
        for fmt in TAB_DATE_FORMATS:
            try:
                return datetime.strptime(candidate, fmt).date()
            except ValueError:
                continue
        # A bare month/day such as "8/14" — assume the current year.
        try:
            month, day = candidate.replace("-", "/").split("/")[:2]
            return date(date.today().year, int(month), int(day))
        except (ValueError, IndexError):
            return None
    for fmt in TAB_DATE_FORMATS:
        try:
            return datetime.strptime(title.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _newest_tab_index(tabs: list[dict]) -> int:
    """Index of the latest tab: the newest parsable date, else the first tab."""
    dated = [(index, _tab_date(tab["title"])) for index, tab in enumerate(tabs)]
    dated = [(index, value) for index, value in dated if value is not None]
    if len(dated) >= 2:
        return max(dated, key=lambda pair: pair[1])[0]
    return 0


def _match_key(text: str) -> str:
    """Loose comparison key for matching highlighted text against a headline."""
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _highlighted_texts(doc_html: str) -> list[str]:
    """The text of every highlighted run in a Google Doc HTML export.

    Google highlights the item's own wording rather than the link, so these are matched
    against item headlines instead of URLs.
    """
    found: list[str] = []
    for color, inner in HIGHLIGHT_SPAN_RE.findall(doc_html or ""):
        if color.strip().lower() in UNHIGHLIGHTED_COLORS:
            continue
        text = " ".join(html.unescape(HTML_TAG_RE.sub("", inner)).split()).strip()
        if len(text) >= HIGHLIGHT_MIN_CHARS:
            found.append(text)
    return found


def _mark_highlighted(items: list[dict], highlighted_texts: list[str]) -> None:
    """Flag items whose headline was highlighted in the source document."""
    keys = [_match_key(text) for text in highlighted_texts]
    keys = [key for key in keys if key]
    for item in items:
        headline_key = _match_key(item["headline"])
        item["highlighted"] = bool(headline_key) and any(
            headline_key in key or key in headline_key for key in keys
        )


def _attach_tab_highlights(tabs: list[dict], doc_html: str) -> None:
    """Give each tab the highlighted text found in its own slice of the HTML export.

    Highlights must be scoped per tab: these documents repeat headlines from one day
    to the next, so a doc-wide list flags items whose twin was highlighted elsewhere.
    A tab whose title cannot be located in the HTML gets no highlights rather than
    borrowing another tab's.
    """
    cursor = 0
    starts: list[int | None] = []
    for tab in tabs:
        found = doc_html.find(tab["title"], cursor) if tab["title"] else -1
        if found == -1:
            starts.append(None)
        else:
            starts.append(found)
            cursor = found + 1

    for index, tab in enumerate(tabs):
        start = starts[index]
        if start is None:
            tab["highlights"] = []
            continue
        later = [value for value in starts[index + 1 :] if value is not None]
        end = min(later) if later else len(doc_html)
        tab["highlights"] = _highlighted_texts(doc_html[start:end])


@st.cache_data(ttl=300, show_spinner="Opening the document…")
def _load_doc_tabs(url: str) -> tuple[str, list[dict]]:
    """Export a Google Doc and split it into tabs, each with its highlighted text.

    Cached so switching tabs is instant. The HTML export is a second call, made only
    for the highlight styling that the markdown export drops.
    """
    doc_name, markdown = export_google_doc_markdown(url)
    tabs = _split_doc_tabs(markdown)
    try:
        _, doc_html = export_google_doc_html(url)
        _attach_tab_highlights(tabs, doc_html)
    except Exception:
        # Highlights are a nice-to-have; never block reading the document over them.
        for tab in tabs:
            tab.setdefault("highlights", [])
    return doc_name, tabs


def _existing_sheet_urls() -> set[str]:
    """Normalized URLs already on the posts tab, so duplicates can be flagged."""
    try:
        rows = get_all_rows(GOOGLE_SHEET_ID)
    except Exception:
        return set()
    return {
        key
        for key in (_normalize_url(row.get("Instagram URL", "")) for row in rows)
        if key
    }


def _pick_key(index: int) -> str:
    return f"ingest_pick_{st.session_state.get('ingest_result_token', '')}_{index}"


def _set_all_picks(items: list[dict], value: bool) -> None:
    for index, item in enumerate(items):
        if value and item.get("already_in_sheet"):
            continue
        st.session_state[_pick_key(index)] = value


def _row_text(item: dict) -> str:
    """One pipe-separated block: headline, link, then any notes."""
    parts = [f"**{item['headline'] or item['url']}**", item["url"]]
    if item["description"]:
        parts.append(item["description"])
    if item.get("highlighted"):
        parts.append("🟡 highlighted")
    if item.get("already_in_sheet"):
        parts.append("already in the sheet")
    return " | ".join(parts)


st.set_page_config(page_title="Ingest", page_icon="📥", layout="centered")
inject_styles()
st.title("Ingest")
st.caption(
    "Paste a document from a client to list every link in it with the document's own wording, "
    "then add the ones you pick to the posts sheet with that client's hashtag. "
    "Twitter/X, Threads, and Reddit links are left out."
)

if not require_auth():
    st.stop()

# Seeded before the widget exists: a widget's state cannot be assigned afterwards.
if st.session_state.pop("_ingest_reset_section", False) or (
    st.session_state.get("ingest_section_tab") not in {"Home", "Ingest", "Substack"}
):
    st.session_state["ingest_section_tab"] = "Ingest"

section_tab = st.segmented_control(
    "Workspace section",
    ["Home", "Ingest", "Substack"],
    key="ingest_section_tab",
    label_visibility="collapsed",
    width="stretch",
)
if section_tab in {"Home", "Substack"}:
    # Both live on the workspace page. Queue this control back to Ingest for the next
    # visit rather than assigning it now, which the widget no longer allows.
    st.session_state["_ingest_reset_section"] = True
    st.session_state["_workspace_pending_tab"] = section_tab
    st.switch_page("pages/workspace.py")

try:
    doc_presets = get_doc_presets(GOOGLE_SHEET_ID)
except Exception as e:
    doc_presets = []
    st.error(f"Could not read the docs tab: {describe_error(e)}")

doc_labels = [preset["label"] for preset in doc_presets]
selected_doc_label = st.selectbox(
    "Document",
    doc_labels,
    index=None,
    key="ingest_doc_label",
    placeholder="None — paste text below instead",
    disabled=not doc_labels,
    help="Comes from the docs tab of the Google Sheet (column A: client, B: Google Doc link, C: hashtag).",
)
selected_doc = next(
    (preset for preset in doc_presets if preset["label"] == selected_doc_label),
    None,
)

tabs: list[dict] = []
doc_name = ""
if selected_doc:
    try:
        doc_name, tabs = _load_doc_tabs(selected_doc["url"])
    except Exception as e:
        st.error(f"Could not open that document: {describe_error(e)}")

selected_tab_text = ""
highlighted_texts: list[str] = []
if tabs:
    tab_titles = [tab["title"] for tab in tabs]
    if st.session_state.get("ingest_doc_tab") not in tab_titles:
        st.session_state["ingest_doc_tab"] = tab_titles[_newest_tab_index(tabs)]
    tab_col, reload_col = st.columns([4, 1], vertical_alignment="bottom")
    with tab_col:
        selected_tab_title = st.selectbox(
            "Tab",
            tab_titles,
            key="ingest_doc_tab",
            help="Defaults to the most recent tab by date.",
        )
    with reload_col:
        if st.button("Reload", key="ingest_reload_doc", width="stretch", help="Re-read the document from Drive."):
            _load_doc_tabs.clear()
            st.session_state.pop("ingest_items", None)
            st.rerun()
    selected_tab = next((tab for tab in tabs if tab["title"] == selected_tab_title), None)
    if selected_tab:
        selected_tab_text = selected_tab["text"]
        highlighted_texts = selected_tab.get("highlights") or []

# A document carries its own hashtag (docs column C); only ask when it cannot.
selected_hashtags = (selected_doc or {}).get("hashtags", "")
if not selected_hashtags:
    try:
        presets = get_hashtag_presets(GOOGLE_SHEET_ID)
    except Exception as e:
        presets = []
        st.error(f"Could not read the hashtags tab: {describe_error(e)}")

    if not presets:
        st.warning(
            "No hashtag presets found. Add rows to the `hashtags` tab of the Google Sheet "
            "(column A: client label, column B: hashtag) and reload this page."
        )

    preset_labels = [preset["label"] for preset in presets]
    selected_label = st.selectbox(
        "Hashtag",
        preset_labels,
        index=0 if preset_labels else None,
        key="ingest_hashtag_label",
        placeholder="No hashtags available",
        disabled=not preset_labels,
        help="Comes from the hashtags tab of the Google Sheet. Applies to every link you add below.",
    )
    selected_hashtags = next(
        (preset["hashtags"] for preset in presets if preset["label"] == selected_label),
        "",
    )

document_text = ""
if not selected_doc:
    document_text = st.text_area(
        "Document text",
        key="ingest_document_text",
        height=260,
        placeholder="Paste a document, email, or list of links here…",
    )

# The selected doc tab wins; the paste box only exists when no document is chosen.
source_text = selected_tab_text or document_text

if st.button(
    "Find items in this tab" if tabs else "Find items",
    key="ingest_extract",
    type="primary",
    width="stretch",
    disabled=not source_text.strip(),
):
    st.session_state.pop("ingest_items", None)
    st.session_state.pop("ingest_added", None)
    st.session_state.pop("ingest_error", None)
    try:
        found, skipped = _extract_items(source_text)
    except Exception as e:
        st.session_state["ingest_error"] = describe_error(e)
    else:
        existing = _existing_sheet_urls()
        for item in found:
            item["already_in_sheet"] = _normalize_url(item["url"]) in existing
        _mark_highlighted(found, highlighted_texts)
        st.session_state["ingest_items"] = found
        st.session_state["ingest_skipped"] = skipped
        st.session_state["ingest_result_token"] = hashlib.sha1(
            source_text.strip().encode("utf-8")
        ).hexdigest()[:10]
    st.rerun()

error_message = st.session_state.get("ingest_error")
if error_message:
    st.error(f"Could not read that document: {error_message}")

added = st.session_state.get("ingest_added")
if added:
    st.success(
        f"Added {added['count']} link{'s' if added['count'] != 1 else ''} to the posts sheet"
        + (f" with {added['hashtags']}." if added["hashtags"] else ".")
    )

items = st.session_state.get("ingest_items")
if items is not None:
    skipped = st.session_state.get("ingest_skipped", 0)
    if not items:
        st.info(
            "No links were found in that text."
            + (f" {skipped} Twitter/X, Threads, or Reddit link(s) were skipped." if skipped else "")
        )
    else:
        st.divider()
        remaining = [item for item in items if not item.get("added")]
        st.subheader(f"{len(remaining)} item{'s' if len(remaining) != 1 else ''} found")
        if skipped:
            st.caption(f"{skipped} Twitter/X, Threads, or Reddit link(s) were skipped.")

        select_col, clear_col = st.columns(2)
        with select_col:
            if st.button("Select all", key="ingest_select_all", width="stretch"):
                _set_all_picks(items, True)
                st.rerun()
        with clear_col:
            if st.button("Clear all", key="ingest_clear_all", width="stretch"):
                _set_all_picks(items, False)
                st.rerun()

        # One table: checkbox column on the left, one text block per row on the right.
        # Nothing starts checked — the widget default is False, so no seeding here.
        with st.container(border=True):
            for index, item in enumerate(items):
                if item.get("added"):
                    continue
                check_col, text_col = st.columns([1, 20], vertical_alignment="center")
                with check_col:
                    st.checkbox(
                        item["headline"] or item["url"],
                        key=_pick_key(index),
                        label_visibility="collapsed",
                    )
                with text_col:
                    st.markdown(_row_text(item))

        picked = [
            i
            for i, item in enumerate(items)
            if not item.get("added") and st.session_state.get(_pick_key(i))
        ]
        st.divider()
        if st.button(
            f"Add {len(picked)} to Google Sheets" if picked else "Add to Google Sheets",
            key="ingest_add_to_sheet",
            type="primary",
            width="stretch",
            disabled=not picked or not selected_hashtags,
        ):
            urls = [items[i]["url"] for i in picked]
            try:
                append_link_rows(GOOGLE_SHEET_ID, urls, selected_hashtags)
            except Exception as e:
                st.error(f"Could not add to the posts sheet: {describe_error(e)}")
            else:
                for i in picked:
                    items[i]["added"] = True
                    st.session_state.pop(_pick_key(i), None)
                st.session_state["ingest_items"] = items
                st.session_state["ingest_added"] = {
                    "count": len(urls),
                    "hashtags": selected_hashtags,
                }
                st.rerun()

        if not selected_hashtags:
            st.caption("Pick a hashtag above before adding.")
