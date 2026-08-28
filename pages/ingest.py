"""Ingest page: read a client document back in full, tick the links you want, add them to the posts sheet.

No AI is involved and nothing is rewritten or summarized. The document is shown back
line for line, exactly as written, with every http(s) URL lifted onto a line of its own
and given a checkbox — so nothing in a long document can be silently dropped, and the
links stay easy to hit. Links on platforms that are never posted from (Twitter/X,
Threads, Reddit) and links to Google Docs/Drive are still shown, just without a checkbox.

Pasted rich text is accepted as HTML, because a plain-text paste from Gmail drops every
href and leaves link text like "IG" with nothing behind it.
"""

import hashlib
import html
import os
import re
import sys
from datetime import date, datetime
from html.parser import HTMLParser
from urllib.parse import parse_qs, unquote, urlparse, urlunparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config import GOOGLE_SHEET_ID
from drive import export_google_doc_markdown_and_html, extract_drive_file_id
from sheets import append_link_rows, get_all_rows, get_doc_presets, get_hashtag_presets
from utils.auth import require_auth
from utils.error_labels import describe_error
from utils.styles import inject as inject_styles

# Shorter than this, a line is a lead-in (e.g. "@aaronparnas:") rather than a headline.
HEADLINE_MIN_CHARS = 20

MARKDOWN_LINK_RE = re.compile(r"\[([^\]\n]*)\]\(\s*(https?://[^\s)]+?)\s*\)")
BARE_URL_RE = re.compile(r"https?://[^\s<>()\[\]\"'\\]+")
LIST_MARKER_RE = re.compile(r"^\s*(?:[*\-+•]|\d+[.)])\s+")
MARKDOWN_ESCAPE_RE = re.compile(r"\\([\\`*_{}\[\]()#+\-.!|>~])")
# The few characters Streamlit would read as markup when the document is shown back.
MARKDOWN_SPECIAL_RE = re.compile(r"([\\`*_\[\]#])")
# Instagram query strings (?hl=en, ?img_index=1) point at the same post.
INSTAGRAM_HOSTS = ("instagram.com",)
# Never posted from, so these are dropped on sight: platforms that are not used, plus
# Google Docs/Drive links, which are the source documents themselves rather than items.
BLOCKED_HOSTS = (
    "twitter.com",
    "x.com",
    "t.co",
    "threads.com",
    "threads.net",
    "reddit.com",
    "docs.google.com",
    "drive.google.com",
)
BLOCKED_LABEL = "Twitter/X, Threads, Reddit, or Google Docs/Drive"

# A highlighted run in the HTML export is a span with a non-white background colour.
HIGHLIGHT_SPAN_RE = re.compile(
    r"<span[^>]*background-color:\s*([^;\"']+)[^>]*>(.*?)</span>", re.IGNORECASE | re.DOTALL
)
HTML_TAG_RE = re.compile(r"<[^>]+>")
UNHIGHLIGHTED_COLORS = {"#ffffff", "#fff", "transparent", "white", "none", "initial", "inherit"}
# Last entry in the Document dropdown: type a link instead of using the docs tab.
CUSTOM_DOC_OPTION = "Enter your own URL"

# Pasted rich text (from Gmail, a Doc, a web page) arrives as HTML rather than markdown.
HTML_SOURCE_RE = re.compile(r"<\s*(a|div|p|span|table|br|html|body|ul|ol|li)\b", re.IGNORECASE)
HTML_BLOCK_TAGS = {
    "p", "div", "li", "tr", "td", "blockquote", "section", "article", "ul", "ol", "table",
}
HTML_HEADING_RE = re.compile(r"^h([1-6])$")
HTML_SKIP_TAGS = {"style", "script", "head", "title", "meta", "link"}
# Kept as markdown bold so a pasted item headline still reads as a headline.
HTML_BOLD_TAGS = {"b", "strong"}
RGB_RE = re.compile(r"rgba?\(\s*(\d+)[,\s]+(\d+)[,\s]+(\d+)", re.IGNORECASE)
# Below this length a highlighted run is a fragment (a heading, a word) and matching
# it against headlines would produce false positives.
HIGHLIGHT_MIN_CHARS = 25
# A line of wording must be at least this distinctive before a highlight can match it,
# so a stray word does not test as "contained in" some unrelated highlighted sentence.
HIGHLIGHT_MATCH_MIN_CHARS = 12

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
    """Links on platforms that are never posted from, or to the source docs themselves."""
    return _host_matches(url, BLOCKED_HOSTS)


def _unwrap_redirect(url: str) -> str:
    """Gmail and Doc HTML route links through google.com/url?q=<real link>."""
    cleaned = (url or "").strip()
    parsed = urlparse(cleaned)
    if parsed.netloc.lower().endswith("google.com") and parsed.path.rstrip("/") == "/url":
        target = parse_qs(parsed.query).get("q", [""])[0]
        if target:
            return unquote(target)
    return cleaned


def _canonical_url(url: str) -> str:
    """Trim trailing punctuation, and drop tracking query strings from Instagram links."""
    cleaned = _unwrap_redirect(url).strip().rstrip(").,;:\"'>]}")
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
    """A markdown heading line, which titles a section rather than an item.

    Bold alone does not qualify: in these documents the item headlines are bold too,
    and treating them as section titles is what used to make them disappear.
    """
    if has_links:
        return False
    stripped = line.strip()
    if not stripped or LIST_MARKER_RE.match(line):
        return False
    return stripped.startswith("#")


def _is_bold_line(line: str) -> bool:
    """True when the line's own wording is entirely bold, as item headlines are."""
    core = LIST_MARKER_RE.sub("", MARKDOWN_LINK_RE.sub(" ", line)).strip()
    core = re.sub(r"^#+\s*", "", core).strip()
    return bool(re.fullmatch(r"\*\*.+\*\*", core))


def _document_blocks(text: str) -> list[dict]:
    """The whole document read back in order, as text lines and one row per link.

    Nothing is dropped: every line of wording is kept verbatim as a text block, and
    every link becomes its own block on its own line so it is easy to tick. Links that
    are blocked or already listed above are still shown, just without a checkbox.
    """
    if _looks_like_html(text):
        text = _html_to_markdownish(text)

    blocks: list[dict] = []
    seen: set[str] = set()
    link_index = 0

    for raw_line in _unescape_markdown(text).splitlines():
        links = _line_links(raw_line)
        own_text = _text_without_links(raw_line)

        if own_text:
            blocks.append(
                {
                    "kind": "text",
                    "text": own_text,
                    "heading": _is_section_heading(raw_line, has_links=bool(links)),
                    "bold": _is_bold_line(raw_line),
                    "bullet": bool(LIST_MARKER_RE.match(raw_line)),
                    "lead_in": _is_lead_in(own_text),
                }
            )

        for label, url in links:
            canonical = _canonical_url(url)
            if not canonical or not _is_http_url(canonical):
                continue
            key = _normalize_url(canonical)
            duplicate = bool(key) and key in seen
            if key:
                seen.add(key)
            blocks.append(
                {
                    "kind": "link",
                    "index": link_index,
                    "label": label,
                    "url": canonical,
                    "blocked": _is_blocked_url(canonical),
                    "duplicate": duplicate,
                }
            )
            link_index += 1

    return blocks


def _is_selectable(block: dict) -> bool:
    """True for a link that can be ticked and added to the sheet."""
    return (
        block["kind"] == "link"
        and not block["blocked"]
        and not block["duplicate"]
        and not block.get("added")
    )


def _link_blocks(blocks: list[dict]) -> list[dict]:
    return [block for block in blocks if block["kind"] == "link"]


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
    """Loose comparison key for matching highlighted text against a line of wording."""
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _is_highlight_color(value: str) -> bool:
    """True for a background colour that reads as a highlight rather than plain paper.

    Handles both the hex Google's export writes and the rgb() a rich-text paste produces.
    """
    cleaned = (value or "").strip().lower().rstrip(";")
    if not cleaned or cleaned in UNHIGHLIGHTED_COLORS:
        return False
    rgb = RGB_RE.match(cleaned)
    if rgb:
        channels = [int(part) for part in rgb.groups()]
    elif cleaned.startswith("#"):
        digits = cleaned[1:]
        if len(digits) == 3:
            digits = "".join(char * 2 for char in digits)
        if len(digits) < 6:
            return False
        try:
            channels = [int(digits[i : i + 2], 16) for i in (0, 2, 4)]
        except ValueError:
            return False
    else:
        # A named colour other than the plain ones listed above.
        return True
    return not all(channel >= 250 for channel in channels)


def _highlighted_texts(source_html: str) -> list[str]:
    """The text of every highlighted run in an HTML document.

    Highlighting lands on the item's own wording rather than on the link, so these are
    matched against the document's lines of text instead of URLs.
    """
    found: list[str] = []
    for color, inner in HIGHLIGHT_SPAN_RE.findall(source_html or ""):
        if not _is_highlight_color(color):
            continue
        text = " ".join(html.unescape(HTML_TAG_RE.sub("", inner)).split()).strip()
        if len(text) >= HIGHLIGHT_MIN_CHARS:
            found.append(text)
    return found


class _MarkdownishHTMLParser(HTMLParser):
    """Rewrites HTML into the bulleted, `[label](url)` shape the link parser reads.

    Pasted rich text keeps its hrefs, which a plain-text paste loses entirely — link
    text like "IG" arrives with no URL behind it.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_depth = 0
        self.href_stack: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in HTML_SKIP_TAGS:
            self.skip_depth += 1
            return
        if self.skip_depth:
            return
        heading = HTML_HEADING_RE.match(tag)
        if tag == "br":
            self.parts.append("\n")
        elif tag == "li":
            self.parts.append("\n* ")
        elif heading:
            self.parts.append("\n\n" + "#" * int(heading.group(1)) + " ")
        elif tag in HTML_BLOCK_TAGS:
            self.parts.append("\n")
        elif tag in HTML_BOLD_TAGS:
            self.parts.append("**")
        if tag == "a":
            href = (dict(attrs).get("href") or "").strip()
            self.href_stack.append(href)
            self.parts.append("[")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in HTML_SKIP_TAGS:
            self.skip_depth = max(0, self.skip_depth - 1)
            return
        if self.skip_depth:
            return
        if tag == "a":
            href = self.href_stack.pop() if self.href_stack else ""
            self.parts.append(f"]({href})" if href else "]")
        elif tag in HTML_BOLD_TAGS:
            self.parts.append("**")
        elif tag in HTML_BLOCK_TAGS or HTML_HEADING_RE.match(tag):
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self.skip_depth:
            return
        self.parts.append(data.replace("\n", " "))


def _looks_like_html(source: str) -> bool:
    return bool(HTML_SOURCE_RE.search(source or ""))


def _html_to_markdownish(source: str) -> str:
    """Flatten pasted HTML into text the markdown link parser can read."""
    parser = _MarkdownishHTMLParser()
    parser.feed(source or "")
    parser.close()
    text = re.sub(r"\n{3,}", "\n\n", "".join(parser.parts))
    return "\n".join(" ".join(line.split()) for line in text.splitlines()).strip()


def _mark_highlighted(blocks: list[dict], highlighted_texts: list[str]) -> None:
    """Flag the wording that was highlighted in the source, and the links under it.

    Highlighting lands on an item's own words rather than on its links, so a link
    inherits the flag from the highlighted line it sits beneath. A plain line of
    wording clears it again; a short lead-in like "@aaronparnas:" does not.
    """
    keys = [key for key in (_match_key(text) for text in highlighted_texts) if key]
    inherited = False
    for block in blocks:
        if block["kind"] != "text":
            block["highlighted"] = inherited
            continue
        key = _match_key(block["text"])
        block["highlighted"] = (
            not block["heading"]
            and len(key) >= HIGHLIGHT_MATCH_MIN_CHARS
            and any(key in other or other in key for other in keys)
        )
        if block["highlighted"]:
            inherited = True
        elif not block["lead_in"]:
            inherited = False


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


@st.cache_data(ttl=1800, show_spinner="Opening the document…")
def _load_doc_tabs(url: str) -> tuple[str, list[dict]]:
    """Export a Google Doc and split it into tabs, each with its highlighted text.

    Both exports come from one parallel call. Cached for half an hour, so switching
    tabs and reopening a document are instant; Reload re-reads it from Drive.
    """
    doc_name, markdown, doc_html = export_google_doc_markdown_and_html(url)
    tabs = _split_doc_tabs(markdown)
    if doc_html:
        try:
            _attach_tab_highlights(tabs, doc_html)
        except Exception:
            # Highlights are a nice-to-have; never block reading the document over them.
            pass
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


def _reel_key(index: int) -> str:
    return f"ingest_reel_{st.session_state.get('ingest_result_token', '')}_{index}"


def _is_reel_selectable(block: dict) -> bool:
    """The 'reel' tick is offered on Instagram links only."""
    return _is_selectable(block) and _is_instagram_url(block["url"])


def _is_picked(block: dict) -> bool:
    """Ticking 'reel' adds the link too, so a reel can be marked in one click."""
    return bool(
        st.session_state.get(_pick_key(block["index"]))
        or st.session_state.get(_reel_key(block["index"]))
    )


def _set_all_picks(blocks: list[dict], value: bool) -> None:
    for block in _link_blocks(blocks):
        if not _is_selectable(block):
            continue
        if value and block.get("already_in_sheet"):
            continue
        st.session_state[_pick_key(block["index"])] = value
        # Clearing has to drop the reel ticks as well, or they would keep the link picked.
        if not value and _is_reel_selectable(block):
            st.session_state[_reel_key(block["index"])] = False


def _rich_paste_available() -> bool:
    """Whether the rich-text paste box can be shown at all."""
    try:
        from streamlit_quill import st_quill  # noqa: F401
    except Exception:
        return False
    return True


def _rich_paste_box() -> str:
    """A rich-text box that keeps hyperlinks when pasting from Gmail or a doc.

    Returns HTML. A plain text area would drop every href, leaving link text like "IG"
    with nothing behind it.
    """
    from streamlit_quill import st_quill

    return st_quill(
        html=True,
        # False disables Quill's toolbar; an empty list renders it as a blank grey bar.
        toolbar=False,
        placeholder="Paste a document, email, or list of links here…",
        key="ingest_document_rich",
    ) or ""


def _escape_markdown(text: str) -> str:
    """Show the document's wording as written, without Streamlit reading it as markup."""
    return MARKDOWN_SPECIAL_RE.sub(r"\\\1", text or "")


def _text_row(block: dict) -> str:
    """A line of the document's own wording, styled roughly as the document styles it."""
    body = _escape_markdown(block["text"])
    if block["heading"] or block["bold"]:
        body = f"**{body}**"
    if block["bullet"]:
        body = f"• {body}"
    if block.get("highlighted"):
        body = f"{body} 🟡"
    return body


def _link_row(block: dict) -> str:
    """One link on its own line: its label in the document, the URL, then any notes."""
    parts = []
    if block["label"]:
        parts.append(f"**{_escape_markdown(block['label'])}**")
    parts.append(block["url"])
    if block.get("highlighted"):
        parts.append("🟡 highlighted")
    if block["blocked"]:
        parts.append(f"not used ({BLOCKED_LABEL})")
    if block["duplicate"]:
        parts.append("same link as above")
    if block.get("added"):
        parts.append("✅ added")
    elif block.get("already_in_sheet"):
        parts.append("already in the sheet")
    return " | ".join(parts)


st.set_page_config(page_title="Ingest", page_icon="📥", layout="centered")
inject_styles()
st.title("Ingest")
st.caption(
    "Paste a document from a client to read it back in full, with a checkbox beside every "
    "link, then add the ones you pick to the posts sheet with that client's hashtag. "
    "Twitter/X, Threads, Reddit, and Google Docs/Drive links are shown but cannot be picked."
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
    [*doc_labels, CUSTOM_DOC_OPTION],
    index=None,
    key="ingest_doc_label",
    placeholder="None — paste text below instead",
    help=(
        "Comes from the docs tab of the Google Sheet (column A: client, B: Google Doc "
        "link, C: hashtag, D: comment link)."
    ),
)
selected_doc = next(
    (preset for preset in doc_presets if preset["label"] == selected_doc_label),
    None,
)

# A one-off link typed in rather than kept on the docs tab.
custom_doc_url = ""
if selected_doc_label == CUSTOM_DOC_OPTION:
    custom_doc_url = st.text_input(
        "Document URL",
        key="ingest_custom_doc_url",
        placeholder="https://docs.google.com/document/d/…",
        help="Any Google Doc you can open. Nothing needs sharing with the service account.",
    ).strip()

doc_to_load = selected_doc["url"] if selected_doc else custom_doc_url
if doc_to_load and not extract_drive_file_id(doc_to_load):
    # Without this the whole URL is sent to Drive as a file id and comes back a raw 404.
    st.error("That does not look like a Google Doc link — it should contain `/d/<id>/`.")
    doc_to_load = ""

tabs: list[dict] = []
doc_name = ""
if doc_to_load:
    try:
        doc_name, tabs = _load_doc_tabs(doc_to_load)
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
            st.session_state.pop("ingest_blocks", None)
            st.rerun()
    selected_tab = next((tab for tab in tabs if tab["title"] == selected_tab_title), None)
    if selected_tab:
        selected_tab_text = selected_tab["text"]
        highlighted_texts = selected_tab.get("highlights") or []

# A document can carry its own comment link (docs column D). Every link added from that
# document gets it as the Top Comment, which the caption step turns into the "Comment
# LINK" CTA when it is a bare URL.
selected_comment_link = (selected_doc or {}).get("comment_link", "")
if selected_doc and selected_comment_link:
    st.caption(f"Comment link for this document: {selected_comment_link}")

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
if selected_doc_label is None:
    if _rich_paste_available():
        st.caption(
            "Paste straight from Gmail or a doc — formatting is kept, so links behind text "
            "like “IG” survive. Plain text works too."
        )
        document_text = _rich_paste_box() or ""
    else:
        document_text = st.text_area(
            "Document text",
            key="ingest_document_text",
            height=260,
            placeholder="Paste a document, email, or list of links here…",
        )

# The selected doc tab wins; the paste box only exists when no document is chosen.
source_text = selected_tab_text or document_text

# Pasted rich text carries its own highlighting, the same as a Doc export does.
if not selected_tab_text and _looks_like_html(source_text):
    highlighted_texts = _highlighted_texts(source_text)

if st.button(
    "Read this tab" if tabs else "Read this document",
    key="ingest_extract",
    type="primary",
    width="stretch",
    disabled=not source_text.strip(),
):
    st.session_state.pop("ingest_blocks", None)
    st.session_state.pop("ingest_added", None)
    st.session_state.pop("ingest_error", None)
    try:
        blocks = _document_blocks(source_text)
    except Exception as e:
        st.session_state["ingest_error"] = describe_error(e)
    else:
        existing = _existing_sheet_urls()
        for block in _link_blocks(blocks):
            block["already_in_sheet"] = _normalize_url(block["url"]) in existing
        _mark_highlighted(blocks, highlighted_texts)
        st.session_state["ingest_blocks"] = blocks
        st.session_state["ingest_result_token"] = hashlib.sha1(
            source_text.strip().encode("utf-8")
        ).hexdigest()[:10]
    st.rerun()

error_message = st.session_state.get("ingest_error")
if error_message:
    st.error(f"Could not read that document: {error_message}")

added = st.session_state.get("ingest_added")
if added:
    message = (
        f"Added {added['count']} link{'s' if added['count'] != 1 else ''} to the posts sheet"
        + (f" with {added['hashtags']}." if added["hashtags"] else ".")
    )
    if added.get("comment_link"):
        message += f" Top comment set to {added['comment_link']}."
    st.success(message)

blocks = st.session_state.get("ingest_blocks")
if blocks is not None:
    links = _link_blocks(blocks)
    selectable = [block for block in links if _is_selectable(block)]
    if not links:
        st.info("No links were found in that text.")
    else:
        st.divider()
        st.subheader(f"{len(selectable)} link{'s' if len(selectable) != 1 else ''} to pick from")
        st.caption(
            "The document is shown back in full, with a checkbox beside every link that can "
            f"be added. {BLOCKED_LABEL} links are listed without one. Tick **reel** on an "
            "Instagram link to have it transcribed as a reel even if it is not a /reel/ link."
        )

        select_col, clear_col = st.columns(2)
        with select_col:
            if st.button("Select all", key="ingest_select_all", width="stretch"):
                _set_all_picks(blocks, True)
                st.rerun()
        with clear_col:
            if st.button("Clear all", key="ingest_clear_all", width="stretch"):
                _set_all_picks(blocks, False)
                st.rerun()

        # The document, in order: wording on its own lines, then each link on a line of
        # its own with a checkbox in the left gutter. Text lines keep that gutter empty
        # so everything stays aligned. Nothing starts checked — the widget default is
        # False, so no seeding here.
        with st.container(border=True):
            for block in blocks:
                check_col, reel_col, text_col = st.columns(
                    [1, 3, 17], vertical_alignment="center"
                )
                if block["kind"] == "text":
                    with text_col:
                        st.markdown(_text_row(block))
                    continue
                with check_col:
                    if _is_selectable(block):
                        st.checkbox(
                            block["url"],
                            key=_pick_key(block["index"]),
                            label_visibility="collapsed",
                        )
                with reel_col:
                    if _is_reel_selectable(block):
                        st.checkbox(
                            "reel",
                            key=_reel_key(block["index"]),
                            help=(
                                "Process this link as a reel — transcribe the video — even "
                                "if it is not a /reel/ link. Ticking this adds the link too."
                            ),
                        )
                with text_col:
                    st.markdown(_link_row(block))

        picked = [block for block in selectable if _is_picked(block)]
        st.divider()
        if st.button(
            f"Add {len(picked)} to Google Sheets" if picked else "Add to Google Sheets",
            key="ingest_add_to_sheet",
            type="primary",
            width="stretch",
            disabled=not picked or not selected_hashtags,
        ):
            urls = [block["url"] for block in picked]
            reel_urls = {
                block["url"]
                for block in picked
                if st.session_state.get(_reel_key(block["index"]))
            }
            try:
                append_link_rows(
                    GOOGLE_SHEET_ID,
                    urls,
                    selected_hashtags,
                    selected_comment_link,
                    reel_urls=reel_urls,
                )
            except Exception as e:
                st.error(f"Could not add to the posts sheet: {describe_error(e)}")
            else:
                for block in picked:
                    block["added"] = True
                    st.session_state.pop(_pick_key(block["index"]), None)
                    st.session_state.pop(_reel_key(block["index"]), None)
                st.session_state["ingest_blocks"] = blocks
                st.session_state["ingest_added"] = {
                    "count": len(urls),
                    "hashtags": selected_hashtags,
                    "comment_link": selected_comment_link,
                }
                st.rerun()

        if not selected_hashtags:
            st.caption("Pick a hashtag above before adding.")
