"""Lightweight article extraction helpers for non-Instagram links."""

from __future__ import annotations

import multiprocessing
import re
from html import unescape
from html.parser import HTMLParser
from queue import Empty
from urllib.parse import urljoin, urlparse

import requests


_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
)
_REQUEST_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}
_REQUEST_TIMEOUT = (8, 25)
_ARTICLE_TIMEOUT_SECONDS = 40
_MAX_HTML_BYTES = 3 * 1024 * 1024

_NOISE_PATTERNS = [
    r"^copyright\s+\d{4}.*all rights reserved\.?$",
    r"^\(ap photo/.*\)$",
    r"^ap photo/.*$",
    r"^read more at the link in our bio\.?$",
]


def _clean_text(value: str) -> str:
    text = unescape(value or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_compare_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _looks_like_noise(paragraph: str) -> bool:
    cleaned = _clean_text(paragraph)
    if not cleaned:
        return True
    lowered = cleaned.lower()
    for pattern in _NOISE_PATTERNS:
        if re.match(pattern, lowered, re.IGNORECASE):
            return True
    if "(ap photo/" in lowered:
        return True
    if "all rights reserved" in lowered:
        return True
    return False


def _extract_meta(html: str, attr_name: str, attr_value: str) -> str:
    pattern = re.compile(
        rf"<meta[^>]+{attr_name}\s*=\s*[\"']{re.escape(attr_value)}[\"'][^>]+content\s*=\s*[\"'](.*?)[\"']",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(html)
    if match:
        return _clean_text(match.group(1))

    reverse_pattern = re.compile(
        rf"<meta[^>]+content\s*=\s*[\"'](.*?)[\"'][^>]+{attr_name}\s*=\s*[\"']{re.escape(attr_value)}[\"']",
        re.IGNORECASE | re.DOTALL,
    )
    match = reverse_pattern.search(html)
    if match:
        return _clean_text(match.group(1))
    return ""


class _ArticleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_title = False
        self._capture_paragraph = False
        self._skip_depth = 0
        self.title_parts: list[str] = []
        self.current_paragraph: list[str] = []
        self.paragraphs: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if lowered == "title":
            self._capture_title = True
        elif lowered == "p":
            self._capture_paragraph = True
            self.current_paragraph = []

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"} and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if lowered == "title":
            self._capture_title = False
        elif lowered == "p" and self._capture_paragraph:
            paragraph = _clean_text("".join(self.current_paragraph))
            if paragraph:
                self.paragraphs.append(paragraph)
            self._capture_paragraph = False
            self.current_paragraph = []

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        if self._capture_title:
            self.title_parts.append(data)
        if self._capture_paragraph:
            self.current_paragraph.append(data)


def _extract_title_and_body(html: str) -> tuple[str, list[str]]:
    parser = _ArticleParser()
    parser.feed(html)
    title = _clean_text("".join(parser.title_parts))
    paragraphs = [_clean_text(p) for p in parser.paragraphs if _clean_text(p)]
    return title, paragraphs


def _compose_source_text(title: str, description: str, paragraphs: list[str]) -> str:
    parts: list[str] = []
    seen: set[str] = set()

    cleaned_title = _clean_text(title)
    cleaned_description = _clean_text(description)
    if cleaned_title:
        parts.append(cleaned_title)
        seen.add(_normalize_compare_text(cleaned_title))
    if cleaned_description and cleaned_description.lower() != cleaned_title.lower():
        parts.append(cleaned_description)
        seen.add(_normalize_compare_text(cleaned_description))

    for paragraph in paragraphs:
        if _looks_like_noise(paragraph):
            continue
        lowered = paragraph.lower()
        if cleaned_description and lowered == cleaned_description.lower():
            continue
        if cleaned_title and lowered == cleaned_title.lower():
            continue
        normalized = _normalize_compare_text(paragraph)
        if normalized in seen:
            continue
        if len(paragraph) < 40:
            continue
        sentence_count = len(re.findall(r"[.!?](?:\s|$)", paragraph))
        if sentence_count <= 1 and len(paragraph) < 120:
            continue
        parts.append(paragraph)
        seen.add(normalized)
        if len(" ".join(parts)) >= 1800:
            break

    source_text = "\n\n".join(parts).strip()
    if len(source_text) > 2200:
        source_text = source_text[:2200].rsplit(" ", 1)[0].strip() + "..."
    return source_text


def _fallback_source_text(title: str, description: str) -> str:
    parts: list[str] = []
    cleaned_title = _clean_text(title)
    cleaned_description = _clean_text(description)
    if cleaned_title:
        parts.append(cleaned_title)
    if cleaned_description and cleaned_description.lower() != cleaned_title.lower():
        parts.append(cleaned_description)
    return "\n\n".join(parts).strip()


def _fetch_article_html(url: str) -> tuple[str, str]:
    with requests.Session() as session:
        with session.get(
            url,
            timeout=_REQUEST_TIMEOUT,
            headers=_REQUEST_HEADERS,
            allow_redirects=True,
            stream=True,
        ) as response:
            final_url = response.url or url
            response.raise_for_status()
            chunks: list[bytes] = []
            total_bytes = 0
            for chunk in response.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                chunks.append(chunk)
                total_bytes += len(chunk)
                if total_bytes >= _MAX_HTML_BYTES:
                    break
            encoding = response.encoding or "utf-8"
            html = b"".join(chunks).decode(encoding, errors="replace")
            return final_url, html


def _fetch_article_source_inner(url: str) -> dict:
    final_url, html = _fetch_article_html(url)

    og_title = _extract_meta(html, "property", "og:title")
    og_description = _extract_meta(html, "property", "og:description")
    og_image = _extract_meta(html, "property", "og:image")
    meta_description = _extract_meta(html, "name", "description")
    twitter_title = _extract_meta(html, "name", "twitter:title")
    twitter_description = _extract_meta(html, "name", "twitter:description")
    twitter_image = _extract_meta(html, "name", "twitter:image")

    parsed_title, paragraphs = _extract_title_and_body(html)
    title = og_title or twitter_title or parsed_title
    description = og_description or twitter_description or meta_description
    summary_text = _fallback_source_text(title, description)
    source_text = _compose_source_text(title, description, paragraphs)
    if not source_text:
        source_text = summary_text
    if not source_text:
        raise ValueError("Could not extract enough article text from that link.")

    parsed = urlparse(final_url)
    domain = parsed.netloc.replace("www.", "")
    image_url = og_image or twitter_image
    if image_url:
        image_url = urljoin(final_url, image_url)
    return {
        "url": final_url,
        "domain": domain,
        "title": title,
        "description": description,
        "image_url": image_url,
        "summary_text": summary_text,
        "source_text": source_text,
    }


def _article_source_worker(url: str, output_queue) -> None:
    try:
        output_queue.put(("ok", _fetch_article_source_inner(url)))
    except Exception as error:
        status = getattr(getattr(error, "response", None), "status_code", None)
        output_queue.put(("error", error.__class__.__name__, str(error), status))


def fetch_article_source(url: str) -> dict:
    context = multiprocessing.get_context("spawn")
    output_queue = context.Queue(maxsize=1)
    process = context.Process(target=_article_source_worker, args=(url, output_queue))
    process.daemon = True
    process.start()
    process.join(_ARTICLE_TIMEOUT_SECONDS)

    if process.is_alive():
        process.terminate()
        process.join(2)
        raise TimeoutError(f"Article request timed out after {_ARTICLE_TIMEOUT_SECONDS} seconds.")

    try:
        result = output_queue.get_nowait()
    except Empty:
        raise RuntimeError("Article extraction failed before returning a result.")

    if result[0] == "ok":
        return result[1]

    _, error_name, message, status = result
    status_label = f" ({status})" if status else ""
    raise RuntimeError(f"{error_name}{status_label}: {message}")
