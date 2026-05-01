"""Lightweight article extraction helpers for non-Instagram links."""

from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urlparse

import requests


_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
)


def _clean_text(value: str) -> str:
    text = unescape(value or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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

    cleaned_title = _clean_text(title)
    cleaned_description = _clean_text(description)
    if cleaned_title:
        parts.append(cleaned_title)
    if cleaned_description and cleaned_description.lower() != cleaned_title.lower():
        parts.append(cleaned_description)

    for paragraph in paragraphs:
        lowered = paragraph.lower()
        if cleaned_description and lowered == cleaned_description.lower():
            continue
        if cleaned_title and lowered == cleaned_title.lower():
            continue
        if len(paragraph) < 40:
            continue
        parts.append(paragraph)
        if len(" ".join(parts)) >= 1800:
            break

    source_text = "\n\n".join(parts).strip()
    if len(source_text) > 2200:
        source_text = source_text[:2200].rsplit(" ", 1)[0].strip() + "..."
    return source_text


def fetch_article_source(url: str) -> dict:
    response = requests.get(
        url,
        timeout=20,
        headers={"User-Agent": _USER_AGENT},
        allow_redirects=True,
    )
    response.raise_for_status()
    html = response.text or ""

    og_title = _extract_meta(html, "property", "og:title")
    og_description = _extract_meta(html, "property", "og:description")
    meta_description = _extract_meta(html, "name", "description")
    twitter_title = _extract_meta(html, "name", "twitter:title")
    twitter_description = _extract_meta(html, "name", "twitter:description")

    parsed_title, paragraphs = _extract_title_and_body(html)
    title = og_title or twitter_title or parsed_title
    description = og_description or twitter_description or meta_description
    source_text = _compose_source_text(title, description, paragraphs)
    if not source_text:
        raise ValueError("Could not extract enough article text from that link.")

    parsed = urlparse(response.url or url)
    domain = parsed.netloc.replace("www.", "")
    return {
        "url": response.url or url,
        "domain": domain,
        "title": title,
        "description": description,
        "source_text": source_text,
    }
