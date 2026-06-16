# news.py
"""Fetch latest news context via Serper API to enrich caption generation."""

import hashlib
import re
import requests
from collections import Counter
from config import SERPER_API_KEY

_news_cache: dict[str, str] = {}


def get_latest_news_summary(transcript: str, num_results: int = 5) -> str:
    """Extract keywords from transcript, search for related news, return context string."""
    if not SERPER_API_KEY:
        return "LATEST NEWS CONTEXT:\nNo API key configured.\n\n"

    cache_key = hashlib.md5(transcript.encode()).hexdigest()
    if cache_key in _news_cache:
        return _news_cache[cache_key]

    try:
        words = re.findall(r"\b\w+\b", transcript)
        stopwords = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at",
            "to", "for", "of", "with", "by", "is", "was", "are", "were",
            "this", "that", "it", "he", "she", "they", "we", "you", "i",
        }
        cleaned = [w.lower() for w in words if w.lower() not in stopwords]
        proper_nouns = [
            words[i]
            for i in range(len(words))
            if words[i][0:1].isupper()
            and words[i].lower() not in stopwords
            and (i == 0 or not words[i - 1].endswith("."))
        ]
        frequent = [w for w, _ in Counter(cleaned).most_common(10) if len(w) > 3][:5]
        keywords = list(set(proper_nouns + frequent))
        if len(keywords) < 3:
            keywords.extend(["latest news", "political strategy", "geopolitics"])

        query = " ".join(keywords[:5]) + " latest news today"
        resp = requests.post(
            "https://google.serper.dev/news",
            json={"q": query, "num": num_results},
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        items = resp.json().get("news", [])[:num_results]
        if items:
            lines = [
                f"- {it.get('title', '')} ({it.get('date', '')}): {it.get('snippet', '')}"
                for it in items
            ]
            result = "LATEST NEWS CONTEXT:\n" + "\n".join(lines) + "\n\n"
        else:
            result = "LATEST NEWS CONTEXT:\nNo recent news found.\n\n"
        _news_cache[cache_key] = result
        return result

    except Exception:
        return "LATEST NEWS CONTEXT:\nUnable to fetch news at this time.\n\n"
