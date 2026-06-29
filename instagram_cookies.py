"""Shared helper for obtaining an Instagram cookies file path.

Priority order:
1. Local www.instagram.com_cookies.txt in the repo root
2. INSTAGRAM_COOKIES secret from Secret Manager
"""
from __future__ import annotations

import contextlib
import os
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
_LOCAL_COOKIES = REPO_ROOT / "www.instagram.com_cookies.txt"


@contextlib.contextmanager
def instagram_cookies_file(explicit_path: str | None = None):
    """Yield a path to a Netscape cookies file for instagram.com.

    If explicit_path is given, validate it exists and yield it directly.
    Otherwise try the local file, then fall back to Secret Manager.
    """
    if explicit_path:
        if not os.path.exists(explicit_path):
            raise RuntimeError(
                f"Cookies file not found: {explicit_path}\n"
                "Export from Chrome using 'Get cookies.txt LOCALLY' while logged in to instagram.com."
            )
        yield explicit_path
        return

    if _LOCAL_COOKIES.exists():
        yield str(_LOCAL_COOKIES)
        return

    from config import INSTAGRAM_COOKIES
    if not INSTAGRAM_COOKIES or INSTAGRAM_COOKIES == "instagram-cookies":
        raise RuntimeError(
            "No Instagram cookies available.\n"
            "Options:\n"
            "  1. Export from Chrome using 'Get cookies.txt LOCALLY' and save as "
            f"{_LOCAL_COOKIES.name} in the repo root.\n"
            "  2. Upload the cookies file content to Secret Manager as 'instagram-cookies'."
        )

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix="_ig_cookies.txt", delete=False
    )
    try:
        tmp.write(INSTAGRAM_COOKIES)
        tmp.flush()
        tmp.close()
        yield tmp.name
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
