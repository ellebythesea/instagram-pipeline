from __future__ import annotations

import re

import streamlit as st

_BASE_CSS = """
.stApp [data-testid="stAppViewContainer"] {
    padding-bottom: 9rem;
}
"""

_POST_EDITOR_CSS = """
div[data-testid="stVerticalBlock"]:has(> div.sticky-generate-anchor) {
    position: fixed;
    right: 1.25rem;
    bottom: 1.25rem;
    width: min(460px, calc(100vw - 2.5rem));
    z-index: 999;
    background: rgba(255, 255, 255, 0.96);
    border: 1px solid rgba(0, 0, 0, 0.08);
    border-radius: 18px;
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.08);
    padding: 0.9rem 1rem;
    backdrop-filter: blur(10px);
}
.sticky-generate-anchor {
    display: none;
}
.editor-row {
    border: 1px solid rgba(0, 0, 0, 0.08);
    border-radius: 18px;
    padding: 1rem 1rem 0.5rem;
    margin-bottom: 1rem;
    background: rgba(255, 255, 255, 0.85);
}
.editor-row [data-testid="stCodeBlock"] {
    max-height: 3.1rem;
    overflow: hidden;
    margin-bottom: 0.5rem;
}
.editor-row [data-testid="stCodeBlock"] pre {
    max-height: 3.1rem;
    overflow: hidden;
    white-space: pre-wrap;
    margin: 0;
}
@media (max-width: 640px) {
    div[data-testid="stVerticalBlock"]:has(> div.sticky-generate-anchor) {
        right: 0.75rem;
        left: 0.75rem;
        width: auto;
        bottom: 0.75rem;
    }
}
"""

_WORKSPACE_CSS = """
.workspace-shell {
    max-width: 1120px;
}
section[data-testid="stSidebar"] {
    display: none;
}
[data-testid="collapsedControl"] {
    display: none;
}
.workspace-note {
    padding: 0.85rem 1rem;
    border: 1px solid rgba(15, 23, 42, 0.08);
    border-radius: 14px;
    background: rgba(248, 250, 252, 0.9);
    margin-bottom: 1rem;
}
.workspace-home-card {
    border: 1px solid rgba(15, 23, 42, 0.12);
    border-radius: 24px;
    padding: 1.25rem;
    background: #fff;
    box-shadow: 0 12px 32px rgba(15, 23, 42, 0.06);
    margin-bottom: 1rem;
}
.workspace-results-card {
    border: 1px solid rgba(15, 23, 42, 0.12);
    border-radius: 20px;
    padding: 1rem;
    background: #fff;
    margin-top: 1rem;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) {
    border: 1px solid rgba(15, 23, 42, 0.12);
    border-radius: 24px;
    padding: 1.25rem;
    background: #fff;
    margin-bottom: 2.25rem;
    box-shadow: 0 12px 32px rgba(15, 23, 42, 0.06);
}
.workspace-row-tabs [role="radiogroup"] {
    gap: 0.45rem;
    overflow-x: auto;
    white-space: nowrap;
    padding-bottom: 0.25rem;
}
.workspace-row-tabs [role="radiogroup"] label {
    border: 1px solid rgba(15, 23, 42, 0.12);
    border-radius: 999px;
    padding: 0.2rem 0.75rem;
    background: #fff;
}
.workspace-row-tabs [role="radiogroup"] label:has(input:checked) {
    background: #111827;
    color: white;
    border-color: #111827;
}
.workspace-row-summary {
    display: flex;
    gap: 0.65rem;
    flex-wrap: wrap;
    margin: 0.15rem 0 0.85rem;
}
.workspace-chip {
    border: 1px solid rgba(15, 23, 42, 0.12);
    border-radius: 999px;
    padding: 0.25rem 0.7rem;
    font-size: 0.85rem;
    color: #334155;
    background: #fff;
}
.workspace-home-card .stButton > button {
    min-height: 3.15rem;
    border-radius: 14px;
}
.workspace-status-line {
    color: #64748b;
    font-size: 0.92rem;
    margin-bottom: 0.2rem;
}
.workspace-section-label {
    font-size: 0.8rem;
    font-weight: 700;
    letter-spacing: 0.03em;
    text-transform: uppercase;
    color: #64748b;
    margin: 0.2rem 0 0.55rem;
}
.workspace-action-note {
    font-size: 0.92rem;
    color: #475569;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) .stButton > button {
    min-height: 3rem;
    border-radius: 14px;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="column"] {
    min-width: 0;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="stHorizontalBlock"] {
    display: flex;
    flex-direction: row;
    align-items: stretch;
    gap: 1rem;
    flex-wrap: nowrap;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-action-anchor) [data-testid="stHorizontalBlock"] {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 4rem 4rem;
    align-items: stretch;
    column-gap: 0.75rem;
    row-gap: 0;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="stHorizontalBlock"] > [data-testid="column"]:first-child {
    flex: 0 0 42%;
    width: 42%;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="stHorizontalBlock"] > [data-testid="column"]:last-child {
    flex: 0 0 58%;
    width: 58%;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="stCodeBlock"] {
    margin: 0.2rem 0 0.35rem;
    min-height: 2.1rem;
    max-height: 2.1rem;
    overflow: hidden;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="stCodeBlock"] pre {
    min-height: 2.1rem;
    max-height: 2.1rem;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    line-height: 1.1rem;
    padding: 0.45rem 2.75rem 0.45rem 0.7rem;
    border-radius: 12px;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="stCodeBlock"] code {
    line-height: 1.1rem;
    font-size: 0.86rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    display: block;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-action-anchor) .stButton > button {
    white-space: nowrap;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-action-anchor) [data-testid="stHorizontalBlock"] > [data-testid="column"] {
    min-width: 0;
    width: auto;
    max-width: none;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-action-anchor) [data-testid="stHorizontalBlock"] > [data-testid="column"]:first-child {
    grid-column: 1;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-action-anchor) [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) {
    grid-column: 2;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-action-anchor) [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(3) {
    grid-column: 3;
}
.workspace-content-tabs [data-baseweb="tab-list"] {
    gap: 0.5rem;
    flex-wrap: nowrap;
    overflow-x: auto;
}
.workspace-content-tabs [data-baseweb="tab"] {
    white-space: nowrap;
}
.workspace-plain-copy-text {
    font-size: 10px;
    line-height: 1.45;
    color: #64748b;
    white-space: pre-wrap;
    margin-top: 0.15rem;
    padding-right: 0.25rem;
}
.workspace-edit-main-anchor,
.workspace-action-anchor,
.workspace-generate-anchor {
    display: none;
}
div[data-testid="stVerticalBlock"]:has(> div.workspace-generate-anchor) {
    position: fixed;
    right: 1rem;
    bottom: 1rem;
    width: min(420px, calc(100vw - 2rem));
    z-index: 999;
    background: rgba(255, 255, 255, 0.96);
    border: 1px solid rgba(15, 23, 42, 0.12);
    border-radius: 18px;
    box-shadow: 0 12px 32px rgba(15, 23, 42, 0.12);
    padding: 0.9rem 1rem;
    backdrop-filter: blur(10px);
}
@media (max-width: 640px) {
    div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) {
        padding: 1rem;
    }
    div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="stHorizontalBlock"] > [data-testid="column"]:first-child {
        flex: 0 0 44%;
        width: 44%;
    }
    div[data-testid="stVerticalBlock"]:has(> div.workspace-edit-main-anchor) [data-testid="stHorizontalBlock"] > [data-testid="column"]:last-child {
        flex: 0 0 56%;
        width: 56%;
    }
    div[data-testid="stVerticalBlock"]:has(> div.workspace-action-anchor) [data-testid="stHorizontalBlock"] {
        display: grid;
        grid-template-columns: minmax(0, 1fr) 4rem 4rem;
        column-gap: 0.75rem;
    }
}
"""

_PAGE_CSS = {
    "default": _BASE_CSS,
    "post_editor": _BASE_CSS + _POST_EDITOR_CSS,
    "workspace": _BASE_CSS + _WORKSPACE_CSS,
}

_DECLARATION_RE = re.compile(r":\s*([^;{}]+);")


def _importantize(css: str) -> str:
    def repl(match: re.Match[str]) -> str:
        value = match.group(1).strip()
        if value.endswith("!important"):
            return f": {value};"
        return f": {value} !important;"

    return _DECLARATION_RE.sub(repl, css)


def inject(page: str = "default") -> None:
    css = _PAGE_CSS.get(page, _BASE_CSS)
    st.markdown(f"<style>{_importantize(css)}</style>", unsafe_allow_html=True)
