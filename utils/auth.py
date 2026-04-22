from __future__ import annotations

import datetime as dt
import hashlib
import hmac

import extra_streamlit_components as stx
import streamlit as st

from config import APP_PASSWORD

COOKIE_NAME = "instagram_pipeline_auth"
SESSION_KEY = "authenticated"


def _cookie_manager() -> stx.CookieManager:
    manager = st.session_state.get("_cookie_manager_instance")
    if manager is None:
        manager = stx.CookieManager()
        st.session_state["_cookie_manager_instance"] = manager
    return manager


def _cookie_value() -> str:
    secret = (APP_PASSWORD or "").encode("utf-8")
    return hmac.new(secret, b"instagram-pipeline-auth-v1", hashlib.sha256).hexdigest()


def _set_auth_cookie() -> None:
    expires_at = dt.datetime.utcnow() + dt.timedelta(days=30)
    _cookie_manager().set(
        COOKIE_NAME,
        _cookie_value(),
        expires_at=expires_at,
        path="/",
        same_site="strict",
    )


def require_auth() -> bool:
    if not APP_PASSWORD:
        st.session_state[SESSION_KEY] = True
        return True

    if st.session_state.get(SESSION_KEY):
        return True

    cookies = _cookie_manager().get_all() or {}
    if cookies.get(COOKIE_NAME) == _cookie_value():
        st.session_state[SESSION_KEY] = True
        return True

    with st.form("auth_login_form", clear_on_submit=False):
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Log in", type="primary", use_container_width=True)

    if submitted:
        if password == APP_PASSWORD:
            st.session_state[SESSION_KEY] = True
            _set_auth_cookie()
            st.rerun()
        else:
            st.error("Incorrect password.")

    return False
