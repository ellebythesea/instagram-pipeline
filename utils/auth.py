from __future__ import annotations

import datetime as dt
import hashlib
import hmac

import extra_streamlit_components as stx
import streamlit as st

from config import APP_PASSWORD

COOKIE_NAME = "instagram_pipeline_auth"
SESSION_KEY = "authenticated"
LOGIN_ERROR_KEY = "_auth_login_error"


def _cookie_manager() -> stx.CookieManager:
    manager = st.session_state.get("_cookie_manager_instance")
    if manager is None:
        manager = stx.CookieManager()
        st.session_state["_cookie_manager_instance"] = manager
    return manager


def _cookie_value() -> str:
    secret = (APP_PASSWORD or "").encode("utf-8")
    return hmac.new(secret, b"instagram-pipeline-auth-v1", hashlib.sha256).hexdigest()


def _set_authenticated() -> None:
    st.session_state[SESSION_KEY] = True
    st.session_state.pop(LOGIN_ERROR_KEY, None)


def _set_auth_cookie() -> None:
    expires_at = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=30)
    _cookie_manager().set(
        COOKIE_NAME,
        _cookie_value(),
        expires_at=expires_at,
        path="/",
        same_site="lax",
    )


def _native_cookie_value() -> str:
    try:
        return st.context.cookies.get(COOKIE_NAME, "")
    except Exception:
        return ""


def require_auth() -> bool:
    st.session_state.setdefault(SESSION_KEY, False)

    if not APP_PASSWORD:
        _set_authenticated()
        return True

    if st.session_state.get(SESSION_KEY):
        return True

    if _native_cookie_value() == _cookie_value():
        _set_authenticated()
        return True

    with st.form("auth_login_form", clear_on_submit=False):
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Log in", type="primary", use_container_width=True)

    if submitted:
        if password == APP_PASSWORD:
            _set_authenticated()
            _set_auth_cookie()
            return True
        else:
            st.session_state[LOGIN_ERROR_KEY] = "Incorrect password."

    error_message = st.session_state.get(LOGIN_ERROR_KEY)
    if error_message:
        st.error(error_message)

    return False
