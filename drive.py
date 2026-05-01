"""Google Drive upload helper."""

import json
import os
from json import JSONDecodeError

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from config import (
    GOOGLE_OAUTH_CLIENT_JSON,
    GOOGLE_OAUTH_TOKEN_JSON,
    GOOGLE_SERVICE_ACCOUNT_JSON,
)

_SCOPES = ["https://www.googleapis.com/auth/drive"]


def _get_service():
    if GOOGLE_OAUTH_TOKEN_JSON:
        creds = UserCredentials.from_authorized_user_info(
            json.loads(GOOGLE_OAUTH_TOKEN_JSON),
            scopes=_SCOPES,
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        return build("drive", "v3", credentials=creds)

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
    return build("drive", "v3", credentials=creds)


def upload_to_drive(file_path: str, filename: str, folder_id: str) -> str:
    """Upload a file to Google Drive and return the web view link."""
    service = _get_service()

    uploaded = (
        service.files()
        .create(
            body={"name": filename, "parents": [folder_id]},
            media_body=MediaFileUpload(file_path, resumable=True),
            fields="id,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    service.permissions().create(
        fileId=uploaded["id"],
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True,
    ).execute()

    return uploaded.get("webViewLink", "")


def get_or_create_subfolder(parent_folder_id: str, folder_name: str) -> str:
    """Return a child folder id under the given parent, creating it if needed."""
    service = _get_service()
    escaped_name = folder_name.replace("'", "\\'")
    query = (
        f"name = '{escaped_name}' and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        f"'{parent_folder_id}' in parents and trashed = false"
    )
    result = (
        service.files()
        .list(
            q=query,
            fields="files(id,name)",
            pageSize=1,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = result.get("files", [])
    if files:
        return files[0]["id"]

    created = (
        service.files()
        .create(
            body={
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created["id"]
