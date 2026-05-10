"""Google Drive upload helper."""

import io
import json
import os
import re
from json import JSONDecodeError
from urllib.parse import parse_qs, urlparse

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from config import (
    GOOGLE_OAUTH_TOKEN_JSON,
    GOOGLE_SERVICE_ACCOUNT_JSON,
)

_SCOPES = ["https://www.googleapis.com/auth/drive"]


def _raise_drive_step_error(step: str, exc: Exception) -> None:
    raise RuntimeError(f"Google Drive step failed during {step}. Raw error: {exc}") from exc


def _get_service():
    if GOOGLE_OAUTH_TOKEN_JSON:
        try:
            oauth_info = json.loads(GOOGLE_OAUTH_TOKEN_JSON)
            creds = UserCredentials.from_authorized_user_info(
                oauth_info,
                scopes=_SCOPES,
            )
        except JSONDecodeError as exc:
            raise RuntimeError("GOOGLE_OAUTH_TOKEN_JSON is not valid JSON.") from exc
        except Exception as exc:
            raise RuntimeError("GOOGLE_OAUTH_TOKEN_JSON is malformed or incomplete.") from exc
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as exc:
                raise RuntimeError("Google OAuth refresh failed. Regenerate GOOGLE_OAUTH_TOKEN_JSON.") from exc
        return build("drive", "v3", credentials=creds)

    creds_src = GOOGLE_SERVICE_ACCOUNT_JSON
    if not creds_src:
        raise RuntimeError(
            "Neither GOOGLE_OAUTH_TOKEN_JSON nor GOOGLE_SERVICE_ACCOUNT_JSON is configured. "
            "Set GOOGLE_OAUTH_TOKEN_JSON for personal My Drive uploads, or "
            "GOOGLE_SERVICE_ACCOUNT_JSON for shared-drive/service-account access."
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


def _find_file_in_folder(service, folder_id: str, filename: str) -> dict:
    escaped_name = (filename or "").replace("\\", "\\\\").replace("'", "\\'")
    query = (
        f"name = '{escaped_name}' and "
        f"'{folder_id}' in parents and trashed = false"
    )
    try:
        result = (
            service.files()
            .list(
                q=query,
                fields="files(id,name,webViewLink)",
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
    except Exception as exc:
        _raise_drive_step_error("listing files in the target folder", exc)
    files = result.get("files", [])
    return files[0] if files else {}


def upload_to_drive(file_path: str, filename: str, folder_id: str, overwrite: bool = False) -> str:
    """Upload a file to Google Drive and return the web view link."""
    service = _get_service()
    media_body = MediaFileUpload(file_path, resumable=True)

    existing = _find_file_in_folder(service, folder_id, filename) if overwrite else {}
    try:
        if existing.get("id"):
            uploaded = (
                service.files()
                .update(
                    fileId=existing["id"],
                    body={"name": filename},
                    media_body=media_body,
                    fields="id,webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
            )
        else:
            uploaded = (
                service.files()
                .create(
                    body={"name": filename, "parents": [folder_id]},
                    media_body=media_body,
                    fields="id,webViewLink",
                    supportsAllDrives=True,
                )
                .execute()
            )
    except Exception as exc:
        step = "updating an existing Drive file" if existing.get("id") else "creating a Drive file"
        _raise_drive_step_error(step, exc)

    try:
        service.permissions().create(
            fileId=uploaded["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
    except Exception as exc:
        _raise_drive_step_error("setting public reader permission on the uploaded file", exc)

    return uploaded.get("webViewLink", "")


def extract_drive_file_id(link: str) -> str:
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", link or "")
    if match:
        return match.group(1)
    parsed = urlparse(link or "")
    return parse_qs(parsed.query).get("id", [""])[0]


def get_drive_file_metadata(link_or_file_id: str) -> dict:
    service = _get_service()
    file_id = extract_drive_file_id(link_or_file_id) or (link_or_file_id or "").strip()
    if not file_id:
        raise ValueError(f"Could not parse a Drive file id from {link_or_file_id!r}")
    try:
        return (
            service.files()
            .get(
                fileId=file_id,
                fields="id,name,webViewLink,mimeType",
                supportsAllDrives=True,
            )
            .execute()
        )
    except Exception as exc:
        _raise_drive_step_error("reading Drive file metadata", exc)


def copy_drive_file_to_folder(link_or_file_id: str, folder_id: str, filename: str = "") -> str:
    service = _get_service()
    metadata = get_drive_file_metadata(link_or_file_id)
    body = {"parents": [folder_id]}
    if filename.strip():
        body["name"] = filename.strip()
    try:
        copied = (
            service.files()
            .copy(
                fileId=metadata["id"],
                body=body,
                fields="id,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
    except Exception as exc:
        _raise_drive_step_error("copying a Drive file into the target folder", exc)
    try:
        service.permissions().create(
            fileId=copied["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
    except Exception as exc:
        _raise_drive_step_error("setting public reader permission on the copied file", exc)
    return copied.get("webViewLink", "")


def download_drive_file(link_or_file_id: str, dest_path: str) -> str:
    service = _get_service()
    metadata = get_drive_file_metadata(link_or_file_id)
    try:
        request = service.files().get_media(fileId=metadata["id"], supportsAllDrives=True)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _status, done = downloader.next_chunk()
    except Exception as exc:
        _raise_drive_step_error("downloading a Drive file", exc)
    with open(dest_path, "wb") as handle:
        handle.write(buffer.getvalue())
    return dest_path


def get_or_create_subfolder(parent_folder_id: str, folder_name: str) -> str:
    """Return a child folder id under the given parent, creating it if needed."""
    service = _get_service()
    escaped_name = folder_name.replace("'", "\\'")
    query = (
        f"name = '{escaped_name}' and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        f"'{parent_folder_id}' in parents and trashed = false"
    )
    try:
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
    except Exception as exc:
        _raise_drive_step_error("listing subfolders in the target Drive folder", exc)
    files = result.get("files", [])
    if files:
        return files[0]["id"]

    try:
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
    except Exception as exc:
        _raise_drive_step_error("creating a Drive subfolder", exc)
    return created["id"]
