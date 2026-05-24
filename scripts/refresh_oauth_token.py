#!/usr/bin/env python3
"""Interactively build a Desktop OAuth client JSON, run the auth flow, and print the token."""

from __future__ import annotations

import getpass
import json
import tempfile
import os

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]

def main() -> int:
    print("Enter your OAuth 2.0 Desktop client credentials.")
    client_id = input("Client ID: ").strip()
    client_secret = getpass.getpass("Client secret: ").strip()

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "project_id": "vioo-instagram-pipeline",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "redirect_uris": ["http://localhost"],
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(client_config, f)
        tmp_path = f.name

    try:
        flow = InstalledAppFlow.from_client_secrets_file(tmp_path, SCOPES)
        creds = flow.run_local_server(port=0)
    finally:
        os.unlink(tmp_path)

    token_json = json.loads(creds.to_json())
    print("\n--- TOKEN JSON (copy everything below this line) ---")
    print(json.dumps(token_json, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
