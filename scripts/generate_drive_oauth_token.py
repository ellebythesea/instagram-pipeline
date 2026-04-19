"""Generate a Google Drive OAuth token JSON for server-side uploads.

Usage:
  python scripts/generate_drive_oauth_token.py /path/to/oauth-client.json

This opens a browser locally, asks you to sign in with the Google account that
owns the target Drive folder, and prints the authorized user JSON that should be
stored as GOOGLE_OAUTH_TOKEN_JSON.
"""

import json
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/generate_drive_oauth_token.py /path/to/oauth-client.json")
        return 1

    client_path = sys.argv[1]
    flow = InstalledAppFlow.from_client_secrets_file(client_path, SCOPES)
    creds = flow.run_local_server(port=0)
    print(json.dumps(json.loads(creds.to_json())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
