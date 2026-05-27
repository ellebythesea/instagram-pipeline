# Instagram Pipeline

Streamlit workspace for:

- adding Instagram posts, reels, and article links to a Google Sheet
- processing rows into an editor
- generating captions and headlines
- uploading Instagram media to Google Drive
- optionally transcribing reels locally on your Mac from your synced Drive folder

## Main Flow

### Actions tab

Use this to:

- `Add to sheet`
- `Generate headline`
- `Caption this`
- `Download media`

Instagram links and article links both work for `Add to sheet`, `Generate headline`, and `Caption this`.

### Edit tab

This is the main working area.

Rows that have been processed for editing appear here with:

- preview image
- source username or article source
- generated caption
- original caption
- transcript for Instagram rows
- scheduling controls
- row actions like transcribe, generate caption, skip, add CTA, or delete

### Substack tab

This section has three subtabs:

- `Promote` generates Instagram posts to drive traffic to Substack articles.
- `Monitors` watches Instagram comments on election guide posts.
- `Guides` creates Substack election article prompts from candidate names.

## Google Sheet Structure

### Tab: posts

Main Instagram pipeline. 24 columns A–X:

| Col | Header |
|-----|--------|
| A | Instagram URL |
| B | Required Hashtags |
| C | Source Username |
| D | Generated Caption |
| E | Media Type |
| F | Photo Count |
| G | Media Drive Link |
| H | Thumbnail Drive Link |
| I | Original Caption |
| J | Transcript |
| K | Top Comment |
| L | Speaker Name |
| M | Footer |
| N | Status |
| O | Caption Context |
| P | Scheduled Time |
| Q | name |
| R | text1 |
| S | text2 |
| T | text3 |
| U | Slide CTA |
| V | text4 |
| W | text5 |
| X | text6 |

Status values: empty (pending), `ingested`, `done`, `slides`, `error: [reason]`

The app restores headers if they are missing.

### Tab: monitors

Instagram posts being monitored for comments on election guide articles. 6 columns:

| Col | Header |
|-----|--------|
| A | label |
| B | url |
| C | last |
| D | status |
| E | substack url |
| F | summary |

Status values: `open`, `closed`

### Tab: substack

Substack articles to generate posts from. 4 columns:

| Col | Header |
|-----|--------|
| A | url |
| B | article |
| C | status |
| D | notes |

Status values: `open`, `ingested`, `posts created`

### Tab: substack_posts

Generated Instagram posts from Substack articles. 15 columns:

| Col | Header |
|-----|--------|
| A | url |
| B | angle |
| C | caption |
| D | text1 |
| E | text2 |
| F | text3 |
| G | text4 |
| H | text5 |
| I | text6 |
| J | cta |
| K | status |
| L | slide_prompt |
| M | slide_input |
| N | post_type |
| O | topics |

Status values: `slide prompt ready`, `row created`, `posted`

### Tab: fundraising

Referral link presets for top comments. 2 columns:

| Col | Header |
|-----|--------|
| A | label |
| B | link (full top comment text with referral URL) |

### Tab: __workspace_meta__

Internal key/value store used by the app. Do not edit manually.

| Col | Header |
|-----|--------|
| A | key |
| B | value |

## Drive Media Folder

The app uploads Instagram media into your Drive folder and you sync that folder locally on your Mac.

The local transcription script auto-detects the synced media folder from common Google Drive locations, including:

```text
/Users/lisa/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/
/Users/lisamollica/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/
```

## Local Helper Scripts

These are the local scripts in `scripts/` and what they do.

### Google Drive OAuth token refresh

If Drive uploads start failing because the OAuth token expired or refresh stopped working, regenerate `GOOGLE_OAUTH_TOKEN_JSON`.

1. Download or locate your Google OAuth client JSON for the Drive app.
   This is the `Desktop app` OAuth client file from Google Cloud, not the service-account JSON.
2. Run:

```bash
.venv/bin/python scripts/generate_drive_oauth_token.py "/path/to/oauth-client.json"
```

3. Complete the browser login/consent flow.
4. Copy the full JSON printed by the script.
5. Replace `GOOGLE_OAUTH_TOKEN_JSON` in Streamlit secrets with that new JSON.
6. Redeploy or reload the app.

Expected Streamlit secret format:

```toml
GOOGLE_OAUTH_TOKEN_JSON = """{
  "token": "...",
  "refresh_token": "...",
  "token_uri": "https://oauth2.googleapis.com/token",
  "client_id": "...",
  "client_secret": "...",
  "scopes": ["https://www.googleapis.com/auth/drive"],
  "expiry": "2026-05-10T19:19:16Z"
}"""
```

Notes:

- The `expiry` field changing is normal.
- The important field is `refresh_token`; if that is missing or revoked, uploads will break again after the access token expires.
- For this project, keep `GOOGLE_OAUTH_TOKEN_JSON` in Streamlit secrets so personal My Drive uploads use the fresh token immediately.

### Google Drive OAuth health check

To verify that the current Drive OAuth token can still refresh and access the configured Drive folder, run:

```bash
.venv/bin/python scripts/check_drive_oauth.py
```

If it exits with `FAILED`, regenerate `GOOGLE_OAUTH_TOKEN_JSON` before uploads break in the app.

You can run this on a schedule from your Mac or any machine that has the same secrets available.

## Local Reel Transcription

If you want free local transcription on your Mac instead of paying for transcript runs in the cloud app, use the local script:

```bash
.venv/bin/python scripts/local_transcribe_reels.py
```

You can still override the folder explicitly:

```bash
.venv/bin/python scripts/local_transcribe_reels.py --media-dir "/path/to/instagram pipeline media"
```

That script:

- reads the Google Sheet
- finds rows where:
  - `Media Type = reel`
  - `Transcript` is blank
  - `Media Drive Link` exists
- looks up the Drive filename for the reel
- finds the matching synced local video in your Drive folder
- runs a local Whisper backend
- writes the transcript back to the Google Sheet
- regenerates the caption from that transcript

Constraint for local cleanup:

- a local original video is kept only if some current sheet row still resolves to that exact Drive filename
- a local `*_segments/` folder is kept only if its source video still matches a current sheet row
- a local screenshot is kept only if its underlying `YYMMDD_postId` key still matches a current sheet row
- anything else is treated as orphaned local media and moved into `safe_for_deletion/`

### Local transcription dependency

Install one local Whisper backend first:

```bash
pip install faster-whisper
```

If you prefer the OpenAI Whisper Python package instead:

```bash
pip install openai-whisper
```

The script tries `faster-whisper` first, then falls back to `openai-whisper`.

## Local One-Minute Video Splitter

This script only works on files that are already downloaded locally. By default it auto-detects common Google Drive split folders, including:

```text
/Users/lisa/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/splits
/Users/lisamollica/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/splits
```

Run it with:

```bash
.venv/bin/python scripts/split_video_minutes.py
```

Or point it at a different folder:

```bash
.venv/bin/python scripts/split_video_minutes.py "/path/to/folder"
```

What it does:

- looks for local video files already in that folder
- splits them into exact one-minute `.mp4` segments using `ffmpeg`
- center-crops each segment to `4:5` before saving
- creates a sibling output folder like `my_video_segments/`
- names the segments `one.mp4`, `two.mp4`, `three.mp4`, and so on
- skips any source video that already has segments created

Requirement:

```bash
ffmpeg
```

## Local Auto-Split Folder Watcher

If you want the split to happen automatically whenever you drag a video into the folder, run the watcher:

```bash
.venv/bin/python scripts/watch_split_folder.py
```

By default it watches the same auto-detected split folder, including:

```text
/Users/lisa/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/splits
/Users/lisamollica/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/splits
```

You can also point it at another folder:

```bash
.venv/bin/python scripts/watch_split_folder.py "/path/to/folder"
```

What it does:

- watches the folder continuously
- waits until a newly dropped video stops changing size
- splits it into one-minute segments and center-crops them to `4:5`
- automatically runs the one-minute split
- skips files that already have a `*_segments` folder with output files

## Running Locally

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Run the app:

```bash
streamlit run app.py
```

## Secret Manager Setup

The app can read most runtime secrets from Google Secret Manager.

### Secret Manager secret names

By default, `config.py` looks for these secrets:

- `openai-api-key`
- `apify-api`
- `google-sheet-id`
- `google-folder-id`
- `google-oauth-id`
- `google-oauth-token`
- `google-service-account`
- `password`
- `serper-id`

Optional:

- `google-worksheet-name`

Optional secret names:

- `google-screenshots-subfolder`
- `apify-reel-actor-id`
- `apify-post-actor-id`

### Bootstrap credential

You still need one Google bootstrap credential outside Secret Manager so the app can authenticate to Secret Manager in the first place.

Use one of:

- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_CREDENTIALS_BASE64`

That bootstrap credential can live in:

- Streamlit Cloud secrets
- local `.streamlit/local_secrets.toml`
- `.env`
- shell environment variables

If your Secret Manager project differs from the bootstrap service account project, also set:

- `SECRET_MANAGER_PROJECT_ID`

## Required Secrets / Fallback Values

These still work as direct fallback values if Secret Manager is unavailable or you want to override one value locally:

```toml
OPENAI_API_KEY = "..."
APIFY_API_TOKEN = "..."
GOOGLE_SHEET_ID = "..."
GOOGLE_WORKSHEET_NAME = "..."
GOOGLE_DRIVE_FOLDER_ID = "..."
GOOGLE_SERVICE_ACCOUNT_JSON = '''{...}'''
GOOGLE_CREDENTIALS_BASE64 = "..."
GOOGLE_OAUTH_TOKEN_JSON = '''{"token":"...","refresh_token":"..."}'''
APP_PASSWORD = "..."
```

Notes:

- `OPENAI_API_KEY` powers caption/headline generation and some OCR/image-text flows.
- `APIFY_API_TOKEN` powers Instagram scraping.
- `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_CREDENTIALS_BASE64` is the bootstrap credential for Secret Manager and Sheets access.
- `GOOGLE_OAUTH_TOKEN_JSON` is used for Drive uploads into a personal My Drive folder.
- Share the Google Sheet and Drive folder with the service account email so Sheets access and Secret Manager bootstrap work.

## Current Caption Behavior

### Instagram rows

- captions are generated from transcript, original caption, or caption context
- reels auto-prepend a `LINK` CTA if no custom top comment exists
- original captions can be previewed with footer and required hashtags

### Article rows

- article source text is extracted from the page
- captions are not auto-generated during `Process for editing`
- article captions prepend:
  - `Comment LINK (on instagram) and we will DM you the link to https://...`
- article rows do not show a transcript tab
- article rows do not append source text back under the generated caption

## Useful Commands

Run the app locally in Streamlit:

```bash
streamlit run app.py
.venv/bin/streamlit run app.py
```

Run local reel transcription for all blank-transcript reel rows and archive orphaned local media into `safe_for_deletion/`:

```bash
.venv/bin/python scripts/local_transcribe_reels.py
```

Archive orphaned local media only, without running transcription:

```bash
.venv/bin/python scripts/archive_orphaned_media.py
```

Split all already-downloaded videos in the local `splits` folder into one-minute chunks:

```bash
.venv/bin/python scripts/split_video_minutes.py
```

Watch the local `splits` folder and auto-split new videos as you drag them in:

```bash
.venv/bin/python scripts/watch_split_folder.py
```
