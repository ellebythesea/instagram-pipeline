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

### Data tab

This shows the sheet-backed table view and lets you batch-process pending rows into the editor.

## Google Sheet Columns

The app expects this order:

1. `Instagram URL`
2. `Source Username`
3. `Generated Caption`
4. `Media Type`
5. `Photo Count`
6. `Media Drive Link`
7. `Thumbnail Drive Link`
8. `Original Caption`
9. `Transcript`
10. `Top Comment`
11. `Required Hashtags`
12. `Speaker Name`
13. `Footer`
14. `Status`
15. `Caption Context`
16. `Scheduled Time`

The app restores headers if they are missing.

## Drive Media Folder

The app uploads Instagram media into your Drive folder and you sync that folder locally on your Mac.

The local transcription script auto-detects the synced media folder from common Google Drive locations, including:

```text
/Users/lisa/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/
/Users/lisamollica/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/
```

## Local Helper Scripts

These are the local scripts in `scripts/` and what they do.

### Google Drive OAuth token generator

```bash
python scripts/generate_drive_oauth_token.py "/path/to/oauth-client.json"
```

Use this when you need to refresh `GOOGLE_OAUTH_TOKEN_JSON`.

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

This script only works on files that are already downloaded locally. By default it scans:

```text
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

By default it watches:

```text
/Users/lisamollica/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/splits
```

You can also point it at another folder:

```bash
.venv/bin/python scripts/watch_split_folder.py "/path/to/folder"
```

What it does:

- watches the folder continuously
- waits until a newly dropped video stops changing size
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

- `GOOGLE_CREDENTIALS_BASE64`
- `GOOGLE_SERVICE_ACCOUNT_JSON`

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
GOOGLE_CREDENTIALS_BASE64 = "..."
# or
GOOGLE_SERVICE_ACCOUNT_JSON = '''{...}'''
GOOGLE_OAUTH_TOKEN_JSON = '''{...}'''
GOOGLE_OAUTH_CLIENT_JSON = '''{...}'''
APP_PASSWORD = "..."
```

Notes:

- `OPENAI_API_KEY` powers caption/headline generation and some OCR/image-text flows.
- `APIFY_API_TOKEN` powers Instagram scraping.
- `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_CREDENTIALS_BASE64` is used for Sheets access and as the Secret Manager bootstrap credential.
- `GOOGLE_OAUTH_TOKEN_JSON` is used for Drive uploads when running with OAuth.

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

Run local reel transcription for all blank-transcript reel rows:

```bash
.venv/bin/python scripts/local_transcribe_reels.py
```

Split all already-downloaded videos in the local `splits` folder into one-minute chunks:

```bash
.venv/bin/python scripts/split_video_minutes.py
```

Watch the local `splits` folder and auto-split new videos as you drag them in:

```bash
.venv/bin/python scripts/watch_split_folder.py
```
