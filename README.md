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
- row actions like transcribe, download, skip, add CTA, or delete

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

Your local synced media folder is:

```text
/Users/lisamollica/Library/CloudStorage/GoogleDrive-voteinorout@gmail.com/My Drive/_apps/vioo instagram pipeline/instagram pipeline media/
```

## Local Reel Transcription

If you want free local transcription on your Mac instead of paying for transcript runs in the cloud app, use the local script:

```bash
python scripts/local_transcribe_reels.py
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

## Running Locally

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Run the app:

```bash
streamlit run app.py
```

## Required Secrets

These are normally provided through Streamlit secrets or environment variables:

```toml
OPENAI_API_KEY = "..."
APIFY_API_TOKEN = "..."
GOOGLE_SHEET_ID = "..."
GOOGLE_DRIVE_FOLDER_ID = "..."
GOOGLE_SERVICE_ACCOUNT_JSON = '''{...}'''
GOOGLE_OAUTH_TOKEN_JSON = '''{...}'''
GOOGLE_OAUTH_CLIENT_JSON = '''{...}'''
APP_PASSWORD = "..."
```

Notes:

- `OPENAI_API_KEY` powers caption/headline generation and some OCR/image-text flows.
- `APIFY_API_TOKEN` powers Instagram scraping.
- `GOOGLE_SERVICE_ACCOUNT_JSON` is used for Sheets access.
- `GOOGLE_OAUTH_TOKEN_JSON` is used for Drive uploads when running with OAuth.

## Current Caption Behavior

### Instagram rows

- captions are generated from transcript, original caption, or caption context
- reels auto-prepend a `LINK` CTA if no custom top comment exists
- original captions can be previewed with footer and required hashtags

### Article rows

- article source text is extracted from the page
- captions are auto-generated during `Process for editing`
- article captions prepend:
  - `Comment LINK (on instagram) and we will DM you the link to https://...`
- article rows do not show a transcript tab
- article rows do not append source text back under the generated caption

## Useful Commands

Run local reel transcription for all blank-transcript reel rows:

```bash
python scripts/local_transcribe_reels.py
```
