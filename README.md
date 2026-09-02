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

### Ingest tab

For working through the documents clients send over. A third workspace tab alongside `Home` and
`Substack`.

Pick a client from the `Document` dropdown (populated from the `docs` tab) and the page reads that
client's Google Doc live, lists its tabs, and selects the most recent one by date. The last entry,
`Enter your own URL`, reveals an input for a one-off Google Doc link instead. Or leave the dropdown on
`None` and paste text into the box. `Reload` re-reads the doc from Drive; it is otherwise cached for
five minutes so switching tabs is instant.

`Read this tab` shows the document back in full, in order and line for line, with every link lifted
onto a line of its own and a checkbox in the left gutter beside the ones that can be added. Headlines
stay bold, bullets stay bullets, and nothing is dropped — the wording around a link is right there
above it rather than squeezed into the row. Check off what you want and `Add to Google Sheets` appends
one `posts` row per link. The hashtag comes from column C of the `docs` row; when there is no such row
to read it from — a typed URL, pasted text, or a `docs` row with column C blank — a `Hashtag` dropdown
appears instead, fed by the `hashtags` tab.

Column D of the `docs` row is that client's comment link. When it is filled in, every link added from
that document gets it as its `Top Comment`, so the caption step ends the post with the standard
`Comment LINK (on instagram) and we will DM you the link to …` CTA pointing at it. A bare URL is
expanded into that CTA; anything else is used as the top comment as written. Leave column D blank and
rows are added with no top comment, as before.

No AI is involved and no text is rewritten or summarized. Links are found by regex and the rest of the
document is shown exactly as written, so nothing in a long document is skipped or reworded, and the
page costs nothing to run. Nothing starts checked. Notes on a link line can include:

- `🟡 highlighted` — the wording above the link is highlighted in the doc. Detected by exporting the
  doc as HTML (the markdown export drops styling) and matching highlighted runs against the
  document's own lines, scoped to the selected tab, since these docs repeat headlines from one day to
  the next. The highlighted line itself is marked with a 🟡 too.
- `not used (…)` — a link on Twitter/X, Threads, or Reddit, or to a Google Doc/Drive file. Still
  shown so the document reads whole, but with no checkbox.
- `same link as above` — the same URL appeared earlier in the document, so only the first one is
  checkable.
- `already in the sheet` — the link is already on the `posts` tab. Those rows are skipped by
  `Select all`.

### Substack tab

This section has three subtabs:

- `Promote` generates Instagram posts to drive traffic to Substack articles.
- `Monitors` watches Instagram comments on election guide posts.
- `Guides` creates Substack election article prompts from candidate names.

### Create Reel Lines

An `App actions` entry on the `Home` tab for the other kind of post: one video, ten headlines, no
carousel.

Paste a link, upload a video, or do both. A reel link is downloaded; an upload is used as-is; when
both are given the upload is the media and the link is only the comment link. Either way the video is
transcribed, saved whole to the main Drive folder — no 60-second split — and given a thumbnail. Then,
in order:

- the transcript generates a caption, ending in the standard `Comment LINK (on instagram) and we will
  DM you the link to …` CTA when a link was given
- the transcript and that caption together generate ten one-line clickbait headlines in sentence
  case, so the headlines and the caption land on the same angle

A link that is not a reel still works: another Instagram post contributes its caption, and anything
else is read as an article. Those rows get headlines and a caption but no video.

The result is an ordinary `posts` row, so it lands in the `Edit` tab like any other. Opening it goes
straight to the `Reels` tab, with the ten headlines as the options in its **Pick a headline**
dropdown and the first one already in the headline box — the headlines are written to be burnt into
the video, so that is where they are wanted first. Picking a different one drops it into the box,
where it can be edited before the reel is generated.

The video is centre-cropped to 5:4 and laid into the middle of a 1080x1920 black canvas, with the
headline burnt into the bar above it and the bar below left empty to caption in Instagram.
**Crop position** slides the crop up or down so faces stay in frame. **Fit whole video**, the toggle
under `A-` and `A+` above the preview, drops the crop instead and fits the whole frame into a box
144px taller than the crop, 1080x1008: nothing is cut off, and a vertical video fills the box top to
bottom, 566x1008 for a 9:16 reel. The box is a `#1C2027` panel the full width of the canvas, so the
video sits in something rather than floating in black — that panel is the ~256px either side. The
bars above and below stay 456px, so there is still room for the headline and for captioning in
Instagram. There is nothing to position in that mode, so the crop slider is greyed
out. **Preview frame** picks which second the still is taken from; its track is the clip's own
length, so it scrubs end to end whatever that is, and it stays greyed out until the video is loaded
and its length is readable. **Generate reel** is what encodes the video and uploads it to Drive.

It also shows a `Headlines` tab in place of `Slides`, for copying the headlines out rather than
burning one in: each headline sits in its own copy block, wrapped rather than
scrolled so the whole line reads inside the block, and the caption to copy is at the bottom. Buttons at the top open the Instagram link, if there was one,
and the video in Drive. The `Original` tab holds the transcript, as it does for any other row.

Because the headlines live in `text1`, the `Slides` and `Make generic` row actions are hidden for
these rows — generating slide copy would overwrite them.

### Generate Reel Lines

The same finish, as a row action, for a post that already exists. **Post actions → Generate Reel
Lines** writes ten headlines into `text1` from the row's transcript (or its context or original
caption, whichever it has), marks the row `reel lines` in `Slide CTA`, and opens it on the `Reels`
tab with the new headlines in the dropdown. Run it again to rewrite the headlines; the previous
pick is dropped so the fresh set is what is on offer.

It is greyed out on a row with nothing to write headlines from — process the post first.

## Google Sheet Structure

### Tab: posts

Main Instagram pipeline. 27 columns A–AA:

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
| Y | quote |
| Z | text7 |
| AA | text8 |

A carousel runs from `text1` to `text8`. Only `text1`–`text3` are generated by default; every slide
after that appears in the editor as soon as it has copy, so adding `text7` or `text8` to a row (by
hand or in a pasted slide result) gives that post two more slides. `text7` and `text8` were added
after the original layout — the app widens the sheet and labels Z and AA the first time it writes
them, and older rows with nothing in those columns are unaffected.

Status values: empty (pending), `reel` (pending, forced to process as a reel), `ingested`, `done`, `slides`, `error: [reason]`

Type `reel` into Status on a pending row to have it transcribed as a reel even when its link is a
`/p/` one, and finished as a **Create Reel Lines** post — ten headlines in `text1` and `reel lines`
in `Slide CTA`, instead of carousel slide copy. `reels`, `reel lines` and `reels lines` all count as
the same marker, in any capitalisation and with hyphens or extra spaces, since the cell is typed by
hand. The Ingest page's **reel** checkbox writes the same marker. The Status marker is replaced by
the normal status once the row is processed; the `Slide CTA` marker is written at the start of
processing and is what makes the row a Reel Lines row from then on.

The headlines are written at the point the row's caption is written, which for a reel is after it has
been transcribed — the transcript is what they are drawn from. **Run all** ingests the row first
(no transcript yet) and transcribes and captions it in its Whisper step, so that is where the
headlines land; **Process post** on the row does both in one go.

To force the reel treatment *without* Reel Lines, type `reel` into Media Type rather than Status.
`scripts/run_pipeline.py` honours the marker as a plain reel; neither it nor
`scripts/local_transcribe_reels.py` writes the headlines, so process the row from the app for those.

The app restores headers if they are missing.

`Create Reel Lines` rows reuse this schema: `Slide CTA` (col U) is set to `reel lines` to mark the
row kind, and `text1` (col R) holds the ten headlines, one per line, instead of slide copy. `text2`
through `text8` stay empty.

### Tab: substack

Substack articles to generate posts from and optionally monitor for comments. 9 columns:

| Col | Header |
|-----|--------|
| A | url |
| B | name |
| C | article |
| D | topic breakdown |
| E | status |
| F | instagram url |
| G | monitoring status |
| H | last comment retrieved |
| I | summary |

Status values: `open`, `ingested`, `posts created`

Monitoring status values: `open`, `closed`

The app auto-upgrades older 4-column `substack` tabs by adding the monitoring columns.

### Tab: monitors

Legacy fallback tab for Instagram comment monitoring. Existing rows still work, but new monitoring should be tracked on the `substack` tab instead.

### Substack Promote Storage

Generated Instagram posts from Substack articles now write directly into the main `posts` tab.

Rows are stored as normal post rows with:
- `Instagram URL` = the Substack article URL
- `Media Type` = `article`
- `Generated Caption` populated
- `Original Caption` and `Transcript` set to the article body
- `Caption Context` carrying Substack promote metadata
- slide fields populated when available

The old `substack_posts` tab is no longer required for new Promote posts.

Status values: `slide prompt ready`, `row created`, `posted`

### Tab: fundraising

Referral link presets for top comments. 2 columns:

| Col | Header |
|-----|--------|
| A | label |
| B | link (full top comment text with referral URL) |

### Tab: hashtags

Client/organization hashtag presets. Populates the `Hashtag` dropdown on the Ingest tab. 2 columns,
header row optional:

| Col | Header |
|-----|--------|
| A | label (client name shown in the dropdown) |
| B | hashtags written into Required Hashtags |

### Tab: docs

Client source documents. Populates the `Document` dropdown on the Ingest tab. 4 columns, header row
optional:

| Col | Header |
|-----|--------|
| A | label (client name shown in the dropdown) |
| B | Google Doc link to read items from |
| C | hashtags written into Required Hashtags for that doc (optional) |
| D | comment link written into Top Comment for that doc (optional) |

Any doc you can open works — it is read with the app's OAuth token, so nothing needs sharing with the
service account.

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

## Automated Pipeline (GitHub Actions)

`.github/workflows/run_pipeline.yml` runs `scripts/run_pipeline.py` on GitHub's runners: ingest
pending rows, transcribe reels, split videos, then sweep orphaned Drive files.

There is **no schedule** — the workflow is manual only. Trigger it from the repo's **Actions** tab
(*Run Pipeline* → *Run workflow*), or with `gh workflow run run_pipeline.yml`. To automate it again,
add a `schedule:` block back to the `on:` trigger in that file.

The run needs only the `GOOGLE_SERVICE_ACCOUNT_JSON` repository secret; everything else is pulled
from Google Secret Manager by `config.py`.

## Local Helper Scripts

These are the local scripts in `scripts/` and what they do.

### Ingest reels via yt-dlp with your own Chrome session

The scrapers already try yt-dlp first and only fall back to Apify when yt-dlp fails. Use this script
when you want to ingest pending reel rows from your own logged-in Chrome session instead — it avoids
the Apify fallback entirely, which helps when Instagram is blocking unauthenticated yt-dlp requests.

**One-time setup:** Install the Chrome extension **"Get cookies.txt LOCALLY"**, navigate to instagram.com while logged in, click the extension, and export. Save the file to the repo root as `www.instagram.com_cookies.txt` (it's gitignored).

```bash
.venv/bin/python scripts/ingest_with_ytdlp.py
```

Override the cookies file location if needed:

```bash
.venv/bin/python scripts/ingest_with_ytdlp.py --cookies /path/to/cookies.txt
```

The script processes all pending Instagram rows (reels, photos, and carousels). Article rows are skipped. Re-export the cookies file when your Instagram session expires.

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
- `instagram-cookies` — contents of a Netscape cookies file exported from Chrome while logged in to instagram.com; used by yt-dlp for authenticated scraping when no local `www.instagram.com_cookies.txt` is present

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
- required hashtags lead the caption's hashtag group, ahead of the generated
  ones, so Instagram indexes them first — in the editor's `Caption` tab as well
  as in what is written to the sheet, so a caption generated before this was the
  rule is reordered on its way to the screen

### Article rows

- article source text is extracted from the page
- when the page cannot be read — connection error, bot wall, paywall, consent
  screen, or a hang that trips the 40s timeout — the pipeline searches Serper for
  the same story and reads a **full article from a different outlet** instead.
  See [Unreadable articles](#unreadable-articles).
- captions are not auto-generated during `Process for editing`
- article captions prepend:
  - `Comment LINK (on instagram) and we will DM you the link to https://...`
- article rows do not show a transcript tab
- article rows do not append source text back under the generated caption

### Unreadable articles

Some outlets will not serve the page to the pipeline: they refuse the connection,
return a bot check, sit behind a paywall or consent wall, or simply hang. Rather
than failing the row, `article_source.py` recovers in this order:

1. **The page itself** — direct fetch and HTML extraction.
2. **Reader fallback** — the same URL through `r.jina.ai`.
3. **Alternate article** — Serper searches for the same story, and the pipeline
   fetches and extracts a *full article from a different outlet*. Candidates are
   filtered so they are recent (14 days), on a different registrable domain than
   the one that failed, not a social/aggregator host, and topically matched to the
   original headline or URL slug. The first candidate that yields readable body
   text wins.
4. **Search snippets** — only if no alternate article can be read either, the
   Serper snippets are stitched into source text, as before.

If nothing topically relevant comes back, the row is still created — it just
needs the text by hand. See [Pasting article text by hand](#pasting-article-text-by-hand).

When step 3 supplies the text:

- `Instagram URL` and the `Comment LINK` CTA keep the link you pasted, so the post
  still promotes your source.
- `Source Username` becomes the outlet the text actually came from (for example
  `apnews.com`), so attribution stays honest.
- The app shows a notice naming the outlet and the alternate URL; `run_pipeline.py`
  prints the same line.

The payload carries `alternate_source: True`, `source_url` (the article that was
read) and `requested_url` (the link you pasted) if you need them elsewhere.

Everything here needs `SERPER_API_KEY`. Without it, the chain stops after step 2
and the row goes straight to the paste step below.

### Pasting article text by hand

When every automatic route fails, the row is **not** lost. Ingest still creates the
post with:

- `Media Type` = `article`
- `Source Username` = the domain of the link you pasted
- `Status` = `needs source: [reason]`, e.g.
  `needs source: Article access blocked or paywalled (403).`

The row opens in the Edit tab like any other, and carries an extra panel under the
post header:

- the reason it could not be read
- an `Open article link` button
- a text area for the article text — or any other context you want the post written
  from, pasted from anywhere
- a `Build post from this text` button

Submitting writes the text to `Original Caption`, `Transcript`, and `Caption Context`,
flips the row to `ingested`, and then runs the same generation the automatic path
would have: caption first, then slide copy. The row lands on `done` with slide text,
exactly as if the article had been readable.

The pasted text is the only source used, so paste the body of the story rather than a
headline if you want a caption with substance. The grid shows a `!` badge on rows
still waiting for text.

### Video upload or transcription that seems stuck

Every ffmpeg/ffprobe call and every OpenAI call in the media path is bounded, so a
step that cannot finish now fails with a reason instead of sitting there:

- **ffmpeg audio extraction** — 300s, then the original file is tried as-is
- **Whisper transcription** — 240s, one attempt, no retry (a retry would re-upload
  the whole file)
- **Other ffmpeg work** — 60s for probes, 120s for single-frame grabs, 900s for
  re-encodes and 60-second splits
- **Chat completions in `caption.py`** — 60s, one retry

Whisper also rejects uploads over 25MB. Audio extracted at the pipeline's settings
runs about 4MB/hour, so a normal video is nowhere near it — that limit only bites
when ffmpeg could not read the file and the raw video is sent instead. When that
happens the app now says so and names the size rather than attempting the upload.

If an upload still appears to do nothing, the file is most likely still travelling
from the browser to the server. `maxUploadSize` in `.streamlit/config.toml` is 400MB,
which is large enough that a phone video can take a while on a slow connection, and
Streamlit shows no progress for that leg.

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
