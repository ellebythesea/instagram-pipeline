# Instagram Caption Pipeline -- Build Instructions

Read my existing project files first. Reuse my credentials, .env, Streamlit setup, and any existing patterns. This is a modification of an existing app, not a new project.

The GitHub repo is: https://github.com/ellebythesea/instagram-pipeline.git

---

## What This App Does

A Streamlit app that processes Instagram posts into formatted social media captions. Two streams:

- **Reels/Videos**: Apify fetches video, thumbnail, transcript, original caption, and username in one call. Claude generates caption. Mostly automatic.
- **Photos/Carousels**: Apify fetches all images, original caption, and username. Marked for manual review since there's no spoken content.

---

## Google Sheet Columns

Fixed and named. Never shift regardless of what's filled or empty.

| Col | Header | Filled By | Notes |
|-----|--------|-----------|-------|
| A | Instagram URL | User | Required |
| B | Source Username | Script | From Apify response |
| C | Media Type | Script | "reel" or "photo" |
| D | Photo Count | Script | Number of images if carousel, blank if reel |
| E | Media Drive Link | Script | Link to file(s) in Google Drive |
| F | Thumbnail Drive Link | Script | Link to thumbnail/first image |
| G | Original Caption | Script | Caption from the original post |
| H | Transcript | Script | From Apify (reels only, blank for photos) |
| I | Speaker Name | User | Optional, filled in app or sheet |
| J | Required Hashtags | User | Optional, via dropdown or typed |
| K | Top Comment | User | Optional, prepended above generated caption |
| L | Footer | User | User's standard footer text |
| M | Generated Caption | Script | AI-generated caption |
| N | Status | Script | empty / ingested / done / error: [reason] |

---

## Two Apify Actors

### Reels Actor
Use the Instagram Reel Scraper/Analyzer that returns: video URL, thumbnail, transcript, caption, hashtags, mentions, username, likes, views, comments, duration.

Actor ID from user's Apify console: xMc5Ga1oCONPmWJIa (or whichever reel actor they've configured).

### Posts/Carousel Actor
Use a general Instagram Post Scraper that returns: all image URLs, caption, username, hashtags, mentions.

Build both as swappable modules. Each is one file with one function signature:
```python
def process_url(url: str) -> dict:
    # Returns: username, media_type, media_urls, thumbnail_url,
    #          original_caption, transcript (if reel), photo_count
```

If the URL contains "/reel/" or "/reels/", route to the reels actor. Otherwise route to the posts actor.

---

## Workflow Step 1: Ingest (Button: "Process New Rows")

Find rows where Status (col N) is empty. For each row:

1. Detect URL type (reel vs post) from the URL pattern.
2. Call the appropriate Apify actor.
3. Download all media files. Upload to a single flat Google Drive folder (no subfolders). Name files with post ID and date for sorting.
4. Upload thumbnail (reels) or first image (carousels/single photos) as the thumbnail.
5. Write: username (B), media type (C), photo count (D), media link (E), thumbnail link (F), original caption (G), transcript (H).
6. Set Status to "ingested".
7. If any step fails, set Status to "error: [reason]" and continue to next row.

---

## Workflow Step 2: Generate Captions (Button: "Generate Captions")

Find rows where Status is "ingested". For each row:

1. If reel: use transcript (H). If photo or transcript is empty: use original caption (G).
2. Send to Claude API with the caption prompt below.
3. If Top Comment (K) exists, prepend above generated caption with a blank line.
4. If Footer (L) exists, append below generated caption with a blank line.
5. Write final caption to col M.
6. Set Status to "done".

---

## Streamlit App Pages

### Page: Pipeline Dashboard

- Table showing all rows from the Google Sheet with their status.
- **"Process New Rows"** button -- ingests empty-status rows.
- **"Generate Captions"** button -- generates captions for ingested rows.
- Progress bar and log for each operation.

### Page: Post Editor

For each ingested row, show a card with:

- Thumbnail image (from col F)
- Original caption (col G, read-only)
- Transcript (col H, read-only)
- Input: Speaker Name (col I)
- Input: Required Hashtags (col J) with preset dropdown options:
  - "Good Influence"
  - "American Experiment Project"
  - "Palette Media"
  (Selecting inserts the hashtag. User can also type custom ones.)
- Input: Top Comment (col K)
- Input: Footer (col L)
- Display: Generated Caption (col M) with a **copy button**
- Save button writes changes back to the Google Sheet.

---

## Mobile: iOS Shortcut

Create an iOS Shortcut called "Add to Pipeline" that:

1. Accepts a shared URL from Instagram's share sheet (or clipboard).
2. Appends a new row to the Google Sheet with the URL in column A.
3. Shows a confirmation notification.

Implementation: HTTP POST to Google Sheets API. Claude Code should provide step-by-step instructions for building this in the iOS Shortcuts app.

---

## Caption Prompt (system message to Claude API)

You are a sharp political analyst. Rewrite the transcript into a short, clear social post under 1300 characters using exactly two simple paragraphs.

The first paragraph must be 250 characters or fewer and serve as the most important summary. It must include all hashtags. Use 3 to 5 relevant hashtags total. Prioritize the main people the post is about, then include one single word subject hashtag that helps with trending news discovery, followed by any remaining relevant tags. Replace the normal word or phrase in the sentence with the hashtag version, for example use #DonaldTrump in the sentence instead of writing the name normally. Do not add a separate hashtag only line at the end.

The second paragraph should add context using verified facts, dates, and numbers when relevant. Include direct quotes from the transcript when available. Verify names and quotes carefully. Any hashtag used in the caption body counts toward the total of 3 to 5 hashtags. Avoid speculation, flourish, links, or references to Trump's current office status.

## User message logic for Claude API

- Always include the transcript (H) or original caption (G) if no transcript.
- If Speaker Name (I) exists: "The speaker in this transcript is: [name]. Reference them by name."
- If Required Hashtags (J) exist: "These hashtags MUST be included as part of the 3-5 total: [hashtags]"
- Top Comment (K) and Footer (L) are NOT sent to Claude. They are prepended/appended after.

---

## Services

- **Apify** -- two actors: reel scraper (transcript + video + metadata) and post scraper (images + metadata)
- **Anthropic Claude** -- caption generation
- **Google Sheets API + Drive API** -- read/write sheet, upload media

## Dependencies

streamlit, google-auth, google-api-python-client, gspread, apify-client, anthropic, python-dotenv, requests

## Key Constraints

- Each Apify actor is a single swappable module (one file each).
- All media goes to one flat Google Drive folder. No subfolders.
- Sheet columns never move or reorder. Use named headers, not positional index.
- The app works both ways: user can fill metadata in the sheet directly OR in the Streamlit Post Editor. Both write to the same place.
- URL routing: "/reel/" or "/reels/" in URL = reel actor. Everything else = post actor.
