# Instagram Caption Pipeline

## Task

Build a new Streamlit page that turns Instagram links into formatted social media captions. I paste links into a Google Sheet, click a button, and the system downloads the video, transcribes it, generates a caption, and writes everything back to the sheet.

## Plan

1. Read my existing project files first. Reuse my credentials, .env, and Streamlit setup.
2. Add a new Streamlit page called "Instagram Pipeline" with a "Process New Rows" button.
3. When clicked, read the Google Sheet and find rows where Status (column I) is empty.
4. For each new row:
   a. Call Apify to download the Instagram media and extract the source username.
   b. Upload the media to a Google Drive folder.
   c. If video, send audio to OpenAI Whisper for transcription.
   d. Send transcript to Claude API with the caption prompt below.
   e. Write results back to columns E through I.
   f. If any step fails, set Status to "error: [reason]" and continue to the next row.

## Sheet Columns

| Col | Name | Filled By |
|-----|------|-----------|
| A | Instagram URL | Me |
| B | Speaker Name | Me (optional) |
| C | Required Hashtags | Me (optional) |
| D | Top Comment | Me (optional) |
| E | Source Username | Script |
| F | Media Drive Link | Script |
| G | Transcript | Script |
| H | Generated Caption | Script |
| I | Status | Script |

## Caption Prompt (send this as the system message to Claude API)

You are a sharp political analyst. Rewrite the transcript into a short, clear social post under 1300 characters using exactly two simple paragraphs.

The first paragraph must be 250 characters or fewer and serve as the most important summary. It must include all hashtags. Use 3 to 5 relevant hashtags total. Prioritize the main people the post is about, then include one single word subject hashtag that helps with trending news discovery, followed by any remaining relevant tags. Replace the normal word or phrase in the sentence with the hashtag version, for example use #DonaldTrump in the sentence instead of writing the name normally. Do not add a separate hashtag only line at the end.

The second paragraph should add context using verified facts, dates, and numbers when relevant. Include direct quotes from the transcript when available. Verify names and quotes carefully. Any hashtag used in the caption body counts toward the total of 3 to 5 hashtags. Avoid speculation, flourish, links, or references to Trump's current office status.

## User message logic for Claude API

- Always include the transcript.
- If Speaker Name (col B) exists, add: "The speaker in this transcript is: [name]. Reference them by name."
- If Required Hashtags (col C) exist, add: "These hashtags MUST be included as part of the 3-5 total: [hashtags]"
- If Top Comment (col D) exists, do NOT send it to Claude. After getting the response, prepend it above the caption with a blank line between.

## Services

- **Apify** (instagram download) -- use apify-client Python package
- **OpenAI Whisper** (transcription)
- **Anthropic Claude** (caption generation)
- **Google Sheets API + Drive API** (read/write sheet, upload media)

## Dependencies

streamlit, google-auth, google-api-python-client, gspread, apify-client, openai, anthropic, python-dotenv, requests

## Key constraint

Build the Instagram downloader as a single swappable module so I can replace Apify later without touching anything else.
