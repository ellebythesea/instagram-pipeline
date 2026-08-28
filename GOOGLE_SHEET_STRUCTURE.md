# Google Sheet Structure

This documents every tab in the Google Sheet well enough to recreate it from scratch.

---

## Tab: posts

Main Instagram pipeline. 28 columns A–AB. **The app restores this header row automatically if it is missing.**

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
| AB | Reel Drive Link |

### Slide columns

A carousel runs from `text1` to `text8`. Slide generation fills `text1`–`text3`; the rest are filled
by pasted slide results or by hand. The editor draws a slide for every one of these columns that has
copy, so a row with `text7` and `text8` filled in shows eight slides, each with its own preview,
font controls, and edit button, and the link CTA moves to whichever slide is last.

`text7` and `text8` (Z and AA) came after the original A–Y layout, and `Reel Drive Link` (AB) after
those. Rows are read positionally with missing columns treated as empty, so a sheet that never grew
those columns still loads; the first time the app writes them it widens the grid and fills in the
header cells if they are blank.

### Reel Drive Link

`Reel Drive Link` (col AB) holds the reel the **Reels** tab composes — the video centre-cropped to
5:4 on a 1080×1920 canvas with a headline burnt into the bar above it — uploaded beside the row's
other previews. It is written when the reel is generated and is separate from `Media Drive Link`,
which keeps pointing at the untouched source video.

Holding it on the row rather than in the session is what lets the post be picked up on another
machine with the reel still attached: the tab shows Drive's poster frame for the file and a button
to open it, without the local encode that only exists on the machine that made it.

**Status values:** empty (pending), `reel` (pending, forced to process as a reel), `ingested`, `done`, `slides`, `needs source: [reason]`, `error: [reason]`

Typing `reel` into Status on a pending row makes it process through the reel scraper and get transcribed, even when its link is a `/p/` one rather than `/reel/`, and then finishes it as a Reel Lines post: ten headlines in `text1` (col R) and `reel lines` in `Slide CTA` (col U), rather than carousel slide copy. The Status marker is consumed on processing — the row comes out with the normal `ingested`/`done` status and a Media Type of `reel` — so the `Slide CTA` marker is written up front and is what identifies the row afterwards. The Ingest page's **reel** checkbox writes the same marker.

The marker is typed by hand, so `reel`, `reels`, `reel line`, `reel lines` and `reels lines` all count, in any capitalisation and with hyphens, underscores or extra spaces (`Reels`, `Reel-Lines`). Anything else in Status is treated as a real status, so a misspelling leaves the row pending and untouched. The headlines are written when the caption is — after transcription — so they appear in the Whisper step of **Run all**, not in its ingest step.

Typing `reel` into Media Type instead forces the reel treatment on its own, without the Reel Lines finish.

No reel gets carousel slide copy, however it came to be one — a `/reel/` link, `reel` in Status, or `reel` in
Media Type. A reel is posted as a video, so slides would never be used and the model call is skipped. Rows
carrying the Reel Lines marker get their ten headlines instead; any other reel gets none up front, and the
**Reels** tab writes them on demand.

`needs source: [reason]` marks an article row whose page could not be read. The row is still created so the article text can be pasted in by hand on the Edit tab, which builds the caption and slide copy and moves the row to `ingested`.

**Media Type values:** `post`, `reel`, `article`

### Reel Lines rows

Rows created by the `Create Reel Lines` app action use this same schema with two columns repurposed:

- `Slide CTA` (col U) is set to `reel lines`. This is what marks the row as a Reel Lines post, so the
  editor shows a `Headlines` tab instead of `Slides`. Nothing else writes col U unless a slide CTA is
  picked, and the Slides tab — the only place that offers one — is replaced for these rows.
- `text1` (col R) holds the ten headlines, one per line, instead of slide copy. `text2`–`text8` and
  `quote` stay empty.

Everything else is ordinary: `Transcript` holds the transcription, `Generated Caption` the caption,
`Media Drive Link` the video, and `Top Comment` the comment link when one was given.

---

## Tab: substack

Substack articles to generate Instagram posts from and optionally monitor for comments. 9 columns.

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

**Status values:** `open`, `ingested`, `posts created`

- `url` — full Substack article URL
- `name` — short label used in the article selector instead of showing the full URL
- `article` — full article body text (pasted in via the app or manually)
- `topic breakdown` — persisted JSON/list of reusable article themes for the Promote flow
- `status` — workflow state; update this manually when you are done with an article
- `instagram url` — the Instagram post URL tied to this article for comment monitoring
- `monitoring status` — whether the comments tab should include this row
- `last comment retrieved` — ISO timestamp of the last comment check
- `summary` — AI-generated comment pattern summary

The app auto-upgrades older 4-column `substack` tabs by adding the monitoring columns.

---

## Tab: monitors

Legacy fallback tab for Instagram comment monitoring. Existing rows still work, but new monitoring should be tracked on the `substack` tab instead.

---

## Substack Promote Storage

Substack Promote posts now write directly into the main `posts` tab instead of a separate `substack_posts` tab.

These rows use the standard posts schema with:
- `Instagram URL` set to the Substack article URL
- `Media Type` set to `article`
- `Generated Caption` filled in
- `Original Caption` and `Transcript` containing the article text
- `Caption Context` containing Substack promote metadata
- slide columns filled when generated or after slide results are applied

**Status values:** `slide prompt ready`, `row created`, `posted`

- `url` — the Substack article URL this post came from
- `angle` — the one-sentence post angle chosen during idea generation
- `caption` — full Instagram caption
- `text1/text2/text3/text4/text5/text6` — carousel slide text
- `cta` — call-to-action label (e.g. `Save link for Substack`)
- `slide_prompt` — reusable ChatGPT prompt for making or remaking carousel slide copy
- `slide_input` — article-specific input to paste with the slide prompt
- `post_type` — `high_level_summary` or `article_subset`
- `topics` — comma-separated topics used for the post concept

---

## Tab: fundraising

Referral link presets for top comments. 2 columns.

| Col | Header |
|-----|--------|
| A | label |
| B | link |

- `label` — short display name shown in the app dropdown
- `link` — full top comment text including the referral URL

---

## Tab: hashtags

Client/organization hashtag presets. Populates the `Hashtag` dropdown on the Ingest page. 2 columns.

| Col | Header |
|-----|--------|
| A | label |
| B | hashtags |

- `label` — client/organization name shown in the app dropdown
- `hashtags` — hashtag text written into `Required Hashtags` (col B of `posts`) for every link added on the Ingest page

The header row is optional; rows missing either column are ignored.

---

## Tab: docs

Client source documents. Populates the `Document` dropdown on the Ingest page. 4 columns.

| Col | Header |
|-----|--------|
| A | label |
| B | url |
| C | hashtags |
| D | comment link |

- `label` — client/organization name shown in the app dropdown
- `url` — Google Doc link the client's items are read from; its tabs are listed in the app
- `hashtags` — hashtag text written into `Required Hashtags` for links added from that document. When
  blank, the Ingest page falls back to the `hashtags` tab dropdown.
- `comment link` — written into `Top Comment` (col K of `posts`) for every link added from that
  document. A bare URL becomes the standard `Comment LINK (on instagram) and we will DM you the
  link to …` CTA when the caption is generated; any other text is used as the top comment as
  written. Leave blank for no top comment.

The header row is optional; rows missing a label or url are ignored. `hashtags` and
`comment link` are both optional, so older 3-column and 2-column `docs` tabs keep working.

---

## Tab: __workspace_meta__

Internal key/value store used by the app. **Do not edit manually.**

| Col | Header |
|-----|--------|
| A | key |
| B | value |

Known keys written by the app:
- `last_scheduled_times` — JSON array of the last assigned scheduled time slots
- `slide_cta_options` — JSON object mapping row numbers to selected slide CTA choices

---

## Tab: Safe to Delete

Where deleted rows go. The app creates this tab the first time a row is deleted, so
nothing is removed from the workbook outright.

Columns A–AA match the `posts` tab exactly, followed by one extra column:

| Col | Header |
|-----|--------|
| AB | Deleted At |

`Deleted At` is a `YYYY-MM-DD HH:MM:SS` stamp of when the row was moved here. A tab created before
`text7`/`text8` existed keeps its old header row, so its `Deleted At` label sits in column Z while
newly archived rows write the stamp in AB. Nothing in the app reads this tab, so the mismatch is
cosmetic — relabel the header row if you want it tidy.

To restore a row, copy its A–AA cells back into a `posts` row and delete the copy
here.
