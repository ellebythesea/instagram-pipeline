# Google Sheet Structure

This documents every tab in the Google Sheet well enough to recreate it from scratch.

---

## Tab: posts

Main Instagram pipeline. 24 columns A–X. **The app restores this header row automatically if it is missing.**

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

**Status values:** empty (pending), `ingested`, `done`, `slides`, `error: [reason]`

**Media Type values:** `post`, `reel`, `article`

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

Columns A–Y match the `posts` tab exactly, followed by one extra column:

| Col | Header |
|-----|--------|
| Z | Deleted At |

`Deleted At` is a `YYYY-MM-DD HH:MM:SS` stamp of when the row was moved here.

To restore a row, copy its A–Y cells back into a `posts` row and delete the copy
here. Nothing in the app reads this tab.
