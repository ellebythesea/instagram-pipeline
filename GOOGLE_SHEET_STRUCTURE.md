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

## Tab: monitors

Instagram posts being monitored for comments on election guide articles. 6 columns.

| Col | Header |
|-----|--------|
| A | label |
| B | url |
| C | last |
| D | status |
| E | substack url |
| F | summary |

**Status values:** `open`, `closed`

- `label` — a short human-readable name for the post being monitored
- `url` — full Instagram post URL
- `last` — ISO timestamp of the last time comments were checked (written by the app)
- `substack url` — the Substack article URL this post is promoting (used in comments tab)
- `summary` — AI-generated comment pattern summary (written by the app)

---

## Tab: substack

Substack articles to generate Instagram posts from. 4 columns.

| Col | Header |
|-----|--------|
| A | url |
| B | article |
| C | status |
| D | notes |

**Status values:** `open`, `ingested`, `posts created`

- `url` — full Substack article URL
- `article` — full article body text (pasted in via the app or manually)
- `status` — workflow state; update this manually when you are done with an article

---

## Tab: substack_posts

Generated Instagram posts from Substack articles. 15 columns.

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

## Tab: __workspace_meta__

Internal key/value store used by the app. **Do not edit manually.**

| Col | Header |
|-----|--------|
| A | key |
| B | value |

Known keys written by the app:
- `last_scheduled_times` — JSON array of the last assigned scheduled time slots
- `slide_cta_options` — JSON object mapping row numbers to selected slide CTA choices
