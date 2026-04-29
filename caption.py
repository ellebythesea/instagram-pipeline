# caption.py
"""Transcribe video audio and generate social-media captions via OpenAI."""

import os
import re
import subprocess
import tempfile
from typing import Optional

import openai

from config import (
    OPENAI_API_KEY,
    TRIM_SILENCE,
    AUDIO_SAMPLE_RATE,
    AUDIO_CHANNELS,
    AUDIO_BITRATE,
    CAPTION_SPLIT_THRESHOLD,
)
from news import get_latest_news_summary


client = openai.OpenAI(api_key=OPENAI_API_KEY)


# ---------------------------------------------------------------------------
# Audio helpers
# ---------------------------------------------------------------------------

def _get_ffmpeg_path() -> str:
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return "ffmpeg"


def _extract_audio(video_path: str) -> Optional[str]:
    """Extract mono low-bitrate WAV from video for faster Whisper upload."""
    try:
        ffmpeg = _get_ffmpeg_path()
        fd, out = tempfile.mkstemp(suffix=".wav")
        os.close(fd)

        cmd = [
            ffmpeg, "-y", "-i", video_path,
            "-vn",
            "-ac", str(AUDIO_CHANNELS),
            "-ar", str(AUDIO_SAMPLE_RATE),
            "-b:a", str(AUDIO_BITRATE),
        ]
        if TRIM_SILENCE:
            af = (
                "silenceremove=start_periods=1:start_duration=0.5:start_threshold=-40dB:"
                "stop_periods=1:stop_duration=0.8:stop_threshold=-40dB"
            )
            cmd.extend(["-af", af])
        cmd.append(out)

        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode != 0:
            try:
                os.unlink(out)
            except Exception:
                pass
            return None
        return out
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Transcription
# ---------------------------------------------------------------------------

def transcribe_video(video_path: str) -> Optional[str]:
    """Send video audio to Whisper and return the transcript text."""
    processed = None
    try:
        processed = _extract_audio(video_path)
        src = processed or video_path
        with open(src, "rb") as f:
            result = client.audio.transcriptions.create(model="whisper-1", file=f)
        return result.text
    except Exception as e:
        return None
    finally:
        if processed and os.path.exists(processed):
            try:
                os.unlink(processed)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Caption formatting (ported from existing app)
# ---------------------------------------------------------------------------

def _format_caption(text: str) -> str:
    """Clean up GPT output into readable 1-2 paragraph form with inline hashtags."""
    try:
        s = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()

        # Pull trailing hashtag block
        tag_block = None
        m = re.search(r"(\s*(?:#[\w\d_]+)(?:\s*#[\w\d_]+)+)\s*$", s)
        if m:
            tag_block = m.group(1)
            s = s[: m.start()].rstrip()

        s = re.sub(r"\n{3,}", "\n\n", s).strip()

        # Build paragraphs
        paragraphs: list[str] = []
        if "\n\n" in s:
            parts = [" ".join(p.strip().splitlines()) for p in s.split("\n\n")]
            paragraphs = parts[:2] if len(parts) <= 2 else [parts[0], " ".join(parts[1:]).strip()]
        else:
            sentences = re.split(r"(?<=[.!?\u2026][)\]}'\"'\u201d]?)\s+", s)
            sentences = [seg.strip() for seg in sentences if seg.strip()]
            body = " ".join(sentences)
            if len(sentences) <= 2:
                if len(body) > CAPTION_SPLIT_THRESHOLD and len(sentences) >= 2:
                    paragraphs = [sentences[0], " ".join(sentences[1:]).strip()]
                else:
                    paragraphs = [body]
            else:
                total = sum(len(x) for x in sentences)
                target = max(total // 2, 1)
                acc, acc_len = [], 0
                for i, sent in enumerate(sentences):
                    acc.append(sent)
                    acc_len += len(sent)
                    if acc_len >= target and i < len(sentences) - 1:
                        break
                p1 = " ".join(acc).strip()
                p2 = " ".join(sentences[len(acc):]).strip()
                paragraphs = [p1, p2] if p2 else [p1]

        # Inline hashtags
        if tag_block:
            tags = list(dict.fromkeys(re.findall(r"#[\w\d_]+", tag_block)))
            if tags:
                if not paragraphs:
                    paragraphs = [""]
                p1 = paragraphs[0]
                for tag in tags:
                    if re.search(rf"(?<!\w){re.escape(tag)}(?!\w)", p1):
                        continue
                    phrase = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", tag.lstrip("#").replace("_", " ")).strip()
                    if phrase:
                        replaced = re.sub(rf"(?i)\b{re.escape(phrase)}\b", tag, p1, count=1)
                        if replaced != p1:
                            p1 = replaced
                            continue
                    p1 = f"{p1} {tag}".strip()
                paragraphs[0] = p1

        return "\n\n".join(p for p in paragraphs if p)
    except Exception:
        return text


def _sanitize(text: str) -> str:
    """Remove 'Former President Trump' phrasing."""
    def repl(m):
        suffix = m.group(1) or ""
        return "President Trump" + suffix

    for pat in [
        r"(?i)\bformer\s+(?:u\.?s\.?\s+)?president\s+(?:donald\s+(?:j\.?\s+)?trump|trump)('s|'s)?",
        r"(?i)\bex[-\s]?president\s+(?:donald\s+(?:j\.?\s+)?trump|trump)('s|'s)?",
    ]:
        text = re.sub(pat, repl, text)
    return text


# ---------------------------------------------------------------------------
# Caption generation
# ---------------------------------------------------------------------------

SYS_PROMPT = (
    "You are a sharp political analyst. Rewrite the source material into a short, clear social post "
    "under 1300 characters using exactly 2 simple paragraphs. The first paragraph must be the "
    "most important summary in 250 characters or fewer, and it must include all hashtags. Use "
    "3 to 5 relevant hashtags total, prioritizing the main people the post is about, then a "
    "single-word subject hashtag that helps discovery in trending news, then any remaining "
    "relevant tags. Replace the normal word/phrase in the sentence with the hashtag version "
    "(example: use #DonaldTrump in the sentence instead of Donald Trump), rather than adding a "
    "separate hashtag-only line at the end. The second paragraph should add a bit more context "
    "with verified facts, dates, and numbers when relevant. Include direct quotes "
    "where available. Verify names and quotes carefully. Any hashtag that appears in the caption "
    "body counts toward the same total of 3 to 5 hashtags. Never use these hashtags: #Trump, "
    "#ICE, #DonaldTrump, #Epstein, #JeffreyEpstein. Avoid speculation, flourish, links, or Trump's current "
    "office status. Do not refer to the source as a transcript, clip, speech, interview, or video unless that is explicitly certain. "
    "Do not write phrases like during his speech, in the transcript, in this clip, or in the video. "
    "Write as if you are describing the underlying event or claim directly."
)


def generate_caption(
    transcript: str,
    speaker_name: str = "",
    extra_prompt: str = "",
) -> str:
    """Generate a social-media caption from a transcript."""
    try:
        news_context = get_latest_news_summary(transcript)
        if news_context.startswith("LATEST NEWS CONTEXT:\nNo recent news") or news_context.startswith("LATEST NEWS CONTEXT:\nUnable"):
            news_context = "LATEST NEWS CONTEXT:\nNo external news context available. Focus solely on the transcript for analysis.\n\n"

        sys = SYS_PROMPT
        if speaker_name:
            sys += (
                f" The speaker in the video is {speaker_name}. Mention their name once, then "
                "refer to them with he, she, or they. If gender is unclear, use they. "
                "Do not repeat their name multiple times."
            )
        if extra_prompt:
            sys += f" Additional instructions: {extra_prompt.strip()}"

        user_content = f"{news_context}\n\nTRANSCRIPT:\n{transcript}"

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": sys},
                {"role": "user", "content": user_content},
            ],
            max_tokens=500,
            temperature=0.35,
        )
        text = response.choices[0].message.content.strip()
        return _format_caption(_sanitize(text))

    except Exception as e:
        return f"Error generating caption: {e}"
