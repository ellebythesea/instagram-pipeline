# Instagram Caption Generator

Standalone Streamlit app that takes an Instagram reel/post URL, downloads the video, transcribes the audio, and generates a ready-to-post political caption with a footer containing the original Instagram caption and username.

## Project Structure

```
instagram-caption-app/
  app.py              Main Streamlit app
  instagram.py        Downloads video + metadata via yt-dlp
  caption.py          Audio extraction, Whisper transcription, GPT-4o caption generation
  news.py             Fetches related news via Serper API for prompt context
  config.py           Loads secrets from Streamlit Cloud or environment variables
  requirements.txt    Python dependencies
  packages.txt        System-level dependencies for Streamlit Cloud (ffmpeg)
  .streamlit/
    config.toml       Dark theme config
```

## How It Works

1. You paste an Instagram URL and the speaker's name into the form.
2. The app downloads the video and extracts metadata (original caption, username) using yt-dlp.
3. Audio is extracted with ffmpeg and sent to OpenAI Whisper for transcription.
4. The transcript is combined with recent news context (via Serper) and sent to GPT-4o, which generates a two-paragraph caption with inline hashtags.
5. A footer is appended with the original Instagram caption and @username.

## Deploying to Streamlit Cloud

### 1. Push to GitHub

Create a new GitHub repo (public or private) and push this folder as the root of the repo:

```bash
cd instagram-caption-app
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/instagram-caption-app.git
git branch -M main
git push -u origin main
```

### 2. Connect on Streamlit Cloud

Go to https://share.streamlit.io and sign in with your GitHub account.

Click "New app" and fill in:

- **Repository:** YOUR_USERNAME/instagram-caption-app
- **Branch:** main
- **Main file path:** app.py

Click "Deploy".

### 3. Add Secrets

In the Streamlit Cloud dashboard, open your app's settings and go to the "Secrets" tab. Paste the following (with your actual keys):

```toml
OPENAI_API_KEY = "sk-..."
SERPER_API_KEY = "your-serper-key"
APP_PASSWORD = "whatever-password-you-want"
```

- `OPENAI_API_KEY` (required) -- used for Whisper transcription and GPT-4o caption generation.
- `SERPER_API_KEY` (optional) -- enables news context enrichment. Without it, captions still generate but without current-events context.
- `APP_PASSWORD` (optional) -- if set, the app will prompt for a password before showing the form. Leave it out to make the app open to anyone with the link.

After saving secrets, the app will automatically reboot.

## Running Locally

```bash
pip install -r requirements.txt
```

You also need ffmpeg installed on your system (`brew install ffmpeg` on macOS, `sudo apt install ffmpeg` on Ubuntu).

Set your API keys as environment variables or create a `.streamlit/secrets.toml` file:

```toml
OPENAI_API_KEY = "sk-..."
SERPER_API_KEY = "your-serper-key"
APP_PASSWORD = "your-password"
```

Then run:

```bash
streamlit run app.py
```

The app will open at http://localhost:8501.

## Optional Config

These can also be added to your secrets if you want to tweak audio processing:

```toml
TRIM_SILENCE = "true"          # Trim silence from audio before transcription
AUDIO_SAMPLE_RATE = "16000"    # Sample rate for audio extraction
AUDIO_CHANNELS = "1"           # Mono audio
AUDIO_BITRATE = "32k"          # Audio bitrate
CAPTION_SPLIT_THRESHOLD = "400" # Character threshold for splitting into 2 paragraphs
```
