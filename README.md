# 🎣 TrendLens
*Reverse-engineering short-form virality with AI.*

## 📖 Vision
TrendLens is being built as the ultimate AI copilot for short-form content creators. The long-term goal is to create a complete, multi-platform intelligence tool (Instagram, TikTok, YouTube Shorts) that doesn't just show you vanity metrics, but actually analyzes *why* a video went viral and gives actionable, AI-driven advice on how to make better content.

## 🛠️ Current State (MVP)
Currently, the platform focuses on **Instagram Reels**. It acts as a modular data pipeline that:
1. **Identifies True Outliers:** Uses statistical Z-scores to find videos that performed exceptionally well *relative to a specific creator's baseline* (ignoring inflated numbers from mega-influencers).
2. **Extracts the Hook:** Bypasses anti-bot measures to securely download the CDN audio track of viral outliers.
3. **AI Transcription:** Passes the audio through local OpenAI Whisper models to accurately extract the "Hook" (the critical first 3-5 seconds of spoken audio).

The result is a clean dataset of proven viral hooks in your specific niche, ready to be studied or fed into an LLM for content strategy.

## 🗺️ The Roadmap
Our architecture is fully Object-Oriented and designed to scale. Upcoming features include:
- [ ] **Multi-Platform Support:** `TikTokAnalyzer` and `YTShortsAnalyzer` modules.
- [ ] **Deep Insights Engine:** Combining views, likes, and comments into weighted Engagement Z-Scores.
- [ ] **AI Content Strategist:** Passing transcribed hooks into an LLM (e.g., Claude/GPT) to categorize hook psychology (e.g., "Negative Hook", "Curiosity Loop") and suggest scripts.
- [ ] **Web Interface:** A sleek Streamlit/FastAPI frontend for drag-and-drop analysis.

## 🏗️ Architecture
This project is built with scalability in mind, using a modular Object-Oriented architecture. 

```text
ai-content-helper/
├── config/                  # Centralized settings (AppConfig)
├── core/                    # Core services (Downloader, Transcriber, Pipeline)
├── analyzers/               # Platform-specific logic (InstagramAnalyzer)
├── data/                    # Local storage for CSVs and temp audio
│   ├── input/               # Drop your Apify CSVs here
│   ├── output/              # Final viral hook CSVs are saved here
│   └── temp/                # Temporary audio files (.m4a) during processing
├── main.py                  # Entry point to execute the pipeline
└── requirements.txt         # Python dependencies

```

## ⚙️ Prerequisites

1. **Python 3.8+** installed.
2. **FFmpeg** installed on your system (Required by OpenAI Whisper for audio processing).
* **Windows:** `winget install ffmpeg` 
* **Mac:** `brew install ffmpeg`
* **Linux:** `sudo apt update && sudo apt install ffmpeg`



## 🚀 Installation

1. Clone the repository:
```bash
git clone git clone https://github.com/walidght/viral-hook-extractor.git
cd viral-hook-extractor

```


2. Create and activate a virtual environment:
```bash
python -m venv venv
# Windows: venv\Scripts\activate
# Mac/Linux: source venv/bin/activate

```


3. Install dependencies:
```bash
pip install -r requirements.txt

```



## 📊 Usage

**Step 1: Get the Data**

1. Run the [Apify Instagram Scraper](https://apify.com/apify/instagram-scraper) for your target profiles.
2. Export the results as a CSV.
3. Place the CSV into the `data/input/` folder.

**Step 2: Configure**
Open `main.py` and ensure the `AppConfig` matches your input filename:

```python
config = AppConfig(
    input_csv="data/input/your_apify_data.csv",
    output_csv="data/output/viral_hooks.csv",
    z_score_threshold=1.5
)

```

**Step 3: Run the Pipeline**

```bash
python main.py

```

## 🧠 How it Works

1. **Analyze:** Calculates an engagement Z-score for each creator in the CSV.
2. **Filter:** Isolates videos that performed >1.5 standard deviations above that specific creator's average.
3. **Download:** Uses the direct `audioUrl` from the CDN to download just the audio track (bypassing anti-bot measures).
4. **Transcribe:** Feeds the audio into local OpenAI Whisper (`base` model).
5. **Extract:** Parses the AI transcript to grab the first sentences (the hook).
6. **Clean:** Deletes the temporary audio and outputs a clean CSV with the viral hooks.