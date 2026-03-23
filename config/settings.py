from dataclasses import dataclass


@dataclass
class AppConfig:
    """Centralized configuration for easy injection via CLI or Web Interface."""
    input_csv: str = "data/input/apify_data.csv"
    output_csv: str = "data/output/viral_hooks.csv"
    z_score_threshold: float = 1.5
    baseline_days: int = 30
    temp_dir: str = "data/temp"
    whisper_model: str = "base"
    hook_sentence_count: int = -1  # -1 means full transcription, otherwise it limits to the first N sentences
    db_path: str = "data/trendlens.db"
    google_sheet_csv_url: str = "https://docs.google.com/spreadsheets/d/e/xyz/pub?gid=0&single=true&output=csv"
    scrape_interval_days: int = 7
