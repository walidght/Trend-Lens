from dataclasses import dataclass
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class AppConfig:
    def __init__(self):
        """Centralized configuration for easy injection via CLI or Web Interface."""
        self.input_csv: str = "data/input/apify_data.csv"
        self.output_csv: str = "data/output/viral_hooks.csv"
        self.z_score_threshold: float = 1.5
        self.baseline_days: int = 30
        self.temp_dir: str = "data/temp"
        self.whisper_model: str = "base"
        # -1 means full transcription, otherwise it limits to the first N sentences
        self.hook_sentence_count: int = -1
        self.db_path: str = "data/trendlens.db"
        self.google_sheet_csv_url: str = "https://docs.google.com/spreadsheets/d/e/xyz/pub?gid=0&single=true&output=csv"
        self.scrape_interval_days: int = 7
        self.apify_api_token: str = os.getenv("APIFY_API_TOKEN")

        self._validate_config()

    def _validate_config(self):
        """Fails fast if critical secrets are missing."""
        missing_keys = []
        if not self.apify_api_token:
            missing_keys.append("APIFY_API_TOKEN")

        if missing_keys:
            logger.warning(
                f"⚠️ Missing Environment Variables: {', '.join(missing_keys)}")
            logger.warning(
                "Please ensure they are set in your .env file or system environment.")
