import io
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

import requests
import pandas as pd

from config.settings import AppConfig
from config.mappings import build_profile_url
from core.repository import TrendLensRepository

logger = logging.getLogger(__name__)


class SheetIngestor:
    """Handles fetching target profiles from Google Sheets and managing the scrape queue."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    # ==========================================
    # PUBLIC WORKFLOW METHODS
    # ==========================================

    def sync_creators_to_db(self, sheet_id: int, sheet_url: str) -> List[Tuple[str, str]]:
        """Main pipeline: Fetches CSV, cleans data, inserts creators, and links to the sheet.

        Returns the list of (username, platform) pairs that were newly inserted, so
        callers can trigger a history backfill for those profiles.
        """
        try:
            df = self._fetch_csv_from_url(sheet_url)

            df = self._clean_and_validate_dataframe(df)
            if df.empty:
                return []

            creators_data = list(zip(df['username'], df['platform']))
            new_creators = self.repo.bulk_insert_creators(creators_data)

            self._link_creators_by_platform(sheet_id, df)

            logger.info(f"Synced {len(df)} creators from Sheet. {len(new_creators)} new profiles added to DB.")
            return new_creators

        except requests.RequestException as e:
            logger.error(f"Network error while fetching Google Sheet: {e}")
            return []
        except ValueError as e:
            logger.error(f"Data validation error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error syncing Google Sheet: {e}")
            return []

    def generate_scrape_list(self, platform: str = 'instagram', sheet_id: int = None) -> List[str]:
        """Finds creators who haven't been scraped recently and formats them for Apify."""
        cutoff_date = datetime.now() - timedelta(days=self.config.scrape_interval_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        usernames = self.repo.get_creators_due_for_scrape(platform, cutoff_str, sheet_id)

        try:
            urls = [build_profile_url(platform, user) for user in usernames]
        except ValueError as e:
            logger.error(str(e))
            return []

        logger.info(f"Generated scrape list: {len(urls)} {platform} profiles are due for updates.")
        return urls

    # ==========================================
    # PRIVATE HELPER METHODS
    # ==========================================

    def _fetch_csv_from_url(self, sheet_url: str) -> pd.DataFrame:
        """Handles the network request to Google Sheets."""
        clean_url = sheet_url.strip()
        
        # Auto-fix standard browser links to CSV export links
        if "/edit" in clean_url:
            clean_url = f"{clean_url.split('/edit')[0]}/export?format=csv"

        logger.info(f"Downloading Google Sheet from: {clean_url}")
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(clean_url, headers=headers, timeout=15)
        response.raise_for_status()

        return pd.read_csv(io.StringIO(response.text))

    def _clean_and_validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensures the dataframe has correct columns and clean data."""
        if 'username' not in df.columns or 'platform' not in df.columns:
            raise ValueError("Google Sheet must contain 'username' and 'platform' columns.")

        # Copy to avoid SettingWithCopyWarning
        df = df.copy()

        # Clean string formats
        df['username'] = df['username'].astype(str).str.strip()
        df['platform'] = df['platform'].astype(str).str.strip().str.lower()
        
        # Drop rows with missing crucial data or empty strings
        df = df.dropna(subset=['username', 'platform'])
        df = df[(df['username'] != '') & (df['platform'] != '')]

        return df

    def _link_creators_by_platform(self, sheet_id: int, df: pd.DataFrame):
        """Groups creators by platform and links them to the sheet in batches."""
        for platform in df['platform'].unique():
            platform_usernames = df[df['platform'] == platform]['username'].tolist()
            self.repo.link_creators_to_sheet(sheet_id, platform_usernames, platform)