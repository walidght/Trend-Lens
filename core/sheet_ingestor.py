import io
import logging
from datetime import datetime, timedelta
from typing import List, Dict

import requests
import pandas as pd

from config.settings import AppConfig
from core.repository import TrendLensRepository

logger = logging.getLogger(__name__)


class SheetIngestor:
    """Handles fetching target profiles from Google Sheets and managing the scrape queue."""

    # 1. Define URL templates cleanly at the class level
    URL_TEMPLATES: Dict[str, str] = {
        'instagram': "https://www.instagram.com/{}/",
        'tiktok': "https://www.tiktok.com/@{}",
        'youtube': "https://www.youtube.com/{}"
    }

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    # ==========================================
    # PUBLIC WORKFLOW METHODS
    # ==========================================

    def sync_creators_to_db(self, sheet_id: int, sheet_url: str) -> int:
        """Main pipeline: Fetches CSV, cleans data, inserts creators, and links to the sheet."""
        try:
            # 1. Fetch and Parse
            df = self._fetch_csv_from_url(sheet_url)
            
            # 2. Validate and Clean
            df = self._clean_and_validate_dataframe(df)
            if df.empty:
                return 0

            # 3. Insert into Database
            creators_data = list(zip(df['username'], df['platform']))
            
            # TODO: To handle the "scrape immediately" feature later, we will need to update 
            # bulk_insert_creators to return a list of the *new* usernames, rather than just the count.
            # so that we can scrape 30 days of content (for newly added profiles) to calculate the mean and std for the profile
            added_count = self.repo.bulk_insert_creators(creators_data)

            # 4. Link to the specific Sheet
            self._link_creators_by_platform(sheet_id, df)

            logger.info(f"Synced {len(df)} creators from Sheet. {added_count} new profiles added to DB.")
            return added_count

        except requests.RequestException as e:
            logger.error(f"Network error while fetching Google Sheet: {e}")
            return 0
        except ValueError as e:
            logger.error(f"Data validation error: {e}")
            return 0
        except Exception as e:
            logger.error(f"Unexpected error syncing Google Sheet: {e}")
            return 0

    def generate_scrape_list(self, platform: str = 'instagram', sheet_id: int = None) -> List[str]:
        """Finds creators who haven't been scraped recently and formats them for Apify."""
        if platform not in self.URL_TEMPLATES:
            logger.error(f"Unsupported platform for URL generation: {platform}")
            return []

        # 1. Calculate the cutoff threshold
        cutoff_date = datetime.now() - timedelta(days=self.config.scrape_interval_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        # 2. Fetch due usernames
        usernames = self.repo.get_creators_due_for_scrape(platform, cutoff_str, sheet_id)

        # 3. Format URLs using the class-level template
        template = self.URL_TEMPLATES[platform]
        urls = [template.format(user) for user in usernames]

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