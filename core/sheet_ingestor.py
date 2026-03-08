import pandas as pd
import logging
from datetime import datetime, timedelta
from config.settings import AppConfig
from core.repository import TrendLensRepository

logger = logging.getLogger(__name__)


class SheetIngestor:
    """Handles fetching target profiles from Google Sheets and managing the scrape queue."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    def sync_creators_to_db(self) -> int:
        """Fetches the published Google Sheet CSV and inserts new creators into SQLite."""
        logger.info(
            f"Fetching Google Sheet from: {self.config.google_sheet_csv_url}")

        try:
            # Pandas can read a web URL like a local file
            df = pd.read_csv(self.config.google_sheet_csv_url)

            # Ensure the sheet has the required columns
            if 'username' not in df.columns or 'platform' not in df.columns:
                logger.error(
                    "Your Google Sheet must contain 'username' and 'platform' columns.")
                return 0

            df['username'] = df['username'].astype(str).str.strip()
            df['platform'] = df['platform'].astype(str).str.strip().str.lower()
            df = df.dropna(subset=['username', 'platform'])

            creators_data = list(zip(df['username'], df['platform']))

            # Insert into database
            added_count = self.repo.bulk_insert_creators(creators_data)

            logger.info(
                f"Synced {len(df)} creators from Sheet. {added_count} new profiles added to DB.")
            return added_count

        except Exception as e:
            logger.error(f"Failed to sync Google Sheet: {e}")
            return 0

    def generate_scrape_list(self, platform: str = 'instagram') -> list:
        """Finds creators who haven't been scraped recently and formats them for Apify."""
        # Calculate the cutoff date (e.g., 7 days ago)
        cutoff_date = datetime.now() - timedelta(days=self.config.scrape_interval_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        usernames = self.repo.get_creators_due_for_scrape(platform, cutoff_str)

        # Format the usernames into full URLs for the Apify Actor
        urls = []
        for username in usernames:
            if platform == 'instagram':
                urls.append(f"https://www.instagram.com/{username}/")
            elif platform == 'tiktok':
                urls.append(f"https://www.tiktok.com/@{username}")
            elif platform == 'youtube':
                urls.append(f"https://www.youtube.com/{username}")

        logger.info(
            f"Generated scrape list: {len(urls)} {platform} profiles are due for updates.")
        return urls
