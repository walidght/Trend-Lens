import logging
import pandas as pd
from core.repository import TrendLensRepository
from core.ingestors import DataIngestor
from core.scraper import BaseScraper
from config.mappings import PLATFORM_MAPPINGS

logger = logging.getLogger(__name__)


class AutomationOrchestrator:
    """Manages the end-to-end automated scraping and ingestion workflow."""

    def __init__(self, repo: TrendLensRepository, scraper: BaseScraper, ingestor: DataIngestor):
        self.repo = repo
        self.scraper = scraper
        self.ingestor = ingestor

    def run_auto_sync(self, platform_name: str, sheet_id: int, max_items: int = 30) -> dict:
        """Runs the fully automated pipeline for a specific platform and sheet."""

        # 1. Look up the configuration mapping
        platform_data = PLATFORM_MAPPINGS.get(platform_name)
        if not platform_data:
            logger.error("Invalid platform name provided.")
            return {"status": "error", "message": "Invalid platform mapping."}

        actor_id = platform_data.get("actor_id")
        # Extract the base platform ('instagram' from 'Instagram (Apify)') for the DB query
        base_platform = platform_name.split()[0].lower()

        # 2. Get profiles due for scraping (Currently 7-day rule, later Queue table)
        from datetime import datetime, timedelta
        cutoff_date = datetime.now() - timedelta(days=7)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        usernames = self.repo.get_creators_due_for_scrape(
            base_platform, cutoff_str, sheet_id)

        if not usernames:
            return {"status": "success", "message": "No profiles due for scraping.", "new_videos": 0}

        # Format URLs
        # TODO: abstract this into mappings.py later, also in sheet_ingestor.py
        urls = [f"https://www.instagram.com/{u}/" if base_platform ==
                'instagram' else f"https://www.tiktok.com/@{u}" for u in usernames]

        # 3. Trigger the Scraper abstraction
        raw_data = self.scraper.scrape_profiles(
            urls, target_identifier=actor_id, max_items=max_items)

        if not raw_data:
            return {"status": "error", "message": "Scraper returned no data."}

        # 4. Pass to the Ingestor abstraction
        df = pd.DataFrame(raw_data)
        stats = self.ingestor.ingest_dataframe(df, platform_name=platform_name)

        return {
            "status": "success",
            "message": f"Successfully synced {len(usernames)} profiles.",
            "new_videos": stats["new_videos"],
            "new_metrics": stats["new_metrics"]
        }
