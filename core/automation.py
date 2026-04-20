import logging
from datetime import datetime, timedelta

import pandas as pd

from config.settings import AppConfig
from config.mappings import PLATFORM_MAPPINGS, build_profile_url
from core.repository import TrendLensRepository
from core.ingestors import DataIngestor

logger = logging.getLogger(__name__)


class AutomationOrchestrator:
    """Manages the end-to-end automated scraping and ingestion workflow."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository, scraper, ingestor: DataIngestor):
        self.config = config
        self.repo = repo
        self.scraper = scraper
        self.ingestor = ingestor

    def run_auto_sync(self, platform_name: str, sheet_id: int, max_items: int = 30) -> dict:
        """Runs the fully automated pipeline for a specific platform and sheet."""

        platform_data = PLATFORM_MAPPINGS.get(platform_name)
        if not platform_data:
            logger.error("Invalid platform name provided.")
            return {"status": "error", "message": "Invalid platform mapping."}

        actor_id = platform_data.get("actor_id")
        base_platform = platform_data.get("base_platform")
        if not base_platform:
            return {"status": "error", "message": f"Platform mapping for '{platform_name}' is missing 'base_platform'."}

        cutoff_date = datetime.now() - timedelta(days=self.config.scrape_interval_days)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        usernames = self.repo.get_creators_due_for_scrape(
            base_platform, cutoff_str, sheet_id)

        if not usernames:
            return {"status": "success", "message": "No profiles due for scraping.", "new_videos": 0}

        try:
            urls = [build_profile_url(base_platform, u) for u in usernames]
        except ValueError as e:
            logger.error(str(e))
            return {"status": "error", "message": str(e)}

        run_input = {
            "directUrls": urls,
            "resultsLimit": max_items,
            "resultsType": "posts",
        }

        raw_data = self.scraper.run_actor(
            run_input, target_identifier=actor_id)

        if not raw_data:
            return {"status": "error", "message": "Scraper returned no data."}

        df = pd.DataFrame(raw_data)
        stats = self.ingestor.ingest_dataframe(df, platform_name=platform_name)

        return {
            "status": "success",
            "message": f"Successfully synced {len(usernames)} profiles.",
            "new_videos": stats["new_videos"],
            "new_metrics": stats["new_metrics"]
        }
