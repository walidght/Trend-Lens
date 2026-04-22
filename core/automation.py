import csv
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Optional

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

    # ==========================================
    # PUBLIC WORKFLOWS
    # ==========================================

    def run_auto_sync(self, platform_name: str, sheet_id: int, max_items: int = 30) -> dict:
        """Scans the DB for profiles due for a refresh and scrapes them."""
        platform_data = self._lookup_platform(platform_name)
        if not platform_data:
            return {"status": "error", "message": f"Invalid platform mapping: {platform_name}"}

        base_platform = platform_data["base_platform"]
        scrape_cutoff = datetime.now() - timedelta(days=self.config.scrape_interval_days)
        cutoff_str = scrape_cutoff.strftime('%Y-%m-%d %H:%M:%S')

        usernames = self.repo.get_creators_due_for_scrape(base_platform, cutoff_str, sheet_id)
        if not usernames:
            return {"status": "success", "message": "No profiles due for scraping.", "new_videos": 0, "new_metrics": 0}

        content_cutoff = datetime.now() - timedelta(days=7)
        return self._scrape_and_ingest(platform_name, base_platform, usernames, max_items, cutoff_date=content_cutoff)

    def run_backfill(self, new_creators: List[Tuple[str, str]], max_items: Optional[int] = None) -> dict:
        """Scrapes history for a specific set of (username, base_platform) pairs.

        Used after a fresh sheet sync to seed baseline metrics for newly-added
        creators. Groups creators by platform and issues one Apify run per platform.
        """
        if not new_creators:
            return {"status": "success", "message": "No creators to backfill.", "new_videos": 0, "new_metrics": 0}

        limit = max_items if max_items is not None else self.config.backfill_max_items

        # Group usernames by their base platform
        by_platform: dict = defaultdict(list)
        for username, base_platform in new_creators:
            by_platform[base_platform].append(username)

        agg_videos = 0
        agg_metrics = 0
        failures: List[str] = []
        successes: List[str] = []

        for base_platform, usernames in by_platform.items():
            platform_name = self._platform_name_for(base_platform)
            if not platform_name:
                logger.warning(f"No PLATFORM_MAPPINGS entry found for base_platform='{base_platform}'. Skipping.")
                failures.append(f"{base_platform} (unmapped)")
                continue

            backfill_cutoff = datetime.now() - timedelta(days=self.config.baseline_days)
            result = self._scrape_and_ingest(platform_name, base_platform, usernames, limit, cutoff_date=backfill_cutoff)
            if result["status"] == "success":
                agg_videos += result.get("new_videos", 0)
                agg_metrics += result.get("new_metrics", 0)
                successes.append(f"{base_platform} ({len(usernames)})")
            else:
                failures.append(f"{base_platform}: {result['message']}")

        status = "success" if not failures else ("partial" if successes else "error")
        summary_parts = []
        if successes:
            summary_parts.append(f"Backfilled {', '.join(successes)}")
        if failures:
            summary_parts.append(f"Failed: {', '.join(failures)}")

        return {
            "status": status,
            "message": ". ".join(summary_parts) if summary_parts else "No platforms processed.",
            "new_videos": agg_videos,
            "new_metrics": agg_metrics,
        }

    # ==========================================
    # INTERNAL HELPERS
    # ==========================================

    def _scrape_and_ingest(self, platform_name: str, base_platform: str, usernames: List[str], max_items: int, cutoff_date: Optional[datetime] = None) -> dict:
        """Builds URLs, triggers the scraper, and ingests the results."""
        platform_data = PLATFORM_MAPPINGS.get(platform_name, {})
        actor_id = platform_data.get("actor_id")
        if not actor_id:
            return {"status": "error", "message": f"No actor_id configured for {platform_name}."}

        builder = platform_data.get("run_input_builder")
        if not builder:
            return {"status": "error", "message": f"No run_input_builder configured for {platform_name}."}

        try:
            urls = [build_profile_url(base_platform, u) for u in usernames]
        except ValueError as e:
            logger.error(str(e))
            return {"status": "error", "message": str(e)}

        run_input = builder(urls, max_items, cutoff_date)

        raw_data = self.scraper.run_actor(run_input, target_identifier=actor_id)
        self._save_raw_csv(raw_data, platform_name, usernames)
        if not raw_data:
            return {"status": "error", "message": "Scraper returned no data."}

        df = pd.DataFrame(raw_data)
        stats = self.ingestor.ingest_dataframe(df, platform_name=platform_name)

        return {
            "status": "success",
            "message": f"Synced {len(usernames)} {base_platform} profiles.",
            "new_videos": stats["new_videos"],
            "new_metrics": stats["new_metrics"],
        }

    def _save_raw_csv(self, raw_data: list, platform_name: str, usernames: List[str]) -> None:
        if not raw_data:
            return
        log_dir = Path("data/apify_logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        slug = "_".join(usernames[:3])
        filename = log_dir / f"{platform_name}_{slug}_{timestamp}.csv"
        keys = list(raw_data[0].keys())
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(raw_data)
        logger.info(f"Saved raw Apify response to {filename}")

    @staticmethod
    def _lookup_platform(platform_name: str) -> Optional[dict]:
        data = PLATFORM_MAPPINGS.get(platform_name)
        if not data:
            logger.error(f"Invalid platform name: {platform_name}")
            return None
        if not data.get("base_platform"):
            logger.error(f"Platform mapping for '{platform_name}' is missing 'base_platform'.")
            return None
        return data

    @staticmethod
    def _platform_name_for(base_platform: str) -> Optional[str]:
        """Reverse-lookup: find the PLATFORM_MAPPINGS display name for a base platform."""
        for name, data in PLATFORM_MAPPINGS.items():
            if data.get("base_platform") == base_platform:
                return name
        return None
