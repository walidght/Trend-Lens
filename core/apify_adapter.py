import logging
from apify_client import ApifyClient
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class ApifyAdapter():
    """Concrete implementation for the Apify API."""

    def __init__(self, api_token: str):
        if not api_token:
            logger.error("Apify API token is missing!")
        self.client = ApifyClient(api_token)

    def run_actor(self, run_input: Dict[str, Any], target_identifier: str) -> List[Dict[str, Any]]:

        logger.info(
            f"Triggering Apify Cloud ({target_identifier}) for {run_input} URLs...")

        try:
            # Synchronous call - perfectly fine for background cron jobs!
            run = self.client.actor(
                target_identifier).call(run_input=run_input)
            logger.info("Apify run complete! Fetching dataset...")

            dataset = self.client.dataset(
                run["defaultDatasetId"]).list_items().items
            return dataset

        except Exception as e:
            logger.error(f"Apify API execution failed: {e}")
            return []
