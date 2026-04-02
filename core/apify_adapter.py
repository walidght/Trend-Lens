import logging
from apify_client import ApifyClient
from core.scraper import BaseScraper
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class ApifyAdapter(BaseScraper):
    """Concrete implementation for the Apify API."""
    
    def __init__(self, api_token: str):
        if not api_token:
            logger.error("Apify API token is missing!")
        self.client = ApifyClient(api_token)

    def scrape_profiles(self, urls: List[str], target_identifier: str, max_items: int = 30) -> List[Dict[str, Any]]:
        if not urls:
            return []
            
        logger.info(f"Triggering Apify Cloud ({target_identifier}) for {len(urls)} URLs...")
        
        run_input = {
            "directUrls": urls,
            "resultsLimit": max_items,
            "resultsType": "posts",
        }
        
        try:
            # Synchronous call - perfectly fine for background cron jobs!
            run = self.client.actor(target_identifier).call(run_input=run_input)
            logger.info("Apify run complete! Fetching dataset...")
            
            dataset = self.client.dataset(run["defaultDatasetId"]).list_items().items
            return dataset
            
        except Exception as e:
            logger.error(f"Apify API execution failed: {e}")
            return []