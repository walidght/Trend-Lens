from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseScraper(ABC):
    """Abstract interface for all third-party scraping services."""
    
    @abstractmethod
    def scrape_profiles(self, urls: List[str], target_identifier: str, max_items: int = 30) -> List[Dict[str, Any]]:
        """
        Executes a scrape and returns a list of raw dictionaries.
        :param urls: List of profile URLs.
        :param target_identifier: The internal ID for the scraper (e.g., Actor ID).
        """
        pass