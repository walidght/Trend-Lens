import requests
from pathlib import Path
import logging

# __name__ automatically gets the name of the current file/module (e.g., 'core.downloader')
logger = logging.getLogger(__name__)

class MediaDownloader:
    """Handles all network requests for downloading media."""

    @staticmethod
    def download_audio(url: str, output_path: Path) -> bool:
        try:
            response = requests.get(url, stream=True, timeout=15)
            response.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error downloading {url}: {e}")
            return False