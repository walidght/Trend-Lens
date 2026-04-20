import logging
from datetime import datetime
from urllib.parse import urlparse
from typing import Dict, Any

import pandas as pd

from config.settings import AppConfig
from core.repository import TrendLensRepository
from config.mappings import PLATFORM_MAPPINGS

logger = logging.getLogger(__name__)


class DataIngestor:
    """Normalizes raw data from any source into a standard format for the database."""

    # 1. Define schema configurations at the class level
    STANDARD_NUMERIC_COLS = ['views', 'likes', 'comments']
    STANDARD_STRING_COLS = ['username', 'url', 'audio_url', 'published_date']

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    def ingest_dataframe(self, df: pd.DataFrame, platform_name: str) -> dict:
        """Main pipeline: Maps, cleans, and ingests a raw dataframe."""
        platform_data = PLATFORM_MAPPINGS.get(platform_name)

        if not platform_data:
            logger.error(
                f"No column mapping found for platform: {platform_name}")
            return {"new_videos": 0, "new_metrics": 0}

        try:
            # 2. Delegate distinct steps to private helper methods
            df = self._apply_mappings(df, platform_data)
            df = self._ensure_schema(df)

            base_platform = platform_data.get("base_platform", "unknown")
            records = self._format_and_clean_data(df, base_platform)

            if not records:
                logger.warning(
                    f"No valid records found after processing {platform_name} data.")
                return {"new_videos": 0, "new_metrics": 0}

            return self.repo.bulk_ingest_apify_data(records)

        except Exception as e:
            logger.error(
                f"Data ingestion pipeline failed for {platform_name}: {e}")
            return {"new_videos": 0, "new_metrics": 0}

    def _apply_mappings(self, df: pd.DataFrame, platform_data: dict) -> pd.DataFrame:
        """Applies custom transforms and renames columns based on the registry."""
        df = df.copy()  # Prevents SettingWithCopyWarning in Pandas

        custom_transforms = platform_data.get("custom_transforms", {})
        column_map = platform_data.get("columns", {})

        for new_col_name, transform_function in custom_transforms.items():
            df[new_col_name] = transform_function(df)

        return df.rename(columns=column_map)

    def _ensure_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Backfills missing columns to ensure database compliance."""
        for col in self.STANDARD_NUMERIC_COLS:
            if col not in df.columns:
                df[col] = 0

        for col in self.STANDARD_STRING_COLS:
            if col not in df.columns:
                df[col] = None

        if 'is_collab' not in df.columns:
            df['is_collab'] = False

        return df

    def _format_and_clean_data(self, df: pd.DataFrame, base_platform: str) -> list[Dict[str, Any]]:
        """Cleans data using Pandas vectorization instead of row-by-row iteration."""
        # Drop rows missing crucial identifiers early
        df = df.dropna(subset=['username', 'url'])

        # Vectorized string cleaning
        df['username'] = df['username'].astype(str).str.strip()
        df['url'] = df['url'].astype(str).str.strip()

        # Filter out empty strings
        df = df[(df['username'] != '') & (df['url'] != '')]

        if df.empty:
            return []

        # Vectorized robust URL parsing
        df['video_id'] = df['url'].apply(
            lambda x: urlparse(x).path.rstrip('/').split('/')[-1]
        )

        # Vectorized type casting and fallback filling
        for col in self.STANDARD_NUMERIC_COLS:
            # to_numeric converts weird strings to NaN, fillna(0) makes them 0, astype(int) finalizes it
            df[col] = pd.to_numeric(
                df[col], errors='coerce').fillna(0).astype(int)

        df['is_collab'] = df['is_collab'].astype(bool)

        # Datetime columns are typed as DateTime in the ORM, so hand over real
        # datetime objects (not strings). pd.to_datetime handles Apify's ISO
        # timestamps; errors='coerce' maps junk to NaT, which becomes None below.
        df['scraped_at'] = datetime.now()
        df['published_date'] = pd.to_datetime(
            df['published_date'], utc=True, errors='coerce'
        ).dt.tz_localize(None).astype(object)

        df['platform'] = base_platform

        # Clean NaNs/NaTs to None for SQLite
        df = df.where(pd.notnull(df), None)

        # Return a list of dictionaries automatically mapped to the columns
        return df.to_dict(orient='records')
