import pandas as pd
import logging
from datetime import datetime
from config.settings import AppConfig
from core.repository import TrendLensRepository
from config.mappings import PLATFORM_MAPPINGS

logger = logging.getLogger(__name__)


class DataIngestor:
    """Normalizes raw data from any source into a standard format for the database."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    def ingest_dataframe(self, df: pd.DataFrame, platform_name: str) -> dict:
        """
        Takes a raw dataframe and a dictionary mapping the raw columns to our standard columns.
        Example platform_map for Instagram: {'videoPlayCount': 'views', 'likesCount': 'likes'}
        """
        platform_data = PLATFORM_MAPPINGS.get(platform_name)

        if not platform_data:
            logger.error(
                f"No column mapping found for platform: {platform_name}")
            return {"new_videos": 0, "new_metrics": 0}
        
        column_map = platform_data.get("columns", {})
        custom_transforms = platform_data.get("custom_transforms", {})

        # Apply custom transforms FIRST (while the raw column names still exist)
        for new_col_name, transform_function in custom_transforms.items():
            df[new_col_name] = transform_function(df)

        # Standardize the remaining 1-to-1 column names
        df = df.rename(columns=column_map)

        # Ensure all required standard columns exist (fallback to 0 or None)
        standard_cols = ['username', 'url', 'views', 'likes',
                         'comments', 'audio_url', 'published_date', 'is_collab']
        for col in standard_cols:
            if col not in df.columns:
                df[col] = 0 if col in ['views', 'likes', 'comments'] else None

        # 3. Clean NaNs for SQLite
        df = df.where(pd.notnull(df), None)
        today_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        records = []

        # 4. Build the standardized records list
        for _, row in df.iterrows():
            username = str(row.get('username', '')).strip(
            ) if row.get('username') else ''
            url = str(row.get('url', '')) if row.get('url') else ''

            if not username or not url:
                continue

            # Extract a unique video ID from the URL (works for IG, TikTok, and YT)
            video_id = url.rstrip('/').split('/')[-1]

            records.append({
                "username": username,
                "video_id": video_id,
                "url": url,
                "audio_url": row.get('audio_url'),
                "published_date": row.get('published_date'),
                "views": int(row['views']) if row.get('views') is not None else 0,
                "likes": int(row['likes']) if row.get('likes') is not None else 0,
                "comments": int(row['comments']) if row.get('comments') is not None else 0,
                "is_collab": bool(row.get('is_collab')),
                "scraped_at": today_str
            })

        if not records:
            return {"new_videos": 0, "new_metrics": 0}

        # 5. Pass to the universal bulk ingestor!
        return self.repo.bulk_ingest_apify_data(records)
