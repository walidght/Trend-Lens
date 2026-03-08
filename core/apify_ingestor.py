import pandas as pd
import logging
from datetime import datetime
from config.settings import AppConfig
from core.repository import TrendLensRepository

logger = logging.getLogger(__name__)

class ApifyIngestor:
    """Parses raw Apify CSV data, cleans it, and passes it to the repository in bulk."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    def ingest_dataframe(self, df: pd.DataFrame) -> dict:
        """Processes the dataframe and returns a summary of rows inserted."""

        # Standardize column names
        if 'ownerUsername' not in df.columns and 'ownerFullName' in df.columns:
            df.rename(columns={'ownerFullName': 'ownerUsername'}, inplace=True)

        for col in ['videoPlayCount', 'likesCount', 'commentsCount']:
            if col not in df.columns:
                df[col] = 0

        if 'audioUrl' not in df.columns:
            df['audioUrl'] = None

        if 'coauthorProducers/0/username' in df.columns:
            df['is_collab'] = df['coauthorProducers/0/username'].notna()
        elif 'taggedUsers/0/username' in df.columns:
            df['is_collab'] = df['taggedUsers/0/username'].notna()
        else:
            df['is_collab'] = False

        df = df.where(pd.notnull(df), None)

        today_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        records = []

        for _, row in df.iterrows():
            username = str(row.get('ownerUsername', '')).strip() if row.get('ownerUsername') else ''
            url = str(row.get('url', '')) if row.get('url') else ''

            if not username or not url:
                continue

            video_id = url.rstrip('/').split('/')[-1]

            # Build the dictionary for this row
            records.append({
                "username": username,
                "video_id": video_id,
                "url": url,
                "audio_url": row.get('audioUrl'),
                "published_date": row.get('timestamp'),
                "views": int(row['videoPlayCount']) if row.get('videoPlayCount') is not None else 0,
                "likes": int(row['likesCount']) if row.get('likesCount') is not None else 0,
                "comments": int(row['commentsCount']) if row.get('commentsCount') is not None else 0,
                "is_collab": bool(row.get('is_collab')),
                "scraped_at": today_str
            })

        if not records:
            return {"new_videos": 0, "new_metrics": 0}

        # Pass the entire list in one shot!
        total_stats = self.repo.bulk_ingest_apify_data(records)

        logger.info(f"Bulk ingestion complete. Touched {total_stats['new_videos']} videos and logged {total_stats['new_metrics']} new metrics.")
        return total_stats