import pandas as pd
import logging
from datetime import datetime
from config import AppConfig
from core.repository import TrendLensRepository

logger = logging.getLogger(__name__)


class ApifyIngestor:
    """Parses raw Apify CSV data and safely inserts it into the SQLite relational tables."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    def ingest_dataframe(self, df: pd.DataFrame) -> dict:
        """Processes the dataframe and returns a summary of rows inserted."""

        # Standardize column names in case Apify changes them slightly
        if 'ownerUsername' not in df.columns and 'ownerFullName' in df.columns:
            df.rename(columns={'ownerFullName': 'ownerUsername'}, inplace=True)

        # Fallbacks for missing columns
        for col in ['videoPlayCount', 'likesCount', 'commentsCount']:
            if col not in df.columns:
                df[col] = 0

        if 'audioUrl' not in df.columns:
            df['audioUrl'] = None

        # Determine Collab flag
        if 'coauthorProducers/0/username' in df.columns:
            df['is_collab'] = df['coauthorProducers/0/username'].notna()
        elif 'taggedUsers/0/username' in df.columns:
            df['is_collab'] = df['taggedUsers/0/username'].notna()
        else:
            df['is_collab'] = False

        total_stats = {"new_videos": 0, "new_metrics": 0}
        today_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for _, row in df.iterrows():
            username = str(row.get('ownerUsername', '')).strip()
            url = str(row.get('url', ''))

            if not username or not url or pd.isna(url):
                continue

            # 1. Extract the unique Instagram Shortcode (e.g., C123XYZ)
            video_id = url.rstrip('/').split('/')[-1]

            row_stats = self.repo.ingest_apify_row(
                username=username,
                video_id=video_id,
                url=url,
                audio_url=row.get('audioUrl'),
                published_date=row.get('timestamp'),
                views=int(row['videoPlayCount']) if pd.notna(
                    row['videoPlayCount']) else 0,
                likes=int(row['likesCount']) if pd.notna(
                    row['likesCount']) else 0,
                comments=int(row['commentsCount']) if pd.notna(
                    row['commentsCount']) else 0,
                is_collab=bool(row['is_collab']),
                scraped_at=today_str
            )

            # Aggregate stats for the Streamlit UI
            total_stats["new_videos"] += row_stats["new_videos"]
            total_stats["new_metrics"] += row_stats["new_metrics"]

        logger.info(
            f"Ingestion complete. Added {total_stats['new_videos']} new videos and {total_stats['new_metrics']} metric snapshots.")
        return total_stats
