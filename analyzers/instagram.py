import pandas as pd
import logging
from config import AppConfig
from core import TrendLensRepository

logger = logging.getLogger(__name__)


class InstagramAnalyzer:
    """Pulls data from SQLite, calculates insights, and identifies viral outliers."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    def process_data(self) -> pd.DataFrame:
        logger.info("Loading latest video metrics from SQLite database...")

        df = self.repo.get_videos_missing_hooks()

        if df.empty:
            logger.info("No new videos require analysis.")
            return df

        df = self._calculate_insights(df)

        return self._filter_outliers(df)

    def _calculate_insights(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates Z-Scores based on the fetched SQLite data."""
        # Ensure we don't have math errors on nulls
        df['videoPlayCount'] = df['videoPlayCount'].fillna(0).astype(int)

        # Calculate Z-Score, grouped by creator
        df['view_z_score'] = df.groupby('ownerUsername')['videoPlayCount'].transform(
            # We add a check for len(x) > 1 because standard deviation of 1 item is NaN
            lambda x: (x - x.mean()) / x.std() if len(x) > 1 else 0
        )

        # Fill any lingering NaNs with 0
        df['view_z_score'] = df['view_z_score'].fillna(0)

        return df

    def _filter_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filters out average videos and keeps only the viral ones."""
        outliers = df[df['view_z_score'] >=
                      self.config.z_score_threshold].copy()
        logger.info(
            f"Identified {len(outliers)} viral outliers ready for hook extraction.")
        return outliers
