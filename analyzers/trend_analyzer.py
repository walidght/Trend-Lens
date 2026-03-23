import pandas as pd
import logging
from config import AppConfig
from core import TrendLensRepository

logger = logging.getLogger(__name__)


class TrendAnalyzer:
    """A universal AI pipeline that detects mathematical outliers across any platform."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    def process_data(self, sheet_id: int = None) -> pd.DataFrame:
        logger.info("Loading latest video metrics from SQLite database...")

        df = self.repo.get_all_latest_metrics(sheet_id)


        if df.empty:
            logger.info("No videos found in database.")
            return df

        # Convert the Apify string timestamp into a real Datetime object
        # errors='coerce' turns bad data into NaT (Not a Time) so it doesn't crash
        df['published_date'] = pd.to_datetime(
            df['published_date'], utc=True, errors='coerce')

        # Calculate the cutoff date (e.g., 30 days ago)
        cutoff_date = pd.Timestamp.utcnow() - pd.Timedelta(days=self.config.baseline_days)

        # Filter the dataframe to only include recent videos
        recent_df = df[df['published_date'] >= cutoff_date].copy()

        if recent_df.empty:
            logger.info(
                f"No videos found in the last {self.config.baseline_days} days.")
            return recent_df

        logger.info(
            f"Calculating baseline using {len(recent_df)} videos from the last {self.config.baseline_days} days.")

        recent_df = self._calculate_insights(recent_df)

        outliers_df = self._filter_outliers(recent_df)

        # Filter out the ones we ALREADY transcribed!
        pending_outliers = outliers_df[outliers_df['hook_text'].isnull()].copy(
        )

        logger.info(f"Found {len(outliers_df)} total recent outliers.")
        logger.info(
            f"{len(pending_outliers)} of those are NEW and need AI transcription.")

        return pending_outliers

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
