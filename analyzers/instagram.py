import pandas as pd
import logging
from config import AppConfig


# __name__ automatically gets the name of the current file/module (e.g., 'core.downloader')
logger = logging.getLogger(__name__)


class InstagramAnalyzer:
    """Handles platform-specific data cleaning and insight generation."""

    def __init__(self, config: AppConfig):
        self.config = config

    def process_data(self) -> pd.DataFrame:
        logger.info(f"Loading data from {self.config.input_csv}")
        df = pd.read_csv(self.config.input_csv)

        df = self._calculate_insights(df)
        return self._filter_outliers(df)

    def _calculate_insights(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add new metric calculations here in the future."""
        # Calculate Z-Score
        df['view_z_score'] = df.groupby('ownerUsername')['videoPlayCount'].transform(
            lambda x: (x - x.mean()) / x.std()
        )

        # Flag Collaborations
        if 'coauthorProducers/0/username' in df.columns:
            df['is_collab'] = df['coauthorProducers/0/username'].notna()
        elif 'taggedUsers/0/username' in df.columns:
            df['is_collab'] = df['taggedUsers/0/username'].notna()
        else:
            df['is_collab'] = False

        return df

    def _filter_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        outliers = df[df['view_z_score'] >=
                      self.config.z_score_threshold].copy()
        logger.info(f"Identified {len(outliers)} viral outliers.")
        return outliers
