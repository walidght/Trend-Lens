import pandas as pd
import logging
from config import AppConfig
from core import TrendLensRepository

logger = logging.getLogger(__name__)


class TrendAnalyzer:
    """Detects viral outliers by comparing recent videos against a prior baseline window."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository):
        self.config = config
        self.repo = repo

    def process_data(self, sheet_id: int = None) -> pd.DataFrame:
        """Main entry point.

        Splits all videos into a baseline window (days candidate_days–baseline_days old)
        and a candidate window (last candidate_days). Scores each candidate against its
        creator's baseline, updates z-scores in the DB for confirmed outliers, and returns
        the subset that still needs Whisper transcription.
        """
        logger.info("Loading latest video metrics from database...")
        df = self.repo.get_all_latest_metrics(sheet_id)

        if df.empty:
            logger.info("No videos found in database.")
            return df

        df['published_date'] = pd.to_datetime(df['published_date'], utc=True, errors='coerce')
        df = df.dropna(subset=['published_date'])

        now = pd.Timestamp.utcnow()
        candidate_cutoff = now - pd.Timedelta(days=self.config.candidate_days)
        baseline_start = now - pd.Timedelta(days=self.config.baseline_days + self.config.candidate_days)

        candidates = df[df['published_date'] >= candidate_cutoff].copy()
        baseline_pool = df[
            (df['published_date'] >= baseline_start) &
            (df['published_date'] < candidate_cutoff)
        ].copy()

        if candidates.empty:
            logger.info(f"No videos published in the last {self.config.candidate_days} days.")
            return pd.DataFrame()

        candidates = self._score_candidates(candidates, baseline_pool)

        outliers = candidates[candidates['new_z_score'] >= self.config.z_score_threshold].copy()

        if outliers.empty:
            logger.info("No viral outliers found this run.")
            return pd.DataFrame()

        self._persist_z_scores(outliers)

        pending_transcription = outliers[outliers['hook_text'].isnull()].copy()
        logger.info(
            f"Found {len(outliers)} viral outliers; "
            f"{len(pending_transcription)} still need transcription."
        )
        return pending_transcription

    def _score_candidates(self, candidates: pd.DataFrame, baseline_pool: pd.DataFrame) -> pd.DataFrame:
        """Adds new_z_score to each candidate using its creator's baseline stats.

        Creators with <2 baseline videos are skipped (std is undefined).
        """
        baseline_stats = (
            baseline_pool.groupby('ownerUsername')['videoPlayCount']
            .agg(baseline_mean='mean', baseline_std='std')
            .dropna()
        )
        # dropna removes creators whose std is NaN (single baseline video)
        baseline_stats = baseline_stats[baseline_stats['baseline_std'] > 0]

        candidates = candidates.merge(baseline_stats, on='ownerUsername', how='left')

        def _z(row):
            if pd.isna(row.get('baseline_mean')) or pd.isna(row.get('baseline_std')):
                return None
            return (row['videoPlayCount'] - row['baseline_mean']) / row['baseline_std']

        candidates['new_z_score'] = candidates.apply(_z, axis=1)
        candidates = candidates.dropna(subset=['new_z_score'])
        return candidates

    def _persist_z_scores(self, outliers: pd.DataFrame):
        """Updates the DB for each outlier where the new z is higher than the stored one."""
        for _, row in outliers.iterrows():
            stored = row.get('view_z_score')
            new_z = float(row['new_z_score'])
            if pd.isna(stored) or new_z > float(stored):
                self.repo.update_z_score(row['video_id'], new_z)
