from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from analyzers.trend_analyzer import TrendAnalyzer
from config.settings import AppConfig


@pytest.fixture
def config():
    cfg = AppConfig.__new__(AppConfig)
    cfg.baseline_days = 30
    cfg.candidate_days = 7
    cfg.z_score_threshold = 1.5
    return cfg


@pytest.fixture
def analyzer(config, repo):
    return TrendAnalyzer(config, repo)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ingest(repo, make_record, username, video_id, views, published_date):
    repo.bulk_ingest_apify_data([make_record(
        username=username,
        video_id=video_id,
        url=f"https://www.instagram.com/p/{video_id}/",
        views=views,
        published_date=published_date,
    )])


def _baseline_date(days_ago: int) -> datetime:
    """Returns a tz-naive UTC datetime that falls in the baseline window (>7 days old)."""
    return datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(days=days_ago)


def _candidate_date(days_ago: int = 2) -> datetime:
    """Returns a tz-naive UTC datetime that falls in the candidate window (<= 7 days old)."""
    return datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(days=days_ago)


# ---------------------------------------------------------------------------
# process_data integration tests
# ---------------------------------------------------------------------------

class TestProcessData:
    def test_empty_db_returns_empty(self, analyzer):
        assert analyzer.process_data().empty

    def test_candidate_without_baseline_excluded(self, analyzer, repo, make_record):
        # No baseline videos — creator should be skipped
        _ingest(repo, make_record, "alice", "v1", 1000, _candidate_date())
        assert analyzer.process_data().empty

    def test_video_older_than_candidate_window_not_scored(self, analyzer, repo, make_record):
        # Seed a baseline and a "candidate" that is actually too old
        for i, v in enumerate([100, 110, 90, 105, 95]):
            _ingest(repo, make_record, "alice", f"b{i}", v, _baseline_date(20 + i))
        _ingest(repo, make_record, "alice", "old", 9000, _baseline_date(15))
        # "old" is 15 days ago — outside the 7-day candidate window → not scored
        assert analyzer.process_data().empty

    def test_identifies_viral_candidate(self, analyzer, repo, make_record):
        for i, v in enumerate([100, 110, 90, 105, 95]):
            _ingest(repo, make_record, "alice", f"b{i}", v, _baseline_date(20 + i))
        _ingest(repo, make_record, "alice", "spike", 10000, _candidate_date())

        result = analyzer.process_data()
        assert len(result) == 1
        assert result.iloc[0]["video_id"] == "spike"

    def test_non_viral_candidate_not_returned(self, analyzer, repo, make_record):
        for i, v in enumerate([100, 110, 90, 105, 95]):
            _ingest(repo, make_record, "alice", f"b{i}", v, _baseline_date(20 + i))
        _ingest(repo, make_record, "alice", "normal", 102, _candidate_date())
        assert analyzer.process_data().empty

    def test_already_transcribed_outlier_excluded(self, analyzer, repo, make_record):
        for i, v in enumerate([100, 110, 90, 105, 95]):
            _ingest(repo, make_record, "alice", f"b{i}", v, _baseline_date(20 + i))
        _ingest(repo, make_record, "alice", "spike", 10000, _candidate_date())

        repo.save_extracted_hook("spike", "already done")
        assert analyzer.process_data().empty

    def test_z_score_persisted_to_db_on_first_detection(self, analyzer, repo, make_record):
        for i, v in enumerate([100, 110, 90, 105, 95]):
            _ingest(repo, make_record, "alice", f"b{i}", v, _baseline_date(20 + i))
        _ingest(repo, make_record, "alice", "spike", 10000, _candidate_date())

        analyzer.process_data()

        metrics = repo.get_all_latest_metrics()
        spike_row = metrics[metrics["video_id"] == "spike"].iloc[0]
        assert spike_row["view_z_score"] is not None
        assert float(spike_row["view_z_score"]) >= 1.5

    def test_first_viral_at_set_on_first_detection(self, analyzer, repo, make_record):
        for i, v in enumerate([100, 110, 90, 105, 95]):
            _ingest(repo, make_record, "alice", f"b{i}", v, _baseline_date(20 + i))
        _ingest(repo, make_record, "alice", "spike", 10000, _candidate_date())

        analyzer.process_data()

        metrics = repo.get_all_latest_metrics()
        spike_row = metrics[metrics["video_id"] == "spike"].iloc[0]
        assert spike_row["first_viral_at"] is not None

    def test_first_viral_at_not_overwritten_on_second_detection(self, analyzer, repo, make_record):
        for i, v in enumerate([100, 110, 90, 105, 95]):
            _ingest(repo, make_record, "alice", f"b{i}", v, _baseline_date(20 + i))
        _ingest(repo, make_record, "alice", "spike", 10000, _candidate_date())

        analyzer.process_data()
        metrics_before = repo.get_all_latest_metrics()
        first_viral_at = metrics_before[metrics_before["video_id"] == "spike"].iloc[0]["first_viral_at"]

        # Run again — z-score may update but first_viral_at must not change
        analyzer.process_data()
        metrics_after = repo.get_all_latest_metrics()
        first_viral_at_2 = metrics_after[metrics_after["video_id"] == "spike"].iloc[0]["first_viral_at"]

        assert first_viral_at == first_viral_at_2


# ---------------------------------------------------------------------------
# _score_candidates unit tests
# ---------------------------------------------------------------------------

class TestScoreCandidates:
    def test_creator_with_single_baseline_video_skipped(self, analyzer):
        candidates = pd.DataFrame({
            "ownerUsername": ["alice"],
            "videoPlayCount": [1000],
            "video_id": ["v1"],
        })
        baseline = pd.DataFrame({
            "ownerUsername": ["alice"],
            "videoPlayCount": [500],
        })
        out = analyzer._score_candidates(candidates, baseline)
        # Only 1 baseline row → std is NaN → creator dropped
        assert out.empty

    def test_z_score_computed_correctly(self, analyzer):
        baseline = pd.DataFrame({
            "ownerUsername": ["alice"] * 4,
            "videoPlayCount": [100, 110, 90, 100],
        })
        candidates = pd.DataFrame({
            "ownerUsername": ["alice"],
            "videoPlayCount": [200],
            "video_id": ["v1"],
        })
        out = analyzer._score_candidates(candidates, baseline)
        assert len(out) == 1
        assert out.iloc[0]["new_z_score"] > 0

    def test_different_creators_scored_independently(self, analyzer):
        baseline = pd.DataFrame({
            "ownerUsername": ["alice", "alice", "bob", "bob"],
            "videoPlayCount": [100, 100, 50, 50],
        })
        candidates = pd.DataFrame({
            "ownerUsername": ["alice", "bob"],
            "videoPlayCount": [100, 50],  # same as mean → z = 0
            "video_id": ["va", "vb"],
        })
        out = analyzer._score_candidates(candidates, baseline)
        assert (out["new_z_score"] == 0).all()


# ---------------------------------------------------------------------------
# _filter / _persist unit tests
# ---------------------------------------------------------------------------

class TestPersistZScores:
    def test_lower_z_does_not_overwrite(self, analyzer, repo, make_record):
        repo.bulk_ingest_apify_data([make_record(video_id="v1")])
        repo.update_z_score("v1", 3.0)

        outliers = pd.DataFrame({
            "video_id": ["v1"],
            "new_z_score": [2.0],
            "view_z_score": [3.0],
            "hook_text": [None],
        })
        analyzer._persist_z_scores(outliers)

        metrics = repo.get_all_latest_metrics()
        assert float(metrics[metrics["video_id"] == "v1"].iloc[0]["view_z_score"]) == 3.0

    def test_higher_z_overwrites(self, analyzer, repo, make_record):
        repo.bulk_ingest_apify_data([make_record(video_id="v1")])
        repo.update_z_score("v1", 1.8)

        outliers = pd.DataFrame({
            "video_id": ["v1"],
            "new_z_score": [4.0],
            "view_z_score": [1.8],
            "hook_text": [None],
        })
        analyzer._persist_z_scores(outliers)

        metrics = repo.get_all_latest_metrics()
        assert float(metrics[metrics["video_id"] == "v1"].iloc[0]["view_z_score"]) == 4.0
