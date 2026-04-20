from datetime import datetime, timedelta

import pandas as pd
import pytest

from analyzers.trend_analyzer import TrendAnalyzer
from config.settings import AppConfig


@pytest.fixture
def config():
    cfg = AppConfig.__new__(AppConfig)
    cfg.baseline_days = 30
    cfg.z_score_threshold = 1.5
    return cfg


@pytest.fixture
def analyzer(config, repo):
    return TrendAnalyzer(config, repo)


def _ingest(repo, make_record, username, video_id, views, published_date):
    repo.bulk_ingest_apify_data([
        make_record(
            username=username,
            video_id=video_id,
            url=f"https://www.instagram.com/p/{video_id}/",
            views=views,
            published_date=published_date,
        )
    ])


class TestProcessData:
    def test_empty_db_returns_empty(self, analyzer):
        result = analyzer.process_data()
        assert result.empty

    def test_videos_older_than_baseline_excluded(self, analyzer, repo, make_record):
        old = datetime.now() - timedelta(days=60)
        _ingest(repo, make_record, "alice", "v1", 1000, old)
        result = analyzer.process_data()
        assert result.empty

    def test_identifies_outlier_above_threshold(self, analyzer, repo, make_record):
        recent = datetime.now() - timedelta(days=5)
        # 4 normal videos + 1 viral — viral should cross 1.5 Z
        for i, views in enumerate([100, 110, 90, 105, 10000]):
            _ingest(repo, make_record, "alice", f"v{i}", views, recent)

        result = analyzer.process_data()
        assert len(result) == 1
        assert result.iloc[0]["video_id"] == "v4"
        assert float(result.iloc[0]["view_z_score"]) >= 1.5

    def test_non_outliers_filtered_out(self, analyzer, repo, make_record):
        recent = datetime.now() - timedelta(days=5)
        for i, views in enumerate([100, 110, 90, 105, 95]):
            _ingest(repo, make_record, "alice", f"v{i}", views, recent)
        result = analyzer.process_data()
        assert result.empty

    def test_already_transcribed_outliers_excluded(self, analyzer, repo, make_record):
        recent = datetime.now() - timedelta(days=5)
        for i, views in enumerate([100, 110, 90, 105, 10000]):
            _ingest(repo, make_record, "alice", f"v{i}", views, recent)

        repo.save_extracted_hook("v4", "already done", 3.0)
        result = analyzer.process_data()
        assert result.empty


class TestCalculateInsights:
    def test_single_video_gets_zero_zscore(self, analyzer):
        df = pd.DataFrame({
            "ownerUsername": ["alice"],
            "videoPlayCount": [1000],
        })
        out = analyzer._calculate_insights(df)
        assert float(out["view_z_score"].iloc[0]) == 0.0

    def test_z_score_computed_per_creator_group(self, analyzer):
        df = pd.DataFrame({
            "ownerUsername": ["alice", "alice", "alice", "bob", "bob"],
            "videoPlayCount": [100, 100, 1000, 50, 50],
        })
        out = analyzer._calculate_insights(df)
        # alice's 1000 should have a positive z-score; bob's (equal values) → 0
        alice_top = out[(out["ownerUsername"] == "alice") & (out["videoPlayCount"] == 1000)]
        assert float(alice_top["view_z_score"].iloc[0]) > 0
        bob_rows = out[out["ownerUsername"] == "bob"]
        assert (bob_rows["view_z_score"] == 0).all()

    def test_null_views_filled_with_zero(self, analyzer):
        df = pd.DataFrame({
            "ownerUsername": ["alice", "alice"],
            "videoPlayCount": [None, 100],
        })
        out = analyzer._calculate_insights(df)
        assert not out["view_z_score"].isna().any()


class TestFilterOutliers:
    def test_keeps_only_above_threshold(self, analyzer):
        df = pd.DataFrame({"view_z_score": [0.5, 1.4, 1.5, 2.0]})
        out = analyzer._filter_outliers(df)
        assert out["view_z_score"].tolist() == [1.5, 2.0]
