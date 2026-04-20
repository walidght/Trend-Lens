from datetime import datetime

import pandas as pd
import pytest

from core.ingestors import DataIngestor


@pytest.fixture
def ingestor(repo):
    return DataIngestor(config=None, repo=repo)


class TestIngestDataframe:
    def test_unknown_platform_returns_zero_stats(self, ingestor):
        df = pd.DataFrame({"ownerUsername": ["alice"], "videoPlayCount": [100]})
        stats = ingestor.ingest_dataframe(df, "Snapchat (Apify)")
        assert stats == {"new_videos": 0, "new_metrics": 0}

    def test_instagram_mapping_ingests_records(self, ingestor, repo):
        df = pd.DataFrame({
            "ownerUsername": ["alice"],
            "url": ["https://www.instagram.com/p/vid1/"],
            "videoPlayCount": [100],
            "likesCount": [10],
            "commentsCount": [2],
            "audioUrl": ["https://cdn/a.mp4"],
            "timestamp": ["2026-01-01T12:00:00.000Z"],
            "coauthorProducers/0/username": [None],
        })
        stats = ingestor.ingest_dataframe(df, "Instagram (Apify)")
        assert stats["new_videos"] == 1

        metrics = repo.get_all_latest_metrics()
        assert metrics.iloc[0]["ownerUsername"] == "alice"
        assert int(metrics.iloc[0]["videoPlayCount"]) == 100

    def test_tiktok_mapping_ingests_records(self, ingestor, repo):
        df = pd.DataFrame({
            "authorMeta/name": ["bob"],
            "url": ["https://www.tiktok.com/@bob/video/vid2"],
            "playCount": [500],
            "diggCount": [50],
            "commentCount": [5],
            "createTimeISO": ["2026-01-02T12:00:00.000Z"],
        })
        stats = ingestor.ingest_dataframe(df, "TikTok (Apify)")
        assert stats["new_videos"] == 1

        metrics = repo.get_all_latest_metrics()
        assert metrics.iloc[0]["ownerUsername"] == "bob"
        assert int(metrics.iloc[0]["videoPlayCount"]) == 500

    def test_rows_missing_username_or_url_are_dropped(self, ingestor, repo):
        df = pd.DataFrame({
            "ownerUsername": ["alice", None, "carol"],
            "url": ["https://www.instagram.com/p/v1/", "https://www.instagram.com/p/v2/", None],
            "videoPlayCount": [100, 200, 300],
            "likesCount": [10, 20, 30],
            "commentsCount": [1, 2, 3],
            "audioUrl": ["a", "b", "c"],
            "timestamp": ["2026-01-01T00:00:00Z"] * 3,
        })
        stats = ingestor.ingest_dataframe(df, "Instagram (Apify)")
        assert stats["new_videos"] == 1

    def test_empty_dataframe_returns_zero(self, ingestor):
        stats = ingestor.ingest_dataframe(pd.DataFrame(), "Instagram (Apify)")
        assert stats == {"new_videos": 0, "new_metrics": 0}


class TestEnsureSchema:
    def test_fills_missing_numeric_columns_with_zero(self, ingestor):
        df = pd.DataFrame({"username": ["a"], "url": ["u"]})
        result = ingestor._ensure_schema(df)
        for col in DataIngestor.STANDARD_NUMERIC_COLS:
            assert col in result.columns
            assert result[col].iloc[0] == 0

    def test_fills_missing_string_columns_with_none(self, ingestor):
        df = pd.DataFrame({"views": [1]})
        result = ingestor._ensure_schema(df)
        for col in DataIngestor.STANDARD_STRING_COLS:
            assert col in result.columns
            assert result[col].iloc[0] is None

    def test_fills_missing_is_collab_with_false(self, ingestor):
        df = pd.DataFrame({"views": [1]})
        result = ingestor._ensure_schema(df)
        assert bool(result["is_collab"].iloc[0]) is False

    def test_does_not_overwrite_existing_columns(self, ingestor):
        df = pd.DataFrame({"views": [42], "username": ["alice"], "is_collab": [True]})
        result = ingestor._ensure_schema(df)
        assert result["views"].iloc[0] == 42
        assert result["username"].iloc[0] == "alice"
        assert bool(result["is_collab"].iloc[0]) is True


class TestFormatAndCleanData:
    def _full_df(self, **overrides):
        base = {
            "username": ["alice"],
            "url": ["https://www.instagram.com/p/vid1/"],
            "audio_url": ["https://cdn/a.mp4"],
            "published_date": ["2026-01-01T12:00:00Z"],
            "views": [100],
            "likes": [10],
            "comments": [2],
            "is_collab": [False],
        }
        base.update(overrides)
        return pd.DataFrame(base)

    def test_extracts_video_id_from_url(self, ingestor):
        df = self._full_df()
        records = ingestor._format_and_clean_data(df, "instagram")
        assert records[0]["video_id"] == "vid1"

    def test_extracts_video_id_with_trailing_slash(self, ingestor):
        df = self._full_df(url=["https://www.tiktok.com/@bob/video/abc123/"])
        records = ingestor._format_and_clean_data(df, "tiktok")
        assert records[0]["video_id"] == "abc123"

    def test_scraped_at_is_datetime_object(self, ingestor):
        df = self._full_df()
        records = ingestor._format_and_clean_data(df, "instagram")
        assert isinstance(records[0]["scraped_at"], datetime)

    def test_published_date_is_datetime_object(self, ingestor):
        df = self._full_df()
        records = ingestor._format_and_clean_data(df, "instagram")
        assert isinstance(records[0]["published_date"], datetime)

    def test_invalid_published_date_becomes_none(self, ingestor):
        df = self._full_df(published_date=["not-a-date"])
        records = ingestor._format_and_clean_data(df, "instagram")
        assert records[0]["published_date"] is None

    def test_non_numeric_view_count_coerced_to_zero(self, ingestor):
        df = self._full_df(views=["garbage"])
        records = ingestor._format_and_clean_data(df, "instagram")
        assert records[0]["views"] == 0

    def test_base_platform_set_on_each_record(self, ingestor):
        df = self._full_df()
        records = ingestor._format_and_clean_data(df, "tiktok")
        assert records[0]["platform"] == "tiktok"

    def test_whitespace_only_username_filtered_out(self, ingestor):
        df = self._full_df(username=["   "])
        records = ingestor._format_and_clean_data(df, "instagram")
        assert records == []

    def test_strips_whitespace_from_username_and_url(self, ingestor):
        df = self._full_df(username=["  alice  "], url=["  https://www.instagram.com/p/vid1/  "])
        records = ingestor._format_and_clean_data(df, "instagram")
        assert records[0]["username"] == "alice"
        assert records[0]["url"] == "https://www.instagram.com/p/vid1/"
