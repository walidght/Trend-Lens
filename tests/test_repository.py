from datetime import datetime, timedelta

import pytest


class TestAddSheet:
    def test_returns_true_for_new_sheet(self, repo):
        assert repo.add_sheet("Niche A", "https://example/a.csv") is True

    def test_returns_false_for_duplicate_name(self, repo):
        repo.add_sheet("Niche A", "https://example/a.csv")
        assert repo.add_sheet("Niche A", "https://example/b.csv") is False

    def test_get_all_sheets_returns_added(self, repo):
        repo.add_sheet("Niche A", "https://example/a.csv")
        repo.add_sheet("Niche B", "https://example/b.csv")
        sheets = repo.get_all_sheets()
        assert set(sheets.keys()) == {"Niche A", "Niche B"}
        assert sheets["Niche A"]["url"] == "https://example/a.csv"


class TestBulkInsertCreators:
    def test_returns_only_newly_inserted_pairs(self, repo):
        # First insert: both should be returned as new
        first = repo.bulk_insert_creators([("alice", "instagram"), ("bob", "tiktok")])
        assert set(first) == {("alice", "instagram"), ("bob", "tiktok")}

    def test_conflict_skips_return_empty(self, repo):
        repo.bulk_insert_creators([("alice", "instagram")])
        # Second call with same pair should return empty — ON CONFLICT DO NOTHING skips it
        second = repo.bulk_insert_creators([("alice", "instagram")])
        assert second == []

    def test_mixed_new_and_existing(self, repo):
        repo.bulk_insert_creators([("alice", "instagram")])
        result = repo.bulk_insert_creators([("alice", "instagram"), ("bob", "tiktok")])
        assert set(result) == {("bob", "tiktok")}

    def test_same_username_different_platforms_both_new(self, repo):
        result = repo.bulk_insert_creators([("alice", "instagram"), ("alice", "tiktok")])
        assert set(result) == {("alice", "instagram"), ("alice", "tiktok")}

    def test_empty_input_returns_empty(self, repo):
        assert repo.bulk_insert_creators([]) == []


class TestBulkIngestApifyData:
    def test_empty_records_returns_zero_stats(self, repo):
        stats = repo.bulk_ingest_apify_data([])
        assert stats == {"new_videos": 0, "new_metrics": 0}

    def test_single_record_creates_creator_video_metric_insight(self, repo, make_record):
        record = make_record()
        stats = repo.bulk_ingest_apify_data([record])

        assert stats["new_videos"] == 1
        # The creator, video, and metric rows should now exist
        metrics_df = repo.get_all_latest_metrics()
        assert len(metrics_df) == 1
        assert metrics_df.iloc[0]["ownerUsername"] == "alice"
        assert int(metrics_df.iloc[0]["videoPlayCount"]) == 100

    def test_same_video_same_day_does_not_create_duplicate_metric(self, repo, make_record):
        record = make_record()
        repo.bulk_ingest_apify_data([record])
        # Ingest the exact same record again — metrics unique index should block duplicate
        stats = repo.bulk_ingest_apify_data([record])
        assert stats["new_metrics"] == 0

    def test_same_video_different_day_creates_new_metric(self, repo, make_record):
        repo.bulk_ingest_apify_data([make_record(scraped_at=datetime(2026, 4, 20, 12))])
        stats = repo.bulk_ingest_apify_data([
            make_record(views=200, scraped_at=datetime(2026, 4, 21, 12))
        ])
        assert stats["new_metrics"] == 1
        # Latest metrics should reflect the newer entry
        metrics_df = repo.get_all_latest_metrics()
        assert int(metrics_df.iloc[0]["videoPlayCount"]) == 200


class TestGetCreatorsNeverScraped:
    def test_returns_pairs_with_null_last_scraped_at(self, repo):
        repo.bulk_insert_creators([("alice", "instagram"), ("bob", "tiktok")])
        pending = repo.get_creators_never_scraped()
        assert set(pending) == {("alice", "instagram"), ("bob", "tiktok")}

    def test_excludes_scraped_creators(self, repo, make_record):
        repo.bulk_insert_creators([("alice", "instagram"), ("bob", "tiktok")])
        # Ingesting data for alice sets her last_scraped_at
        repo.bulk_ingest_apify_data([make_record(username="alice")])
        pending = repo.get_creators_never_scraped()
        assert set(pending) == {("bob", "tiktok")}

    def test_sheet_filter_restricts_results(self, repo):
        repo.add_sheet("S1", "u1")
        s1_id = repo.get_all_sheets()["S1"]["id"]
        repo.bulk_insert_creators([("alice", "instagram"), ("bob", "tiktok")])
        repo.link_creators_to_sheet(s1_id, ["alice"], "instagram")
        # Only alice is linked to sheet S1
        pending = repo.get_creators_never_scraped(sheet_id=s1_id)
        assert pending == [("alice", "instagram")]


class TestGetCreatorsDueForScrape:
    def test_never_scraped_is_due(self, repo):
        repo.bulk_insert_creators([("alice", "instagram")])
        cutoff = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        assert repo.get_creators_due_for_scrape("instagram", cutoff) == ["alice"]

    def test_scraped_before_cutoff_is_due(self, repo, make_record):
        # Alice was scraped a long time ago
        old = datetime.now() - timedelta(days=30)
        repo.bulk_ingest_apify_data([make_record(username="alice", scraped_at=old)])

        cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        assert repo.get_creators_due_for_scrape("instagram", cutoff) == ["alice"]

    def test_recently_scraped_is_not_due(self, repo, make_record):
        recent = datetime.now()
        repo.bulk_ingest_apify_data([make_record(username="alice", scraped_at=recent)])

        cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        assert repo.get_creators_due_for_scrape("instagram", cutoff) == []

    def test_platform_filter(self, repo):
        repo.bulk_insert_creators([("alice", "instagram"), ("bob", "tiktok")])
        cutoff = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        assert repo.get_creators_due_for_scrape("tiktok", cutoff) == ["bob"]


class TestSaveExtractedHook:
    def test_updates_hook_text(self, repo, make_record):
        repo.bulk_ingest_apify_data([make_record(video_id="vid1")])
        repo.save_extracted_hook("vid1", "This is the hook.")

        hooks = repo.get_latest_hooks_preview()
        assert len(hooks) == 1
        assert hooks.iloc[0]["hook_text"] == "This is the hook."


class TestUpdateZScore:
    def test_sets_zscore(self, repo, make_record):
        repo.bulk_ingest_apify_data([make_record(video_id="vid1")])
        repo.update_z_score("vid1", 2.5)

        metrics = repo.get_all_latest_metrics()
        assert float(metrics.iloc[0]["view_z_score"]) == pytest.approx(2.5)

    def test_sets_first_viral_at_on_first_call(self, repo, make_record):
        repo.bulk_ingest_apify_data([make_record(video_id="vid1")])
        repo.update_z_score("vid1", 2.5)

        metrics = repo.get_all_latest_metrics()
        assert metrics.iloc[0]["first_viral_at"] is not None

    def test_first_viral_at_not_overwritten_on_subsequent_call(self, repo, make_record):
        repo.bulk_ingest_apify_data([make_record(video_id="vid1")])
        repo.update_z_score("vid1", 2.5)
        first = repo.get_all_latest_metrics().iloc[0]["first_viral_at"]

        repo.update_z_score("vid1", 3.0)
        second = repo.get_all_latest_metrics().iloc[0]["first_viral_at"]

        assert first == second

    def test_z_score_can_be_updated_higher(self, repo, make_record):
        repo.bulk_ingest_apify_data([make_record(video_id="vid1")])
        repo.update_z_score("vid1", 2.0)
        repo.update_z_score("vid1", 4.0)

        metrics = repo.get_all_latest_metrics()
        assert float(metrics.iloc[0]["view_z_score"]) == pytest.approx(4.0)


class TestLinkCreatorsToSheet:
    def test_links_existing_creators(self, repo):
        repo.add_sheet("S1", "u1")
        s1_id = repo.get_all_sheets()["S1"]["id"]
        repo.bulk_insert_creators([("alice", "instagram")])
        repo.link_creators_to_sheet(s1_id, ["alice"], "instagram")

        # Pending-backfill query on that sheet should now see alice
        pending = repo.get_creators_never_scraped(sheet_id=s1_id)
        assert pending == [("alice", "instagram")]

    def test_idempotent(self, repo):
        repo.add_sheet("S1", "u1")
        s1_id = repo.get_all_sheets()["S1"]["id"]
        repo.bulk_insert_creators([("alice", "instagram")])
        repo.link_creators_to_sheet(s1_id, ["alice"], "instagram")
        # Second link should not raise or duplicate
        repo.link_creators_to_sheet(s1_id, ["alice"], "instagram")
        pending = repo.get_creators_never_scraped(sheet_id=s1_id)
        assert pending == [("alice", "instagram")]

    def test_empty_usernames_noop(self, repo):
        repo.add_sheet("S1", "u1")
        s1_id = repo.get_all_sheets()["S1"]["id"]
        # Should simply not raise
        repo.link_creators_to_sheet(s1_id, [], "instagram")


class TestGetDashboardData:
    def test_returns_data_scoped_to_sheet(self, repo, make_record):
        repo.add_sheet("S1", "u1")
        s1_id = repo.get_all_sheets()["S1"]["id"]
        repo.bulk_ingest_apify_data([make_record(username="alice", video_id="v1")])
        repo.link_creators_to_sheet(s1_id, ["alice"], "instagram")

        df = repo.get_dashboard_data(s1_id)
        assert len(df) == 1
        assert df.iloc[0]["username"] == "alice"

    def test_excludes_other_sheets(self, repo, make_record):
        repo.add_sheet("S1", "u1")
        repo.add_sheet("S2", "u2")
        s1_id = repo.get_all_sheets()["S1"]["id"]
        s2_id = repo.get_all_sheets()["S2"]["id"]

        repo.bulk_ingest_apify_data([make_record(username="alice", video_id="v1")])
        repo.link_creators_to_sheet(s1_id, ["alice"], "instagram")

        # S2 has no linked creators
        df = repo.get_dashboard_data(s2_id)
        assert df.empty
