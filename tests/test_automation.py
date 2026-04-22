from unittest.mock import MagicMock

import pytest

from config.settings import AppConfig
from core.automation import AutomationOrchestrator


@pytest.fixture
def config():
    cfg = AppConfig.__new__(AppConfig)
    cfg.scrape_interval_days = 7
    cfg.backfill_max_items = 21
    cfg.baseline_days = 30
    return cfg


@pytest.fixture
def scraper():
    return MagicMock()


@pytest.fixture
def ingestor_mock():
    m = MagicMock()
    m.ingest_dataframe.return_value = {"new_videos": 3, "new_metrics": 3}
    return m


@pytest.fixture
def orchestrator(config, repo, scraper, ingestor_mock):
    return AutomationOrchestrator(config, repo, scraper, ingestor_mock)


class TestRunBackfill:
    def test_empty_list_short_circuits(self, orchestrator, scraper):
        result = orchestrator.run_backfill([])
        assert result["status"] == "success"
        assert result["new_videos"] == 0
        scraper.run_actor.assert_not_called()

    def test_groups_by_platform_single_call_per_platform(self, orchestrator, scraper, ingestor_mock):
        scraper.run_actor.return_value = [{"ownerUsername": "alice"}]
        orchestrator.run_backfill([
            ("alice", "instagram"),
            ("bob", "instagram"),
            ("carol", "tiktok"),
        ])
        # One scrape call per platform
        assert scraper.run_actor.call_count == 2
        assert ingestor_mock.ingest_dataframe.call_count == 2

    def test_uses_config_backfill_limit_by_default(self, config, orchestrator, scraper):
        scraper.run_actor.return_value = [{"x": 1}]
        orchestrator.run_backfill([("alice", "instagram")])
        run_input, _ = scraper.run_actor.call_args[0], scraper.run_actor.call_args[1]
        passed_input = scraper.run_actor.call_args.args[0]
        assert passed_input["resultsLimit"] == config.backfill_max_items

    def test_explicit_max_items_overrides_config(self, orchestrator, scraper):
        scraper.run_actor.return_value = [{"x": 1}]
        orchestrator.run_backfill([("alice", "instagram")], max_items=5)
        passed_input = scraper.run_actor.call_args.args[0]
        assert passed_input["resultsLimit"] == 5

    def test_unmapped_base_platform_reported_as_failure(self, orchestrator, scraper):
        result = orchestrator.run_backfill([("alice", "snapchat")])
        assert result["status"] == "error"
        assert "snapchat" in result["message"].lower()
        scraper.run_actor.assert_not_called()

    def test_partial_status_when_some_platforms_fail(self, orchestrator, scraper):
        # IG succeeds, TikTok returns empty (→ error)
        def run_actor_side_effect(run_input, target_identifier):
            if "instagram" in target_identifier:
                return [{"ownerUsername": "alice"}]
            return []
        scraper.run_actor.side_effect = run_actor_side_effect

        result = orchestrator.run_backfill([
            ("alice", "instagram"),
            ("carol", "tiktok"),
        ])
        assert result["status"] == "partial"

    def test_aggregates_stats_across_platforms(self, orchestrator, scraper, ingestor_mock):
        scraper.run_actor.return_value = [{"x": 1}]
        ingestor_mock.ingest_dataframe.return_value = {"new_videos": 5, "new_metrics": 10}

        result = orchestrator.run_backfill([
            ("alice", "instagram"),
            ("bob", "tiktok"),
        ])
        assert result["new_videos"] == 10
        assert result["new_metrics"] == 20


class TestRunAutoSync:
    def test_invalid_platform_returns_error(self, orchestrator):
        result = orchestrator.run_auto_sync("Snapchat (Apify)", sheet_id=1)
        assert result["status"] == "error"

    def test_no_due_creators_short_circuits(self, orchestrator, scraper):
        result = orchestrator.run_auto_sync("Instagram (Apify)", sheet_id=None)
        assert result["status"] == "success"
        assert result["new_videos"] == 0
        scraper.run_actor.assert_not_called()

    def test_scrapes_due_creators(self, orchestrator, scraper, repo, ingestor_mock):
        repo.bulk_insert_creators([("alice", "instagram")])
        scraper.run_actor.return_value = [{"ownerUsername": "alice"}]

        result = orchestrator.run_auto_sync("Instagram (Apify)", sheet_id=None)
        assert result["status"] == "success"
        scraper.run_actor.assert_called_once()
        passed_input = scraper.run_actor.call_args.args[0]
        assert passed_input["directUrls"] == ["https://www.instagram.com/alice/"]

    def test_scraper_empty_result_returns_error(self, orchestrator, scraper, repo):
        repo.bulk_insert_creators([("alice", "instagram")])
        scraper.run_actor.return_value = []
        result = orchestrator.run_auto_sync("Instagram (Apify)", sheet_id=None)
        assert result["status"] == "error"
