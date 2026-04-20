from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from config.settings import AppConfig
from core.sheet_ingestor import SheetIngestor


@pytest.fixture
def config():
    cfg = AppConfig.__new__(AppConfig)
    cfg.scrape_interval_days = 7
    return cfg


@pytest.fixture
def ingestor(config, repo):
    return SheetIngestor(config, repo)


class TestCleanAndValidateDataframe:
    def test_raises_when_required_columns_missing(self, ingestor):
        df = pd.DataFrame({"username": ["alice"]})
        with pytest.raises(ValueError, match="must contain 'username' and 'platform'"):
            ingestor._clean_and_validate_dataframe(df)

    def test_strips_whitespace_and_lowercases_platform(self, ingestor):
        df = pd.DataFrame({"username": ["  alice  "], "platform": ["  Instagram  "]})
        out = ingestor._clean_and_validate_dataframe(df)
        assert out["username"].iloc[0] == "alice"
        assert out["platform"].iloc[0] == "instagram"

    def test_drops_rows_with_empty_values(self, ingestor):
        df = pd.DataFrame({
            "username": ["alice", "", "carol"],
            "platform": ["instagram", "tiktok", ""],
        })
        out = ingestor._clean_and_validate_dataframe(df)
        assert out["username"].tolist() == ["alice"]


class TestFetchCsvFromUrl:
    def test_rewrites_edit_url_to_export(self, ingestor):
        fake_response = MagicMock()
        fake_response.text = "username,platform\nalice,instagram\n"
        fake_response.raise_for_status = MagicMock()

        with patch("core.sheet_ingestor.requests.get", return_value=fake_response) as mock_get:
            ingestor._fetch_csv_from_url("https://docs.google.com/spreadsheets/d/abc/edit#gid=0")
            called_url = mock_get.call_args[0][0]
            assert called_url == "https://docs.google.com/spreadsheets/d/abc/export?format=csv"


class TestSyncCreatorsToDb:
    def _patch_fetch(self, ingestor, df):
        return patch.object(ingestor, "_fetch_csv_from_url", return_value=df)

    def test_returns_newly_inserted_creators(self, ingestor, repo):
        repo.add_sheet("S1", "u1")
        sheet_id = repo.get_all_sheets()["S1"]["id"]
        df = pd.DataFrame({
            "username": ["alice", "bob"],
            "platform": ["instagram", "tiktok"],
        })
        with self._patch_fetch(ingestor, df):
            new_creators = ingestor.sync_creators_to_db(sheet_id, "http://x")
        assert set(new_creators) == {("alice", "instagram"), ("bob", "tiktok")}

    def test_links_creators_to_sheet(self, ingestor, repo):
        repo.add_sheet("S1", "u1")
        sheet_id = repo.get_all_sheets()["S1"]["id"]
        df = pd.DataFrame({"username": ["alice"], "platform": ["instagram"]})
        with self._patch_fetch(ingestor, df):
            ingestor.sync_creators_to_db(sheet_id, "http://x")

        pending = repo.get_creators_never_scraped(sheet_id=sheet_id)
        assert pending == [("alice", "instagram")]

    def test_network_error_returns_empty(self, ingestor, repo):
        repo.add_sheet("S1", "u1")
        sheet_id = repo.get_all_sheets()["S1"]["id"]
        with patch.object(ingestor, "_fetch_csv_from_url",
                          side_effect=requests.RequestException("down")):
            assert ingestor.sync_creators_to_db(sheet_id, "http://x") == []

    def test_validation_error_returns_empty(self, ingestor, repo):
        repo.add_sheet("S1", "u1")
        sheet_id = repo.get_all_sheets()["S1"]["id"]
        bad_df = pd.DataFrame({"username": ["alice"]})
        with self._patch_fetch(ingestor, bad_df):
            assert ingestor.sync_creators_to_db(sheet_id, "http://x") == []

    def test_empty_after_cleaning_returns_empty(self, ingestor, repo):
        repo.add_sheet("S1", "u1")
        sheet_id = repo.get_all_sheets()["S1"]["id"]
        df = pd.DataFrame({"username": [""], "platform": [""]})
        with self._patch_fetch(ingestor, df):
            assert ingestor.sync_creators_to_db(sheet_id, "http://x") == []


class TestGenerateScrapeList:
    def test_builds_instagram_urls(self, ingestor, repo):
        repo.bulk_insert_creators([("alice", "instagram"), ("bob", "instagram")])
        urls = ingestor.generate_scrape_list(platform="instagram")
        assert set(urls) == {
            "https://www.instagram.com/alice/",
            "https://www.instagram.com/bob/",
        }

    def test_builds_tiktok_urls(self, ingestor, repo):
        repo.bulk_insert_creators([("charlie", "tiktok")])
        urls = ingestor.generate_scrape_list(platform="tiktok")
        assert urls == ["https://www.tiktok.com/@charlie"]

    def test_unsupported_platform_returns_empty(self, ingestor, repo):
        repo.bulk_insert_creators([("dan", "snapchat")])
        assert ingestor.generate_scrape_list(platform="snapchat") == []

    def test_respects_sheet_filter(self, ingestor, repo):
        repo.add_sheet("S1", "u1")
        sheet_id = repo.get_all_sheets()["S1"]["id"]
        repo.bulk_insert_creators([("alice", "instagram"), ("bob", "instagram")])
        repo.link_creators_to_sheet(sheet_id, ["alice"], "instagram")

        urls = ingestor.generate_scrape_list(platform="instagram", sheet_id=sheet_id)
        assert urls == ["https://www.instagram.com/alice/"]
