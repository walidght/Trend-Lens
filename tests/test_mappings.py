import pandas as pd
import pytest

from config.mappings import (
    PLATFORM_MAPPINGS,
    build_profile_url,
    calculate_ig_collab,
    get_available_platforms,
)


class TestBuildProfileUrl:
    def test_instagram_url(self):
        assert build_profile_url("instagram", "zuck") == "https://www.instagram.com/zuck/"

    def test_tiktok_url(self):
        assert build_profile_url("tiktok", "charlidamelio") == "https://www.tiktok.com/@charlidamelio"

    def test_youtube_url(self):
        assert build_profile_url("youtube", "@mrbeast") == "https://www.youtube.com/@mrbeast"

    def test_unknown_platform_raises(self):
        with pytest.raises(ValueError, match="Unsupported platform"):
            build_profile_url("snapchat", "someone")

    def test_empty_platform_raises(self):
        with pytest.raises(ValueError):
            build_profile_url("", "user")


class TestCalculateIgCollab:
    def test_coauthor_column_marks_collab(self):
        df = pd.DataFrame({"coauthorProducers/0/username": ["partner", None, "other"]})
        result = calculate_ig_collab(df)
        assert result.tolist() == [True, False, True]

    def test_tagged_column_used_when_coauthor_missing(self):
        df = pd.DataFrame({"taggedUsers/0/username": [None, "tagged", None]})
        result = calculate_ig_collab(df)
        assert result.tolist() == [False, True, False]

    def test_coauthor_takes_precedence_over_tagged(self):
        # If both columns exist, coauthor wins (it's a stronger signal).
        df = pd.DataFrame({
            "coauthorProducers/0/username": [None, None],
            "taggedUsers/0/username": ["t1", "t2"],
        })
        result = calculate_ig_collab(df)
        assert result.tolist() == [False, False]

    def test_no_relevant_columns_returns_all_false(self):
        df = pd.DataFrame({"unrelated": [1, 2, 3]})
        result = calculate_ig_collab(df)
        assert result.tolist() == [False, False, False]


class TestGetAvailablePlatforms:
    def test_returns_all_registry_keys(self):
        platforms = get_available_platforms()
        assert set(platforms) == set(PLATFORM_MAPPINGS.keys())

    def test_includes_known_platforms(self):
        platforms = get_available_platforms()
        assert "Instagram (Apify)" in platforms
        assert "TikTok (Apify)" in platforms
