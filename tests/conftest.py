"""Shared pytest fixtures."""
import sys
from pathlib import Path

# Make the project root importable so `import core.xxx` works when pytest is run from any cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime

import pytest

from core.database import DatabaseManager
from core.repository import TrendLensRepository


@pytest.fixture
def db(tmp_path):
    """Fresh SQLite-backed DatabaseManager with all tables created. One file per test."""
    manager = DatabaseManager(str(tmp_path / "trendlens_test.db"))
    manager.setup_database()
    yield manager
    manager.engine.dispose()


@pytest.fixture
def repo(db):
    return TrendLensRepository(db)


def _make_record(
    username="alice",
    platform="instagram",
    video_id="vid1",
    url="https://www.instagram.com/p/vid1/",
    audio_url="https://cdn/a.mp4",
    published_date=datetime(2026, 1, 1),
    views=100,
    likes=10,
    comments=2,
    is_collab=False,
    scraped_at=datetime(2026, 4, 20, 12, 0, 0),
):
    return {
        "username": username,
        "platform": platform,
        "video_id": video_id,
        "url": url,
        "audio_url": audio_url,
        "published_date": published_date,
        "views": views,
        "likes": likes,
        "comments": comments,
        "is_collab": is_collab,
        "scraped_at": scraped_at,
    }


@pytest.fixture
def make_record():
    """Factory for synthetic apify-shaped records. Override fields via kwargs."""
    return _make_record
