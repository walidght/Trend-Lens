import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Handles SQLite database connections and schema setup."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        # Ensure the data directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def get_connection(self) -> sqlite3.Connection:
        """Creates a connection and enforces strict SQLite rules."""
        conn = sqlite3.connect(self.db_path)
        # Enforce foreign key constraints (SQLite disables them by default)
        conn.execute("PRAGMA foreign_keys = ON;")
        # Return rows as dictionary-like objects (easier for pandas/data manipulation)
        conn.row_factory = sqlite3.Row
        return conn

    def setup_database(self):
        """Creates all necessary tables if they don't already exist."""
        logger.info(f"Initializing database at {self.db_path}...")

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 1. CREATORS TABLE
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS creators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    last_scraped_at DATETIME,
                    UNIQUE(username, platform)
                )
            """)

            # 2. VIDEOS TABLE (Static Data)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    video_id TEXT PRIMARY KEY,
                    creator_id INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    published_date DATETIME,
                    FOREIGN KEY(creator_id) REFERENCES creators(id) ON DELETE CASCADE
                )
            """)

            # 3. VIDEO METRICS TABLE (Dynamic/Time-Series Data)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS video_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    scraped_at DATETIME NOT NULL,
                    views INTEGER DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    comments INTEGER DEFAULT 0,
                    FOREIGN KEY(video_id) REFERENCES videos(video_id) ON DELETE CASCADE
                )
            """)

            # Create a unique index on the date part of scraped_at to prevent multiple entries for the same video on the same day
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_video_day 
                ON video_metrics(video_id, DATE(scraped_at))
            """)

            # 4. VIDEO INSIGHTS TABLE (AI Analysis Data)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS video_insights (
                    video_id TEXT PRIMARY KEY,
                    hook_text TEXT,
                    hook_category TEXT,
                    view_z_score REAL,
                    is_collab BOOLEAN,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(video_id) REFERENCES videos(video_id) ON DELETE CASCADE
                )
            """)

        logger.info("Database schema successfully verified/created.")
