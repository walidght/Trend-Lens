import pandas as pd
import logging
from core.database import DatabaseManager

logger = logging.getLogger(__name__)


class TrendLensRepository:
    """Abstracts all database queries away from the business logic."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    def get_videos_missing_hooks(self) -> pd.DataFrame:
        """Fetches the latest metrics for videos that haven't been processed by AI yet."""
        query = """
            SELECT 
                v.video_id, v.url, v.audio_url,
                c.username as ownerUsername,
                m.views as videoPlayCount, m.likes as likesCount, m.comments as commentsCount,
                vi.is_collab
            FROM videos v
            JOIN creators c ON v.creator_id = c.id
            JOIN (
                SELECT video_id, views, likes, comments
                FROM video_metrics 
                WHERE (video_id, scraped_at) IN (
                    SELECT video_id, MAX(scraped_at) 
                    FROM video_metrics GROUP BY video_id
                )
            ) m ON v.video_id = m.video_id
            LEFT JOIN video_insights vi ON v.video_id = vi.video_id
            WHERE vi.hook_text IS NULL
        """
        with self.db.get_connection() as conn:
            return pd.read_sql_query(query, conn)

    def save_extracted_hook(self, video_id: str, hook_text: str, z_score: float):
        """Saves the AI-extracted hook back to the database."""
        query = """
            UPDATE video_insights 
            SET hook_text = ?, view_z_score = ?, updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ?
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (hook_text, z_score, video_id))
            conn.commit()

    def get_latest_hooks_preview(self, limit: int = 10) -> pd.DataFrame:
        """Fetches recently extracted hooks for the UI dashboard."""
        query = """
            SELECT c.username, v.url, vi.view_z_score, vi.hook_text
            FROM video_insights vi
            JOIN videos v ON vi.video_id = v.video_id
            JOIN creators c ON v.creator_id = c.id
            WHERE vi.hook_text IS NOT NULL
            ORDER BY vi.updated_at DESC
            LIMIT ?
        """
        with self.db.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=(limit,))
        
    def ingest_apify_row(self, username: str, video_id: str, url: str, audio_url: str, 
                         published_date: str, views: int, likes: int, comments: int, 
                         is_collab: bool, scraped_at: str) -> dict:
        """Upserts creator and video, and inserts metrics and insight stubs."""
        stats = {"new_videos": 0, "new_metrics": 0}

        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            # 1. Upsert Creator
            cursor.execute("""
                INSERT INTO creators (username, platform, last_scraped_at)
                VALUES (?, 'instagram', ?)
                ON CONFLICT(username, platform) DO UPDATE SET last_scraped_at = ?
            """, (username, scraped_at, scraped_at))
            
            # Get creator ID
            cursor.execute("SELECT id FROM creators WHERE username = ? AND platform = 'instagram'", (username,))
            creator_id = cursor.fetchone()['id']

            # 2. Upsert Video
            cursor.execute("""
                INSERT INTO videos (video_id, creator_id, url, audio_url, published_date)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET audio_url = excluded.audio_url
            """, (video_id, creator_id, url, audio_url, published_date))
            
            # SQLite rowcount returns > 0 if a new row was added OR an existing row was updated.
            # For our MVP tracking, we'll count it as a new video interaction.
            if cursor.rowcount > 0:  
                stats["new_videos"] += 1

            # 3. Insert Metrics (Protected by UNIQUE constraint)
            cursor.execute("""
                INSERT OR IGNORE INTO video_metrics (video_id, scraped_at, views, likes, comments)
                VALUES (?, ?, ?, ?, ?)
            """, (video_id, scraped_at, views, likes, comments))
            
            if cursor.rowcount > 0:
                stats["new_metrics"] += 1

            # 4. Insert Insights Stub
            cursor.execute("""
                INSERT OR IGNORE INTO video_insights (video_id, is_collab)
                VALUES (?, ?)
            """, (video_id, is_collab))

            conn.commit()

        return stats
    
    def bulk_insert_creators(self, creators_list: list) -> int:
        """Inserts multiple creators into the DB and returns the number of new additions."""
        added_count = 0
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            for username, platform in creators_list:
                cursor.execute("""
                    INSERT INTO creators (username, platform)
                    VALUES (?, ?)
                    ON CONFLICT(username, platform) DO NOTHING
                """, (username, platform))
                
                if cursor.rowcount > 0:
                    added_count += 1
            conn.commit()
        return added_count

    def get_creators_due_for_scrape(self, platform: str, cutoff_str: str) -> list:
        """Returns a list of usernames that haven't been scraped since the cutoff date."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT username FROM creators 
                WHERE platform = ? 
                AND (last_scraped_at IS NULL OR last_scraped_at < ?)
            """, (platform, cutoff_str))
            
            # Extract just the usernames into a simple Python list
            return [row['username'] for row in cursor.fetchall()]
