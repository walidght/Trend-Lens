import logging
import pandas as pd
from typing import List, Dict, Optional

from sqlalchemy import select, update, func, desc
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError

from core.database import (
    DatabaseManager, 
    Creator, 
    Sheet, 
    Video, 
    VideoMetric, 
    VideoInsight, 
    sheet_creators_table
)

logger = logging.getLogger(__name__)


class TrendLensRepository:
    """Abstracts all database queries away from the business logic using SQLAlchemy ORM/Core."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    # ==========================================
    # DATA RETRIEVAL (ANALYTICS & UI)
    # ==========================================
    
    def _get_latest_metrics_subquery(self):
        """Helper method: Creates a reusable subquery that isolates the most recent metric entry per video."""
        latest_metric_dates = select(
            VideoMetric.video_id,
            func.max(VideoMetric.scraped_at).label("max_scraped_at")
        ).group_by(VideoMetric.video_id).subquery("latest_dates")

        # Join the metrics table back to itself to get the actual views/likes for those max dates
        return select(VideoMetric).join(
            latest_metric_dates,
            (VideoMetric.video_id == latest_metric_dates.c.video_id) &
            (VideoMetric.scraped_at == latest_metric_dates.c.max_scraped_at)
        ).subquery("m")

    def get_all_latest_metrics(self, sheet_id: Optional[int] = None) -> pd.DataFrame:
        """Fetches the latest metrics, optionally filtered by a specific sheet."""
        latest_metrics = self._get_latest_metrics_subquery()

        stmt = select(
            Video.video_id, Video.url, Video.audio_url, Video.published_date,
            Creator.username.label("ownerUsername"),
            latest_metrics.c.views.label("videoPlayCount"),
            latest_metrics.c.likes.label("likesCount"),
            latest_metrics.c.comments.label("commentsCount"),
            VideoInsight.is_collab,
            VideoInsight.hook_text
        ).select_from(Video)\
         .join(Creator, Video.creator_id == Creator.id)\
         .join(latest_metrics, Video.video_id == latest_metrics.c.video_id)\
         .outerjoin(VideoInsight, Video.video_id == VideoInsight.video_id)

        if sheet_id:
            stmt = stmt.join(sheet_creators_table, Creator.id == sheet_creators_table.c.creator_id)\
                       .where(sheet_creators_table.c.sheet_id == sheet_id)

        # Let Pandas execute the SQLAlchemy selectable directly!
        with self.db.engine.connect() as conn:
            return pd.read_sql(stmt, conn)

    def get_dashboard_data(self, sheet_id: int) -> pd.DataFrame:
        """Fetches all recent video data for charting, including the extracted hooks."""
        latest_metrics = self._get_latest_metrics_subquery()

        stmt = select(
            Creator.username, Video.url, Video.published_date,
            latest_metrics.c.views, 
            VideoInsight.view_z_score, 
            VideoInsight.hook_text
        ).select_from(Video)\
         .join(Creator, Video.creator_id == Creator.id)\
         .join(sheet_creators_table, Creator.id == sheet_creators_table.c.creator_id)\
         .join(latest_metrics, Video.video_id == latest_metrics.c.video_id)\
         .outerjoin(VideoInsight, Video.video_id == VideoInsight.video_id)\
         .where(sheet_creators_table.c.sheet_id == sheet_id)

        with self.db.engine.connect() as conn:
            df = pd.read_sql(stmt, conn)

        if not df.empty:
            df['published_date'] = pd.to_datetime(df['published_date'], utc=True, errors='coerce')
        return df

    def get_latest_hooks_preview(self, limit: int = 10) -> pd.DataFrame:
        """Fetches recently extracted hooks for the UI dashboard."""
        stmt = select(
            Creator.username, Video.url, VideoInsight.view_z_score, VideoInsight.hook_text
        ).select_from(VideoInsight)\
         .join(Video, VideoInsight.video_id == Video.video_id)\
         .join(Creator, Video.creator_id == Creator.id)\
         .where(VideoInsight.hook_text.isnot(None))\
         .order_by(desc(VideoInsight.updated_at))\
         .limit(limit)

        with self.db.engine.connect() as conn:
            return pd.read_sql(stmt, conn)

    # ==========================================
    # DATA INGESTION & UPDATES
    # ==========================================

    def save_extracted_hook(self, video_id: str, hook_text: str, z_score: float):
        """Saves the AI-extracted hook back to the database."""
        stmt = update(VideoInsight).where(VideoInsight.video_id == video_id).values(
            hook_text=hook_text,
            view_z_score=z_score,
            updated_at=func.current_timestamp()
        )
        with self.db.get_session() as session:
            session.execute(stmt)
            session.commit()

    def bulk_ingest_apify_data(self, records: List[Dict]) -> Dict[str, int]:
        """Upserts batches of creators, videos, metrics, and insights securely and efficiently."""
        stats = {"new_videos": 0, "new_metrics": 0}
        if not records:
            return stats

        with self.db.get_session() as session:
            # 1. Upsert Creators
            unique_creators = list({(r['username'], r['platform'], r['scraped_at']) for r in records})
            creator_values = [{"username": u, "platform": p, "last_scraped_at": s} for u, p, s in unique_creators]
            
            stmt_creators = sqlite_insert(Creator).values(creator_values)
            stmt_creators = stmt_creators.on_conflict_do_update(
                index_elements=['username', 'platform'],
                set_={'last_scraped_at': stmt_creators.excluded.last_scraped_at}
            )
            session.execute(stmt_creators)

            # 2. Fetch Creator IDs to map to videos
            usernames = [u for u, _, _ in unique_creators]
            platforms = [p for _, p, _ in unique_creators]
            stmt_fetch_ids = select(Creator.id, Creator.username).where(
                Creator.username.in_(usernames), Creator.platform.in_(platforms)
            )
            creator_map = {username: cid for cid, username in session.execute(stmt_fetch_ids)}

            # 3. Prepare data batches
            videos_batch = []
            metrics_batch = []
            insights_batch = []

            for r in records:
                cid = creator_map.get(r['username'])
                if not cid:
                    continue

                videos_batch.append({
                    "video_id": r['video_id'], "creator_id": cid, "url": r['url'], 
                    "audio_url": r['audio_url'], "published_date": r['published_date']
                })
                metrics_batch.append({
                    "video_id": r['video_id'], "scraped_at": r['scraped_at'],
                    "views": r['views'], "likes": r['likes'], "comments": r['comments']
                })
                insights_batch.append({
                    "video_id": r['video_id'], "is_collab": r['is_collab']
                })

            # 4. Upsert Videos
            stmt_videos = sqlite_insert(Video).values(videos_batch)
            stmt_videos = stmt_videos.on_conflict_do_update(
                index_elements=['video_id'],
                set_={'audio_url': stmt_videos.excluded.audio_url}
            )
            result_videos = session.execute(stmt_videos)
            stats["new_videos"] = result_videos.rowcount

            # 5. Insert Metrics (Ignore if exact date duplicate)
            stmt_metrics = sqlite_insert(VideoMetric).values(metrics_batch).on_conflict_do_nothing()
            result_metrics = session.execute(stmt_metrics)
            stats["new_metrics"] = result_metrics.rowcount

            # 6. Insert Insight Stubs (Ignore if already exists)
            stmt_insights = sqlite_insert(VideoInsight).values(insights_batch).on_conflict_do_nothing()
            session.execute(stmt_insights)

            session.commit()

        return stats

    def bulk_insert_creators(self, creators_list: List[tuple]) -> List[tuple]:
        """Inserts multiple creators and returns the (username, platform) pairs that were newly inserted."""
        if not creators_list:
            return []

        values = [{"username": u, "platform": p} for u, p in creators_list]
        stmt = (
            sqlite_insert(Creator)
            .values(values)
            .on_conflict_do_nothing(index_elements=['username', 'platform'])
            .returning(Creator.username, Creator.platform)
        )

        with self.db.get_session() as session:
            result = session.execute(stmt)
            rows = [(row.username, row.platform) for row in result]
            session.commit()
            return rows

    def get_creators_never_scraped(self, sheet_id: Optional[int] = None) -> List[tuple]:
        """Returns (username, platform) pairs for creators that have never been scraped."""
        stmt = select(Creator.username, Creator.platform).where(Creator.last_scraped_at.is_(None))

        if sheet_id:
            stmt = stmt.join(sheet_creators_table, Creator.id == sheet_creators_table.c.creator_id)\
                       .where(sheet_creators_table.c.sheet_id == sheet_id)

        with self.db.get_session() as session:
            return [(row.username, row.platform) for row in session.execute(stmt)]

    # ==========================================
    # SHEET MANAGEMENT & WORKFLOW
    # ==========================================

    def get_creators_due_for_scrape(self, platform: str, cutoff_str: str, sheet_id: Optional[int] = None) -> List[str]:
        """Returns a list of usernames due for a scrape, optionally filtered by a specific sheet."""
        stmt = select(Creator.username).where(
            (Creator.platform == platform) &
            ((Creator.last_scraped_at.is_(None)) | (Creator.last_scraped_at < cutoff_str))
        )

        if sheet_id:
            stmt = stmt.join(sheet_creators_table, Creator.id == sheet_creators_table.c.creator_id)\
                       .where(sheet_creators_table.c.sheet_id == sheet_id)

        with self.db.get_session() as session:
            return list(session.scalars(stmt).all())

    def add_sheet(self, name: str, url: str) -> bool:
        """Adds a new Google Sheet to the database."""
        stmt = sqlite_insert(Sheet).values(name=name, url=url)
        try:
            with self.db.get_session() as session:
                session.execute(stmt)
                session.commit()
            return True
        except IntegrityError:
            logger.warning(f"Sheet with name '{name}' already exists.")
            return False

    def get_all_sheets(self) -> Dict[str, Dict[str, str]]:
        """Returns a dictionary of sheets for the UI."""
        stmt = select(Sheet.id, Sheet.name, Sheet.url)
        with self.db.get_session() as session:
            return {row.name: {"id": row.id, "url": row.url} for row in session.execute(stmt)}

    def link_creators_to_sheet(self, sheet_id: int, usernames: List[str], platform: str = 'instagram'):
        """Links a list of existing creators to a specific sheet."""
        if not usernames:
            return

        with self.db.get_session() as session:
            # 1. Fetch Creator IDs
            stmt_fetch = select(Creator.id).where(
                Creator.username.in_(usernames), Creator.platform == platform
            )
            creator_ids = session.scalars(stmt_fetch).all()

            if not creator_ids:
                return

            # 2. Bulk link via junction table (Ignore if already linked)
            links = [{"sheet_id": sheet_id, "creator_id": cid} for cid in creator_ids]
            stmt_link = sqlite_insert(sheet_creators_table).values(links).on_conflict_do_nothing()
            
            session.execute(stmt_link)
            session.commit()