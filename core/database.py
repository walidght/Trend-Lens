import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    Float,
    ForeignKey,
    UniqueConstraint,
    Index,
    Table,
    text,
    event
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
    Session
)

logger = logging.getLogger(__name__)

# ==========================================
# 1. ORM BASE & ASSOCIATION TABLES
# ==========================================


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy declarative models."""
    pass


# Junction table for Many-to-Many relationship between Sheets and Creators
sheet_creators_table = Table(
    "sheet_creators",
    Base.metadata,
    Column("sheet_id", ForeignKey("sheets.id",
           ondelete="CASCADE"), primary_key=True),
    Column("creator_id", ForeignKey("creators.id",
           ondelete="CASCADE"), primary_key=True),
)

# ==========================================
# 2. DATABASE MODELS
# ==========================================


class Creator(Base):
    __tablename__ = "creators"
    __table_args__ = (UniqueConstraint(
        "username", "platform", name="uq_username_platform"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String, nullable=False)
    platform: Mapped[str] = mapped_column(String, nullable=False)
    last_scraped_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    sheets: Mapped[List["Sheet"]] = relationship(
        secondary=sheet_creators_table, back_populates="creators")
    videos: Mapped[List["Video"]] = relationship(
        back_populates="creator", cascade="all, delete-orphan")


class Sheet(Base):
    __tablename__ = "sheets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    url: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    creators: Mapped[List["Creator"]] = relationship(
        secondary=sheet_creators_table, back_populates="sheets")


class Video(Base):
    __tablename__ = "videos"

    video_id: Mapped[str] = mapped_column(String, primary_key=True)
    creator_id: Mapped[int] = mapped_column(ForeignKey(
        "creators.id", ondelete="CASCADE"), nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    published_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    audio_url: Mapped[Optional[str]] = mapped_column(String)

    # Relationships
    creator: Mapped["Creator"] = relationship(back_populates="videos")
    metrics: Mapped[List["VideoMetric"]] = relationship(
        back_populates="video", cascade="all, delete-orphan")
    insight: Mapped[Optional["VideoInsight"]] = relationship(
        back_populates="video", cascade="all, delete-orphan", uselist=False)


class VideoMetric(Base):
    __tablename__ = "video_metrics"
    __table_args__ = (
        # SQLite-specific functional index to enforce one entry per video per day
        Index("idx_video_day", "video_id", text(
            "DATE(scraped_at)"), unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    video_id: Mapped[str] = mapped_column(ForeignKey(
        "videos.video_id", ondelete="CASCADE"), nullable=False)
    scraped_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    views: Mapped[int] = mapped_column(Integer, default=0)
    likes: Mapped[int] = mapped_column(Integer, default=0)
    comments: Mapped[int] = mapped_column(Integer, default=0)

    # Relationships
    video: Mapped["Video"] = relationship(back_populates="metrics")


class VideoInsight(Base):
    __tablename__ = "video_insights"

    # video_id serves as both the Primary Key and the Foreign Key
    video_id: Mapped[str] = mapped_column(ForeignKey(
        "videos.video_id", ondelete="CASCADE"), primary_key=True)
    hook_text: Mapped[Optional[str]] = mapped_column(String)
    hook_category: Mapped[Optional[str]] = mapped_column(String)
    view_z_score: Mapped[Optional[float]] = mapped_column(Float)
    is_collab: Mapped[Optional[bool]] = mapped_column(Boolean)
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=text("CURRENT_TIMESTAMP"))

    # Relationships
    video: Mapped["Video"] = relationship(back_populates="insight")


# ==========================================
# 3. DATABASE MANAGER
# ==========================================
# Enforce Foreign Keys in SQLite automatically whenever a connection is created
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class DatabaseManager:
    """Handles SQLAlchemy engine, session creation, and schema setup."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # SQLite connection string
        db_url = f"sqlite:///{self.db_path}"

        # Create the engine
        self.engine = create_engine(db_url, echo=False)

        # Create a configured "Session" class
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine)

    def get_session(self) -> Session:
        """Returns a new SQLAlchemy session."""
        return self.SessionLocal()

    def setup_database(self):
        """Creates all necessary tables based on the Declarative models."""
        logger.info(f"Initializing database at {self.db_path}...")

        # This single line reads all the classes we defined above and creates the exact tables!
        Base.metadata.create_all(bind=self.engine)

        logger.info(
            "Database schema successfully verified/created via SQLAlchemy.")
