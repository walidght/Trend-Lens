"""
Migration 001 — add first_viral_at to video_insights
=====================================================
Why this exists
---------------
The TrendAnalyzer was refactored to use a proper baseline/candidate window split
(candidate_days vs baseline_days in AppConfig). As part of that change, video_insights
gained a new nullable column `first_viral_at` (DateTime) that records *when* a video
was first detected as viral. It is set once on first detection and deliberately never
overwritten, so Metabase reports can always answer "when was this first flagged?".

`setup_database()` uses create_all(checkfirst=True) which creates missing *tables*
but does not ALTER existing ones. Run this script once against any database that was
created before this migration was introduced.

Usage
-----
    python scripts/migrations/001_add_first_viral_at.py
    # or target a specific db:
    python scripts/migrations/001_add_first_viral_at.py path/to/trendlens.db
"""

import sys
from pathlib import Path

# Allow running from any working directory
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import sqlite3


def run(db_path: str = "data/trendlens.db"):
    path = Path(db_path)
    if not path.exists():
        print(f"Database not found at {path}. Nothing to migrate.")
        return

    con = sqlite3.connect(path)
    cur = con.cursor()

    # Check whether the column already exists so the script is safe to re-run.
    cur.execute("PRAGMA table_info(video_insights)")
    existing = {row[1] for row in cur.fetchall()}

    if "first_viral_at" in existing:
        print("Column 'first_viral_at' already exists — nothing to do.")
    else:
        cur.execute("ALTER TABLE video_insights ADD COLUMN first_viral_at DATETIME")
        con.commit()
        print("Added column 'first_viral_at' to video_insights.")

    con.close()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "data/trendlens.db"
    run(target)
