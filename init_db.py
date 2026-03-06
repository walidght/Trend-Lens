import logging
from config import AppConfig
from core import DatabaseManager

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    config = AppConfig()
    db = DatabaseManager(config.db_path)
    
    # This will create the trendlens.db file and all tables
    db.setup_database()
    print("✅ Done! Check your 'data/' folder for trendlens.db")