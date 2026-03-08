from pathlib import Path
import pandas as pd
import logging
from config import AppConfig
from core.downloader import MediaDownloader
from core.repository import TrendLensRepository

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Ties all services together to run the data pipeline."""

    def __init__(self, config: AppConfig, repo: TrendLensRepository, analyzer, transcriber):
        self.config = config
        self.repo = repo
        self.temp_dir = Path(config.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Initialize services
        self.transcriber = transcriber
        self.analyzer = analyzer
        self.downloader = MediaDownloader()

    def run(self, progress_callback=None) -> int:
        logger.info("Starting pipeline execution.")
        outliers_df = self.analyzer.process_data()

        if outliers_df.empty:
            logger.info("No new outliers to process.")
            return 0

        total_videos = len(outliers_df)
        processed_count = 0

        for index, row in outliers_df.iterrows():
            video_id = row['video_id']
            audio_url = row.get('audio_url')
            url = row.get('url', 'Unknown URL')
            z_score = float(row['view_z_score'])

            logger.info(f"Processing: {url} (Z-Score: {z_score:.2f})")

            if pd.isna(audio_url) or not audio_url:
                logger.warning("No audio URL found in row. Skipping.")
                continue

            audio_path = self.temp_dir / f"temp_audio_{video_id}.m4a"

            # Execute Download and Transcribe
            if self.downloader.download_audio(audio_url, audio_path):
                hook = self.transcriber.extract_hook(
                    audio_path, self.config.hook_sentence_count)

                logger.info("Hook extracted successfully.")

                self.repo.save_extracted_hook(video_id, hook, float(row['view_z_score']))
                processed_count += 1

                # Cleanup
                if audio_path.exists():
                    audio_path.unlink()
            else:
                logger.error(f"Download failed for {url}")

            # Update Streamlit Loading Bar
            if progress_callback:
                progress_callback(processed_count, total_videos, video_id)

        return processed_count
