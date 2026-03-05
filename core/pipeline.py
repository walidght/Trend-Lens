from pathlib import Path
import pandas as pd
import logging
from config import AppConfig
from core import MediaDownloader

# __name__ automatically gets the name of the current file/module (e.g., 'core.downloader')
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    """Ties all services together to run the data pipeline."""

    def __init__(self, config: AppConfig, analyzer, transcriber):
        self.config = config
        self.temp_dir = Path(config.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Initialize services
        self.transcriber = transcriber
        self.analyzer = analyzer
        self.downloader = MediaDownloader()

    def run(self):
        logger.info("Starting pipeline execution.")
        outliers_df = self.analyzer.process_data()
        hooks = []

        for index, row in outliers_df.iterrows():
            url = row.get('url', 'Unknown URL')
            audio_url = row.get('audioUrl')

            logger.info(
                f"Processing: {url} (Z-Score: {row['view_z_score']:.2f})")

            if pd.isna(audio_url):
                logger.warning("No audio URL found in row. Skipping.")
                hooks.append("No audio URL")
                continue

            audio_path = self.temp_dir / f"temp_audio_{index}.m4a"

            # Execute Download and Transcribe
            if self.downloader.download_audio(audio_url, audio_path):
                hook = self.transcriber.extract_hook(
                    audio_path, self.config.hook_sentence_count)
                hooks.append(hook)
                logger.info(f"Hook extracted successfully.")

                # Cleanup
                if audio_path.exists():
                    audio_path.unlink()
            else:
                hooks.append("Download failed")

        outliers_df['hook'] = hooks
        self._export_results(outliers_df)

    def _export_results(self, df: pd.DataFrame):
        target_columns = [
            'url', 'ownerUsername', 'videoPlayCount', 'likesCount',
            'commentsCount', 'is_collab', 'view_z_score', 'hook'
        ]
        valid_columns = [col for col in target_columns if col in df.columns]

        final_df = df[valid_columns]
        final_df.to_csv(self.config.output_csv, index=False)
        logger.info(
            f"Pipeline complete. Results saved to {self.config.output_csv}")
