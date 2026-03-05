import logging
from config import AppConfig
from core import PipelineOrchestrator
from core import TranscriptionService
from analyzers import InstagramAnalyzer

# 1. Setup global logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

# 2. Load Configuration
config = AppConfig(
    input_csv="data/input/dataset_instagram-scraper.csv", 
    output_csv="data/output/viral_hooks.csv"
)

# 3. Initialize Services
transcriber = TranscriptionService(config.whisper_model)
insta_analyzer = InstagramAnalyzer(config)

# 4. Inject them into the Pipeline and run
pipeline = PipelineOrchestrator(
    config=config, 
    analyzer=insta_analyzer, 
    transcriber=transcriber
)

pipeline.run()