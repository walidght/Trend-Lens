from pathlib import Path
import whisper
import re
import logging

# __name__ automatically gets the name of the current file/module (e.g., 'core.downloader')
logger = logging.getLogger(__name__)


class TranscriptionService:
    """Wraps the Whisper AI model to ensure it is only loaded into memory once."""

    def __init__(self, model_name: str):
        logger.info(f"Loading Whisper model '{model_name}' into memory...")
        self.model = whisper.load_model(model_name)

    def extract_hook(self, audio_path: Path, num_sentences: int) -> str:
        try:
            result = self.model.transcribe(str(audio_path))
            full_text = result["text"].strip()

            if not full_text:
                return "No speech detected."

            sentences = re.split(r'(?<=[.!?]) +', full_text)
            return ' '.join(sentences[:num_sentences]).strip()

        except FileNotFoundError:
            logger.error("FFmpeg not found in system PATH.")
            return "FFmpeg Error"
        except Exception as e:
            logger.error(f"Transcription failure: {e}")
            return "Transcription Error"
