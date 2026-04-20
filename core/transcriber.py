import re
import logging
from pathlib import Path
from typing import Optional

import whisper

logger = logging.getLogger(__name__)


class TranscriptionService:
    """Handles audio transcription and text processing using the Whisper AI model."""

    def __init__(self, model_name: str):
        logger.info(f"Loading Whisper model '{model_name}' into memory...")
        try:
            self.model = whisper.load_model(model_name)
        except Exception as e:
            logger.error(f"Failed to load Whisper model '{model_name}': {e}")
            raise

    # ==========================================
    # PUBLIC WORKFLOW
    # ==========================================

    def transcribe_and_extract_hook(self, audio_path: Path, num_sentences: Optional[int] = None) -> Optional[str]:
        """
        Orchestrates transcribing an audio file and extracting the hook.
        Returns None if transcription fails or no speech is detected.
        """
        if not audio_path.exists():
            logger.error(f"Audio file not found: {audio_path}")
            return None

        full_text = self._transcribe_audio(audio_path)
        
        if not full_text:
            return None

        return self._extract_sentences(full_text, num_sentences)

    # ==========================================
    # PRIVATE HELPER METHODS
    # ==========================================

    def _transcribe_audio(self, audio_path: Path) -> Optional[str]:
        """Handles strictly the raw transcription of the audio file using Whisper."""
        try:
            result = self.model.transcribe(str(audio_path))
            text = result.get("text", "").strip()
            
            return text if text else None

        except FileNotFoundError:
            logger.error("FFmpeg not found in system PATH. Whisper requires FFmpeg to read audio.")
            return None
        except Exception as e:
            logger.error(f"Transcription failure for {audio_path.name}: {e}")
            return None

    def _extract_sentences(self, text: str, num_sentences: Optional[int]) -> str:
        """Parses a block of text and returns up to the specified number of sentences."""
        # If no limit is set, return the whole text
        if num_sentences is None:
            return text

        # Split by sentence-ending punctuation followed by spaces
        sentences = re.split(r'(?<=[.!?]) +', text)
        return ' '.join(sentences[:num_sentences]).strip()