from dataclasses import dataclass


@dataclass
class AppConfig:
    """Centralized configuration for easy injection via CLI or Web Interface."""
    input_csv: str
    output_csv: str
    z_score_threshold: float = 1.5
    temp_dir: str = "data/temp"
    whisper_model: str = "base"
    hook_sentence_count: int = 10
