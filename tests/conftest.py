"""Shared pytest fixtures. Expanded as we add tests per module."""
import sys
from pathlib import Path

# Make the project root importable so `import core.xxx` works when pytest is run from any cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
