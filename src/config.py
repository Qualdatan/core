"""Zentrale Konfiguration für das Analyse-Projekt."""

import os
from pathlib import Path

# Pfade
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "input"
TRANSCRIPTS_DIR = INPUT_DIR / "transcripts"
CODEBASES_DIR = INPUT_DIR / "codebases"
RECIPES_DIR = PROJECT_ROOT / "recipes"
OUTPUT_ROOT = PROJECT_ROOT / "output"

# Sicherstellen, dass Verzeichnisse existieren
for d in [INPUT_DIR, TRANSCRIPTS_DIR, CODEBASES_DIR, OUTPUT_ROOT]:
    d.mkdir(parents=True, exist_ok=True)

# Defaults (ueberschreibbar via .env)
DEFAULT_RECIPE = os.getenv("DEFAULT_RECIPE", "mayring")
ENV_CLAUDE_MODEL = os.getenv("CLAUDE_MODEL")          # None = Recipe-Default
ENV_CLAUDE_MAX_TOKENS = os.getenv("CLAUDE_MAX_TOKENS") # None = Recipe-Default
