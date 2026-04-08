"""Zentrale Konfiguration für das Analyse-Projekt."""

from pathlib import Path

# Pfade
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "input"
TRANSCRIPTS_DIR = INPUT_DIR / "transcripts"
CODEBASES_DIR = INPUT_DIR / "codebases"
RECIPES_DIR = PROJECT_ROOT / "recipes"
OUTPUT_DIR = PROJECT_ROOT / "output"
CACHE_DIR = OUTPUT_DIR / ".cache"

# Sicherstellen, dass Verzeichnisse existieren
for d in [INPUT_DIR, TRANSCRIPTS_DIR, CODEBASES_DIR, OUTPUT_DIR, CACHE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Output-Dateien
ANALYSIS_JSON = OUTPUT_DIR / "analysis_results.json"
CODEBOOK_XLSX = OUTPUT_DIR / "codebook.xlsx"
QDPX_FILE = OUTPUT_DIR / "project.qdpx"
EVALUATION_XLSX = OUTPUT_DIR / "auswertung.xlsx"

# Default
DEFAULT_RECIPE = "mayring"
