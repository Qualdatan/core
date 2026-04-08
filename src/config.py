"""Zentrale Konfiguration für das Mayring-Analyse-Projekt."""

from pathlib import Path

# Pfade
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRANSCRIPTS_DIR = PROJECT_ROOT / "input"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Output-Dateien
ANALYSIS_JSON = OUTPUT_DIR / "analysis_results.json"
CODEBOOK_XLSX = OUTPUT_DIR / "codebook.xlsx"
QDPX_FILE = OUTPUT_DIR / "project.qdpx"
EVALUATION_XLSX = OUTPUT_DIR / "auswertung.xlsx"

# Hauptkategorien nach Mayring (deduktiv)
HAUPTKATEGORIEN = {
    "A": "Projektakquise und Auftragsvergabe",
    "B": "Planungsprozess und Projektablauf",
    "C": "Digitale Werkzeuge und BIM",
    "D": "Kommunikation und Zusammenarbeit",
    "E": "Normen, Vorschriften und Regularien",
    "F": "Kosten und Wirtschaftlichkeit",
    "G": "Nachhaltigkeit und Energieeffizienz",
    "H": "Herausforderungen und Problemfelder",
    "I": "Innovation und Zukunftsperspektiven",
    "J": "Qualitätssicherung und Kontrolle",
    "K": "Erfahrungswissen und Best Practices",
}

# Claude API
CLAUDE_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 16384
