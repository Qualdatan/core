"""Zentrale Konfiguration für das Analyse-Projekt.

Pfade sind via .env ueberschreibbar — siehe .env.example. Damit kann man
statische Daten (methods, codebases) und run-spezifische Daten (companies,
projects, transcripts) auch ausserhalb der Codebase ablegen.
"""

import os
from pathlib import Path

# Projekt-Wurzel (Speicherort dieses Files: src/qualdatan_core/config.py).
# In Dev/Test zeigt das auf die Repo-Wurzel (damit `input/…`-Defaults greifen);
# Prod-Installationen sollten stattdessen per Env-Var (METHODS_DIR,
# CODEBASES_DIR, …) auf einen expliziten Pfad zeigen.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _path(env_var: str, default: Path) -> Path:
    """Liest einen Pfad aus os.environ oder nimmt den Default. Relative
    Pfade werden gegen PROJECT_ROOT aufgeloest, absolute Pfade bleiben
    wie sie sind.
    """
    raw = os.environ.get(env_var)
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        else:
            p = p.resolve()
        return p
    return default.resolve()


# === Statische Daten (ueber Runs hinweg geteilt) ===
METHODS_DIR     = _path("METHODS_DIR",     PROJECT_ROOT / "input" / "methods")
CODEBASES_DIR   = _path("CODEBASES_DIR",   PROJECT_ROOT / "input" / "codebases")

# === Run-spezifische Daten ===
COMPANIES_DIR   = _path("COMPANIES_DIR",   PROJECT_ROOT / "input" / "companies")
TRANSCRIPTS_DIR = _path("TRANSCRIPTS_DIR", PROJECT_ROOT / "input" / "transcripts")
PROJECTS_DIR    = _path("PROJECTS_DIR",    PROJECT_ROOT / "input" / "projects")

# === Output ===
OUTPUT_ROOT     = _path("OUTPUT_DIR",      PROJECT_ROOT / "output")

# Backward-Compat alias (alter Name fuer den Methods-Ordner)
RECIPES_DIR = METHODS_DIR

# INPUT_DIR bleibt als reiner Convenience-Wert fuer Code, der noch darauf zeigt
INPUT_DIR = PROJECT_ROOT / "input"

# Output immer anlegen — wir schreiben hier rein.
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# Input-Defaults (TRANSCRIPTS/CODEBASES/PROJECTS/COMPANIES) nur anlegen, wenn
# sie unterhalb von PROJECT_ROOT liegen — d.h. der Default-Pfad. Wenn der
# User per .env auf einen externen Pfad zeigt, soll die Pipeline NICHT
# ungefragt fremde Ordner anlegen, sondern beim Lesen sauber failen.
for _d in (TRANSCRIPTS_DIR, CODEBASES_DIR, PROJECTS_DIR, COMPANIES_DIR):
    try:
        _d.relative_to(PROJECT_ROOT)
    except ValueError:
        continue  # external path → user is responsible
    _d.mkdir(parents=True, exist_ok=True)

# Defaults (ueberschreibbar via .env)
DEFAULT_RECIPE = os.getenv("DEFAULT_RECIPE", "mayring")
ENV_CLAUDE_MODEL = os.getenv("CLAUDE_MODEL")          # None = Recipe-Default
ENV_CLAUDE_MAX_TOKENS = os.getenv("CLAUDE_MAX_TOKENS") # None = Recipe-Default
