"""RunContext: Verwaltet Run-Verzeichnisse, Zustand und Resume-Logik."""

import json
from datetime import datetime
from enum import Enum
from pathlib import Path

from .config import OUTPUT_ROOT


class RunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"


class RunContext:
    """Kontext fuer einen einzelnen Pipeline-Durchlauf.

    Erstellt pro Run ein eigenes Verzeichnis:
        output/2026-04-08_14-30-15/
        ├── .cache/
        │   ├── prompts/        ← gesendete Prompts
        │   ├── responses/      ← rohe API-Antworten
        │   └── parsed/         ← extrahiertes JSON
        ├── evaluation/
        │   ├── codebook.xlsx
        │   └── auswertung.xlsx
        ├── qda/
        │   └── project.qdpx
        ├── analysis_results.json
        └── run_state.json
    """

    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.cache_dir = run_dir / ".cache"
        self.prompts_dir = self.cache_dir / "prompts"
        self.responses_dir = self.cache_dir / "responses"
        self.parsed_dir = self.cache_dir / "parsed"
        self.evaluation_dir = run_dir / "evaluation"
        self.qda_dir = run_dir / "qda"

        # Output-Dateien
        self.analysis_json = run_dir / "analysis_results.json"
        self.codebook_xlsx = self.evaluation_dir / "codebook.xlsx"
        self.evaluation_xlsx = self.evaluation_dir / "auswertung.xlsx"
        self.qdpx_file = self.qda_dir / "project.qdpx"
        self.state_file = run_dir / "run_state.json"

    def ensure_dirs(self):
        """Erstellt alle Unterverzeichnisse."""
        for d in [
            self.cache_dir, self.prompts_dir, self.responses_dir,
            self.parsed_dir, self.evaluation_dir, self.qda_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Run State
    # ------------------------------------------------------------------

    def _load_state(self) -> dict:
        if self.state_file.exists():
            return json.loads(self.state_file.read_text(encoding="utf-8"))
        return {}

    def _save_state(self, state: dict):
        self.state_file.write_text(
            json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def init_state(self, recipe_id: str, codebase_name: str | None,
                   transcripts: list[str]):
        """Initialisiert den Run-State beim Start."""
        state = {
            "status": RunStatus.RUNNING,
            "recipe_id": recipe_id,
            "codebase_name": codebase_name,
            "transcripts": transcripts,
            "completed_transcripts": [],
            "steps_completed": [],
            "started_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }
        self._save_state(state)

    def mark_transcript_done(self, filename: str):
        """Markiert ein Transkript als fertig analysiert."""
        state = self._load_state()
        if filename not in state.get("completed_transcripts", []):
            state.setdefault("completed_transcripts", []).append(filename)
        state["updated_at"] = datetime.now().isoformat()
        self._save_state(state)

    def mark_step_done(self, step: int):
        """Markiert einen Pipeline-Schritt als abgeschlossen."""
        state = self._load_state()
        if step not in state.get("steps_completed", []):
            state.setdefault("steps_completed", []).append(step)
        state["updated_at"] = datetime.now().isoformat()
        self._save_state(state)

    def mark_completed(self):
        """Markiert den gesamten Run als abgeschlossen."""
        state = self._load_state()
        state["status"] = RunStatus.COMPLETED
        state["finished_at"] = datetime.now().isoformat()
        state["updated_at"] = datetime.now().isoformat()
        self._save_state(state)

    def get_state(self) -> dict:
        return self._load_state()

    def get_pending_transcripts(self) -> list[str]:
        """Gibt Transkripte zurueck, die noch nicht analysiert wurden."""
        state = self._load_state()
        all_t = state.get("transcripts", [])
        done_t = state.get("completed_transcripts", [])
        return [t for t in all_t if t not in done_t]

    def is_step_done(self, step: int) -> bool:
        state = self._load_state()
        return step in state.get("steps_completed", [])

    # ------------------------------------------------------------------
    # Cache: Prompts, Responses, Parsed JSON
    # ------------------------------------------------------------------

    def _safe_filename(self, name: str) -> str:
        """Macht Dateinamen sicher."""
        return name.replace("/", "_").replace("\\", "_").replace(" ", "_")

    def cache_prompt(self, filename: str, prompt: str):
        """Speichert den gesendeten Prompt."""
        safe = self._safe_filename(filename)
        path = self.prompts_dir / f"{safe}.txt"
        path.write_text(prompt, encoding="utf-8")

    def cache_response(self, filename: str, response_text: str):
        """Speichert die rohe API-Antwort."""
        safe = self._safe_filename(filename)
        path = self.responses_dir / f"{safe}.txt"
        path.write_text(response_text, encoding="utf-8")

    def cache_parsed(self, filename: str, data: dict):
        """Speichert das extrahierte JSON."""
        safe = self._safe_filename(filename)
        path = self.parsed_dir / f"{safe}.json"
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def get_cached_parsed(self, filename: str) -> dict | None:
        """Liest gecachtes Parse-Ergebnis, falls vorhanden."""
        safe = self._safe_filename(filename)
        path = self.parsed_dir / f"{safe}.json"
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        return None


# ======================================================================
# Run-Verwaltung: Erstellen, Finden, Resume
# ======================================================================

def create_run() -> RunContext:
    """Erstellt einen neuen Run mit Datetime-Verzeichnis."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = OUTPUT_ROOT / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)
    ctx = RunContext(run_dir)
    ctx.ensure_dirs()

    # Symlink 'latest' aktualisieren
    latest = OUTPUT_ROOT / "latest"
    if latest.is_symlink() or latest.exists():
        latest.unlink()
    try:
        latest.symlink_to(run_dir.name)
    except OSError:
        pass  # Symlinks auf Windows manchmal nicht moeglich

    return ctx


def find_interrupted_runs() -> list[RunContext]:
    """Findet alle unterbrochenen (nicht abgeschlossenen) Runs."""
    interrupted = []
    if not OUTPUT_ROOT.exists():
        return interrupted
    for d in sorted(OUTPUT_ROOT.iterdir(), reverse=True):
        if not d.is_dir() or d.name in ("latest", ".cache"):
            continue
        state_file = d / "run_state.json"
        if state_file.exists():
            try:
                state = json.loads(state_file.read_text(encoding="utf-8"))
                if state.get("status") != RunStatus.COMPLETED:
                    interrupted.append(RunContext(d))
            except (json.JSONDecodeError, OSError):
                continue
    return interrupted


def resume_run(run_dir: Path) -> RunContext:
    """Setzt einen unterbrochenen Run fort."""
    ctx = RunContext(run_dir)
    ctx.ensure_dirs()
    state = ctx._load_state()
    state["status"] = RunStatus.RUNNING
    state["updated_at"] = datetime.now().isoformat()
    ctx._save_state(state)
    return ctx
