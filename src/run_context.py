"""RunContext: Verwaltet Run-Verzeichnisse, Zustand und Resume-Logik.

Nutzt pipeline.db (SQLite) fuer State-Tracking.
Prompts und Responses bleiben als Textdateien (Debugging).
"""

import json
from datetime import datetime
from enum import Enum
from pathlib import Path

from .config import OUTPUT_ROOT
from .db import PipelineDB


class RunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"


class RunContext:
    """Kontext fuer einen einzelnen Pipeline-Durchlauf.

    Erstellt pro Run ein eigenes Verzeichnis:
        output/2026-04-08_14-30-15/
        ├── pipeline.db          ← SQLite (Status, Cache, Ergebnisse)
        ├── prompts/             ← Textdateien (Debugging)
        ├── responses/           ← Textdateien (Debugging)
        ├── evaluation/
        │   ├── codebook.xlsx
        │   └── auswertung.xlsx
        ├── qda/
        │   └── project.qdpx
        └── analysis_results.json
    """

    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.cache_dir = run_dir / ".cache"  # Legacy-Kompatibilitaet
        self.prompts_dir = run_dir / "prompts"
        self.responses_dir = run_dir / "responses"
        self.parsed_dir = self.cache_dir / "parsed"  # Legacy
        self.evaluation_dir = run_dir / "evaluation"
        self.qda_dir = run_dir / "qda"

        # Output-Dateien
        self.analysis_json = run_dir / "analysis_results.json"
        self.codebook_xlsx = self.evaluation_dir / "codebook.xlsx"
        self.evaluation_xlsx = self.evaluation_dir / "auswertung.xlsx"
        self.qdpx_file = self.qda_dir / "project.qdpx"

        # Datenbank
        self.db = PipelineDB(run_dir / "pipeline.db")

    # ------------------------------------------------------------------
    # Annotator-Pfade (Phase 5)
    # ------------------------------------------------------------------

    @property
    def annotated_dir(self) -> Path:
        """Verzeichnis fuer annotierte PDFs: <run>/annotated/."""
        return self.run_dir / "annotated"

    @property
    def mapping_dir(self) -> Path:
        """Verzeichnis fuer Code-Farb-Mapping: <run>/mapping/."""
        return self.run_dir / "mapping"

    def annotated_path_for(self, project: str, relative_path: str) -> Path:
        """Liefert den Zielpfad fuer ein annotiertes PDF und legt Parent an.

        Der ``relative_path`` ist relativ zum Projekt-Ordner
        (wie er in ``pdf_documents.relative_path`` steht); bereits
        vorangestellte ``project``-Segmente werden toleriert und nicht
        doppelt eingefuegt.
        """
        rel = Path(relative_path)
        # Wenn relative_path bereits mit dem Projekt startet, nicht doppeln
        if project and rel.parts and rel.parts[0] == project:
            target = self.annotated_dir / rel
        elif project:
            target = self.annotated_dir / project / rel
        else:
            target = self.annotated_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    # ------------------------------------------------------------------
    # Company-aware Pfade (Phase 3)
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_segment(name: str) -> str:
        """Macht einen Company-/Projekt-Namen pfadtauglich."""
        return (name or "").replace("/", "_").replace("\\", "_").strip()

    def company_dir(self, company_name: str) -> Path:
        """Liefert ``<run>/<company>`` und legt das Verzeichnis an."""
        d = self.run_dir / self._safe_segment(company_name)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def company_qdpx_path(self, company_name: str,
                          name: str = "interviews.qdpx") -> Path:
        """Liefert ``<run>/<company>/qda/<name>`` und legt den Parent an."""
        target = self.company_dir(company_name) / "qda" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    def company_annotated_dir(self, company_name: str) -> Path:
        """Liefert ``<run>/<company>/annotated`` und legt das Verzeichnis an."""
        d = self.company_dir(company_name) / "annotated"
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ------------------------------------------------------------------
    # Company-scoped Interview-Pfade (Phase 4: saubere Output-Struktur)
    # ------------------------------------------------------------------

    def company_analysis_json(self, company_name: str) -> Path:
        """Liefert ``<run>/<company>/analysis_results.json`` (Parent angelegt)."""
        target = self.company_dir(company_name) / "analysis_results.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    def company_prompts_dir(self, company_name: str) -> Path:
        """Liefert ``<run>/<company>/prompts/`` und legt das Verzeichnis an."""
        d = self.company_dir(company_name) / "prompts"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def company_responses_dir(self, company_name: str) -> Path:
        """Liefert ``<run>/<company>/responses/`` und legt das Verzeichnis an."""
        d = self.company_dir(company_name) / "responses"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def company_interview_sample_dir(self, company_name: str) -> Path:
        """Liefert ``<run>/<company>/_interview_sample/`` und legt es an."""
        d = self.company_dir(company_name) / "_interview_sample"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def ensure_dirs(self):
        """Erstellt nur die wirklich notwendigen Unterverzeichnisse.

        Die Consumer (step1, step3, step4, ...) sind selbst dafuer
        verantwortlich, ihre Ausgabe-Verzeichnisse via ``mkdir(parents=True,
        exist_ok=True)`` anzulegen, bevor sie schreiben. Dadurch vermeiden
        wir leere ``evaluation/``- und ``qda/``-Ordner in Runs, die sie
        gar nicht benutzen.
        """
        for d in [self.cache_dir, self.parsed_dir]:
            d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Run State (via DB)
    # ------------------------------------------------------------------

    def init_state(self, recipe_id: str | None = None,
                   codebase_name: str | None = None,
                   transcripts: list[str] | None = None,
                   mode: str | None = None,
                   companies: list[str] | None = None,
                   **extra):
        """Initialisiert den Run-State beim Start.

        Alle Parameter sind optional, damit die Funktion sowohl vom
        klassischen Interview-/Dokument-Flow als auch vom neuen
        Company-Orchestrator in Phase 3 genutzt werden kann. ``extra``
        schluesselt zusaetzliche State-Felder direkt in die DB durch.
        """
        self.db.set_state("status", RunStatus.RUNNING)
        if recipe_id is not None:
            self.db.set_state("recipe_id", recipe_id)
        if codebase_name is not None:
            self.db.set_state("codebase_name", codebase_name)
        self.db.set_state("transcripts", transcripts or [])
        if mode is not None:
            self.db.set_state("mode", mode)
        if companies is not None:
            self.db.set_state("companies", companies)
        for key, value in extra.items():
            self.db.set_state(key, value)
        self.db.set_state("completed_transcripts", [])
        self.db.set_state("steps_completed", [])
        self.db.set_state("started_at", datetime.now().isoformat())
        self.db.set_state("updated_at", datetime.now().isoformat())

    def mark_transcript_done(self, filename: str):
        """Markiert ein Transkript als fertig analysiert."""
        done = self.db.get_state("completed_transcripts", [])
        if filename not in done:
            done.append(filename)
        self.db.set_state("completed_transcripts", done)
        self.db.set_state("updated_at", datetime.now().isoformat())

    def mark_step_done(self, step: int):
        """Markiert einen Pipeline-Schritt als abgeschlossen."""
        steps = self.db.get_state("steps_completed", [])
        if step not in steps:
            steps.append(step)
        self.db.set_state("steps_completed", steps)
        self.db.set_state("updated_at", datetime.now().isoformat())

    def mark_completed(self):
        """Markiert den gesamten Run als abgeschlossen."""
        self.db.set_state("status", RunStatus.COMPLETED)
        self.db.set_state("finished_at", datetime.now().isoformat())
        self.db.set_state("updated_at", datetime.now().isoformat())

    def get_state(self) -> dict:
        return self.db.get_all_state()

    def get_pending_transcripts(self) -> list[str]:
        """Gibt Transkripte zurueck, die noch nicht analysiert wurden."""
        all_t = self.db.get_state("transcripts", [])
        done_t = self.db.get_state("completed_transcripts", [])
        return [t for t in all_t if t not in done_t]

    def is_step_done(self, step: int) -> bool:
        steps = self.db.get_state("steps_completed", [])
        return step in steps

    # ------------------------------------------------------------------
    # Cache: Prompts, Responses (bleiben Textdateien)
    # ------------------------------------------------------------------

    def _safe_filename(self, name: str) -> str:
        """Macht Dateinamen sicher."""
        return name.replace("/", "_").replace("\\", "_").replace(" ", "_")

    def cache_prompt(self, filename: str, prompt: str,
                     prompts_dir: Path | None = None):
        """Speichert den gesendeten Prompt.

        ``prompts_dir`` erlaubt es, einen alternativen Ablageort zu
        verwenden (z.B. company-scoped ``<run>/<company>/prompts``).
        """
        safe = self._safe_filename(filename)
        target_dir = prompts_dir or self.prompts_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{safe}.txt"
        path.write_text(prompt, encoding="utf-8")

    def cache_response(self, filename: str, response_text: str,
                       responses_dir: Path | None = None):
        """Speichert die rohe API-Antwort.

        ``responses_dir`` erlaubt es, einen alternativen Ablageort zu
        verwenden (z.B. company-scoped ``<run>/<company>/responses``).
        """
        safe = self._safe_filename(filename)
        target_dir = responses_dir or self.responses_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{safe}.txt"
        path.write_text(response_text, encoding="utf-8")

    def cache_parsed(self, filename: str, data: dict):
        """Speichert das extrahierte JSON (Legacy + DB)."""
        safe = self._safe_filename(filename)
        # Weiterhin als Datei fuer Debugging
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
        db_path = d / "pipeline.db"
        state_file = d / "run_state.json"  # Legacy
        if db_path.exists():
            try:
                ctx = RunContext(d)
                status = ctx.db.get_state("status")
                if status != RunStatus.COMPLETED:
                    interrupted.append(ctx)
            except Exception:
                continue
        elif state_file.exists():
            # Legacy: JSON-basierter State
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
    ctx.db.set_state("status", RunStatus.RUNNING)
    ctx.db.set_state("updated_at", datetime.now().isoformat())
    return ctx
