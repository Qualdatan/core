"""RunContext: Verwaltet Run-Verzeichnisse, Zustand und Resume-Logik.

Nutzt pipeline.db (SQLite) fuer State-Tracking.
Prompts und Responses bleiben als Textdateien (Debugging).

Phase D.2: Der RunContext kann zusaetzlich (additiv) an die globale
:class:`qualdatan_core.app_db.AppDB` angebunden werden. Die bestehende
``pipeline.db``-Semantik bleibt unveraendert; die App-DB spiegelt lediglich
Projekt-/Run-Katalog sowie Materialien/Facets fuer das Multi-Repo-Setup.
"""

import json
import sqlite3
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from .config import OUTPUT_ROOT
from .db import PipelineDB

if TYPE_CHECKING:
    from .app_db import AppDB


class RunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    FAILED = "failed"


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

    def __init__(self, run_dir: Path,
                 app_db: "AppDB | None" = None,
                 app_project_id: int | None = None,
                 app_run_id: int | None = None):
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

        # Datenbank (per-Run, bleibt unveraendert)
        self.db = PipelineDB(run_dir / "pipeline.db")

        # Optionale Anbindung an globale App-DB (Phase D.2)
        self.app_db: "AppDB | None" = app_db
        self.app_project_id: int | None = app_project_id
        self.app_run_id: int | None = app_run_id

    # ------------------------------------------------------------------
    # App-DB Anbindung (Phase D.2)
    # ------------------------------------------------------------------

    def attach_to_app_db(self, app_db: "AppDB", project_name: str, *,
                         preset_id: str = "",
                         config: dict | None = None,
                         description: str = "") -> int:
        """Verbindet diesen RunContext idempotent mit der App-DB.

        Holt das Projekt per :func:`get_project_by_name` oder legt es via
        :func:`create_project` neu an. Erzeugt anschliessend einen Run mit
        ``run_dir=str(self.run_dir)`` und Status ``pending``. Bei
        ``UNIQUE(project_id, run_dir)``-Verletzung (sqlite3.IntegrityError)
        wird der bestehende Run-Datensatz geholt.

        Args:
            app_db: Offene :class:`AppDB`-Instanz.
            project_name: Name des Projekts in der App-DB.
            preset_id: Optionales Bundle-/Preset-Kennzeichen.
            config: Optionaler Config-Snapshot, als JSON gespeichert.
            description: Beschreibung fuer neu angelegte Projekte.

        Returns:
            Die neue oder bestehende ``app_run_id``.
        """
        from .app_db import (
            create_project,
            create_run as _app_db_create_run,
            get_project_by_name,
        )

        project = get_project_by_name(app_db, project_name)
        if project is None:
            project = create_project(
                app_db,
                name=project_name,
                description=description,
                preset_id=preset_id,
            )

        config_json = json.dumps(config or {})
        try:
            run = _app_db_create_run(
                app_db,
                project_id=project.id,
                run_dir=str(self.run_dir),
                config_json=config_json,
                status="pending",
            )
            run_id = run.id
        except sqlite3.IntegrityError:
            # UNIQUE(project_id, run_dir) verletzt -> existierenden Run holen
            with app_db.connection() as conn:
                row = conn.execute(
                    "SELECT id FROM runs WHERE project_id = ? AND run_dir = ?",
                    (project.id, str(self.run_dir)),
                ).fetchone()
            if row is None:  # pragma: no cover - defensive
                raise
            run_id = row["id"]

        self.app_db = app_db
        self.app_project_id = project.id
        self.app_run_id = run_id
        return run_id

    def register_material(self, material_kind: str, path: str, *,
                          relative_path: str = "",
                          source_label: str = "") -> int | None:
        """Registriert ein Material in der App-DB (falls angebunden).

        Args:
            material_kind: Z.B. ``"transcript"``, ``"pdf_text"``, ``"image"``.
            path: Absoluter Pfad zur Quelldatei.
            relative_path: Projekt-relativer Pfad (optional).
            source_label: Freies Label (optional).

        Returns:
            Die neue ``run_material``-ID, oder ``None`` wenn dieser Context
            nicht an eine App-DB angebunden ist oder das Einfuegen scheitert.
        """
        if self.app_db is None or self.app_run_id is None:
            return None
        try:
            from .app_db import add_run_material
            material = add_run_material(
                self.app_db,
                self.app_run_id,
                material_kind=material_kind,
                path=path,
                relative_path=relative_path,
                source_label=source_label,
            )
            return material.id
        except Exception:
            return None

    def register_facet(self, facet_id: str, *, bundle_id: str = "",
                       params: dict | None = None) -> int | None:
        """Aktiviert eine Facet fuer diesen Run in der App-DB.

        Args:
            facet_id: Stabiler Facet-Identifier.
            bundle_id: Optionales Bundle, zu dem die Facet gehoert.
            params: Optionale Parameter (werden als JSON serialisiert).

        Returns:
            Die neue ``run_facet``-ID, oder ``None`` wenn nicht angebunden
            oder das Einfuegen scheitert.
        """
        if self.app_db is None or self.app_run_id is None:
            return None
        try:
            from .app_db import add_run_facet
            facet = add_run_facet(
                self.app_db,
                self.app_run_id,
                facet_id=facet_id,
                bundle_id=bundle_id,
                params_json=json.dumps(params or {}),
            )
            return facet.id
        except Exception:
            return None

    def mark_failed(self, reason: str = ""):
        """Markiert den Run als fehlgeschlagen.

        Setzt in der ``pipeline.db`` den Status auf ``INTERRUPTED`` (damit
        der Resume-Mechanismus den Run findet) und spiegelt dies, falls
        angebunden, als ``failed`` in die App-DB (best-effort).

        Args:
            reason: Optionaler Fehlergrund (wird in der pipeline.db gesetzt).
        """
        self.db.set_state("status", RunStatus.INTERRUPTED)
        if reason:
            self.db.set_state("error_reason", reason)
        self.db.set_state("updated_at", datetime.now().isoformat())

        if self.app_db is not None and self.app_run_id is not None:
            try:
                from .app_db import update_run_status
                update_run_status(
                    self.app_db,
                    self.app_run_id,
                    "failed",
                    finished_at=datetime.now().isoformat(),
                )
            except Exception:
                pass

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

        # App-DB Mirror (best-effort, Phase D.2)
        if self.app_db is not None and self.app_run_id is not None:
            try:
                from .app_db import update_run_status
                update_run_status(self.app_db, self.app_run_id, "running")
            except Exception:
                pass

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

        # App-DB Mirror (best-effort, Phase D.2)
        if self.app_db is not None and self.app_run_id is not None:
            try:
                from .app_db import update_run_status
                update_run_status(
                    self.app_db,
                    self.app_run_id,
                    "completed",
                    finished_at=datetime.now().isoformat(),
                )
            except Exception:
                pass

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

def create_run(*, app_db: "AppDB | None" = None,
               project_name: str | None = None,
               preset_id: str = "",
               config: dict | None = None) -> RunContext:
    """Erstellt einen neuen Run mit Datetime-Verzeichnis.

    Args:
        app_db: Optionale App-DB-Instanz. Wenn zusammen mit ``project_name``
            gesetzt, wird der neue :class:`RunContext` automatisch via
            :meth:`RunContext.attach_to_app_db` angebunden.
        project_name: Projektname in der App-DB (nur relevant mit ``app_db``).
        preset_id: Bundle-/Preset-ID fuer das Projekt (optional).
        config: Config-Snapshot, der als JSON in die App-DB gespeichert wird.

    Returns:
        Der frisch erzeugte, optional an die App-DB angebundene RunContext.
    """
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

    if app_db is not None and project_name:
        ctx.attach_to_app_db(
            app_db,
            project_name,
            preset_id=preset_id,
            config=config,
        )

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
