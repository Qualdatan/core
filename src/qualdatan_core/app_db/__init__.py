# SPDX-License-Identifier: AGPL-3.0-only
"""Globale Qualdatan-App-DB.

Ersetzt in Phase D nach und nach die vielen `pipeline.db`-Dateien pro Run
durch eine einzige, globale SQLite unter
``<platformdirs.user_documents_dir>/Qualdatan/app.db``. Pfad kann mit der
Env-Variable ``QUALDATAN_APP_DB`` ueberschrieben werden.

Scope Phase D (dieses Modul):
    - Schema v1: projects, runs, run_materials, run_facets, codings,
      codebook_entries, cache_llm, cache_pdf, app_state.
    - Schema-Versionierung via ``PRAGMA user_version``.
    - Low-Level :class:`AppDB` mit Connection-Management.
    - DAOs liegen in Geschwister-Modulen / werden von den jeweiligen
      Feature-Teams (Phase D.A–D.D) ergaenzt.

Legacy-Hinweis: ``qualdatan_core.db.PipelineDB`` bleibt bestehen und
verwaltet weiterhin per-Run Caching/Artefakte. Die App-DB haelt das
Projekt-/Run-Katalog-Level und die globalen Caches (LLM, PDF).
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

try:
    from platformdirs import user_documents_dir
except ImportError:  # pragma: no cover
    user_documents_dir = None  # type: ignore


_ENV_PATH = "QUALDATAN_APP_DB"
_SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
_SCHEMA_V1 = """
-- Projekt-Katalog ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS projects (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    preset_id   TEXT NOT NULL DEFAULT '',      -- e.g. Bundle-Id
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Runs ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id   INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    run_dir      TEXT NOT NULL,                 -- absolut, enthaelt Artefakte
    config_json  TEXT NOT NULL DEFAULT '{}',
    status       TEXT NOT NULL DEFAULT 'pending', -- pending|running|completed|failed
    started_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at  TEXT,
    UNIQUE (project_id, run_dir)
);
CREATE INDEX IF NOT EXISTS idx_runs_project_status
    ON runs(project_id, status);

-- Welche Materialien im Run sind ---------------------------------------------
CREATE TABLE IF NOT EXISTS run_materials (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    material_kind   TEXT NOT NULL,        -- transcript|pdf_text|pdf_visual|image
    path            TEXT NOT NULL,        -- absolut
    relative_path   TEXT NOT NULL DEFAULT '',
    source_label    TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_run_materials_run ON run_materials(run_id);

-- Welche Facets im Run aktiv + mit welchen Parametern ------------------------
CREATE TABLE IF NOT EXISTS run_facets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    facet_id    TEXT NOT NULL,
    bundle_id   TEXT NOT NULL DEFAULT '',
    params_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_run_facets_run ON run_facets(run_id);

-- Codings (flaches Ergebnis-Log, cross-run) ----------------------------------
CREATE TABLE IF NOT EXISTS codings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    project_id      INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    document        TEXT NOT NULL,            -- Dateiname / Quelle
    code_id         TEXT NOT NULL,
    segment_start   INTEGER,                  -- char offset; NULL fuer visual
    segment_end     INTEGER,
    text            TEXT NOT NULL DEFAULT '',
    bbox_json       TEXT NOT NULL DEFAULT '',  -- fuer visual facets
    confidence      REAL,
    justification   TEXT NOT NULL DEFAULT '',
    facet_id        TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_codings_run ON codings(run_id);
CREATE INDEX IF NOT EXISTS idx_codings_project_code
    ON codings(project_id, code_id);

-- Per-Projekt-Codebook-Overrides (Phase E Vorbereitung) ----------------------
CREATE TABLE IF NOT EXISTS codebook_entries (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id            INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    code_id               TEXT NOT NULL,
    label_override        TEXT,
    color_override        TEXT,
    definition_override   TEXT,
    examples_override     TEXT,       -- JSON-Array von Strings
    updated_at            TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (project_id, code_id)
);

-- LLM-Response-Cache ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS cache_llm (
    key_sha      TEXT PRIMARY KEY,       -- sha256 ueber (model + prompt + params)
    model        TEXT NOT NULL,
    prompt_hash  TEXT NOT NULL,
    response     TEXT NOT NULL,
    tokens_in    INTEGER,
    tokens_out   INTEGER,
    created_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- PDF-Extraktions-Cache ------------------------------------------------------
CREATE TABLE IF NOT EXISTS cache_pdf (
    key_sha          TEXT PRIMARY KEY,   -- sha256 ueber (path + mtime + size)
    path             TEXT NOT NULL,
    mtime            REAL NOT NULL,
    size_bytes       INTEGER NOT NULL,
    extraction_json  TEXT NOT NULL,
    created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Globaler Key-Value (letzter Run, UI-Hints etc.) ----------------------------
CREATE TABLE IF NOT EXISTS app_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------
def default_app_db_path() -> Path:
    """Default-Pfad: ``<Documents>/Qualdatan/app.db`` bzw. Env-Override."""
    env = os.environ.get(_ENV_PATH)
    if env:
        return Path(env)
    if user_documents_dir is None:  # pragma: no cover
        return Path.home() / "Documents" / "Qualdatan" / "app.db"
    return Path(user_documents_dir()) / "Qualdatan" / "app.db"


# ---------------------------------------------------------------------------
# Connection + Migrations
# ---------------------------------------------------------------------------
def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Bringt ``conn`` auf :data:`_SCHEMA_VERSION`.

    Jede Version ``N`` hat einen Block ``_MIGRATE_V{N-1}_TO_V{N}`` (derzeit
    nur v0 → v1 = Initial-Schema). Neue Versionen werden hier ergaenzt.
    """

    current = conn.execute("PRAGMA user_version").fetchone()[0]
    if current >= _SCHEMA_VERSION:
        return
    # v0 → v1: Initial-Schema.
    if current < 1:
        conn.executescript(_SCHEMA_V1)
    # Zukuenftige Migrationen hier:
    # if current < 2: conn.executescript(_MIGRATE_V1_TO_V2)
    conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")


class AppDB:
    """Duenner Wrapper um die globale SQLite.

    Thread-safe (Connection per Thread). Feature-DAOs werden in
    Geschwister-Modulen implementiert und greifen per :meth:`connection`
    auf den Connection-Pool zu.

    Example:
        >>> db = AppDB.open(":memory:")
        >>> with db.transaction() as conn:
        ...     conn.execute("INSERT INTO projects(name) VALUES (?)", ("demo",))
        >>> db.close()
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._local = threading.local()
        self._closed = False
        self._ensure_schema()

    @classmethod
    def open(cls, path: Path | str | None = None) -> "AppDB":
        """Oeffnet die App-DB unter ``path`` (Default: :func:`default_app_db_path`).

        Legt fehlende Verzeichnisse an. Akzeptiert ``":memory:"`` fuer Tests.
        """
        if path is None:
            path = default_app_db_path()
        path = Path(path) if str(path) != ":memory:" else Path(":memory:")
        if str(path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        return cls(path)

    # ------------------------------------------------------------------
    @property
    def path(self) -> Path:
        return self._path

    @property
    def schema_version(self) -> int:
        with self.connection() as conn:
            return conn.execute("PRAGMA user_version").fetchone()[0]

    # ------------------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self._path), isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        if str(self._path) != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def _get_thread_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def _ensure_schema(self) -> None:
        conn = self._get_thread_conn()
        _apply_migrations(conn)

    @contextmanager
    def connection(self):
        """Context manager for a thread-local connection."""
        if self._closed:
            raise RuntimeError("AppDB bereits geschlossen")
        yield self._get_thread_conn()

    @contextmanager
    def transaction(self):
        """Gibt eine Verbindung mit aktiver Transaktion zurueck.

        Commit bei erfolgreichem Exit, Rollback bei Exception.
        """
        conn = self._get_thread_conn()
        conn.execute("BEGIN")
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
        self._closed = True

    def __enter__(self) -> "AppDB":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def open_app_db(path: Path | str | None = None) -> AppDB:
    """Convenience-Wrapper, spiegelt :meth:`AppDB.open`."""
    return AppDB.open(path)


from .caches import (
    LLMCacheEntry,
    PDFCacheEntry,
    llm_cache_clear,
    llm_cache_get,
    llm_cache_invalidate,
    llm_cache_key,
    llm_cache_put,
    pdf_cache_clear,
    pdf_cache_get,
    pdf_cache_get_by_key,
    pdf_cache_key,
    pdf_cache_put,
    prompt_hash,
)
from .codebook import (
    CodebookEntry,
    get_codebook_entry,
    list_codebook_entries,
    reset_codebook_entry,
    upsert_codebook_entry,
)
from .codings import (
    CodeFrequency,
    CodingRecord,
    add_coded_segment,
    add_coding,
    add_codings_bulk,
    code_frequencies,
    codings_by_document,
    count_codings,
    delete_codings_for_run,
    get_coding,
    list_codings,
    unique_codes_for_project,
)
from .migrate import MigrationReport, migrate_legacy_output
from .projects import (
    VALID_RUN_STATUSES,
    Project,
    Run,
    RunFacet,
    RunMaterial,
    add_run_facet,
    add_run_material,
    create_project,
    create_run,
    delete_project,
    delete_run,
    get_latest_run,
    get_project,
    get_project_by_name,
    get_run,
    list_projects,
    list_run_facets,
    list_run_materials,
    list_runs,
    update_project,
    update_run_status,
)

__all__ = [
    # foundation
    "AppDB", "open_app_db", "default_app_db_path",
    # projects
    "Project", "Run", "RunMaterial", "RunFacet", "VALID_RUN_STATUSES",
    "create_project", "get_project", "get_project_by_name", "list_projects",
    "update_project", "delete_project",
    "create_run", "get_run", "list_runs", "update_run_status",
    "get_latest_run", "delete_run",
    "add_run_material", "list_run_materials",
    "add_run_facet", "list_run_facets",
    # caches
    "LLMCacheEntry", "PDFCacheEntry",
    "llm_cache_key", "prompt_hash", "pdf_cache_key",
    "llm_cache_get", "llm_cache_put", "llm_cache_invalidate", "llm_cache_clear",
    "pdf_cache_get", "pdf_cache_get_by_key", "pdf_cache_put", "pdf_cache_clear",
    # codings
    "CodingRecord", "CodeFrequency",
    "add_coding", "add_coded_segment", "add_codings_bulk",
    "get_coding", "list_codings", "count_codings", "delete_codings_for_run",
    "code_frequencies", "codings_by_document", "unique_codes_for_project",
    # codebook overrides
    "CodebookEntry",
    "upsert_codebook_entry", "get_codebook_entry",
    "list_codebook_entries", "reset_codebook_entry",
    # migrate
    "MigrationReport", "migrate_legacy_output",
]
