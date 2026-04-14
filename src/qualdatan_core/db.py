"""Pipeline-Datenbank: SQLite-Backend fuer Cache, Status und Ergebnisse.

Ersetzt die vielen Einzel-JSON-Dateien durch eine einzige pipeline.db.
WAL-Modus fuer robuste parallele Zugriffe.

Prompts und Responses bleiben als Textdateien (Debugging).
"""

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
-- Pipeline-Runs und Status
CREATE TABLE IF NOT EXISTS run_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Companies (Phase 2: Company-Layout)
CREATE TABLE IF NOT EXISTS companies (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT UNIQUE NOT NULL,
    source_dir TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Projekte innerhalb einer Company (Phase 2)
CREATE TABLE IF NOT EXISTS projects (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id  INTEGER NOT NULL REFERENCES companies(id),
    folder_name TEXT NOT NULL,
    code        TEXT,
    name        TEXT,
    source_dir  TEXT,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, folder_name)
);

-- Interview-Dokumente (.docx in Interviews/) pro Company (Phase 2)
CREATE TABLE IF NOT EXISTS interview_documents (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    filename   TEXT NOT NULL,
    path       TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, filename)
);

-- PDF-Dokumente
CREATE TABLE IF NOT EXISTS pdf_documents (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    project       TEXT NOT NULL,
    filename      TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    path          TEXT NOT NULL,
    file_size_kb  INTEGER DEFAULT 0,
    page_count    INTEGER DEFAULT 0,
    document_type TEXT DEFAULT '',
    confidence    REAL DEFAULT 0.0,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Pipeline-Status pro PDF pro Schritt
CREATE TABLE IF NOT EXISTS pipeline_status (
    pdf_id      INTEGER NOT NULL REFERENCES pdf_documents(id),
    step        TEXT NOT NULL,  -- 'extraction', 'classification', 'coding', 'visual_triage', 'visual_detail', 'export'
    status      TEXT NOT NULL DEFAULT 'pending',  -- 'pending', 'running', 'done', 'error'
    started_at  TEXT,
    finished_at TEXT,
    error_msg   TEXT,
    PRIMARY KEY (pdf_id, step)
);

-- Extraktionen (die grossen JSON-Daten)
CREATE TABLE IF NOT EXISTS extractions (
    pdf_id   INTEGER PRIMARY KEY REFERENCES pdf_documents(id),
    data     TEXT NOT NULL,  -- JSON
    n_pages  INTEGER DEFAULT 0,
    n_blocks INTEGER DEFAULT 0
);

-- Seitenmetriken (aus Klassifikation)
CREATE TABLE IF NOT EXISTS page_metrics (
    pdf_id         INTEGER NOT NULL REFERENCES pdf_documents(id),
    page           INTEGER NOT NULL,
    text_coverage  REAL DEFAULT 0.0,
    image_coverage REAL DEFAULT 0.0,
    text_char_count INTEGER DEFAULT 0,
    drawing_count  INTEGER DEFAULT 0,
    aspect_ratio   REAL DEFAULT 1.0,
    is_landscape   INTEGER DEFAULT 0,
    page_format    TEXT DEFAULT '',
    page_width     REAL DEFAULT 0.0,
    page_height    REAL DEFAULT 0.0,
    PRIMARY KEY (pdf_id, page)
);

-- Klassifikation pro Seite
CREATE TABLE IF NOT EXISTS classifications (
    pdf_id        INTEGER NOT NULL REFERENCES pdf_documents(id),
    page          INTEGER NOT NULL,
    page_type     TEXT NOT NULL DEFAULT 'text',
    confidence    REAL DEFAULT 0.0,
    plan_subtype  TEXT DEFAULT '',
    has_title_block INTEGER DEFAULT 0,
    title_block_json TEXT DEFAULT '{}',
    PRIMARY KEY (pdf_id, page)
);

-- Kodierungen (Text + Visual gemeinsam)
CREATE TABLE IF NOT EXISTS codings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pdf_id      INTEGER NOT NULL REFERENCES pdf_documents(id),
    page        INTEGER NOT NULL,
    block_id    TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'text',  -- 'text', 'visual_triage', 'visual_detail'
    description TEXT DEFAULT '',
    ganzer_block INTEGER DEFAULT 1,
    begruendung TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS coding_codes (
    coding_id INTEGER NOT NULL REFERENCES codings(id),
    code_id   TEXT NOT NULL,
    PRIMARY KEY (coding_id, code_id)
);

-- Neue (induktiv vorgeschlagene) Codes
CREATE TABLE IF NOT EXISTS neue_codes (
    code_id          TEXT PRIMARY KEY,
    code_name        TEXT NOT NULL,
    hauptkategorie   TEXT NOT NULL,
    kodierdefinition TEXT DEFAULT '',
    pdf_id           INTEGER REFERENCES pdf_documents(id)
);

-- Visual Triage (Pass 1)
CREATE TABLE IF NOT EXISTS visual_triage (
    pdf_id             INTEGER NOT NULL REFERENCES pdf_documents(id),
    page               INTEGER NOT NULL,
    page_type          TEXT DEFAULT '',
    priority           TEXT DEFAULT 'skip',
    estimated_log      TEXT DEFAULT '',
    lph_evidence       TEXT DEFAULT '',
    confidence         REAL DEFAULT 0.0,
    description        TEXT DEFAULT '',
    building_elements  TEXT DEFAULT '[]',  -- JSON array
    PRIMARY KEY (pdf_id, page)
);

-- Visual Detail (Pass 2)
CREATE TABLE IF NOT EXISTS visual_detail (
    pdf_id           INTEGER NOT NULL REFERENCES pdf_documents(id),
    page             INTEGER NOT NULL,
    description      TEXT DEFAULT '',
    elements_json    TEXT DEFAULT '[]',  -- JSON array of ElementDetail
    annotations_json TEXT DEFAULT '[]',  -- JSON array
    cross_refs_json  TEXT DEFAULT '[]',  -- JSON array
    PRIMARY KEY (pdf_id, page)
);

-- Indizes fuer haeufige Abfragen
CREATE INDEX IF NOT EXISTS idx_pipeline_status_step ON pipeline_status(step, status);
CREATE INDEX IF NOT EXISTS idx_codings_pdf ON codings(pdf_id);
CREATE INDEX IF NOT EXISTS idx_coding_codes_code ON coding_codes(code_id);
CREATE INDEX IF NOT EXISTS idx_classifications_type ON classifications(page_type);
CREATE INDEX IF NOT EXISTS idx_visual_triage_priority ON visual_triage(priority);
"""


# ---------------------------------------------------------------------------
# Connection Pool (Thread-safe)
# ---------------------------------------------------------------------------


class PipelineDB:
    """SQLite-Datenbank fuer die Pipeline.

    Thread-safe: Jeder Thread bekommt seine eigene Connection.
    WAL-Modus fuer parallele Lese-/Schreibzugriffe.
    """

    def __init__(self, db_path: Path):
        """Oeffnet/erstellt die SQLite unter ``db_path`` und migriert das Schema.

        Args:
            db_path: Pfad zur DB-Datei; Parent-Verzeichnisse werden angelegt.
        """
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Gibt die Connection fuer den aktuellen Thread zurueck."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn

    def _init_db(self):
        """Erstellt das Schema."""
        conn = self._get_conn()
        conn.executescript(_SCHEMA)
        conn.commit()
        # Additive ALTERs (SQLite ADD COLUMN ist nicht idempotent)
        self._ensure_column(
            "pdf_documents",
            "project_id",
            "project_id INTEGER REFERENCES projects(id)",
        )
        self._ensure_column(
            "pdf_documents",
            "company_id",
            "company_id INTEGER REFERENCES companies(id)",
        )
        self._ensure_column(
            "pdf_documents",
            "source_kind",
            "source_kind TEXT",
        )
        conn.commit()

    def _ensure_column(self, table: str, col: str, ddl: str):
        """Fuegt eine Spalte hinzu, falls sie noch nicht existiert.

        SQLite unterstuetzt kein ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS``,
        deshalb muss man vorher PRAGMA table_info pruefen. Idempotent —
        mehrfaches _init_db ist safe.
        """
        conn = self._get_conn()
        cur = conn.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cur.fetchall()}
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")

    @contextmanager
    def transaction(self):
        """Context-Manager fuer Transaktionen."""
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def close(self):
        """Schliesst die Connection des aktuellen Threads."""
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    # ------------------------------------------------------------------
    # Run State (ersetzt run_state.json)
    # ------------------------------------------------------------------

    def set_state(self, key: str, value):
        """Setzt einen State-Wert."""
        val_json = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO run_state (key, value) VALUES (?, ?)",
                (key, val_json),
            )

    def get_state(self, key: str, default=None):
        """Liest einen State-Wert."""
        conn = self._get_conn()
        row = conn.execute("SELECT value FROM run_state WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except (json.JSONDecodeError, TypeError):
            return row["value"]

    def get_all_state(self) -> dict:
        """Liest alle State-Werte."""
        conn = self._get_conn()
        rows = conn.execute("SELECT key, value FROM run_state").fetchall()
        result = {}
        for row in rows:
            try:
                result[row["key"]] = json.loads(row["value"])
            except (json.JSONDecodeError, TypeError):
                result[row["key"]] = row["value"]
        return result

    # ------------------------------------------------------------------
    # Companies / Projects / Interview Documents (Phase 2)
    # ------------------------------------------------------------------

    def upsert_company(self, name: str, source_dir: str = "") -> int:
        """Insert-or-get fuer eine Company. Gibt die company.id zurueck.

        `source_dir` wird beim ersten Insert gesetzt und bei folgenden
        Aufrufen NICHT ueberschrieben — damit bleibt die Herkunft stabil
        ueber Runs hinweg.
        """
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO companies (name, source_dir) VALUES (?, ?)",
                (name, source_dir),
            )
            row = conn.execute("SELECT id FROM companies WHERE name = ?", (name,)).fetchone()
            return row["id"]

    def upsert_project(
        self, company_id: int, folder_name: str, code: str | None, name: str, source_dir: str = ""
    ) -> int:
        """Insert-or-get fuer ein Projekt innerhalb einer Company.

        Unique-Key ist ``(company_id, folder_name)`` — d.h. ein
        umbenannter Ordner zaehlt als neues Projekt.
        """
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO projects
                    (company_id, folder_name, code, name, source_dir)
                VALUES (?, ?, ?, ?, ?)
                """,
                (company_id, folder_name, code, name, source_dir),
            )
            row = conn.execute(
                "SELECT id FROM projects WHERE company_id = ? AND folder_name = ?",
                (company_id, folder_name),
            ).fetchone()
            return row["id"]

    def upsert_interview_doc(self, company_id: int, filename: str, path: str) -> int:
        """Insert-or-get fuer ein Interview-Dokument.

        Unique-Key ist ``(company_id, filename)`` — mehrere Interviews
        mit gleichem Dateinamen pro Company sind nicht erlaubt (waere
        sowieso verwirrend).
        """
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO interview_documents
                    (company_id, filename, path)
                VALUES (?, ?, ?)
                """,
                (company_id, filename, path),
            )
            row = conn.execute(
                "SELECT id FROM interview_documents WHERE company_id = ? AND filename = ?",
                (company_id, filename),
            ).fetchone()
            return row["id"]

    def list_companies_in_db(self) -> list[dict]:
        """Gibt alle Companies mit ihrer Projektzahl zurueck."""
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT c.id, c.name, c.source_dir, c.created_at,
                   COUNT(p.id) AS project_count
            FROM companies c
            LEFT JOIN projects p ON p.company_id = c.id
            GROUP BY c.id, c.name, c.source_dir, c.created_at
            ORDER BY c.name
            """
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # PDF Documents
    # ------------------------------------------------------------------

    def upsert_pdf(
        self,
        project: str,
        filename: str,
        relative_path: str,
        path: str,
        file_size_kb: int = 0,
        page_count: int = 0,
    ) -> int:
        """Fuegt ein PDF ein oder aktualisiert es. Gibt die ID zurueck."""
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO pdf_documents (project, filename, relative_path, path, file_size_kb, page_count)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(relative_path) DO UPDATE SET
                    path = excluded.path,
                    file_size_kb = excluded.file_size_kb,
                    page_count = CASE WHEN excluded.page_count > 0 THEN excluded.page_count ELSE pdf_documents.page_count END
            """,
                (project, filename, relative_path, path, file_size_kb, page_count),
            )
            row = conn.execute(
                "SELECT id FROM pdf_documents WHERE relative_path = ?",
                (relative_path,),
            ).fetchone()
            return row["id"]

    def get_pdf_id(self, relative_path: str) -> int | None:
        """Gibt die PDF-ID zurueck oder None."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT id FROM pdf_documents WHERE relative_path = ?",
            (relative_path,),
        ).fetchone()
        return row["id"] if row else None

    def get_pdf(self, pdf_id: int) -> dict | None:
        """Gibt alle Felder eines PDFs zurueck."""
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM pdf_documents WHERE id = ?", (pdf_id,)).fetchone()
        return dict(row) if row else None

    def get_all_pdfs(self) -> list[dict]:
        """Gibt alle PDFs zurueck."""
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM pdf_documents ORDER BY project, filename").fetchall()
        return [dict(r) for r in rows]

    def update_pdf_classification(self, pdf_id: int, document_type: str, confidence: float):
        """Aktualisiert Dokumenttyp und Confidence."""
        with self.transaction() as conn:
            conn.execute(
                "UPDATE pdf_documents SET document_type = ?, confidence = ? WHERE id = ?",
                (document_type, confidence, pdf_id),
            )

    # ------------------------------------------------------------------
    # Pipeline Status
    # ------------------------------------------------------------------

    def set_step_status(self, pdf_id: int, step: str, status: str, error_msg: str = ""):
        """Setzt den Status eines Pipeline-Schritts."""
        now = datetime.now().isoformat()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_status (pdf_id, step, status, started_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(pdf_id, step) DO UPDATE SET
                    status = excluded.status,
                    started_at = CASE
                        WHEN excluded.status = 'running' THEN excluded.started_at
                        ELSE pipeline_status.started_at
                    END,
                    finished_at = CASE
                        WHEN excluded.status IN ('done', 'error') THEN ?
                        ELSE NULL
                    END,
                    error_msg = ?
            """,
                (pdf_id, step, status, now, now, error_msg),
            )

    def is_step_done(self, pdf_id: int, step: str) -> bool:
        """Prueft ob ein Schritt abgeschlossen ist."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT status FROM pipeline_status WHERE pdf_id = ? AND step = ?",
            (pdf_id, step),
        ).fetchone()
        return row is not None and row["status"] == "done"

    def get_pending_pdfs(self, step: str) -> list[int]:
        """Gibt PDF-IDs zurueck die einen bestimmten Schritt noch nicht haben."""
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT d.id FROM pdf_documents d
            LEFT JOIN pipeline_status ps ON d.id = ps.pdf_id AND ps.step = ?
            WHERE ps.status IS NULL OR ps.status NOT IN ('done')
            ORDER BY d.id
        """,
            (step,),
        ).fetchall()
        return [r["id"] for r in rows]

    def get_step_summary(self) -> dict:
        """Gibt eine Zusammenfassung aller Schritte zurueck."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT step, status, COUNT(*) as cnt
            FROM pipeline_status
            GROUP BY step, status
            ORDER BY step, status
        """).fetchall()
        summary = {}
        for r in rows:
            summary.setdefault(r["step"], {})[r["status"]] = r["cnt"]
        return summary

    # ------------------------------------------------------------------
    # Extractions
    # ------------------------------------------------------------------

    def save_extraction(self, pdf_id: int, data: dict):
        """Speichert Extraktionsdaten."""
        n_pages = len(data.get("pages", []))
        n_blocks = sum(len(p.get("blocks", [])) for p in data.get("pages", []))
        data_json = json.dumps(data, ensure_ascii=False)
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO extractions (pdf_id, data, n_pages, n_blocks)
                VALUES (?, ?, ?, ?)
            """,
                (pdf_id, data_json, n_pages, n_blocks),
            )

    def load_extraction(self, pdf_id: int) -> dict | None:
        """Laedt Extraktionsdaten."""
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM extractions WHERE pdf_id = ?", (pdf_id,)).fetchone()
        if row:
            return json.loads(row["data"])
        return None

    def has_extraction(self, pdf_id: int) -> bool:
        """``True`` wenn fuer ``pdf_id`` bereits eine Extraction-Zeile existiert."""
        conn = self._get_conn()
        row = conn.execute("SELECT 1 FROM extractions WHERE pdf_id = ?", (pdf_id,)).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # Page Metrics + Classifications
    # ------------------------------------------------------------------

    def save_page_metrics(self, pdf_id: int, page: int, metrics: dict):
        """Speichert Seitenmetriken."""
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO page_metrics
                    (pdf_id, page, text_coverage, image_coverage, text_char_count,
                     drawing_count, aspect_ratio, is_landscape, page_format, page_width, page_height)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    pdf_id,
                    page,
                    metrics.get("text_coverage", 0.0),
                    metrics.get("image_coverage", 0.0),
                    metrics.get("text_char_count", 0),
                    metrics.get("drawing_count", 0),
                    metrics.get("aspect_ratio", 1.0),
                    1 if metrics.get("is_landscape") else 0,
                    metrics.get("page_format", ""),
                    metrics.get("page_width", 0.0),
                    metrics.get("page_height", 0.0),
                ),
            )

    def save_classification(
        self,
        pdf_id: int,
        page: int,
        page_type: str,
        confidence: float,
        plan_subtype: str = "",
        has_title_block: bool = False,
        title_block: dict = None,
    ):
        """Speichert Seitenklassifikation."""
        tb_json = json.dumps(title_block or {}, ensure_ascii=False)
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO classifications
                    (pdf_id, page, page_type, confidence, plan_subtype,
                     has_title_block, title_block_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    pdf_id,
                    page,
                    page_type,
                    confidence,
                    plan_subtype,
                    1 if has_title_block else 0,
                    tb_json,
                ),
            )

    def get_classifications(self, pdf_id: int) -> list[dict]:
        """Laedt alle Klassifikationen fuer ein PDF."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM classifications WHERE pdf_id = ? ORDER BY page",
            (pdf_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Codings (Text + Visual)
    # ------------------------------------------------------------------

    def save_coding(
        self,
        pdf_id: int,
        page: int,
        block_id: str,
        codes: list[str],
        source: str = "text",
        description: str = "",
        ganzer_block: bool = True,
        begruendung: str = "",
    ) -> int:
        """Speichert eine Kodierung. Gibt die coding_id zurueck."""
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO codings (pdf_id, page, block_id, source, description, ganzer_block, begruendung)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    pdf_id,
                    page,
                    block_id,
                    source,
                    description,
                    1 if ganzer_block else 0,
                    begruendung,
                ),
            )
            coding_id = cursor.lastrowid
            for code_id in codes:
                conn.execute(
                    "INSERT OR IGNORE INTO coding_codes (coding_id, code_id) VALUES (?, ?)",
                    (coding_id, code_id),
                )
            return coding_id

    def save_neue_codes(self, neue_codes: list[dict], pdf_id: int = None):
        """Speichert neu vorgeschlagene Codes."""
        with self.transaction() as conn:
            for nc in neue_codes:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO neue_codes
                        (code_id, code_name, hauptkategorie, kodierdefinition, pdf_id)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        nc["code_id"],
                        nc["code_name"],
                        nc.get("hauptkategorie", ""),
                        nc.get("kodierdefinition", ""),
                        pdf_id,
                    ),
                )

    def get_codings_for_pdf(self, pdf_id: int) -> list[dict]:
        """Laedt alle Kodierungen fuer ein PDF."""
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT c.*, GROUP_CONCAT(cc.code_id) as code_ids
            FROM codings c
            JOIN coding_codes cc ON c.id = cc.coding_id
            WHERE c.pdf_id = ?
            GROUP BY c.id
            ORDER BY c.page, c.block_id
        """,
            (pdf_id,),
        ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            d["codes"] = d.pop("code_ids", "").split(",") if d.get("code_ids") else []
            results.append(d)
        return results

    def get_all_codings_by_code(self, code_id: str) -> list[dict]:
        """Findet alle Kodierungen fuer einen bestimmten Code (Toolkit-Export)."""
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT c.*, d.project, d.filename, d.relative_path
            FROM codings c
            JOIN coding_codes cc ON c.id = cc.coding_id
            JOIN pdf_documents d ON c.pdf_id = d.id
            WHERE cc.code_id = ?
            ORDER BY d.project, d.filename, c.page
        """,
            (code_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_coding_summary(self) -> dict:
        """Gibt Kodierungs-Statistik zurueck (fuer Toolkit-Export)."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT cc.code_id, COUNT(*) as count, c.source
            FROM coding_codes cc
            JOIN codings c ON cc.coding_id = c.id
            GROUP BY cc.code_id, c.source
            ORDER BY cc.code_id
        """).fetchall()
        summary = {}
        for r in rows:
            summary.setdefault(r["code_id"], {})[r["source"]] = r["count"]
        return summary

    # ------------------------------------------------------------------
    # Visual Triage + Detail
    # ------------------------------------------------------------------

    def save_visual_triage(self, pdf_id: int, page: int, triage: dict):
        """Speichert Triage-Ergebnis."""
        elements = json.dumps(triage.get("building_elements", []), ensure_ascii=False)
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO visual_triage
                    (pdf_id, page, page_type, priority, estimated_log, lph_evidence,
                     confidence, description, building_elements)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    pdf_id,
                    page,
                    triage.get("page_type", ""),
                    triage.get("priority", "skip"),
                    triage.get("estimated_log", ""),
                    triage.get("lph_evidence", ""),
                    triage.get("confidence", 0.0),
                    triage.get("description", ""),
                    elements,
                ),
            )

    def save_visual_detail(self, pdf_id: int, page: int, detail: dict):
        """Speichert Detail-Ergebnis.

        Die einzelnen Bauelemente in `building_elements` koennen ein
        zusaetzliches `bbox`-Feld [x0, y0, x1, y1] (normalisiert 0..1,
        Ursprung oben-links) enthalten, das aus Pass 3 (Localisation)
        stammt. Es wird als Teil der `elements_json`-JSON-Spalte
        gespeichert - keine Schemaaenderung noetig.
        """
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO visual_detail
                    (pdf_id, page, description, elements_json, annotations_json, cross_refs_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    pdf_id,
                    page,
                    detail.get("description", ""),
                    json.dumps(detail.get("building_elements", []), ensure_ascii=False),
                    json.dumps(detail.get("annotations", []), ensure_ascii=False),
                    json.dumps(detail.get("cross_references", []), ensure_ascii=False),
                ),
            )

    def get_visual_triage(self, pdf_id: int) -> list[dict]:
        """Laedt alle Triage-Ergebnisse fuer ein PDF."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM visual_triage WHERE pdf_id = ? ORDER BY page",
            (pdf_id,),
        ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            d["building_elements"] = json.loads(d.get("building_elements", "[]"))
            results.append(d)
        return results

    def get_visual_detail(self, pdf_id: int) -> list[dict]:
        """Laedt alle Detail-Ergebnisse fuer ein PDF."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM visual_detail WHERE pdf_id = ? ORDER BY page",
            (pdf_id,),
        ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            d["building_elements"] = json.loads(d.get("elements_json", "[]"))
            d["annotations"] = json.loads(d.get("annotations_json", "[]"))
            d["cross_references"] = json.loads(d.get("cross_refs_json", "[]"))
            results.append(d)
        return results

    def has_visual_triage(self, pdf_id: int) -> bool:
        """``True`` wenn Visual-Triage-Ergebnisse fuer ``pdf_id`` vorliegen."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT 1 FROM visual_triage WHERE pdf_id = ? LIMIT 1", (pdf_id,)
        ).fetchone()
        return row is not None

    def has_visual_detail(self, pdf_id: int) -> bool:
        """``True`` wenn Visual-Detail-Eintraege fuer ``pdf_id`` vorliegen."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT 1 FROM visual_detail WHERE pdf_id = ? LIMIT 1", (pdf_id,)
        ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # Aggregation (fuer Toolkit-Export / Phase 7)
    # ------------------------------------------------------------------

    def get_all_building_elements(self) -> list[dict]:
        """Gibt alle erkannten Bauelemente ueber alle PDFs zurueck."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT vd.pdf_id, d.project, d.filename, vd.page,
                   vd.elements_json, vd.description
            FROM visual_detail vd
            JOIN pdf_documents d ON vd.pdf_id = d.id
            ORDER BY d.project, d.filename, vd.page
        """).fetchall()
        results = []
        for r in rows:
            elements = json.loads(r["elements_json"])
            for elem in elements:
                elem["project"] = r["project"]
                elem["filename"] = r["filename"]
                elem["page"] = r["page"]
                results.append(elem)
        return results

    def get_documents_by_type(self, doc_type: str) -> list[dict]:
        """Gibt alle PDFs eines bestimmten Typs zurueck."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM pdf_documents WHERE document_type = ? ORDER BY project, filename",
            (doc_type,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_log_evidence_summary(self) -> list[dict]:
        """Aggregiert LOG-Evidenz ueber alle Projekte (fuer Toolkit)."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT vt.estimated_log, vt.page_type,
                   COUNT(*) as page_count,
                   GROUP_CONCAT(DISTINCT d.project) as projects
            FROM visual_triage vt
            JOIN pdf_documents d ON vt.pdf_id = d.id
            WHERE vt.estimated_log != ''
            GROUP BY vt.estimated_log, vt.page_type
            ORDER BY vt.estimated_log
        """).fetchall()
        return [dict(r) for r in rows]
