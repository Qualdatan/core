"""Persistente Triangulations-DB (Phase 6).

Liest aus den ``pipeline.db`` der einzelnen Runs (``RunContext.db``) und
schreibt projekt-zentrierte Daten in eine zentrale SQLite-DB an
``TRIANGULATION_DB``. Idempotent: ein Projekt existiert nur einmal pro
``(company, folder_name)``.

Die Triangulator-DB liegt ausserhalb der Run-Verzeichnisse (Default:
``<OUTPUT_ROOT>/_triangulation.db``) und akkumuliert so Informationen
ueber alle Runs hinweg, damit Cross-Run-Vergleiche moeglich werden.

Wichtig: dieses Modul halluziniert nichts. Es schreibt nur, was in den
pipeline.db-Dateien der Runs tatsaechlich enthalten ist. Fuer alte Runs
ohne Phase-2-Tabellen (``companies``/``projects``) gibt es einen Legacy-
Fallback: die ``pdf_documents.project``-Spalte wird als Folder-Name unter
einer Pseudo-Company ``__legacy__`` verwendet.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ..config import OUTPUT_ROOT, TRIANGULATION_DB


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  source_dir TEXT,
  first_seen TEXT,
  last_seen TEXT
);

CREATE TABLE IF NOT EXISTS projects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_id INTEGER NOT NULL REFERENCES companies(id),
  folder_name TEXT NOT NULL,
  code TEXT,
  name TEXT,
  source_dir TEXT,
  last_run_id INTEGER,
  UNIQUE(company_id, folder_name)
);

CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_dir TEXT UNIQUE NOT NULL,
  mode TEXT,
  started_at TEXT,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS project_facts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  key TEXT NOT NULL,
  value TEXT,
  source_type TEXT,
  source_doc TEXT,
  source_page INTEGER,
  evidence_quote TEXT,
  confidence REAL,
  run_id INTEGER REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS project_lph (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  lph INTEGER NOT NULL,
  leistungen_json TEXT,
  arbeitsweise TEXT,
  UNIQUE(project_id, lph)
);

CREATE TABLE IF NOT EXISTS project_awf (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  awf_code TEXT NOT NULL,
  awf_name TEXT,
  kategorie TEXT,
  evidence_count INTEGER DEFAULT 0,
  UNIQUE(project_id, awf_code)
);

CREATE TABLE IF NOT EXISTS project_elements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  awf_code TEXT,
  ifc_class TEXT,
  log_value TEXT,
  loi_attributes_json TEXT,
  evidence_doc TEXT,
  evidence_page INTEGER,
  evidence_quote TEXT
);

CREATE TABLE IF NOT EXISTS project_interfaces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL REFERENCES projects(id),
  from_role TEXT,
  to_role TEXT,
  information TEXT,
  lph INTEGER,
  evidence_doc TEXT
);

CREATE INDEX IF NOT EXISTS idx_facts_project_key ON project_facts(project_id, key);
CREATE INDEX IF NOT EXISTS idx_awf_project ON project_awf(project_id);
CREATE INDEX IF NOT EXISTS idx_elements_project ON project_elements(project_id);
"""


LEGACY_COMPANY_NAME = "__legacy__"


# ---------------------------------------------------------------------------
# DB-Wrapper
# ---------------------------------------------------------------------------

class TriangulationDB:
    """Persistente Triangulations-DB.

    Verwaltung via Context-Manager empfohlen::

        with TriangulationDB() as db:
            db.upsert_company("HKS")
    """

    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else TRIANGULATION_DB
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]

    def __enter__(self) -> "TriangulationDB":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Upserts
    # ------------------------------------------------------------------

    def upsert_company(self, name: str, source_dir: str | None = None) -> int:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cur = self.conn.execute(
            "SELECT id FROM companies WHERE name = ?", (name,),
        )
        row = cur.fetchone()
        if row is None:
            cur = self.conn.execute(
                """
                INSERT INTO companies (name, source_dir, first_seen, last_seen)
                VALUES (?, ?, ?, ?)
                """,
                (name, source_dir, now, now),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        # Update last_seen + optional source_dir
        company_id = int(row["id"])
        if source_dir:
            self.conn.execute(
                "UPDATE companies SET last_seen = ?, source_dir = COALESCE(?, source_dir) WHERE id = ?",
                (now, source_dir, company_id),
            )
        else:
            self.conn.execute(
                "UPDATE companies SET last_seen = ? WHERE id = ?",
                (now, company_id),
            )
        self.conn.commit()
        return company_id

    def upsert_project(
        self,
        company_id: int,
        folder_name: str,
        code: str | None = None,
        name: str | None = None,
        source_dir: str | None = None,
        last_run_id: int | None = None,
    ) -> int:
        cur = self.conn.execute(
            "SELECT id FROM projects WHERE company_id = ? AND folder_name = ?",
            (company_id, folder_name),
        )
        row = cur.fetchone()
        if row is None:
            cur = self.conn.execute(
                """
                INSERT INTO projects
                    (company_id, folder_name, code, name, source_dir, last_run_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (company_id, folder_name, code, name, source_dir, last_run_id),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        project_id = int(row["id"])
        # Update non-null fields (keep existing values if new is None)
        self.conn.execute(
            """
            UPDATE projects SET
                code        = COALESCE(?, code),
                name        = COALESCE(?, name),
                source_dir  = COALESCE(?, source_dir),
                last_run_id = COALESCE(?, last_run_id)
            WHERE id = ?
            """,
            (code, name, source_dir, last_run_id, project_id),
        )
        self.conn.commit()
        return project_id

    def insert_run(
        self,
        run_dir: str,
        mode: str = "unknown",
        started_at: str | None = None,
        finished_at: str | None = None,
    ) -> int:
        """Insert a run entry. If a run with same ``run_dir`` exists, returns
        the existing id and updates ``finished_at``/``mode`` if provided."""
        cur = self.conn.execute("SELECT id FROM runs WHERE run_dir = ?", (run_dir,))
        row = cur.fetchone()
        if row is None:
            cur = self.conn.execute(
                """
                INSERT INTO runs (run_dir, mode, started_at, finished_at)
                VALUES (?, ?, ?, ?)
                """,
                (run_dir, mode, started_at, finished_at),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        run_id = int(row["id"])
        self.conn.execute(
            """
            UPDATE runs SET
                mode = COALESCE(?, mode),
                started_at = COALESCE(?, started_at),
                finished_at = COALESCE(?, finished_at)
            WHERE id = ?
            """,
            (mode, started_at, finished_at, run_id),
        )
        self.conn.commit()
        return run_id

    def add_fact(
        self,
        project_id: int,
        key: str,
        value: str,
        *,
        source_type: str | None = None,
        source_doc: str | None = None,
        source_page: int | None = None,
        evidence_quote: str | None = None,
        confidence: float | None = None,
        run_id: int | None = None,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO project_facts
                (project_id, key, value, source_type, source_doc, source_page,
                 evidence_quote, confidence, run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id, key, value, source_type, source_doc, source_page,
                evidence_quote, confidence, run_id,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def upsert_awf(
        self,
        project_id: int,
        awf_code: str,
        awf_name: str | None = None,
        kategorie: str | None = None,
        evidence_count: int = 0,
    ) -> int:
        cur = self.conn.execute(
            "SELECT id, evidence_count FROM project_awf WHERE project_id = ? AND awf_code = ?",
            (project_id, awf_code),
        )
        row = cur.fetchone()
        if row is None:
            cur = self.conn.execute(
                """
                INSERT INTO project_awf
                    (project_id, awf_code, awf_name, kategorie, evidence_count)
                VALUES (?, ?, ?, ?, ?)
                """,
                (project_id, awf_code, awf_name, kategorie, evidence_count),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        awf_id = int(row["id"])
        new_count = int(row["evidence_count"] or 0) + int(evidence_count)
        self.conn.execute(
            """
            UPDATE project_awf SET
                awf_name = COALESCE(?, awf_name),
                kategorie = COALESCE(?, kategorie),
                evidence_count = ?
            WHERE id = ?
            """,
            (awf_name, kategorie, new_count, awf_id),
        )
        self.conn.commit()
        return awf_id

    def upsert_lph(
        self,
        project_id: int,
        lph: int,
        leistungen_json: str | None = None,
        arbeitsweise: str | None = None,
    ) -> int:
        cur = self.conn.execute(
            "SELECT id FROM project_lph WHERE project_id = ? AND lph = ?",
            (project_id, lph),
        )
        row = cur.fetchone()
        if row is None:
            cur = self.conn.execute(
                """
                INSERT INTO project_lph (project_id, lph, leistungen_json, arbeitsweise)
                VALUES (?, ?, ?, ?)
                """,
                (project_id, lph, leistungen_json, arbeitsweise),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        lph_id = int(row["id"])
        self.conn.execute(
            """
            UPDATE project_lph SET
                leistungen_json = COALESCE(?, leistungen_json),
                arbeitsweise = COALESCE(?, arbeitsweise)
            WHERE id = ?
            """,
            (leistungen_json, arbeitsweise, lph_id),
        )
        self.conn.commit()
        return lph_id

    def add_element(
        self,
        project_id: int,
        awf_code: str | None = None,
        ifc_class: str | None = None,
        log_value: str | None = None,
        loi_attributes_json: str | None = None,
        evidence_doc: str | None = None,
        evidence_page: int | None = None,
        evidence_quote: str | None = None,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO project_elements
                (project_id, awf_code, ifc_class, log_value, loi_attributes_json,
                 evidence_doc, evidence_page, evidence_quote)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id, awf_code, ifc_class, log_value, loi_attributes_json,
                evidence_doc, evidence_page, evidence_quote,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def add_interface(
        self,
        project_id: int,
        from_role: str | None = None,
        to_role: str | None = None,
        information: str | None = None,
        lph: int | None = None,
        evidence_doc: str | None = None,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO project_interfaces
                (project_id, from_role, to_role, information, lph, evidence_doc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (project_id, from_role, to_role, information, lph, evidence_doc),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def list_companies(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM companies ORDER BY name"
        ).fetchall()
        return [dict(r) for r in rows]

    def list_projects(self, company: str | None = None) -> list[dict]:
        if company is None:
            rows = self.conn.execute(
                """
                SELECT p.*, c.name AS company_name
                FROM projects p
                JOIN companies c ON p.company_id = c.id
                ORDER BY c.name, p.folder_name
                """,
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT p.*, c.name AS company_name
                FROM projects p
                JOIN companies c ON p.company_id = c.id
                WHERE c.name = ?
                ORDER BY p.folder_name
                """,
                (company,),
            ).fetchall()
        return [dict(r) for r in rows]

    def project_overview(self, project_id: int) -> dict:
        row = self.conn.execute(
            """
            SELECT p.*, c.name AS company_name
            FROM projects p
            JOIN companies c ON p.company_id = c.id
            WHERE p.id = ?
            """,
            (project_id,),
        ).fetchone()
        if row is None:
            return {}
        result: dict[str, Any] = dict(row)
        result["facts"] = [
            dict(r) for r in self.conn.execute(
                "SELECT * FROM project_facts WHERE project_id = ? ORDER BY key",
                (project_id,),
            ).fetchall()
        ]
        result["awf"] = [
            dict(r) for r in self.conn.execute(
                "SELECT * FROM project_awf WHERE project_id = ? ORDER BY awf_code",
                (project_id,),
            ).fetchall()
        ]
        result["elements"] = [
            dict(r) for r in self.conn.execute(
                "SELECT * FROM project_elements WHERE project_id = ? ORDER BY id",
                (project_id,),
            ).fetchall()
        ]
        result["lph"] = [
            dict(r) for r in self.conn.execute(
                "SELECT * FROM project_lph WHERE project_id = ? ORDER BY lph",
                (project_id,),
            ).fetchall()
        ]
        result["interfaces"] = [
            dict(r) for r in self.conn.execute(
                "SELECT * FROM project_interfaces WHERE project_id = ? ORDER BY id",
                (project_id,),
            ).fetchall()
        ]
        return result


def open_triangulation_db(path: Path | None = None) -> TriangulationDB:
    """Convenience-Factory. Oeffnet die Triangulations-DB an ``path`` oder am
    konfigurierten Default (``TRIANGULATION_DB``)."""
    return TriangulationDB(path)


# ---------------------------------------------------------------------------
# Reader: update_from_run
# ---------------------------------------------------------------------------

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _open_pipeline_db_readonly(db_path: Path) -> sqlite3.Connection:
    """Oeffnet eine pipeline.db read-only via URI."""
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _extract_awf_from_codings(
    src: sqlite3.Connection,
    pdf_ids: Iterable[int] | None = None,
) -> dict[str, int]:
    """Extrahiert AwF-Codes aus codings+coding_codes.

    Ein AwF-Code wird erkannt an einem Prefix 'C-' (Kategorie C fuer
    Anwendungsfaelle, hardcoded im ersten Wurf — spaeter konfigurierbar).

    Returns: dict awf_code -> evidence_count
    """
    params: tuple = ()
    where = ""
    if pdf_ids is not None:
        ids = list(pdf_ids)
        if not ids:
            return {}
        placeholders = ",".join(["?"] * len(ids))
        where = f" AND c.pdf_id IN ({placeholders})"
        params = tuple(ids)
    sql = f"""
        SELECT cc.code_id AS code_id, COUNT(*) AS cnt
        FROM coding_codes cc
        JOIN codings c ON c.id = cc.coding_id
        WHERE cc.code_id LIKE 'C-%'{where}
        GROUP BY cc.code_id
    """
    try:
        rows = src.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {r["code_id"]: int(r["cnt"]) for r in rows}


def _extract_elements_from_visual_detail(
    src: sqlite3.Connection,
    pdf_ids: Iterable[int] | None = None,
) -> list[dict]:
    """Parst visual_detail.elements_json und liefert flache dicts."""
    params: tuple = ()
    where = ""
    if pdf_ids is not None:
        ids = list(pdf_ids)
        if not ids:
            return []
        placeholders = ",".join(["?"] * len(ids))
        where = f" WHERE vd.pdf_id IN ({placeholders})"
        params = tuple(ids)
    sql = f"""
        SELECT vd.pdf_id, vd.page, vd.elements_json,
               d.filename, d.relative_path
        FROM visual_detail vd
        LEFT JOIN pdf_documents d ON d.id = vd.pdf_id
        {where}
    """
    try:
        rows = src.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        return []
    out: list[dict] = []
    for r in rows:
        raw = r["elements_json"] or "[]"
        try:
            elements = json.loads(raw)
        except (ValueError, TypeError):
            continue
        if not isinstance(elements, list):
            continue
        doc_name = r["filename"] or r["relative_path"] or ""
        for elem in elements:
            if not isinstance(elem, dict):
                continue
            out.append({
                "pdf_id": r["pdf_id"],
                "page": r["page"],
                "doc": doc_name,
                "ifc_class": elem.get("ifc_class"),
                "log_value": elem.get("log_achieved") or elem.get("log_value") or elem.get("log"),
                "awf_code": elem.get("awf_code"),
                "loi_attributes": elem.get("loi_attributes") or elem.get("loi"),
                "evidence_quote": elem.get("description") or elem.get("evidence"),
            })
    return out


def update_from_run(
    run_dir: Path,
    db_path: Path | None = None,
    mode: str = "unknown",
) -> dict:
    """Liest pipeline.db des angegebenen Runs und updated die Triangulations-DB.

    Liest aus pipeline.db:
    - companies, projects (Phase-2-Tabellen; optional)
    - pdf_documents (mit company_id/project_id falls vorhanden)
    - codings + coding_codes → project_awf (Codes mit Prefix 'C-')
    - visual_detail → project_elements

    Schreibt in der Triangulations-DB:
    - companies (upsert, touch last_seen)
    - projects (upsert per company+folder_name)
    - runs (insert)
    - project_awf
    - project_elements

    Legacy-Fallback: Wenn die pipeline.db keine ``companies``/``projects``-
    Tabellen hat (Run vor Phase 2), wird jede ``pdf_documents.project`` als
    eigener Projekt-Folder unter Pseudo-Company ``__legacy__`` angelegt.

    Returns: stats dict mit gezaehlten Inserts.
    """
    run_dir = Path(run_dir)
    pipeline_db = run_dir / "pipeline.db"
    if not pipeline_db.exists():
        raise FileNotFoundError(f"pipeline.db nicht gefunden: {pipeline_db}")

    stats = {
        "run_dir": str(run_dir),
        "companies": 0,
        "projects": 0,
        "awf": 0,
        "elements": 0,
        "legacy_mode": False,
    }

    src = _open_pipeline_db_readonly(pipeline_db)
    try:
        has_companies = _table_exists(src, "companies")
        has_projects = _table_exists(src, "projects")
        has_pdf = _table_exists(src, "pdf_documents")
        use_legacy = not (has_companies and has_projects)
        stats["legacy_mode"] = use_legacy

        with TriangulationDB(db_path) as tri:
            started = datetime.now(timezone.utc).isoformat(timespec="seconds")
            run_id = tri.insert_run(
                run_dir=str(run_dir),
                mode=mode,
                started_at=started,
                finished_at=started,
            )

            # Map src_pdf_id -> (project_id, doc_name)
            pdf_to_project: dict[int, tuple[int, str]] = {}

            if not use_legacy and has_pdf:
                # Phase-2-Pfad: companies + projects direkt lesen
                comp_rows = src.execute(
                    "SELECT id, name, source_dir FROM companies"
                ).fetchall()
                src_company_to_tri: dict[int, int] = {}
                for cr in comp_rows:
                    tri_cid = tri.upsert_company(cr["name"], cr["source_dir"])
                    src_company_to_tri[int(cr["id"])] = tri_cid
                    stats["companies"] += 1

                proj_rows = src.execute(
                    """
                    SELECT id, company_id, folder_name, code, name, source_dir
                    FROM projects
                    """,
                ).fetchall()
                src_project_to_tri: dict[int, int] = {}
                for pr in proj_rows:
                    tri_cid = src_company_to_tri.get(int(pr["company_id"]))
                    if tri_cid is None:
                        continue
                    tri_pid = tri.upsert_project(
                        company_id=tri_cid,
                        folder_name=pr["folder_name"],
                        code=pr["code"],
                        name=pr["name"],
                        source_dir=pr["source_dir"],
                        last_run_id=run_id,
                    )
                    src_project_to_tri[int(pr["id"])] = tri_pid
                    stats["projects"] += 1

                # pdf_documents with project_id linkage
                try:
                    pdf_rows = src.execute(
                        """
                        SELECT id, project, filename, project_id
                        FROM pdf_documents
                        """,
                    ).fetchall()
                except sqlite3.OperationalError:
                    pdf_rows = []
                for pr in pdf_rows:
                    src_pid = pr["project_id"]
                    if src_pid is None:
                        continue
                    tri_pid = src_project_to_tri.get(int(src_pid))
                    if tri_pid is None:
                        continue
                    pdf_to_project[int(pr["id"])] = (tri_pid, pr["filename"] or "")
            elif has_pdf:
                # Legacy-Pfad: Pseudo-Company __legacy__, Projekt pro distinct project-String
                legacy_cid = tri.upsert_company(LEGACY_COMPANY_NAME, str(run_dir))
                stats["companies"] += 1
                try:
                    pdf_rows = src.execute(
                        "SELECT id, project, filename FROM pdf_documents"
                    ).fetchall()
                except sqlite3.OperationalError:
                    pdf_rows = []
                folder_to_tri_pid: dict[str, int] = {}
                for pr in pdf_rows:
                    folder = pr["project"] or "(unknown)"
                    if folder not in folder_to_tri_pid:
                        tri_pid = tri.upsert_project(
                            company_id=legacy_cid,
                            folder_name=folder,
                            code=None,
                            name=folder,
                            source_dir=None,
                            last_run_id=run_id,
                        )
                        folder_to_tri_pid[folder] = tri_pid
                        stats["projects"] += 1
                    pdf_to_project[int(pr["id"])] = (
                        folder_to_tri_pid[folder], pr["filename"] or "",
                    )

            # --- AwF-Extraction per Projekt ---
            # Gruppiere pdf_ids pro tri_project_id
            project_pdfs: dict[int, list[int]] = {}
            for src_pid, (tri_pid, _name) in pdf_to_project.items():
                project_pdfs.setdefault(tri_pid, []).append(src_pid)

            for tri_pid, pdfs in project_pdfs.items():
                awf_map = _extract_awf_from_codings(src, pdfs)
                for awf_code, count in awf_map.items():
                    tri.upsert_awf(
                        project_id=tri_pid,
                        awf_code=awf_code,
                        awf_name=None,
                        kategorie="C",
                        evidence_count=count,
                    )
                    stats["awf"] += 1

                # --- Element-Extraction ---
                elements = _extract_elements_from_visual_detail(src, pdfs)
                for elem in elements:
                    loi = elem.get("loi_attributes")
                    loi_json = (
                        json.dumps(loi, ensure_ascii=False)
                        if loi is not None else None
                    )
                    tri.add_element(
                        project_id=tri_pid,
                        awf_code=elem.get("awf_code"),
                        ifc_class=elem.get("ifc_class"),
                        log_value=elem.get("log_value"),
                        loi_attributes_json=loi_json,
                        evidence_doc=elem.get("doc"),
                        evidence_page=elem.get("page"),
                        evidence_quote=elem.get("evidence_quote"),
                    )
                    stats["elements"] += 1
    finally:
        src.close()

    return stats


# ---------------------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------------------

def list_run_dirs(output_root: Path | None = None) -> list[Path]:
    """Listet alle Run-Verzeichnisse unter ``output_root`` die eine
    ``pipeline.db`` enthalten. Default: ``OUTPUT_ROOT``."""
    root = Path(output_root) if output_root else OUTPUT_ROOT
    if not root.exists():
        return []
    result: list[Path] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        if child.name.startswith("_"):
            continue  # skip hidden/meta dirs
        if (child / "pipeline.db").exists():
            result.append(child)
    return result


def rebuild_from_all_runs(
    db_path: Path | None = None,
    output_root: Path | None = None,
) -> dict:
    """Drop & rebuild: loescht alle Tabellen-Inhalte und re-importiert alle
    Runs aus ``output_root``. Returns aggregated stats.
    """
    target = Path(db_path) if db_path else TRIANGULATION_DB
    # Clear existing content (but keep schema).
    with TriangulationDB(target) as tri:
        for table in (
            "project_interfaces",
            "project_elements",
            "project_awf",
            "project_lph",
            "project_facts",
            "runs",
            "projects",
            "companies",
        ):
            tri.conn.execute(f"DELETE FROM {table}")
        tri.conn.execute(
            "DELETE FROM sqlite_sequence WHERE name IN "
            "('companies','projects','runs','project_facts','project_lph',"
            "'project_awf','project_elements','project_interfaces')"
        )
        tri.conn.commit()

    total = {
        "runs_imported": 0,
        "companies": 0,
        "projects": 0,
        "awf": 0,
        "elements": 0,
        "legacy_runs": 0,
    }
    for run_dir in list_run_dirs(output_root):
        try:
            stats = update_from_run(run_dir, db_path=target)
        except Exception:
            continue
        total["runs_imported"] += 1
        total["companies"] += stats.get("companies", 0)
        total["projects"] += stats.get("projects", 0)
        total["awf"] += stats.get("awf", 0)
        total["elements"] += stats.get("elements", 0)
        if stats.get("legacy_mode"):
            total["legacy_runs"] += 1
    return total
