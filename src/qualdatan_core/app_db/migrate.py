# SPDX-License-Identifier: AGPL-3.0-only
"""Migration von Legacy-``output/run_*`` Verzeichnissen in die App-DB.

Pre-Phase-D wurden Runs in ``output/run_<timestamp>/pipeline.db`` abgelegt.
Ab Phase D zieht das globale App-DB-Schema (``projects``, ``runs``,
``run_materials``, ``codings``) diese Daten ein — dieses Modul stellt den
Scan + Import bereit.

Der Import ist bewusst defensiv:

- Fehlende oder defekte ``pipeline.db`` -> Warnung + Skip.
- Fehlende Tabellen/Spalten -> Warnung, aber andere Tabellen werden trotzdem
  migriert (Legacy-Schema variierte ueber Monate).
- Idempotent: Dank ``UNIQUE (project_id, run_dir)`` in ``runs`` und einem
  entsprechenden Pre-Check werden bereits importierte Runs uebersprungen.

Siehe :func:`migrate_legacy_output` fuer den Einstiegspunkt.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from . import AppDB


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
@dataclass
class MigrationReport:
    """Ergebnis eines Legacy-Imports.

    Attributes:
        run_dirs_scanned: Anzahl ``run_*``-Verzeichnisse, die inspiziert wurden.
        run_dirs_migrated: Runs, fuer die tatsaechlich Zeilen in die App-DB
            geschrieben wurden (bei ``dry_run`` bleibt 0).
        run_dirs_skipped: Runs ohne ``pipeline.db`` oder bereits importiert.
        projects_created: Neu in ``projects`` angelegte Eintraege.
        codings_imported: Summe ueber alle Runs.
        materials_imported: Summe ``pdf_text`` + ``transcript``.
        warnings: freie Textzeilen (defekte DBs, fehlende Tabellen, ...).
    """

    run_dirs_scanned: int = 0
    run_dirs_migrated: int = 0
    run_dirs_skipped: int = 0
    projects_created: int = 0
    codings_imported: int = 0
    materials_imported: int = 0
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_STATUS_MAP = {
    "COMPLETED": "completed",
    "RUNNING": "failed",  # wurde unterbrochen -> failed
}


def _normalize_status(raw: str | None) -> str:
    if raw is None:
        return "failed"
    return _STATUS_MAP.get(raw.strip().upper(), "failed")


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


_BLOCK_RANGE_RE = re.compile(r"(\d+)\s*[-:]\s*(\d+)")


def _parse_block_range(block_id: str | None) -> tuple[int | None, int | None]:
    """Versucht ``block_id`` als Char-Range zu deuten (``"123-456"``).

    Legacy hat block_id meist als ``"p1_b3"`` oder aehnlich gespeichert —
    in diesen Faellen gibt die Funktion ``(None, None)`` zurueck.
    """
    if not block_id:
        return (None, None)
    m = _BLOCK_RANGE_RE.fullmatch(block_id.strip())
    if not m:
        return (None, None)
    a, b = int(m.group(1)), int(m.group(2))
    if a > b:
        a, b = b, a
    return (a, b)


def _get_run_state(conn: sqlite3.Connection) -> dict[str, str]:
    """Liest ``run_state`` als Flat-Dict (Keys + Values sind Strings)."""
    if not _table_exists(conn, "run_state"):
        return {}
    rows = conn.execute("SELECT key, value FROM run_state").fetchall()
    out: dict[str, str] = {}
    for row in rows:
        k, v = row[0], row[1]
        if isinstance(v, str) and v and v[0] in '"{[':
            try:
                decoded = json.loads(v)
                v = decoded if isinstance(decoded, str) else v
            except Exception:
                # Rohwert war zwar JSON-artig, aber nicht decodierbar —
                # dann den Legacy-String 1:1 uebernehmen.
                pass
        out[k] = v if isinstance(v, str) else json.dumps(v)
    return out


def _find_run_dirs(output_root: Path) -> list[Path]:
    """Gibt alle ``run_*``-Verzeichnisse unter ``output_root`` zurueck.

    Wenn ``output_root`` selbst ein ``run_*``-Verzeichnis ist, wird es als
    einziger Kandidat zurueckgegeben.
    """
    if not output_root.exists() or not output_root.is_dir():
        return []
    name = output_root.name
    if name.startswith("run_") and (output_root / "pipeline.db").exists():
        return [output_root]
    results: list[Path] = []
    for child in sorted(output_root.iterdir()):
        if not child.is_dir():
            continue
        if child.is_symlink():
            # "latest" Symlink o.ae. ueberspringen — das Original kommt
            # ueber den eigentlichen Dir-Walk.
            continue
        if child.name.startswith("run_"):
            results.append(child)
    return results


def _connect_legacy_ro(path: Path) -> sqlite3.Connection:
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_project(app_conn: sqlite3.Connection, name: str, description: str) -> tuple[int, bool]:
    """Insert-or-get. Gibt ``(project_id, created)`` zurueck."""
    row = app_conn.execute("SELECT id FROM projects WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return (int(row[0]), False)
    cur = app_conn.execute(
        "INSERT INTO projects(name, description, preset_id) VALUES (?, ?, 'legacy')",
        (name, description),
    )
    return (int(cur.lastrowid), True)


def _existing_run_id(app_conn: sqlite3.Connection, project_id: int, run_dir: str) -> int | None:
    row = app_conn.execute(
        "SELECT id FROM runs WHERE project_id = ? AND run_dir = ?",
        (project_id, run_dir),
    ).fetchone()
    return int(row[0]) if row is not None else None


# ---------------------------------------------------------------------------
# Core migration
# ---------------------------------------------------------------------------
def migrate_legacy_output(
    db: AppDB, output_root: Path, *, dry_run: bool = False
) -> MigrationReport:
    """Scannt ``output_root`` rekursiv nach ``run_*``-Verzeichnissen.

    Fuer jedes Verzeichnis mit ``pipeline.db`` wird ein Run pro Legacy-Company
    (bzw. ein synthetisches ``legacy-<dirname>``-Projekt, falls keine
    Company-Eintraege existieren) in der App-DB angelegt und Codings +
    Materialien kopiert.

    Args:
        db: offene App-DB.
        output_root: Verzeichnis mit ``run_*`` Unterordnern. Zeigt es direkt
            auf ein ``run_*``, wird dieses einzeln migriert.
        dry_run: Wenn True: nur Scan + Report, keine Writes.

    Returns:
        :class:`MigrationReport` mit Zaehlern und Warnungen.
    """
    report = MigrationReport()
    output_root = Path(output_root)

    run_dirs = _find_run_dirs(output_root)

    for run_dir in run_dirs:
        report.run_dirs_scanned += 1
        pipeline_path = run_dir / "pipeline.db"
        if not pipeline_path.exists():
            report.run_dirs_skipped += 1
            report.warnings.append(f"{run_dir}: keine pipeline.db gefunden")
            continue
        try:
            codings, materials, warnings, migrated = _migrate_one_run(
                db, pipeline_path, run_dir, dry_run, report
            )
        except sqlite3.DatabaseError as exc:
            report.run_dirs_skipped += 1
            report.warnings.append(f"{run_dir}: pipeline.db nicht lesbar ({exc})")
            continue

        report.codings_imported += codings
        report.materials_imported += materials
        report.warnings.extend(warnings)
        if migrated:
            report.run_dirs_migrated += 1
        else:
            report.run_dirs_skipped += 1

    return report


def _migrate_one_run(
    db: AppDB,
    pipeline_db_path: Path,
    run_dir: Path,
    dry_run: bool,
    report: MigrationReport,
) -> tuple[int, int, list[str], bool]:
    """Importiert genau einen Legacy-Run.

    Args:
        db: offene App-DB.
        pipeline_db_path: absoluter Pfad zur ``pipeline.db``.
        run_dir: Run-Verzeichnis (wird als ``runs.run_dir`` gespeichert).
        dry_run: Wenn True, nur lesen.
        report: wird fuer ``projects_created`` direkt modifiziert.

    Returns:
        Tuple ``(codings_imported, materials_imported, warnings, migrated)``.
        ``migrated=False`` bedeutet, dass der Run bereits in der App-DB
        existierte oder bei dry_run keine Writes vorgenommen wurden — die
        App-DB hat sich also nicht veraendert.
    """
    warnings: list[str] = []
    codings_total = 0
    materials_total = 0
    migrated_any = False

    run_dir_str = str(run_dir.resolve())

    legacy = _connect_legacy_ro(pipeline_db_path)
    try:
        state = _get_run_state(legacy)
        status = _normalize_status(state.get("status"))
        started_at = state.get("started_at")
        finished_at = state.get("updated_at") if status == "completed" else None

        # --- Companies ermitteln ---------------------------------------
        company_rows: list[sqlite3.Row | dict] = []
        if _table_exists(legacy, "companies"):
            try:
                company_rows = list(legacy.execute("SELECT id, name FROM companies").fetchall())
            except sqlite3.Error as exc:
                warnings.append(f"{run_dir}: companies nicht lesbar ({exc})")

        if not company_rows:
            company_rows = [{"id": None, "name": f"legacy-{run_dir.name}"}]

        # --- Pro Company einen Run anlegen -----------------------------
        for comp in company_rows:
            comp_id = comp["id"] if hasattr(comp, "keys") else comp.get("id")
            comp_name = comp["name"] if hasattr(comp, "keys") else comp.get("name")
            if not comp_name:
                comp_name = f"legacy-{run_dir.name}"

            codings_here, materials_here, warns_here, did_write = _migrate_company_slice(
                db=db,
                legacy=legacy,
                run_dir=run_dir,
                run_dir_str=run_dir_str,
                project_name=str(comp_name),
                legacy_company_id=comp_id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                dry_run=dry_run,
                report=report,
            )
            codings_total += codings_here
            materials_total += materials_here
            warnings.extend(warns_here)
            if did_write:
                migrated_any = True
    finally:
        legacy.close()

    return (codings_total, materials_total, warnings, migrated_any)


def _migrate_company_slice(
    *,
    db: AppDB,
    legacy: sqlite3.Connection,
    run_dir: Path,
    run_dir_str: str,
    project_name: str,
    legacy_company_id: int | None,
    status: str,
    started_at: str | None,
    finished_at: str | None,
    dry_run: bool,
    report: MigrationReport,
) -> tuple[int, int, list[str], bool]:
    """Migriert die Codings/Materialien einer einzelnen Legacy-Company."""
    warnings: list[str] = []

    if dry_run:
        # Wir simulieren die Zaehler trotzdem, damit der Report aussagekraeftig
        # bleibt. Kein Write.
        sim_codings = _count_codings_for_company(legacy, legacy_company_id, warnings, run_dir)
        sim_materials = _count_materials_for_company(legacy, legacy_company_id, warnings, run_dir)
        return (sim_codings, sim_materials, warnings, False)

    with db.transaction() as app_conn:
        project_id, created = _ensure_project(
            app_conn,
            project_name,
            description=f"Imported from legacy run {run_dir.name}",
        )
        if created:
            report.projects_created += 1

        existing = _existing_run_id(app_conn, project_id, run_dir_str)
        if existing is not None:
            # Idempotenz — Run wurde bereits importiert.
            return (0, 0, warnings, False)

        cur = app_conn.execute(
            """
            INSERT INTO runs(project_id, run_dir, config_json, status,
                             started_at, finished_at)
            VALUES (?, ?, ?, ?,
                    COALESCE(?, CURRENT_TIMESTAMP), ?)
            """,
            (
                project_id,
                run_dir_str,
                "{}",
                status,
                started_at,
                finished_at,
            ),
        )
        run_id = int(cur.lastrowid)

        materials_imported = _import_materials(
            app_conn, legacy, run_id, legacy_company_id, warnings, run_dir
        )
        codings_imported = _import_codings(
            app_conn,
            legacy,
            run_id=run_id,
            project_id=project_id,
            legacy_company_id=legacy_company_id,
            warnings=warnings,
            run_dir=run_dir,
        )

    return (codings_imported, materials_imported, warnings, True)


# ---------------------------------------------------------------------------
# Materials
# ---------------------------------------------------------------------------
def _import_materials(
    app_conn: sqlite3.Connection,
    legacy: sqlite3.Connection,
    run_id: int,
    legacy_company_id: int | None,
    warnings: list[str],
    run_dir: Path,
) -> int:
    count = 0
    # PDFs
    if _table_exists(legacy, "pdf_documents"):
        cols = _columns(legacy, "pdf_documents")
        has_company = "company_id" in cols
        try:
            if has_company and legacy_company_id is not None:
                rows = legacy.execute(
                    "SELECT path, relative_path, filename FROM pdf_documents WHERE company_id = ?",
                    (legacy_company_id,),
                ).fetchall()
            elif legacy_company_id is None:
                rows = legacy.execute(
                    "SELECT path, relative_path, filename FROM pdf_documents"
                ).fetchall()
            else:
                # company_id Spalte nicht da, aber mehrere Companies -> nicht
                # eindeutig zuordenbar. Nimm alle, damit nichts verloren geht.
                rows = legacy.execute(
                    "SELECT path, relative_path, filename FROM pdf_documents"
                ).fetchall()
        except sqlite3.Error as exc:
            warnings.append(f"{run_dir}: pdf_documents nicht lesbar ({exc})")
            rows = []
        for r in rows:
            app_conn.execute(
                """
                INSERT INTO run_materials(run_id, material_kind, path,
                                          relative_path, source_label)
                VALUES (?, 'pdf_text', ?, ?, ?)
                """,
                (run_id, r["path"] or "", r["relative_path"] or "", r["filename"] or ""),
            )
            count += 1

    # Interviews
    if _table_exists(legacy, "interview_documents"):
        try:
            if legacy_company_id is not None:
                rows = legacy.execute(
                    "SELECT path, filename FROM interview_documents WHERE company_id = ?",
                    (legacy_company_id,),
                ).fetchall()
            else:
                rows = legacy.execute("SELECT path, filename FROM interview_documents").fetchall()
        except sqlite3.Error as exc:
            warnings.append(f"{run_dir}: interview_documents nicht lesbar ({exc})")
            rows = []
        for r in rows:
            app_conn.execute(
                """
                INSERT INTO run_materials(run_id, material_kind, path,
                                          relative_path, source_label)
                VALUES (?, 'transcript', ?, '', ?)
                """,
                (run_id, r["path"] or "", r["filename"] or ""),
            )
            count += 1

    return count


def _count_materials_for_company(
    legacy: sqlite3.Connection,
    legacy_company_id: int | None,
    warnings: list[str],
    run_dir: Path,
) -> int:
    count = 0
    if _table_exists(legacy, "pdf_documents"):
        try:
            cols = _columns(legacy, "pdf_documents")
            if "company_id" in cols and legacy_company_id is not None:
                row = legacy.execute(
                    "SELECT COUNT(*) FROM pdf_documents WHERE company_id = ?",
                    (legacy_company_id,),
                ).fetchone()
            else:
                row = legacy.execute("SELECT COUNT(*) FROM pdf_documents").fetchone()
            count += int(row[0])
        except sqlite3.Error as exc:
            warnings.append(f"{run_dir}: pdf_documents count ({exc})")
    if _table_exists(legacy, "interview_documents"):
        try:
            if legacy_company_id is not None:
                row = legacy.execute(
                    "SELECT COUNT(*) FROM interview_documents WHERE company_id = ?",
                    (legacy_company_id,),
                ).fetchone()
            else:
                row = legacy.execute("SELECT COUNT(*) FROM interview_documents").fetchone()
            count += int(row[0])
        except sqlite3.Error as exc:
            warnings.append(f"{run_dir}: interview_documents count ({exc})")
    return count


# ---------------------------------------------------------------------------
# Codings
# ---------------------------------------------------------------------------
def _import_codings(
    app_conn: sqlite3.Connection,
    legacy: sqlite3.Connection,
    *,
    run_id: int,
    project_id: int,
    legacy_company_id: int | None,
    warnings: list[str],
    run_dir: Path,
) -> int:
    if not _table_exists(legacy, "codings"):
        return 0
    cols = _columns(legacy, "codings")
    # Legacy-Schema: codings(id, pdf_id, page, block_id, source, description, ...,
    # begruendung).
    has_begruendung = "begruendung" in cols
    has_description = "description" in cols
    has_block = "block_id" in cols

    select_cols = ["c.id", "c.pdf_id"]
    if has_block:
        select_cols.append("c.block_id")
    else:
        select_cols.append("'' AS block_id")
    if has_description:
        select_cols.append("c.description")
    else:
        select_cols.append("'' AS description")
    if has_begruendung:
        select_cols.append("c.begruendung")
    else:
        select_cols.append("'' AS begruendung")

    sql = f"SELECT {', '.join(select_cols)} FROM codings c"
    try:
        coding_rows = legacy.execute(sql).fetchall()
    except sqlite3.Error as exc:
        warnings.append(f"{run_dir}: codings nicht lesbar ({exc})")
        return 0

    # Preload join-tables
    has_coding_codes = _table_exists(legacy, "coding_codes")
    has_pdf_docs = _table_exists(legacy, "pdf_documents")
    has_interview_docs = _table_exists(legacy, "interview_documents")

    # Build pdf_id -> (relative_path_or_path, company_id_if_any)
    pdf_lookup: dict[int, tuple[str, int | None]] = {}
    if has_pdf_docs:
        pdf_cols = _columns(legacy, "pdf_documents")
        has_company = "company_id" in pdf_cols
        select = "id, COALESCE(NULLIF(relative_path, ''), path, filename) AS doc"
        if has_company:
            select += ", company_id"
        try:
            for r in legacy.execute(f"SELECT {select} FROM pdf_documents").fetchall():
                pdf_lookup[int(r["id"])] = (
                    r["doc"] or "",
                    r["company_id"] if has_company else None,
                )
        except sqlite3.Error as exc:
            warnings.append(f"{run_dir}: pdf_documents join ({exc})")

    # Preload codes per coding
    codes_lookup: dict[int, list[str]] = {}
    if has_coding_codes:
        try:
            for r in legacy.execute("SELECT coding_id, code_id FROM coding_codes").fetchall():
                codes_lookup.setdefault(int(r["coding_id"]), []).append(r["code_id"])
        except sqlite3.Error as exc:
            warnings.append(f"{run_dir}: coding_codes nicht lesbar ({exc})")

    count = 0
    for row in coding_rows:
        pdf_id = row["pdf_id"]
        doc, pdf_company = (
            pdf_lookup.get(int(pdf_id), ("", None)) if pdf_id is not None else ("", None)
        )

        # Wenn wir eine spezifische Company filtern und das PDF einer
        # anderen Company gehoert, ueberspringen. Wenn keine Zuordnung
        # moeglich ist (Spalte fehlt), zaehle alle mit.
        if (
            legacy_company_id is not None
            and pdf_company is not None
            and pdf_company != legacy_company_id
        ):
            continue

        block_id = row["block_id"] or ""
        seg_start, seg_end = _parse_block_range(block_id)
        description = row["description"] or ""
        begruendung = row["begruendung"] or ""

        code_ids = codes_lookup.get(int(row["id"]), [])
        if not code_ids:
            code_ids = [""]  # mindestens ein Eintrag, Code bleibt leer

        for code_id in code_ids:
            app_conn.execute(
                """
                INSERT INTO codings(run_id, project_id, document, code_id,
                                    segment_start, segment_end, text,
                                    bbox_json, confidence, justification,
                                    facet_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, '', NULL, ?, '')
                """,
                (
                    run_id,
                    project_id,
                    doc,
                    code_id,
                    seg_start,
                    seg_end,
                    description,
                    begruendung,
                ),
            )
            count += 1

    # Interviews hatten in der Legacy-DB keine eigenen Codings — die wurden
    # ueber einen anderen Flow geschrieben. Wenn sie existieren, bleibt
    # count trotzdem korrekt (wir importieren nur, was da ist).
    _ = has_interview_docs
    return count


def _count_codings_for_company(
    legacy: sqlite3.Connection,
    legacy_company_id: int | None,
    warnings: list[str],
    run_dir: Path,
) -> int:
    """Schnelle Zaehlung fuer dry-run (kein Code-Join — nur codings*codes)."""
    if not _table_exists(legacy, "codings"):
        return 0
    try:
        if not _table_exists(legacy, "coding_codes"):
            row = legacy.execute("SELECT COUNT(*) FROM codings").fetchone()
            return int(row[0])

        # 1 Coding kann N Codes haben -> N Zeilen in codings
        if legacy_company_id is None:
            row = legacy.execute("SELECT COUNT(*) FROM coding_codes").fetchone()
            return int(row[0])

        pdf_cols = _columns(legacy, "pdf_documents")
        if "company_id" not in pdf_cols:
            row = legacy.execute("SELECT COUNT(*) FROM coding_codes").fetchone()
            return int(row[0])

        row = legacy.execute(
            """
            SELECT COUNT(*) FROM coding_codes cc
            JOIN codings c ON cc.coding_id = c.id
            JOIN pdf_documents d ON c.pdf_id = d.id
            WHERE d.company_id = ?
            """,
            (legacy_company_id,),
        ).fetchone()
        return int(row[0])
    except sqlite3.Error as exc:
        warnings.append(f"{run_dir}: count codings ({exc})")
        return 0


__all__ = [
    "MigrationReport",
    "migrate_legacy_output",
]
