# SPDX-License-Identifier: AGPL-3.0-only
"""DAO fuer Projekte, Runs, Run-Materialien und Run-Facets (App-DB Phase D).

Kapselt CRUD-Zugriffe auf die Tabellen ``projects``, ``runs``,
``run_materials`` und ``run_facets`` der globalen :class:`AppDB`.
Funktionale API: jede Funktion bekommt die :class:`AppDB` als erstes
Argument, gibt frozen Dataclasses zurueck (keine ``sqlite3.Row``-Leaks).

Public names (werden vom Coordinator in ``app_db/__init__.py`` re-exportiert):

    Project, Run, RunMaterial, RunFacet,
    create_project, get_project, get_project_by_name, list_projects,
    update_project, delete_project,
    create_run, get_run, list_runs, update_run_status, get_latest_run,
    delete_run,
    add_run_material, list_run_materials,
    add_run_facet, list_run_facets,
    VALID_RUN_STATUSES,
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from . import AppDB

VALID_RUN_STATUSES: frozenset[str] = frozenset({"pending", "running", "completed", "failed"})
_TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "failed"})


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Project:
    """Ein Projekt im Projekt-Katalog."""

    id: int
    name: str
    description: str
    preset_id: str
    created_at: str


@dataclass(frozen=True)
class Run:
    """Ein einzelner Pipeline-Run eines Projekts."""

    id: int
    project_id: int
    run_dir: str
    config_json: str
    status: str
    started_at: str
    finished_at: str | None


@dataclass(frozen=True)
class RunMaterial:
    """Eine Materialquelle (Transcript/PDF/Bild) innerhalb eines Runs."""

    id: int
    run_id: int
    material_kind: str
    path: str
    relative_path: str
    source_label: str


@dataclass(frozen=True)
class RunFacet:
    """Eine im Run aktive Facet mit Parametern."""

    id: int
    run_id: int
    facet_id: str
    bundle_id: str
    params_json: str


# ---------------------------------------------------------------------------
# Row -> Dataclass helpers
# ---------------------------------------------------------------------------
def _row_to_project(row: sqlite3.Row) -> Project:
    return Project(
        id=row["id"],
        name=row["name"],
        description=row["description"],
        preset_id=row["preset_id"],
        created_at=row["created_at"],
    )


def _row_to_run(row: sqlite3.Row) -> Run:
    return Run(
        id=row["id"],
        project_id=row["project_id"],
        run_dir=row["run_dir"],
        config_json=row["config_json"],
        status=row["status"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def _row_to_material(row: sqlite3.Row) -> RunMaterial:
    return RunMaterial(
        id=row["id"],
        run_id=row["run_id"],
        material_kind=row["material_kind"],
        path=row["path"],
        relative_path=row["relative_path"],
        source_label=row["source_label"],
    )


def _row_to_facet(row: sqlite3.Row) -> RunFacet:
    return RunFacet(
        id=row["id"],
        run_id=row["run_id"],
        facet_id=row["facet_id"],
        bundle_id=row["bundle_id"],
        params_json=row["params_json"],
    )


def _rows(cursor: Iterable[sqlite3.Row], mapper) -> list:
    return [mapper(r) for r in cursor]


def _utc_now_iso() -> str:
    # Timezone-naive UTC ISO string (stripped "+00:00") for schema-kompatible
    # Timestamps analog zu SQLite's CURRENT_TIMESTAMP.
    return datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds")


def _validate_status(status: str) -> None:
    if status not in VALID_RUN_STATUSES:
        raise ValueError(
            f"Ungueltiger Run-Status: {status!r}. Erlaubt: {sorted(VALID_RUN_STATUSES)}"
        )


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------
def create_project(
    db: AppDB,
    *,
    name: str,
    description: str = "",
    preset_id: str = "",
) -> Project:
    """Legt ein Projekt an und gibt es zurueck.

    Raises:
        sqlite3.IntegrityError: wenn ``name`` bereits existiert (UNIQUE).
    """

    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO projects(name, description, preset_id) VALUES (?, ?, ?)",
            (name, description, preset_id),
        )
        new_id = cur.lastrowid
        row = conn.execute("SELECT * FROM projects WHERE id = ?", (new_id,)).fetchone()
    return _row_to_project(row)


def get_project(db: AppDB, project_id: int) -> Project | None:
    """Liest ein Projekt per ID, ``None`` wenn nicht vorhanden."""
    with db.connection() as conn:
        row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    return _row_to_project(row) if row else None


def get_project_by_name(db: AppDB, name: str) -> Project | None:
    """Liest ein Projekt per Name."""
    with db.connection() as conn:
        row = conn.execute("SELECT * FROM projects WHERE name = ?", (name,)).fetchone()
    return _row_to_project(row) if row else None


def list_projects(db: AppDB) -> list[Project]:
    """Alle Projekte alphabetisch nach Name."""
    with db.connection() as conn:
        rows = conn.execute("SELECT * FROM projects ORDER BY name ASC").fetchall()
    return _rows(rows, _row_to_project)


def update_project(
    db: AppDB,
    project_id: int,
    *,
    name: str | None = None,
    description: str | None = None,
    preset_id: str | None = None,
) -> Project:
    """Partielles Update eines Projekts.

    Raises:
        LookupError: wenn das Projekt nicht existiert.
    """
    updates: dict[str, Any] = {}
    if name is not None:
        updates["name"] = name
    if description is not None:
        updates["description"] = description
    if preset_id is not None:
        updates["preset_id"] = preset_id

    with db.transaction() as conn:
        existing = conn.execute("SELECT id FROM projects WHERE id = ?", (project_id,)).fetchone()
        if existing is None:
            raise LookupError(f"Projekt {project_id} existiert nicht")
        if updates:
            cols = ", ".join(f"{k} = ?" for k in updates)
            conn.execute(
                f"UPDATE projects SET {cols} WHERE id = ?",
                (*updates.values(), project_id),
            )
        row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    return _row_to_project(row)


def delete_project(db: AppDB, project_id: int) -> None:
    """Loescht ein Projekt (kaskadiert auf Runs/Materials/Facets/Codings)."""
    with db.transaction() as conn:
        conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------
def create_run(
    db: AppDB,
    *,
    project_id: int,
    run_dir: str,
    config_json: str = "{}",
    status: str = "pending",
) -> Run:
    """Legt einen Run fuer ``project_id`` an.

    Raises:
        ValueError: bei ungueltigem ``status``.
        sqlite3.IntegrityError: bei unbekanntem ``project_id`` oder
            Verletzung des ``UNIQUE(project_id, run_dir)``.
    """
    _validate_status(status)
    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO runs(project_id, run_dir, config_json, status) VALUES (?, ?, ?, ?)",
            (project_id, run_dir, config_json, status),
        )
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (cur.lastrowid,)).fetchone()
    return _row_to_run(row)


def get_run(db: AppDB, run_id: int) -> Run | None:
    """Liest einen Run per ID."""
    with db.connection() as conn:
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    return _row_to_run(row) if row else None


def list_runs(
    db: AppDB,
    *,
    project_id: int | None = None,
    status: str | None = None,
    limit: int | None = None,
) -> list[Run]:
    """Runs filtern nach Projekt und/oder Status, neueste zuerst.

    Raises:
        ValueError: wenn ``status`` gesetzt und ungueltig ist.
    """
    if status is not None:
        _validate_status(status)
    clauses: list[str] = []
    params: list[Any] = []
    if project_id is not None:
        clauses.append("project_id = ?")
        params.append(project_id)
    if status is not None:
        clauses.append("status = ?")
        params.append(status)
    sql = "SELECT * FROM runs"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    with db.connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    return _rows(rows, _row_to_run)


def update_run_status(
    db: AppDB,
    run_id: int,
    status: str,
    *,
    finished_at: str | None = None,
) -> Run:
    """Setzt den Status eines Runs.

    Bei ``status="running"`` bleibt ``finished_at`` unveraendert.
    Bei terminalem Status (``completed`` / ``failed``) wird
    ``finished_at`` gesetzt, falls noch ``NULL``. Ein explizit
    uebergebenes ``finished_at`` hat Vorrang.

    Raises:
        ValueError: bei ungueltigem Status.
        LookupError: wenn der Run nicht existiert.
    """
    _validate_status(status)
    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise LookupError(f"Run {run_id} existiert nicht")

        if status == "running":
            # finished_at unveraendert lassen
            conn.execute("UPDATE runs SET status = ? WHERE id = ?", (status, run_id))
        elif status in _TERMINAL_STATUSES:
            ts = finished_at
            if ts is None and row["finished_at"] is None:
                ts = _utc_now_iso()
            if ts is not None:
                conn.execute(
                    "UPDATE runs SET status = ?, finished_at = ? WHERE id = ?",
                    (status, ts, run_id),
                )
            else:
                conn.execute(
                    "UPDATE runs SET status = ? WHERE id = ?",
                    (status, run_id),
                )
        else:  # pending
            if finished_at is not None:
                conn.execute(
                    "UPDATE runs SET status = ?, finished_at = ? WHERE id = ?",
                    (status, finished_at, run_id),
                )
            else:
                conn.execute(
                    "UPDATE runs SET status = ? WHERE id = ?",
                    (status, run_id),
                )

        new_row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    return _row_to_run(new_row)


def get_latest_run(db: AppDB, project_id: int) -> Run | None:
    """Liefert den neuesten Run des Projekts (nach ``id`` DESC)."""
    with db.connection() as conn:
        row = conn.execute(
            "SELECT * FROM runs WHERE project_id = ? ORDER BY id DESC LIMIT 1",
            (project_id,),
        ).fetchone()
    return _row_to_run(row) if row else None


def delete_run(db: AppDB, run_id: int) -> None:
    """Loescht einen Run (kaskadiert auf Materials/Facets/Codings)."""
    with db.transaction() as conn:
        conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))


# ---------------------------------------------------------------------------
# Materials
# ---------------------------------------------------------------------------
def add_run_material(
    db: AppDB,
    run_id: int,
    *,
    material_kind: str,
    path: str,
    relative_path: str = "",
    source_label: str = "",
) -> RunMaterial:
    """Fuegt ein Material zum Run hinzu.

    Raises:
        sqlite3.IntegrityError: wenn ``run_id`` unbekannt ist.
    """
    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO run_materials"
            "(run_id, material_kind, path, relative_path, source_label) "
            "VALUES (?, ?, ?, ?, ?)",
            (run_id, material_kind, path, relative_path, source_label),
        )
        row = conn.execute("SELECT * FROM run_materials WHERE id = ?", (cur.lastrowid,)).fetchone()
    return _row_to_material(row)


def list_run_materials(db: AppDB, run_id: int) -> list[RunMaterial]:
    """Alle Materialien eines Runs in Einfuegereihenfolge (``id`` ASC)."""
    with db.connection() as conn:
        rows = conn.execute(
            "SELECT * FROM run_materials WHERE run_id = ? ORDER BY id ASC",
            (run_id,),
        ).fetchall()
    return _rows(rows, _row_to_material)


# ---------------------------------------------------------------------------
# Facets
# ---------------------------------------------------------------------------
def add_run_facet(
    db: AppDB,
    run_id: int,
    *,
    facet_id: str,
    bundle_id: str = "",
    params_json: str = "{}",
) -> RunFacet:
    """Aktiviert eine Facet im Run.

    Raises:
        sqlite3.IntegrityError: wenn ``run_id`` unbekannt ist.
    """
    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO run_facets(run_id, facet_id, bundle_id, params_json) VALUES (?, ?, ?, ?)",
            (run_id, facet_id, bundle_id, params_json),
        )
        row = conn.execute("SELECT * FROM run_facets WHERE id = ?", (cur.lastrowid,)).fetchone()
    return _row_to_facet(row)


def list_run_facets(db: AppDB, run_id: int) -> list[RunFacet]:
    """Alle Facets eines Runs in Einfuegereihenfolge (``id`` ASC)."""
    with db.connection() as conn:
        rows = conn.execute(
            "SELECT * FROM run_facets WHERE run_id = ? ORDER BY id ASC",
            (run_id,),
        ).fetchall()
    return _rows(rows, _row_to_facet)


__all__ = [
    "Project",
    "Run",
    "RunMaterial",
    "RunFacet",
    "VALID_RUN_STATUSES",
    "create_project",
    "get_project",
    "get_project_by_name",
    "list_projects",
    "update_project",
    "delete_project",
    "create_run",
    "get_run",
    "list_runs",
    "update_run_status",
    "get_latest_run",
    "delete_run",
    "add_run_material",
    "list_run_materials",
    "add_run_facet",
    "list_run_facets",
]
