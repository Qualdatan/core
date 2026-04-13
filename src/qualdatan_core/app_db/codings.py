# SPDX-License-Identifier: AGPL-3.0-only
"""DAO fuer Codings + Cross-Run-Query-Helpers (App-DB Phase D).

Kapselt CRUD- und Aggregat-Zugriffe auf die Tabelle ``codings`` der
globalen :class:`AppDB`. Codings sind das flache Ergebnis-Log der
Pipeline und koennen cross-run/cross-document aggregiert werden
(Triangulation).

Public names (werden vom Coordinator in ``app_db/__init__.py`` re-exportiert):

    CodingRecord, CodeFrequency,
    add_coding, add_coded_segment, add_codings_bulk,
    get_coding, list_codings, count_codings, delete_codings_for_run,
    code_frequencies, codings_by_document, unique_codes_for_project,
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from . import AppDB
from ..models import CodedSegment


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CodingRecord:
    """Ein einzelner Coding-Eintrag in der App-DB.

    ``segment_start`` / ``segment_end`` sind ``None`` fuer visuelle
    Codings (ohne Zeichen-Offset im Text); ``bbox_json`` haelt dann die
    Bounding-Box.
    """

    id: int
    run_id: int
    project_id: int
    document: str
    code_id: str
    segment_start: int | None
    segment_end: int | None
    text: str
    bbox_json: str
    confidence: float | None
    justification: str
    facet_id: str
    created_at: str


@dataclass(frozen=True)
class CodeFrequency:
    """Haeufigkeits-Aggregat eines Codes innerhalb eines Projekts."""

    code_id: str
    run_count: int
    document_count: int
    coding_count: int


# ---------------------------------------------------------------------------
# Row -> Dataclass helpers
# ---------------------------------------------------------------------------
_CODING_COLUMNS: tuple[str, ...] = (
    "run_id",
    "project_id",
    "document",
    "code_id",
    "segment_start",
    "segment_end",
    "text",
    "bbox_json",
    "confidence",
    "justification",
    "facet_id",
)


def _row_to_coding(row: sqlite3.Row) -> CodingRecord:
    return CodingRecord(
        id=row["id"],
        run_id=row["run_id"],
        project_id=row["project_id"],
        document=row["document"],
        code_id=row["code_id"],
        segment_start=row["segment_start"],
        segment_end=row["segment_end"],
        text=row["text"],
        bbox_json=row["bbox_json"],
        confidence=row["confidence"],
        justification=row["justification"],
        facet_id=row["facet_id"],
        created_at=row["created_at"],
    )


def _fetch_coding(conn: sqlite3.Connection, coding_id: int) -> CodingRecord:
    row = conn.execute(
        "SELECT * FROM codings WHERE id = ?", (coding_id,)
    ).fetchone()
    return _row_to_coding(row)


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------
_FILTER_COLUMNS: tuple[str, ...] = (
    "run_id",
    "project_id",
    "code_id",
    "document",
    "facet_id",
)


def _build_filter_clause(
    filters: Mapping[str, Any],
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    for col in _FILTER_COLUMNS:
        val = filters.get(col)
        if val is None:
            continue
        clauses.append(f"{col} = ?")
        params.append(val)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------
def add_coding(
    db: AppDB,
    *,
    run_id: int,
    project_id: int,
    document: str,
    code_id: str,
    segment_start: int | None = None,
    segment_end: int | None = None,
    text: str = "",
    bbox_json: str = "",
    confidence: float | None = None,
    justification: str = "",
    facet_id: str = "",
) -> CodingRecord:
    """Fuegt ein einzelnes Coding ein und gibt den angelegten Record zurueck.

    Raises:
        sqlite3.IntegrityError: bei unbekanntem ``run_id`` oder
            ``project_id`` (FK-Violation).
    """

    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO codings"
            "(run_id, project_id, document, code_id, segment_start,"
            " segment_end, text, bbox_json, confidence, justification,"
            " facet_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                project_id,
                document,
                code_id,
                segment_start,
                segment_end,
                text,
                bbox_json,
                confidence,
                justification,
                facet_id,
            ),
        )
        return _fetch_coding(conn, cur.lastrowid)


def add_coded_segment(
    db: AppDB,
    *,
    run_id: int,
    project_id: int,
    segment: CodedSegment,
    facet_id: str = "",
    confidence: float | None = None,
) -> CodingRecord:
    """Convenience: mappt ein :class:`CodedSegment` auf ein Coding.

    ``segment.char_start`` → ``segment_start``, ``segment.char_end`` →
    ``segment_end``. ``segment.abgrenzungsregel`` wird **nicht**
    persistiert (gehoert ins Codebook, nicht pro Coding).
    """
    return add_coding(
        db,
        run_id=run_id,
        project_id=project_id,
        document=segment.document,
        code_id=segment.code_id,
        segment_start=segment.char_start,
        segment_end=segment.char_end,
        text=segment.text,
        bbox_json="",
        confidence=confidence,
        justification="",
        facet_id=facet_id,
    )


def add_codings_bulk(
    db: AppDB,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    """Fuegt viele Codings in einer einzigen Transaktion ein.

    Unbekannte Keys werden ignoriert; fehlende Keys erhalten die
    SQL-Defaults (``""`` / ``NULL``). ``run_id``, ``project_id``,
    ``document`` und ``code_id`` sind Pflicht.

    Returns:
        Anzahl eingefuegter Zeilen.

    Raises:
        KeyError: wenn eine Pflicht-Spalte fehlt.
        sqlite3.IntegrityError: bei FK-Violation (Rollback der
            gesamten Charge).
    """
    materialized: list[tuple[Any, ...]] = []
    for row in rows:
        if "run_id" not in row:
            raise KeyError("run_id fehlt in Bulk-Row")
        if "project_id" not in row:
            raise KeyError("project_id fehlt in Bulk-Row")
        if "document" not in row:
            raise KeyError("document fehlt in Bulk-Row")
        if "code_id" not in row:
            raise KeyError("code_id fehlt in Bulk-Row")
        materialized.append(
            (
                row["run_id"],
                row["project_id"],
                row["document"],
                row["code_id"],
                row.get("segment_start"),
                row.get("segment_end"),
                row.get("text", ""),
                row.get("bbox_json", ""),
                row.get("confidence"),
                row.get("justification", ""),
                row.get("facet_id", ""),
            )
        )

    if not materialized:
        return 0

    with db.transaction() as conn:
        conn.executemany(
            "INSERT INTO codings"
            "(run_id, project_id, document, code_id, segment_start,"
            " segment_end, text, bbox_json, confidence, justification,"
            " facet_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            materialized,
        )
    return len(materialized)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
def get_coding(db: AppDB, coding_id: int) -> CodingRecord | None:
    """Liest ein Coding per ID, ``None`` wenn nicht vorhanden."""
    with db.connection() as conn:
        row = conn.execute(
            "SELECT * FROM codings WHERE id = ?", (coding_id,)
        ).fetchone()
    return _row_to_coding(row) if row else None


def list_codings(
    db: AppDB,
    *,
    run_id: int | None = None,
    project_id: int | None = None,
    code_id: str | None = None,
    document: str | None = None,
    facet_id: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[CodingRecord]:
    """Listet Codings mit optionalen Filtern.

    Reihenfolge: ``ORDER BY created_at, id`` (stabil, chronologisch).
    """
    where, params = _build_filter_clause(
        {
            "run_id": run_id,
            "project_id": project_id,
            "code_id": code_id,
            "document": document,
            "facet_id": facet_id,
        }
    )
    sql = f"SELECT * FROM codings{where} ORDER BY created_at, id"
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params = [*params, limit, offset]
    elif offset:
        # SQLite braucht LIMIT fuer OFFSET; -1 = unlimited.
        sql += " LIMIT -1 OFFSET ?"
        params = [*params, offset]
    with db.connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_coding(r) for r in rows]


def count_codings(
    db: AppDB,
    *,
    run_id: int | None = None,
    project_id: int | None = None,
    code_id: str | None = None,
    document: str | None = None,
    facet_id: str | None = None,
) -> int:
    """Zaehlt Codings mit denselben Filtern wie :func:`list_codings`."""
    where, params = _build_filter_clause(
        {
            "run_id": run_id,
            "project_id": project_id,
            "code_id": code_id,
            "document": document,
            "facet_id": facet_id,
        }
    )
    sql = f"SELECT COUNT(*) FROM codings{where}"
    with db.connection() as conn:
        row = conn.execute(sql, params).fetchone()
    return int(row[0])


def delete_codings_for_run(db: AppDB, run_id: int) -> int:
    """Loescht alle Codings eines Runs.

    Returns:
        Anzahl geloeschter Zeilen.
    """
    with db.transaction() as conn:
        cur = conn.execute(
            "DELETE FROM codings WHERE run_id = ?", (run_id,)
        )
    return int(cur.rowcount or 0)


# ---------------------------------------------------------------------------
# Cross-run summaries (Triangulation)
# ---------------------------------------------------------------------------
def code_frequencies(
    db: AppDB,
    *,
    project_id: int,
    code_ids: Sequence[str] | None = None,
) -> list[CodeFrequency]:
    """Aggregat je Code innerhalb eines Projekts.

    Args:
        db: App-DB.
        project_id: Projekt, ueber das aggregiert wird.
        code_ids: Optional auf diese Codes einschraenken.

    Returns:
        Liste nach ``coding_count DESC, code_id`` sortiert.
    """
    sql = (
        "SELECT code_id, "
        "       COUNT(DISTINCT run_id)   AS run_count, "
        "       COUNT(DISTINCT document) AS document_count, "
        "       COUNT(*)                 AS coding_count "
        "FROM codings WHERE project_id = ?"
    )
    params: list[Any] = [project_id]
    if code_ids is not None:
        code_ids = list(code_ids)
        if not code_ids:
            return []
        placeholders = ",".join("?" for _ in code_ids)
        sql += f" AND code_id IN ({placeholders})"
        params.extend(code_ids)
    sql += " GROUP BY code_id ORDER BY coding_count DESC, code_id ASC"

    with db.connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        CodeFrequency(
            code_id=r["code_id"],
            run_count=int(r["run_count"]),
            document_count=int(r["document_count"]),
            coding_count=int(r["coding_count"]),
        )
        for r in rows
    ]


def codings_by_document(
    db: AppDB,
    *,
    project_id: int,
    document: str,
) -> list[CodingRecord]:
    """Alle Codings eines Dokuments innerhalb eines Projekts.

    Reihenfolge: ``segment_start`` (NULLs zuletzt), dann ``id``.
    """
    with db.connection() as conn:
        rows = conn.execute(
            "SELECT * FROM codings "
            "WHERE project_id = ? AND document = ? "
            "ORDER BY segment_start IS NULL, segment_start, id",
            (project_id, document),
        ).fetchall()
    return [_row_to_coding(r) for r in rows]


def unique_codes_for_project(db: AppDB, project_id: int) -> list[str]:
    """Alle in einem Projekt vorkommenden ``code_id``-Werte (sortiert)."""
    with db.connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT code_id FROM codings "
            "WHERE project_id = ? ORDER BY code_id ASC",
            (project_id,),
        ).fetchall()
    return [r["code_id"] for r in rows]


__all__ = [
    "CodingRecord",
    "CodeFrequency",
    "add_coding",
    "add_coded_segment",
    "add_codings_bulk",
    "get_coding",
    "list_codings",
    "count_codings",
    "delete_codings_for_run",
    "code_frequencies",
    "codings_by_document",
    "unique_codes_for_project",
]
