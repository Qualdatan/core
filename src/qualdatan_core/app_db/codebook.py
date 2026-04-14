# SPDX-License-Identifier: AGPL-3.0-only
"""DAO fuer per-Projekt Codebook-Overrides (App-DB Phase E).

Kapselt CRUD-Zugriffe auf die Tabelle ``codebook_entries``. Jede
Projekt/Code-Kombination ist eindeutig (``UNIQUE(project_id, code_id)``)
und haelt optionale Overrides fuer Label, Farbe, Definition und
Beispiel-Liste. ``examples_override`` wird als JSON-Array in der DB
serialisiert und beim Lesen wieder deserialisiert.

Public names (werden vom Coordinator in ``app_db/__init__.py`` re-exportiert):

    CodebookEntry,
    upsert_codebook_entry, get_codebook_entry,
    list_codebook_entries, reset_codebook_entry,
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from . import AppDB


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CodebookEntry:
    """Ein Codebook-Override-Eintrag fuer genau ein (Projekt, Code)-Paar.

    Alle ``*_override``-Felder sind optional. ``examples_override`` ist
    eine Liste von Strings (``None`` wenn nicht gesetzt).
    """

    id: int
    project_id: int
    code_id: str
    label_override: str | None
    color_override: str | None
    definition_override: str | None
    examples_override: list[str] | None
    updated_at: str


# ---------------------------------------------------------------------------
# Row -> Dataclass
# ---------------------------------------------------------------------------
def _deserialise_examples(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if isinstance(data, list):
        return [str(x) for x in data]
    return None


def _serialise_examples(examples: list[str] | None) -> str | None:
    if examples is None:
        return None
    return json.dumps(list(examples), ensure_ascii=False)


def _row_to_entry(row: sqlite3.Row) -> CodebookEntry:
    return CodebookEntry(
        id=row["id"],
        project_id=row["project_id"],
        code_id=row["code_id"],
        label_override=row["label_override"],
        color_override=row["color_override"],
        definition_override=row["definition_override"],
        examples_override=_deserialise_examples(row["examples_override"]),
        updated_at=row["updated_at"],
    )


# ---------------------------------------------------------------------------
# Sentinel fuer "nicht uebergeben" vs. "explizit None"
# ---------------------------------------------------------------------------
class _Unset:
    """Marker fuer nicht uebergebene Kwargs (None ist eine gueltige Ueberschreibung-"clear")."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover
        return "<UNSET>"


_UNSET = _Unset()


# ---------------------------------------------------------------------------
# DAO
# ---------------------------------------------------------------------------
def upsert_codebook_entry(
    db: AppDB,
    project_id: int,
    code_id: str,
    *,
    label_override=_UNSET,
    color_override=_UNSET,
    definition_override=_UNSET,
    examples_override=_UNSET,
) -> CodebookEntry:
    """Insert-or-update eines Codebook-Eintrags fuer (``project_id``, ``code_id``).

    Nur explizit uebergebene Felder werden beim Update veraendert; Felder,
    die nicht als Kwarg uebergeben wurden, bleiben beim bestehenden Wert.
    Beim Insert werden nicht uebergebene Felder als ``NULL`` eingefuegt.

    ``examples_override`` wird JSON-serialisiert in der DB abgelegt.

    Args:
        db: Offene :class:`AppDB`.
        project_id: Projekt-ID (FK auf ``projects``).
        code_id: Code-ID (z. B. ``"A-01"``).
        label_override: Neuer Label-Override (``None`` loescht).
        color_override: Neuer Farb-Override (``"#RRGGBB"``).
        definition_override: Neue Definition.
        examples_override: Liste von Beispielen.

    Returns:
        Der aktuelle :class:`CodebookEntry` nach dem Upsert.

    Raises:
        sqlite3.IntegrityError: wenn ``project_id`` unbekannt ist.
    """
    with db.transaction() as conn:
        existing = conn.execute(
            "SELECT * FROM codebook_entries WHERE project_id = ? AND code_id = ?",
            (project_id, code_id),
        ).fetchone()

        label = (
            (existing["label_override"] if existing is not None else None)
            if isinstance(label_override, _Unset)
            else label_override
        )
        color = (
            (existing["color_override"] if existing is not None else None)
            if isinstance(color_override, _Unset)
            else color_override
        )
        definition = (
            (existing["definition_override"] if existing is not None else None)
            if isinstance(definition_override, _Unset)
            else definition_override
        )
        if isinstance(examples_override, _Unset):
            examples_raw = existing["examples_override"] if existing is not None else None
        else:
            examples_raw = _serialise_examples(examples_override)

        conn.execute(
            "INSERT INTO codebook_entries"
            "(project_id, code_id, label_override, color_override, "
            " definition_override, examples_override, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(project_id, code_id) DO UPDATE SET "
            "  label_override = excluded.label_override, "
            "  color_override = excluded.color_override, "
            "  definition_override = excluded.definition_override, "
            "  examples_override = excluded.examples_override, "
            "  updated_at = CURRENT_TIMESTAMP",
            (project_id, code_id, label, color, definition, examples_raw),
        )
        row = conn.execute(
            "SELECT * FROM codebook_entries WHERE project_id = ? AND code_id = ?",
            (project_id, code_id),
        ).fetchone()
    return _row_to_entry(row)


def get_codebook_entry(db: AppDB, project_id: int, code_id: str) -> CodebookEntry | None:
    """Liest den Eintrag zu (``project_id``, ``code_id``) oder ``None``."""
    with db.connection() as conn:
        row = conn.execute(
            "SELECT * FROM codebook_entries WHERE project_id = ? AND code_id = ?",
            (project_id, code_id),
        ).fetchone()
    return _row_to_entry(row) if row else None


def list_codebook_entries(db: AppDB, project_id: int) -> list[CodebookEntry]:
    """Alle Codebook-Overrides eines Projekts, stabil nach ``code_id``."""
    with db.connection() as conn:
        rows = conn.execute(
            "SELECT * FROM codebook_entries WHERE project_id = ? ORDER BY code_id ASC",
            (project_id,),
        ).fetchall()
    return [_row_to_entry(r) for r in rows]


def reset_codebook_entry(db: AppDB, project_id: int, code_id: str) -> bool:
    """Loescht den Eintrag fuer (``project_id``, ``code_id``).

    Returns:
        ``True`` wenn ein Eintrag geloescht wurde, ``False`` wenn keiner
        existierte.
    """
    with db.transaction() as conn:
        cur = conn.execute(
            "DELETE FROM codebook_entries WHERE project_id = ? AND code_id = ?",
            (project_id, code_id),
        )
        return cur.rowcount > 0


__all__ = [
    "CodebookEntry",
    "upsert_codebook_entry",
    "get_codebook_entry",
    "list_codebook_entries",
    "reset_codebook_entry",
]
