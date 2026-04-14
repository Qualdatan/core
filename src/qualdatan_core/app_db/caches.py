# SPDX-License-Identifier: AGPL-3.0-only
"""DAOs fuer die globalen Caches (``cache_llm`` und ``cache_pdf``).

Beide Caches leben in der App-DB (siehe :mod:`qualdatan_core.app_db`) und
sind run-uebergreifend. Sie werden ueber deterministische SHA256-Keys
indiziert:

* ``cache_llm.key_sha = sha256(model + prompt + sorted(params))``
* ``cache_pdf.key_sha = sha256(path + mtime + size_bytes)``

Das Modul bietet reine Key-Helper (ohne DB-Zugriff) sowie die
Get/Put/Invalidate-Operationen.

Example:
    >>> from qualdatan_core.app_db import AppDB
    >>> from qualdatan_core.app_db.caches import llm_cache_put, llm_cache_get
    >>> db = AppDB.open(":memory:")
    >>> entry = llm_cache_put(db, model="claude-3", prompt="hi", response="hello")
    >>> llm_cache_get(db, entry.key_sha).response
    'hello'
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "LLMCacheEntry",
    "PDFCacheEntry",
    "llm_cache_key",
    "prompt_hash",
    "pdf_cache_key",
    "llm_cache_get",
    "llm_cache_put",
    "llm_cache_invalidate",
    "llm_cache_clear",
    "pdf_cache_get",
    "pdf_cache_get_by_key",
    "pdf_cache_put",
    "pdf_cache_clear",
]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class LLMCacheEntry:
    """Ein gecachter LLM-Response-Eintrag.

    Attributes:
        key_sha: sha256 ueber ``model + prompt + sorted(params)``.
        model: Modell-Identifier (z.B. ``claude-sonnet-4``).
        prompt_hash: sha256 ueber den Prompt allein (fuer Indexing).
        response: Roher Response-Text.
        tokens_in: Optionaler Input-Token-Count.
        tokens_out: Optionaler Output-Token-Count.
        created_at: ISO-Timestamp (SQLite ``CURRENT_TIMESTAMP``).
    """

    key_sha: str
    model: str
    prompt_hash: str
    response: str
    tokens_in: int | None
    tokens_out: int | None
    created_at: str


@dataclass(frozen=True)
class PDFCacheEntry:
    """Ein gecachter PDF-Extraktions-Eintrag.

    Attributes:
        key_sha: sha256 ueber ``path + mtime + size_bytes``.
        path: Absoluter Pfad der Quelldatei.
        mtime: ``os.stat`` mtime (Float Sekunden).
        size_bytes: Dateigroesse in Bytes.
        extraction_json: JSON-serialisiertes Extraktionsergebnis.
        created_at: ISO-Timestamp.
    """

    key_sha: str
    path: str
    mtime: float
    size_bytes: int
    extraction_json: str
    created_at: str


# ---------------------------------------------------------------------------
# Key helpers (pure, no DB)
# ---------------------------------------------------------------------------
def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def prompt_hash(prompt: str) -> str:
    """sha256 ueber den UTF-8-encodierten Prompt.

    Args:
        prompt: Der rohe Prompt-Text.

    Returns:
        Hex-Digest.
    """
    return _sha256_hex(prompt.encode("utf-8"))


def llm_cache_key(
    model: str,
    prompt: str,
    params: Mapping[str, Any] | None = None,
) -> str:
    """Deterministischer Cache-Key fuer einen LLM-Call.

    Params werden via ``json.dumps(..., sort_keys=True, ensure_ascii=False)``
    kanonisiert, sodass die Reihenfolge im Dict keinen Einfluss hat.

    Args:
        model: Modell-Identifier.
        prompt: Prompt-Text.
        params: Optionale Call-Parameter (temperature, max_tokens, ...).

    Returns:
        Hex-Digest sha256.
    """
    params_json = json.dumps(params or {}, sort_keys=True, ensure_ascii=False)
    payload = "\0".join([model, prompt, params_json]).encode("utf-8")
    return _sha256_hex(payload)


def pdf_cache_key(path: str, mtime: float, size_bytes: int) -> str:
    """Deterministischer Cache-Key fuer eine PDF-Extraktion.

    Args:
        path: Absoluter Dateipfad.
        mtime: ``os.stat`` mtime (Float).
        size_bytes: Dateigroesse in Bytes.

    Returns:
        Hex-Digest sha256.
    """
    # mtime via repr() um Float-Rundung konsistent zu halten.
    payload = "\0".join([str(path), repr(float(mtime)), str(int(size_bytes))])
    return _sha256_hex(payload.encode("utf-8"))


# ---------------------------------------------------------------------------
# Row mapping helpers
# ---------------------------------------------------------------------------
def _row_to_llm(row) -> LLMCacheEntry:
    return LLMCacheEntry(
        key_sha=row["key_sha"],
        model=row["model"],
        prompt_hash=row["prompt_hash"],
        response=row["response"],
        tokens_in=row["tokens_in"],
        tokens_out=row["tokens_out"],
        created_at=row["created_at"],
    )


def _row_to_pdf(row) -> PDFCacheEntry:
    return PDFCacheEntry(
        key_sha=row["key_sha"],
        path=row["path"],
        mtime=row["mtime"],
        size_bytes=row["size_bytes"],
        extraction_json=row["extraction_json"],
        created_at=row["created_at"],
    )


# ---------------------------------------------------------------------------
# LLM cache ops
# ---------------------------------------------------------------------------
def llm_cache_get(db, key_sha: str) -> LLMCacheEntry | None:
    """Liefert den Cache-Eintrag zu ``key_sha`` oder ``None``."""
    with db.connection() as conn:
        row = conn.execute("SELECT * FROM cache_llm WHERE key_sha = ?", (key_sha,)).fetchone()
    return _row_to_llm(row) if row is not None else None


def llm_cache_put(
    db,
    *,
    model: str,
    prompt: str,
    response: str,
    params: Mapping[str, Any] | None = None,
    tokens_in: int | None = None,
    tokens_out: int | None = None,
) -> LLMCacheEntry:
    """Speichert einen LLM-Response-Cache-Eintrag (``INSERT OR REPLACE``).

    ``created_at`` wird bei Ueberschreiben auf ``CURRENT_TIMESTAMP``
    zurueckgesetzt (latest-wins-Semantik).

    Returns:
        Den persistierten :class:`LLMCacheEntry`.
    """
    key = llm_cache_key(model, prompt, params)
    ph = prompt_hash(prompt)
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO cache_llm
                (key_sha, model, prompt_hash, response,
                 tokens_in, tokens_out, created_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key_sha) DO UPDATE SET
                model = excluded.model,
                prompt_hash = excluded.prompt_hash,
                response = excluded.response,
                tokens_in = excluded.tokens_in,
                tokens_out = excluded.tokens_out,
                created_at = CURRENT_TIMESTAMP
            """,
            (key, model, ph, response, tokens_in, tokens_out),
        )
    # Re-fetch fuer korrekten created_at.
    entry = llm_cache_get(db, key)
    assert entry is not None  # noqa: S101 — just inserted
    return entry


def llm_cache_invalidate(db, key_sha: str) -> None:
    """Loescht einen einzelnen Eintrag (no-op falls nicht vorhanden)."""
    with db.transaction() as conn:
        conn.execute("DELETE FROM cache_llm WHERE key_sha = ?", (key_sha,))


def llm_cache_clear(db, *, older_than_iso: str | None = None) -> int:
    """Leert den LLM-Cache (ganz oder alle Eintraege aelter als ``older_than_iso``).

    Args:
        older_than_iso: Optionaler ISO-Timestamp. Wenn gesetzt, werden nur
            Eintraege mit ``created_at < older_than_iso`` geloescht.

    Returns:
        Anzahl geloeschter Zeilen.
    """
    with db.transaction() as conn:
        if older_than_iso is None:
            cur = conn.execute("DELETE FROM cache_llm")
        else:
            cur = conn.execute("DELETE FROM cache_llm WHERE created_at < ?", (older_than_iso,))
        return cur.rowcount or 0


# ---------------------------------------------------------------------------
# PDF cache ops
# ---------------------------------------------------------------------------
def _stat_key(path: Path) -> tuple[float, int, str]:
    """Holt mtime, size und Key fuer eine on-disk-Datei."""
    st = os.stat(path)
    key = pdf_cache_key(str(path), st.st_mtime, st.st_size)
    return st.st_mtime, st.st_size, key


def pdf_cache_get(db, path: Path | str) -> PDFCacheEntry | None:
    """Liefert den Cache-Eintrag fuer ``path``, wenn er noch aktuell ist.

    Berechnet den Key aus dem aktuellen ``os.stat`` und liefert nur dann
    eine Zeile, wenn diese per ``key_sha`` uebereinstimmt. Fehlt die Datei
    oder passt der Key nicht mehr (mtime/size-Aenderung), gibt es ``None``.
    Die gespeicherte Zeile wird **nicht** mutiert.
    """
    p = Path(path)
    if not p.exists():
        return None
    try:
        _mtime, _size, key = _stat_key(p)
    except OSError:
        return None
    return pdf_cache_get_by_key(db, key)


def pdf_cache_get_by_key(db, key_sha: str) -> PDFCacheEntry | None:
    """Liefert die Zeile zu ``key_sha`` auch, wenn die Datei verschwunden ist."""
    with db.connection() as conn:
        row = conn.execute("SELECT * FROM cache_pdf WHERE key_sha = ?", (key_sha,)).fetchone()
    return _row_to_pdf(row) if row is not None else None


def pdf_cache_put(
    db,
    *,
    path: Path | str,
    extraction_json: str,
) -> PDFCacheEntry:
    """Persistiert eine PDF-Extraktion. Key wird aus ``os.stat(path)`` abgeleitet.

    Bei gleichem Key (gleiche mtime+size) wird die bestehende Zeile
    ueberschrieben (keine Duplikate).
    """
    p = Path(path)
    st = os.stat(p)
    key = pdf_cache_key(str(p), st.st_mtime, st.st_size)
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO cache_pdf
                (key_sha, path, mtime, size_bytes, extraction_json, created_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key_sha) DO UPDATE SET
                path = excluded.path,
                mtime = excluded.mtime,
                size_bytes = excluded.size_bytes,
                extraction_json = excluded.extraction_json,
                created_at = CURRENT_TIMESTAMP
            """,
            (key, str(p), st.st_mtime, st.st_size, extraction_json),
        )
    entry = pdf_cache_get_by_key(db, key)
    assert entry is not None  # noqa: S101 — just inserted
    return entry


def pdf_cache_clear(db, *, older_than_iso: str | None = None) -> int:
    """Leert den PDF-Cache (ganz oder alle Eintraege aelter als ``older_than_iso``).

    Returns:
        Anzahl geloeschter Zeilen.
    """
    with db.transaction() as conn:
        if older_than_iso is None:
            cur = conn.execute("DELETE FROM cache_pdf")
        else:
            cur = conn.execute("DELETE FROM cache_pdf WHERE created_at < ?", (older_than_iso,))
        return cur.rowcount or 0
