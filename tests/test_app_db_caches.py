# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer ``qualdatan_core.app_db.caches``."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from qualdatan_core.app_db import AppDB
from qualdatan_core.app_db.caches import (
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


@pytest.fixture()
def db(tmp_path: Path) -> AppDB:
    d = AppDB.open(tmp_path / "app.db")
    yield d
    d.close()


# ---------------------------------------------------------------------------
# Pure key helpers
# ---------------------------------------------------------------------------
def test_llm_cache_key_is_deterministic() -> None:
    a = llm_cache_key("m", "hello", {"t": 0.5})
    b = llm_cache_key("m", "hello", {"t": 0.5})
    assert a == b
    assert len(a) == 64


def test_llm_cache_key_changes_with_model() -> None:
    a = llm_cache_key("m1", "hello", None)
    b = llm_cache_key("m2", "hello", None)
    assert a != b


def test_llm_cache_key_changes_with_prompt() -> None:
    a = llm_cache_key("m", "hello", None)
    b = llm_cache_key("m", "world", None)
    assert a != b


def test_llm_cache_key_changes_with_params() -> None:
    a = llm_cache_key("m", "hello", {"t": 0.5})
    b = llm_cache_key("m", "hello", {"t": 0.7})
    assert a != b


def test_llm_cache_key_stable_under_params_order() -> None:
    a = llm_cache_key("m", "hello", {"a": 1, "b": 2})
    b = llm_cache_key("m", "hello", {"b": 2, "a": 1})
    assert a == b


def test_llm_cache_key_none_vs_empty_params_equal() -> None:
    a = llm_cache_key("m", "p", None)
    b = llm_cache_key("m", "p", {})
    assert a == b


def test_prompt_hash_deterministic() -> None:
    assert prompt_hash("abc") == prompt_hash("abc")
    assert prompt_hash("abc") != prompt_hash("abd")
    assert len(prompt_hash("x")) == 64


def test_pdf_cache_key_stable() -> None:
    a = pdf_cache_key("/tmp/x.pdf", 1700000000.5, 12345)
    b = pdf_cache_key("/tmp/x.pdf", 1700000000.5, 12345)
    assert a == b
    assert a != pdf_cache_key("/tmp/x.pdf", 1700000001.5, 12345)
    assert a != pdf_cache_key("/tmp/x.pdf", 1700000000.5, 99999)
    assert a != pdf_cache_key("/tmp/y.pdf", 1700000000.5, 12345)


# ---------------------------------------------------------------------------
# LLM cache DB ops
# ---------------------------------------------------------------------------
def test_llm_put_get_roundtrip(db: AppDB) -> None:
    entry = llm_cache_put(
        db,
        model="claude-3",
        prompt="hi",
        response="hello",
        params={"t": 0.1},
        tokens_in=10,
        tokens_out=20,
    )
    assert isinstance(entry, LLMCacheEntry)
    got = llm_cache_get(db, entry.key_sha)
    assert got is not None
    assert got.response == "hello"
    assert got.model == "claude-3"
    assert got.tokens_in == 10
    assert got.tokens_out == 20
    assert got.prompt_hash == prompt_hash("hi")


def test_llm_get_missing_returns_none(db: AppDB) -> None:
    assert llm_cache_get(db, "deadbeef" * 8) is None


def test_llm_invalidate(db: AppDB) -> None:
    e = llm_cache_put(db, model="m", prompt="p", response="r")
    llm_cache_invalidate(db, e.key_sha)
    assert llm_cache_get(db, e.key_sha) is None
    # no-op on missing
    llm_cache_invalidate(db, e.key_sha)


def test_llm_clear_all(db: AppDB) -> None:
    llm_cache_put(db, model="m", prompt="a", response="r1")
    llm_cache_put(db, model="m", prompt="b", response="r2")
    n = llm_cache_clear(db)
    assert n == 2
    assert llm_cache_clear(db) == 0


def test_llm_clear_older_than(db: AppDB) -> None:
    e1 = llm_cache_put(db, model="m", prompt="a", response="r1")
    # Bump created_at artificially into the past for e1.
    with db.transaction() as conn:
        conn.execute(
            "UPDATE cache_llm SET created_at = ? WHERE key_sha = ?",
            ("2000-01-01 00:00:00", e1.key_sha),
        )
    e2 = llm_cache_put(db, model="m", prompt="b", response="r2")
    n = llm_cache_clear(db, older_than_iso="2020-01-01 00:00:00")
    assert n == 1
    assert llm_cache_get(db, e1.key_sha) is None
    assert llm_cache_get(db, e2.key_sha) is not None


def test_llm_put_overwrites_same_key(db: AppDB) -> None:
    llm_cache_put(db, model="m", prompt="p", response="r1")
    llm_cache_put(db, model="m", prompt="p", response="r2")
    with db.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM cache_llm").fetchone()[0]
    assert count == 1
    key = llm_cache_key("m", "p", None)
    got = llm_cache_get(db, key)
    assert got is not None and got.response == "r2"


# ---------------------------------------------------------------------------
# PDF cache DB ops
# ---------------------------------------------------------------------------
def _make_pdf(tmp_path: Path, name: str = "doc.pdf", data: bytes = b"%PDF-1.4\n") -> Path:
    p = tmp_path / name
    p.write_bytes(data)
    return p


def test_pdf_put_get_roundtrip(db: AppDB, tmp_path: Path) -> None:
    pdf = _make_pdf(tmp_path)
    entry = pdf_cache_put(db, path=pdf, extraction_json='{"pages": 1}')
    assert isinstance(entry, PDFCacheEntry)
    got = pdf_cache_get(db, pdf)
    assert got is not None
    assert got.extraction_json == '{"pages": 1}'
    assert got.size_bytes == pdf.stat().st_size
    assert got.path == str(pdf)


def test_pdf_get_missing_file(db: AppDB, tmp_path: Path) -> None:
    assert pdf_cache_get(db, tmp_path / "nope.pdf") is None


def test_pdf_get_after_touch_returns_none(db: AppDB, tmp_path: Path) -> None:
    pdf = _make_pdf(tmp_path)
    entry = pdf_cache_put(db, path=pdf, extraction_json="{}")
    # Ensure mtime actually changes.
    new_mtime = pdf.stat().st_mtime + 5.0
    os.utime(pdf, (new_mtime, new_mtime))
    assert pdf_cache_get(db, pdf) is None
    # By-key lookup still finds the stored row (unmutated).
    assert pdf_cache_get_by_key(db, entry.key_sha) is not None


def test_pdf_get_after_size_change_returns_none(db: AppDB, tmp_path: Path) -> None:
    pdf = _make_pdf(tmp_path, data=b"%PDF-1.4\nshort")
    pdf_cache_put(db, path=pdf, extraction_json="{}")
    # Append bytes so size (and likely mtime) change.
    with pdf.open("ab") as fh:
        fh.write(b"more-content-here")
    assert pdf_cache_get(db, pdf) is None


def test_pdf_get_by_key_after_delete(db: AppDB, tmp_path: Path) -> None:
    pdf = _make_pdf(tmp_path)
    entry = pdf_cache_put(db, path=pdf, extraction_json='{"ok": true}')
    pdf.unlink()
    assert pdf_cache_get(db, pdf) is None
    got = pdf_cache_get_by_key(db, entry.key_sha)
    assert got is not None
    assert got.extraction_json == '{"ok": true}'


def test_pdf_put_overwrites_same_key(db: AppDB, tmp_path: Path) -> None:
    pdf = _make_pdf(tmp_path)
    pdf_cache_put(db, path=pdf, extraction_json='{"v": 1}')
    pdf_cache_put(db, path=pdf, extraction_json='{"v": 2}')
    with db.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM cache_pdf").fetchone()[0]
    assert count == 1
    got = pdf_cache_get(db, pdf)
    assert got is not None and got.extraction_json == '{"v": 2}'


def test_pdf_clear_all(db: AppDB, tmp_path: Path) -> None:
    p1 = _make_pdf(tmp_path, "a.pdf", b"AAA")
    p2 = _make_pdf(tmp_path, "b.pdf", b"BBBB")
    pdf_cache_put(db, path=p1, extraction_json="{}")
    pdf_cache_put(db, path=p2, extraction_json="{}")
    assert pdf_cache_clear(db) == 2
    assert pdf_cache_clear(db) == 0


def test_pdf_clear_older_than(db: AppDB, tmp_path: Path) -> None:
    p1 = _make_pdf(tmp_path, "a.pdf", b"AAA")
    p2 = _make_pdf(tmp_path, "b.pdf", b"BBBB")
    e1 = pdf_cache_put(db, path=p1, extraction_json="{}")
    with db.transaction() as conn:
        conn.execute(
            "UPDATE cache_pdf SET created_at = ? WHERE key_sha = ?",
            ("2000-01-01 00:00:00", e1.key_sha),
        )
    e2 = pdf_cache_put(db, path=p2, extraction_json="{}")
    n = pdf_cache_clear(db, older_than_iso="2020-01-01 00:00:00")
    assert n == 1
    assert pdf_cache_get_by_key(db, e1.key_sha) is None
    assert pdf_cache_get_by_key(db, e2.key_sha) is not None


def test_pdf_put_accepts_string_path(db: AppDB, tmp_path: Path) -> None:
    pdf = _make_pdf(tmp_path)
    entry = pdf_cache_put(db, path=str(pdf), extraction_json="{}")
    assert entry.path == str(pdf)
    assert pdf_cache_get(db, str(pdf)) is not None
