# SPDX-License-Identifier: AGPL-3.0-only
"""Smoke tests for qualdatan_core.export.pivot (Phase B.4).

Der Umbrella hatte keine Tests fuer dieses Modul. Wir testen hier nur den
defensiven "keine Codings -> keine Datei"-Fall und die Existenz der
Public-API. Umfassende Tests kommen spaeter, wenn die Pivot-Rezepte aus
Bundle-YAML parametrisiert werden.
"""

from __future__ import annotations

from pathlib import Path

from qualdatan_core.export import build_pivot_excel
from qualdatan_core.export.pivot import COLUMNS
from qualdatan_core.run_context import RunContext


def test_returns_zero_when_no_codings(tmp_path: Path) -> None:
    ctx = RunContext(tmp_path / "run_smoke")
    ctx.ensure_dirs()
    # kein Interview-Flow, keine PDF-Codings -> 0 Zeilen, KEINE Datei
    out = tmp_path / "pivot.xlsx"
    n = build_pivot_excel(ctx, out)
    assert n == 0
    assert not out.exists()


def test_columns_contain_core_dimensions() -> None:
    for required in ("Run", "Quelle", "Code", "Hauptkategorie", "Text"):
        assert required in COLUMNS
