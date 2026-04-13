# SPDX-License-Identifier: AGPL-3.0-only
"""Cross-run Triangulations-DB.

Liest aus den ``pipeline.db`` der einzelnen Runs (siehe
:class:`qualdatan_core.run_context.RunContext`) und akkumuliert
projekt-zentrierte Daten in eine zentrale SQLite-DB. Damit werden
Cross-Run-Vergleiche moeglich, ohne Daten zu halluzinieren.
"""

from .db import (
    LEGACY_COMPANY_NAME,
    TriangulationDB,
    list_run_dirs,
    open_triangulation_db,
    rebuild_from_all_runs,
    update_from_run,
)

__all__ = [
    "LEGACY_COMPANY_NAME",
    "TriangulationDB",
    "list_run_dirs",
    "open_triangulation_db",
    "rebuild_from_all_runs",
    "update_from_run",
]
