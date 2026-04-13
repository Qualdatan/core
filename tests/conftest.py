# SPDX-License-Identifier: AGPL-3.0-only
"""Test-Konfiguration fuer qualdatan-core.

Setzt METHODS_DIR und CODEBASES_DIR auf die Test-Fixtures **bevor**
``qualdatan_core.config`` importiert wird, weil dort die Pfade auf
Modulebene aufgeloest werden.
"""

from __future__ import annotations

import os
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"

os.environ.setdefault("METHODS_DIR", str(FIXTURES / "methods"))
os.environ.setdefault("CODEBASES_DIR", str(FIXTURES / "codebases"))
