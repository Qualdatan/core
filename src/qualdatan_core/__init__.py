# SPDX-License-Identifier: AGPL-3.0-only
"""Qualdatan core library.

UI-freie Primitives fuer qualitative Datenanalyse: PDF-Extraktion,
QDPX-Lesen/Schreiben, LLM-Kodierung, Run-Zustand, Recipe-Loader.

Die konkreten Taxonomien/Codebooks/Methoden (z.B. BIM/IFC/Mayring) leben
nicht hier sondern in Bundles (siehe ``qualdatan-plugins`` und das
Umbrella-Repo).
"""

from .plugins import PluginSource

__version__ = "0.1.0"

__all__ = ["PluginSource", "__version__"]
