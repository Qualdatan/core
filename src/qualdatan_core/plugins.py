# SPDX-License-Identifier: AGPL-3.0-only
"""PluginSource-Protocol: schmales Interface, ueber das Core nach Bundles fragt.

Der Core selbst kennt weder Bundles noch den Plugin-Manager. Externe
Konsumenten (``qualdatan-plugins``, Sidecar, TUI) implementieren dieses
Protocol und reichen aktive Bundles an Recipe/Codebook/Facet-Loader weiter.

So bleibt ``qualdatan-core`` unabhaengig vom Plugin-Paket — ein Nutzer kann
die Library ohne Plugin-Manager benutzen und Facets/Codebooks direkt via
:mod:`qualdatan_core.facets.loader` / :mod:`qualdatan_core.recipe` laden.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol, runtime_checkable

from .facets import Facet
from .layouts import FolderLayout


@runtime_checkable
class PluginSource(Protocol):
    """Eine Quelle fuer Bundle-Inhalte (Facets, Codebooks, Methoden, Layouts).

    Implementierungen liefern die **aktiven** Bundles, nicht alle installierten.
    Der Manager in ``qualdatan-plugins`` filtert pro Projekt.
    """

    def iter_facets(self) -> Iterable[Facet]:
        """Alle aktiven Facet-Instanzen."""

    def iter_codebook_paths(self) -> Iterable[Path]:
        """Pfade zu aktiven Codebook-YAMLs (werden ueber ``recipe.py`` geladen)."""

    def iter_method_paths(self) -> Iterable[Path]:
        """Pfade zu aktiven Method-YAMLs."""

    def iter_layouts(self) -> Iterable[FolderLayout]:
        """Aktive Ordner-Layout-Definitionen."""


__all__ = ["PluginSource"]
