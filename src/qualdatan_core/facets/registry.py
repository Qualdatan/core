# SPDX-License-Identifier: AGPL-3.0-only
"""Facet registry and discovery.

Drei Quellen werden unterstuetzt:

1. **In-Process-Registrierung** ueber :func:`register_facet` — fuer Tests
   und fuer Code, der Facets direkt instanziiert.
2. **Python-Entry-Points** unter Gruppe ``qualdatan.facet_types``
   (registriert man via ``[project.entry-points."qualdatan.facet_types"]``
   in ``pyproject.toml``) — fuer Plugin-Pakete, die neue *Typen* von Facets
   einbringen (z.B. einen ``GraphFacet``).
3. **Bundle-YAMLs** ueber :class:`qualdatan_core.facets.loader.YamlFacetLoader`
   — fuer deklarative Facets, die sich auf vorhandene Typen stuetzen
   (siehe ``types.py``: TaxonomyFacet, EvidenceFacet, ...). Bundles bringen
   keine eigenen Typen mit, sondern liefern *Konfigurationen* der mitgelieferten
   Typen plus eigener Codes/Prompts.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from importlib.metadata import entry_points

from .base import Facet

# ---------------------------------------------------------------------------
# In-Process-Registry fuer fertige Facet-Instanzen
# ---------------------------------------------------------------------------
_FACETS: dict[str, Facet] = {}


def register_facet(facet: Facet) -> None:
    """Registriert einen Facet im Process-globalen Index."""

    if not isinstance(facet, Facet):  # type: ignore[misc]
        raise TypeError(
            f"register_facet expects an object that satisfies the Facet "
            f"protocol; got {type(facet).__name__}"
        )
    if facet.id in _FACETS:
        raise ValueError(f"Facet '{facet.id}' is already registered")
    _FACETS[facet.id] = facet


def unregister_facet(facet_id: str) -> None:
    """Entfernt einen registrierten Facet (idempotent)."""

    _FACETS.pop(facet_id, None)


def get_facet(facet_id: str) -> Facet:
    """Liefert eine registrierte Facet-Instanz oder wirft KeyError."""

    return _FACETS[facet_id]


def list_facets() -> list[Facet]:
    """Alle aktuell registrierten Facet-Instanzen (stabile Reihenfolge)."""

    return [_FACETS[k] for k in sorted(_FACETS)]


def clear_registry() -> None:
    """Loescht die In-Process-Registry — vor allem fuer Tests."""

    _FACETS.clear()


# ---------------------------------------------------------------------------
# Facet-*Typen* via Entry-Points
# ---------------------------------------------------------------------------
# Plugin-Pakete koennen neue Facet-*Typen* exportieren (Klassen, die ueber YAML
# konfiguriert werden). Bundles greifen ueber den Typ-Namen darauf zu.
#
# Beispiel-pyproject.toml eines Plugin-Pakets:
#
#     [project.entry-points."qualdatan.facet_types"]
#     graph_facet = "myplugin.facets:GraphFacet"
#
# Die Werte sollten *Klassen oder Factory-Callables* sein, die ein Mapping
# (aus YAML geparst) entgegennehmen und einen Facet liefern.
EntryPointGroup = "qualdatan.facet_types"


def discovered_facet_types() -> dict[str, Callable[..., Facet]]:
    """Sammelt registrierte Facet-Typen aus Entry-Points + Built-ins.

    Built-ins (TaxonomyFacet, ...) werden durch ``types.builtin_facet_types``
    geliefert; Drittpakete koennen weitere Typen via Entry-Point hinzufuegen.
    """

    from .types import builtin_facet_types

    types: dict[str, Callable[..., Facet]] = dict(builtin_facet_types())
    for ep in entry_points(group=EntryPointGroup):
        try:
            types[ep.name] = ep.load()
        except Exception as exc:  # pragma: no cover - defensive
            # Wir wollen den Process nicht killen wenn ein einzelnes Plugin
            # bricht; loggen ohne logger-Setup waere zu invasiv hier — die
            # GUI/TUI kann diese Funktion in einen try/except wickeln.
            raise RuntimeError(
                f"Konnte Facet-Typ '{ep.name}' aus Entry-Point '{ep.value}' nicht laden: {exc}"
            ) from exc
    return types


def register_facets(facets: Iterable[Facet]) -> None:
    """Convenience: mehrere Facets auf einmal registrieren."""

    for f in facets:
        register_facet(f)


__all__ = [
    "register_facet",
    "register_facets",
    "unregister_facet",
    "get_facet",
    "list_facets",
    "clear_registry",
    "discovered_facet_types",
    "EntryPointGroup",
]
