# SPDX-License-Identifier: AGPL-3.0-only
"""Qualdatan Facet system.

Facets sind die domain-agnostische Abstraktion, mit der Bundles beliebige
qualitative Analyse-Aufgaben beschreiben (Taxonomie-Klassifikation,
Evidenz-Stufen, Akteursrollen, Prozessschritte, offene Kodierung).
"""

from .base import CodeContribution, Facet, FacetContext, Material
from .loader import (
    FacetLoadError,
    load_facet_from_dict,
    load_facet_from_yaml,
    load_facets,
    load_facets_from_dir,
)
from .registry import (
    EntryPointGroup,
    clear_registry,
    discovered_facet_types,
    get_facet,
    list_facets,
    register_facet,
    register_facets,
    unregister_facet,
)
from .types import (
    ActorRoleFacet,
    EvidenceFacet,
    FreeCodingFacet,
    ProcessStepFacet,
    TaxonomyFacet,
    builtin_facet_types,
)

__all__ = [
    # base
    "Facet",
    "FacetContext",
    "CodeContribution",
    "Material",
    # registry
    "register_facet",
    "register_facets",
    "unregister_facet",
    "get_facet",
    "list_facets",
    "clear_registry",
    "discovered_facet_types",
    "EntryPointGroup",
    # loader
    "FacetLoadError",
    "load_facet_from_dict",
    "load_facet_from_yaml",
    "load_facets_from_dir",
    "load_facets",
    # types
    "TaxonomyFacet",
    "EvidenceFacet",
    "ActorRoleFacet",
    "ProcessStepFacet",
    "FreeCodingFacet",
    "builtin_facet_types",
]
