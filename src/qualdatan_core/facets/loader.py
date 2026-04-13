# SPDX-License-Identifier: AGPL-3.0-only
"""Load Facet instances from YAML files (bundle data).

YAML-Schema (Beispiel ``bim-basic/facets/ifc-elements.yaml``):

```yaml
id: ifc-elements
type: taxonomy
label: "IFC-Bauelemente"
description: "Klassifikation visuell erkennbarer Bauteile nach IfcClass."
input_kinds: [pdf_visual]
codes:
  - { id: IFC-WALL,  label: "Wand" }
  - { id: IFC-SLAB,  label: "Decke" }
prompt_template: |
  ...optional, sonst Default aus types.py...
```

Der Loader weiss, welche Typen es gibt, indem er :func:`discovered_facet_types`
fragt — das schliesst Built-ins und ueber Entry-Points registrierte Plugin-
Typen ein.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any, Mapping

import yaml

from .base import Facet
from .registry import discovered_facet_types


class FacetLoadError(ValueError):
    """Fehler beim Laden eines Facet-YAML."""


def load_facet_from_dict(data: Mapping[str, Any]) -> Facet:
    """Instanziiert einen Facet aus einer geparsten YAML-Mapping-Struktur."""

    if "type" not in data:
        raise FacetLoadError("Facet-YAML fehlt 'type' (z.B. 'taxonomy', 'evidence', ...)")
    if "id" not in data:
        raise FacetLoadError("Facet-YAML fehlt 'id'")

    type_name = data["type"]
    types = discovered_facet_types()
    if type_name not in types:
        raise FacetLoadError(
            f"Unbekannter Facet-Typ '{type_name}'. "
            f"Verfuegbar: {sorted(types)}"
        )
    factory = types[type_name]
    try:
        return factory(data)
    except Exception as exc:
        raise FacetLoadError(
            f"Konnte Facet '{data.get('id')}' (type='{type_name}') nicht "
            f"instanziieren: {exc}"
        ) from exc


def load_facet_from_yaml(path: Path) -> Facet:
    """Liest ein einzelnes Facet-YAML und instanziiert den Facet."""

    with Path(path).open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, Mapping):
        raise FacetLoadError(f"{path}: Top-Level muss ein YAML-Mapping sein")
    return load_facet_from_dict(data)


def load_facets_from_dir(directory: Path) -> list[Facet]:
    """Laedt alle ``*.yaml`` / ``*.yml`` aus einem Verzeichnis (rekursiv)."""

    directory = Path(directory)
    if not directory.exists():
        return []
    found: list[Facet] = []
    for ext in ("*.yaml", "*.yml"):
        for path in sorted(directory.rglob(ext)):
            found.append(load_facet_from_yaml(path))
    return found


def load_facets(paths: Iterable[Path]) -> list[Facet]:
    """Laedt Facets aus einer Mischung von Dateien und Verzeichnissen."""

    out: list[Facet] = []
    for p in paths:
        p = Path(p)
        if p.is_dir():
            out.extend(load_facets_from_dir(p))
        else:
            out.append(load_facet_from_yaml(p))
    return out


__all__ = [
    "FacetLoadError",
    "load_facet_from_dict",
    "load_facet_from_yaml",
    "load_facets_from_dir",
    "load_facets",
]
