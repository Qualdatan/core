# qualdatan-core

UI-freie Kern-Library der [Qualdatan](https://github.com/GeneralPawz/Qualdatan)-Pipeline: PDF-Extraktion, QDPX-Schreiben/Lesen, LLM-Kodierung, Run-State, Recipe-Loader, Code-Farben. Keine CLI, keine GUI, keine domain-spezifische Logik (die lebt in Bundles, siehe Umbrella-Repo).

## Install

```bash
pip install qualdatan-core
```

Oder lokal aus dem Umbrella-Workspace:

```bash
uv sync
```

## Quickstart

```python
from qualdatan_core.facets.registry import list_facets, register_facet

# Facet registrieren (fuer Tests / programmatische Nutzung)
# register_facet(my_facet)

for facet in list_facets():
    print(facet.id)
```

Die vollständige API-Referenz wird aus den Docstrings generiert: siehe [API Reference](api.md).

## Weiter

- [Architecture](architecture.md) — Paketstruktur, Facet-Konzept, Abgrenzung zu Schwester-Repos.
- [Changelog](changelog.md) — Versionsverlauf.
- Schwester-Repos: [plugins](https://github.com/Qualdatan/plugins), [tui](https://github.com/Qualdatan/tui), [desktop](https://github.com/Qualdatan/desktop).

## Lizenz

AGPL-3.0-only — siehe [LICENSE](https://github.com/Qualdatan/core/blob/main/LICENSE).
