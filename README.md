# qualdatan-core

UI-freie Kern-Library der [Qualdatan](https://github.com/GeneralPawz/Qualdatan)-
Pipeline: PDF-Extraktion, QDPX-Schreiben/Lesen, LLM-Kodierung, Run-State,
Recipe-Loader, Code-Farben. Keine CLI, keine GUI, keine domain-spezifische
Logik (die lebt in Bundles, siehe Umbrella-Repo).

**Status**: frueh. Phase 1 hat die Library aus dem Umbrella-Repo extrahiert,
Phase A baut die Subpakete auf, Phase B abstrahiert Facets.

## Install

```bash
pip install qualdatan-core        # wenn 0.1.0 auf PyPI ist
# oder lokal aus dem Umbrella-Workspace:
uv sync
```

## Quickstart

```python
from qualdatan_core.facets.registry import list_facets, register_facet
from qualdatan_core.pdf.extractor import extract_pdf
from qualdatan_core.recipe import load_recipe

# 1) PDF extrahieren (Text + Block-Koordinaten)
extraction = extract_pdf("interview.pdf")
print(f"{len(extraction.blocks)} Text-Bloecke auf {extraction.page_count} Seiten")

# 2) Recipe laden (Codebook + Prompts)
recipe = load_recipe("recipes/mayring.yaml")
print(recipe.categories.keys())

# 3) Facets aufzaehlen (Entry-Points + In-Process-Registrierungen)
for facet in list_facets():
    print(facet.id, facet.label)
```

Vollstaendige API-Referenz: [qualdatan.github.io/core](https://qualdatan.github.io/core/).

## Development

```bash
pip install -e ".[dev,docs]"
pre-commit install         # Ruff lokal vor jedem Commit
pytest                     # Tests
mkdocs serve               # Doku-Preview
```

- **Lint & Format**: `ruff check .` und `ruff format .`
- **Typen (non-blocking)**: `mypy src/qualdatan_core`
- **Tests + Coverage**: `pytest --cov=src/qualdatan_core`

Details zu Style, Docstrings, Tests und Commit-Flow:
[CONTRIBUTING.md](CONTRIBUTING.md).

## Dokumentation

- Live-Site: https://qualdatan.github.io/core/
- Lokaler Preview: `pip install -e ".[docs]" && mkdocs serve`
- Docs-Policy und -Struktur: [CLAUDE.md](CLAUDE.md)

## Kontext

- Architektur-Ueberblick im Umbrella:
  https://github.com/GeneralPawz/Qualdatan
- Phasen-Plan:
  https://github.com/GeneralPawz/Qualdatan/blob/main/.plans/core-split-and-desktop-app.md
- Schwester-Repos:
  [plugins](https://github.com/Qualdatan/plugins),
  [tui](https://github.com/Qualdatan/tui),
  [desktop](https://github.com/Qualdatan/desktop)

## Lizenz

AGPL-3.0-only — siehe [LICENSE](LICENSE). SPDX-Header-Vorlage:
[Umbrella docs/agpl-header.txt](https://github.com/GeneralPawz/Qualdatan/blob/main/docs/agpl-header.txt).
