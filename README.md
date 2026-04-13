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

## Lizenz

AGPL-3.0-only — siehe [LICENSE](LICENSE). SPDX-Header-Vorlage:
[Umbrella docs/agpl-header.txt](https://github.com/GeneralPawz/Qualdatan/blob/main/docs/agpl-header.txt).

## Kontext

- Architektur-Ueberblick im Umbrella:
  https://github.com/GeneralPawz/Qualdatan
- Phasen-Plan:
  https://github.com/GeneralPawz/Qualdatan/blob/main/.plans/core-split-and-desktop-app.md
- Schwester-Repos:
  [plugins](https://github.com/Qualdatan/plugins),
  [tui](https://github.com/Qualdatan/tui),
  [desktop](https://github.com/Qualdatan/desktop)
