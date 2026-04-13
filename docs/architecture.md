# Architecture

`qualdatan-core` ist die UI-freie Kernbibliothek. Alles, was **nicht** Kern ist (CLI, Desktop-GUI, Plugin-Management, Domain-Daten), lebt in Schwester-Repos.

## Paketstruktur

Der importierbare Namespace ist `qualdatan_core` (`src/qualdatan_core/`). Subpakete werden inkrementell in Phase A.2 aufgebaut:

- `pdf/` — PDF-Extraktion, Annotation-Schreiben (Single-Color-Regel).
- `qdpx/` — REFI-QDA-Reader/Writer.
- `steps/` — Kodier-Pipeline-Steps (MAXQDA-Export).
- `coding/` — LLM-Kodierung (Anthropic).
- `llm/` — Thin Client über `anthropic`.
- `facets/` — Abstraktion für Analyse-Bausteine; siehe unten.

## Facets

Facets sind die Erweiterungspunkte. Drei Quellen werden unterstützt (Details in `qualdatan_core.facets.registry`):

1. **In-Process-Registrierung** — `register_facet()` für Tests und direkt instanziierte Facets.
2. **Python-Entry-Points** (Gruppe `qualdatan.facet_types`) — für Plugin-Pakete, die neue *Typen* einbringen.
3. **Bundle-YAMLs** (via `qualdatan_core.facets.loader.YamlFacetLoader`) — deklarative Facets auf Basis vorhandener Typen.

## Abgrenzung

| Repo | Verantwortlich für |
|------|--------------------|
| **core** (dieses Repo) | Primitives: PDF, QDPX, LLM-Kodierung, Facet-Basis |
| [plugins](https://github.com/Qualdatan/plugins) | Discovery, Install, Verify von Bundles (Tap-Style) |
| [tui](https://github.com/Qualdatan/tui) | Typer/Rich CLI, orchestriert core + plugins |
| [desktop](https://github.com/Qualdatan/desktop) | Tauri-Shell + Python-Sidecar |

## Lizenz & SPDX

AGPL-3.0-only. Jede neue Quelldatei beginnt mit:

```python
# SPDX-License-Identifier: AGPL-3.0-only
```

Siehe [Umbrella-Vorlage](https://github.com/GeneralPawz/Qualdatan/blob/main/docs/agpl-header.txt).
