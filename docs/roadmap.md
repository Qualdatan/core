# Roadmap

Grobe Phasen-Uebersicht der Core-Entwicklung. Detaillierter Phasen-Plan
im Umbrella:
[core-split-and-desktop-app.md](https://github.com/GeneralPawz/Qualdatan/blob/main/.plans/core-split-and-desktop-app.md).

## Status (Stand aktueller Hauptzweig)

| Phase | Thema | Status |
|------:|-------|--------|
| 1    | Extraktion aus Umbrella-Repo, Scaffolding | :material-check: |
| A.1  | `src/` -> `src/qualdatan_core/`, Package-Wiring | :material-check: |
| A.2  | Subpakete (`pdf`, `qdpx`, `coding`, `llm`, `steps`, `facets`) | :material-check: |
| A.3  | `events`-Modul (UI-neutrale Events) | :material-check: |
| B.1  | Facet-Protocol, Registry, YAML-Loader, Built-in-Typen | :material-check: |
| B.2  | Konfigurierbares `FolderLayout` | :material-check: |
| B.3  | Codebook-Curation (`qualdatan_core.curation`) | :material-check: |
| B.4  | Pivot-Export (`qualdatan_core.export`) | :material-check: |
| B.5  | Triangulation-DB (spaeter nach Bundle ausgelagert) | :material-check: |
| B.6  | Orchestrator + Visual-Analyzer | :material-check: |
| B.7  | `VisualTaxonomyFacet` + `VisualEvidenceFacet` | :material-check: |
| C    | `PluginSource`-Protokoll | :material-check: |
| D    | Globale App-DB (`app_db`, Schema v1) | :material-check: |
| D.2  | `RunContext` an App-DB gebunden | :material-check: |
| D.3  | Callsites an `register_material` gebunden | :material-check: |
| E    | CI-Welle 0, Triangulation → Bundle | :material-check: |
| F    | PyPI-Release, stabile Public API | :material-dots-horizontal: geplant |

## Vor v1.0 (naechste Schritte)

- **CI-Gates verschaerfen**: mypy von `continue-on-error` auf blocking
  heben, sobald Baseline sauber ist (aktuell ~80 Fehler).
- **Docstring-Coverage auf 90 %+** (aktuell ~81 %). Ruff-Regel `D`
  aktivieren, sobald private Helfer abgedeckt sind.
- **Test-Coverage auf >= 75 %** (aktuell ~65 %). Fokus: `steps/step3`,
  `steps/step4`, `export/pivot`.
- **PyPI-Publish** (derzeit nur TestPyPI; `publish.yml` hat den realen
  Job unter `if: false`).
- **Public-API-Freeze**: `__all__` in allen Subpaketen finalisieren,
  Breaking-Changes-Policy dokumentieren.

## Langfristig

- Domain-freies Schema v2 (Multi-Tenancy, Vector-Index fuer Semantische
  Suche).
- Async-Bus fuer Event-Fanout zu mehreren Subscribern (TUI + Desktop
  gleichzeitig).
- Headless-Bundle-Runner (core + plugins, ohne TUI/Desktop).
