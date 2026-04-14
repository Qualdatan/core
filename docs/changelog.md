# Changelog

Alle relevanten Änderungen werden hier dokumentiert.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versionierung: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- Ruff-Lint und `ruff format --check` als CI-Gate.
- Coverage-Messung (`pytest-cov`) und Schwellwert (`--cov-fail-under=60`) im CI.
- mypy-Job im CI (vorerst non-blocking).
- `.pre-commit-config.yaml` fuer lokale Ruff-Prufung.
- `CONTRIBUTING.md` mit Docstring-Style, Ruff-Regeln und Test-Erwartungen.
- `docs/getting-started.md` und `docs/roadmap.md` (+ `mkdocs.yml`-Nav).
- API-Referenz (`docs/api.md`) um alle oeffentlichen Module ergaenzt.
- Google-Style-Docstrings fuer oeffentliche Funktionen in `db`, `recipe`,
  `events`, `config_resolver`, `pdf/*`, `coding/*`, `steps/*`.

### Fixed
- Stale `from src.<modul>` Imports in mehreren Tests auf
  `qualdatan_core.<modul>` umgestellt.
- Dead Code (`do_visual` in `pdf_coder`, `cat_guid` in `qdpx.merger`) entfernt.
- Unbenutzte Loop-Variablen umbenannt (B007).

## [0.1.0] — 2026-04-XX

Initiale Core-Library, extrahiert aus dem Umbrella-Repo.

### Added
- **Phase 1**: Scaffolding des `qualdatan-core` Pakets (pyproject, src-Layout).
- **Phase A.1**: Umbenennung `src/` → `src/qualdatan_core/` mit Package-Wiring.
- **Phase A.2**: Aufsplittung in Subpakete (`pdf`, `qdpx`, `coding`, `llm`,
  `steps`, `facets`).
- **Phase A.3**: `qualdatan_core.events` — UI-neutraler Event-Bus mit
  strukturierten, immutablen Events.
- **Phase B.1**: Facet-Protokoll, Registry und YAML-Loader als
  Erweiterungspunkt; Built-in-Typen.
- **Phase B.2**: Konfigurierbares `FolderLayout`-Subpaket.
- **Phase B.3**: Codebook-Curation nach `qualdatan_core.curation`.
- **Phase B.4**: Pivot-Export nach `qualdatan_core.export`.
- **Phase B.5**: Persistente Triangulation-DB nach
  `qualdatan_core.triangulation` (spaeter in Bundle ausgelagert).
- **Phase B.6**: Orchestrator + Visual-Analyzer in `qualdatan_core`.
- **Phase B.7**: `VisualTaxonomyFacet` und `VisualEvidenceFacet`.
- **Phase C**: Schlanke `PluginSource`-Protokoll fuer den Plugin-Manager.
- **Phase D**: Globale App-DB (`qualdatan_core.app_db`, Schema v1) mit
  Projects, Codebook, Codings, LLM/PDF-Caches und Migrationen.
- **Phase D.2**: `RunContext` an die App-DB angebunden (additiv).
- **Phase D.3**: Callsites an `register_material` angebunden.
- **Phase E**: CI-Welle 0 und Triangulation→Bundle.
- Docs-Setup (MkDocs Material + mkdocstrings), GitHub-Pages-Deployment via
  `.github/workflows/docs.yml`.
- Publish-Workflow fuer TestPyPI.
