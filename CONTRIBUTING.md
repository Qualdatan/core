# Contributing zu qualdatan-core

Danke, dass du mitmachen willst. Diese Datei fasst die wichtigsten
Entwicklungs-Konventionen zusammen. Die Docs-Policy steht in
[CLAUDE.md](CLAUDE.md); die Architektur in
[docs/architecture.md](docs/architecture.md).

## Setup

```bash
git clone https://github.com/Qualdatan/core.git
cd core
pip install -e ".[dev,docs]"
pre-commit install
```

## Style

- **Python-Version**: `>=3.11` (wir nutzen `X | Y`-Unions, `StrEnum`-freie
  Enums aus Kompat-Gruenden).
- **Formatter & Linter**: [ruff](https://docs.astral.sh/ruff/). Regeln stehen
  in `pyproject.toml` unter `[tool.ruff]`. Lokal vor dem Commit laeuft
  `pre-commit run --all-files`.
- **Line length**: 100 Zeichen (ruff-Formatter kuemmert sich).
- **Import-Order**: ruff `I` (entspricht isort-Profil `black`).

## Docstrings

Siehe auch [CLAUDE.md](CLAUDE.md):

- **Stil**: Google-Docstring (`Args:`, `Returns:`, `Raises:`, `Example:`).
- **Sprache**: Deutsch ist ok; die Section-Marker bleiben **englisch**,
  sonst erkennt mkdocstrings sie nicht.
- Jede neue oeffentliche Funktion/Klasse bekommt eine Kurz-Doku.
- Narrative-Texte gehoeren nach `docs/`, nicht als Block-Docstring.

## Tests

- Framework: `pytest`.
- Schreibe Tests fuer neue oeffentliche Funktionen, auch wenn sie klein sind.
- Slow/LLM-Tests bekommen `@pytest.mark.slow` und werden im Standard-Run
  uebersprungen.
- Coverage-Schwelle im CI: `--cov-fail-under=60` (steigt mit der Zeit).

```bash
pytest                           # alle Tests
pytest -m "not slow"             # ohne LLM-Calls
pytest --cov=src/qualdatan_core  # mit Coverage
```

## Typen

Mypy laeuft im CI **non-blocking**. Neue Funktionen sollten aber Typen
tragen; Ziel ist perspektivisch `disallow_untyped_defs = true`.

```bash
mypy src/qualdatan_core
```

## Commit-Nachrichten

Konvention bisher: kurze, aktive Zeile im Imperativ, optional mit
Phase-Praefix (z.B. `Phase D.3: ...`). Beispiele siehe `git log`.

## Pull Requests

1. Branch vom aktuellen `main`.
2. Tests + Lint lokal gruen (`pre-commit run --all-files && pytest`).
3. Docstring/Changelog mitpflegen, wenn die PR die API anfasst.
4. Ein PR = ein Thema. Kleine, nachvollziehbare Commits werden bevorzugt.

## SPDX-Header

Jede neue Quelldatei beginnt mit:

```python
# SPDX-License-Identifier: AGPL-3.0-only
```

Vorlage:
[Umbrella docs/agpl-header.txt](https://github.com/GeneralPawz/Qualdatan/blob/main/docs/agpl-header.txt).

## Fragen

Issues in [Qualdatan/core](https://github.com/Qualdatan/core/issues).
