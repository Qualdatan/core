# Getting Started

Diese Seite fuehrt dich in 10 Minuten durch die wichtigsten Bausteine von
`qualdatan-core`. Fuer Details siehe die [API-Referenz](api.md).

## 1. Installation

```bash
pip install qualdatan-core
# oder lokal aus dem Umbrella:
uv sync
```

Dev-Setup fuer Contributions: siehe
[CONTRIBUTING](https://github.com/Qualdatan/core/blob/main/CONTRIBUTING.md).

## 2. PDF extrahieren

Das `pdf`-Subpaket liefert textliche und visuelle Rohdaten:

```python
from qualdatan_core.pdf.extractor import extract_pdf

extraction = extract_pdf("interview.pdf")
print(extraction.page_count, "Seiten")
for block in extraction.blocks[:3]:
    print(block.page, block.bbox, block.text[:80])
```

- `extraction.blocks` liefert Text-Bloecke mit Koordinaten (fuer spaetere
  Annotation).
- `extraction.text` ist der konkatinierte Plaintext.

## 3. Recipes laden

Recipes definieren Codebook + Prompt-Vorlagen im YAML:

```python
from qualdatan_core.recipe import load_recipe

recipe = load_recipe("recipes/mayring.yaml")
print(list(recipe.categories.keys()))
print(recipe.coding_strategy)  # "strict" | "hybrid" | "free"
```

## 4. Run-Verzeichnis + State-DB

Ein `RunContext` buendelt alle Zwischenstaende eines Pipeline-Durchlaufs in
einem Ordner inkl. SQLite:

```python
from pathlib import Path
from qualdatan_core.run_context import RunContext

ctx = RunContext(Path("output/run-001"))
ctx.ensure_dirs()
ctx.init_state("mayring", None, ["interview.pdf"])

# Status-Flags, Caches, Extraktionen in ctx.db
assert not ctx.is_step_done(1)
```

Details: [`qualdatan_core.run_context`](api.md#qualdatan_corerun_context).

## 5. Facets registrieren und auflisten

Facets sind die Erweiterungspunkte fuer LLM-Analysen. Drei Quellen:
In-Process (`register_facet`), Entry-Points und Bundle-YAMLs.

```python
from qualdatan_core.facets.registry import list_facets, register_facet
from qualdatan_core.facets.types import TaxonomyFacet
from qualdatan_core.facets.base import Material

facet = TaxonomyFacet(
    id="sentiment",
    label="Sentiment",
    input_kinds=(Material.TEXT,),
    codebook_contribution=(),
)
register_facet(facet)

for f in list_facets():
    print(f.id, f.label)
```

Siehe [Architecture → Facets](architecture.md#facets).

## 6. Events abfangen

Fuer UI/SSE-Anbindung emittiert der Core strukturierte Events ueber einen
`EventBus`:

```python
from qualdatan_core.events import EventBus, StageProgress

bus = EventBus()
seen = []
bus.subscribe(seen.append)
bus.emit(StageProgress(stage="extract", done=3, total=12))
print(len(seen), "Events empfangen")
```

## 7. App-DB (optional)

Wer projekt- und cache-uebergreifend arbeitet, nutzt die globale App-DB:

```python
from qualdatan_core.app_db import AppDB

with AppDB.open() as app_db:
    print("Schema", app_db.schema_version)
```

## Naechste Schritte

- [Architecture](architecture.md) — Paketgrenzen und Facet-Konzept.
- [API Reference](api.md) — alle oeffentlichen Module.
- [Roadmap](roadmap.md) — aktuelle Phasen-Uebersicht.
