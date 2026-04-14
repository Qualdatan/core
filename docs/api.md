# API Reference

Automatisch aus den Docstrings generiert. Neue Module bitte hier als `:::`-Direktive eintragen.

## Run & Pipeline

### `qualdatan_core.run_context`

::: qualdatan_core.run_context

### `qualdatan_core.db`

::: qualdatan_core.db

### `qualdatan_core.events`

::: qualdatan_core.events

### `qualdatan_core.config`

::: qualdatan_core.config

### `qualdatan_core.config_resolver`

::: qualdatan_core.config_resolver

### `qualdatan_core.recipe`

::: qualdatan_core.recipe

### `qualdatan_core.models`

::: qualdatan_core.models

## App-DB

Die `app_db`-Schicht ist die projekt- und cache-orientierte SQLite-Persistenz.

### Kern

::: qualdatan_core.app_db

### Projects

::: qualdatan_core.app_db.projects

### Codebook

::: qualdatan_core.app_db.codebook

### Codings

::: qualdatan_core.app_db.codings

### Caches (LLM & PDF)

::: qualdatan_core.app_db.caches

### Migrationen

::: qualdatan_core.app_db.migrate

## PDF

### Extraktion

::: qualdatan_core.pdf.extractor

### Annotation

::: qualdatan_core.pdf.annotator

### Scanner

::: qualdatan_core.pdf.scanner

## Coding (LLM)

### Analyzer

::: qualdatan_core.coding.analyzer

### Classifier

::: qualdatan_core.coding.classifier

### Visual

::: qualdatan_core.coding.visual

### Farben

::: qualdatan_core.coding.colors

## QDPX

### Merger (Reader/Writer)

::: qualdatan_core.qdpx.merger

## Steps (Pipeline)

### Step 1 — Analyze

::: qualdatan_core.steps.step1_analyze

### Step 2 — Codebook

::: qualdatan_core.steps.step2_codebook

### Step 3 — QDPX

::: qualdatan_core.steps.step3_qdpx

### Step 4 — Evaluation

::: qualdatan_core.steps.step4_evaluation

## Facets (Erweiterungspunkte)

### Registry

::: qualdatan_core.facets.registry

### Base

::: qualdatan_core.facets.base

### Types

::: qualdatan_core.facets.types

### Loader

::: qualdatan_core.facets.loader

## Layouts

### Folder-Layout

::: qualdatan_core.layouts.folder

## Office

### Konverter

::: qualdatan_core.office.converter

## Export

### Pivot

::: qualdatan_core.export.pivot

## Curation

### Bootstrap

::: qualdatan_core.curation.bootstrap

## Plugins

Entry-Point-Loader fuer optionale Erweiterungen:

::: qualdatan_core.plugins
