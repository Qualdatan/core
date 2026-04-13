# CLAUDE.md — qualdatan-core

## Docs-Policy

Docs sind **nicht optional** und werden **mit dem Code** gepflegt, nicht nachgelagert. Die Site wird automatisch per GitHub Pages unter `https://qualdatan.github.io/core/` veröffentlicht.

### Primäre API-Doku = Docstrings im Code

- Jede Änderung an öffentlicher API → Docstring mitpflegen.
- **Stil**: Google-Docstring (Sections `Args:`, `Returns:`, `Raises:`, `Example:`). Bestehende plain-Docstrings bleiben gültig (mkdocstrings rendert sie), werden bei Anfassen auf Google-Style gehoben.
- **Sprache**: Deutsch ist ok (Konsistenz mit bestehendem Code). Die Section-Marker (`Args:`, `Returns:`, `Raises:`, `Example:`) bleiben **englisch**, sonst erkennt mkdocstrings sie nicht.
- Keine Redundanz: Was der Docstring sagt, wiederholt sich **nicht** in `docs/*.md`.

### Narrative Docs unter `docs/`

Nur übergeordnetes, was nicht zum Code gehört:

- `docs/index.md` — Purpose, Install, Quickstart.
- `docs/architecture.md` — Paket-Aufbau, Facet-Konzept, Abgrenzung zu plugins/tui/desktop.
- `docs/api.md` — mkdocstrings-Direktiven (`::: qualdatan_core.<modul>`), sonst nichts.
- `docs/changelog.md` — Keep-a-Changelog.
- Neue Konzepte → neue MD-Datei + Eintrag in `mkdocs.yml` unter `nav`.

### Lokaler Preview

```bash
pip install -e ".[docs]"
mkdocs serve
```

### Deploy

Automatisch via `.github/workflows/docs.yml` bei Push auf `main`. Pages-Quelle einmalig auf Branch `gh-pages` setzen (Repo-Settings → Pages).
