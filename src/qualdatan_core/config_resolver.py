# SPDX-License-Identifier: AGPL-3.0-only
"""3-Ebenen-Resolver fuer Code-Metadaten (Phase E Config-Resolver).

Praezedenz (von oben nach unten):

1. **run_config** — der aktuell laufende Run kann Codes ueberschreiben
   (hoechste Prioritaet, volatil, nur fuer diesen Run).
2. **Projekt-Codebook** (App-DB ``codebook_entries``) — persistente
   Per-Projekt-Overrides via :mod:`qualdatan_core.app_db.codebook`.
3. **Bundle-Default** — der Wert aus dem aktuellen Methoden-/Codebook-
   Bundle (``label``, ``color``, ``definition``, ``examples``).
4. **Generator-Fallback** — *nur* fuer Farben: deterministischer
   HSV/Golden-Ratio-Ton via :class:`CodeColorMap`. Andere Felder fallen
   auf ``""`` / ``[]`` zurueck.

Die Resolver sind tolerant: fehlt eine Quelle (``run_config=None``,
``project_id=None``, ``app_db=None``, ``bundle_default=None``), wird sie
uebersprungen.

Example:
    >>> resolve_color("A-01", project_id=1, run_config=None, app_db=db,
    ...               bundle_default="#FF0000")
    '#FF0000'
"""

from __future__ import annotations

from typing import Any, Mapping

from .coding.colors import CodeColorMap


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _run_config_value(
    run_config: Mapping[str, Any] | None, code_id: str, key: str
) -> Any:
    """Liest ``run_config['codes'][code_id][key]`` tolerant."""
    if not isinstance(run_config, Mapping):
        return None
    codes = run_config.get("codes")
    if not isinstance(codes, Mapping):
        return None
    entry = codes.get(code_id)
    if not isinstance(entry, Mapping):
        return None
    return entry.get(key)


def _db_entry(app_db, project_id: int | None, code_id: str):
    """Liest den Codebook-Eintrag aus der DB, falls DB+Projekt vorhanden."""
    if app_db is None or project_id is None:
        return None
    # Lazy-Import, um Zyklen zu vermeiden.
    from .app_db.codebook import get_codebook_entry

    return get_codebook_entry(app_db, project_id, code_id)


def _nonempty_str(v: Any) -> str | None:
    if isinstance(v, str) and v != "":
        return v
    return None


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------
def resolve_color(
    code_id: str,
    *,
    project_id: int | None = None,
    run_config: Mapping[str, Any] | None = None,
    app_db=None,
    bundle_default: str | None = None,
) -> str:
    """Loest die Farbe fuer ``code_id`` nach der 3-Ebenen-Praezedenz auf.

    Fallback: deterministischer Hex-Wert aus :class:`CodeColorMap`.

    Returns:
        Hex-Farbcode als ``"#RRGGBB"``.
    """
    v = _nonempty_str(_run_config_value(run_config, code_id, "color"))
    if v is not None:
        return v
    entry = _db_entry(app_db, project_id, code_id)
    if entry is not None and _nonempty_str(entry.color_override) is not None:
        return entry.color_override  # type: ignore[return-value]
    if _nonempty_str(bundle_default) is not None:
        return bundle_default  # type: ignore[return-value]
    return CodeColorMap(codes=[code_id]).get_hex(code_id)


def resolve_label(
    code_id: str,
    *,
    project_id: int | None = None,
    run_config: Mapping[str, Any] | None = None,
    app_db=None,
    bundle_default: str | None = None,
) -> str:
    """Loest das Label fuer ``code_id`` auf.

    Returns:
        Gefundenes Label oder ``""`` wenn keine Quelle etwas liefert.
    """
    v = _nonempty_str(_run_config_value(run_config, code_id, "label"))
    if v is not None:
        return v
    entry = _db_entry(app_db, project_id, code_id)
    if entry is not None and _nonempty_str(entry.label_override) is not None:
        return entry.label_override  # type: ignore[return-value]
    if _nonempty_str(bundle_default) is not None:
        return bundle_default  # type: ignore[return-value]
    return ""


def resolve_definition(
    code_id: str,
    *,
    project_id: int | None = None,
    run_config: Mapping[str, Any] | None = None,
    app_db=None,
    bundle_default: str | None = None,
) -> str:
    """Loest die Definition fuer ``code_id`` auf (``""`` als Fallback)."""
    v = _nonempty_str(_run_config_value(run_config, code_id, "definition"))
    if v is not None:
        return v
    entry = _db_entry(app_db, project_id, code_id)
    if (
        entry is not None
        and _nonempty_str(entry.definition_override) is not None
    ):
        return entry.definition_override  # type: ignore[return-value]
    if _nonempty_str(bundle_default) is not None:
        return bundle_default  # type: ignore[return-value]
    return ""


def resolve_examples(
    code_id: str,
    *,
    project_id: int | None = None,
    run_config: Mapping[str, Any] | None = None,
    app_db=None,
    bundle_default: list[str] | None = None,
) -> list[str]:
    """Loest die Beispiel-Liste fuer ``code_id`` auf (``[]`` als Fallback)."""
    v = _run_config_value(run_config, code_id, "examples")
    if isinstance(v, list) and len(v) > 0:
        return [str(x) for x in v]
    entry = _db_entry(app_db, project_id, code_id)
    if (
        entry is not None
        and entry.examples_override is not None
        and len(entry.examples_override) > 0
    ):
        return list(entry.examples_override)
    if isinstance(bundle_default, list) and len(bundle_default) > 0:
        return [str(x) for x in bundle_default]
    return []


__all__ = [
    "resolve_color",
    "resolve_label",
    "resolve_definition",
    "resolve_examples",
]
