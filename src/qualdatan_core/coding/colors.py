"""Deterministische Farbpalette fuer qualitative Codes (A-01, L-03, O-02, ...).

Zweck:
  Mappt Code-IDs auf RGB-Farben, die stabil ueber Runs hinweg sind und
  beim Annotieren von PDFs (pymupdf) sowie beim Import in MAXQDA
  pro Code einen distinktiven Farbton liefern.

Strategie (Default):
  - Jede Hauptkategorie (A, B, ..., Z) erhaelt einen Basis-Hue auf einem
    stabilen HSV-Rad (Goldener-Schnitt-Offset).
  - Subcodes einer Kategorie (A-01, A-02, ...) teilen den Hue und
    variieren nur Value/Saturation, damit die Kategorie visuell
    erkennbar bleibt.
  - Die Hauptkategorie selbst ("A", "B", ...) erhaelt die saturierteste
    Variante; Subcodes werden stufenweise heller.
  - Reihenfolge der Eingabe ist egal: intern wird nach (Kategorie,
    Subcode-Nummer) sortiert, damit neue Codes bestehende Farben nicht
    verschieben.

Keine pymupdf-Abhaengigkeit, kein globaler State, kein CLI.
"""

from __future__ import annotations

import colorsys
import hashlib
import logging
import re
from pathlib import Path

import yaml

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Konstanten
# ---------------------------------------------------------------------------

# Goldener Schnitt - erzeugt gut verteilte Hues fuer aufeinanderfolgende
# Kategorieletter (A->B->C liefert weit voneinander entfernte Farbtoene).
_GOLDEN_RATIO_CONJ = 0.6180339887498949

# Shade-Rampen fuer Subcodes (Value, Saturation). Index 0 = Hauptkategorie.
# Bis zu ~20 Subcodes visuell unterscheidbar durch Kombination der Rampen.
_SHADE_STEPS: tuple[tuple[float, float], ...] = (
    (0.85, 0.85),  # Hauptkategorie - saturiert, mittel-dunkel
    (0.95, 0.55),  # hell, weniger saturiert
    (0.70, 0.90),  # dunkel, saturiert
    (1.00, 0.35),  # sehr hell (pastell)
    (0.55, 0.80),  # sehr dunkel
    (0.90, 0.70),
    (0.80, 0.45),
    (0.65, 0.60),
    (1.00, 0.55),
    (0.50, 0.95),
    (0.95, 0.30),
    (0.75, 0.75),
    (0.60, 0.40),
    (0.90, 0.95),
    (0.85, 0.25),
    (0.70, 0.55),
    (1.00, 0.75),
    (0.55, 0.50),
    (0.80, 0.90),
    (0.65, 0.25),
    (0.95, 0.85),
)

_HEX_RE = re.compile(r"^#([0-9A-Fa-f]{6})$")
_CODE_RE = re.compile(r"^([A-Za-z])(?:-(\d+))?$")


# ---------------------------------------------------------------------------
# Hex/RGB Helpers
# ---------------------------------------------------------------------------


def _rgb_to_hex(rgb: tuple[float, float, float]) -> str:
    """Konvertiert (r, g, b) im Bereich 0..1 zu '#RRGGBB'."""
    r, g, b = rgb
    ri = max(0, min(255, int(round(r * 255))))
    gi = max(0, min(255, int(round(g * 255))))
    bi = max(0, min(255, int(round(b * 255))))
    return f"#{ri:02X}{gi:02X}{bi:02X}"


def _hex_to_rgb(hex_str: str) -> tuple[float, float, float]:
    """Konvertiert '#RRGGBB' zu (r, g, b) im Bereich 0..1.

    Raises:
        ValueError: wenn der String kein gueltiges #RRGGBB-Format hat.
    """
    if not isinstance(hex_str, str):
        raise ValueError(f"Hex-Wert muss String sein, erhalten: {type(hex_str).__name__}")
    m = _HEX_RE.match(hex_str.strip())
    if not m:
        raise ValueError(f"Ungueltiges Hex-Format: {hex_str!r} (erwartet '#RRGGBB')")
    n = int(m.group(1), 16)
    return ((n >> 16) / 255.0, ((n >> 8) & 0xFF) / 255.0, (n & 0xFF) / 255.0)


# ---------------------------------------------------------------------------
# Code-Parsing
# ---------------------------------------------------------------------------


def _parse_code(code_id: str) -> tuple[str, int] | None:
    """Zerlegt einen Code in (Kategorie, Subcode-Nummer).

    Beispiele:
        'A-01' -> ('A', 1)
        'L'    -> ('L', 0)   # Hauptkategorie
        'foo'  -> None
    """
    if not isinstance(code_id, str):
        return None
    m = _CODE_RE.match(code_id.strip())
    if not m:
        return None
    letter = m.group(1).upper()
    num = int(m.group(2)) if m.group(2) is not None else 0
    return (letter, num)


def _category_hue(letter: str) -> float:
    """Liefert einen stabilen Hue (0..1) fuer einen Kategorieletter."""
    idx = ord(letter.upper()) - ord("A")
    # Golden-Ratio-Distribution verteilt Nachbarletter maximal.
    return (idx * _GOLDEN_RATIO_CONJ) % 1.0


def _shade_for_index(n: int) -> tuple[float, float]:
    """Liefert (Value, Saturation) fuer die n-te Shade-Stufe (0 = Hauptkat)."""
    return _SHADE_STEPS[n % len(_SHADE_STEPS)]


def _fallback_rgb(code_id: str) -> tuple[float, float, float]:
    """Deterministischer Fallback fuer unbekannte/ungueltige Codes (Hash-basiert)."""
    h = hashlib.sha1(code_id.encode("utf-8")).digest()
    hue = h[0] / 255.0
    sat = 0.55 + (h[1] / 255.0) * 0.35  # 0.55..0.90
    val = 0.65 + (h[2] / 255.0) * 0.30  # 0.65..0.95
    return colorsys.hsv_to_rgb(hue, sat, val)


# ---------------------------------------------------------------------------
# CodeColorMap
# ---------------------------------------------------------------------------


class CodeColorMap:
    """Weist qualitativen Codes deterministische Farben zu.

    Attribute (nach __init__ befuellt):
        codes:            sortierte Liste aller eindeutigen Code-IDs
        colors:           dict[code_id] -> (r, g, b) im Bereich 0..1
    """

    def __init__(
        self,
        codes: list[str],
        overrides: dict[str, str] | None = None,
        code_names: dict[str, str] | None = None,
        category_names: dict[str, str] | None = None,
    ):
        """Erzeugt eine deterministische Farbzuordnung fuer ``codes``.

        Args:
            codes: Liste von Code-IDs (Duplikate und Nicht-Strings werden
                ignoriert).
            overrides: Optionale expliziter ``code_id -> "#RRGGBB"``-Map.
            code_names: Anzeige-Namen fuer Codes (nur fuer UI-Export).
            category_names: Anzeige-Namen fuer Kategorien.
        """
        self.code_names: dict[str, str] = dict(code_names or {})
        self.category_names: dict[str, str] = dict(category_names or {})
        self._overrides_raw: dict[str, str] = dict(overrides or {})

        # Eindeutige Codes in stabiler Reihenfolge sammeln
        unique: list[str] = []
        seen: set[str] = set()
        for c in codes:
            if not isinstance(c, str):
                continue
            c = c.strip()
            if not c or c in seen:
                continue
            seen.add(c)
            unique.append(c)

        # Deterministische Sortierung: Kategorie, dann Nummer, dann Original.
        def sort_key(c: str) -> tuple[int, str, int, str]:
            """Sortier-Schluessel: parsebare Codes zuerst, dann nach Kategorie/Nr."""
            parsed = _parse_code(c)
            if parsed is None:
                return (1, "", 0, c)  # Unparsebar ans Ende
            letter, num = parsed
            return (0, letter, num, c)

        self.codes: list[str] = sorted(unique, key=sort_key)
        self.colors: dict[str, tuple[float, float, float]] = {}

        self._build_palette()
        self._apply_overrides()

    # --- Palette-Erzeugung ------------------------------------------------

    def _build_palette(self) -> None:
        """Erzeugt die automatische HSV-Palette aus den Codes."""
        # Codes nach Kategorie gruppieren; innerhalb Kategorie nach Nummer.
        by_cat: dict[str, list[tuple[int, str]]] = {}
        unparseable: list[str] = []
        for c in self.codes:
            parsed = _parse_code(c)
            if parsed is None:
                unparseable.append(c)
                continue
            letter, num = parsed
            by_cat.setdefault(letter, []).append((num, c))

        for letter, entries in by_cat.items():
            # Stabile Reihenfolge: Hauptkategorie (num=0) zuerst,
            # dann Subcodes aufsteigend.
            entries.sort(key=lambda t: (t[0], t[1]))
            hue = _category_hue(letter)
            for i, (_num, code_id) in enumerate(entries):
                val, sat = _shade_for_index(i)
                self.colors[code_id] = colorsys.hsv_to_rgb(hue, sat, val)

        # Fallback fuer unparsebare Codes
        for c in unparseable:
            self.colors[c] = _fallback_rgb(c)

    def _apply_overrides(self) -> None:
        """Ueberschreibt Palette mit manuellen Hex-Werten aus dem YAML."""
        for key, hex_val in self._overrides_raw.items():
            if not isinstance(key, str):
                continue
            if key not in self.colors:
                log.debug("Override fuer unbekannten Code ignoriert: %s", key)
                continue
            try:
                self.colors[key] = _hex_to_rgb(hex_val)
            except ValueError as e:
                log.warning("Ungueltige Override-Farbe fuer %s: %s", key, e)

    # --- Public API -------------------------------------------------------

    def get_rgb(self, code_id: str) -> tuple[float, float, float]:
        """RGB-Tuple im Bereich 0..1 (fuer pymupdf set_colors stroke=...)."""
        if code_id in self.colors:
            return self.colors[code_id]
        # Fallback fuer unbekannte Codes - nicht cachen, damit die Map
        # rein die konfigurierten Codes enthaelt.
        return _fallback_rgb(str(code_id))

    def get_hex(self, code_id: str) -> str:
        """Hex-String '#RRGGBB' fuer den Code."""
        return _rgb_to_hex(self.get_rgb(code_id))

    def to_dict(self) -> dict:
        """Serialisiert die Map in ein dict-freundliches Format."""
        out_codes = []
        for code_id in self.codes:
            parsed = _parse_code(code_id)
            hauptkategorie = parsed[0] if parsed else ""
            rgb = self.colors.get(code_id, _fallback_rgb(code_id))
            out_codes.append(
                {
                    "code_id": code_id,
                    "code_name": self.code_names.get(code_id, ""),
                    "hauptkategorie": hauptkategorie,
                    "kategorie_name": self.category_names.get(hauptkategorie, ""),
                    "color_hex": _rgb_to_hex(rgb),
                    "color_rgb": [round(v, 6) for v in rgb],
                }
            )
        return {"codes": out_codes}

    def to_markdown(self) -> str:
        """Markdown-Tabelle, sortiert nach Kategorie und Code."""
        lines = [
            "| Hex | Code-ID | Code-Name | Hauptkategorie |",
            "| --- | --- | --- | --- |",
        ]
        for code_id in self.codes:
            parsed = _parse_code(code_id)
            hauptkategorie = parsed[0] if parsed else ""
            hex_val = self.get_hex(code_id)
            name = self.code_names.get(code_id, "")
            lines.append(f"| `{hex_val}` | {code_id} | {name} | {hauptkategorie} |")
        return "\n".join(lines)

    # --- Factories --------------------------------------------------------

    @classmethod
    def from_yaml(
        cls,
        yaml_path: Path,
        codes: list[str],
        **kwargs,
    ) -> CodeColorMap:
        """Laedt Overrides aus einer YAML-Datei und erzeugt eine Map.

        Das YAML-Schema ist:
            colors:
              A-01: "#FF6B6B"
              L:    "#0033CC"

        Unbekannte Eintraege werden ignoriert (optional geloggt).
        """
        yaml_path = Path(yaml_path)
        overrides: dict[str, str] = {}
        if yaml_path.exists():
            with yaml_path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            raw = data.get("colors") if isinstance(data, dict) else None
            if isinstance(raw, dict):
                for k, v in raw.items():
                    if isinstance(k, str) and isinstance(v, str):
                        overrides[k] = v
        else:
            log.warning("YAML-Override-Datei nicht gefunden: %s", yaml_path)

        return cls(codes=codes, overrides=overrides, **kwargs)
