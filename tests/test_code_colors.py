"""Tests fuer die Farb-Palette (src/code_colors.py)."""

import colorsys
import pytest
import yaml

from qualdatan_core.coding.colors import (
    CodeColorMap,
    _hex_to_rgb,
    _rgb_to_hex,
)


# ---------------------------------------------------------------------------
# Hex/RGB Helpers
# ---------------------------------------------------------------------------

class TestHexRgbHelpers:
    def test_rgb_to_hex_basic(self):
        assert _rgb_to_hex((1.0, 0.0, 0.0)) == "#FF0000"
        assert _rgb_to_hex((0.0, 1.0, 0.0)) == "#00FF00"
        assert _rgb_to_hex((0.0, 0.0, 1.0)) == "#0000FF"
        assert _rgb_to_hex((0.0, 0.0, 0.0)) == "#000000"
        assert _rgb_to_hex((1.0, 1.0, 1.0)) == "#FFFFFF"

    def test_hex_to_rgb_basic(self):
        r, g, b = _hex_to_rgb("#FF0000")
        assert r == 1.0
        assert g == 0.0
        assert b == 0.0

    def test_hex_case_insensitive(self):
        assert _hex_to_rgb("#ff6b6b") == _hex_to_rgb("#FF6B6B")

    def test_hex_invalid_format_raises(self):
        with pytest.raises(ValueError):
            _hex_to_rgb("FF0000")       # kein '#'
        with pytest.raises(ValueError):
            _hex_to_rgb("#FFF")          # zu kurz
        with pytest.raises(ValueError):
            _hex_to_rgb("#GGGGGG")       # ungueltige Zeichen
        with pytest.raises(ValueError):
            _hex_to_rgb("#FF00000")      # zu lang

    def test_hex_non_string_raises(self):
        with pytest.raises(ValueError):
            _hex_to_rgb(12345)  # type: ignore[arg-type]

    def test_round_trip(self):
        """_rgb_to_hex(_hex_to_rgb(x)) == x fuer gueltige Hex-Werte."""
        for hex_val in ["#FF6B6B", "#0033CC", "#123456", "#ABCDEF", "#000000", "#FFFFFF"]:
            assert _rgb_to_hex(_hex_to_rgb(hex_val)) == hex_val


# ---------------------------------------------------------------------------
# HSV-Palette: Kategorien und Subcodes
# ---------------------------------------------------------------------------

class TestHsvPalette:
    def test_same_category_similar_hue(self):
        """Codes einer Kategorie haben (nahezu) denselben Hue."""
        cmap = CodeColorMap(["A-01", "A-02", "A-03", "A-04"])
        hues = []
        for c in ["A-01", "A-02", "A-03", "A-04"]:
            r, g, b = cmap.get_rgb(c)
            h, _, _ = colorsys.rgb_to_hsv(r, g, b)
            hues.append(h)
        # Hues sollten fast identisch sein (modulo 1).
        for h in hues[1:]:
            diff = min(abs(h - hues[0]), 1.0 - abs(h - hues[0]))
            assert diff < 1e-6

    def test_same_category_different_shades(self):
        """Gleiche Kategorie, aber unterschiedliche Farben (Shade variiert)."""
        cmap = CodeColorMap(["A-01", "A-02", "A-03"])
        hexes = {cmap.get_hex(c) for c in ["A-01", "A-02", "A-03"]}
        assert len(hexes) == 3

    def test_different_categories_distinguishable(self):
        """Unterschiedliche Kategorien bekommen unterschiedliche Hues."""
        cmap = CodeColorMap(["A-01", "B-01", "C-01", "L-01"])
        hues = {}
        for c in ["A-01", "B-01", "C-01", "L-01"]:
            r, g, b = cmap.get_rgb(c)
            h, _, _ = colorsys.rgb_to_hsv(r, g, b)
            hues[c] = h
        # Paarweise klar unterscheidbar
        values = list(hues.values())
        for i in range(len(values)):
            for j in range(i + 1, len(values)):
                diff = min(
                    abs(values[i] - values[j]),
                    1.0 - abs(values[i] - values[j]),
                )
                assert diff > 0.05, f"Hues zu nah: {values[i]} / {values[j]}"

    def test_category_code_itself_gets_color(self):
        """Die reine Hauptkategorie ('A') erhaelt ebenfalls eine Farbe."""
        cmap = CodeColorMap(["A", "A-01", "A-02"])
        assert "A" in cmap.colors
        assert cmap.get_hex("A").startswith("#")

    def test_category_is_strongest_variant(self):
        """Die Hauptkategorie ('A') ist staerker saturiert als ihre Subcodes
        (entspricht Shade-Index 0 = saturierteste Stufe)."""
        cmap = CodeColorMap(["A", "A-01", "A-02"])
        # A nutzt Shade-Index 0 -> Saturation=0.85, Subcodes danach.
        _, s_cat, _ = colorsys.rgb_to_hsv(*cmap.get_rgb("A"))
        _, s_sub1, _ = colorsys.rgb_to_hsv(*cmap.get_rgb("A-01"))
        _, s_sub2, _ = colorsys.rgb_to_hsv(*cmap.get_rgb("A-02"))
        assert s_cat == pytest.approx(0.85, abs=1e-6)
        # Subcodes kommen aus anderen Shade-Stufen.
        assert (s_sub1, s_sub2) != (0.85, 0.85)

    def test_many_subcodes_distinct(self):
        """Bis zu ~20 Subcodes sollen visuell unterscheidbar sein."""
        codes = [f"A-{i:02d}" for i in range(1, 21)]
        cmap = CodeColorMap(codes)
        hexes = {cmap.get_hex(c) for c in codes}
        assert len(hexes) == 20

    def test_handles_z_category(self):
        """Auch nicht-bestehende Kategorien (Z) funktionieren."""
        cmap = CodeColorMap(["Z-01", "Z-02"])
        assert cmap.get_hex("Z-01") != cmap.get_hex("Z-02")


# ---------------------------------------------------------------------------
# Determinismus + Order-Unabhaengigkeit
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_input_twice(self):
        codes = ["A-01", "B-02", "L-03", "O-01"]
        cmap1 = CodeColorMap(codes)
        cmap2 = CodeColorMap(codes)
        for c in codes:
            assert cmap1.get_hex(c) == cmap2.get_hex(c)

    def test_order_independence(self):
        m1 = CodeColorMap(["A-01", "B-01"])
        m2 = CodeColorMap(["B-01", "A-01"])
        assert m1.get_hex("A-01") == m2.get_hex("A-01")
        assert m1.get_hex("B-01") == m2.get_hex("B-01")

    def test_adding_new_code_preserves_existing(self):
        """Ein zusaetzlicher Code in einer NEUEN Kategorie darf
        bestehende Farben nicht verschieben."""
        m1 = CodeColorMap(["A-01", "A-02"])
        m2 = CodeColorMap(["A-01", "A-02", "B-01"])
        assert m1.get_hex("A-01") == m2.get_hex("A-01")
        assert m1.get_hex("A-02") == m2.get_hex("A-02")

    def test_adding_higher_subcode_preserves_lower(self):
        """Ein neuer hoeherer Subcode (A-99) darf kleinere nicht aendern."""
        m1 = CodeColorMap(["A-01", "A-02"])
        m2 = CodeColorMap(["A-01", "A-02", "A-99"])
        assert m1.get_hex("A-01") == m2.get_hex("A-01")
        assert m1.get_hex("A-02") == m2.get_hex("A-02")


# ---------------------------------------------------------------------------
# YAML Overrides
# ---------------------------------------------------------------------------

class TestYamlOverrides:
    def test_from_yaml_override_wins(self, tmp_path):
        yaml_path = tmp_path / "colors.yaml"
        yaml_path.write_text(
            yaml.safe_dump({"colors": {"A-01": "#FF6B6B", "L": "#0033CC"}}),
            encoding="utf-8",
        )
        cmap = CodeColorMap.from_yaml(yaml_path, ["A-01", "A-02", "L"])
        assert cmap.get_hex("A-01") == "#FF6B6B"
        assert cmap.get_hex("L") == "#0033CC"
        # A-02 wird weiterhin automatisch befuellt
        assert cmap.get_hex("A-02").startswith("#")

    def test_unknown_override_entries_ignored(self, tmp_path):
        yaml_path = tmp_path / "colors.yaml"
        yaml_path.write_text(
            yaml.safe_dump({"colors": {"A-01": "#FF6B6B", "X-99": "#00FF00"}}),
            encoding="utf-8",
        )
        # X-99 ist nicht in den Codes - darf keinen Fehler werfen.
        cmap = CodeColorMap.from_yaml(yaml_path, ["A-01"])
        assert cmap.get_hex("A-01") == "#FF6B6B"
        # X-99 nicht in colors-Map (da nicht in codes).
        assert "X-99" not in cmap.colors

    def test_from_yaml_missing_file(self, tmp_path):
        yaml_path = tmp_path / "does_not_exist.yaml"
        cmap = CodeColorMap.from_yaml(yaml_path, ["A-01"])
        assert cmap.get_hex("A-01").startswith("#")

    def test_from_yaml_empty_file(self, tmp_path):
        yaml_path = tmp_path / "empty.yaml"
        yaml_path.write_text("", encoding="utf-8")
        cmap = CodeColorMap.from_yaml(yaml_path, ["A-01"])
        assert cmap.get_hex("A-01").startswith("#")

    def test_invalid_override_hex_is_skipped(self, tmp_path):
        yaml_path = tmp_path / "colors.yaml"
        yaml_path.write_text(
            yaml.safe_dump({"colors": {"A-01": "not-a-color"}}),
            encoding="utf-8",
        )
        # Sollte nicht werfen, A-01 bleibt auf Auto-Wert.
        cmap = CodeColorMap.from_yaml(yaml_path, ["A-01"])
        auto = CodeColorMap(["A-01"])
        assert cmap.get_hex("A-01") == auto.get_hex("A-01")

    def test_direct_overrides_argument(self):
        cmap = CodeColorMap(["A-01", "B-01"], overrides={"A-01": "#112233"})
        assert cmap.get_hex("A-01") == "#112233"
        assert cmap.get_hex("B-01") != "#112233"


# ---------------------------------------------------------------------------
# Fallback fuer unbekannte Codes
# ---------------------------------------------------------------------------

class TestFallback:
    def test_unknown_code_no_exception(self):
        cmap = CodeColorMap(["A-01"])
        assert cmap.get_hex("ZZ-99").startswith("#")
        assert len(cmap.get_rgb("ZZ-99")) == 3

    def test_fallback_is_deterministic(self):
        cmap1 = CodeColorMap([])
        cmap2 = CodeColorMap([])
        assert cmap1.get_hex("foo-bar-baz") == cmap2.get_hex("foo-bar-baz")

    def test_get_rgb_values_in_unit_range(self):
        cmap = CodeColorMap(["A-01"])
        for c in ["A-01", "UNKNOWN"]:
            r, g, b = cmap.get_rgb(c)
            assert 0.0 <= r <= 1.0
            assert 0.0 <= g <= 1.0
            assert 0.0 <= b <= 1.0


# ---------------------------------------------------------------------------
# Serialisierung: to_dict / to_markdown
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_to_dict_shape(self):
        cmap = CodeColorMap(
            ["A-01", "A-02", "L-01"],
            code_names={"A-01": "Vertrag", "L-01": "Leistungsphase"},
            category_names={"A": "Allgemein", "L": "Leistung"},
        )
        d = cmap.to_dict()
        assert "codes" in d
        assert len(d["codes"]) == 3

        a01 = next(c for c in d["codes"] if c["code_id"] == "A-01")
        assert a01["code_name"] == "Vertrag"
        assert a01["hauptkategorie"] == "A"
        assert a01["kategorie_name"] == "Allgemein"
        assert a01["color_hex"].startswith("#")
        assert len(a01["color_rgb"]) == 3

    def test_to_dict_sorted_by_category_then_number(self):
        cmap = CodeColorMap(["L-02", "A-02", "A-01", "L-01"])
        d = cmap.to_dict()
        ids = [c["code_id"] for c in d["codes"]]
        assert ids == ["A-01", "A-02", "L-01", "L-02"]

    def test_to_markdown_contains_headers(self):
        cmap = CodeColorMap(
            ["A-01", "B-01"],
            code_names={"A-01": "Foo", "B-01": "Bar"},
        )
        md = cmap.to_markdown()
        assert "Hex" in md
        assert "Code-ID" in md
        assert "Code-Name" in md
        assert "Hauptkategorie" in md
        assert "A-01" in md
        assert "B-01" in md
        assert "Foo" in md

    def test_to_markdown_sorted(self):
        cmap = CodeColorMap(["B-01", "A-02", "A-01"])
        md = cmap.to_markdown()
        # A-01 soll vor A-02 vor B-01 stehen.
        idx_a01 = md.index("A-01")
        idx_a02 = md.index("A-02")
        idx_b01 = md.index("B-01")
        assert idx_a01 < idx_a02 < idx_b01


# ---------------------------------------------------------------------------
# Konsistenz get_hex <-> get_rgb
# ---------------------------------------------------------------------------

class TestConsistency:
    def test_hex_matches_rgb(self):
        cmap = CodeColorMap(["A-01", "B-02", "L"])
        for c in ["A-01", "B-02", "L"]:
            assert _rgb_to_hex(cmap.get_rgb(c)) == cmap.get_hex(c)

    def test_round_trip_through_instance(self):
        cmap = CodeColorMap(["A-01"])
        hex_val = cmap.get_hex("A-01")
        assert _rgb_to_hex(_hex_to_rgb(hex_val)) == hex_val

    def test_duplicates_in_input_handled(self):
        cmap = CodeColorMap(["A-01", "A-01", "A-02"])
        assert cmap.codes == ["A-01", "A-02"]

    def test_empty_input(self):
        cmap = CodeColorMap([])
        assert cmap.codes == []
        # Fallback-Pfad funktioniert weiterhin.
        assert cmap.get_hex("A-01").startswith("#")
