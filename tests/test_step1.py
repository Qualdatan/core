"""Tests für step1_analyze (JSON-Parsing, Position-Validierung, Cache)."""

import json
import pytest
from src.step1_analyze import (
    extract_json, validate_positions,
    cache_get, cache_put, _cache_key, CACHE_DIR,
)


class TestExtractJson:
    def test_clean_json(self):
        text = '{"segments": [], "kernergebnisse": []}'
        result = extract_json(text)
        assert result["segments"] == []

    def test_json_in_markdown_block(self):
        text = '```json\n{"segments": [{"code_id": "A-01"}]}\n```'
        result = extract_json(text)
        assert result["segments"][0]["code_id"] == "A-01"

    def test_json_with_preamble(self):
        text = 'Hier ist die Analyse:\n\n{"segments": [], "kernergebnisse": []}'
        result = extract_json(text)
        assert "segments" in result

    def test_truncated_json_repaired(self):
        text = '{"segments": [{"code_id": "A-01", "text": "test"}, {"code_id": "B-01", "text": "halb'
        result = extract_json(text)
        assert len(result["segments"]) >= 1
        assert result["segments"][0]["code_id"] == "A-01"

    def test_no_json_raises(self):
        with pytest.raises(ValueError, match="Kein JSON"):
            extract_json("Keine JSON-Daten hier.")


class TestValidatePositions:
    def test_exact_match(self):
        full_text = "Dies ist ein Testtext mit Inhalt."
        segments = [{"text": "Testtext mit Inhalt", "char_start": 0, "char_end": 10}]
        result = validate_positions(segments, full_text)
        assert result[0]["char_start"] == 13
        assert result[0]["char_end"] == 13 + len("Testtext mit Inhalt")

    def test_partial_match_via_prefix(self):
        """Wenn der exakte Text nicht gefunden wird, matcht die 60-Zeichen-Prefix-Suche."""
        full_text = "Einleitung. Hier beginnt der relevante Abschnitt mit viel Text und Kontext. Ende."
        # Text ist leicht länger als im Original (extra Wörter am Ende)
        segments = [{
            "text": "Hier beginnt der relevante Abschnitt mit viel Text und Kontext. Ende. Plus extra.",
            "char_start": 0, "char_end": 50,
        }]
        result = validate_positions(segments, full_text)
        # Prefix-Match findet "Hier beginnt..." ab Position 12
        assert result[0]["char_start"] == 12

    def test_no_match_keeps_original(self):
        full_text = "Komplett anderer Text."
        segments = [{"text": "Nicht vorhanden xyz abc", "char_start": 42, "char_end": 99}]
        result = validate_positions(segments, full_text)
        assert result[0]["char_start"] == 42  # Fallback: Original behalten


class TestCache:
    def test_cache_miss_returns_none(self):
        result = cache_get("test_recipe", "nonexistent.docx", "text", "")
        assert result is None

    def test_cache_roundtrip(self, tmp_path, monkeypatch):
        """Cache put + get gibt die gleichen Daten zurück."""
        monkeypatch.setattr("src.step1_analyze.CACHE_DIR", tmp_path)
        data = {"segments": [{"code_id": "A-01"}], "kernergebnisse": []}

        cache_put("mayring", "test.docx", "inhalt", "", data)
        loaded = cache_get("mayring", "test.docx", "inhalt", "")

        assert loaded is not None
        assert loaded["segments"][0]["code_id"] == "A-01"

    def test_cache_different_recipe_no_hit(self, tmp_path, monkeypatch):
        """Anderes Recipe → kein Cache-Hit."""
        monkeypatch.setattr("src.step1_analyze.CACHE_DIR", tmp_path)
        data = {"segments": [], "kernergebnisse": []}

        cache_put("mayring", "test.docx", "inhalt", "", data)
        loaded = cache_get("prisma", "test.docx", "inhalt", "")

        assert loaded is None

    def test_cache_different_codebase_no_hit(self, tmp_path, monkeypatch):
        """Andere Codebasis → kein Cache-Hit."""
        monkeypatch.setattr("src.step1_analyze.CACHE_DIR", tmp_path)
        data = {"segments": [], "kernergebnisse": []}

        cache_put("mayring", "test.docx", "inhalt", "", data)
        loaded = cache_get("mayring", "test.docx", "inhalt", "neue codebasis")

        assert loaded is None
