"""Datenmodelle für die Analyse-Ergebnisse."""

from dataclasses import dataclass, field, asdict
import json


@dataclass
class CodedSegment:
    """Ein kodiertes Textsegment."""
    code_id: str           # z.B. "A-01"
    code_name: str
    hauptkategorie: str    # Kategorie-Key
    text: str              # exakte Textstelle
    char_start: int        # Zeichenposition Start
    char_end: int          # Zeichenposition Ende
    document: str          # Dateiname des Transkripts
    kodierdefinition: str = ""
    ankerbeispiel: str = ""
    abgrenzungsregel: str = ""


@dataclass
class AnalysisResult:
    """Gesamtergebnis der Analyse."""
    recipe_id: str = ""
    categories: dict = field(default_factory=dict)      # {key: name} aus dem Recipe
    documents: dict = field(default_factory=dict)        # {filename: full_text}
    segments: list = field(default_factory=list)          # Liste von CodedSegment
    codes: dict = field(default_factory=dict)             # {code_id: {name, kategorie, ...}}
    kernergebnisse: list = field(default_factory=list)    # [{nr, befund, erlaeuterung}]

    def to_json(self) -> str:
        data = {
            "recipe_id": self.recipe_id,
            "categories": self.categories,
            "documents": {k: {"length": len(v)} for k, v in self.documents.items()},
            "segments": [asdict(s) for s in self.segments],
            "codes": self.codes,
            "kernergebnisse": self.kernergebnisse,
        }
        return json.dumps(data, ensure_ascii=False, indent=2)

    def save(self, path):
        path.write_text(self.to_json(), encoding="utf-8")

    @staticmethod
    def load(path) -> "AnalysisResult":
        data = json.loads(path.read_text(encoding="utf-8"))
        result = AnalysisResult()
        result.recipe_id = data.get("recipe_id", "")
        result.categories = data.get("categories", {})
        result.codes = data["codes"]
        result.kernergebnisse = data.get("kernergebnisse", [])
        result.segments = [
            CodedSegment(**s) for s in data["segments"]
        ]
        return result
