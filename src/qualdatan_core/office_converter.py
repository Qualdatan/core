"""Office-Konvertierung: .docx/.xlsx/.doc/.xls -> .pdf

Strategien (in dieser Reihenfolge versucht):
  1. Microsoft Office via PowerShell COM (Windows-Host, auch aus WSL2 nutzbar)
  2. LibreOffice headless (`soffice --headless --convert-to pdf`)

Beide Wege werden zur Laufzeit gepruefte. Wenn keiner verfuegbar ist,
wirft `convert_to_pdf` `OfficeConverterUnavailable`.

Excel: Workbook.ExportAsFixedFormat(0) wird verwendet, das exportiert
ALLE Sheets als zusammenhaengende PDF (jedes Sheet kann mehrere Seiten haben).

Caching: Resultierende PDFs werden im Zielverzeichnis abgelegt. Der Aufrufer
ist fuer den Cache-Pfad verantwortlich. Wenn dst existiert und neuer als src
ist, wird die Konvertierung uebersprungen.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


SUPPORTED_EXTENSIONS = {".docx", ".doc", ".xlsx", ".xls"}


class OfficeConverterUnavailable(RuntimeError):
    """Kein Office-Konverter auf diesem System verfuegbar."""


# ---------------------------------------------------------------------------
# Backend-Erkennung (cached)
# ---------------------------------------------------------------------------

_BACKEND_CACHE: str | None = None


def detect_backend() -> str | None:
    """Erkennt das verfuegbare Backend.

    Returns:
        "powershell-com" wenn Windows-Office via PowerShell erreichbar ist,
        "libreoffice" wenn `soffice` im PATH ist,
        None wenn nichts verfuegbar.
    """
    global _BACKEND_CACHE
    if _BACKEND_CACHE is not None:
        return _BACKEND_CACHE if _BACKEND_CACHE != "none" else None

    # 1. PowerShell COM
    if shutil.which("powershell.exe"):
        try:
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-Command",
                 "if (Get-Command Get-ItemProperty -ErrorAction SilentlyContinue) "
                 "{ try { New-Object -ComObject Word.Application | Out-Null; 'ok' } "
                 "catch { 'no-word' } } else { 'no-ps' }"],
                capture_output=True, text=True, timeout=15,
            )
            if "ok" in result.stdout:
                _BACKEND_CACHE = "powershell-com"
                return _BACKEND_CACHE
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

    # 2. LibreOffice headless
    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if soffice:
        _BACKEND_CACHE = "libreoffice"
        return _BACKEND_CACHE

    _BACKEND_CACHE = "none"
    return None


def reset_backend_cache():
    """Setzt das Backend-Caching zurueck. Nur fuer Tests."""
    global _BACKEND_CACHE
    _BACKEND_CACHE = None


# ---------------------------------------------------------------------------
# PowerShell-COM Backend
# ---------------------------------------------------------------------------

# wdFormatPDF = 17
_WORD_PS = """
$word = New-Object -ComObject Word.Application
$word.Visible = $false
$word.DisplayAlerts = 0
try {{
    $doc = $word.Documents.Open("{src}", $false, $true)
    $doc.SaveAs([ref]"{dst}", [ref]17)
    $doc.Close()
}} finally {{
    $word.Quit()
}}
"""

# Excel: ExportAsFixedFormat(Type=0=PDF) auf Workbook -> alle Sheets
_EXCEL_PS = """
$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
try {{
    $wb = $excel.Workbooks.Open("{src}", 0, $true)
    $wb.ExportAsFixedFormat(0, "{dst}")
    $wb.Close($false)
}} finally {{
    $excel.Quit()
}}
"""


def _to_windows_path(p: Path) -> str:
    """WSL-Pfad in Windows-Pfad konvertieren."""
    return subprocess.check_output(
        ["wslpath", "-w", str(p.resolve())]
    ).decode().strip()


def _convert_via_powershell(src: Path, dst: Path) -> None:
    """Konvertiert via Word/Excel COM unter Windows."""
    ext = src.suffix.lower()
    if ext in (".docx", ".doc"):
        template = _WORD_PS
    elif ext in (".xlsx", ".xls"):
        template = _EXCEL_PS
    else:
        raise ValueError(f"Unsupported extension: {ext}")

    win_src = _to_windows_path(src)
    win_dst = _to_windows_path(dst)
    script = template.format(src=win_src, dst=win_dst)

    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", script],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0 or not dst.exists():
        raise RuntimeError(
            f"PowerShell-Konvertierung fehlgeschlagen ({src.name}): "
            f"exit={result.returncode} stderr={result.stderr.strip()[:300]}"
        )


# ---------------------------------------------------------------------------
# LibreOffice Backend
# ---------------------------------------------------------------------------

def _convert_via_libreoffice(src: Path, dst: Path) -> None:
    """Konvertiert via LibreOffice headless."""
    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        raise OfficeConverterUnavailable("Weder soffice noch libreoffice gefunden")

    out_dir = dst.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [soffice, "--headless", "--convert-to", "pdf",
         "--outdir", str(out_dir), str(src)],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"LibreOffice-Konvertierung fehlgeschlagen ({src.name}): "
            f"{result.stderr.strip()[:300]}"
        )

    # LibreOffice schreibt nach <outdir>/<src.stem>.pdf
    produced = out_dir / f"{src.stem}.pdf"
    if produced != dst:
        if dst.exists():
            dst.unlink()
        produced.rename(dst)
    if not dst.exists():
        raise RuntimeError(f"LibreOffice hat kein PDF erzeugt fuer {src.name}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def convert_to_pdf(src: Path, dst: Path,
                   force: bool = False,
                   backend: str | None = None) -> Path:
    """Konvertiert eine Office-Datei zu PDF.

    Args:
        src: Quelldatei (.docx/.xlsx/.doc/.xls)
        dst: Zielpfad fuer das PDF
        force: Wenn True, auch konvertieren wenn dst neuer als src ist
        backend: Optional erzwingen ("powershell-com" oder "libreoffice")

    Returns:
        Pfad der erzeugten PDF (== dst)

    Raises:
        OfficeConverterUnavailable: kein Konverter auf dem System
        ValueError: nicht unterstuetzte Extension
        RuntimeError: Konvertierung fehlgeschlagen
    """
    src = Path(src)
    dst = Path(dst)

    if src.suffix.lower() not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Nicht unterstuetzte Extension: {src.suffix}")

    if not src.exists():
        raise FileNotFoundError(f"Quelldatei fehlt: {src}")

    # Cache: dst neuer als src -> uebernehmen
    if not force and dst.exists():
        if dst.stat().st_mtime >= src.stat().st_mtime:
            return dst

    dst.parent.mkdir(parents=True, exist_ok=True)

    backend = backend or detect_backend()
    if backend is None:
        raise OfficeConverterUnavailable(
            "Kein Office-Konverter gefunden. Installiere LibreOffice "
            "oder nutze MS Office unter Windows mit WSL2."
        )

    if backend == "powershell-com":
        _convert_via_powershell(src, dst)
    elif backend == "libreoffice":
        _convert_via_libreoffice(src, dst)
    else:
        raise ValueError(f"Unbekanntes Backend: {backend}")

    return dst


def find_office_files(base_dir: Path,
                      project_filter: str | None = None) -> list[Path]:
    """Sucht alle Office-Dateien rekursiv unter base_dir.

    Args:
        base_dir: Wurzelverzeichnis (z.B. input/projects)
        project_filter: Optional nur in diesem Top-Level-Ordner suchen

    Returns:
        Liste von Pfaden, sortiert.
    """
    base_dir = Path(base_dir)
    if not base_dir.exists():
        return []

    files = []
    for ext in SUPPORTED_EXTENSIONS:
        for p in base_dir.rglob(f"*{ext}"):
            # Temporaere Office-Lockfiles ignorieren (~$file.docx)
            if p.name.startswith("~$"):
                continue
            if project_filter:
                rel = p.relative_to(base_dir)
                if not rel.parts or rel.parts[0] != project_filter:
                    continue
            files.append(p)
    return sorted(files)
