# SPDX-License-Identifier: AGPL-3.0-only
"""Minimaler Rich-Console-Shim fuer den Orchestrator.

Der Core bleibt moeglichst UI-frei (siehe :mod:`qualdatan_core.events`
fuer die strukturierte Event-API), aber die bestehende Pipeline in
:mod:`qualdatan_core.pdf_coder` gibt Status fortlaufend via Rich aus.
Dieses Modul ist ein kleines, aus der alten ``src/cli.py`` extrahiertes
Set an Helpern; ``qualdatan-tui`` ueberschreibt ``console`` bei Bedarf
durch seinen eigenen Rich-Console-Wrapper.

Zukuenftig (post-B) werden die direkten Prints komplett durch
``EventBus``-Emits ersetzt; dann verschwindet dieses Modul.
"""

from __future__ import annotations

from contextlib import contextmanager

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


PHASE_STYLES = {
    "input": ("\U0001f4e5", "blue"),
    "scan": ("\U0001f50d", "sky_blue1"),
    "ai": ("\U0001f916", "purple"),
    "annotate": ("\u270f\ufe0f", "dark_orange"),
    "output": ("\U0001f4e6", "green"),
    "tri": ("\U0001f517", "medium_purple"),
}


def print_header(title: str, subtitle: str = "") -> None:
    content = f"[bold cyan]{title}[/bold cyan]"
    if subtitle:
        content += f"\n[dim]{subtitle}[/dim]"
    console.print()
    console.print(Panel(content, box=box.ROUNDED, border_style="cyan", padding=(1, 2)))


def print_step(step: str, detail: str = "", phase: str = "ai") -> None:
    if phase in PHASE_STYLES:
        emoji, color = PHASE_STYLES[phase]
        msg = f"[bold {color}]{emoji} {step}[/bold {color}]"
    else:
        msg = f"[bold blue]>> {step}[/bold blue]"
    if detail:
        msg += f"  [dim]{detail}[/dim]"
    console.print(msg)


def print_success(msg: str) -> None:
    console.print(f"[bold green]  [OK][/bold green] {msg}")


def print_warning(msg: str) -> None:
    console.print(f"[bold yellow]  [!][/bold yellow] {msg}")


def print_error(msg: str) -> None:
    console.print(f"[bold red]  [FEHLER][/bold red] {msg}")


def print_summary(rows: list[tuple[str, str]]) -> None:
    table = Table(box=box.ROUNDED, border_style="dim", show_header=False, padding=(0, 1))
    table.add_column("Eigenschaft", style="bold cyan", no_wrap=True)
    table.add_column("Wert", style="white")
    for key, value in rows:
        table.add_row(key, value)
    console.print(table)


@contextmanager
def spinner(message: str, phase: str = "ai"):
    if phase in PHASE_STYLES:
        emoji, color = PHASE_STYLES[phase]
        styled = f"[{color}]{emoji} {message}[/{color}]"
    else:
        styled = message
    with console.status(styled, spinner="dots"):
        yield


__all__ = [
    "console",
    "print_header",
    "print_step",
    "print_success",
    "print_warning",
    "print_error",
    "print_summary",
    "spinner",
]
