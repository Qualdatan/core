# SPDX-License-Identifier: AGPL-3.0-only
"""UI-neutrale Progress-Events fuer Qualdatan.

Der Core feuert Events (Scan, Extract, Code, Refine, Annotate, Export, …)
ueber einen zentralen ``EventBus``. TUI (Rich-Progress), Desktop-Sidecar
(SSE) und Tests (Recorder) subscriben sich, ohne dass der Core irgendetwas
ueber die jeweilige UI weiss.

Design-Entscheidungen
---------------------
* **Prozessinterner Fan-out**, kein Threading/Async-Zwang im Core. Publisher
  ruft ``bus.emit(event)`` synchron; Subscriber werden sequentiell notifiziert.
* **Events sind immutable Dataclasses**. Einfach zu serialisieren (z.B. als
  SSE-Payload oder JSON-Log-Line), gut typed, leicht zu testen.
* **Keine globalen Singletons**: jeder Run bekommt einen eigenen Bus, der in
  ``RunContext`` liegt. Wer keine Events braucht (z.B. Unit-Tests ohne
  Beobachter), bekommt einen NoOp-Bus.

Verwendung
----------

    >>> bus = EventBus()
    >>> seen: list[Event] = []
    >>> unsubscribe = bus.subscribe(seen.append)
    >>> bus.emit(RunStarted(run_id="r1", profile="transcripts"))
    >>> bus.emit(StageStarted(stage="extract", total=12))
    >>> bus.emit(StageProgress(stage="extract", done=3, total=12))
    >>> unsubscribe()
    >>> [type(e).__name__ for e in seen]
    ['RunStarted', 'StageStarted', 'StageProgress']
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------

Severity = Literal["info", "warning", "error"]


@dataclass(frozen=True, slots=True)
class Event:
    """Basis-Event. Subklassen ergaenzen Payload-Felder."""

    event_id: str = field(default_factory=lambda: uuid4().hex)
    emitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class RunStarted(Event):
    """Start eines Pipeline-Durchlaufs."""

    run_id: str = ""
    profile: str = ""  # z.B. "transcripts", "documents", "company"


@dataclass(frozen=True, slots=True)
class RunFinished(Event):
    """Ende eines Pipeline-Durchlaufs (erfolgreich, abgebrochen oder failed)."""

    run_id: str = ""
    status: Literal["done", "failed", "aborted"] = "done"
    duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class StageStarted(Event):
    """Eine Pipeline-Stage (extract, classify, code, annotate, export, …)."""

    stage: str = ""
    total: int | None = None  # None wenn unbekannt


@dataclass(frozen=True, slots=True)
class StageProgress(Event):
    """Fortschritts-Tick einer Stage (``done`` von ``total`` verarbeitet)."""

    stage: str = ""
    done: int = 0
    total: int | None = None
    detail: str = ""  # optional: aktueller Item-Name (z.B. Dateiname)


@dataclass(frozen=True, slots=True)
class StageFinished(Event):
    """Abschluss einer Pipeline-Stage."""

    stage: str = ""
    total: int | None = None


@dataclass(frozen=True, slots=True)
class LogMessage(Event):
    """Frei formatierte Log-Zeile (z.B. fuer UI-Konsole oder SSE)."""

    message: str = ""
    severity: Severity = "info"
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TokensUsed(Event):
    """Fuer LLM-Call-Accounting; kann vom UI zu einem laufenden Zaehler
    aufaddiert werden."""

    stage: str = ""
    model: str = ""
    input_tokens: int = 0
    output_tokens: int = 0


# ---------------------------------------------------------------------------
# Bus
# ---------------------------------------------------------------------------

Subscriber = Callable[[Event], None]


class EventBus:
    """Minimalistischer synchronous fan-out Bus.

    Fehler im Subscriber werden als ``LogMessage(severity="warning")`` zurueck
    auf den Bus emittiert, damit ein kaputter Listener den Core nicht
    lahmlegt. Der faulty Subscriber selbst wird nicht unsubscribed — der
    Aufrufer entscheidet, ob er die Warning sieht und reagiert.
    """

    def __init__(self) -> None:
        """Leerer Bus ohne Subscriber."""
        self._subscribers: list[Subscriber] = []
        self._reentry = False

    def subscribe(self, subscriber: Subscriber) -> Callable[[], None]:
        """Meldet ``subscriber`` an. Gibt eine ``unsubscribe``-Funktion zurueck."""
        self._subscribers.append(subscriber)

        def _unsubscribe() -> None:
            try:
                self._subscribers.remove(subscriber)
            except ValueError:
                pass

        return _unsubscribe

    def emit(self, event: Event) -> None:
        """Feuert ``event`` an alle Subscriber."""
        # Snapshot der Liste, damit Subscriber ohne Race unsubscriben koennen.
        for sub in list(self._subscribers):
            try:
                sub(event)
            except Exception as exc:  # noqa: BLE001 — bus muss robust sein
                if self._reentry:
                    # Wir sind schon im Fehler-Fallback; nicht nochmal emit,
                    # sonst Endlosschleife.
                    continue
                self._reentry = True
                try:
                    self.emit(
                        LogMessage(
                            message=f"EventBus subscriber raised: {exc!r}",
                            severity="warning",
                            context={"subscriber": repr(sub)},
                        )
                    )
                finally:
                    self._reentry = False


class NoOpBus(EventBus):
    """Bus ohne Subscriber — ignoriert alle Events.

    Default fuer Unit-Tests oder headless-Aufrufe, die keine UI haben.
    """

    def emit(self, event: Event) -> None:  # pragma: no cover - trivial
        """No-op: verwirft ``event``."""
        return


__all__ = [
    "Event",
    "EventBus",
    "LogMessage",
    "NoOpBus",
    "RunFinished",
    "RunStarted",
    "Severity",
    "StageFinished",
    "StageProgress",
    "StageStarted",
    "Subscriber",
    "TokensUsed",
]
