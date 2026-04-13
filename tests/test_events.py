# SPDX-License-Identifier: AGPL-3.0-only
"""Tests fuer qualdatan_core.events."""

from __future__ import annotations

import pytest

from qualdatan_core.events import (
    EventBus,
    LogMessage,
    NoOpBus,
    RunFinished,
    RunStarted,
    StageFinished,
    StageProgress,
    StageStarted,
    TokensUsed,
)


def test_subscribe_and_emit_delivers_to_all_subscribers():
    bus = EventBus()
    a: list = []
    b: list = []
    bus.subscribe(a.append)
    bus.subscribe(b.append)

    bus.emit(RunStarted(run_id="r1", profile="transcripts"))

    assert len(a) == 1 and len(b) == 1
    assert isinstance(a[0], RunStarted)
    assert a[0].run_id == "r1"
    assert a[0].profile == "transcripts"


def test_unsubscribe_stops_delivery():
    bus = EventBus()
    seen: list = []
    unsubscribe = bus.subscribe(seen.append)
    bus.emit(StageStarted(stage="extract", total=5))
    unsubscribe()
    bus.emit(StageFinished(stage="extract", total=5))

    assert len(seen) == 1
    assert isinstance(seen[0], StageStarted)


def test_unsubscribe_is_idempotent():
    bus = EventBus()
    unsubscribe = bus.subscribe(lambda _e: None)
    unsubscribe()
    # second call must not raise
    unsubscribe()


def test_subscriber_exception_is_converted_to_log_warning():
    bus = EventBus()
    seen: list = []

    def boom(event):
        raise RuntimeError("nope")

    bus.subscribe(boom)
    bus.subscribe(seen.append)

    bus.emit(StageStarted(stage="extract"))

    # seen receives the original event AND the warning about boom
    kinds = [type(e).__name__ for e in seen]
    assert "StageStarted" in kinds
    assert "LogMessage" in kinds
    warning = next(e for e in seen if isinstance(e, LogMessage))
    assert warning.severity == "warning"
    assert "RuntimeError" in warning.message


def test_faulty_subscriber_stays_subscribed_but_does_not_cause_infinite_loop():
    bus = EventBus()
    counter = {"n": 0}

    def boom(event):
        counter["n"] += 1
        raise ValueError("still broken")

    bus.subscribe(boom)

    bus.emit(StageStarted(stage="extract"))
    bus.emit(StageFinished(stage="extract"))

    # Two emits => boom called twice (and once more for each LogMessage re-emit).
    # What matters is that we *terminate* — no recursion explosion.
    assert counter["n"] >= 2
    assert counter["n"] < 20  # sanity cap; reentry guard must prevent blowup


def test_unsubscribe_during_emit_is_safe():
    bus = EventBus()
    seen: list = []
    unsubscribe_holder: list = []

    def self_unsubscribing(event):
        seen.append(event)
        unsubscribe_holder[0]()

    unsubscribe_holder.append(bus.subscribe(self_unsubscribing))
    bus.subscribe(seen.append)

    bus.emit(StageStarted(stage="extract"))
    # second emit: self_unsubscribing should be gone
    bus.emit(StageFinished(stage="extract"))

    # self_unsubscribing saw the first event only; plain appender saw both
    kinds = [type(e).__name__ for e in seen]
    # extract -> 2x StageStarted (both subscribers got it), then 1x StageFinished
    # (only plain appender left).
    assert kinds.count("StageStarted") == 2
    assert kinds.count("StageFinished") == 1


def test_noop_bus_swallows_everything():
    bus = NoOpBus()
    seen: list = []
    bus.subscribe(seen.append)  # subscribe is inherited but emit is overridden

    bus.emit(RunStarted(run_id="x", profile="transcripts"))
    bus.emit(StageStarted(stage="extract"))

    assert seen == []


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RunStarted(run_id="r", profile="p"),
        lambda: RunFinished(run_id="r", status="done", duration_seconds=1.23),
        lambda: StageStarted(stage="code", total=3),
        lambda: StageProgress(stage="code", done=1, total=3, detail="file.pdf"),
        lambda: StageFinished(stage="code", total=3),
        lambda: LogMessage(message="hi", severity="info"),
        lambda: TokensUsed(stage="code", model="haiku", input_tokens=100, output_tokens=50),
    ],
)
def test_events_are_frozen(factory):
    """Events sollen immutable sein (frozen dataclasses)."""
    event = factory()
    with pytest.raises(Exception):  # FrozenInstanceError wraps in dataclasses
        event.event_id = "mutated"  # type: ignore[misc]


def test_event_has_id_and_timestamp():
    evt = StageStarted(stage="extract", total=5)
    assert evt.event_id  # non-empty
    assert evt.emitted_at is not None


def test_every_event_gets_unique_id():
    a = StageStarted(stage="x")
    b = StageStarted(stage="x")
    assert a.event_id != b.event_id
