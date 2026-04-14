from __future__ import annotations

from datetime import datetime, timezone

import runtime.event_log as event_log


def _event(event_id: int, *, channel: str = event_log.CHANNEL_BUILD_STATE) -> event_log.Event:
    return event_log.Event(
        id=event_id,
        channel=channel,
        event_type=event_log.EVENT_MUTATION,
        entity_id="wf-123",
        entity_kind="workflow",
        payload={"ok": True},
        emitted_at=datetime(2026, 4, 13, tzinfo=timezone.utc),
        emitted_by="test",
    )


class _FakeWakeEvent:
    def __init__(self) -> None:
        self.wait_calls: list[float] = []
        self.clear_calls = 0
        self.set_calls = 0

    def wait(self, timeout: float | None = None) -> bool:
        self.wait_calls.append(0.0 if timeout is None else float(timeout))
        return True

    def clear(self) -> None:
        self.clear_calls += 1

    def set(self) -> None:
        self.set_calls += 1


class _FakeListener:
    def __init__(self) -> None:
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1


def test_iter_channel_uses_wakeup_listener_when_available(monkeypatch) -> None:
    wake_event = _FakeWakeEvent()
    listener = _FakeListener()
    calls = {"read_since": 0}

    monkeypatch.setattr(event_log.threading, "Event", lambda: wake_event)

    def _start_listener(*, channel: str, wakeup_event):
        assert channel == event_log.CHANNEL_BUILD_STATE
        assert wakeup_event is wake_event
        return listener

    def _read_since(_conn, *, channel: str, cursor: int, entity_id: str | None, limit: int):
        calls["read_since"] += 1
        if calls["read_since"] == 1:
            return []
        assert channel == event_log.CHANNEL_BUILD_STATE
        assert entity_id == "wf-123"
        assert cursor == 0
        assert limit == 50
        return [_event(7)]

    monkeypatch.setattr(event_log, "_start_channel_wakeup_listener", _start_listener)
    monkeypatch.setattr(event_log, "read_since", _read_since)
    monkeypatch.setattr(event_log.time, "sleep", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sleep should not be used when listener exists")))

    stream = event_log.iter_channel(
        object(),
        channel=event_log.CHANNEL_BUILD_STATE,
        entity_id="wf-123",
        timeout_seconds=None,
        poll_interval=0.25,
    )

    try:
        first = next(stream)
    finally:
        stream.close()

    assert first.id == 7
    assert wake_event.wait_calls == [0.25]
    assert wake_event.clear_calls == 1
    assert listener.stop_calls == 1


def test_iter_channel_falls_back_to_sleep_without_listener(monkeypatch) -> None:
    wake_event = _FakeWakeEvent()
    sleeps: list[float] = []
    calls = {"read_since": 0}

    monkeypatch.setattr(event_log.threading, "Event", lambda: wake_event)
    monkeypatch.setattr(event_log, "_start_channel_wakeup_listener", lambda **_kwargs: None)

    def _read_since(_conn, *, channel: str, cursor: int, entity_id: str | None, limit: int):
        calls["read_since"] += 1
        if calls["read_since"] == 1:
            return []
        assert channel == event_log.CHANNEL_BUILD_STATE
        assert cursor == 0
        return [_event(9)]

    monkeypatch.setattr(event_log, "read_since", _read_since)
    monkeypatch.setattr(event_log.time, "sleep", lambda seconds: sleeps.append(float(seconds)))

    stream = event_log.iter_channel(
        object(),
        channel=event_log.CHANNEL_BUILD_STATE,
        timeout_seconds=None,
        poll_interval=0.5,
    )

    try:
        first = next(stream)
    finally:
        stream.close()

    assert first.id == 9
    assert sleeps == [0.5]
    assert wake_event.wait_calls == []
    assert wake_event.clear_calls == 0
