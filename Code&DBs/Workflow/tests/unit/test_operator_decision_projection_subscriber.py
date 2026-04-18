from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from runtime import operator_decision_projection_subscriber as subscriber_module
from runtime.operator_decision_projection_subscriber import (
    BRIDGE_SOURCE_OPERATOR_DECISIONS,
    OPERATOR_DECISION_PROJECTION_ID,
    OperatorDecisionProjectionSubscriber,
    aconsume_operator_decision_projection_events,
)


_BASE_NOW = datetime(2026, 4, 17, 15, 0, 0, tzinfo=timezone.utc)


@dataclass
class _FakeEvent:
    id: int
    event_type: str
    payload: dict[str, Any]
    channel: str = "semantic_assertion"
    entity_id: str = ""
    entity_kind: str = ""
    emitted_at: datetime = _BASE_NOW
    emitted_by: str = ""


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def close(self) -> None:
        self.closed = True


@dataclass
class _Recorder:
    starting_cursor: int = 0
    available_events: list[_FakeEvent] = field(default_factory=list)
    emitted_events: list[dict[str, Any]] = field(default_factory=list)
    advanced_to: int | None = None
    next_event_id: int = 1_000_000

    async def aget_cursor(self, _conn, *, subscriber_id, channel) -> int:
        assert channel == "semantic_assertion"
        return self.starting_cursor

    async def aread_since(self, _conn, *, channel, cursor, limit=100, entity_id=None):
        assert channel == "semantic_assertion"
        return [e for e in self.available_events if e.id > cursor][:limit]

    async def aadvance_cursor(self, _conn, *, subscriber_id, channel, event_id):
        assert channel == "semantic_assertion"
        self.advanced_to = event_id

    async def aemit(
        self,
        _conn,
        *,
        channel,
        event_type,
        entity_id,
        entity_kind,
        payload,
        emitted_by,
    ) -> int:
        self.emitted_events.append(
            {
                "channel": channel,
                "event_type": event_type,
                "entity_id": entity_id,
                "entity_kind": entity_kind,
                "payload": dict(payload),
                "emitted_by": emitted_by,
            }
        )
        event_id = self.next_event_id
        self.next_event_id += 1
        return event_id


def _install_recorder(monkeypatch, recorder: _Recorder) -> None:
    monkeypatch.setattr(subscriber_module, "aget_cursor", recorder.aget_cursor)
    monkeypatch.setattr(subscriber_module, "aread_since", recorder.aread_since)
    monkeypatch.setattr(subscriber_module, "aadvance_cursor", recorder.aadvance_cursor)
    monkeypatch.setattr(subscriber_module, "aemit", recorder.aemit)


def _bridge_event(event_id: int, subject_kind: str, subject_ref: str, decision_id: str) -> _FakeEvent:
    return _FakeEvent(
        id=event_id,
        event_type="semantic_assertion_recorded",
        payload={
            "bridge_source": BRIDGE_SOURCE_OPERATOR_DECISIONS,
            "semantic_assertion": {
                "subject_kind": subject_kind,
                "subject_ref": subject_ref,
                "object_ref": decision_id,
            },
        },
    )


def test_subscriber_filters_and_emits_refresh_event(monkeypatch) -> None:
    conn = _FakeConn()
    recorder = _Recorder(
        available_events=[
            _bridge_event(5, "provider", "openai", "operator-decision.1"),
            _FakeEvent(
                id=6,
                event_type="semantic_assertion_recorded",
                payload={
                    "bridge_source": "roadmap_items",
                    "semantic_assertion": {"subject_kind": "roadmap_item", "subject_ref": "r1"},
                },
            ),
            _bridge_event(7, "authority_domain", "decision_tables", "operator-decision.2"),
        ]
    )
    _install_recorder(monkeypatch, recorder)

    async def _connect(_env=None):
        return conn

    result = asyncio.run(
        OperatorDecisionProjectionSubscriber(connect_database=_connect).consume_available_async(
            as_of=_BASE_NOW,
        )
    )

    assert result["subscriber_id"] == "operator_decision_projection_refresher"
    assert result["scanned_count"] == 3
    assert result["relevant_count"] == 2
    assert result["refreshed"] is True
    assert result["projection_as_of"] == _BASE_NOW.isoformat()
    assert result["scope_summary"] == {
        "provider:openai": "operator-decision.1",
        "authority_domain:decision_tables": "operator-decision.2",
    }
    assert recorder.advanced_to == 7
    assert conn.closed is True
    assert len(recorder.emitted_events) == 1
    emitted = recorder.emitted_events[0]
    assert emitted["event_type"] == "operator_decision_projection_refreshed"
    assert emitted["entity_id"] == OPERATOR_DECISION_PROJECTION_ID
    assert emitted["payload"]["relevant_count"] == 2
    assert emitted["payload"]["subscriber_id"] == "operator_decision_projection_refresher"


def test_subscriber_skips_rebuild_when_no_relevant_events(monkeypatch) -> None:
    conn = _FakeConn()
    recorder = _Recorder(
        available_events=[
            _FakeEvent(
                id=11,
                event_type="semantic_assertion_recorded",
                payload={"bridge_source": "roadmap_items"},
            ),
        ]
    )
    _install_recorder(monkeypatch, recorder)

    async def _connect(_env=None):
        return conn

    result = asyncio.run(
        aconsume_operator_decision_projection_events(as_of=_BASE_NOW)
        if False
        else OperatorDecisionProjectionSubscriber(connect_database=_connect).consume_available_async(
            as_of=_BASE_NOW,
        )
    )

    assert result["scanned_count"] == 1
    assert result["relevant_count"] == 0
    assert result["refreshed"] is False
    assert result["projection_as_of"] is None
    assert result["scope_summary"] == {}
    assert recorder.advanced_to == 11
    assert recorder.emitted_events == []


def test_subscriber_no_events_leaves_cursor_untouched(monkeypatch) -> None:
    conn = _FakeConn()
    recorder = _Recorder(starting_cursor=42, available_events=[])
    _install_recorder(monkeypatch, recorder)

    async def _connect(_env=None):
        return conn

    result = asyncio.run(
        OperatorDecisionProjectionSubscriber(connect_database=_connect).consume_available_async(
            as_of=_BASE_NOW,
        )
    )

    assert result["scanned_count"] == 0
    assert result["relevant_count"] == 0
    assert result["refreshed"] is False
    assert result["starting_cursor"] == 42
    assert result["ending_cursor"] == 42
    assert recorder.advanced_to is None


def test_collect_projection_freshness_sync_includes_decision_cursor(monkeypatch) -> None:
    from runtime import circuit_breaker as circuit_breaker_module
    from runtime import route_authority_snapshot as route_authority_module
    from runtime.projection_freshness import (
        EVENT_LOG_CURSOR,
        collect_projection_freshness_sync,
    )

    monkeypatch.setattr(
        circuit_breaker_module,
        "manual_override_cache_refresh_state",
        lambda: (0, None),
    )
    monkeypatch.setattr(
        route_authority_module,
        "iter_route_authority_cache_states",
        lambda: [],
    )

    class _SyncConn:
        def __init__(self) -> None:
            self.calls = 0

        def fetchrow(self, _query: str, *_args: object) -> Any:
            self.calls += 1
            if "MAX(id)" in _query:
                return {"head_id": 10, "head_at": _BASE_NOW}
            return {"last_event_id": 5, "updated_at": _BASE_NOW}

    samples = collect_projection_freshness_sync(_SyncConn(), observed_at=_BASE_NOW)

    event_log_samples = [s for s in samples if s.source_kind == EVENT_LOG_CURSOR]
    projection_ids = {s.projection_id for s in event_log_samples}
    assert "semantic_current_assertions" in projection_ids
    assert OPERATOR_DECISION_PROJECTION_ID in projection_ids
    decision_sample = next(
        s for s in event_log_samples if s.projection_id == OPERATOR_DECISION_PROJECTION_ID
    )
    assert decision_sample.subscriber_id == "operator_decision_projection_refresher"
    assert decision_sample.lag_events == 5
