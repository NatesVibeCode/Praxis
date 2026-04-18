"""Unit tests for :mod:`runtime.bug_candidates_projection_subscriber`.

Covers:
  - The subscriber filters ``receipt_recorded`` events to failures
    (``status == 'failed'`` or non-empty ``failure_code``) and groups
    them by ``(failure_code, node_id)``.
  - A refresh event is emitted back on ``CHANNEL_RECEIPT`` only when
    there are relevant events; otherwise the subscriber advances its
    cursor without emitting, so idle polling does not flood the log.
  - The collector in :mod:`runtime.projection_freshness` surfaces a
    third event-log-cursor sample for the bug_candidates subscriber.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from runtime import bug_candidates_projection_subscriber as subscriber_module
from runtime.bug_candidates_projection_subscriber import (
    BUG_CANDIDATES_PROJECTION_ID,
    BugCandidatesProjectionSubscriber,
)


_BASE_NOW = datetime(2026, 4, 17, 15, 0, 0, tzinfo=timezone.utc)


@dataclass
class _FakeEvent:
    id: int
    event_type: str
    payload: dict[str, Any]
    channel: str = "receipt"
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
        assert channel == "receipt"
        return self.starting_cursor

    async def aread_since(self, _conn, *, channel, cursor, limit=100, entity_id=None):
        assert channel == "receipt"
        return [e for e in self.available_events if e.id > cursor][:limit]

    async def aadvance_cursor(self, _conn, *, subscriber_id, channel, event_id):
        assert channel == "receipt"
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


def _failure_event(
    event_id: int,
    *,
    failure_code: str,
    node_id: str,
    receipt_id: str,
    status: str = "failed",
) -> _FakeEvent:
    return _FakeEvent(
        id=event_id,
        event_type="receipt_recorded",
        payload={
            "receipt_id": receipt_id,
            "run_id": "run-a",
            "node_id": node_id,
            "status": status,
            "failure_code": failure_code,
            "evidence_seq": event_id,
        },
    )


def _success_event(event_id: int) -> _FakeEvent:
    return _FakeEvent(
        id=event_id,
        event_type="receipt_recorded",
        payload={
            "receipt_id": f"receipt-{event_id}",
            "run_id": "run-a",
            "node_id": "node-ok",
            "status": "succeeded",
            "failure_code": None,
            "evidence_seq": event_id,
        },
    )


def test_subscriber_filters_failures_and_emits_refresh(monkeypatch) -> None:
    conn = _FakeConn()
    recorder = _Recorder(
        available_events=[
            _failure_event(5, failure_code="adapter.timeout", node_id="node-a", receipt_id="r-1"),
            _success_event(6),
            _failure_event(7, failure_code="adapter.timeout", node_id="node-a", receipt_id="r-2"),
            _failure_event(8, failure_code="schema.violation", node_id="node-b", receipt_id="r-3"),
        ]
    )
    _install_recorder(monkeypatch, recorder)

    async def _connect(_env=None):
        return conn

    result = asyncio.run(
        BugCandidatesProjectionSubscriber(connect_database=_connect).consume_available_async(
            as_of=_BASE_NOW,
        )
    )

    assert result["subscriber_id"] == "bug_candidates_refresher"
    assert result["channel"] == "receipt"
    assert result["scanned_count"] == 4
    assert result["relevant_count"] == 3
    assert result["refreshed"] is True
    assert result["projection_as_of"] == _BASE_NOW.isoformat()
    assert result["candidate_summary"] == {
        "adapter.timeout:node-a": [
            {"receipt_id": "r-1", "run_id": "run-a", "evidence_seq": 5},
            {"receipt_id": "r-2", "run_id": "run-a", "evidence_seq": 7},
        ],
        "schema.violation:node-b": [
            {"receipt_id": "r-3", "run_id": "run-a", "evidence_seq": 8},
        ],
    }
    assert recorder.advanced_to == 8
    assert conn.closed is True
    assert len(recorder.emitted_events) == 1
    emitted = recorder.emitted_events[0]
    assert emitted["channel"] == "receipt"
    assert emitted["event_type"] == "bug_candidates_projection_refreshed"
    assert emitted["entity_id"] == BUG_CANDIDATES_PROJECTION_ID
    assert emitted["payload"]["relevant_count"] == 3
    assert emitted["payload"]["subscriber_id"] == "bug_candidates_refresher"


def test_subscriber_treats_failed_status_without_code_as_relevant(monkeypatch) -> None:
    conn = _FakeConn()
    recorder = _Recorder(
        available_events=[
            _FakeEvent(
                id=3,
                event_type="receipt_recorded",
                payload={
                    "receipt_id": "r-no-code",
                    "run_id": "run-a",
                    "node_id": "node-z",
                    "status": "failed",
                    "failure_code": None,
                    "evidence_seq": 3,
                },
            ),
        ]
    )
    _install_recorder(monkeypatch, recorder)

    async def _connect(_env=None):
        return conn

    result = asyncio.run(
        BugCandidatesProjectionSubscriber(connect_database=_connect).consume_available_async(
            as_of=_BASE_NOW,
        )
    )

    assert result["relevant_count"] == 1
    assert result["refreshed"] is True
    assert result["candidate_summary"] == {
        "unknown:node-z": [
            {"receipt_id": "r-no-code", "run_id": "run-a", "evidence_seq": 3},
        ],
    }


def test_subscriber_skips_rebuild_when_all_successes(monkeypatch) -> None:
    conn = _FakeConn()
    recorder = _Recorder(
        available_events=[_success_event(10), _success_event(11)],
    )
    _install_recorder(monkeypatch, recorder)

    async def _connect(_env=None):
        return conn

    result = asyncio.run(
        BugCandidatesProjectionSubscriber(connect_database=_connect).consume_available_async(
            as_of=_BASE_NOW,
        )
    )

    assert result["scanned_count"] == 2
    assert result["relevant_count"] == 0
    assert result["refreshed"] is False
    assert result["projection_as_of"] is None
    assert result["candidate_summary"] == {}
    assert recorder.advanced_to == 11
    assert recorder.emitted_events == []


def test_subscriber_no_events_leaves_cursor_untouched(monkeypatch) -> None:
    conn = _FakeConn()
    recorder = _Recorder(starting_cursor=42, available_events=[])
    _install_recorder(monkeypatch, recorder)

    async def _connect(_env=None):
        return conn

    result = asyncio.run(
        BugCandidatesProjectionSubscriber(connect_database=_connect).consume_available_async(
            as_of=_BASE_NOW,
        )
    )

    assert result["scanned_count"] == 0
    assert result["relevant_count"] == 0
    assert result["refreshed"] is False
    assert result["starting_cursor"] == 42
    assert result["ending_cursor"] == 42
    assert recorder.advanced_to is None


def test_collect_projection_freshness_sync_includes_bug_candidates_cursor(monkeypatch) -> None:
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
        def fetchrow(self, query: str, *_args: object) -> Any:
            if "MAX(id)" in query:
                return {"head_id": 12, "head_at": _BASE_NOW}
            return {"last_event_id": 4, "updated_at": _BASE_NOW}

    samples = collect_projection_freshness_sync(_SyncConn(), observed_at=_BASE_NOW)

    event_log_samples = [s for s in samples if s.source_kind == EVENT_LOG_CURSOR]
    projection_ids = {s.projection_id for s in event_log_samples}
    assert BUG_CANDIDATES_PROJECTION_ID in projection_ids
    bug_sample = next(
        s for s in event_log_samples if s.projection_id == BUG_CANDIDATES_PROJECTION_ID
    )
    assert bug_sample.subscriber_id == "bug_candidates_refresher"
    assert bug_sample.lag_events == 8
