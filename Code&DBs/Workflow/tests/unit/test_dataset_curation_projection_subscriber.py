"""Unit tests for runtime/dataset_curation_projection_subscriber.py."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from runtime import dataset_curation_projection_subscriber as subscriber_module
from runtime.dataset_curation_projection_subscriber import (
    DATASET_CURATION_PROJECTION_ID,
    EVENT_CURATION_PROJECTION_REFRESHED,
    EVENT_PROMOTION_RECORDED,
    EVENT_PROMOTION_SUPERSEDED,
    DatasetCurationProjectionSubscriber,
)


_BASE_NOW = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
_CHANNEL = "dataset_curation"


@dataclass
class _FakeEvent:
    id: int
    event_type: str
    payload: dict[str, Any]
    channel: str = _CHANNEL
    entity_id: str = ""
    entity_kind: str = ""
    emitted_at: datetime = _BASE_NOW
    emitted_by: str = ""


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_a) -> bool:
        return False


@dataclass
class _FakeConn:
    promotions: dict[str, dict[str, Any]] = field(default_factory=dict)
    executed: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)
    closed: bool = False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        return "OK"

    async def fetch(self, _query: str, *_args: Any) -> list[Any]:
        return []

    async def fetchrow(self, query: str, *args: Any) -> Any:
        if "FROM dataset_promotions" in query:
            return self.promotions.get(args[0])
        return None

    async def close(self) -> None:
        self.closed = True


@dataclass
class _Recorder:
    starting_cursor: int = 0
    available_events: list[_FakeEvent] = field(default_factory=list)
    emitted: list[dict[str, Any]] = field(default_factory=list)
    advanced_to: int | None = None
    next_event_id: int = 9_000_000

    async def aget_cursor(self, _conn, *, subscriber_id, channel) -> int:
        assert channel == _CHANNEL
        return self.starting_cursor

    async def aread_since(self, _conn, *, channel, cursor, limit=100, entity_id=None):
        assert channel == _CHANNEL
        return [e for e in self.available_events if e.id > cursor][:limit]

    async def aadvance_cursor(self, _conn, *, subscriber_id, channel, event_id):
        assert channel == _CHANNEL
        self.advanced_to = event_id

    async def aemit(self, _conn, *, channel, event_type, entity_id, entity_kind, payload, emitted_by):
        self.emitted.append(
            {
                "channel": channel,
                "event_type": event_type,
                "entity_id": entity_id,
                "payload": dict(payload),
            }
        )
        eid = self.next_event_id
        self.next_event_id += 1
        return eid


def _install(monkeypatch, recorder: _Recorder, conn: _FakeConn) -> None:
    monkeypatch.setattr(subscriber_module, "aget_cursor", recorder.aget_cursor)
    monkeypatch.setattr(subscriber_module, "aread_since", recorder.aread_since)
    monkeypatch.setattr(subscriber_module, "aadvance_cursor", recorder.aadvance_cursor)
    monkeypatch.setattr(subscriber_module, "aemit", recorder.aemit)

    async def _connect(_env):
        return conn

    return _connect


def _sft_promotion(promotion_id: str = "prom_sft_1") -> dict[str, Any]:
    return {
        "promotion_id": promotion_id,
        "candidate_ids": ["cand_a"],
        "dataset_family": "sft",
        "specialist_target": "slm/review",
        "policy_id": "pol_1",
        "payload": json.dumps(
            {
                "prompt": {"task": "review"},
                "target_output": {"verdict": "approve"},
            }
        ),
        "split_tag": "train",
        "promoted_by": "nathan",
        "promotion_kind": "manual",
        "rationale": "ok",
        "decision_ref": "od_1",
        "superseded_by": None,
        "superseded_reason": None,
    }


def test_sft_promotion_materializes_curated_examples(monkeypatch) -> None:
    recorder = _Recorder(
        available_events=[
            _FakeEvent(
                id=10,
                event_type=EVENT_PROMOTION_RECORDED,
                payload={"promotion_id": "prom_sft_1"},
                entity_id="prom_sft_1",
            )
        ]
    )
    conn = _FakeConn(promotions={"prom_sft_1": _sft_promotion()})
    connect = _install(monkeypatch, recorder, conn)

    sub = DatasetCurationProjectionSubscriber(connect_database=connect)
    result = asyncio.run(sub.consume_available_async())

    assert result["promotions_materialized"] == 1
    assert result["promotions_superseded"] == 0
    assert recorder.advanced_to == 10
    assert any("dataset_curated_examples" in q for q, _ in conn.executed)
    # An event must be emitted summarizing the refresh.
    assert any(
        e["event_type"] == EVENT_CURATION_PROJECTION_REFRESHED for e in recorder.emitted
    )


def test_supersede_event_deactivates_all_projection_tables(monkeypatch) -> None:
    recorder = _Recorder(
        available_events=[
            _FakeEvent(
                id=20,
                event_type=EVENT_PROMOTION_SUPERSEDED,
                payload={"promotion_id": "prom_sft_1"},
                entity_id="prom_sft_1",
            )
        ]
    )
    conn = _FakeConn()
    connect = _install(monkeypatch, recorder, conn)

    sub = DatasetCurationProjectionSubscriber(connect_database=connect)
    result = asyncio.run(sub.consume_available_async())

    assert result["promotions_superseded"] == 1
    tables_touched = {
        table
        for table in (
            "dataset_curated_examples",
            "dataset_curated_preference_pairs",
            "dataset_curated_eval_cases",
        )
        if any(table in q for q, _ in conn.executed)
    }
    assert tables_touched == {
        "dataset_curated_examples",
        "dataset_curated_preference_pairs",
        "dataset_curated_eval_cases",
    }


def test_unknown_family_is_skipped_not_materialized(monkeypatch) -> None:
    routing = _sft_promotion("prom_route_1")
    routing["dataset_family"] = "routing"
    recorder = _Recorder(
        available_events=[
            _FakeEvent(
                id=30,
                event_type=EVENT_PROMOTION_RECORDED,
                payload={"promotion_id": "prom_route_1"},
                entity_id="prom_route_1",
            )
        ]
    )
    conn = _FakeConn(promotions={"prom_route_1": routing})
    connect = _install(monkeypatch, recorder, conn)

    sub = DatasetCurationProjectionSubscriber(connect_database=connect)
    result = asyncio.run(sub.consume_available_async())

    assert result["promotions_materialized"] == 0
    assert result["promotions_skipped"] == 1
    # Cursor still advances so we don't re-scan forever.
    assert recorder.advanced_to == 30


def test_irrelevant_event_does_not_count(monkeypatch) -> None:
    recorder = _Recorder(
        available_events=[
            _FakeEvent(
                id=40,
                event_type="something_else",
                payload={},
            )
        ]
    )
    conn = _FakeConn()
    connect = _install(monkeypatch, recorder, conn)

    sub = DatasetCurationProjectionSubscriber(connect_database=connect)
    result = asyncio.run(sub.consume_available_async())

    assert result["promotions_materialized"] == 0
    assert result["promotions_superseded"] == 0
    # Cursor still advances past the scanned event.
    assert recorder.advanced_to == 40


def test_cold_run_emits_nothing(monkeypatch) -> None:
    recorder = _Recorder(available_events=[])
    conn = _FakeConn()
    connect = _install(monkeypatch, recorder, conn)

    sub = DatasetCurationProjectionSubscriber(connect_database=connect)
    result = asyncio.run(sub.consume_available_async())

    assert result["promotions_materialized"] == 0
    assert result["scanned_count"] == 0
    assert recorder.emitted == []
    assert recorder.advanced_to is None
