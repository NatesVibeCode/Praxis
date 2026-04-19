"""Unit tests for runtime/dataset_staleness.py."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

from runtime import dataset_staleness as staleness_module
from runtime.dataset_staleness import (
    EVENT_CANDIDATE_STALENESS_CHANGED,
    EVENT_STALENESS_RECONCILED,
    SUPERSEDE_REASON_DEFINITION,
    SUPERSEDE_REASON_EVIDENCE,
    areconcile_dataset_staleness,
    supersede_stale_active_promotions,
)
from runtime.dataset_curation_projection_subscriber import EVENT_PROMOTION_SUPERSEDED


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_a) -> bool:
        return False


@dataclass
class _FakeConn:
    """Minimal asyncpg-shaped fake.

    The reconciler runs three large SQL statements; we route by substring
    so each call returns the right pre-canned rows.
    """

    definition_stale_ids: list[str] = field(default_factory=list)
    evidence_stale_ids: list[str] = field(default_factory=list)
    active_promotions: list[dict[str, Any]] = field(default_factory=list)
    fetched: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)
    executed: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)
    closed: bool = False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        return "OK"

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self.fetched.append((query, args))
        if "SET staleness_status = 'definition_stale'" in query:
            return [{"candidate_id": cid} for cid in self.definition_stale_ids]
        if "SET staleness_status = 'evidence_stale'" in query:
            return [{"candidate_id": cid} for cid in self.evidence_stale_ids]
        if "FROM dataset_promotions p" in query and "unnest(p.candidate_ids)" in query:
            requested = set(args[0]) if args else set()
            out = []
            for p in self.active_promotions:
                if any(c in requested for c in p["candidate_ids"]):
                    out.append(p)
            return out
        return []

    async def fetchrow(self, _query: str, *_args: Any) -> Any:
        return None

    async def close(self) -> None:
        self.closed = True


@dataclass
class _Recorder:
    emitted: list[dict[str, Any]] = field(default_factory=list)
    invalidated: list[dict[str, Any]] = field(default_factory=list)
    next_id: int = 5_000_000

    async def aemit(self, _conn, *, channel, event_type, entity_id, entity_kind, payload, emitted_by):
        self.emitted.append(
            {
                "channel": channel,
                "event_type": event_type,
                "entity_id": entity_id,
                "payload": dict(payload),
            }
        )
        eid = self.next_id
        self.next_id += 1
        return eid

    async def aemit_cache_invalidation(self, _conn, *, cache_kind, cache_key, reason, invalidated_by):
        self.invalidated.append({"cache_kind": cache_kind, "cache_key": cache_key})


def _install(monkeypatch, conn: _FakeConn) -> tuple[_Recorder, Any]:
    recorder = _Recorder()
    monkeypatch.setattr(staleness_module, "aemit", recorder.aemit)
    monkeypatch.setattr(
        staleness_module, "aemit_cache_invalidation", recorder.aemit_cache_invalidation
    )

    async def _connect(_env):
        return conn

    return recorder, _connect


def test_clean_db_emits_nothing(monkeypatch) -> None:
    conn = _FakeConn()
    recorder, connect = _install(monkeypatch, conn)
    result = asyncio.run(
        areconcile_dataset_staleness(connect_database=connect)
    )
    assert result["definition_stale_candidates"] == []
    assert result["evidence_stale_candidates"] == []
    assert result["superseded_promotions"] == []
    assert recorder.emitted == []
    assert recorder.invalidated == []


def test_definition_stale_supersedes_active_promotion(monkeypatch) -> None:
    conn = _FakeConn(
        definition_stale_ids=["cand_def_1"],
        active_promotions=[
            {
                "promotion_id": "prom_1",
                "candidate_ids": ["cand_def_1"],
                "dataset_family": "sft",
                "specialist_target": "slm/review",
                "policy_id": "pol_1",
                "payload": json.dumps({"prompt": {}, "target_output": {}}),
                "split_tag": "train",
                "staleness_status_seen": "definition_stale",
            }
        ],
    )
    recorder, connect = _install(monkeypatch, conn)

    result = asyncio.run(areconcile_dataset_staleness(connect_database=connect))

    assert result["definition_stale_candidates"] == ["cand_def_1"]
    assert len(result["superseded_promotions"]) == 1
    superseded = result["superseded_promotions"][0]
    assert superseded["promotion_id"] == "prom_1"
    assert superseded["tombstone_id"].startswith("prom_tomb_")

    # The supersede event went onto CHANNEL_DATASET with the right reason.
    supersede_events = [
        e for e in recorder.emitted if e["event_type"] == EVENT_PROMOTION_SUPERSEDED
    ]
    assert len(supersede_events) == 1
    assert supersede_events[0]["payload"]["reason"] == SUPERSEDE_REASON_DEFINITION
    assert supersede_events[0]["payload"]["promotion_id"] == "prom_1"

    # A staleness-changed event also fired.
    assert any(
        e["event_type"] == EVENT_CANDIDATE_STALENESS_CHANGED for e in recorder.emitted
    )

    # Top-level reconciled event + cache invalidation.
    assert any(e["event_type"] == EVENT_STALENESS_RECONCILED for e in recorder.emitted)
    assert recorder.invalidated and recorder.invalidated[0]["cache_kind"] == "dataset_curated_projection"

    # Tombstone INSERT and original UPDATE both ran.
    queries = [q for q, _ in conn.executed]
    assert any("INSERT INTO dataset_promotions" in q for q in queries)
    assert any("UPDATE dataset_promotions" in q for q in queries)


def test_evidence_stale_uses_evidence_reason(monkeypatch) -> None:
    conn = _FakeConn(
        evidence_stale_ids=["cand_ev_1"],
        active_promotions=[
            {
                "promotion_id": "prom_2",
                "candidate_ids": ["cand_ev_1"],
                "dataset_family": "sft",
                "specialist_target": "slm/review",
                "policy_id": "pol_1",
                "payload": json.dumps({}),
                "split_tag": "train",
                "staleness_status_seen": "evidence_stale",
            }
        ],
    )
    recorder, connect = _install(monkeypatch, conn)

    asyncio.run(areconcile_dataset_staleness(connect_database=connect))

    supersede_events = [
        e for e in recorder.emitted if e["event_type"] == EVENT_PROMOTION_SUPERSEDED
    ]
    assert len(supersede_events) == 1
    assert supersede_events[0]["payload"]["reason"] == SUPERSEDE_REASON_EVIDENCE


def test_evidence_stale_matches_canonical_wont_fix_status(monkeypatch) -> None:
    conn = _FakeConn()
    _recorder, connect = _install(monkeypatch, conn)

    asyncio.run(areconcile_dataset_staleness(connect_database=connect))

    evidence_queries = [
        query for query, _args in conn.fetched
        if "wontfix_bugs" in query
    ]
    assert evidence_queries
    assert "UPPER(status) = 'WONT_FIX'" in evidence_queries[0]
    assert "status = 'wont_fix'" not in evidence_queries[0]


def test_supersede_skips_when_no_affected_candidates(monkeypatch) -> None:
    conn = _FakeConn()
    recorder, _ = _install(monkeypatch, conn)
    superseded = asyncio.run(
        supersede_stale_active_promotions(conn, affected_candidate_ids=[])
    )
    assert superseded == []
    assert recorder.emitted == []


def test_stale_candidate_with_no_active_promotion_is_a_noop(monkeypatch) -> None:
    conn = _FakeConn(definition_stale_ids=["cand_orphan"])
    recorder, connect = _install(monkeypatch, conn)
    result = asyncio.run(areconcile_dataset_staleness(connect_database=connect))
    assert result["definition_stale_candidates"] == ["cand_orphan"]
    assert result["superseded_promotions"] == []
    # Staleness-changed event still fires for the candidate, plus reconciled summary.
    types = [e["event_type"] for e in recorder.emitted]
    assert EVENT_CANDIDATE_STALENESS_CHANGED in types
    assert EVENT_STALENESS_RECONCILED in types
    assert EVENT_PROMOTION_SUPERSEDED not in types
