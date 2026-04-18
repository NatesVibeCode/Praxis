from __future__ import annotations

import asyncio
import json
from typing import Any

from runtime.cache_invalidation import (
    CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE,
    CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT,
    EVENT_CACHE_INVALIDATED,
    aemit_cache_invalidation,
)


class _FakeConn:
    def __init__(self) -> None:
        self.event_rows: list[dict[str, Any]] = []
        self.notifications: list[tuple[object, ...]] = []

    async def fetchrow(self, query: str, *args: object) -> Any:
        if "INSERT INTO event_log" not in query:
            raise AssertionError(f"unexpected query: {query}")
        row = {
            "channel": args[0],
            "event_type": args[1],
            "entity_id": args[2],
            "entity_kind": args[3],
            "payload": json.loads(args[4]),
            "emitted_by": args[5],
        }
        self.event_rows.append(row)
        return {"id": len(self.event_rows)}

    async def execute(self, query: str, *args: object) -> str:
        if "pg_notify" in query:
            self.notifications.append(args)
            return "NOTIFY"
        raise AssertionError(f"unexpected execute: {query}")


def test_aemit_cache_invalidation_writes_to_cache_invalidation_channel() -> None:
    conn = _FakeConn()

    event_id = asyncio.run(
        aemit_cache_invalidation(
            conn,
            cache_kind=CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT,
            cache_key="workflow_pool:praxis",
            reason="task_route_eligibility_window_write",
            invalidated_by="operator_write.set_task_route_eligibility",
            decision_ref="decision.route.allow",
        )
    )

    assert event_id == 1
    event = conn.event_rows[0]
    assert event["channel"] == "cache_invalidation"
    assert event["event_type"] == EVENT_CACHE_INVALIDATED
    assert event["entity_id"] == "workflow_pool:praxis"
    assert event["entity_kind"] == CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT
    assert event["emitted_by"] == "operator_write.set_task_route_eligibility"
    assert event["payload"] == {
        "cache_kind": CACHE_KIND_ROUTE_AUTHORITY_SNAPSHOT,
        "cache_key": "workflow_pool:praxis",
        "reason": "task_route_eligibility_window_write",
        "decision_ref": "decision.route.allow",
    }


def test_aemit_cache_invalidation_omits_decision_ref_when_absent() -> None:
    conn = _FakeConn()

    asyncio.run(
        aemit_cache_invalidation(
            conn,
            cache_kind=CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE,
            cache_key="openai",
            reason="circuit_breaker_override_reset",
            invalidated_by="operator_write.set_circuit_breaker_override",
        )
    )

    event = conn.event_rows[0]
    assert event["entity_kind"] == CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE
    assert event["payload"] == {
        "cache_kind": CACHE_KIND_CIRCUIT_BREAKER_OVERRIDE,
        "cache_key": "openai",
        "reason": "circuit_breaker_override_reset",
    }
    assert "decision_ref" not in event["payload"]
