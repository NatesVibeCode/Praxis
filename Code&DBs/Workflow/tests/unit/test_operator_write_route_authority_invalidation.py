from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from surfaces.api import operator_write


class _FakeConn:
    def __init__(self) -> None:
        self.event_rows: list[dict[str, object]] = []

    async def fetchrow(self, query: str, *args: object):
        if "INSERT INTO event_log" in query:
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
        return None

    async def execute(self, _query: str, *_args: object) -> str:
        return "OK"

    async def close(self) -> None:
        return None


class _FakeRepository:
    async def record_task_route_eligibility_window(self, **kwargs):
        effective_from = kwargs["effective_from"]
        return (
            {
                "task_route_eligibility_id": kwargs["task_route_eligibility_id"],
                "task_type": kwargs["task_type"],
                "provider_slug": kwargs["provider_slug"],
                "model_slug": kwargs["model_slug"],
                "eligibility_status": kwargs["eligibility_status"],
                "reason_code": kwargs["reason_code"],
                "rationale": kwargs["rationale"],
                "effective_from": effective_from,
                "effective_to": kwargs["effective_to"],
                "decision_ref": kwargs["decision_ref"],
                "created_at": effective_from,
            },
            (),
        )


def test_task_route_write_invalidates_target_authority_cache(monkeypatch) -> None:
    invalidated: list[str] = []
    conn = _FakeConn()
    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=lambda _env: asyncio.sleep(0, result=conn),
        task_route_eligibility_repository_factory=lambda _conn: _FakeRepository(),
    )
    monkeypatch.setattr(
        operator_write,
        "resolve_workflow_authority_cache_key",
        lambda env=None: f"workflow_pool:{(env or {}).get('WORKFLOW_DATABASE_URL', 'missing')}",
    )
    monkeypatch.setattr(
        operator_write,
        "invalidate_route_authority_cache_key",
        lambda cache_key: invalidated.append(cache_key),
    )

    result = asyncio.run(
        frontdoor._set_task_route_eligibility_window(
            env={"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis"},
            provider_slug="anthropic",
            eligibility_status="eligible",
            effective_to=None,
            task_type="build",
            model_slug="claude",
            reason_code="operator_allow",
            rationale="allow route",
            effective_from=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
            decision_ref="decision.route.allow",
        )
    )

    assert result.task_route_eligibility.provider_slug == "anthropic"
    assert invalidated == ["workflow_pool:postgresql://localhost:5432/praxis"]
    cache_events = [e for e in conn.event_rows if e["channel"] == "cache_invalidation"]
    assert len(cache_events) == 1
    event = cache_events[0]
    assert event["event_type"] == "cache_invalidated"
    assert event["entity_kind"] == "route_authority_snapshot"
    assert event["entity_id"] == "workflow_pool:postgresql://localhost:5432/praxis"
    assert event["emitted_by"] == "operator_write.set_task_route_eligibility"
    assert event["payload"]["reason"] == "task_route_eligibility_window_write"
    assert event["payload"]["decision_ref"] == "decision.route.allow"
