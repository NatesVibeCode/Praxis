from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from surfaces.api import operator_write


class _FakeConn:
    def __init__(self) -> None:
        self.event_rows: list[dict[str, object]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.route_row = {
            "task_type": "materialize",
            "sub_task_type": "*",
            "provider_slug": "openai",
            "model_slug": "gpt-5.4",
            "transport_type": "API",
            "temperature": 0.0,
            "max_tokens": 4096,
            "reasoning_control": {},
            "request_contract_ref": None,
            "cache_policy": {},
            "structured_output_policy": {},
            "streaming_policy": {},
            "updated_at": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
        }

    def transaction(self):
        class _Tx:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return _Tx()

    async def fetchrow(self, query: str, *args: object):
        self.fetchrow_calls.append((query, args))
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
        if "UPDATE task_type_routing" in query:
            updated = dict(self.route_row)
            for index, arg in enumerate(args, start=1):
                placeholder = f"${index}"
                if f"temperature = {placeholder}" in query:
                    updated["temperature"] = arg
                if f"max_tokens = {placeholder}" in query:
                    updated["max_tokens"] = arg
            updated["updated_at"] = datetime(2026, 4, 14, 12, 1, tzinfo=timezone.utc)
            self.route_row = updated
            return updated
        return None

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((query, args))
        if "FROM registry_native_runtime_profile_authority" in query:
            return [
                {"runtime_profile_ref": "praxis"},
                {"runtime_profile_ref": "scratch_agent"},
            ]
        if "FROM task_type_routing" in query:
            return [dict(self.route_row)]
        return []

    async def execute(self, _query: str, *_args: object) -> str:
        self.execute_calls.append((_query, _args))
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
    refresh_calls = [
        (query, args)
        for query, args in conn.execute_calls
        if "refresh_private_provider_job_catalog" in query
        or "refresh_private_provider_control_plane_snapshot" in query
    ]
    assert refresh_calls == [
        ("SELECT refresh_private_provider_job_catalog($1)", ("praxis",)),
        ("SELECT refresh_private_provider_control_plane_snapshot($1)", ("praxis",)),
        ("SELECT refresh_private_provider_job_catalog($1)", ("scratch_agent",)),
        ("SELECT refresh_private_provider_control_plane_snapshot($1)", ("scratch_agent",)),
    ]


def test_task_route_request_write_invalidates_routes_without_touching_eligibility(
    monkeypatch,
) -> None:
    invalidated: list[str] = []
    conn = _FakeConn()
    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=lambda _env: asyncio.sleep(0, result=conn),
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
        frontdoor._set_task_route_request(
            env={"WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis"},
            task_type="materialize",
            provider_slug="openai",
            model_slug="gpt-5.4",
            sub_task_type="*",
            transport_type="API",
            fields_to_update=frozenset({"temperature", "max_tokens"}),
            temperature=0.2,
            clear_temperature=False,
            max_tokens=32768,
            clear_max_tokens=False,
            reasoning_control=None,
            clear_reasoning_control=False,
            request_contract_ref=None,
            clear_request_contract_ref=False,
            cache_policy=None,
            clear_cache_policy=False,
            structured_output_policy=None,
            clear_structured_output_policy=False,
            streaming_policy=None,
            clear_streaming_policy=False,
            reason_code="request_contract_tuning",
            rationale="tune request shape",
            decision_ref="decision.route.request",
        )
    )

    assert result.mutated_fields == ("max_tokens", "temperature")
    assert result.route_request_before["max_tokens"] == 4096
    assert result.route_request_after["max_tokens"] == 32768
    assert result.route_request_after["temperature"] == 0.2
    assert invalidated == ["workflow_pool:postgresql://localhost:5432/praxis"]
    assert not any(
        "task_type_route_eligibility" in query
        for query, _args in conn.fetch_calls + conn.fetchrow_calls + conn.execute_calls
    )
    cache_events = [e for e in conn.event_rows if e["channel"] == "cache_invalidation"]
    assert cache_events[0]["emitted_by"] == "operator_write.set_task_route_request"
    assert cache_events[0]["payload"]["reason"] == "task_route_request_write"
