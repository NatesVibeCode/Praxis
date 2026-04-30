from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime.operations.commands import managed_runtime as commands
from runtime.operations.queries import managed_runtime as queries


NOW = "2026-04-30T12:00:00Z"


def _subsystems():
    return SimpleNamespace(get_pg_conn=lambda: object())


def _identity(workload_class: str = "workflow_build") -> dict[str, object]:
    return {
        "run_id": "run.managed.phase10",
        "tenant_ref": "tenant.acme",
        "environment_ref": "env.prod",
        "workflow_ref": "workflow.object_truth",
        "workload_class": workload_class,
        "attempt": 1,
    }


def _policy(configured_mode: str = "managed") -> dict[str, object]:
    return {
        "tenant_ref": "tenant.acme",
        "environment_ref": "env.prod",
        "configured_mode": configured_mode,
        "managed_workload_classes": ["workflow_build"],
        "policy_decision_refs": ["decision.managed-runtime.phase-10"],
    }


def _meter_events() -> list[dict[str, object]]:
    return [
        {"event_kind": "run_started", "occurred_at": NOW, "idempotency_key": "start"},
        {
            "event_kind": "resource_usage",
            "occurred_at": "2026-04-30T12:00:30Z",
            "idempotency_key": "usage",
            "wall_seconds": "60",
            "cpu_core_seconds": "120",
            "memory_gib_seconds": "240",
        },
        {
            "event_kind": "run_finished",
            "occurred_at": "2026-04-30T12:01:00Z",
            "idempotency_key": "finish",
        },
    ]


def _pricing_schedule() -> dict[str, object]:
    return {
        "schedule_ref": "pricing.managed-runtime",
        "version_ref": "pricing.managed.2026-04-30",
        "effective_at": NOW,
        "currency": "USD",
        "cpu_core_second_rate": "0.001",
        "memory_gib_second_rate": "0.0005",
    }


def test_record_managed_runtime_persists_receipt_cost_and_health(monkeypatch) -> None:
    persist_calls: list[dict[str, object]] = []

    def _persist(conn, **kwargs):
        persist_calls.append(kwargs)
        return {
            "runtime_record_id": kwargs["runtime_record_id"],
            "run_id": kwargs["receipt"]["identity"]["run_id"],
            "execution_mode": kwargs["mode_selection"]["execution_mode"],
        }

    monkeypatch.setattr(commands, "persist_managed_runtime_record", _persist)

    result = commands.handle_record_managed_runtime(
        commands.RecordManagedRuntimeCommand(
            identity=_identity(),
            policy=_policy(),
            meter_events=_meter_events(),
            terminal_status="succeeded",
            generated_at="2026-04-30T12:01:01Z",
            runtime_version_ref="runtime.managed.v1",
            runtime_pool_ref="pool.internal.us-east-1.a",
            pricing_schedule=_pricing_schedule(),
            heartbeats=[
                {
                    "worker_ref": "worker.internal.1",
                    "pool_ref": "pool.internal.us-east-1.a",
                    "observed_at": "2026-04-30T12:01:00Z",
                    "capacity_slots": 4,
                    "active_runs": 1,
                }
            ],
            source_ref="phase_10_test",
        ),
        _subsystems(),
    )

    assert result["ok"] is True
    assert result["operation"] == "authority.managed_runtime.record"
    assert result["execution_mode"] == "managed"
    assert result["cost_summary"]["amount"] == "0.240000"
    assert result["pool_health"]["state"] == "healthy"
    assert result["event_payload"]["metered_event_count"] == 3
    assert result["event_payload"]["pool_health_state"] == "healthy"
    assert persist_calls[0]["pricing_schedule"]["version_ref"] == "pricing.managed.2026-04-30"
    assert persist_calls[0]["pool_health"]["tenant_ref"] == "tenant.acme"
    assert len(persist_calls[0]["meter_events"]) == 3
    assert all(event["receipt_id"] == result["receipt_id"] for event in persist_calls[0]["meter_events"])


def test_record_exported_runtime_keeps_managed_cost_out_of_customer_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        commands,
        "persist_managed_runtime_record",
        lambda conn, **kwargs: {"runtime_record_id": kwargs["runtime_record_id"]},
    )

    result = commands.handle_record_managed_runtime(
        commands.RecordManagedRuntimeCommand(
            identity=_identity(),
            policy=_policy(configured_mode="hybrid"),
            requested_mode="exported",
            meter_events=_meter_events(),
            terminal_status="succeeded",
            generated_at="2026-04-30T12:01:01Z",
            runtime_version_ref="runtime.exported.v1",
        ),
        _subsystems(),
    )

    assert result["execution_mode"] == "exported"
    assert result["cost_summary"]["status"] == "not_applicable"
    assert "cost" not in result["customer_observability"]


def test_record_managed_runtime_fails_fast_when_policy_denies_placement(monkeypatch) -> None:
    monkeypatch.setattr(
        commands,
        "persist_managed_runtime_record",
        lambda conn, **kwargs: {"runtime_record_id": kwargs["runtime_record_id"]},
    )

    with pytest.raises(ValueError, match="managed_runtime.placement_denied:managed_workload_unsupported"):
        commands.handle_record_managed_runtime(
            commands.RecordManagedRuntimeCommand(
                identity=_identity(workload_class="unsupported_job"),
                policy=_policy(),
                meter_events=_meter_events(),
                terminal_status="failed",
                generated_at="2026-04-30T12:01:01Z",
                runtime_version_ref="runtime.managed.v1",
            ),
            _subsystems(),
        )


def test_read_managed_runtime_routes_to_meter_event_listing(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _list_meter_events(conn, **kwargs):
        captured.update(kwargs)
        return [{"event_id": "event.1"}]

    monkeypatch.setattr(queries, "list_managed_runtime_meter_events", _list_meter_events)

    result = queries.handle_read_managed_runtime(
        queries.ReadManagedRuntimeQuery(
            action="list_meter_events",
            run_id="run.managed.phase10",
            event_kind="resource_usage",
            limit=10,
        ),
        _subsystems(),
    )

    assert result["ok"] is True
    assert result["action"] == "list_meter_events"
    assert result["count"] == 1
    assert captured["run_id"] == "run.managed.phase10"
    assert captured["event_kind"] == "resource_usage"
    assert captured["limit"] == 10
