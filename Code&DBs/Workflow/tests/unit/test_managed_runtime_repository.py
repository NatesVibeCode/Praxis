from __future__ import annotations

from storage.postgres import managed_runtime_repository as repo


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "INSERT INTO managed_runtime_records" in sql:
            return {
                "runtime_record_id": args[0],
                "run_id": args[1],
                "receipt_id": args[2],
                "tenant_ref": args[3],
                "environment_ref": args[4],
                "workflow_ref": args[5],
                "execution_mode": args[9],
                "terminal_status": args[10],
                "cost_status": args[16],
                "cost_amount": args[17],
            }
        return None

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


def _receipt() -> dict[str, object]:
    return {
        "receipt_id": "run_receipt.demo",
        "receipt_version_ref": "run_receipt.v1",
        "correction_of_receipt_id": None,
        "identity": {
            "run_id": "run.managed.phase10",
            "tenant_ref": "tenant.acme",
            "environment_ref": "env.prod",
            "workflow_ref": "workflow.object_truth",
            "workload_class": "workflow_build",
            "attempt": 1,
        },
        "configured_mode": "managed",
        "execution_mode": "managed",
        "runtime_version_ref": "runtime.managed.v1",
        "runtime_pool_ref": "pool.internal.us-east-1.a",
        "started_at": "2026-04-30T12:00:00Z",
        "ended_at": "2026-04-30T12:01:00Z",
        "duration_seconds": "60.000",
        "terminal_status": "succeeded",
        "error_classification": None,
        "cost_summary": {
            "status": "finalized",
            "currency": "USD",
            "amount": "0.240000",
            "pricing_schedule_version_ref": "pricing.managed.2026-04-30",
            "calculation_basis": "pricing_schedule_version",
        },
        "policy_reason_code": "managed_selected",
        "policy_decision_refs": ["decision.managed-runtime.phase-10"],
        "generated_at": "2026-04-30T12:01:01Z",
        "finalized_at": "2026-04-30T12:01:01Z",
        "execution_labels": ["phase-10"],
    }


def _usage_summary() -> dict[str, object]:
    return {
        "run_id": "run.managed.phase10",
        "tenant_ref": "tenant.acme",
        "environment_ref": "env.prod",
        "workflow_ref": "workflow.object_truth",
        "execution_mode": "managed",
        "runtime_version_ref": "runtime.managed.v1",
        "started_at": "2026-04-30T12:00:00Z",
        "ended_at": "2026-04-30T12:01:00Z",
        "duration_seconds": "60.000",
        "billable_wall_seconds": "60.000",
        "billable_cpu_core_seconds": "120.000",
        "billable_memory_gib_seconds": "240.000",
        "billable_accelerator_seconds": "0.000",
        "metered_event_count": 3,
        "duplicate_meter_event_count": 0,
        "diagnostic_event_count": 0,
        "cost_summary": _receipt()["cost_summary"],
    }


def test_persist_managed_runtime_record_writes_parent_and_facets() -> None:
    conn = _RecordingConn()

    persisted = repo.persist_managed_runtime_record(
        conn,
        runtime_record_id="managed_runtime_record.demo",
        receipt=_receipt(),
        usage_summary=_usage_summary(),
        mode_selection={
            "allowed": True,
            "configured_mode": "managed",
            "execution_mode": "managed",
            "reason_code": "managed_selected",
            "reason": "eligible",
            "identity": _receipt()["identity"],
            "policy_decision_refs": ["decision.managed-runtime.phase-10"],
        },
        meter_events=[
            {
                "event_id": "event.usage",
                "idempotency_key": "usage",
                "run_id": "run.managed.phase10",
                "tenant_ref": "tenant.acme",
                "environment_ref": "env.prod",
                "workflow_ref": "workflow.object_truth",
                "execution_mode": "managed",
                "runtime_version_ref": "runtime.managed.v1",
                "occurred_at": "2026-04-30T12:00:30Z",
                "event_kind": "resource_usage",
                "billable": True,
                "receipt_id": None,
                "source_event_ref": None,
            }
        ],
        pricing_schedule={
            "schedule_ref": "pricing.managed-runtime",
            "version_ref": "pricing.managed.2026-04-30",
            "effective_at": "2026-04-30T12:00:00Z",
            "currency": "USD",
            "cpu_core_second_rate": "0.001000",
            "memory_gib_second_rate": "0.000500",
            "accelerator_second_rate": "0.000000",
            "minimum_charge": "0.000000",
        },
        heartbeats=[
            {
                "worker_ref": "worker.internal.1",
                "pool_ref": "pool.internal.us-east-1.a",
                "tenant_ref": "tenant.acme",
                "environment_ref": "env.prod",
                "runtime_version_ref": "runtime.managed.v1",
                "observed_at": "2026-04-30T12:01:00Z",
                "capacity_slots": 4,
                "active_runs": 1,
                "accepting_work": True,
                "last_error_code": None,
            }
        ],
        pool_health={
            "pool_ref": "pool.internal.us-east-1.a",
            "tenant_ref": "tenant.acme",
            "environment_ref": "env.prod",
            "state": "healthy",
            "evaluated_at": "2026-04-30T12:01:01Z",
            "dispatch_allowed": True,
            "reason_codes": [],
        },
        audit_events=[
            {
                "audit_event_id": "audit.managed.phase10",
                "occurred_at": "2026-04-30T12:01:01Z",
                "actor_ref": "operator:nate",
                "action": "record",
                "target_kind": "managed_runtime_record",
                "target_ref": "managed_runtime_record.demo",
                "tenant_ref": "tenant.acme",
                "environment_ref": "env.prod",
                "reason_code": "phase_10_live_proof",
                "run_id": "run.managed.phase10",
            }
        ],
        customer_observability={"run_id": "run.managed.phase10", "status": "succeeded"},
        internal_audit={"contract": "managed_runtime.internal_audit.v1"},
        observed_by_ref="operator:nate",
        source_ref="phase_10_test",
    )

    assert "INSERT INTO managed_runtime_records" in conn.fetchrow_calls[0][0]
    assert persisted["runtime_record_id"] == "managed_runtime_record.demo"
    assert persisted["execution_mode"] == "managed"
    assert any("managed_runtime_pricing_schedule_versions" in call[0] for call in conn.execute_calls)
    assert any("DELETE FROM managed_runtime_meter_events" in call[0] for call in conn.execute_calls)
    assert any("INSERT INTO managed_runtime_meter_events" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO managed_runtime_heartbeats" in call[0] for call in conn.batch_calls)
    assert any("INSERT INTO managed_runtime_pool_health_snapshots" in call[0] for call in conn.execute_calls)
    assert any("INSERT INTO managed_runtime_audit_events" in call[0] for call in conn.batch_calls)


def test_managed_runtime_repository_lists_records_and_facets() -> None:
    conn = _RecordingConn()

    records = repo.list_managed_runtime_records(
        conn,
        tenant_ref="tenant.acme",
        execution_mode="managed",
    )
    meter_events = repo.list_managed_runtime_meter_events(
        conn,
        run_id="run.managed.phase10",
        event_kind="resource_usage",
    )
    pool_health = repo.list_managed_runtime_pool_health(
        conn,
        pool_ref="pool.internal.us-east-1.a",
        health_state="healthy",
    )

    assert records == []
    assert meter_events == []
    assert pool_health == []
    assert "FROM managed_runtime_records" in conn.fetch_calls[0][0]
    assert "FROM managed_runtime_meter_events" in conn.fetch_calls[1][0]
    assert "FROM managed_runtime_pool_health_snapshots" in conn.fetch_calls[2][0]
