from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from runtime.managed_runtime import (
    CostStatus,
    ExecutionMode,
    ExecutionModePolicy,
    ManagedRuntimePolicyError,
    MeterEventKind,
    PoolHealthState,
    PricingScheduleVersion,
    RunIdentity,
    RunMeterEvent,
    RunPlacementRequest,
    RunTerminalStatus,
    RuntimeHeartbeat,
    build_internal_audit_contract,
    build_usage_summary,
    customer_observability_summary,
    derive_pool_health,
    finalize_run_receipt,
    select_execution_mode,
)


NOW = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)


def _identity(*, workload_class: str = "workflow_build") -> RunIdentity:
    return RunIdentity(
        run_id="run.managed.1",
        tenant_ref="tenant.acme",
        environment_ref="env.prod",
        workflow_ref="workflow.object_truth",
        workload_class=workload_class,
        attempt=1,
    )


def _policy(
    *,
    configured_mode: ExecutionMode = ExecutionMode.MANAGED,
    managed_workloads: frozenset[str] = frozenset({"workflow_build"}),
) -> ExecutionModePolicy:
    return ExecutionModePolicy(
        tenant_ref="tenant.acme",
        environment_ref="env.prod",
        configured_mode=configured_mode,
        managed_workload_classes=managed_workloads,
        policy_decision_refs=("decision.managed-runtime.phase-10",),
    )


def _event(
    *,
    kind: MeterEventKind,
    at: datetime,
    key: str,
    mode: ExecutionMode = ExecutionMode.MANAGED,
    wall: Decimal | int = 0,
    cpu: Decimal | int = 0,
    memory: Decimal | int = 0,
) -> RunMeterEvent:
    identity = _identity()
    return RunMeterEvent(
        event_id=f"event.{key}",
        idempotency_key=key,
        run_id=identity.run_id,
        tenant_ref=identity.tenant_ref,
        environment_ref=identity.environment_ref,
        workflow_ref=identity.workflow_ref,
        execution_mode=mode,
        runtime_version_ref="runtime.managed.v1",
        occurred_at=at,
        event_kind=kind,
        wall_seconds=wall,
        cpu_core_seconds=cpu,
        memory_gib_seconds=memory,
    )


def _schedule(version_ref: str = "pricing.managed.2026-04-30") -> PricingScheduleVersion:
    return PricingScheduleVersion(
        schedule_ref="pricing.managed-runtime",
        version_ref=version_ref,
        effective_at=NOW,
        currency="USD",
        cpu_core_second_rate=Decimal("0.001"),
        memory_gib_second_rate=Decimal("0.0005"),
    )


def _managed_summary():
    return build_usage_summary(
        [
            _event(kind=MeterEventKind.RUN_STARTED, at=NOW, key="start"),
            _event(
                kind=MeterEventKind.RESOURCE_USAGE,
                at=NOW + timedelta(seconds=30),
                key="usage",
                wall=60,
                cpu=120,
                memory=240,
            ),
            _event(kind=MeterEventKind.RUN_FINISHED, at=NOW + timedelta(seconds=60), key="finish"),
        ],
        pricing_schedule=_schedule(),
        cost_status=CostStatus.FINALIZED,
    )


def test_managed_mode_selected_for_supported_workload() -> None:
    selection = select_execution_mode(
        RunPlacementRequest(identity=_identity()),
        _policy(),
    )

    assert selection.allowed is True
    assert selection.configured_mode is ExecutionMode.MANAGED
    assert selection.execution_mode is ExecutionMode.MANAGED
    assert selection.reason_code == "managed_selected"


def test_unsupported_managed_run_fails_fast() -> None:
    selection = select_execution_mode(
        RunPlacementRequest(identity=_identity(workload_class="unsupported_job")),
        _policy(),
    )

    assert selection.allowed is False
    assert selection.execution_mode is None
    assert selection.reason_code == "managed_workload_unsupported"
    with pytest.raises(ManagedRuntimePolicyError):
        selection.require_allowed()


def test_hybrid_routes_unsupported_managed_workload_to_exported() -> None:
    selection = select_execution_mode(
        RunPlacementRequest(identity=_identity(workload_class="customer_hosted_job")),
        _policy(configured_mode=ExecutionMode.HYBRID),
    )

    assert selection.allowed is True
    assert selection.configured_mode is ExecutionMode.HYBRID
    assert selection.execution_mode is ExecutionMode.EXPORTED
    assert selection.reason_code == "hybrid_routed_exported"


def test_metering_dedupes_by_idempotency_key_and_prices_schedule_version() -> None:
    usage = _event(
        kind=MeterEventKind.RESOURCE_USAGE,
        at=NOW + timedelta(seconds=30),
        key="usage",
        wall=60,
        cpu=120,
        memory=240,
    )
    duplicate = _event(
        kind=MeterEventKind.RESOURCE_USAGE,
        at=NOW + timedelta(seconds=30),
        key="usage",
        wall=999,
        cpu=999,
        memory=999,
    )

    summary = build_usage_summary(
        [
            _event(kind=MeterEventKind.RUN_STARTED, at=NOW, key="start"),
            usage,
            duplicate,
            _event(kind=MeterEventKind.RUN_FINISHED, at=NOW + timedelta(seconds=60), key="finish"),
        ],
        pricing_schedule=_schedule(),
        cost_status=CostStatus.FINALIZED,
    )

    assert summary.metered_event_count == 3
    assert summary.duplicate_meter_event_count == 1
    assert summary.billable_cpu_core_seconds == Decimal("120.000")
    assert summary.billable_memory_gib_seconds == Decimal("240.000")
    assert summary.cost_summary.pricing_schedule_version_ref == "pricing.managed.2026-04-30"
    assert summary.cost_summary.amount == Decimal("0.240000")
    assert summary.cost_summary.status is CostStatus.FINALIZED


def test_receipt_finalization_is_deterministic_and_versioned() -> None:
    identity = _identity()
    selection = select_execution_mode(RunPlacementRequest(identity=identity), _policy())
    summary = _managed_summary()

    first = finalize_run_receipt(
        identity=identity,
        selection=selection,
        usage_summary=summary,
        terminal_status=RunTerminalStatus.SUCCEEDED,
        generated_at=NOW + timedelta(seconds=61),
        runtime_pool_ref="pool.internal.us-east-1.a",
        execution_labels=("phase-10",),
    )
    second = finalize_run_receipt(
        identity=identity,
        selection=selection,
        usage_summary=summary,
        terminal_status=RunTerminalStatus.SUCCEEDED,
        generated_at=NOW + timedelta(seconds=62),
        runtime_pool_ref="pool.internal.us-east-1.a",
        execution_labels=("phase-10",),
    )

    assert first.receipt_id == second.receipt_id
    assert first.receipt_version_ref == "run_receipt.v1"
    assert first.finalized_at == NOW + timedelta(seconds=61)
    assert first.cost_summary.pricing_schedule_version_ref == "pricing.managed.2026-04-30"


def test_exported_summary_does_not_emit_managed_runtime_cost() -> None:
    identity = _identity()
    selection = select_execution_mode(
        RunPlacementRequest(identity=identity, requested_mode=ExecutionMode.EXPORTED),
        _policy(),
    )
    summary = build_usage_summary(
        [
            _event(kind=MeterEventKind.RUN_STARTED, at=NOW, key="start", mode=ExecutionMode.EXPORTED),
            _event(kind=MeterEventKind.RUN_FINISHED, at=NOW + timedelta(seconds=20), key="finish", mode=ExecutionMode.EXPORTED),
        ],
        pricing_schedule=_schedule(),
    )
    receipt = finalize_run_receipt(
        identity=identity,
        selection=selection,
        usage_summary=summary,
        terminal_status=RunTerminalStatus.SUCCEEDED,
        generated_at=NOW + timedelta(seconds=21),
    )

    customer = customer_observability_summary(receipt=receipt)

    assert receipt.cost_summary.status is CostStatus.NOT_APPLICABLE
    assert "cost" not in customer


def test_stale_heartbeat_health_blocks_dispatch() -> None:
    heartbeat = RuntimeHeartbeat(
        worker_ref="worker.internal.1",
        pool_ref="pool.internal.us-east-1.a",
        tenant_ref="tenant.acme",
        environment_ref="env.prod",
        runtime_version_ref="runtime.managed.v1",
        observed_at=NOW - timedelta(seconds=70),
        capacity_slots=4,
        active_runs=0,
    )

    health = derive_pool_health(
        [heartbeat],
        pool_ref="pool.internal.us-east-1.a",
        now=NOW,
        heartbeat_fresh_seconds=30,
        unavailable_after_seconds=120,
        clock_skew_grace_seconds=0,
    )

    assert health.state is PoolHealthState.STALE
    assert health.dispatch_allowed is False
    assert health.reason_codes == ("some_workers_stale", "stale_heartbeat")
    customer = health.to_customer_dict()
    assert "pool_ref" not in customer
    assert customer["reason_codes"] == ("runtime_capacity_degraded", "runtime_capacity_stale")


def test_customer_summary_redacts_internal_runtime_identifiers() -> None:
    identity = _identity()
    selection = select_execution_mode(RunPlacementRequest(identity=identity), _policy())
    summary = _managed_summary()
    receipt = finalize_run_receipt(
        identity=identity,
        selection=selection,
        usage_summary=summary,
        terminal_status=RunTerminalStatus.FAILED,
        generated_at=NOW + timedelta(seconds=61),
        runtime_pool_ref="pool.internal.us-east-1.a",
        error_classification="managed_runtime_worker_error",
    )
    health = derive_pool_health(
        [
            RuntimeHeartbeat(
                worker_ref="worker.internal.1",
                pool_ref="pool.internal.us-east-1.a",
                tenant_ref="tenant.acme",
                environment_ref="env.prod",
                runtime_version_ref="runtime.managed.v1",
                observed_at=NOW,
                capacity_slots=4,
                active_runs=1,
                last_error_code="worker.crashed",
            )
        ],
        pool_ref="pool.internal.us-east-1.a",
        now=NOW,
        heartbeat_fresh_seconds=30,
    )

    customer = customer_observability_summary(receipt=receipt, pool_health=health)
    audit = build_internal_audit_contract(receipt=receipt, meter_events=[], pool_health=health)

    assert customer["failure"] == {"reason_code": "managed_runtime_worker_error"}
    assert "runtime_pool_ref" not in customer
    assert "pool_ref" not in customer["runtime_health"]
    assert "worker_ref" not in str(customer)
    assert audit["pool_health"]["pool_ref"] == "pool.internal.us-east-1.a"
