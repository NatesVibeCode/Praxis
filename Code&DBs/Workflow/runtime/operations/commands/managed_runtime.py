"""CQRS command for managed-runtime accounting and observability authority."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from runtime.managed_runtime import (
    AuditEvent,
    CostStatus,
    ExecutionMode,
    ExecutionModePolicy,
    MeterEventKind,
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
from storage.postgres.managed_runtime_repository import persist_managed_runtime_record


ExecutionModeLiteral = Literal["managed", "exported", "hybrid"]
MeterEventKindLiteral = Literal["run_started", "resource_usage", "run_finished", "diagnostic"]
CostStatusLiteral = Literal["estimated", "provisional", "finalized", "not_applicable"]
TerminalStatusLiteral = Literal["succeeded", "failed", "cancelled"]


class RunIdentityInput(BaseModel):
    run_id: str
    tenant_ref: str
    environment_ref: str
    workflow_ref: str
    workload_class: str
    attempt: int = Field(default=1, ge=1)

    def to_identity(self) -> RunIdentity:
        return RunIdentity(
            run_id=self.run_id,
            tenant_ref=self.tenant_ref,
            environment_ref=self.environment_ref,
            workflow_ref=self.workflow_ref,
            workload_class=self.workload_class,
            attempt=self.attempt,
        )


class ExecutionModePolicyInput(BaseModel):
    tenant_ref: str
    environment_ref: str
    configured_mode: ExecutionModeLiteral
    managed_workload_classes: list[str] = Field(default_factory=list)
    exported_workload_classes: list[str] = Field(default_factory=list)
    workload_mode_overrides: dict[str, ExecutionModeLiteral] = Field(default_factory=dict)
    managed_enabled: bool = True
    tenant_managed_allowed: bool = True
    policy_decision_refs: list[str] = Field(default_factory=list)

    def to_policy(self) -> ExecutionModePolicy:
        return ExecutionModePolicy(
            tenant_ref=self.tenant_ref,
            environment_ref=self.environment_ref,
            configured_mode=self.configured_mode,
            managed_workload_classes=frozenset(self.managed_workload_classes),
            exported_workload_classes=frozenset(self.exported_workload_classes),
            workload_mode_overrides=self.workload_mode_overrides,
            managed_enabled=self.managed_enabled,
            tenant_managed_allowed=self.tenant_managed_allowed,
            policy_decision_refs=tuple(self.policy_decision_refs),
        )


class RunMeterEventInput(BaseModel):
    occurred_at: datetime
    event_kind: MeterEventKindLiteral
    event_id: str | None = None
    idempotency_key: str | None = None
    wall_seconds: Decimal | int | float | str = Decimal("0")
    cpu_core_seconds: Decimal | int | float | str = Decimal("0")
    memory_gib_seconds: Decimal | int | float | str = Decimal("0")
    accelerator_seconds: Decimal | int | float | str = Decimal("0")
    billable: bool = True
    source_event_ref: str | None = None

    def to_event(
        self,
        *,
        identity: RunIdentity,
        execution_mode: ExecutionMode,
        runtime_version_ref: str,
    ) -> RunMeterEvent:
        return RunMeterEvent(
            event_id=self.event_id or "",
            idempotency_key=self.idempotency_key or "",
            run_id=identity.run_id,
            tenant_ref=identity.tenant_ref,
            environment_ref=identity.environment_ref,
            workflow_ref=identity.workflow_ref,
            execution_mode=execution_mode,
            runtime_version_ref=runtime_version_ref,
            occurred_at=self.occurred_at,
            event_kind=MeterEventKind(self.event_kind),
            wall_seconds=self.wall_seconds,
            cpu_core_seconds=self.cpu_core_seconds,
            memory_gib_seconds=self.memory_gib_seconds,
            accelerator_seconds=self.accelerator_seconds,
            billable=self.billable,
            source_event_ref=self.source_event_ref,
        )


class PricingScheduleVersionInput(BaseModel):
    schedule_ref: str
    version_ref: str
    effective_at: datetime
    currency: str = "USD"
    cpu_core_second_rate: Decimal | int | float | str = Decimal("0")
    memory_gib_second_rate: Decimal | int | float | str = Decimal("0")
    accelerator_second_rate: Decimal | int | float | str = Decimal("0")
    minimum_charge: Decimal | int | float | str = Decimal("0")

    def to_schedule(self) -> PricingScheduleVersion:
        return PricingScheduleVersion(
            schedule_ref=self.schedule_ref,
            version_ref=self.version_ref,
            effective_at=self.effective_at,
            currency=self.currency,
            cpu_core_second_rate=self.cpu_core_second_rate,
            memory_gib_second_rate=self.memory_gib_second_rate,
            accelerator_second_rate=self.accelerator_second_rate,
            minimum_charge=self.minimum_charge,
        )


class RuntimeHeartbeatInput(BaseModel):
    worker_ref: str
    pool_ref: str
    observed_at: datetime
    capacity_slots: int = Field(ge=0)
    active_runs: int = Field(ge=0)
    tenant_ref: str | None = None
    environment_ref: str | None = None
    runtime_version_ref: str | None = None
    accepting_work: bool = True
    stuck_run_refs: list[str] = Field(default_factory=list)
    last_error_code: str | None = None

    def to_heartbeat(
        self,
        *,
        identity: RunIdentity,
        runtime_version_ref: str,
    ) -> RuntimeHeartbeat:
        return RuntimeHeartbeat(
            worker_ref=self.worker_ref,
            pool_ref=self.pool_ref,
            tenant_ref=self.tenant_ref or identity.tenant_ref,
            environment_ref=self.environment_ref or identity.environment_ref,
            runtime_version_ref=self.runtime_version_ref or runtime_version_ref,
            observed_at=self.observed_at,
            capacity_slots=self.capacity_slots,
            active_runs=self.active_runs,
            accepting_work=self.accepting_work,
            stuck_run_refs=tuple(self.stuck_run_refs),
            last_error_code=self.last_error_code,
        )


class AuditEventInput(BaseModel):
    audit_event_id: str
    occurred_at: datetime
    actor_ref: str
    action: str
    target_kind: str
    target_ref: str
    reason_code: str
    run_id: str | None = None
    tenant_ref: str | None = None
    environment_ref: str | None = None
    before_version_ref: str | None = None
    after_version_ref: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)

    def to_audit_event(self, *, identity: RunIdentity) -> AuditEvent:
        return AuditEvent(
            audit_event_id=self.audit_event_id,
            occurred_at=self.occurred_at,
            actor_ref=self.actor_ref,
            action=self.action,
            target_kind=self.target_kind,
            target_ref=self.target_ref,
            tenant_ref=self.tenant_ref or identity.tenant_ref,
            environment_ref=self.environment_ref or identity.environment_ref,
            reason_code=self.reason_code,
            run_id=self.run_id or identity.run_id,
            before_version_ref=self.before_version_ref,
            after_version_ref=self.after_version_ref,
            details=self.details,
        )


class RecordManagedRuntimeCommand(BaseModel):
    """Record one managed/exported/hybrid runtime accounting snapshot."""

    identity: RunIdentityInput
    policy: ExecutionModePolicyInput
    meter_events: list[RunMeterEventInput] = Field(min_length=1)
    terminal_status: TerminalStatusLiteral
    generated_at: datetime
    runtime_version_ref: str
    requested_mode: ExecutionModeLiteral | None = None
    runtime_pool_ref: str | None = None
    pricing_schedule: PricingScheduleVersionInput | None = None
    cost_status: CostStatusLiteral = "finalized"
    heartbeats: list[RuntimeHeartbeatInput] = Field(default_factory=list)
    pool_ref: str | None = None
    heartbeat_fresh_seconds: int = Field(default=30, ge=1)
    unavailable_after_seconds: int | None = Field(default=None, ge=1)
    clock_skew_grace_seconds: int = Field(default=5, ge=0)
    audit_events: list[AuditEventInput] = Field(default_factory=list)
    error_classification: str | None = None
    execution_labels: list[str] = Field(default_factory=list)
    correction_of_receipt_id: str | None = None
    runtime_record_id: str | None = None
    observed_by_ref: str | None = None
    source_ref: str | None = None
    require_dispatch_allowed: bool = False

    @field_validator(
        "runtime_version_ref",
        "runtime_pool_ref",
        "pool_ref",
        "error_classification",
        "correction_of_receipt_id",
        "runtime_record_id",
        "observed_by_ref",
        "source_ref",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("text fields must be non-empty strings when provided")
        return value.strip()


def handle_record_managed_runtime(
    command: RecordManagedRuntimeCommand,
    subsystems: Any,
) -> dict[str, Any]:
    identity = command.identity.to_identity()
    policy = command.policy.to_policy()
    selection = select_execution_mode(
        RunPlacementRequest(
            identity=identity,
            requested_mode=command.requested_mode,
            labels=tuple(command.execution_labels),
        ),
        policy,
    )
    if not selection.allowed or selection.execution_mode is None:
        raise ValueError(f"managed_runtime.placement_denied:{selection.reason_code}")

    runtime_version_ref = str(command.runtime_version_ref)
    meter_events = [
        item.to_event(
            identity=identity,
            execution_mode=selection.execution_mode,
            runtime_version_ref=runtime_version_ref,
        )
        for item in command.meter_events
    ]
    pricing_schedule = (
        command.pricing_schedule.to_schedule()
        if command.pricing_schedule is not None
        else None
    )
    usage_summary = build_usage_summary(
        meter_events,
        pricing_schedule=pricing_schedule,
        cost_status=CostStatus(command.cost_status),
    )
    receipt = finalize_run_receipt(
        identity=identity,
        selection=selection,
        usage_summary=usage_summary,
        terminal_status=RunTerminalStatus(command.terminal_status),
        generated_at=command.generated_at,
        runtime_pool_ref=command.runtime_pool_ref,
        error_classification=command.error_classification,
        execution_labels=tuple(command.execution_labels),
        correction_of_receipt_id=command.correction_of_receipt_id,
    )
    heartbeats = [
        item.to_heartbeat(identity=identity, runtime_version_ref=runtime_version_ref)
        for item in command.heartbeats
    ]
    selected_pool_ref = command.pool_ref or command.runtime_pool_ref or (
        heartbeats[0].pool_ref if heartbeats else None
    )
    pool_health = None
    if heartbeats and selected_pool_ref:
        pool_health = derive_pool_health(
            heartbeats,
            pool_ref=selected_pool_ref,
            now=command.generated_at,
            heartbeat_fresh_seconds=command.heartbeat_fresh_seconds,
            unavailable_after_seconds=command.unavailable_after_seconds,
            clock_skew_grace_seconds=command.clock_skew_grace_seconds,
        )
        if command.require_dispatch_allowed and not pool_health.dispatch_allowed:
            codes = ",".join(pool_health.reason_codes)
            raise ValueError(f"managed_runtime.dispatch_blocked:{codes}")

    audit_events = [item.to_audit_event(identity=identity) for item in command.audit_events]
    pool_health_payload = None
    if pool_health is not None:
        pool_health_payload = {
            **pool_health.to_internal_dict(),
            "tenant_ref": identity.tenant_ref,
            "environment_ref": identity.environment_ref,
        }
    internal_audit = build_internal_audit_contract(
        receipt=receipt,
        meter_events=meter_events,
        pool_health=pool_health,
        audit_events=audit_events,
    )
    customer_summary = customer_observability_summary(
        receipt=receipt,
        pool_health=pool_health,
    )
    runtime_record_id = command.runtime_record_id or _default_record_id(receipt.receipt_id)
    persisted = persist_managed_runtime_record(
        subsystems.get_pg_conn(),
        runtime_record_id=runtime_record_id,
        receipt=receipt.to_dict(internal=True),
        usage_summary=usage_summary.to_dict(customer_safe=False),
        mode_selection=selection.to_dict(),
        meter_events=_meter_event_payloads(meter_events, receipt_id=receipt.receipt_id),
        pricing_schedule=pricing_schedule.to_dict() if pricing_schedule is not None else None,
        heartbeats=[heartbeat.to_internal_dict() for heartbeat in heartbeats],
        pool_health=pool_health_payload,
        audit_events=[event.to_internal_dict() for event in audit_events],
        customer_observability=customer_summary,
        internal_audit=internal_audit,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    event_payload = _event_payload(
        runtime_record_id=runtime_record_id,
        receipt=receipt.to_dict(internal=True),
        usage_summary=usage_summary.to_dict(customer_safe=False),
        mode_selection=selection.to_dict(),
        pool_health=pool_health_payload,
    )
    return {
        "ok": True,
        "operation": "authority.managed_runtime.record",
        "runtime_record_id": runtime_record_id,
        "run_id": identity.run_id,
        "receipt_id": receipt.receipt_id,
        "execution_mode": selection.execution_mode.value,
        "configured_mode": selection.configured_mode.value,
        "terminal_status": receipt.terminal_status.value,
        "cost_summary": receipt.cost_summary.to_dict(customer_safe=False),
        "usage_summary": usage_summary.to_dict(customer_safe=False),
        "pool_health": pool_health_payload,
        "customer_observability": customer_summary,
        "internal_audit": internal_audit,
        "persisted": persisted,
        "event_payload": event_payload,
    }


def _default_record_id(receipt_id: str) -> str:
    payload = json.dumps(["managed_runtime", receipt_id], separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"managed_runtime_record.{digest}"


def _meter_event_payloads(events: list[RunMeterEvent], *, receipt_id: str) -> list[dict[str, Any]]:
    payloads = []
    for event in events:
        payload = event.to_dict()
        payload["receipt_id"] = payload.get("receipt_id") or receipt_id
        payloads.append(payload)
    return payloads


def _event_payload(
    *,
    runtime_record_id: str,
    receipt: dict[str, Any],
    usage_summary: dict[str, Any],
    mode_selection: dict[str, Any],
    pool_health: dict[str, Any] | None,
) -> dict[str, Any]:
    identity = dict(receipt["identity"])
    cost = dict(receipt["cost_summary"])
    return {
        "runtime_record_id": runtime_record_id,
        "run_id": identity["run_id"],
        "tenant_ref": identity["tenant_ref"],
        "environment_ref": identity["environment_ref"],
        "workflow_ref": identity["workflow_ref"],
        "workload_class": identity["workload_class"],
        "configured_mode": mode_selection["configured_mode"],
        "execution_mode": mode_selection["execution_mode"],
        "terminal_status": receipt["terminal_status"],
        "receipt_id": receipt["receipt_id"],
        "cost_status": cost["status"],
        "cost_amount": cost["amount"],
        "currency": cost["currency"],
        "pricing_schedule_version_ref": cost["pricing_schedule_version_ref"],
        "metered_event_count": usage_summary["metered_event_count"],
        "duplicate_meter_event_count": usage_summary["duplicate_meter_event_count"],
        "pool_health_state": pool_health["state"] if pool_health else None,
        "dispatch_allowed": pool_health["dispatch_allowed"] if pool_health else None,
    }


__all__ = [
    "AuditEventInput",
    "ExecutionModePolicyInput",
    "PricingScheduleVersionInput",
    "RecordManagedRuntimeCommand",
    "RunIdentityInput",
    "RunMeterEventInput",
    "RuntimeHeartbeatInput",
    "handle_record_managed_runtime",
]
