"""Pure managed-runtime accounting, receipt, health, and projection contracts.

This module intentionally has no database or transport dependency. It defines
the deterministic domain records Phase 11 can persist through CQRS operations.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

_MONEY_QUANT = Decimal("0.000001")
_SECONDS_QUANT = Decimal("0.001")


class ManagedRuntimePolicyError(ValueError):
    """Raised when a workload cannot be placed under the requested policy."""

    def __init__(self, selection: "ModeSelection") -> None:
        super().__init__(selection.reason_code)
        self.selection = selection


class ExecutionMode(Enum):
    MANAGED = "managed"
    EXPORTED = "exported"
    HYBRID = "hybrid"


class MeterEventKind(Enum):
    RUN_STARTED = "run_started"
    RESOURCE_USAGE = "resource_usage"
    RUN_FINISHED = "run_finished"
    DIAGNOSTIC = "diagnostic"


class CostStatus(Enum):
    ESTIMATED = "estimated"
    PROVISIONAL = "provisional"
    FINALIZED = "finalized"
    NOT_APPLICABLE = "not_applicable"


class RunTerminalStatus(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PoolHealthState(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _utc(value, field_name="datetime").isoformat().replace("+00:00", "Z")


def _decimal(value: Decimal | int | float | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    return Decimal(value)


def _seconds(value: Decimal | int | float | str) -> Decimal:
    return _decimal(value).quantize(_SECONDS_QUANT, rounding=ROUND_HALF_UP)


def _money(value: Decimal | int | float | str) -> Decimal:
    return _decimal(value).quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)


def _require_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} must be a non-empty string")
    return text


def _mode(value: ExecutionMode | str, *, field_name: str = "execution_mode") -> ExecutionMode:
    if isinstance(value, ExecutionMode):
        return value
    try:
        return ExecutionMode(str(value))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be one of {[m.value for m in ExecutionMode]}") from exc


def _event_kind(value: MeterEventKind | str) -> MeterEventKind:
    if isinstance(value, MeterEventKind):
        return value
    return MeterEventKind(str(value))


def _cost_status(value: CostStatus | str) -> CostStatus:
    if isinstance(value, CostStatus):
        return value
    return CostStatus(str(value))


def _terminal_status(value: RunTerminalStatus | str) -> RunTerminalStatus:
    if isinstance(value, RunTerminalStatus):
        return value
    return RunTerminalStatus(str(value))


def _stable_ref(prefix: str, *parts: object) -> str:
    payload = json.dumps([str(part) for part in parts], sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}.{digest}"


def _unique_sorted(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in values if str(value).strip()}))


def _decimal_str(value: Decimal) -> str:
    return format(value, "f")


@dataclass(frozen=True, slots=True)
class RunIdentity:
    run_id: str
    tenant_ref: str
    environment_ref: str
    workflow_ref: str
    workload_class: str
    attempt: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _require_text(self.run_id, field_name="run_id"))
        object.__setattr__(self, "tenant_ref", _require_text(self.tenant_ref, field_name="tenant_ref"))
        object.__setattr__(
            self,
            "environment_ref",
            _require_text(self.environment_ref, field_name="environment_ref"),
        )
        object.__setattr__(self, "workflow_ref", _require_text(self.workflow_ref, field_name="workflow_ref"))
        object.__setattr__(
            self,
            "workload_class",
            _require_text(self.workload_class, field_name="workload_class"),
        )
        if int(self.attempt) < 1:
            raise ValueError("attempt must be >= 1")
        object.__setattr__(self, "attempt", int(self.attempt))

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "tenant_ref": self.tenant_ref,
            "environment_ref": self.environment_ref,
            "workflow_ref": self.workflow_ref,
            "workload_class": self.workload_class,
            "attempt": self.attempt,
        }


@dataclass(frozen=True, slots=True)
class RunPlacementRequest:
    identity: RunIdentity
    requested_mode: ExecutionMode | str | None = None
    labels: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.requested_mode is not None:
            object.__setattr__(self, "requested_mode", _mode(self.requested_mode))
        object.__setattr__(self, "labels", _unique_sorted(self.labels))


@dataclass(frozen=True, slots=True)
class ExecutionModePolicy:
    tenant_ref: str
    environment_ref: str
    configured_mode: ExecutionMode | str
    managed_workload_classes: frozenset[str] = field(default_factory=frozenset)
    exported_workload_classes: frozenset[str] = field(default_factory=frozenset)
    workload_mode_overrides: Mapping[str, ExecutionMode | str] = field(default_factory=dict)
    managed_enabled: bool = True
    tenant_managed_allowed: bool = True
    policy_decision_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "tenant_ref", _require_text(self.tenant_ref, field_name="tenant_ref"))
        object.__setattr__(
            self,
            "environment_ref",
            _require_text(self.environment_ref, field_name="environment_ref"),
        )
        object.__setattr__(self, "configured_mode", _mode(self.configured_mode, field_name="configured_mode"))
        object.__setattr__(
            self,
            "managed_workload_classes",
            frozenset(_unique_sorted(self.managed_workload_classes)),
        )
        object.__setattr__(
            self,
            "exported_workload_classes",
            frozenset(_unique_sorted(self.exported_workload_classes)),
        )
        object.__setattr__(
            self,
            "workload_mode_overrides",
            {
                _require_text(workload, field_name="workload_mode_overrides key"): _mode(mode)
                for workload, mode in dict(self.workload_mode_overrides).items()
            },
        )
        object.__setattr__(self, "policy_decision_refs", _unique_sorted(self.policy_decision_refs))


@dataclass(frozen=True, slots=True)
class ModeSelection:
    allowed: bool
    configured_mode: ExecutionMode
    execution_mode: ExecutionMode | None
    reason_code: str
    reason: str
    identity: RunIdentity
    policy_decision_refs: tuple[str, ...] = ()

    def require_allowed(self) -> "ModeSelection":
        if not self.allowed:
            raise ManagedRuntimePolicyError(self)
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "configured_mode": self.configured_mode.value,
            "execution_mode": self.execution_mode.value if self.execution_mode else None,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "identity": self.identity.to_dict(),
            "policy_decision_refs": list(self.policy_decision_refs),
        }


def _exported_supported(policy: ExecutionModePolicy, workload_class: str) -> bool:
    return not policy.exported_workload_classes or workload_class in policy.exported_workload_classes


def _managed_supported(policy: ExecutionModePolicy, workload_class: str) -> bool:
    return workload_class in policy.managed_workload_classes


def _deny(
    *,
    configured_mode: ExecutionMode,
    reason_code: str,
    reason: str,
    identity: RunIdentity,
    policy: ExecutionModePolicy,
) -> ModeSelection:
    return ModeSelection(
        allowed=False,
        configured_mode=configured_mode,
        execution_mode=None,
        reason_code=reason_code,
        reason=reason,
        identity=identity,
        policy_decision_refs=policy.policy_decision_refs,
    )


def _allow(
    *,
    configured_mode: ExecutionMode,
    execution_mode: ExecutionMode,
    reason_code: str,
    reason: str,
    identity: RunIdentity,
    policy: ExecutionModePolicy,
) -> ModeSelection:
    return ModeSelection(
        allowed=True,
        configured_mode=configured_mode,
        execution_mode=execution_mode,
        reason_code=reason_code,
        reason=reason,
        identity=identity,
        policy_decision_refs=policy.policy_decision_refs,
    )


def _managed_denial(policy: ExecutionModePolicy, identity: RunIdentity) -> tuple[str, str] | None:
    if not policy.managed_enabled:
        return ("managed_runtime_disabled", "Managed runtime is disabled for this environment.")
    if not policy.tenant_managed_allowed:
        return ("tenant_managed_runtime_denied", "Tenant policy does not allow managed runtime.")
    if not _managed_supported(policy, identity.workload_class):
        return ("managed_workload_unsupported", "Workload class is not eligible for managed runtime.")
    return None


def select_execution_mode(
    request: RunPlacementRequest,
    policy: ExecutionModePolicy,
) -> ModeSelection:
    """Resolve configured managed/exported/hybrid policy into an actual placement."""
    identity = request.identity
    if identity.tenant_ref != policy.tenant_ref or identity.environment_ref != policy.environment_ref:
        return _deny(
            configured_mode=policy.configured_mode,
            reason_code="policy_scope_mismatch",
            reason="Execution policy does not match the run tenant/environment.",
            identity=identity,
            policy=policy,
        )

    configured = (
        request.requested_mode
        or policy.workload_mode_overrides.get(identity.workload_class)
        or policy.configured_mode
    )
    configured = _mode(configured)

    if configured is ExecutionMode.MANAGED:
        denial = _managed_denial(policy, identity)
        if denial:
            code, reason = denial
            return _deny(
                configured_mode=configured,
                reason_code=code,
                reason=reason,
                identity=identity,
                policy=policy,
            )
        return _allow(
            configured_mode=configured,
            execution_mode=ExecutionMode.MANAGED,
            reason_code="managed_selected",
            reason="Workload is eligible for managed runtime.",
            identity=identity,
            policy=policy,
        )

    if configured is ExecutionMode.EXPORTED:
        if not _exported_supported(policy, identity.workload_class):
            return _deny(
                configured_mode=configured,
                reason_code="exported_workload_unsupported",
                reason="Workload class is not eligible for exported execution.",
                identity=identity,
                policy=policy,
            )
        return _allow(
            configured_mode=configured,
            execution_mode=ExecutionMode.EXPORTED,
            reason_code="exported_selected",
            reason="Workload will run in customer-hosted exported mode.",
            identity=identity,
            policy=policy,
        )

    denial = _managed_denial(policy, identity)
    if denial is None:
        return _allow(
            configured_mode=configured,
            execution_mode=ExecutionMode.MANAGED,
            reason_code="hybrid_routed_managed",
            reason="Hybrid policy routed this eligible workload to managed runtime.",
            identity=identity,
            policy=policy,
        )
    if _exported_supported(policy, identity.workload_class):
        return _allow(
            configured_mode=configured,
            execution_mode=ExecutionMode.EXPORTED,
            reason_code="hybrid_routed_exported",
            reason="Hybrid policy preserved exported execution for this workload.",
            identity=identity,
            policy=policy,
        )
    code, reason = denial
    return _deny(
        configured_mode=configured,
        reason_code=code,
        reason=reason,
        identity=identity,
        policy=policy,
    )


@dataclass(frozen=True, slots=True)
class RunMeterEvent:
    event_id: str
    idempotency_key: str
    run_id: str
    tenant_ref: str
    environment_ref: str
    workflow_ref: str
    execution_mode: ExecutionMode | str
    runtime_version_ref: str
    occurred_at: datetime
    event_kind: MeterEventKind | str
    wall_seconds: Decimal | int | float | str = Decimal("0")
    cpu_core_seconds: Decimal | int | float | str = Decimal("0")
    memory_gib_seconds: Decimal | int | float | str = Decimal("0")
    accelerator_seconds: Decimal | int | float | str = Decimal("0")
    billable: bool = True
    receipt_id: str | None = None
    source_event_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", _require_text(self.run_id, field_name="run_id"))
        object.__setattr__(self, "tenant_ref", _require_text(self.tenant_ref, field_name="tenant_ref"))
        object.__setattr__(
            self,
            "environment_ref",
            _require_text(self.environment_ref, field_name="environment_ref"),
        )
        object.__setattr__(self, "workflow_ref", _require_text(self.workflow_ref, field_name="workflow_ref"))
        object.__setattr__(self, "execution_mode", _mode(self.execution_mode))
        object.__setattr__(
            self,
            "runtime_version_ref",
            _require_text(self.runtime_version_ref, field_name="runtime_version_ref"),
        )
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, field_name="occurred_at"))
        object.__setattr__(self, "event_kind", _event_kind(self.event_kind))
        object.__setattr__(self, "wall_seconds", _seconds(self.wall_seconds))
        object.__setattr__(self, "cpu_core_seconds", _seconds(self.cpu_core_seconds))
        object.__setattr__(self, "memory_gib_seconds", _seconds(self.memory_gib_seconds))
        object.__setattr__(self, "accelerator_seconds", _seconds(self.accelerator_seconds))
        key = str(self.idempotency_key or "").strip()
        if not key:
            key = _stable_ref("meter_idempotency", self.run_id, self.event_kind.value, self.occurred_at.isoformat())
        object.__setattr__(self, "idempotency_key", key)
        event_id = str(self.event_id or "").strip()
        if not event_id:
            event_id = _stable_ref("meter_event", key)
        object.__setattr__(self, "event_id", event_id)
        if self.event_kind is MeterEventKind.DIAGNOSTIC:
            object.__setattr__(self, "billable", False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "idempotency_key": self.idempotency_key,
            "run_id": self.run_id,
            "tenant_ref": self.tenant_ref,
            "environment_ref": self.environment_ref,
            "workflow_ref": self.workflow_ref,
            "execution_mode": self.execution_mode.value,
            "runtime_version_ref": self.runtime_version_ref,
            "occurred_at": _iso(self.occurred_at),
            "event_kind": self.event_kind.value,
            "wall_seconds": _decimal_str(self.wall_seconds),
            "cpu_core_seconds": _decimal_str(self.cpu_core_seconds),
            "memory_gib_seconds": _decimal_str(self.memory_gib_seconds),
            "accelerator_seconds": _decimal_str(self.accelerator_seconds),
            "billable": self.billable,
            "receipt_id": self.receipt_id,
            "source_event_ref": self.source_event_ref,
        }


def dedupe_meter_events(events: Iterable[RunMeterEvent]) -> tuple[RunMeterEvent, ...]:
    """Return first-seen meter events keyed by idempotency key."""
    unique: dict[str, RunMeterEvent] = {}
    for event in events:
        unique.setdefault(event.idempotency_key, event)
    return tuple(sorted(unique.values(), key=lambda item: (item.occurred_at, item.event_id)))


@dataclass(frozen=True, slots=True)
class LineItem:
    dimension: str
    units: Decimal
    unit_rate: Decimal
    amount: Decimal

    def to_dict(self) -> dict[str, str]:
        return {
            "dimension": self.dimension,
            "units": _decimal_str(self.units),
            "unit_rate": _decimal_str(self.unit_rate),
            "amount": _decimal_str(self.amount),
        }


@dataclass(frozen=True, slots=True)
class CostSummary:
    status: CostStatus | str
    currency: str
    amount: Decimal | int | float | str
    pricing_schedule_version_ref: str | None
    line_items: tuple[LineItem, ...] = ()
    calculation_basis: str = "metered_usage"

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _cost_status(self.status))
        object.__setattr__(self, "currency", _require_text(self.currency, field_name="currency"))
        object.__setattr__(self, "amount", _money(self.amount))
        if self.pricing_schedule_version_ref is not None:
            object.__setattr__(
                self,
                "pricing_schedule_version_ref",
                _require_text(
                    self.pricing_schedule_version_ref,
                    field_name="pricing_schedule_version_ref",
                ),
            )

    @classmethod
    def not_applicable(cls, *, reason: str = "not_managed_runtime") -> "CostSummary":
        return cls(
            status=CostStatus.NOT_APPLICABLE,
            currency="USD",
            amount=Decimal("0"),
            pricing_schedule_version_ref=None,
            line_items=(),
            calculation_basis=reason,
        )

    def to_dict(self, *, customer_safe: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": self.status.value,
            "currency": self.currency,
            "amount": _decimal_str(self.amount),
            "pricing_schedule_version_ref": self.pricing_schedule_version_ref,
            "calculation_basis": self.calculation_basis,
        }
        if not customer_safe:
            payload["line_items"] = [item.to_dict() for item in self.line_items]
        return payload


@dataclass(frozen=True, slots=True)
class PricingScheduleVersion:
    schedule_ref: str
    version_ref: str
    effective_at: datetime
    currency: str = "USD"
    cpu_core_second_rate: Decimal | int | float | str = Decimal("0")
    memory_gib_second_rate: Decimal | int | float | str = Decimal("0")
    accelerator_second_rate: Decimal | int | float | str = Decimal("0")
    minimum_charge: Decimal | int | float | str = Decimal("0")

    def __post_init__(self) -> None:
        object.__setattr__(self, "schedule_ref", _require_text(self.schedule_ref, field_name="schedule_ref"))
        object.__setattr__(self, "version_ref", _require_text(self.version_ref, field_name="version_ref"))
        object.__setattr__(self, "effective_at", _utc(self.effective_at, field_name="effective_at"))
        object.__setattr__(self, "currency", _require_text(self.currency, field_name="currency"))
        object.__setattr__(self, "cpu_core_second_rate", _money(self.cpu_core_second_rate))
        object.__setattr__(self, "memory_gib_second_rate", _money(self.memory_gib_second_rate))
        object.__setattr__(self, "accelerator_second_rate", _money(self.accelerator_second_rate))
        object.__setattr__(self, "minimum_charge", _money(self.minimum_charge))

    def estimate(
        self,
        *,
        cpu_core_seconds: Decimal,
        memory_gib_seconds: Decimal,
        accelerator_seconds: Decimal,
        status: CostStatus | str,
    ) -> CostSummary:
        line_items: list[LineItem] = []
        for dimension, units, rate in (
            ("cpu_core_seconds", cpu_core_seconds, self.cpu_core_second_rate),
            ("memory_gib_seconds", memory_gib_seconds, self.memory_gib_second_rate),
            ("accelerator_seconds", accelerator_seconds, self.accelerator_second_rate),
        ):
            amount = _money(units * rate)
            if units or rate or amount:
                line_items.append(LineItem(dimension=dimension, units=units, unit_rate=rate, amount=amount))
        subtotal = _money(sum((item.amount for item in line_items), Decimal("0")))
        amount = subtotal
        if self.minimum_charge > amount:
            adjustment = _money(self.minimum_charge - amount)
            line_items.append(
                LineItem(
                    dimension="minimum_charge_adjustment",
                    units=Decimal("1.000"),
                    unit_rate=adjustment,
                    amount=adjustment,
                )
            )
            amount = self.minimum_charge
        return CostSummary(
            status=status,
            currency=self.currency,
            amount=amount,
            pricing_schedule_version_ref=self.version_ref,
            line_items=tuple(line_items),
            calculation_basis="pricing_schedule_version",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schedule_ref": self.schedule_ref,
            "version_ref": self.version_ref,
            "effective_at": _iso(self.effective_at),
            "currency": self.currency,
            "cpu_core_second_rate": _decimal_str(self.cpu_core_second_rate),
            "memory_gib_second_rate": _decimal_str(self.memory_gib_second_rate),
            "accelerator_second_rate": _decimal_str(self.accelerator_second_rate),
            "minimum_charge": _decimal_str(self.minimum_charge),
        }


@dataclass(frozen=True, slots=True)
class RunUsageSummary:
    run_id: str
    tenant_ref: str
    environment_ref: str
    workflow_ref: str
    execution_mode: ExecutionMode
    runtime_version_ref: str
    started_at: datetime
    ended_at: datetime
    duration_seconds: Decimal
    billable_wall_seconds: Decimal
    billable_cpu_core_seconds: Decimal
    billable_memory_gib_seconds: Decimal
    billable_accelerator_seconds: Decimal
    metered_event_count: int
    duplicate_meter_event_count: int
    diagnostic_event_count: int
    cost_summary: CostSummary

    @property
    def pricing_schedule_version_ref(self) -> str | None:
        return self.cost_summary.pricing_schedule_version_ref

    def to_dict(self, *, customer_safe: bool = False) -> dict[str, Any]:
        payload = {
            "run_id": self.run_id,
            "tenant_ref": self.tenant_ref,
            "environment_ref": self.environment_ref,
            "workflow_ref": self.workflow_ref,
            "execution_mode": self.execution_mode.value,
            "runtime_version_ref": self.runtime_version_ref,
            "started_at": _iso(self.started_at),
            "ended_at": _iso(self.ended_at),
            "duration_seconds": _decimal_str(self.duration_seconds),
            "billable_wall_seconds": _decimal_str(self.billable_wall_seconds),
            "billable_cpu_core_seconds": _decimal_str(self.billable_cpu_core_seconds),
            "billable_memory_gib_seconds": _decimal_str(self.billable_memory_gib_seconds),
            "billable_accelerator_seconds": _decimal_str(self.billable_accelerator_seconds),
            "cost_summary": self.cost_summary.to_dict(customer_safe=customer_safe),
        }
        if not customer_safe:
            payload.update(
                {
                    "metered_event_count": self.metered_event_count,
                    "duplicate_meter_event_count": self.duplicate_meter_event_count,
                    "diagnostic_event_count": self.diagnostic_event_count,
                }
            )
        return payload


def _assert_same_run(events: Sequence[RunMeterEvent]) -> None:
    if not events:
        raise ValueError("at least one meter event is required")
    first = events[0]
    for event in events[1:]:
        if (
            event.run_id,
            event.tenant_ref,
            event.environment_ref,
            event.workflow_ref,
            event.execution_mode,
            event.runtime_version_ref,
        ) != (
            first.run_id,
            first.tenant_ref,
            first.environment_ref,
            first.workflow_ref,
            first.execution_mode,
            first.runtime_version_ref,
        ):
            raise ValueError("meter events must belong to one run/runtime identity")


def build_usage_summary(
    events: Sequence[RunMeterEvent],
    *,
    pricing_schedule: PricingScheduleVersion | None = None,
    cost_status: CostStatus | str = CostStatus.ESTIMATED,
) -> RunUsageSummary:
    unique_events = dedupe_meter_events(events)
    _assert_same_run(unique_events)
    first = unique_events[0]
    started_at = next(
        (event.occurred_at for event in unique_events if event.event_kind is MeterEventKind.RUN_STARTED),
        unique_events[0].occurred_at,
    )
    ended_at = next(
        (
            event.occurred_at
            for event in reversed(unique_events)
            if event.event_kind is MeterEventKind.RUN_FINISHED
        ),
        unique_events[-1].occurred_at,
    )
    if ended_at < started_at:
        raise ValueError("meter events produce a negative run duration")
    billable_events = tuple(event for event in unique_events if event.billable)
    billable_wall_seconds = sum((event.wall_seconds for event in billable_events), Decimal("0"))
    duration_seconds = _seconds(Decimal(str((ended_at - started_at).total_seconds())))
    if not billable_wall_seconds and first.execution_mode is ExecutionMode.MANAGED:
        billable_wall_seconds = duration_seconds
    cpu_core_seconds = sum((event.cpu_core_seconds for event in billable_events), Decimal("0"))
    memory_gib_seconds = sum((event.memory_gib_seconds for event in billable_events), Decimal("0"))
    accelerator_seconds = sum((event.accelerator_seconds for event in billable_events), Decimal("0"))

    if first.execution_mode is ExecutionMode.MANAGED and pricing_schedule is not None:
        cost_summary = pricing_schedule.estimate(
            cpu_core_seconds=cpu_core_seconds,
            memory_gib_seconds=memory_gib_seconds,
            accelerator_seconds=accelerator_seconds,
            status=_cost_status(cost_status),
        )
    elif first.execution_mode is ExecutionMode.MANAGED:
        cost_summary = CostSummary(
            status=CostStatus.PROVISIONAL,
            currency="USD",
            amount=Decimal("0"),
            pricing_schedule_version_ref=None,
            line_items=(),
            calculation_basis="pricing_schedule_missing",
        )
    else:
        cost_summary = CostSummary.not_applicable()

    return RunUsageSummary(
        run_id=first.run_id,
        tenant_ref=first.tenant_ref,
        environment_ref=first.environment_ref,
        workflow_ref=first.workflow_ref,
        execution_mode=first.execution_mode,
        runtime_version_ref=first.runtime_version_ref,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
        billable_wall_seconds=_seconds(billable_wall_seconds),
        billable_cpu_core_seconds=_seconds(cpu_core_seconds),
        billable_memory_gib_seconds=_seconds(memory_gib_seconds),
        billable_accelerator_seconds=_seconds(accelerator_seconds),
        metered_event_count=len(unique_events),
        duplicate_meter_event_count=len(events) - len(unique_events),
        diagnostic_event_count=sum(1 for event in unique_events if event.event_kind is MeterEventKind.DIAGNOSTIC),
        cost_summary=cost_summary,
    )


@dataclass(frozen=True, slots=True)
class RunReceipt:
    receipt_id: str
    receipt_version_ref: str
    identity: RunIdentity
    configured_mode: ExecutionMode
    execution_mode: ExecutionMode
    runtime_version_ref: str
    runtime_pool_ref: str | None
    started_at: datetime
    ended_at: datetime
    duration_seconds: Decimal
    terminal_status: RunTerminalStatus
    usage_summary: RunUsageSummary
    cost_summary: CostSummary
    policy_reason_code: str
    policy_decision_refs: tuple[str, ...]
    generated_at: datetime
    finalized_at: datetime
    error_classification: str | None = None
    execution_labels: tuple[str, ...] = ()
    correction_of_receipt_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "configured_mode", _mode(self.configured_mode))
        object.__setattr__(self, "execution_mode", _mode(self.execution_mode))
        object.__setattr__(self, "terminal_status", _terminal_status(self.terminal_status))
        object.__setattr__(self, "started_at", _utc(self.started_at, field_name="started_at"))
        object.__setattr__(self, "ended_at", _utc(self.ended_at, field_name="ended_at"))
        object.__setattr__(self, "generated_at", _utc(self.generated_at, field_name="generated_at"))
        object.__setattr__(self, "finalized_at", _utc(self.finalized_at, field_name="finalized_at"))
        object.__setattr__(self, "duration_seconds", _seconds(self.duration_seconds))
        object.__setattr__(self, "policy_decision_refs", _unique_sorted(self.policy_decision_refs))
        object.__setattr__(self, "execution_labels", _unique_sorted(self.execution_labels))

    def to_dict(self, *, internal: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "receipt_id": self.receipt_id,
            "receipt_version_ref": self.receipt_version_ref,
            "correction_of_receipt_id": self.correction_of_receipt_id,
            "identity": self.identity.to_dict(),
            "configured_mode": self.configured_mode.value,
            "execution_mode": self.execution_mode.value,
            "runtime_version_ref": self.runtime_version_ref,
            "started_at": _iso(self.started_at),
            "ended_at": _iso(self.ended_at),
            "duration_seconds": _decimal_str(self.duration_seconds),
            "terminal_status": self.terminal_status.value,
            "error_classification": self.error_classification,
            "cost_summary": self.cost_summary.to_dict(customer_safe=not internal),
            "policy_reason_code": self.policy_reason_code,
            "policy_decision_refs": list(self.policy_decision_refs),
            "generated_at": _iso(self.generated_at),
            "finalized_at": _iso(self.finalized_at),
            "execution_labels": list(self.execution_labels),
        }
        if internal:
            payload["runtime_pool_ref"] = self.runtime_pool_ref
            payload["usage_summary"] = self.usage_summary.to_dict(customer_safe=False)
        return payload


def finalize_run_receipt(
    *,
    identity: RunIdentity,
    selection: ModeSelection,
    usage_summary: RunUsageSummary,
    terminal_status: RunTerminalStatus | str,
    generated_at: datetime,
    runtime_pool_ref: str | None = None,
    receipt_version_ref: str = "run_receipt.v1",
    error_classification: str | None = None,
    execution_labels: Sequence[str] = (),
    correction_of_receipt_id: str | None = None,
) -> RunReceipt:
    selection.require_allowed()
    if usage_summary.run_id != identity.run_id:
        raise ValueError("usage summary run_id must match receipt identity")
    if selection.execution_mode is None:
        raise ValueError("allowed selection must include execution_mode")
    receipt_id = _stable_ref(
        "run_receipt",
        identity.run_id,
        identity.attempt,
        receipt_version_ref,
        correction_of_receipt_id or "",
    )
    generated = _utc(generated_at, field_name="generated_at")
    return RunReceipt(
        receipt_id=receipt_id,
        receipt_version_ref=receipt_version_ref,
        identity=identity,
        configured_mode=selection.configured_mode,
        execution_mode=selection.execution_mode,
        runtime_version_ref=usage_summary.runtime_version_ref,
        runtime_pool_ref=runtime_pool_ref,
        started_at=usage_summary.started_at,
        ended_at=usage_summary.ended_at,
        duration_seconds=usage_summary.duration_seconds,
        terminal_status=_terminal_status(terminal_status),
        usage_summary=usage_summary,
        cost_summary=usage_summary.cost_summary,
        policy_reason_code=selection.reason_code,
        policy_decision_refs=selection.policy_decision_refs,
        generated_at=generated,
        finalized_at=generated,
        error_classification=error_classification,
        execution_labels=tuple(execution_labels),
        correction_of_receipt_id=correction_of_receipt_id,
    )


@dataclass(frozen=True, slots=True)
class RuntimeHeartbeat:
    worker_ref: str
    pool_ref: str
    tenant_ref: str
    environment_ref: str
    runtime_version_ref: str
    observed_at: datetime
    capacity_slots: int
    active_runs: int
    accepting_work: bool = True
    stuck_run_refs: tuple[str, ...] = ()
    last_error_code: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "worker_ref", _require_text(self.worker_ref, field_name="worker_ref"))
        object.__setattr__(self, "pool_ref", _require_text(self.pool_ref, field_name="pool_ref"))
        object.__setattr__(self, "tenant_ref", _require_text(self.tenant_ref, field_name="tenant_ref"))
        object.__setattr__(
            self,
            "environment_ref",
            _require_text(self.environment_ref, field_name="environment_ref"),
        )
        object.__setattr__(
            self,
            "runtime_version_ref",
            _require_text(self.runtime_version_ref, field_name="runtime_version_ref"),
        )
        object.__setattr__(self, "observed_at", _utc(self.observed_at, field_name="observed_at"))
        object.__setattr__(self, "capacity_slots", max(0, int(self.capacity_slots)))
        object.__setattr__(self, "active_runs", max(0, int(self.active_runs)))
        object.__setattr__(self, "stuck_run_refs", _unique_sorted(self.stuck_run_refs))

    def age_seconds(self, *, now: datetime) -> Decimal:
        return _seconds(Decimal(str((_utc(now, field_name="now") - self.observed_at).total_seconds())))

    def to_internal_dict(self) -> dict[str, Any]:
        return {
            "worker_ref": self.worker_ref,
            "pool_ref": self.pool_ref,
            "tenant_ref": self.tenant_ref,
            "environment_ref": self.environment_ref,
            "runtime_version_ref": self.runtime_version_ref,
            "observed_at": _iso(self.observed_at),
            "capacity_slots": self.capacity_slots,
            "active_runs": self.active_runs,
            "accepting_work": self.accepting_work,
            "stuck_run_refs": list(self.stuck_run_refs),
            "last_error_code": self.last_error_code,
        }


_CUSTOMER_HEALTH_REASON_CODES = {
    "capacity_starved": "runtime_capacity_limited",
    "no_heartbeat": "runtime_unavailable",
    "some_workers_stale": "runtime_capacity_degraded",
    "stale_heartbeat": "runtime_capacity_stale",
    "stuck_runs_detected": "runtime_attention_required",
    "unavailable_heartbeat": "runtime_unavailable",
    "worker_errors_present": "runtime_attention_required",
}


@dataclass(frozen=True, slots=True)
class PoolHealthSummary:
    pool_ref: str
    state: PoolHealthState
    evaluated_at: datetime
    fresh_worker_count: int
    stale_worker_count: int
    unavailable_worker_count: int
    capacity_slots: int
    active_runs: int
    stuck_run_count: int
    reason_codes: tuple[str, ...]
    dispatch_allowed: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "pool_ref", _require_text(self.pool_ref, field_name="pool_ref"))
        if not isinstance(self.state, PoolHealthState):
            object.__setattr__(self, "state", PoolHealthState(str(self.state)))
        object.__setattr__(self, "evaluated_at", _utc(self.evaluated_at, field_name="evaluated_at"))
        object.__setattr__(self, "reason_codes", _unique_sorted(self.reason_codes))

    @property
    def available_slots(self) -> int:
        return max(0, self.capacity_slots - self.active_runs)

    def to_internal_dict(self) -> dict[str, Any]:
        return {
            "pool_ref": self.pool_ref,
            "state": self.state.value,
            "evaluated_at": _iso(self.evaluated_at),
            "fresh_worker_count": self.fresh_worker_count,
            "stale_worker_count": self.stale_worker_count,
            "unavailable_worker_count": self.unavailable_worker_count,
            "capacity_slots": self.capacity_slots,
            "active_runs": self.active_runs,
            "available_slots": self.available_slots,
            "stuck_run_count": self.stuck_run_count,
            "reason_codes": list(self.reason_codes),
            "dispatch_allowed": self.dispatch_allowed,
        }

    def to_customer_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "evaluated_at": _iso(self.evaluated_at),
            "capacity_available": self.dispatch_allowed,
            "reason_codes": _unique_sorted(
                _CUSTOMER_HEALTH_REASON_CODES.get(code, "runtime_attention_required")
                for code in self.reason_codes
            ),
        }


def derive_pool_health(
    heartbeats: Sequence[RuntimeHeartbeat],
    *,
    pool_ref: str,
    now: datetime,
    heartbeat_fresh_seconds: int,
    unavailable_after_seconds: int | None = None,
    clock_skew_grace_seconds: int = 5,
) -> PoolHealthSummary:
    evaluated_at = _utc(now, field_name="now")
    pool_ref = _require_text(pool_ref, field_name="pool_ref")
    unavailable_after = unavailable_after_seconds or heartbeat_fresh_seconds * 3
    pool_heartbeats = tuple(heartbeat for heartbeat in heartbeats if heartbeat.pool_ref == pool_ref)
    if not pool_heartbeats:
        return PoolHealthSummary(
            pool_ref=pool_ref,
            state=PoolHealthState.UNAVAILABLE,
            evaluated_at=evaluated_at,
            fresh_worker_count=0,
            stale_worker_count=0,
            unavailable_worker_count=0,
            capacity_slots=0,
            active_runs=0,
            stuck_run_count=0,
            reason_codes=("no_heartbeat",),
            dispatch_allowed=False,
        )

    fresh: list[RuntimeHeartbeat] = []
    stale: list[RuntimeHeartbeat] = []
    unavailable: list[RuntimeHeartbeat] = []
    fresh_window = Decimal(heartbeat_fresh_seconds + clock_skew_grace_seconds)
    unavailable_window = Decimal(unavailable_after + clock_skew_grace_seconds)
    for heartbeat in pool_heartbeats:
        age = heartbeat.age_seconds(now=evaluated_at)
        if age <= fresh_window and heartbeat.accepting_work:
            fresh.append(heartbeat)
        elif age <= unavailable_window:
            stale.append(heartbeat)
        else:
            unavailable.append(heartbeat)

    reason_codes: list[str] = []
    capacity_slots = sum(heartbeat.capacity_slots for heartbeat in fresh)
    active_runs = sum(heartbeat.active_runs for heartbeat in fresh)
    stuck_run_count = sum(len(heartbeat.stuck_run_refs) for heartbeat in pool_heartbeats)
    if stale:
        reason_codes.append("some_workers_stale")
    if unavailable:
        reason_codes.append("unavailable_heartbeat")
    if stuck_run_count:
        reason_codes.append("stuck_runs_detected")
    if any(heartbeat.last_error_code for heartbeat in pool_heartbeats):
        reason_codes.append("worker_errors_present")
    if capacity_slots <= active_runs and fresh:
        reason_codes.append("capacity_starved")

    if fresh:
        state = PoolHealthState.DEGRADED if reason_codes else PoolHealthState.HEALTHY
        dispatch_allowed = capacity_slots > active_runs and "stuck_runs_detected" not in reason_codes
    elif stale:
        state = PoolHealthState.STALE
        dispatch_allowed = False
        reason_codes.append("stale_heartbeat")
    else:
        state = PoolHealthState.UNAVAILABLE
        dispatch_allowed = False
        reason_codes.append("unavailable_heartbeat")

    return PoolHealthSummary(
        pool_ref=pool_ref,
        state=state,
        evaluated_at=evaluated_at,
        fresh_worker_count=len(fresh),
        stale_worker_count=len(stale),
        unavailable_worker_count=len(unavailable),
        capacity_slots=capacity_slots,
        active_runs=active_runs,
        stuck_run_count=stuck_run_count,
        reason_codes=tuple(reason_codes),
        dispatch_allowed=dispatch_allowed,
    )


@dataclass(frozen=True, slots=True)
class AuditEvent:
    audit_event_id: str
    occurred_at: datetime
    actor_ref: str
    action: str
    target_kind: str
    target_ref: str
    tenant_ref: str
    environment_ref: str
    reason_code: str
    run_id: str | None = None
    before_version_ref: str | None = None
    after_version_ref: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, field_name="occurred_at"))
        for field_name in (
            "audit_event_id",
            "actor_ref",
            "action",
            "target_kind",
            "target_ref",
            "tenant_ref",
            "environment_ref",
            "reason_code",
        ):
            object.__setattr__(self, field_name, _require_text(getattr(self, field_name), field_name=field_name))
        object.__setattr__(self, "details", dict(self.details))

    def to_internal_dict(self) -> dict[str, Any]:
        return {
            "audit_event_id": self.audit_event_id,
            "occurred_at": _iso(self.occurred_at),
            "actor_ref": self.actor_ref,
            "action": self.action,
            "target_kind": self.target_kind,
            "target_ref": self.target_ref,
            "tenant_ref": self.tenant_ref,
            "environment_ref": self.environment_ref,
            "reason_code": self.reason_code,
            "run_id": self.run_id,
            "before_version_ref": self.before_version_ref,
            "after_version_ref": self.after_version_ref,
            "details": dict(self.details),
        }


def build_internal_audit_contract(
    *,
    receipt: RunReceipt,
    meter_events: Sequence[RunMeterEvent],
    pool_health: PoolHealthSummary | None = None,
    audit_events: Sequence[AuditEvent] = (),
) -> dict[str, Any]:
    return {
        "contract": "managed_runtime.internal_audit.v1",
        "run": receipt.to_dict(internal=True),
        "meter_event_ids": [event.event_id for event in dedupe_meter_events(meter_events)],
        "meter_event_count": len(dedupe_meter_events(meter_events)),
        "duplicate_meter_event_count": len(meter_events) - len(dedupe_meter_events(meter_events)),
        "pool_health": pool_health.to_internal_dict() if pool_health else None,
        "audit_events": [event.to_internal_dict() for event in audit_events],
    }


def customer_observability_summary(
    *,
    receipt: RunReceipt,
    pool_health: PoolHealthSummary | None = None,
) -> dict[str, Any]:
    identity = receipt.identity
    payload: dict[str, Any] = {
        "tenant_ref": identity.tenant_ref,
        "environment_ref": identity.environment_ref,
        "run_id": identity.run_id,
        "workflow_ref": identity.workflow_ref,
        "attempt": identity.attempt,
        "execution_mode": receipt.execution_mode.value,
        "configured_mode": receipt.configured_mode.value,
        "status": receipt.terminal_status.value,
        "started_at": _iso(receipt.started_at),
        "ended_at": _iso(receipt.ended_at),
        "duration_seconds": _decimal_str(receipt.duration_seconds),
        "receipt": {
            "receipt_id": receipt.receipt_id,
            "version_ref": receipt.receipt_version_ref,
            "generated_at": _iso(receipt.generated_at),
        },
    }
    if receipt.error_classification:
        payload["failure"] = {"reason_code": receipt.error_classification}
    if receipt.execution_mode is ExecutionMode.MANAGED and receipt.cost_summary.status is not CostStatus.NOT_APPLICABLE:
        payload["cost"] = receipt.cost_summary.to_dict(customer_safe=True)
    if receipt.execution_mode is ExecutionMode.MANAGED and pool_health is not None:
        payload["runtime_health"] = pool_health.to_customer_dict()
    return payload


__all__ = [
    "AuditEvent",
    "CostStatus",
    "CostSummary",
    "ExecutionMode",
    "ExecutionModePolicy",
    "LineItem",
    "ManagedRuntimePolicyError",
    "MeterEventKind",
    "ModeSelection",
    "PoolHealthState",
    "PoolHealthSummary",
    "PricingScheduleVersion",
    "RunIdentity",
    "RunMeterEvent",
    "RunPlacementRequest",
    "RunReceipt",
    "RunTerminalStatus",
    "RunUsageSummary",
    "RuntimeHeartbeat",
    "build_internal_audit_contract",
    "build_usage_summary",
    "customer_observability_summary",
    "dedupe_meter_events",
    "derive_pool_health",
    "finalize_run_receipt",
    "select_execution_mode",
]
