"""Native primary cutover runtime and persistence seam."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from typing import Any, NoReturn, Protocol

from authority.operator_control import (
    CutoverGateAuthorityRecord,
    OperatorDecisionAuthorityRecord,
)


class NativePrimaryCutoverError(RuntimeError):
    """Raised when native cutover authority cannot be resolved safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _fail(
    reason_code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> NoReturn:
    raise NativePrimaryCutoverError(reason_code, message, details=details)


_fail_value = partial(_fail, "native_primary_cutover.invalid_value")


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail_value(
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail_value(
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        _fail_value(
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        _fail_value(
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _slugify(value: str) -> str:
    normalized = value.strip().lower()
    fallback = "target"
    return "".join(char if char.isalnum() else "-" for char in normalized).replace("--", "-").strip("-") or fallback


def _normalized_targets(
    *,
    roadmap_item_id: str | None,
    workflow_class_id: str | None,
    schedule_definition_id: str | None,
) -> tuple[str, str]:
    fields = (
        ("roadmap_item", roadmap_item_id),
        ("workflow_class", workflow_class_id),
        ("schedule_definition", schedule_definition_id),
    )
    chosen = tuple((field_name, value) for field_name, value in fields if value is not None)
    if len(chosen) != 1:
        _fail(
            "native_primary_cutover.invalid_target",
            "exactly one target must be provided",
            details={
                "provided_fields": ",".join(field_name for field_name, value in fields if value is not None),
            },
        )
    target_kind, target_ref = chosen[0]
    return target_kind, _require_text(target_ref, field_name=f"{target_kind}_id")


@dataclass
class NativePrimaryCutoverGateRecord:
    gate_id: str = ""
    decision_id: str = ""
    decision_key: str = ""
    gate_key: str = ""
    decision_status: str = "decided"
    gate_status: str = "open"
    gate_kind: str = "native_cutover"
    decided_by: str = ""
    decision_source: str = ""
    rationale: str = ""
    gate_name: str = ""
    title: str = ""
    roadmap_item_id: str | None = None
    workflow_class_id: str | None = None
    schedule_definition_id: str | None = None
    target_kind: str = ""
    target_ref: str = ""
    status: str = "open"
    gate_policy: Mapping[str, Any] = field(default_factory=dict)
    required_evidence: Mapping[str, Any] = field(default_factory=dict)
    opened_by_decision_id: str = ""
    closed_by_decision_id: str | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "gate_id": self.gate_id,
            "decision_id": self.decision_id,
            "decision_key": self.decision_key,
            "gate_key": self.gate_key,
            "decision_status": self.decision_status,
            "gate_status": self.gate_status,
            "status": self.status,
            "gate_kind": self.gate_kind,
            "decided_by": self.decided_by,
            "decision_source": self.decision_source,
            "rationale": self.rationale,
            "title": self.title,
            "gate_name": self.gate_name,
            "target_kind": self.target_kind,
            "target_ref": self.target_ref,
            "roadmap_item_id": self.roadmap_item_id,
            "workflow_class_id": self.workflow_class_id,
            "schedule_definition_id": self.schedule_definition_id,
            "gate_policy": self.gate_policy,
            "required_evidence": self.required_evidence,
            "opened_by_decision_id": self.opened_by_decision_id,
            "closed_by_decision_id": self.closed_by_decision_id,
            "opened_at": None if self.opened_at is None else self.opened_at.isoformat(),
            "closed_at": None if self.closed_at is None else self.closed_at.isoformat(),
            "created_at": None if self.created_at is None else self.created_at.isoformat(),
            "updated_at": None if self.updated_at is None else self.updated_at.isoformat(),
        }


class NativePrimaryCutoverRepository(Protocol):
    async def record_cutover_gate(
        self,
        *,
        operator_decision: OperatorDecisionAuthorityRecord,
        cutover_gate: CutoverGateAuthorityRecord,
    ) -> tuple[OperatorDecisionAuthorityRecord, CutoverGateAuthorityRecord]:
        ...


class PostgresNativePrimaryCutoverRepository:
    """Adapter to operator-control repository persistence."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    async def record_cutover_gate(
        self,
        *,
        operator_decision: OperatorDecisionAuthorityRecord,
        cutover_gate: CutoverGateAuthorityRecord,
    ) -> tuple[OperatorDecisionAuthorityRecord, CutoverGateAuthorityRecord]:
        from storage.postgres.operator_control_repository import PostgresOperatorControlRepository

        repository = PostgresOperatorControlRepository(self._conn)
        return await repository.record_cutover_gate(
            operator_decision=operator_decision,
            cutover_gate=cutover_gate,
        )


@dataclass
class NativePrimaryCutoverRuntime:
    """Deterministic runtime for admitting native primary cutover gates."""

    repository: NativePrimaryCutoverRepository

    async def admit_gate(self, **kwargs: Any) -> NativePrimaryCutoverGateRecord:
        decided_by = _require_text(kwargs.get("decided_by"), field_name="decided_by")
        decision_source = _require_text(kwargs.get("decision_source"), field_name="decision_source")
        rationale = _require_text(kwargs.get("rationale"), field_name="rationale")
        road_item_id = _optional_text(kwargs.get("roadmap_item_id"), field_name="roadmap_item_id")
        workflow_class_id = _optional_text(
            kwargs.get("workflow_class_id"),
            field_name="workflow_class_id",
        )
        schedule_definition_id = _optional_text(
            kwargs.get("schedule_definition_id"),
            field_name="schedule_definition_id",
        )
        target_kind, target_ref = _normalized_targets(
            roadmap_item_id=road_item_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
        )
        title = _optional_text(kwargs.get("title"), field_name="title") or "Native primary cutover"
        gate_name = _optional_text(kwargs.get("gate_name"), field_name="gate_name") or f"{target_kind}:{target_ref}"
        gate_policy = _require_mapping(
            {} if kwargs.get("gate_policy") is None else kwargs.get("gate_policy"),
            field_name="gate_policy",
        )
        required_evidence = _require_mapping(
            {} if kwargs.get("required_evidence") is None else kwargs.get("required_evidence"),
            field_name="required_evidence",
        )
        decided_at = _require_datetime(
            datetime.now(timezone.utc) if kwargs.get("decided_at") is None else kwargs.get("decided_at"),
            field_name="decided_at",
        )
        opened_at = _require_datetime(
            decided_at if kwargs.get("opened_at") is None else kwargs.get("opened_at"),
            field_name="opened_at",
        )
        created_at = _require_datetime(
            datetime.now(timezone.utc) if kwargs.get("created_at") is None else kwargs.get("created_at"),
            field_name="created_at",
        )
        updated_at = _require_datetime(
            created_at if kwargs.get("updated_at") is None else kwargs.get("updated_at"),
            field_name="updated_at",
        )

        if opened_at < decided_at:
            _fail(
                "native_primary_cutover.invalid_timing",
                "opened_at must be greater than or equal to decided_at",
                details={
                    "decided_at": decided_at.isoformat(),
                    "opened_at": opened_at.isoformat(),
                },
            )
        if updated_at < created_at:
            _fail(
                "native_primary_cutover.invalid_timing",
                "updated_at must be greater than or equal to created_at",
                details={
                    "created_at": created_at.isoformat(),
                    "updated_at": updated_at.isoformat(),
                },
            )

        target_slug = _slugify(f"{target_kind}_{target_ref}")
        seed = uuid.uuid4().hex
        decision_id = f"operator_decision.native-primary-cutover.{seed}"
        gate_id = f"cutover_gate.native-primary-cutover.{seed}"
        decision_key = f"native-primary-cutover::{target_kind}:{target_slug}:{seed[:8]}"
        gate_key = f"native-primary-cutover-gate::{target_kind}:{target_slug}:{seed[:8]}"

        operator_decision = OperatorDecisionAuthorityRecord(
            operator_decision_id=decision_id,
            decision_key=decision_key,
            decision_kind="native_primary_cutover",
            decision_status="decided",
            title=title,
            rationale=rationale,
            decided_by=decided_by,
            decision_source=decision_source,
            effective_from=opened_at,
            effective_to=None,
            decided_at=decided_at,
            created_at=created_at,
            updated_at=updated_at,
        )
        cutover_gate = CutoverGateAuthorityRecord(
            cutover_gate_id=gate_id,
            gate_key=gate_key,
            gate_name=gate_name,
            gate_kind="native_cutover_gate",
            gate_status="open",
            target_kind=target_kind,
            target_ref=target_ref,
            gate_policy=gate_policy,
            required_evidence=required_evidence,
            opened_by_decision_id=decision_id,
            closed_by_decision_id=None,
            opened_at=opened_at,
            closed_at=None,
            created_at=created_at,
            updated_at=updated_at,
        )
        persisted_decision, persisted_gate = await self.repository.record_cutover_gate(
            operator_decision=operator_decision,
            cutover_gate=cutover_gate,
        )
        return NativePrimaryCutoverGateRecord(
            gate_id=persisted_gate.cutover_gate_id,
            decision_id=persisted_decision.operator_decision_id,
            decision_key=persisted_decision.decision_key,
            gate_key=persisted_gate.gate_key,
            decision_status=persisted_decision.decision_status,
            gate_status=persisted_gate.gate_status,
            gate_kind=persisted_gate.gate_kind,
            decided_by=persisted_decision.decided_by,
            decision_source=persisted_decision.decision_source,
            rationale=persisted_decision.rationale,
            title=persisted_decision.title,
            gate_name=persisted_gate.gate_name,
            roadmap_item_id=road_item_id if target_kind == "roadmap_item" else None,
            workflow_class_id=workflow_class_id if target_kind == "workflow_class" else None,
            schedule_definition_id=schedule_definition_id if target_kind == "schedule_definition" else None,
            target_kind=persisted_gate.target_kind,
            target_ref=persisted_gate.target_ref,
            status=persisted_gate.gate_status,
            gate_policy=persisted_gate.gate_policy,
            required_evidence=persisted_gate.required_evidence,
            opened_by_decision_id=persisted_gate.opened_by_decision_id,
            closed_by_decision_id=persisted_gate.closed_by_decision_id,
            opened_at=persisted_gate.opened_at,
            closed_at=persisted_gate.closed_at,
            created_at=persisted_decision.created_at,
            updated_at=persisted_decision.updated_at,
        )
