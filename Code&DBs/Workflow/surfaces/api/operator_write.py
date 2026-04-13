"""Merged operator write surface over control writes and workflow-flow entrypoints."""

from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from policy.workflow_classes import (
    WorkflowClassAuthorityRecord,
    WorkflowClassCatalog,
    load_workflow_class_catalog,
)
from policy.native_primary_cutover import (
    NativePrimaryCutoverGateRecord,
    NativePrimaryCutoverRepository,
    NativePrimaryCutoverRuntime,
    PostgresNativePrimaryCutoverRepository,
)
from runtime.instance import NativeDagInstance, resolve_native_instance
from runtime.recurring_review_repair_flow import (
    RecurringReviewRepairFlowRequest,
    RecurringReviewRepairFlowResolution,
    resolve_recurring_review_repair_flow,
)
from runtime.work_item_workflow_bindings import (
    PostgresWorkItemWorkflowBindingRepository,
    WorkItemWorkflowBindingRecord,
    WorkItemWorkflowBindingRepository,
    WorkItemWorkflowBindingRuntime,
)
from storage.postgres import (
    PostgresRoadmapAuthoringRepository,
    PostgresTaskRouteEligibilityRepository,
    connect_workflow_database,
)
from ._operator_helpers import _json_compatible, _normalize_as_of, _now, _run_async


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str:
        """Execute one statement."""

    async def fetch(self, query: str, *args: object) -> list[Any]:
        """Fetch rows."""

    async def fetchrow(self, query: str, *args: object) -> Any:
        """Fetch one row."""

    def transaction(self) -> AsyncIterator[object]:
        """Open a transaction context."""

    async def close(self) -> None:
        """Close the connection."""


_TASK_ROUTE_ELIGIBILITY_STATUSES = frozenset({"eligible", "rejected"})
_ROADMAP_WRITE_ACTIONS = frozenset({"preview", "validate", "commit"})
_WORK_ITEM_CLOSEOUT_ACTIONS = frozenset({"preview", "commit"})
_ROADMAP_ITEM_KINDS = frozenset({"capability", "initiative"})
_ROADMAP_STATUSES = frozenset({"active"})
_ROADMAP_PRIORITIES = frozenset({"p1", "p2"})
_BUG_CLOSEOUT_EVIDENCE_ROLE = "validates_fix"
_ROADMAP_COMPLETED_STATUS = "completed"


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _normalize_task_route_eligibility_status(value: object) -> str:
    normalized = _require_text(value, field_name="eligibility_status").lower()
    if normalized not in _TASK_ROUTE_ELIGIBILITY_STATUSES:
        raise ValueError(
            "eligibility_status must be one of eligible, rejected"
        )
    return normalized


def _scope_fragment(value: str | None, *, fallback: str) -> str:
    if value is None:
        return fallback
    normalized = value.strip().lower()
    fragments = [
        char if char.isalnum() else "-"
        for char in normalized
    ]
    collapsed = "".join(fragments).strip("-")
    while "--" in collapsed:
        collapsed = collapsed.replace("--", "-")
    return collapsed or fallback


def _coerce_text_sequence(
    value: object,
    *,
    field_name: str,
) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_require_text(value, field_name=field_name),)
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field_name} must be a list of non-empty strings")
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(
            _require_text(item, field_name=f"{field_name}[{index}]")
        )
    return tuple(dict.fromkeys(normalized))


def _normalize_registry_paths(value: object) -> tuple[str, ...]:
    return _coerce_text_sequence(value, field_name="registry_paths")


def _normalize_roadmap_action(value: object) -> str:
    normalized = _require_text(value, field_name="action").lower()
    if normalized not in _ROADMAP_WRITE_ACTIONS:
        allowed = ", ".join(sorted(_ROADMAP_WRITE_ACTIONS))
        raise ValueError(f"action must be one of {allowed}")
    return normalized


def _normalize_work_item_closeout_action(value: object) -> str:
    normalized = _require_text(value, field_name="action").lower()
    if normalized not in _WORK_ITEM_CLOSEOUT_ACTIONS:
        allowed = ", ".join(sorted(_WORK_ITEM_CLOSEOUT_ACTIONS))
        raise ValueError(f"action must be one of {allowed}")
    return normalized


def _normalize_roadmap_item_kind(value: object | None, *, template: str) -> str:
    if value is None:
        return "capability" if template == "hard_cutover_program" else "capability"
    normalized = _require_text(value, field_name="item_kind").lower()
    if normalized not in _ROADMAP_ITEM_KINDS:
        allowed = ", ".join(sorted(_ROADMAP_ITEM_KINDS))
        raise ValueError(f"item_kind must be one of {allowed}")
    return normalized


def _normalize_roadmap_status(value: object | None) -> str:
    if value is None:
        return "active"
    normalized = _require_text(value, field_name="status").lower()
    if normalized not in _ROADMAP_STATUSES:
        allowed = ", ".join(sorted(_ROADMAP_STATUSES))
        raise ValueError(f"status must be one of {allowed}")
    return normalized


def _normalize_roadmap_priority(value: object | None) -> str:
    if value is None:
        return "p2"
    normalized = _require_text(value, field_name="priority").lower()
    if normalized not in _ROADMAP_PRIORITIES:
        allowed = ", ".join(sorted(_ROADMAP_PRIORITIES))
        raise ValueError(f"priority must be one of {allowed}")
    return normalized


def _slugify_roadmap_text(value: str) -> str:
    lowered = value.strip().lower()
    tokens = re.findall(r"[a-z0-9]+", lowered)
    return ".".join(tokens) or "item"


def _roadmap_key_from_item_id(roadmap_item_id: str) -> str:
    prefix = "roadmap_item."
    if roadmap_item_id.startswith(prefix):
        return f"roadmap.{roadmap_item_id[len(prefix):]}"
    return roadmap_item_id.replace("_", ".")


def _roadmap_dependency_id(
    *,
    roadmap_item_id: str,
    depends_on_roadmap_item_id: str,
    dependency_kind: str,
) -> str:
    return (
        f"roadmap_item_dependency."
        f"{roadmap_item_id.replace('_', '.').replace(':', '.').replace('/', '.')}"
        f".{dependency_kind}."
        f"{depends_on_roadmap_item_id.replace('_', '.').replace(':', '.').replace('/', '.')}"
    )


def _parse_phase_order(value: object) -> tuple[int, ...] | None:
    if not isinstance(value, str) or not value.strip():
        return None
    tokens = value.strip().split(".")
    parsed: list[int] = []
    for token in tokens:
        if not token.isdigit():
            return None
        parsed.append(int(token))
    return tuple(parsed) if parsed else None


def _format_phase_order(parts: tuple[int, ...]) -> str:
    return ".".join(str(part) for part in parts)


def _next_phase_order(existing_values: tuple[str, ...]) -> str:
    parsed_values = [parsed for value in existing_values if (parsed := _parse_phase_order(value)) is not None]
    if not parsed_values:
        return "1"
    best = max(parsed_values)
    if len(best) == 1:
        return _format_phase_order((best[0] + 1,))
    return _format_phase_order((*best[:-1], best[-1] + 1))


def _default_approval_tag(now: datetime) -> str:
    return f"operator-write-{now.astimezone(timezone.utc).date().isoformat()}"


def _default_decision_ref(slug: str, now: datetime) -> str:
    return f"decision.{now.astimezone(timezone.utc).date().isoformat()}.{slug}"


def _acceptance_payload(
    *,
    tier: str,
    phase_ready: bool,
    approval_tag: str,
    outcome_gate: str,
    phase_order: str,
    reference_doc: str | None,
    must_have: tuple[str, ...],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "tier": tier,
        "must_have": list(must_have),
        "phase_ready": phase_ready,
        "approval_tag": approval_tag,
        "outcome_gate": outcome_gate,
        "phase_order": phase_order,
    }
    if reference_doc:
        payload["reference_doc"] = reference_doc
    return payload


@dataclass(frozen=True, slots=True)
class _RoadmapTemplateChild:
    suffix: str
    title: str
    priority: str
    summary: str
    must_have: tuple[str, ...]


_ROADMAP_TEMPLATE_CHILDREN: dict[str, tuple[_RoadmapTemplateChild, ...]] = {
    "single_capability": (),
    "hard_cutover_program": (
        _RoadmapTemplateChild(
            suffix="contracts",
            title="Canonical authoring contract and template pack",
            priority="p1",
            summary="Define the typed authoring contract, template library, and normalization rules so roadmap writes stop requiring hand-built rows.",
            must_have=(
                "Define the typed roadmap authoring contract.",
                "Ship reusable template definitions for common roadmap package shapes.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="validation_gate",
            title="Shared validation and normalization gate",
            priority="p1",
            summary="Move roadmap authoring through one preview-first validation gate that auto-fixes deterministic issues and blocks only on unsafe ambiguity.",
            must_have=(
                "Preview, validate, and commit all run through one shared gate.",
                "Ids, keys, and dependency ids are generated automatically when safe.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="frontdoors",
            title="CLI and MCP operator-write front doors",
            priority="p1",
            summary="Expose the shared gate through native-operator CLI and MCP so roadmap authoring stops depending on raw SQL or one-off scripts.",
            must_have=(
                "CLI and MCP call the same write service.",
                "Preview output is identical across both front doors.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="derived_views",
            title="Derived views and roadmap export cleanup",
            priority="p2",
            summary="Make roadmap markdown and operator views derived from DB-backed authority so authoring stays single-source.",
            must_have=(
                "Roadmap exports derive from DB-backed rows.",
                "No parallel markdown-only roadmap authority remains.",
            ),
        ),
        _RoadmapTemplateChild(
            suffix="proof",
            title="Validation proof and operator adoption",
            priority="p2",
            summary="Prove the gate is safe through preview parity, transaction safety, and representative roadmap authoring scenarios.",
            must_have=(
                "Representative roadmap authoring scenarios are covered by tests.",
                "Commit only occurs after the shared validation gate passes cleanly.",
            ),
        ),
    ),
}


def _require_roadmap_template(value: object | None) -> str:
    if value is None:
        return "single_capability"
    normalized = _require_text(value, field_name="template").lower()
    if normalized not in _ROADMAP_TEMPLATE_CHILDREN:
        allowed = ", ".join(sorted(_ROADMAP_TEMPLATE_CHILDREN))
        raise ValueError(f"template must be one of {allowed}")
    return normalized


def _roadmap_item_payload(
    *,
    roadmap_item_id: str,
    roadmap_key: str,
    title: str,
    item_kind: str,
    status: str,
    priority: str,
    parent_roadmap_item_id: str | None,
    source_bug_id: str | None,
    registry_paths: tuple[str, ...],
    summary: str,
    acceptance_criteria: Mapping[str, Any],
    decision_ref: str,
    created_at: datetime,
    updated_at: datetime,
) -> dict[str, Any]:
    return {
        "roadmap_item_id": roadmap_item_id,
        "roadmap_key": roadmap_key,
        "title": title,
        "item_kind": item_kind,
        "status": status,
        "priority": priority,
        "parent_roadmap_item_id": parent_roadmap_item_id,
        "source_bug_id": source_bug_id,
        "registry_paths": list(registry_paths),
        "summary": summary,
        "acceptance_criteria": _json_compatible(acceptance_criteria),
        "decision_ref": decision_ref,
        "target_start_at": None,
        "target_end_at": None,
        "completed_at": None,
        "created_at": created_at.isoformat(),
        "updated_at": updated_at.isoformat(),
    }


def _roadmap_dependency_payload(
    *,
    roadmap_item_dependency_id: str,
    roadmap_item_id: str,
    depends_on_roadmap_item_id: str,
    dependency_kind: str,
    decision_ref: str,
    created_at: datetime,
) -> dict[str, Any]:
    return {
        "roadmap_item_dependency_id": roadmap_item_dependency_id,
        "roadmap_item_id": roadmap_item_id,
        "depends_on_roadmap_item_id": depends_on_roadmap_item_id,
        "dependency_kind": dependency_kind,
        "decision_ref": decision_ref,
        "created_at": created_at.isoformat(),
    }


def _task_route_scope_label(
    *,
    provider_slug: str,
    task_type: str | None,
    model_slug: str | None,
) -> str:
    if task_type is None and model_slug is None:
        return f"provider={provider_slug}"
    if task_type is None:
        return f"provider={provider_slug} model={model_slug}"
    if model_slug is None:
        return f"provider={provider_slug} task_type={task_type}"
    return f"provider={provider_slug} task_type={task_type} model={model_slug}"


def _task_route_eligibility_id(
    *,
    provider_slug: str,
    task_type: str | None,
    model_slug: str | None,
    eligibility_status: str,
    effective_from: datetime,
) -> str:
    timestamp = effective_from.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        "task-route-eligibility."
        f"{_scope_fragment(provider_slug, fallback='provider')}"
        f".{_scope_fragment(task_type, fallback='any-task')}"
        f".{_scope_fragment(model_slug, fallback='any-model')}"
        f".{eligibility_status}.{timestamp}"
    )


def _task_route_decision_ref(
    *,
    provider_slug: str,
    task_type: str | None,
    model_slug: str | None,
    eligibility_status: str,
    effective_from: datetime,
) -> str:
    timestamp = effective_from.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        "decision:task-route-eligibility:"
        f"{_scope_fragment(provider_slug, fallback='provider')}:"
        f"{_scope_fragment(task_type, fallback='any-task')}:"
        f"{_scope_fragment(model_slug, fallback='any-model')}:"
        f"{eligibility_status}:{timestamp}"
    )


def _default_task_route_rationale(
    *,
    provider_slug: str,
    task_type: str | None,
    model_slug: str | None,
    eligibility_status: str,
    effective_to: datetime | None,
) -> str:
    action = "enabled" if eligibility_status == "eligible" else "disabled"
    until = (
        ""
        if effective_to is None
        else f" until {effective_to.astimezone(timezone.utc).isoformat()}"
    )
    return f"Operator {action} route scope {_task_route_scope_label(provider_slug=provider_slug, task_type=task_type, model_slug=model_slug)}{until}"


@dataclass(frozen=True, slots=True)
class TaskRouteEligibilityRecord:
    task_route_eligibility_id: str
    task_type: str | None
    provider_slug: str
    model_slug: str | None
    eligibility_status: str
    reason_code: str
    rationale: str
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str
    created_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "task_route_eligibility_id": self.task_route_eligibility_id,
            "task_type": self.task_type,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "eligibility_status": self.eligibility_status,
            "reason_code": self.reason_code,
            "rationale": self.rationale,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                None if self.effective_to is None else self.effective_to.isoformat()
            ),
            "decision_ref": self.decision_ref,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class TaskRouteEligibilityWriteResult:
    task_route_eligibility: TaskRouteEligibilityRecord
    superseded_task_route_eligibility_ids: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "task_route_eligibility": self.task_route_eligibility.to_json(),
            "superseded_task_route_eligibility_ids": list(
                self.superseded_task_route_eligibility_ids
            ),
        }


def _closeout_resolution_summary(*, bug_id: str, evidence_count: int) -> str:
    noun = "evidence row" if evidence_count == 1 else "evidence rows"
    return (
        "Auto-closed by work-item closeout reconciler from explicit "
        f"{_BUG_CLOSEOUT_EVIDENCE_ROLE} proof ({evidence_count} {noun}) for {bug_id}."
    )


def _task_route_eligibility_record_from_row(row: Mapping[str, Any]) -> TaskRouteEligibilityRecord:
    return TaskRouteEligibilityRecord(
        task_route_eligibility_id=str(row["task_route_eligibility_id"]),
        task_type=str(row["task_type"]) if row["task_type"] is not None else None,
        provider_slug=str(row["provider_slug"]),
        model_slug=str(row["model_slug"]) if row["model_slug"] is not None else None,
        eligibility_status=str(row["eligibility_status"]),
        reason_code=str(row["reason_code"]),
        rationale=str(row["rationale"]),
        effective_from=row["effective_from"],
        effective_to=row["effective_to"],
        decision_ref=str(row["decision_ref"]),
        created_at=row["created_at"],
    )


@dataclass(slots=True)
class OperatorControlFrontdoor:
    """Repo-local operator surface for bounded operator-control writes."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )
    task_route_eligibility_repository_factory: Callable[[
        _Connection,
    ], Any] | None = None
    roadmap_repository_factory: Callable[[
        _Connection,
    ], Any] | None = None
    binding_repository_factory: Callable[[
        _Connection,
    ], WorkItemWorkflowBindingRepository] | None = None
    native_primary_cutover_repository_factory: Callable[[
        _Connection,
    ], NativePrimaryCutoverRepository] | None = None

    def __post_init__(self) -> None:
        if self.task_route_eligibility_repository_factory is None:
            self.task_route_eligibility_repository_factory = (
                self._default_task_route_eligibility_repository_factory
            )
        if self.roadmap_repository_factory is None:
            self.roadmap_repository_factory = self._default_roadmap_repository_factory
        if self.binding_repository_factory is None:
            self.binding_repository_factory = self._default_binding_repository_factory
        if self.native_primary_cutover_repository_factory is None:
            self.native_primary_cutover_repository_factory = (
                self._default_native_primary_cutover_repository_factory
            )

    @staticmethod
    def _default_task_route_eligibility_repository_factory(
        conn: _Connection,
    ) -> Any:
        return PostgresTaskRouteEligibilityRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_roadmap_repository_factory(
        conn: _Connection,
    ) -> Any:
        return PostgresRoadmapAuthoringRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_binding_repository_factory(
        conn: _Connection,
    ) -> WorkItemWorkflowBindingRepository:
        return PostgresWorkItemWorkflowBindingRepository(conn)  # type: ignore[arg-type]

    @staticmethod
    def _default_native_primary_cutover_repository_factory(
        conn: _Connection,
    ) -> NativePrimaryCutoverRepository:
        return PostgresNativePrimaryCutoverRepository(conn)  # type: ignore[arg-type]

    async def _record_work_item_workflow_binding(
        self,
        *,
        env: Mapping[str, str] | None,
        binding_kind: str,
        bug_id: str | None,
        roadmap_item_id: str | None,
        workflow_class_id: str | None,
        schedule_definition_id: str | None,
        workflow_run_id: str | None,
        binding_status: str,
        bound_by_decision_id: str | None,
        created_at: datetime | None,
        updated_at: datetime | None,
    ) -> WorkItemWorkflowBindingRecord:
        conn = await self.connect_database(env)
        try:
            assert self.binding_repository_factory is not None
            runtime = WorkItemWorkflowBindingRuntime(
                repository=self.binding_repository_factory(conn),
            )
            return await runtime.record_binding(
                binding_kind=binding_kind,
                bug_id=bug_id,
                roadmap_item_id=roadmap_item_id,
                workflow_class_id=workflow_class_id,
                schedule_definition_id=schedule_definition_id,
                workflow_run_id=workflow_run_id,
                binding_status=binding_status,
                bound_by_decision_id=bound_by_decision_id,
                created_at=created_at,
                updated_at=updated_at,
            )
        finally:
            await conn.close()

    async def _admit_native_primary_cutover_gate(
        self,
        *,
        env: Mapping[str, str] | None,
        decided_by: str,
        decision_source: str,
        rationale: str,
        roadmap_item_id: str | None,
        workflow_class_id: str | None,
        schedule_definition_id: str | None,
        title: str | None,
        gate_name: str | None,
        gate_policy: Mapping[str, Any] | None,
        required_evidence: Mapping[str, Any] | None,
        decided_at: datetime | None,
        opened_at: datetime | None,
        created_at: datetime | None,
        updated_at: datetime | None,
    ) -> NativePrimaryCutoverGateRecord:
        conn = await self.connect_database(env)
        try:
            assert self.native_primary_cutover_repository_factory is not None
            runtime = NativePrimaryCutoverRuntime(
                repository=self.native_primary_cutover_repository_factory(conn),
            )
            return await runtime.admit_gate(
                decided_by=decided_by,
                decision_source=decision_source,
                rationale=rationale,
                roadmap_item_id=roadmap_item_id,
                workflow_class_id=workflow_class_id,
                schedule_definition_id=schedule_definition_id,
                title=title,
                gate_name=gate_name,
                gate_policy=gate_policy,
                required_evidence=required_evidence,
                decided_at=decided_at,
                opened_at=opened_at,
                created_at=created_at,
                updated_at=updated_at,
            )
        finally:
            await conn.close()

    async def _set_task_route_eligibility_window(
        self,
        *,
        env: Mapping[str, str] | None,
        provider_slug: str,
        eligibility_status: str,
        effective_to: datetime | None,
        task_type: str | None,
        model_slug: str | None,
        reason_code: str,
        rationale: str | None,
        effective_from: datetime | None,
        decision_ref: str | None,
    ) -> TaskRouteEligibilityWriteResult:
        normalized_provider_slug = _require_text(
            provider_slug,
            field_name="provider_slug",
        ).lower()
        normalized_eligibility_status = _normalize_task_route_eligibility_status(
            eligibility_status,
        )
        normalized_task_type = _optional_text(task_type, field_name="task_type")
        normalized_model_slug = _optional_text(model_slug, field_name="model_slug")
        normalized_reason_code = _require_text(reason_code, field_name="reason_code")
        normalized_effective_from = (
            _now()
            if effective_from is None
            else _normalize_as_of(
                effective_from,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_from",
            )
        )
        normalized_effective_to = (
            None
            if effective_to is None
            else _normalize_as_of(
                effective_to,
                error_type=ValueError,
                reason_code="operator_control.invalid_effective_to",
            )
        )
        if (
            normalized_effective_to is not None
            and normalized_effective_to <= normalized_effective_from
        ):
            raise ValueError("effective_to must be later than effective_from")
        normalized_rationale = (
            _optional_text(rationale, field_name="rationale")
            or _default_task_route_rationale(
                provider_slug=normalized_provider_slug,
                task_type=normalized_task_type,
                model_slug=normalized_model_slug,
                eligibility_status=normalized_eligibility_status,
                effective_to=normalized_effective_to,
            )
        )
        normalized_decision_ref = (
            _optional_text(decision_ref, field_name="decision_ref")
            or _task_route_decision_ref(
                provider_slug=normalized_provider_slug,
                task_type=normalized_task_type,
                model_slug=normalized_model_slug,
                eligibility_status=normalized_eligibility_status,
                effective_from=normalized_effective_from,
            )
        )
        task_route_eligibility_id = _task_route_eligibility_id(
            provider_slug=normalized_provider_slug,
            task_type=normalized_task_type,
            model_slug=normalized_model_slug,
            eligibility_status=normalized_eligibility_status,
            effective_from=normalized_effective_from,
        )

        conn = await self.connect_database(env)
        try:
            assert self.task_route_eligibility_repository_factory is not None
            repository = self.task_route_eligibility_repository_factory(conn)
            inserted_row, superseded_rows = await repository.record_task_route_eligibility_window(
                task_route_eligibility_id=task_route_eligibility_id,
                task_type=normalized_task_type,
                provider_slug=normalized_provider_slug,
                model_slug=normalized_model_slug,
                eligibility_status=normalized_eligibility_status,
                reason_code=normalized_reason_code,
                rationale=normalized_rationale,
                effective_from=normalized_effective_from,
                effective_to=normalized_effective_to,
                decision_ref=normalized_decision_ref,
                )
            if inserted_row is None:
                raise RuntimeError("failed to read inserted task route eligibility row")
            return TaskRouteEligibilityWriteResult(
                task_route_eligibility=_task_route_eligibility_record_from_row(
                    inserted_row,
                ),
                superseded_task_route_eligibility_ids=tuple(
                    str(row["task_route_eligibility_id"])
                    for row in superseded_rows
                ),
            )
        finally:
            await conn.close()

    async def _fetch_roadmap_item(
        self,
        conn: _Connection,
        *,
        roadmap_item_id: str,
    ) -> Mapping[str, Any] | None:
        return await conn.fetchrow(
            """
            SELECT
                roadmap_item_id,
                roadmap_key,
                title,
                item_kind,
                status,
                priority,
                parent_roadmap_item_id,
                source_bug_id,
                registry_paths,
                summary,
                acceptance_criteria,
                decision_ref,
                target_start_at,
                target_end_at,
                completed_at,
                created_at,
                updated_at
            FROM roadmap_items
            WHERE roadmap_item_id = $1
            """,
            roadmap_item_id,
        )

    async def _roadmap_item_exists(
        self,
        conn: _Connection,
        *,
        roadmap_item_id: str,
    ) -> bool:
        row = await conn.fetchrow(
            "SELECT roadmap_item_id FROM roadmap_items WHERE roadmap_item_id = $1",
            roadmap_item_id,
        )
        return row is not None

    async def _bug_exists(
        self,
        conn: _Connection,
        *,
        bug_id: str,
    ) -> bool:
        row = await conn.fetchrow(
            "SELECT bug_id FROM bugs WHERE bug_id = $1",
            bug_id,
        )
        return row is not None

    async def _roadmap_sibling_phase_orders(
        self,
        conn: _Connection,
        *,
        parent_roadmap_item_id: str | None,
    ) -> tuple[str, ...]:
        rows = await conn.fetch(
            """
            SELECT acceptance_criteria->>'phase_order' AS phase_order
            FROM roadmap_items
            WHERE parent_roadmap_item_id IS NOT DISTINCT FROM $1
            ORDER BY roadmap_item_id
            """,
            parent_roadmap_item_id,
        )
        return tuple(
            str(row["phase_order"])
            for row in rows
            if row.get("phase_order")
        )

    async def _prepare_roadmap_write(
        self,
        conn: _Connection,
        *,
        action: str,
        title: str,
        intent_brief: str,
        template: str,
        priority: str,
        parent_roadmap_item_id: str | None,
        slug: str | None,
        depends_on: tuple[str, ...],
        source_bug_id: str | None,
        registry_paths: tuple[str, ...],
        decision_ref: str | None,
        item_kind: str | None,
        status: str | None,
        tier: str | None,
        phase_ready: bool | None,
        approval_tag: str | None,
        reference_doc: str | None,
        outcome_gate: str | None,
    ) -> dict[str, Any]:
        now = _now()
        normalized_action = _normalize_roadmap_action(action)
        normalized_title = _require_text(title, field_name="title")
        normalized_intent_brief = _require_text(
            intent_brief,
            field_name="intent_brief",
        )
        normalized_template = _require_roadmap_template(template)
        normalized_priority = _normalize_roadmap_priority(priority)
        normalized_parent = _optional_text(
            parent_roadmap_item_id,
            field_name="parent_roadmap_item_id",
        )
        normalized_source_bug_id = _optional_text(
            source_bug_id,
            field_name="source_bug_id",
        )
        normalized_registry_paths = _normalize_registry_paths(registry_paths)
        normalized_status = _normalize_roadmap_status(status)
        normalized_depends_on = tuple(
            dependency
            for dependency in depends_on
            if dependency != normalized_parent
        )
        auto_fixes: list[str] = []
        warnings: list[str] = []
        blocking_errors: list[str] = []

        parent_row: Mapping[str, Any] | None = None
        if normalized_parent is not None:
            parent_row = await self._fetch_roadmap_item(
                conn,
                roadmap_item_id=normalized_parent,
            )
            if parent_row is None:
                blocking_errors.append(
                    f"parent roadmap item not found: {normalized_parent}"
                )

        if normalized_source_bug_id is not None and not await self._bug_exists(
            conn,
            bug_id=normalized_source_bug_id,
        ):
            blocking_errors.append(
                f"source bug not found: {normalized_source_bug_id}"
            )

        for dependency in normalized_depends_on:
            if not await self._roadmap_item_exists(conn, roadmap_item_id=dependency):
                blocking_errors.append(f"dependency roadmap item not found: {dependency}")

        normalized_slug = _optional_text(slug, field_name="slug")
        if normalized_slug is None:
            normalized_slug = _slugify_roadmap_text(normalized_title)
            auto_fixes.append(f"slug generated from title: {normalized_slug}")
        else:
            normalized_slug = _slugify_roadmap_text(normalized_slug)

        normalized_item_kind = _normalize_roadmap_item_kind(
            item_kind,
            template=normalized_template,
        )

        parent_acceptance = (
            parent_row.get("acceptance_criteria")
            if parent_row is not None
            and isinstance(parent_row.get("acceptance_criteria"), Mapping)
            else {}
        )
        normalized_tier = (
            _optional_text(tier, field_name="tier")
            or (
                str(parent_acceptance.get("tier")).strip()
                if isinstance(parent_acceptance.get("tier"), str)
                and str(parent_acceptance.get("tier")).strip()
                else "tier_1"
            )
        )
        normalized_phase_ready = (
            bool(phase_ready)
            if phase_ready is not None
            else bool(parent_acceptance.get("phase_ready", False))
        )
        normalized_approval_tag = (
            _optional_text(approval_tag, field_name="approval_tag")
            or (
                str(parent_acceptance.get("approval_tag")).strip()
                if isinstance(parent_acceptance.get("approval_tag"), str)
                and str(parent_acceptance.get("approval_tag")).strip()
                else _default_approval_tag(now)
            )
        )
        if approval_tag is None:
            auto_fixes.append(f"approval_tag generated: {normalized_approval_tag}")
        normalized_reference_doc = _optional_text(
            reference_doc,
            field_name="reference_doc",
        )
        normalized_outcome_gate = (
            _optional_text(outcome_gate, field_name="outcome_gate")
            or normalized_intent_brief
        )
        normalized_decision_ref = (
            _optional_text(decision_ref, field_name="decision_ref")
            or _default_decision_ref(normalized_slug.replace(".", "-"), now)
        )
        if decision_ref is None:
            auto_fixes.append(f"decision_ref generated: {normalized_decision_ref}")

        root_roadmap_item_id = (
            f"{normalized_parent}.{normalized_slug}"
            if normalized_parent is not None
            else f"roadmap_item.{normalized_slug}"
        )
        root_roadmap_key = _roadmap_key_from_item_id(root_roadmap_item_id)
        if root_roadmap_item_id in normalized_depends_on:
            blocking_errors.append(
                f"roadmap item cannot depend on itself: {root_roadmap_item_id}"
            )

        sibling_phase_orders = await self._roadmap_sibling_phase_orders(
            conn,
            parent_roadmap_item_id=normalized_parent,
        )
        root_phase_order = _next_phase_order(sibling_phase_orders)
        auto_fixes.append(f"phase_order assigned: {root_phase_order}")

        root_acceptance = _acceptance_payload(
            tier=normalized_tier,
            phase_ready=normalized_phase_ready,
            approval_tag=normalized_approval_tag,
            outcome_gate=normalized_outcome_gate,
            phase_order=root_phase_order,
            reference_doc=normalized_reference_doc,
            must_have=(normalized_intent_brief,),
        )

        root_item = _roadmap_item_payload(
            roadmap_item_id=root_roadmap_item_id,
            roadmap_key=root_roadmap_key,
            title=normalized_title,
            item_kind=normalized_item_kind,
            status=normalized_status,
            priority=normalized_priority,
            parent_roadmap_item_id=normalized_parent,
            source_bug_id=normalized_source_bug_id,
            registry_paths=normalized_registry_paths,
            summary=normalized_intent_brief,
            acceptance_criteria=root_acceptance,
            decision_ref=normalized_decision_ref,
            created_at=now,
            updated_at=now,
        )

        preview_items: list[dict[str, Any]] = [root_item]
        preview_dependencies: list[dict[str, Any]] = []

        for dependency in normalized_depends_on:
            preview_dependencies.append(
                _roadmap_dependency_payload(
                    roadmap_item_dependency_id=_roadmap_dependency_id(
                        roadmap_item_id=root_roadmap_item_id,
                        depends_on_roadmap_item_id=dependency,
                        dependency_kind="blocks",
                    ),
                    roadmap_item_id=root_roadmap_item_id,
                    depends_on_roadmap_item_id=dependency,
                    dependency_kind="blocks",
                    decision_ref=normalized_decision_ref,
                    created_at=now,
                )
            )

        previous_item_id = root_roadmap_item_id
        template_children = _ROADMAP_TEMPLATE_CHILDREN[normalized_template]
        for index, child in enumerate(template_children, start=1):
            child_item_id = f"{root_roadmap_item_id}.{child.suffix}"
            child_phase_order = f"{root_phase_order}.{index}"
            child_acceptance = _acceptance_payload(
                tier=normalized_tier,
                phase_ready=normalized_phase_ready,
                approval_tag=normalized_approval_tag,
                outcome_gate=child.summary,
                phase_order=child_phase_order,
                reference_doc=normalized_reference_doc,
                must_have=child.must_have,
            )
            preview_items.append(
                _roadmap_item_payload(
                    roadmap_item_id=child_item_id,
                    roadmap_key=_roadmap_key_from_item_id(child_item_id),
                    title=child.title,
                    item_kind="capability",
                    status=normalized_status,
                    priority=child.priority,
                    parent_roadmap_item_id=root_roadmap_item_id,
                    source_bug_id=normalized_source_bug_id,
                    registry_paths=normalized_registry_paths,
                    summary=child.summary,
                    acceptance_criteria=child_acceptance,
                    decision_ref=normalized_decision_ref,
                    created_at=now,
                    updated_at=now,
                )
            )
            preview_dependencies.append(
                _roadmap_dependency_payload(
                    roadmap_item_dependency_id=_roadmap_dependency_id(
                        roadmap_item_id=child_item_id,
                        depends_on_roadmap_item_id=previous_item_id,
                        dependency_kind="blocks",
                    ),
                    roadmap_item_id=child_item_id,
                    depends_on_roadmap_item_id=previous_item_id,
                    dependency_kind="blocks",
                    decision_ref=normalized_decision_ref,
                    created_at=now,
                )
            )
            previous_item_id = child_item_id

        normalized_payload = {
            "action": normalized_action,
            "template": normalized_template,
            "title": normalized_title,
            "intent_brief": normalized_intent_brief,
            "slug": normalized_slug,
            "item_kind": normalized_item_kind,
            "status": normalized_status,
            "priority": normalized_priority,
            "parent_roadmap_item_id": normalized_parent,
            "depends_on": list(normalized_depends_on),
            "source_bug_id": normalized_source_bug_id,
            "registry_paths": list(normalized_registry_paths),
            "decision_ref": normalized_decision_ref,
            "tier": normalized_tier,
            "phase_ready": normalized_phase_ready,
            "approval_tag": normalized_approval_tag,
            "reference_doc": normalized_reference_doc,
            "outcome_gate": normalized_outcome_gate,
            "root_phase_order": root_phase_order,
        }

        return {
            "action": normalized_action,
            "normalized_payload": normalized_payload,
            "auto_fixes": auto_fixes,
            "warnings": warnings,
            "blocking_errors": blocking_errors,
            "preview": {
                "roadmap_items": preview_items,
                "roadmap_item_dependencies": preview_dependencies,
            },
        }

    async def _roadmap_write(
        self,
        *,
        env: Mapping[str, str] | None,
        action: str,
        title: str,
        intent_brief: str,
        template: str = "single_capability",
        priority: str = "p2",
        parent_roadmap_item_id: str | None = None,
        slug: str | None = None,
        depends_on: tuple[str, ...] = (),
        source_bug_id: str | None = None,
        registry_paths: tuple[str, ...] = (),
        decision_ref: str | None = None,
        item_kind: str | None = None,
        status: str | None = None,
        tier: str | None = None,
        phase_ready: bool | None = None,
        approval_tag: str | None = None,
        reference_doc: str | None = None,
        outcome_gate: str | None = None,
    ) -> dict[str, Any]:
        conn = await self.connect_database(env)
        try:
            preview = await self._prepare_roadmap_write(
                conn,
                action=action,
                title=title,
                intent_brief=intent_brief,
                template=template,
                priority=priority,
                parent_roadmap_item_id=parent_roadmap_item_id,
                slug=slug,
                depends_on=depends_on,
                source_bug_id=source_bug_id,
                registry_paths=registry_paths,
                decision_ref=decision_ref,
                item_kind=item_kind,
                status=status,
                tier=tier,
                phase_ready=phase_ready,
                approval_tag=approval_tag,
                reference_doc=reference_doc,
                outcome_gate=outcome_gate,
            )
            if preview["blocking_errors"] or preview["action"] != "commit":
                preview["committed"] = False
                return preview

            assert self.roadmap_repository_factory is not None
            repository = self.roadmap_repository_factory(conn)
            commit_summary = await repository.record_roadmap_package(
                roadmap_items=preview["preview"]["roadmap_items"],
                roadmap_item_dependencies=preview["preview"]["roadmap_item_dependencies"],
            )

            preview["committed"] = True
            preview["commit_summary"] = commit_summary
            return preview
        finally:
            await conn.close()

    async def _fetch_bug_rows_for_closeout(
        self,
        conn: _Connection,
        *,
        bug_ids: tuple[str, ...],
    ) -> tuple[Mapping[str, Any], ...]:
        if bug_ids:
            rows = await conn.fetch(
                """
                SELECT
                    bug_id,
                    title,
                    status,
                    resolution_summary,
                    resolved_at,
                    updated_at
                FROM bugs
                WHERE bug_id = ANY($1::text[])
                ORDER BY bug_id
                """,
                list(bug_ids),
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    bug_id,
                    title,
                    status,
                    resolution_summary,
                    resolved_at,
                    updated_at
                FROM bugs
                WHERE resolved_at IS NULL
                ORDER BY opened_at DESC, created_at DESC, bug_id
                """
            )
        return tuple(rows)

    async def _fetch_bug_evidence_for_closeout(
        self,
        conn: _Connection,
        *,
        bug_ids: tuple[str, ...],
    ) -> dict[str, tuple[Mapping[str, Any], ...]]:
        if not bug_ids:
            return {}
        rows = await conn.fetch(
            """
            SELECT
                bug_id,
                evidence_kind,
                evidence_ref,
                evidence_role
            FROM bug_evidence_links
            WHERE bug_id = ANY($1::text[])
            ORDER BY bug_id, created_at, bug_evidence_link_id
            """,
            list(bug_ids),
        )
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["bug_id"]), []).append(dict(row))
        return {bug_id: tuple(items) for bug_id, items in grouped.items()}

    async def _fetch_roadmap_rows_for_closeout(
        self,
        conn: _Connection,
        *,
        roadmap_item_ids: tuple[str, ...],
        source_bug_ids: tuple[str, ...],
    ) -> tuple[Mapping[str, Any], ...]:
        if roadmap_item_ids:
            return tuple(
                await conn.fetch(
                    """
                    SELECT
                        roadmap_item_id,
                        title,
                        status,
                        source_bug_id,
                        completed_at,
                        updated_at
                    FROM roadmap_items
                    WHERE roadmap_item_id = ANY($1::text[])
                    ORDER BY roadmap_item_id
                    """,
                    list(roadmap_item_ids),
                )
            )
        if not source_bug_ids:
            return ()
        return tuple(
            await conn.fetch(
                """
                SELECT
                    roadmap_item_id,
                    title,
                    status,
                    source_bug_id,
                    completed_at,
                    updated_at
                FROM roadmap_items
                WHERE source_bug_id = ANY($1::text[])
                  AND completed_at IS NULL
                ORDER BY roadmap_item_id
                """,
                list(source_bug_ids),
            )
        )

    async def _reconcile_work_item_closeout(
        self,
        *,
        env: Mapping[str, str] | None,
        action: str,
        bug_ids: tuple[str, ...],
        roadmap_item_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        normalized_action = _normalize_work_item_closeout_action(action)
        conn = await self.connect_database(env)
        try:
            scoped_roadmap_rows = await self._fetch_roadmap_rows_for_closeout(
                conn,
                roadmap_item_ids=roadmap_item_ids,
                source_bug_ids=(),
            )
            supplemental_bug_ids = tuple(
                dict.fromkeys(
                    str(row["source_bug_id"])
                    for row in scoped_roadmap_rows
                    if row.get("source_bug_id") is not None
                )
            )
            scoped_bug_ids = tuple(
                dict.fromkeys((*bug_ids, *supplemental_bug_ids))
            )
            bug_rows = await self._fetch_bug_rows_for_closeout(
                conn,
                bug_ids=scoped_bug_ids,
            )
            if not roadmap_item_ids:
                proof_bug_ids = tuple(str(row["bug_id"]) for row in bug_rows)
                scoped_roadmap_rows = await self._fetch_roadmap_rows_for_closeout(
                    conn,
                    roadmap_item_ids=(),
                    source_bug_ids=proof_bug_ids,
                )

            evidence_by_bug_id = await self._fetch_bug_evidence_for_closeout(
                conn,
                bug_ids=tuple(str(row["bug_id"]) for row in bug_rows),
            )
            proof_bug_ids = {
                bug_id
                for bug_id, rows in evidence_by_bug_id.items()
                if any(str(row["evidence_role"]) == _BUG_CLOSEOUT_EVIDENCE_ROLE for row in rows)
            }
            now = _now()

            bug_candidates: list[dict[str, Any]] = []
            bug_skipped: list[dict[str, Any]] = []
            for row in bug_rows:
                bug_id = str(row["bug_id"])
                evidence_refs = [
                    {
                        "kind": str(evidence["evidence_kind"]),
                        "ref": str(evidence["evidence_ref"]),
                        "role": str(evidence["evidence_role"]),
                    }
                    for evidence in evidence_by_bug_id.get(bug_id, ())
                    if str(evidence["evidence_role"]) == _BUG_CLOSEOUT_EVIDENCE_ROLE
                ]
                if row.get("resolved_at") is None and evidence_refs:
                    bug_candidates.append(
                        {
                            "bug_id": bug_id,
                            "current_status": str(row["status"]),
                            "next_status": "FIXED",
                            "reason_codes": ["explicit_fix_proof_present"],
                            "evidence_refs": evidence_refs,
                            "resolution_summary": _closeout_resolution_summary(
                                bug_id=bug_id,
                                evidence_count=len(evidence_refs),
                            ),
                        }
                    )
                    continue
                reason_codes = []
                if row.get("resolved_at") is not None:
                    reason_codes.append("already_resolved")
                if bug_id not in proof_bug_ids:
                    reason_codes.append("missing_validates_fix_evidence")
                if reason_codes:
                    bug_skipped.append(
                        {
                            "bug_id": bug_id,
                            "current_status": str(row["status"]),
                            "reason_codes": reason_codes,
                        }
                    )

            roadmap_candidates: list[dict[str, Any]] = []
            roadmap_skipped: list[dict[str, Any]] = []
            for row in scoped_roadmap_rows:
                roadmap_item_id = str(row["roadmap_item_id"])
                source_bug_id = (
                    str(row["source_bug_id"])
                    if row.get("source_bug_id") is not None
                    else None
                )
                if row.get("completed_at") is None and source_bug_id in proof_bug_ids:
                    roadmap_candidates.append(
                        {
                            "roadmap_item_id": roadmap_item_id,
                            "source_bug_id": source_bug_id,
                            "current_status": str(row["status"]),
                            "next_status": _ROADMAP_COMPLETED_STATUS,
                            "reason_codes": [
                                "source_bug_has_explicit_fix_proof",
                            ],
                            "evidence_refs": [
                                {
                                    "kind": str(evidence["evidence_kind"]),
                                    "ref": str(evidence["evidence_ref"]),
                                    "role": str(evidence["evidence_role"]),
                                }
                                for evidence in evidence_by_bug_id.get(source_bug_id or "", ())
                                if str(evidence["evidence_role"]) == _BUG_CLOSEOUT_EVIDENCE_ROLE
                            ],
                        }
                    )
                    continue
                reason_codes = []
                if row.get("completed_at") is not None:
                    reason_codes.append("already_completed")
                if source_bug_id is None:
                    reason_codes.append("missing_source_bug")
                elif source_bug_id not in proof_bug_ids:
                    reason_codes.append("source_bug_missing_validates_fix_evidence")
                if reason_codes:
                    roadmap_skipped.append(
                        {
                            "roadmap_item_id": roadmap_item_id,
                            "source_bug_id": source_bug_id,
                            "current_status": str(row["status"]),
                            "reason_codes": reason_codes,
                        }
                    )

            payload: dict[str, Any] = {
                "action": normalized_action,
                "proof_threshold": {
                    "bug_requires_evidence_role": _BUG_CLOSEOUT_EVIDENCE_ROLE,
                    "roadmap_requires_source_bug_fix_proof": True,
                },
                "evaluated": {
                    "bug_ids": [str(row["bug_id"]) for row in bug_rows],
                    "roadmap_item_ids": [str(row["roadmap_item_id"]) for row in scoped_roadmap_rows],
                },
                "candidates": {
                    "bugs": bug_candidates,
                    "roadmap_items": roadmap_candidates,
                },
                "skipped": {
                    "bugs": bug_skipped,
                    "roadmap_items": roadmap_skipped,
                },
                "committed": False,
                "applied": {
                    "bugs": [],
                    "roadmap_items": [],
                },
            }
            if normalized_action != "commit":
                return payload

            async with conn.transaction():
                applied_bug_rows = []
                if bug_candidates:
                    applied_bug_rows = await conn.fetch(
                        """
                        UPDATE bugs AS bug
                        SET
                            status = 'FIXED',
                            resolved_at = COALESCE(bug.resolved_at, $1),
                            updated_at = $1,
                            resolution_summary = COALESCE(
                                NULLIF(bug.resolution_summary, ''),
                                candidate.resolution_summary
                            )
                        FROM UNNEST($2::text[], $3::text[]) AS candidate(
                            bug_id,
                            resolution_summary
                        )
                        WHERE bug.bug_id = candidate.bug_id
                          AND bug.resolved_at IS NULL
                        RETURNING
                            bug.bug_id,
                            bug.status,
                            bug.resolved_at,
                            bug.resolution_summary
                        """,
                        now,
                        [candidate["bug_id"] for candidate in bug_candidates],
                        [
                            candidate["resolution_summary"]
                            for candidate in bug_candidates
                        ],
                    )
                applied_roadmap_rows = []
                if roadmap_candidates:
                    applied_roadmap_rows = await conn.fetch(
                        """
                        UPDATE roadmap_items
                        SET
                            status = $1,
                            completed_at = COALESCE(completed_at, $2),
                            updated_at = $2
                        WHERE roadmap_item_id = ANY($3::text[])
                          AND completed_at IS NULL
                        RETURNING roadmap_item_id, status, completed_at, source_bug_id
                        """,
                        _ROADMAP_COMPLETED_STATUS,
                        now,
                        [candidate["roadmap_item_id"] for candidate in roadmap_candidates],
                    )
            payload["committed"] = True
            payload["applied"] = {
                "bugs": [
                    {
                        "bug_id": str(row["bug_id"]),
                        "status": str(row["status"]),
                        "resolved_at": row["resolved_at"].isoformat() if row["resolved_at"] is not None else None,
                        "resolution_summary": str(row["resolution_summary"]) if row["resolution_summary"] is not None else None,
                    }
                    for row in applied_bug_rows
                ],
                "roadmap_items": [
                    {
                        "roadmap_item_id": str(row["roadmap_item_id"]),
                        "status": str(row["status"]),
                        "completed_at": row["completed_at"].isoformat() if row["completed_at"] is not None else None,
                        "source_bug_id": str(row["source_bug_id"]) if row["source_bug_id"] is not None else None,
                    }
                    for row in applied_roadmap_rows
                ],
            }
            return payload
        finally:
            await conn.close()

    async def set_task_route_eligibility_window_async(
        self,
        *,
        provider_slug: str,
        eligibility_status: str,
        effective_to: datetime | None = None,
        task_type: str | None = None,
        model_slug: str | None = None,
        reason_code: str = "operator_control",
        rationale: str | None = None,
        effective_from: datetime | None = None,
        decision_ref: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        result = await self._set_task_route_eligibility_window(
            env=env,
            provider_slug=provider_slug,
            eligibility_status=eligibility_status,
            effective_to=effective_to,
            task_type=task_type,
            model_slug=model_slug,
            reason_code=reason_code,
            rationale=rationale,
            effective_from=effective_from,
            decision_ref=decision_ref,
        )
        return result.to_json()

    async def roadmap_write_async(
        self,
        *,
        action: str = "preview",
        title: str,
        intent_brief: str,
        template: str = "single_capability",
        priority: str = "p2",
        parent_roadmap_item_id: str | None = None,
        slug: str | None = None,
        depends_on: tuple[str, ...] | list[str] | None = None,
        source_bug_id: str | None = None,
        registry_paths: tuple[str, ...] | list[str] | None = None,
        decision_ref: str | None = None,
        item_kind: str | None = None,
        status: str | None = None,
        tier: str | None = None,
        phase_ready: bool | None = None,
        approval_tag: str | None = None,
        reference_doc: str | None = None,
        outcome_gate: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return await self._roadmap_write(
            env=env,
            action=action,
            title=title,
            intent_brief=intent_brief,
            template=template,
            priority=priority,
            parent_roadmap_item_id=parent_roadmap_item_id,
            slug=slug,
            depends_on=_coerce_text_sequence(depends_on, field_name="depends_on"),
            source_bug_id=source_bug_id,
            registry_paths=_coerce_text_sequence(registry_paths, field_name="registry_paths"),
            decision_ref=decision_ref,
            item_kind=item_kind,
            status=status,
            tier=tier,
            phase_ready=phase_ready,
            approval_tag=approval_tag,
            reference_doc=reference_doc,
            outcome_gate=outcome_gate,
        )

    async def reconcile_work_item_closeout_async(
        self,
        *,
        action: str = "preview",
        bug_ids: tuple[str, ...] | list[str] | None = None,
        roadmap_item_ids: tuple[str, ...] | list[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return await self._reconcile_work_item_closeout(
            env=env,
            action=action,
            bug_ids=_coerce_text_sequence(bug_ids, field_name="bug_ids"),
            roadmap_item_ids=_coerce_text_sequence(
                roadmap_item_ids,
                field_name="roadmap_item_ids",
            ),
        )

    def set_task_route_eligibility_window(
        self,
        *,
        provider_slug: str,
        eligibility_status: str,
        effective_to: datetime | None = None,
        task_type: str | None = None,
        model_slug: str | None = None,
        reason_code: str = "operator_control",
        rationale: str | None = None,
        effective_from: datetime | None = None,
        decision_ref: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        result = _run_async(
            self._set_task_route_eligibility_window(
                env=env,
                provider_slug=provider_slug,
                eligibility_status=eligibility_status,
                effective_to=effective_to,
                task_type=task_type,
                model_slug=model_slug,
                reason_code=reason_code,
                rationale=rationale,
                effective_from=effective_from,
                decision_ref=decision_ref,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )
        return result.to_json()

    def reconcile_work_item_closeout(
        self,
        *,
        action: str = "preview",
        bug_ids: tuple[str, ...] | list[str] | None = None,
        roadmap_item_ids: tuple[str, ...] | list[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.reconcile_work_item_closeout_async(
                action=action,
                bug_ids=bug_ids,
                roadmap_item_ids=roadmap_item_ids,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    def roadmap_write(
        self,
        *,
        action: str = "preview",
        title: str,
        intent_brief: str,
        template: str = "single_capability",
        priority: str = "p2",
        parent_roadmap_item_id: str | None = None,
        slug: str | None = None,
        depends_on: tuple[str, ...] | list[str] | None = None,
        source_bug_id: str | None = None,
        registry_paths: tuple[str, ...] | list[str] | None = None,
        decision_ref: str | None = None,
        item_kind: str | None = None,
        status: str | None = None,
        tier: str | None = None,
        phase_ready: bool | None = None,
        approval_tag: str | None = None,
        reference_doc: str | None = None,
        outcome_gate: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.roadmap_write_async(
                action=action,
                title=title,
                intent_brief=intent_brief,
                template=template,
                priority=priority,
                parent_roadmap_item_id=parent_roadmap_item_id,
                slug=slug,
                depends_on=depends_on,
                source_bug_id=source_bug_id,
                registry_paths=registry_paths,
                decision_ref=decision_ref,
                item_kind=item_kind,
                status=status,
                tier=tier,
                phase_ready=phase_ready,
                approval_tag=approval_tag,
                reference_doc=reference_doc,
                outcome_gate=outcome_gate,
                env=env,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )

    async def record_work_item_workflow_binding_async(
        self,
        *,
        binding_kind: str,
        bug_id: str | None = None,
        roadmap_item_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        workflow_run_id: str | None = None,
        binding_status: str = "active",
        bound_by_decision_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Record one canonical work-item workflow binding in async contexts."""

        record = await self._record_work_item_workflow_binding(
            env=env,
            binding_kind=binding_kind,
            bug_id=bug_id,
            roadmap_item_id=roadmap_item_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
            workflow_run_id=workflow_run_id,
            binding_status=binding_status,
            bound_by_decision_id=bound_by_decision_id,
            created_at=created_at,
            updated_at=updated_at,
        )
        return {"binding": record.to_json()}

    async def admit_native_primary_cutover_gate_async(
        self,
        *,
        decided_by: str,
        decision_source: str,
        rationale: str,
        roadmap_item_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        title: str | None = None,
        gate_name: str | None = None,
        gate_policy: Mapping[str, Any] | None = None,
        required_evidence: Mapping[str, Any] | None = None,
        decided_at: datetime | None = None,
        opened_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Admit one bounded native-primary cutover gate in async contexts."""

        record = await self._admit_native_primary_cutover_gate(
            env=env,
            decided_by=decided_by,
            decision_source=decision_source,
            rationale=rationale,
            roadmap_item_id=roadmap_item_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
            title=title,
            gate_name=gate_name,
            gate_policy=gate_policy,
            required_evidence=required_evidence,
            decided_at=decided_at,
            opened_at=opened_at,
            created_at=created_at,
            updated_at=updated_at,
        )
        return {"native_primary_cutover": record.to_json()}

    def record_work_item_workflow_binding(
        self,
        *,
        binding_kind: str,
        bug_id: str | None = None,
        roadmap_item_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        workflow_run_id: str | None = None,
        binding_status: str = "active",
        bound_by_decision_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Record one canonical work-item workflow binding through Postgres."""

        record = _run_async(
            self._record_work_item_workflow_binding(
                env=env,
                binding_kind=binding_kind,
                bug_id=bug_id,
                roadmap_item_id=roadmap_item_id,
                workflow_class_id=workflow_class_id,
                schedule_definition_id=schedule_definition_id,
                workflow_run_id=workflow_run_id,
                binding_status=binding_status,
                bound_by_decision_id=bound_by_decision_id,
                created_at=created_at,
                updated_at=updated_at,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )
        return {"binding": record.to_json()}

    def admit_native_primary_cutover_gate(
        self,
        *,
        decided_by: str,
        decision_source: str,
        rationale: str,
        roadmap_item_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        title: str | None = None,
        gate_name: str | None = None,
        gate_policy: Mapping[str, Any] | None = None,
        required_evidence: Mapping[str, Any] | None = None,
        decided_at: datetime | None = None,
        opened_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Admit one bounded native-primary cutover gate through Postgres."""

        record = _run_async(
            self._admit_native_primary_cutover_gate(
                env=env,
                decided_by=decided_by,
                decision_source=decision_source,
                rationale=rationale,
                roadmap_item_id=roadmap_item_id,
                workflow_class_id=workflow_class_id,
                schedule_definition_id=schedule_definition_id,
                title=title,
                gate_name=gate_name,
                gate_policy=gate_policy,
                required_evidence=required_evidence,
                decided_at=decided_at,
                opened_at=opened_at,
                created_at=created_at,
                updated_at=updated_at,
            ),
            message=(
                "operator_control.async_boundary_required: "
                "operator control sync entrypoints require a non-async call boundary"
            ),
        )
        return {"native_primary_cutover": record.to_json()}


def record_work_item_workflow_binding(
    *,
    binding_kind: str,
    bug_id: str | None = None,
    roadmap_item_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    workflow_run_id: str | None = None,
    binding_status: str = "active",
    bound_by_decision_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical work-item workflow binding through the default frontdoor."""

    return OperatorControlFrontdoor().record_work_item_workflow_binding(
        binding_kind=binding_kind,
        bug_id=bug_id,
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        workflow_run_id=workflow_run_id,
        binding_status=binding_status,
        bound_by_decision_id=bound_by_decision_id,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def arecord_work_item_workflow_binding(
    *,
    binding_kind: str,
    bug_id: str | None = None,
    roadmap_item_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    workflow_run_id: str | None = None,
    binding_status: str = "active",
    bound_by_decision_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one canonical work-item workflow binding through the default async frontdoor."""

    return await OperatorControlFrontdoor().record_work_item_workflow_binding_async(
        binding_kind=binding_kind,
        bug_id=bug_id,
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        workflow_run_id=workflow_run_id,
        binding_status=binding_status,
        bound_by_decision_id=bound_by_decision_id,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def admit_native_primary_cutover_gate(
    *,
    decided_by: str,
    decision_source: str,
    rationale: str,
    roadmap_item_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    title: str | None = None,
    gate_name: str | None = None,
    gate_policy: Mapping[str, Any] | None = None,
    required_evidence: Mapping[str, Any] | None = None,
    decided_at: datetime | None = None,
    opened_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Admit one bounded native-primary cutover gate through the default frontdoor."""

    return OperatorControlFrontdoor().admit_native_primary_cutover_gate(
        decided_by=decided_by,
        decision_source=decision_source,
        rationale=rationale,
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        title=title,
        gate_name=gate_name,
        gate_policy=gate_policy,
        required_evidence=required_evidence,
        decided_at=decided_at,
        opened_at=opened_at,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def aadmit_native_primary_cutover_gate(
    *,
    decided_by: str,
    decision_source: str,
    rationale: str,
    roadmap_item_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    title: str | None = None,
    gate_name: str | None = None,
    gate_policy: Mapping[str, Any] | None = None,
    required_evidence: Mapping[str, Any] | None = None,
    decided_at: datetime | None = None,
    opened_at: datetime | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Admit one bounded native-primary cutover gate through the default async frontdoor."""

    return await OperatorControlFrontdoor().admit_native_primary_cutover_gate_async(
        decided_by=decided_by,
        decision_source=decision_source,
        rationale=rationale,
        roadmap_item_id=roadmap_item_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        title=title,
        gate_name=gate_name,
        gate_policy=gate_policy,
        required_evidence=required_evidence,
        decided_at=decided_at,
        opened_at=opened_at,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def reconcile_work_item_closeout(
    *,
    action: str = "preview",
    bug_ids: tuple[str, ...] | list[str] | None = None,
    roadmap_item_ids: tuple[str, ...] | list[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Preview or commit proof-backed bug and roadmap closeout through the default frontdoor."""

    return OperatorControlFrontdoor().reconcile_work_item_closeout(
        action=action,
        bug_ids=bug_ids,
        roadmap_item_ids=roadmap_item_ids,
        env=env,
    )


async def areconcile_work_item_closeout(
    *,
    action: str = "preview",
    bug_ids: tuple[str, ...] | list[str] | None = None,
    roadmap_item_ids: tuple[str, ...] | list[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Preview or commit proof-backed bug and roadmap closeout through the default async frontdoor."""

    return await OperatorControlFrontdoor().reconcile_work_item_closeout_async(
        action=action,
        bug_ids=bug_ids,
        roadmap_item_ids=roadmap_item_ids,
        env=env,
    )


def roadmap_write(
    *,
    action: str = "preview",
    title: str,
    intent_brief: str,
    template: str = "single_capability",
    priority: str = "p2",
    parent_roadmap_item_id: str | None = None,
    slug: str | None = None,
    depends_on: tuple[str, ...] | list[str] | None = None,
    source_bug_id: str | None = None,
    registry_paths: tuple[str, ...] | list[str] | None = None,
    decision_ref: str | None = None,
    item_kind: str | None = None,
    status: str | None = None,
    tier: str | None = None,
    phase_ready: bool | None = None,
    approval_tag: str | None = None,
    reference_doc: str | None = None,
    outcome_gate: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Write one roadmap item or packaged roadmap program through the default frontdoor."""

    return OperatorControlFrontdoor().roadmap_write(
        action=action,
        title=title,
        intent_brief=intent_brief,
        template=template,
        priority=priority,
        parent_roadmap_item_id=parent_roadmap_item_id,
        slug=slug,
        depends_on=depends_on,
        source_bug_id=source_bug_id,
        registry_paths=registry_paths,
        decision_ref=decision_ref,
        item_kind=item_kind,
        status=status,
        tier=tier,
        phase_ready=phase_ready,
        approval_tag=approval_tag,
        reference_doc=reference_doc,
        outcome_gate=outcome_gate,
        env=env,
    )


async def aroadmap_write(
    *,
    action: str = "preview",
    title: str,
    intent_brief: str,
    template: str = "single_capability",
    priority: str = "p2",
    parent_roadmap_item_id: str | None = None,
    slug: str | None = None,
    depends_on: tuple[str, ...] | list[str] | None = None,
    source_bug_id: str | None = None,
    registry_paths: tuple[str, ...] | list[str] | None = None,
    decision_ref: str | None = None,
    item_kind: str | None = None,
    status: str | None = None,
    tier: str | None = None,
    phase_ready: bool | None = None,
    approval_tag: str | None = None,
    reference_doc: str | None = None,
    outcome_gate: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Write one roadmap item or packaged roadmap program through the default async frontdoor."""

    return await OperatorControlFrontdoor().roadmap_write_async(
        action=action,
        title=title,
        intent_brief=intent_brief,
        template=template,
        priority=priority,
        parent_roadmap_item_id=parent_roadmap_item_id,
        slug=slug,
        depends_on=depends_on,
        source_bug_id=source_bug_id,
        registry_paths=registry_paths,
        decision_ref=decision_ref,
        item_kind=item_kind,
        status=status,
        tier=tier,
        phase_ready=phase_ready,
        approval_tag=approval_tag,
        reference_doc=reference_doc,
        outcome_gate=outcome_gate,
        env=env,
    )


def set_task_route_eligibility_window(
    *,
    provider_slug: str,
    eligibility_status: str,
    effective_to: datetime | None = None,
    task_type: str | None = None,
    model_slug: str | None = None,
    reason_code: str = "operator_control",
    rationale: str | None = None,
    effective_from: datetime | None = None,
    decision_ref: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one bounded task-route eligibility window through the default frontdoor."""

    return OperatorControlFrontdoor().set_task_route_eligibility_window(
        provider_slug=provider_slug,
        eligibility_status=eligibility_status,
        effective_to=effective_to,
        task_type=task_type,
        model_slug=model_slug,
        reason_code=reason_code,
        rationale=rationale,
        effective_from=effective_from,
        decision_ref=decision_ref,
        env=env,
    )


async def aset_task_route_eligibility_window(
    *,
    provider_slug: str,
    eligibility_status: str,
    effective_to: datetime | None = None,
    task_type: str | None = None,
    model_slug: str | None = None,
    reason_code: str = "operator_control",
    rationale: str | None = None,
    effective_from: datetime | None = None,
    decision_ref: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Record one bounded task-route eligibility window through the default async frontdoor."""

    return await OperatorControlFrontdoor().set_task_route_eligibility_window_async(
        provider_slug=provider_slug,
        eligibility_status=eligibility_status,
        effective_to=effective_to,
        task_type=task_type,
        model_slug=model_slug,
        reason_code=reason_code,
        rationale=rationale,
        effective_from=effective_from,
        decision_ref=decision_ref,
        env=env,
    )


class NativeWorkflowFlowError(RuntimeError):
    """Raised when the native workflow-flow surface cannot complete safely."""

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


@dataclass(frozen=True, slots=True)
class _WorkflowFlowSpec:
    flow_name: str
    class_name: str


_FLOW_SPECS: tuple[_WorkflowFlowSpec, ...] = (
    _WorkflowFlowSpec(flow_name="review", class_name="review"),
    _WorkflowFlowSpec(flow_name="repair", class_name="repair"),
    _WorkflowFlowSpec(flow_name="fanout", class_name="fanout"),
)


@dataclass(frozen=True, slots=True)
class NativeWorkflowFlowRecord:
    """One operator-visible workflow flow resolved from class authority."""

    flow_name: str
    workflow_class: WorkflowClassAuthorityRecord
    as_of: datetime

    @property
    def class_name(self) -> str:
        return self.workflow_class.class_name

    @property
    def class_kind(self) -> str:
        return self.workflow_class.class_kind

    @property
    def workflow_lane_id(self) -> str:
        return self.workflow_class.workflow_lane_id

    @property
    def review_required(self) -> bool:
        return self.workflow_class.review_required

    @property
    def decision_ref(self) -> str:
        return self.workflow_class.decision_ref

    def to_json(self) -> dict[str, Any]:
        return {
            "flow_name": self.flow_name,
            "workflow_class": {
                "workflow_class_id": self.workflow_class.workflow_class_id,
                "class_name": self.workflow_class.class_name,
                "class_kind": self.workflow_class.class_kind,
                "workflow_lane_id": self.workflow_class.workflow_lane_id,
                "status": self.workflow_class.status,
                "queue_shape": _json_compatible(self.workflow_class.queue_shape),
                "throttle_policy": _json_compatible(self.workflow_class.throttle_policy),
                "review_required": self.workflow_class.review_required,
                "effective_from": self.workflow_class.effective_from.isoformat(),
                "effective_to": (
                    None
                    if self.workflow_class.effective_to is None
                    else self.workflow_class.effective_to.isoformat()
                ),
                "decision_ref": self.workflow_class.decision_ref,
                "created_at": self.workflow_class.created_at.isoformat(),
            },
        }


@dataclass(frozen=True, slots=True)
class NativeWorkflowFlowCatalog:
    """Inspectable snapshot of native review, repair, and fanout flow authority."""

    flow_records: tuple[NativeWorkflowFlowRecord, ...]
    as_of: datetime
    workflow_class_authority: str = "policy.workflow_classes"

    @property
    def flow_names(self) -> tuple[str, ...]:
        return tuple(record.flow_name for record in self.flow_records)

    @classmethod
    def from_workflow_class_catalog(
        cls,
        *,
        class_catalog: WorkflowClassCatalog,
    ) -> "NativeWorkflowFlowCatalog":
        flow_records = tuple(
            NativeWorkflowFlowRecord(
                flow_name=spec.flow_name,
                workflow_class=class_catalog.resolve(class_name=spec.class_name).workflow_class,
                as_of=class_catalog.as_of,
            )
            for spec in _FLOW_SPECS
        )
        return cls(
            flow_records=flow_records,
            as_of=class_catalog.as_of,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "workflow_class_authority": self.workflow_class_authority,
            "as_of": self.as_of.isoformat(),
            "flow_names": list(self.flow_names),
            "flows": [record.to_json() for record in self.flow_records],
        }


@dataclass(frozen=True, slots=True)
class NativeRecurringReviewRepairFlowReadModel:
    """Operator-visible recurring review/repair read over the bounded flow seam."""

    recurring_review_repair_flow: RecurringReviewRepairFlowResolution
    as_of: datetime
    recurring_flow_authority: str = "runtime.recurring_review_repair_flow"

    def to_json(self) -> dict[str, Any]:
        return {
            "recurring_flow_authority": self.recurring_flow_authority,
            "as_of": self.as_of.isoformat(),
            "recurring_review_repair_flow": self.recurring_review_repair_flow.to_json(),
        }


@dataclass(slots=True)
class NativeWorkflowFlowFrontdoor:
    """Repo-local frontdoor for workflow-class review, repair, and fanout flows."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )

    def _resolve_instance(
        self,
        *,
        env: Mapping[str, str] | None,
    ) -> tuple[Mapping[str, str], NativeDagInstance]:
        source = env if env is not None else os.environ
        return source, resolve_native_instance(env=source)

    async def _inspect_workflow_flows(
        self,
        *,
        env: Mapping[str, str] | None,
        as_of: datetime,
    ) -> NativeWorkflowFlowCatalog:
        conn = await self.connect_database(env)
        try:
            class_catalog = await load_workflow_class_catalog(
                conn,
                as_of=as_of,
            )
            return NativeWorkflowFlowCatalog.from_workflow_class_catalog(
                class_catalog=class_catalog,
            )
        finally:
            await conn.close()

    async def _inspect_recurring_review_repair_flow(
        self,
        *,
        env: Mapping[str, str] | None,
        request: RecurringReviewRepairFlowRequest,
        as_of: datetime,
    ) -> NativeRecurringReviewRepairFlowReadModel:
        conn = await self.connect_database(env)
        try:
            resolution = await resolve_recurring_review_repair_flow(
                conn,  # type: ignore[arg-type]
                request=request,
                as_of=as_of,
            )
            return NativeRecurringReviewRepairFlowReadModel(
                recurring_review_repair_flow=resolution,
                as_of=resolution.as_of,
            )
        finally:
            await conn.close()

    def inspect_workflow_flows(
        self,
        *,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        """Inspect the native review, repair, and fanout flows through class authority."""

        source, instance = self._resolve_instance(env=env)
        flow_catalog = _run_async(
            self._inspect_workflow_flows(
                env=source,
                as_of=(
                    _now()
                    if as_of is None
                    else _normalize_as_of(
                        as_of,
                        error_type=NativeWorkflowFlowError,
                        reason_code="operator_workflow_flows.invalid_as_of",
                    )
                ),
            ),
            error_type=NativeWorkflowFlowError,
            reason_code="operator_workflow_flows.async_boundary_required",
            message="native workflow-flow sync entrypoints require a non-async call boundary",
        )
        return {
            "native_instance": instance.to_contract(),
            "workflow_class_authority": flow_catalog.workflow_class_authority,
            "as_of": flow_catalog.as_of.isoformat(),
            "flow_names": list(flow_catalog.flow_names),
            "flows": [record.to_json() for record in flow_catalog.flow_records],
        }

    def inspect_recurring_review_repair_flow(
        self,
        *,
        request: RecurringReviewRepairFlowRequest,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        """Inspect one bounded recurring review/repair operator path."""

        source, instance = self._resolve_instance(env=env)
        read_model = _run_async(
            self._inspect_recurring_review_repair_flow(
                env=source,
                request=request,
                as_of=(
                    _now()
                    if as_of is None
                    else _normalize_as_of(
                        as_of,
                        error_type=NativeWorkflowFlowError,
                        reason_code="operator_workflow_flows.invalid_as_of",
                    )
                ),
            ),
            error_type=NativeWorkflowFlowError,
            reason_code="operator_workflow_flows.async_boundary_required",
            message="native workflow-flow sync entrypoints require a non-async call boundary",
        )
        payload = read_model.to_json()
        return {
            "native_instance": instance.to_contract(),
            "recurring_flow_authority": payload["recurring_flow_authority"],
            "as_of": payload["as_of"],
            "recurring_review_repair_flow": payload["recurring_review_repair_flow"],
        }


def inspect_workflow_flows(
    *,
    env: Mapping[str, str] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Inspect the native review, repair, and fanout flows through repo-local authority."""

    return NativeWorkflowFlowFrontdoor().inspect_workflow_flows(
        env=env,
        as_of=as_of,
    )


def inspect_recurring_review_repair_flow(
    *,
    request: RecurringReviewRepairFlowRequest,
    env: Mapping[str, str] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Inspect one bounded recurring review/repair path through repo-local authority."""

    return NativeWorkflowFlowFrontdoor().inspect_recurring_review_repair_flow(
        request=request,
        env=env,
        as_of=as_of,
    )


__all__ = [
    "NativeWorkflowFlowCatalog",
    "NativeWorkflowFlowError",
    "NativeWorkflowFlowFrontdoor",
    "NativeWorkflowFlowRecord",
    "NativeRecurringReviewRepairFlowReadModel",
    "OperatorControlFrontdoor",
    "TaskRouteEligibilityRecord",
    "TaskRouteEligibilityWriteResult",
    "aadmit_native_primary_cutover_gate",
    "aroadmap_write",
    "areconcile_work_item_closeout",
    "aset_task_route_eligibility_window",
    "arecord_work_item_workflow_binding",
    "admit_native_primary_cutover_gate",
    "inspect_workflow_flows",
    "inspect_recurring_review_repair_flow",
    "roadmap_write",
    "reconcile_work_item_closeout",
    "record_work_item_workflow_binding",
    "set_task_route_eligibility_window",
]
