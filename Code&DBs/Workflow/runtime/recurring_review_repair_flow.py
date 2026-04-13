"""Bounded recurring review/repair flow over stored class and window authority.

This module keeps the recurring review/repair path explicit and fail closed:

- schedule/window meaning comes from stored ``schedule_definitions`` and
  ``recurring_run_windows`` rows
- review and repair dispatch meaning comes from stored ``workflow_classes`` and
  ``workflow_lane_policies`` rows
- the active recurring schedule must point at the resolved review class
- no shell-owned schedule folklore or broad scheduler cutover is consulted
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any

import asyncpg

from authority.workflow_class_resolution import (
    WorkflowClassResolutionDecision,
    WorkflowClassResolutionRuntime,
    load_workflow_class_resolution_runtime,
)
from runtime.scheduler_window_repository import (
    SchedulerWindowAuthorityCatalog,
    SchedulerWindowAuthorityResolution,
    load_scheduler_window_authority,
)
from runtime._helpers import _fail as _shared_fail


class RecurringReviewRepairFlowError(RuntimeError):
    """Raised when the bounded recurring review/repair flow cannot be resolved."""

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


_fail = partial(_shared_fail, error_type=RecurringReviewRepairFlowError)


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "recurring_review_repair_flow.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(
            "recurring_review_repair_flow.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "recurring_review_repair_flow.invalid_request",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


@dataclass(frozen=True, slots=True)
class RecurringReviewRepairFlowRequest:
    """Explicit request envelope for one bounded recurring review/repair flow."""

    target_ref: str
    schedule_kind: str
    review_class_name: str
    review_policy_scope: str
    review_work_kind: str
    repair_class_name: str
    repair_policy_scope: str
    repair_work_kind: str

    def normalized(self) -> "RecurringReviewRepairFlowRequest":
        return RecurringReviewRepairFlowRequest(
            target_ref=_require_text(self.target_ref, field_name="target_ref"),
            schedule_kind=_require_text(
                self.schedule_kind,
                field_name="schedule_kind",
            ),
            review_class_name=_require_text(
                self.review_class_name,
                field_name="review_class_name",
            ),
            review_policy_scope=_require_text(
                self.review_policy_scope,
                field_name="review_policy_scope",
            ),
            review_work_kind=_require_text(
                self.review_work_kind,
                field_name="review_work_kind",
            ),
            repair_class_name=_require_text(
                self.repair_class_name,
                field_name="repair_class_name",
            ),
            repair_policy_scope=_require_text(
                self.repair_policy_scope,
                field_name="repair_policy_scope",
            ),
            repair_work_kind=_require_text(
                self.repair_work_kind,
                field_name="repair_work_kind",
            ),
        )


@dataclass(frozen=True, slots=True)
class RecurringReviewRepairFlowResolution:
    """Resolved review schedule and repair path for one recurring flow."""

    request: RecurringReviewRepairFlowRequest
    schedule: SchedulerWindowAuthorityResolution
    review_workflow: WorkflowClassResolutionDecision
    repair_workflow: WorkflowClassResolutionDecision
    as_of: datetime
    workflow_class_authority: str = "authority.workflow_class_resolution"
    schedule_authority: str = "runtime.scheduler_window_repository"

    @property
    def schedule_definition_id(self) -> str:
        return self.schedule.schedule_definition_id

    @property
    def recurring_run_window_id(self) -> str:
        return self.schedule.recurring_run_window_id

    @property
    def capacity_remaining(self) -> int | None:
        if self.schedule.capacity_limit is None:
            return None
        return self.schedule.capacity_limit - self.schedule.capacity_used

    def to_json(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "authorities": {
                "workflow_class": self.workflow_class_authority,
                "schedule": self.schedule_authority,
            },
            "request": {
                "target_ref": self.request.target_ref,
                "schedule_kind": self.request.schedule_kind,
                "review_class_name": self.request.review_class_name,
                "review_policy_scope": self.request.review_policy_scope,
                "review_work_kind": self.request.review_work_kind,
                "repair_class_name": self.request.repair_class_name,
                "repair_policy_scope": self.request.repair_policy_scope,
                "repair_work_kind": self.request.repair_work_kind,
            },
            "schedule": {
                "schedule_definition_id": self.schedule.schedule_definition_id,
                "schedule_name": self.schedule.schedule_name,
                "schedule_kind": self.schedule.schedule_kind,
                "target_ref": self.schedule.target_ref,
                "recurring_run_window_id": self.schedule.recurring_run_window_id,
                "window_status": self.schedule.window_status,
                "capacity_limit": self.schedule.capacity_limit,
                "capacity_used": self.schedule.capacity_used,
                "capacity_remaining": self.capacity_remaining,
                "decision_ref": self.schedule.decision_ref,
            },
            "review": _workflow_class_decision_payload(self.review_workflow),
            "repair": _workflow_class_decision_payload(self.repair_workflow),
        }


def _workflow_class_decision_payload(
    decision: WorkflowClassResolutionDecision,
) -> dict[str, Any]:
    return {
        "workflow_class_id": decision.workflow_class_id,
        "class_name": decision.class_name,
        "class_kind": decision.class_kind,
        "workflow_lane_id": decision.workflow_lane_id,
        "workflow_lane_policy_id": decision.workflow_lane_policy_id,
        "policy_scope": decision.policy_scope,
        "work_kind": decision.work_kind,
        "queue_shape": dict(decision.queue_shape),
        "throttle_policy": dict(decision.throttle_policy),
        "review_required": decision.review_required,
        "decision_ref": decision.decision_ref,
    }


def _resolve_flow(
    *,
    request: RecurringReviewRepairFlowRequest,
    workflow_runtime: WorkflowClassResolutionRuntime,
    scheduler_authority: SchedulerWindowAuthorityCatalog,
) -> RecurringReviewRepairFlowResolution:
    schedule = scheduler_authority.resolve(
        target_ref=request.target_ref,
        schedule_kind=request.schedule_kind,
    )
    review_workflow = workflow_runtime.resolve(
        class_name=request.review_class_name,
        policy_scope=request.review_policy_scope,
        work_kind=request.review_work_kind,
    )
    repair_workflow = workflow_runtime.resolve(
        class_name=request.repair_class_name,
        policy_scope=request.repair_policy_scope,
        work_kind=request.repair_work_kind,
    )

    if schedule.schedule_definition.workflow_class_id != review_workflow.workflow_class_id:
        raise _fail(
            "recurring_review_repair_flow.schedule_review_class_mismatch",
            "active recurring schedule did not point at the resolved review class",
            details={
                "schedule_definition_id": schedule.schedule_definition_id,
                "schedule_workflow_class_id": schedule.schedule_definition.workflow_class_id,
                "review_workflow_class_id": review_workflow.workflow_class_id,
                "target_ref": request.target_ref,
                "schedule_kind": request.schedule_kind,
            },
        )
    if review_workflow.workflow_class_id == repair_workflow.workflow_class_id:
        raise _fail(
            "recurring_review_repair_flow.repair_path_collapsed",
            "review and repair flow steps must resolve to distinct workflow classes",
            details={
                "workflow_class_id": review_workflow.workflow_class_id,
                "review_class_name": request.review_class_name,
                "repair_class_name": request.repair_class_name,
            },
        )
    if schedule.capacity_limit is not None and schedule.capacity_used >= schedule.capacity_limit:
        raise _fail(
            "recurring_review_repair_flow.window_capacity_exhausted",
            "active recurring review window has no remaining capacity",
            details={
                "schedule_definition_id": schedule.schedule_definition_id,
                "recurring_run_window_id": schedule.recurring_run_window_id,
                "capacity_limit": schedule.capacity_limit,
                "capacity_used": schedule.capacity_used,
            },
        )

    return RecurringReviewRepairFlowResolution(
        request=request,
        schedule=schedule,
        review_workflow=review_workflow,
        repair_workflow=repair_workflow,
        as_of=workflow_runtime.as_of,
    )


async def resolve_recurring_review_repair_flow(
    conn: asyncpg.Connection,
    *,
    request: RecurringReviewRepairFlowRequest,
    as_of: datetime,
) -> RecurringReviewRepairFlowResolution:
    """Resolve one bounded recurring review/repair flow from stored authority."""

    normalized_request = request.normalized()
    normalized_as_of = _normalize_as_of(as_of)

    async with conn.transaction():
        workflow_runtime = await load_workflow_class_resolution_runtime(
            conn,
            as_of=normalized_as_of,
        )
        scheduler_authority = await load_scheduler_window_authority(
            conn,
            as_of=normalized_as_of,
        )

    if workflow_runtime.as_of != scheduler_authority.as_of:
        raise _fail(
            "recurring_review_repair_flow.snapshot_drifted",
            "workflow-class and scheduler-window authorities must share one as_of snapshot",
            details={
                "workflow_class_as_of": workflow_runtime.as_of.isoformat(),
                "schedule_as_of": scheduler_authority.as_of.isoformat(),
            },
        )

    return _resolve_flow(
        request=normalized_request,
        workflow_runtime=workflow_runtime,
        scheduler_authority=scheduler_authority,
    )


__all__ = [
    "RecurringReviewRepairFlowError",
    "RecurringReviewRepairFlowRequest",
    "RecurringReviewRepairFlowResolution",
    "resolve_recurring_review_repair_flow",
]
