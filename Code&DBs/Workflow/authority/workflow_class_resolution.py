"""DB-backed workflow-class resolution over class and lane-policy authority.

This module keeps workflow-class resolution explicit and fail closed:

- workflow-class meaning comes from stored ``workflow_classes`` rows
- lane-policy meaning comes from stored ``workflow_lane_policies`` rows
- a resolved class and policy must agree on the workflow lane id
- no wrapper folklore or default-path guessing is consulted
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from policy._authority_validation import (
    normalize_as_of as _shared_normalize_as_of,
    require_text as _shared_require_text,
)
from policy.workflow_classes import (
    WorkflowClassAuthorityRecord,
    WorkflowClassCatalog,
    load_workflow_class_catalog,
)
from policy.workflow_lanes import (
    WorkflowLaneCatalog,
    WorkflowLanePolicyAuthorityRecord,
    load_workflow_lane_catalog,
)


class WorkflowClassResolutionError(RuntimeError):
    """Raised when a class resolution cannot be completed safely."""

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


def _error(
    reason_code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> WorkflowClassResolutionError:
    return WorkflowClassResolutionError(reason_code, message, details=details)


def _normalize_as_of(value: datetime) -> datetime:
    return _shared_normalize_as_of(
        value,
        error_factory=_error,
        reason_code="workflow_class.invalid_as_of",
    )


def _require_text(value: object, *, field_name: str) -> str:
    return _shared_require_text(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="workflow_class.invalid_input",
    )


@dataclass(frozen=True, slots=True)
class WorkflowClassResolutionDecision:
    """Resolved workflow-class and lane-policy pair for one native path."""

    workflow_class: WorkflowClassAuthorityRecord
    lane_policy: WorkflowLanePolicyAuthorityRecord
    as_of: datetime

    @property
    def workflow_class_id(self) -> str:
        return self.workflow_class.workflow_class_id

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
    def queue_shape(self) -> Mapping[str, Any]:
        return self.workflow_class.queue_shape

    @property
    def throttle_policy(self) -> Mapping[str, Any]:
        return self.workflow_class.throttle_policy

    @property
    def review_required(self) -> bool:
        return self.workflow_class.review_required

    @property
    def workflow_class_decision_ref(self) -> str:
        return self.workflow_class.decision_ref

    @property
    def workflow_lane_policy_id(self) -> str:
        return self.lane_policy.workflow_lane_policy_id

    @property
    def policy_scope(self) -> str:
        return self.lane_policy.policy_scope

    @property
    def work_kind(self) -> str:
        return self.lane_policy.work_kind

    @property
    def match_rules(self) -> Mapping[str, Any]:
        return self.lane_policy.match_rules

    @property
    def lane_parameters(self) -> Mapping[str, Any]:
        return self.lane_policy.lane_parameters

    @property
    def decision_ref(self) -> str:
        return self.lane_policy.decision_ref


@dataclass(frozen=True, slots=True)
class WorkflowClassResolutionRuntime:
    """Resolve one workflow-class path from stored class and lane-policy rows."""

    workflow_class_catalog: WorkflowClassCatalog
    lane_catalog: WorkflowLaneCatalog

    def __post_init__(self) -> None:
        if self.workflow_class_catalog.as_of != self.lane_catalog.as_of:
            raise WorkflowClassResolutionError(
                "workflow_class.snapshot_drifted",
                "workflow-class and lane-policy catalogs must share one as_of snapshot",
                details={
                    "workflow_class_as_of": self.workflow_class_catalog.as_of.isoformat(),
                    "lane_policy_as_of": self.lane_catalog.as_of.isoformat(),
                },
            )

    @property
    def as_of(self) -> datetime:
        return self.workflow_class_catalog.as_of

    def resolve(
        self,
        *,
        class_name: str,
        policy_scope: str,
        work_kind: str,
    ) -> WorkflowClassResolutionDecision:
        normalized_class_name = _require_text(class_name, field_name="class_name")
        normalized_policy_scope = _require_text(policy_scope, field_name="policy_scope")
        normalized_work_kind = _require_text(work_kind, field_name="work_kind")

        class_resolution = self.workflow_class_catalog.resolve(
            class_name=normalized_class_name,
        )
        lane_resolution = self.lane_catalog.resolve(
            policy_scope=normalized_policy_scope,
            work_kind=normalized_work_kind,
        )
        if (
            class_resolution.workflow_class.workflow_lane_id
            != lane_resolution.workflow_lane.workflow_lane_id
        ):
            raise WorkflowClassResolutionError(
                "workflow_class.lane_policy_mismatch",
                (
                    "lane policy resolved to a different workflow lane than "
                    "the canonical workflow class"
                ),
                details={
                    "class_name": normalized_class_name,
                    "workflow_class_id": class_resolution.workflow_class.workflow_class_id,
                    "workflow_lane_id": class_resolution.workflow_class.workflow_lane_id,
                    "workflow_lane_policy_id": (
                        lane_resolution.lane_policy.workflow_lane_policy_id
                    ),
                    "lane_policy_workflow_lane_id": (
                        lane_resolution.workflow_lane.workflow_lane_id
                    ),
                    "policy_scope": normalized_policy_scope,
                    "work_kind": normalized_work_kind,
                },
            )

        return WorkflowClassResolutionDecision(
            workflow_class=class_resolution.workflow_class,
            lane_policy=lane_resolution.lane_policy,
            as_of=self.as_of,
        )


async def load_workflow_class_resolution_runtime(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> WorkflowClassResolutionRuntime:
    """Load one deterministic workflow-class runtime snapshot from Postgres."""

    normalized_as_of = _normalize_as_of(as_of)
    async with conn.transaction():
        workflow_class_catalog = await load_workflow_class_catalog(
            conn,
            as_of=normalized_as_of,
        )
        lane_catalog = await load_workflow_lane_catalog(
            conn,
            as_of=normalized_as_of,
        )
    runtime = WorkflowClassResolutionRuntime(
        workflow_class_catalog=workflow_class_catalog,
        lane_catalog=lane_catalog,
    )
    if runtime.as_of != normalized_as_of:
        raise WorkflowClassResolutionError(
            "workflow_class.snapshot_drifted",
            "loaded workflow-class runtime did not preserve the requested as_of",
            details={"as_of": normalized_as_of.isoformat()},
        )
    return runtime


__all__ = [
    "WorkflowClassResolutionDecision",
    "WorkflowClassResolutionError",
    "WorkflowClassResolutionRuntime",
    "load_workflow_class_resolution_runtime",
]
