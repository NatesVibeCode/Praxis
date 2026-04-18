"""Canonical native workflow lane catalog authority.

This module reads the native workflow lane taxonomy from Postgres-backed
authority rows. It does not infer lanes from queue text, wrapper scripts, or
worker execution behavior.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any
import json

import asyncpg

from storage.migrations import WorkflowMigrationError, workflow_migration_statements

from ._authority_validation import (
    normalize_as_of as _shared_normalize_as_of,
    require_bool as _shared_require_bool,
    require_int as _shared_require_int,
    require_mapping as _shared_require_mapping,
    require_text as _shared_require_text,
)

_DUPLICATE_SQLSTATES = {"42P07", "42710", "23505"}
_PLATFORM_AUTHORITY_SCHEMA_FILENAME = "006_platform_authority_schema.sql"
_WORKFLOW_LANE_SCHEMA_MARKERS = ("workflow_lanes", "workflow_lane_policies")
_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


class WorkflowLaneCatalogError(RuntimeError):
    """Raised when lane catalog authority cannot be resolved safely."""

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
) -> WorkflowLaneCatalogError:
    return WorkflowLaneCatalogError(reason_code, message, details=details)


@dataclass(frozen=True, slots=True)
class WorkflowLaneAuthorityRecord:
    """Canonical workflow lane row."""

    workflow_lane_id: str
    lane_name: str
    lane_kind: str
    status: str
    concurrency_cap: int
    default_route_kind: str
    review_required: bool
    retry_policy: Mapping[str, Any]
    effective_from: datetime
    effective_to: datetime | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WorkflowLanePolicyAuthorityRecord:
    """Canonical workflow-lane policy row."""

    workflow_lane_policy_id: str
    workflow_lane_id: str
    policy_scope: str
    work_kind: str
    match_rules: Mapping[str, Any]
    lane_parameters: Mapping[str, Any]
    decision_ref: str
    effective_from: datetime
    effective_to: datetime | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class _NativeWorkflowLaneAdmissionSpec:
    workflow_lane_id: str
    lane_name: str
    lane_kind: str
    concurrency_cap: int
    default_route_kind: str
    review_required: bool
    retry_policy: Mapping[str, Any]
    workflow_lane_policy_id: str
    policy_scope: str
    work_kind: str
    match_rules: Mapping[str, Any]
    lane_parameters: Mapping[str, Any]

    def lane_row_payload(self, *, as_of: datetime) -> dict[str, Any]:
        return {
            "workflow_lane_id": self.workflow_lane_id,
            "lane_name": self.lane_name,
            "lane_kind": self.lane_kind,
            "status": "active",
            "concurrency_cap": self.concurrency_cap,
            "default_route_kind": self.default_route_kind,
            "review_required": self.review_required,
            "retry_policy": json.dumps(
                self.retry_policy,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "effective_from": as_of,
            "effective_to": None,
            "created_at": as_of,
        }

    def lane_policy_payload(self, *, as_of: datetime) -> dict[str, Any]:
        return {
            "workflow_lane_policy_id": self.workflow_lane_policy_id,
            "workflow_lane_id": self.workflow_lane_id,
            "policy_scope": self.policy_scope,
            "work_kind": self.work_kind,
            "match_rules": json.dumps(
                self.match_rules,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "lane_parameters": json.dumps(
                self.lane_parameters,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "decision_ref": f"decision:lane-policy:{self.lane_name}",
            "effective_from": as_of,
            "effective_to": None,
            "created_at": as_of,
        }


@dataclass(frozen=True, slots=True)
class WorkflowLaneResolution:
    """Resolved lane and lane-policy pair for one work class."""

    workflow_lane: WorkflowLaneAuthorityRecord
    lane_policy: WorkflowLanePolicyAuthorityRecord
    as_of: datetime

    @property
    def lane_name(self) -> str:
        return self.workflow_lane.lane_name

    @property
    def lane_kind(self) -> str:
        return self.workflow_lane.lane_kind

    @property
    def workflow_lane_id(self) -> str:
        return self.workflow_lane.workflow_lane_id

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
    def route_kind(self) -> str:
        return self.workflow_lane.default_route_kind

    @property
    def review_required(self) -> bool:
        return self.workflow_lane.review_required

    @property
    def concurrency_cap(self) -> int:
        return self.workflow_lane.concurrency_cap

    @property
    def retry_policy(self) -> Mapping[str, Any]:
        return self.workflow_lane.retry_policy

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
class WorkflowLaneCatalog:
    """Inspectible snapshot of active native workflow lane authority."""

    lane_records: tuple[WorkflowLaneAuthorityRecord, ...]
    lane_policy_records: tuple[WorkflowLanePolicyAuthorityRecord, ...]
    as_of: datetime

    @property
    def lane_names(self) -> tuple[str, ...]:
        return tuple(record.lane_name for record in self.lane_records)

    @property
    def policy_keys(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (record.policy_scope, record.work_kind)
            for record in self.lane_policy_records
        )

    def resolve(
        self,
        *,
        policy_scope: str,
        work_kind: str,
    ) -> WorkflowLaneResolution:
        normalized_policy_scope = _require_text(
            policy_scope,
            field_name="policy_scope",
        )
        normalized_work_kind = _require_text(work_kind, field_name="work_kind")
        matching_policies = [
            record
            for record in self.lane_policy_records
            if record.policy_scope == normalized_policy_scope
            and record.work_kind == normalized_work_kind
        ]
        if not matching_policies:
            raise WorkflowLaneCatalogError(
                "workflow_lane.policy_missing",
                (
                    "missing authoritative lane policy for "
                    f"policy_scope={normalized_policy_scope!r} work_kind={normalized_work_kind!r}"
                ),
                details={
                    "policy_scope": normalized_policy_scope,
                    "work_kind": normalized_work_kind,
                },
            )
        if len(matching_policies) > 1:
            raise WorkflowLaneCatalogError(
                "workflow_lane.policy_ambiguous",
                (
                    "ambiguous authoritative lane policy for "
                    f"policy_scope={normalized_policy_scope!r} work_kind={normalized_work_kind!r}"
                ),
                details={
                    "policy_scope": normalized_policy_scope,
                    "work_kind": normalized_work_kind,
                    "matching_policy_ids": ",".join(
                        record.workflow_lane_policy_id for record in matching_policies
                    ),
                },
            )

        lane_policy = matching_policies[0]
        lane_by_id = {record.workflow_lane_id: record for record in self.lane_records}
        workflow_lane = lane_by_id.get(lane_policy.workflow_lane_id)
        if workflow_lane is None:
            raise WorkflowLaneCatalogError(
                "workflow_lane.lane_missing",
                (
                    "lane policy resolved to a missing active workflow lane for "
                    f"workflow_lane_id={lane_policy.workflow_lane_id!r}"
                ),
                details={
                    "workflow_lane_id": lane_policy.workflow_lane_id,
                    "workflow_lane_policy_id": lane_policy.workflow_lane_policy_id,
                    "policy_scope": normalized_policy_scope,
                    "work_kind": normalized_work_kind,
                },
            )

        return WorkflowLaneResolution(
            workflow_lane=workflow_lane,
            lane_policy=lane_policy,
            as_of=self.as_of,
        )

    @classmethod
    def from_records(
        cls,
        *,
        lane_records: Sequence[WorkflowLaneAuthorityRecord],
        lane_policy_records: Sequence[WorkflowLanePolicyAuthorityRecord],
        as_of: datetime,
    ) -> "WorkflowLaneCatalog":
        normalized_as_of = _normalize_as_of(as_of)
        ordered_lanes = tuple(
            sorted(
                lane_records,
                key=lambda record: (
                    record.lane_name,
                    record.effective_from,
                    record.created_at,
                    record.workflow_lane_id,
                ),
            )
        )
        ordered_policies = tuple(
            sorted(
                lane_policy_records,
                key=lambda record: (
                    record.policy_scope,
                    record.work_kind,
                    record.effective_from,
                    record.created_at,
                    record.workflow_lane_policy_id,
                ),
            )
        )
        if not ordered_lanes:
            raise WorkflowLaneCatalogError(
                "workflow_lane.catalog_empty",
                "no active workflow lane rows were available for the requested snapshot",
                details={"as_of": normalized_as_of.isoformat()},
            )
        if not ordered_policies:
            raise WorkflowLaneCatalogError(
                "workflow_lane.policy_catalog_empty",
                "no active workflow lane policy rows were available for the requested snapshot",
                details={"as_of": normalized_as_of.isoformat()},
            )

        _validate_unique_lane_names(ordered_lanes, as_of=normalized_as_of)
        _validate_unique_policy_keys(ordered_policies, as_of=normalized_as_of)
        lane_by_id = {record.workflow_lane_id: record for record in ordered_lanes}
        missing_lane_ids = tuple(
            record.workflow_lane_id
            for record in ordered_policies
            if record.workflow_lane_id not in lane_by_id
        )
        if missing_lane_ids:
            raise WorkflowLaneCatalogError(
                "workflow_lane.lane_missing",
                "one or more lane policies referenced a missing active workflow lane",
                details={
                    "as_of": normalized_as_of.isoformat(),
                    "missing_lane_ids": ",".join(dict.fromkeys(missing_lane_ids)),
                },
            )
        return cls(
            lane_records=ordered_lanes,
            lane_policy_records=ordered_policies,
            as_of=normalized_as_of,
        )


_NATIVE_WORKFLOW_LANE_ADMISSION_SPECS: tuple[_NativeWorkflowLaneAdmissionSpec, ...] = (
    _NativeWorkflowLaneAdmissionSpec(
        workflow_lane_id="workflow_lane.review",
        lane_name="review",
        lane_kind="review",
        concurrency_cap=1,
        default_route_kind="review",
        review_required=True,
        retry_policy={
            "max_attempts": 1,
            "backoff": "none",
        },
        workflow_lane_policy_id="workflow_lane_policy.review",
        policy_scope="workflow.review",
        work_kind="review",
        match_rules={
            "work_kind": "review",
            "review": True,
        },
        lane_parameters={
            "route_kind": "review",
            "manual_review": True,
        },
    ),
    _NativeWorkflowLaneAdmissionSpec(
        workflow_lane_id="workflow_lane.repair",
        lane_name="repair",
        lane_kind="repair",
        concurrency_cap=1,
        default_route_kind="repair",
        review_required=True,
        retry_policy={
            "max_attempts": 2,
            "backoff": "linear",
        },
        workflow_lane_policy_id="workflow_lane_policy.repair",
        policy_scope="workflow.repair",
        work_kind="repair",
        match_rules={
            "work_kind": "repair",
            "repair": True,
        },
        lane_parameters={
            "route_kind": "repair",
            "manual_intervention": True,
        },
    ),
    _NativeWorkflowLaneAdmissionSpec(
        workflow_lane_id="workflow_lane.smoke",
        lane_name="smoke",
        lane_kind="smoke",
        concurrency_cap=2,
        default_route_kind="smoke",
        review_required=False,
        retry_policy={
            "max_attempts": 3,
            "backoff": "fast",
        },
        workflow_lane_policy_id="workflow_lane_policy.smoke",
        policy_scope="workflow.smoke",
        work_kind="smoke",
        match_rules={
            "work_kind": "smoke",
            "smoke": True,
        },
        lane_parameters={
            "route_kind": "smoke",
            "fast_path": True,
        },
    ),
    _NativeWorkflowLaneAdmissionSpec(
        workflow_lane_id="workflow_lane.fanout",
        lane_name="fanout",
        lane_kind="fanout",
        concurrency_cap=8,
        default_route_kind="fanout",
        review_required=False,
        retry_policy={
            "max_attempts": 1,
            "backoff": "none",
        },
        workflow_lane_policy_id="workflow_lane_policy.fanout",
        policy_scope="workflow.fanout",
        work_kind="fanout",
        match_rules={
            "work_kind": "fanout",
            "fanout": True,
        },
        lane_parameters={
            "route_kind": "fanout",
            "batching": "parallel",
            "provider_kind": "api",
        },
    ),
    _NativeWorkflowLaneAdmissionSpec(
        workflow_lane_id="workflow_lane.loop",
        lane_name="loop",
        lane_kind="loop",
        concurrency_cap=8,
        default_route_kind="loop",
        review_required=False,
        retry_policy={
            "max_attempts": 1,
            "backoff": "none",
        },
        workflow_lane_policy_id="workflow_lane_policy.loop",
        policy_scope="workflow.loop",
        work_kind="loop",
        match_rules={
            "work_kind": "loop",
            "loop": True,
        },
        lane_parameters={
            "route_kind": "loop",
            "batching": "parallel",
        },
    ),
    _NativeWorkflowLaneAdmissionSpec(
        workflow_lane_id="workflow_lane.promotion-gated",
        lane_name="promotion-gated",
        lane_kind="promotion-gated",
        concurrency_cap=1,
        default_route_kind="gated",
        review_required=True,
        retry_policy={
            "max_attempts": 1,
            "backoff": "none",
        },
        workflow_lane_policy_id="workflow_lane_policy.promotion-gated",
        policy_scope="workflow.gated",
        work_kind="promotion-gated",
        match_rules={
            "work_kind": "promotion-gated",
            "promotion_gate": True,
        },
        lane_parameters={
            "route_kind": "gated",
            "requires_approval": True,
        },
    ),
)


def _normalize_as_of(value: datetime) -> datetime:
    return _shared_normalize_as_of(
        value,
        error_factory=_error,
        reason_code="workflow_lane.invalid_as_of",
    )


def _require_text(value: object, *, field_name: str) -> str:
    return _shared_require_text(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="workflow_lane.invalid_record",
    )


def _require_int(value: object, *, field_name: str) -> int:
    return _shared_require_int(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="workflow_lane.invalid_record",
    )


def _require_bool(value: object, *, field_name: str) -> bool:
    return _shared_require_bool(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="workflow_lane.invalid_record",
    )


def _require_mapping(value: object, *, field_name: str) -> dict[str, Any]:
    return _shared_require_mapping(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="workflow_lane.invalid_record",
        parse_json_strings=True,
        normalize_keys=True,
        mapping_label="object",
    )


def _lane_record_from_row(row: asyncpg.Record) -> WorkflowLaneAuthorityRecord:
    concurrency_cap = _require_int(row["concurrency_cap"], field_name="concurrency_cap")
    if concurrency_cap < 1:
        raise WorkflowLaneCatalogError(
            "workflow_lane.invalid_record",
            "concurrency_cap must be greater than zero",
            details={
                "field": "concurrency_cap",
                "value": concurrency_cap,
            },
        )
    return WorkflowLaneAuthorityRecord(
        workflow_lane_id=_require_text(row["workflow_lane_id"], field_name="workflow_lane_id"),
        lane_name=_require_text(row["lane_name"], field_name="lane_name"),
        lane_kind=_require_text(row["lane_kind"], field_name="lane_kind"),
        status=_require_text(row["status"], field_name="status"),
        concurrency_cap=concurrency_cap,
        default_route_kind=_require_text(
            row["default_route_kind"],
            field_name="default_route_kind",
        ),
        review_required=_require_bool(
            row["review_required"],
            field_name="review_required",
        ),
        retry_policy=_require_mapping(row["retry_policy"], field_name="retry_policy"),
        effective_from=_normalize_as_of(row["effective_from"]),
        effective_to=(
            _normalize_as_of(row["effective_to"])
            if row["effective_to"] is not None
            else None
        ),
        created_at=_normalize_as_of(row["created_at"]),
    )


def _lane_policy_record_from_row(
    row: asyncpg.Record,
) -> WorkflowLanePolicyAuthorityRecord:
    return WorkflowLanePolicyAuthorityRecord(
        workflow_lane_policy_id=_require_text(
            row["workflow_lane_policy_id"],
            field_name="workflow_lane_policy_id",
        ),
        workflow_lane_id=_require_text(row["workflow_lane_id"], field_name="workflow_lane_id"),
        policy_scope=_require_text(row["policy_scope"], field_name="policy_scope"),
        work_kind=_require_text(row["work_kind"], field_name="work_kind"),
        match_rules=_require_mapping(row["match_rules"], field_name="match_rules"),
        lane_parameters=_require_mapping(
            row["lane_parameters"],
            field_name="lane_parameters",
        ),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        effective_from=_normalize_as_of(row["effective_from"]),
        effective_to=(
            _normalize_as_of(row["effective_to"])
            if row["effective_to"] is not None
            else None
        ),
        created_at=_normalize_as_of(row["created_at"]),
    )


def _validate_unique_lane_names(
    lane_records: Sequence[WorkflowLaneAuthorityRecord],
    *,
    as_of: datetime,
) -> None:
    grouped: dict[str, list[WorkflowLaneAuthorityRecord]] = {}
    for record in lane_records:
        grouped.setdefault(record.lane_name, []).append(record)
    duplicates = {
        lane_name: tuple(record.workflow_lane_id for record in records)
        for lane_name, records in grouped.items()
        if len(records) > 1
    }
    if duplicates:
        lane_name, lane_ids = next(iter(duplicates.items()))
        raise WorkflowLaneCatalogError(
            "workflow_lane.ambiguous_lane",
            f"ambiguous active lane rows for lane_name={lane_name!r}",
            details={
                "as_of": as_of.isoformat(),
                "lane_name": lane_name,
                "workflow_lane_ids": ",".join(lane_ids),
            },
        )


def _validate_unique_policy_keys(
    lane_policy_records: Sequence[WorkflowLanePolicyAuthorityRecord],
    *,
    as_of: datetime,
) -> None:
    grouped: dict[tuple[str, str], list[WorkflowLanePolicyAuthorityRecord]] = {}
    for record in lane_policy_records:
        grouped.setdefault((record.policy_scope, record.work_kind), []).append(record)
    duplicates = {
        policy_key: tuple(
            record.workflow_lane_policy_id for record in records
        )
        for policy_key, records in grouped.items()
        if len(records) > 1
    }
    if duplicates:
        (policy_scope, work_kind), policy_ids = next(iter(duplicates.items()))
        raise WorkflowLaneCatalogError(
            "workflow_lane.ambiguous_policy",
            (
                "ambiguous active lane policy rows for "
                f"policy_scope={policy_scope!r} work_kind={work_kind!r}"
            ),
            details={
                "as_of": as_of.isoformat(),
                "policy_scope": policy_scope,
                "work_kind": work_kind,
                "workflow_lane_policy_ids": ",".join(policy_ids),
            },
        )


@lru_cache(maxsize=1)
def _platform_authority_schema_statements() -> tuple[str, ...]:
    try:
        statements = workflow_migration_statements(_PLATFORM_AUTHORITY_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "workflow_lane.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "workflow_lane.schema_missing"
        )
        message = (
            "workflow lane catalog schema file did not contain executable statements"
            if reason_code == "workflow_lane.schema_empty"
            else "workflow lane catalog schema file could not be read"
        )
        raise WorkflowLaneCatalogError(
            reason_code,
            message,
            details=exc.details,
        ) from exc
    statements = tuple(
        statement
        for statement in statements
        if any(marker in statement for marker in _WORKFLOW_LANE_SCHEMA_MARKERS)
    )
    if not statements:
        raise WorkflowLaneCatalogError(
            "workflow_lane.schema_empty",
            "workflow lane catalog schema file did not contain lane statements",
            details={"filename": _PLATFORM_AUTHORITY_SCHEMA_FILENAME},
        )
    return statements


def _is_duplicate_object_error(error: BaseException) -> bool:
    sqlstate = getattr(error, "sqlstate", None)
    if sqlstate in {"42P07", "42710"}:
        return True
    if sqlstate != "23505":
        return False
    detail = str(getattr(error, "detail", "") or "")
    message = str(error)
    return "pg_type_typname_nsp_index" in detail or "already exists" in message


async def bootstrap_workflow_lane_catalog_schema(conn: asyncpg.Connection) -> None:
    """Apply the platform-authority schema in an idempotent, fail-closed way."""

    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in _platform_authority_schema_statements():
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise WorkflowLaneCatalogError(
                    "workflow_lane.schema_bootstrap_failed",
                    "failed to bootstrap the workflow lane catalog schema",
                    details={
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "statement": statement[:120],
                    },
                ) from exc


async def admit_native_workflow_lane_catalog(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> WorkflowLaneCatalog:
    """Admit the native lane taxonomy as canonical active storage rows."""

    normalized_as_of = _normalize_as_of(as_of)
    async with conn.transaction():
        for spec in _NATIVE_WORKFLOW_LANE_ADMISSION_SPECS:
            lane_payload = spec.lane_row_payload(as_of=normalized_as_of)
            await conn.execute(
                """
                INSERT INTO workflow_lanes (
                    workflow_lane_id,
                    lane_name,
                    lane_kind,
                    status,
                    concurrency_cap,
                    default_route_kind,
                    review_required,
                    retry_policy,
                    effective_from,
                    effective_to,
                    created_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11
                )
                ON CONFLICT (workflow_lane_id) DO UPDATE SET
                    lane_name = EXCLUDED.lane_name,
                    lane_kind = EXCLUDED.lane_kind,
                    status = EXCLUDED.status,
                    concurrency_cap = EXCLUDED.concurrency_cap,
                    default_route_kind = EXCLUDED.default_route_kind,
                    review_required = EXCLUDED.review_required,
                    retry_policy = EXCLUDED.retry_policy,
                    effective_from = EXCLUDED.effective_from,
                    effective_to = EXCLUDED.effective_to,
                    created_at = EXCLUDED.created_at
                """,
                lane_payload["workflow_lane_id"],
                lane_payload["lane_name"],
                lane_payload["lane_kind"],
                lane_payload["status"],
                lane_payload["concurrency_cap"],
                lane_payload["default_route_kind"],
                lane_payload["review_required"],
                lane_payload["retry_policy"],
                lane_payload["effective_from"],
                lane_payload["effective_to"],
                lane_payload["created_at"],
            )

            policy_payload = spec.lane_policy_payload(as_of=normalized_as_of)
            await conn.execute(
                """
                INSERT INTO workflow_lane_policies (
                    workflow_lane_policy_id,
                    workflow_lane_id,
                    policy_scope,
                    work_kind,
                    match_rules,
                    lane_parameters,
                    decision_ref,
                    effective_from,
                    effective_to,
                    created_at
                ) VALUES (
                    $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10
                )
                ON CONFLICT (workflow_lane_policy_id) DO UPDATE SET
                    workflow_lane_id = EXCLUDED.workflow_lane_id,
                    policy_scope = EXCLUDED.policy_scope,
                    work_kind = EXCLUDED.work_kind,
                    match_rules = EXCLUDED.match_rules,
                    lane_parameters = EXCLUDED.lane_parameters,
                    decision_ref = EXCLUDED.decision_ref,
                    effective_from = EXCLUDED.effective_from,
                    effective_to = EXCLUDED.effective_to,
                    created_at = EXCLUDED.created_at
                """,
                policy_payload["workflow_lane_policy_id"],
                policy_payload["workflow_lane_id"],
                policy_payload["policy_scope"],
                policy_payload["work_kind"],
                policy_payload["match_rules"],
                policy_payload["lane_parameters"],
                policy_payload["decision_ref"],
                policy_payload["effective_from"],
                policy_payload["effective_to"],
                policy_payload["created_at"],
            )

    return await load_workflow_lane_catalog(conn, as_of=normalized_as_of)


class PostgresWorkflowLaneCatalogRepository:
    """Explicit Postgres repository for canonical workflow lane authority rows."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def fetch_lane_records(
        self,
        *,
        as_of: datetime,
    ) -> tuple[WorkflowLaneAuthorityRecord, ...]:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    workflow_lane_id,
                    lane_name,
                    lane_kind,
                    status,
                    concurrency_cap,
                    default_route_kind,
                    review_required,
                    retry_policy,
                    effective_from,
                    effective_to,
                    created_at
                FROM workflow_lanes
                WHERE status = 'active'
                  AND effective_from <= $1
                  AND (effective_to IS NULL OR effective_to > $1)
                ORDER BY lane_name, effective_from DESC, created_at DESC, workflow_lane_id
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise WorkflowLaneCatalogError(
                "workflow_lane.read_failed",
                "failed to read active workflow lane rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_lane_record_from_row(row) for row in rows)

    async def fetch_lane_policy_records(
        self,
        *,
        as_of: datetime,
    ) -> tuple[WorkflowLanePolicyAuthorityRecord, ...]:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    workflow_lane_policy_id,
                    workflow_lane_id,
                    policy_scope,
                    work_kind,
                    match_rules,
                    lane_parameters,
                    decision_ref,
                    effective_from,
                    effective_to,
                    created_at
                FROM workflow_lane_policies
                WHERE effective_from <= $1
                  AND (effective_to IS NULL OR effective_to > $1)
                ORDER BY policy_scope, work_kind, effective_from DESC, created_at DESC, workflow_lane_policy_id
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise WorkflowLaneCatalogError(
                "workflow_lane.read_failed",
                "failed to read active workflow lane policy rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_lane_policy_record_from_row(row) for row in rows)

    async def load_catalog(
        self,
        *,
        as_of: datetime,
    ) -> WorkflowLaneCatalog:
        async with self._conn.transaction():
            lane_records = await self.fetch_lane_records(as_of=as_of)
            lane_policy_records = await self.fetch_lane_policy_records(as_of=as_of)
            return WorkflowLaneCatalog.from_records(
                lane_records=lane_records,
                lane_policy_records=lane_policy_records,
                as_of=as_of,
            )


async def load_workflow_lane_catalog(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> WorkflowLaneCatalog:
    """Load the canonical active workflow lane catalog from Postgres."""

    repository = PostgresWorkflowLaneCatalogRepository(conn)
    return await repository.load_catalog(as_of=as_of)


__all__ = [
    "WorkflowLaneAuthorityRecord",
    "WorkflowLaneCatalog",
    "WorkflowLaneCatalogError",
    "WorkflowLanePolicyAuthorityRecord",
    "WorkflowLaneResolution",
    "PostgresWorkflowLaneCatalogRepository",
    "admit_native_workflow_lane_catalog",
    "bootstrap_workflow_lane_catalog_schema",
    "load_workflow_lane_catalog",
]
