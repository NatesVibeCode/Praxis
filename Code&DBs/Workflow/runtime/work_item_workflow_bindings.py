"""Canonical work-item workflow binding authority over explicit Postgres rows.

This module records durable bindings from bug and roadmap work onto workflow
classes, schedules, or runs. It does not infer lineage from queue naming,
wrapper history, or operator memory.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache, partial
from typing import Any, Protocol, cast

import asyncpg

from runtime._helpers import _fail as _shared_fail, _json_compatible
from storage.migrations import WorkflowMigrationError, workflow_migration_statements

_SCHEMA_FILENAME = "010_operator_control_authority.sql"
_DUPLICATE_SQLSTATES = {"42P07", "42710"}


class WorkItemWorkflowBindingError(RuntimeError):
    """Raised when a work-item workflow binding cannot be resolved safely."""

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


_fail = partial(_shared_fail, error_type=WorkItemWorkflowBindingError)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "work_item_workflow_binding.invalid_value",
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
        raise _fail(
            "work_item_workflow_binding.invalid_value",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise _fail(
                "work_item_workflow_binding.invalid_value",
                f"{field_name} must be a datetime",
                details={"field": field_name, "value_type": type(value).__name__},
            ) from exc
    else:
        raise _fail(
            "work_item_workflow_binding.invalid_value",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _fail(
            "work_item_workflow_binding.invalid_value",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return parsed.astimezone(timezone.utc)


def _source_fields(
    *,
    issue_id: str | None,
    bug_id: str | None,
    roadmap_item_id: str | None,
    cutover_gate_id: str | None,
) -> tuple[tuple[str, str], ...]:
    fields = (
        ("issue_id", issue_id),
        ("bug_id", bug_id),
        ("roadmap_item_id", roadmap_item_id),
        ("cutover_gate_id", cutover_gate_id),
    )
    selected = tuple((field_name, value) for field_name, value in fields if value is not None)
    if len(selected) != 1:
        raise _fail(
            "work_item_workflow_binding.invalid_source",
            "exactly one of issue_id, bug_id, roadmap_item_id, or cutover_gate_id must be provided",
            details={
                "provided_fields": ",".join(field_name for field_name, value in fields if value is not None),
            },
        )
    return selected


def _target_fields(
    *,
    workflow_class_id: str | None,
    schedule_definition_id: str | None,
    workflow_run_id: str | None,
) -> tuple[tuple[str, str], ...]:
    fields = (
        ("workflow_class_id", workflow_class_id),
        ("schedule_definition_id", schedule_definition_id),
        ("workflow_run_id", workflow_run_id),
    )
    selected = tuple((field_name, value) for field_name, value in fields if value is not None)
    if not selected:
        raise _fail(
            "work_item_workflow_binding.invalid_target",
            "at least one workflow target must be provided",
            details={"provided_fields": ""},
        )
    return selected


def work_item_workflow_binding_id(
    *,
    binding_kind: str,
    issue_id: str | None = None,
    bug_id: str | None = None,
    roadmap_item_id: str | None = None,
    cutover_gate_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    workflow_run_id: str | None = None,
) -> str:
    """Return the canonical identity for one work-item workflow binding."""

    normalized_binding_kind = _require_text(binding_kind, field_name="binding_kind")
    source_fields = _source_fields(
        issue_id=issue_id,
        bug_id=bug_id,
        roadmap_item_id=roadmap_item_id,
        cutover_gate_id=cutover_gate_id,
    )
    target_fields = _target_fields(
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        workflow_run_id=workflow_run_id,
    )

    parts = ["work_item_workflow_binding", normalized_binding_kind]
    for field_name, value in source_fields:
        parts.extend((field_name, value))
    for field_name, value in target_fields:
        parts.extend((field_name, value))
    return ":".join(parts)


@dataclass(frozen=True, slots=True)
class WorkItemWorkflowBindingRecord:
    """Canonical binding row from a work item onto native workflow targets."""

    work_item_workflow_binding_id: str
    binding_kind: str
    binding_status: str
    issue_id: str | None
    roadmap_item_id: str | None
    bug_id: str | None
    cutover_gate_id: str | None
    workflow_class_id: str | None
    schedule_definition_id: str | None
    workflow_run_id: str | None
    bound_by_decision_id: str | None
    created_at: datetime
    updated_at: datetime

    @property
    def source_kind(self) -> str:
        if self.issue_id is not None:
            return "issue"
        if self.bug_id is not None:
            return "bug"
        if self.roadmap_item_id is not None:
            return "roadmap_item"
        if self.cutover_gate_id is not None:
            return "cutover_gate"
        raise WorkItemWorkflowBindingError(
            "work_item_workflow_binding.invalid_record",
            "binding row must have exactly one source",
            details={"binding_id": self.work_item_workflow_binding_id},
        )

    @property
    def source_id(self) -> str:
        if self.issue_id is not None:
            return self.issue_id
        if self.bug_id is not None:
            return self.bug_id
        if self.roadmap_item_id is not None:
            return self.roadmap_item_id
        if self.cutover_gate_id is not None:
            return self.cutover_gate_id
        raise WorkItemWorkflowBindingError(
            "work_item_workflow_binding.invalid_record",
            "binding row must have exactly one source",
            details={"binding_id": self.work_item_workflow_binding_id},
        )

    @property
    def target_refs(self) -> dict[str, str]:
        refs: dict[str, str] = {}
        if self.workflow_class_id is not None:
            refs["workflow_class_id"] = self.workflow_class_id
        if self.schedule_definition_id is not None:
            refs["schedule_definition_id"] = self.schedule_definition_id
        if self.workflow_run_id is not None:
            refs["workflow_run_id"] = self.workflow_run_id
        return refs

    @property
    def authority_tuple(self) -> tuple[object, ...]:
        """Return the canonical binding shape for exact authority round-trips."""

        return (
            self.work_item_workflow_binding_id,
            self.binding_kind,
            self.binding_status,
            self.issue_id,
            self.roadmap_item_id,
            self.bug_id,
            self.cutover_gate_id,
            self.workflow_class_id,
            self.schedule_definition_id,
            self.workflow_run_id,
            self.bound_by_decision_id,
            self.created_at,
            self.updated_at,
        )

    def to_json(self) -> dict[str, Any]:
        source: dict[str, Any] = {"kind": self.source_kind, "id": self.source_id}
        if self.issue_id is not None:
            source["issue_id"] = self.issue_id
        if self.bug_id is not None:
            source["bug_id"] = self.bug_id
        if self.roadmap_item_id is not None:
            source["roadmap_item_id"] = self.roadmap_item_id
        if self.cutover_gate_id is not None:
            source["cutover_gate_id"] = self.cutover_gate_id
        return {
            "work_item_workflow_binding_id": self.work_item_workflow_binding_id,
            "binding_kind": self.binding_kind,
            "binding_status": self.binding_status,
            "source": source,
            "targets": self.target_refs,
            "bound_by_decision_id": self.bound_by_decision_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def project_work_item_workflow_binding(row: Mapping[str, Any]) -> WorkItemWorkflowBindingRecord:
    """Project one canonical binding JSON payload back into the binding record."""

    source = _require_mapping(row.get("source"), field_name="source")
    targets = _require_mapping(row.get("targets"), field_name="targets")
    source_kind = _require_text(source.get("kind"), field_name="source.kind")
    source_id = _require_text(source.get("id"), field_name="source.id")
    issue_id = None
    bug_id = None
    roadmap_item_id = None
    cutover_gate_id = None
    if source_kind == "issue":
        issue_id = source_id
    elif source_kind == "bug":
        bug_id = source_id
    elif source_kind == "roadmap_item":
        roadmap_item_id = source_id
    elif source_kind == "cutover_gate":
        cutover_gate_id = source_id
    else:
        raise WorkItemWorkflowBindingError(
            "work_item_workflow_binding.invalid_projection",
            "work binding source.kind must be issue, bug, roadmap_item, or cutover_gate",
            details={"source_kind": source_kind},
        )

    workflow_class_id = targets.get("workflow_class_id")
    schedule_definition_id = targets.get("schedule_definition_id")
    workflow_run_id = targets.get("workflow_run_id")
    if workflow_class_id is None and schedule_definition_id is None and workflow_run_id is None:
        raise WorkItemWorkflowBindingError(
            "work_item_workflow_binding.invalid_projection",
            "work binding must target at least one workflow target",
            details={"binding_id": row.get("work_item_workflow_binding_id")},
        )

    return WorkItemWorkflowBindingRecord(
        work_item_workflow_binding_id=_require_text(
            row.get("work_item_workflow_binding_id"),
            field_name="work_item_workflow_binding_id",
        ),
        binding_kind=_require_text(row.get("binding_kind"), field_name="binding_kind"),
        binding_status=_require_text(row.get("binding_status"), field_name="binding_status"),
        issue_id=issue_id,
        roadmap_item_id=roadmap_item_id,
        bug_id=bug_id,
        cutover_gate_id=cutover_gate_id,
        workflow_class_id=(
            _require_text(workflow_class_id, field_name="targets.workflow_class_id")
            if workflow_class_id is not None
            else None
        ),
        schedule_definition_id=(
            _require_text(
                schedule_definition_id,
                field_name="targets.schedule_definition_id",
            )
            if schedule_definition_id is not None
            else None
        ),
        workflow_run_id=(
            _require_text(workflow_run_id, field_name="targets.workflow_run_id")
            if workflow_run_id is not None
            else None
        ),
        bound_by_decision_id=(
            _require_text(
                row.get("bound_by_decision_id"),
                field_name="bound_by_decision_id",
            )
            if row.get("bound_by_decision_id") is not None
            else None
        ),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


class WorkItemWorkflowBindingRepository(Protocol):
    """Minimal repository contract for work-item workflow bindings."""

    async def load_binding(
        self,
        *,
        work_item_workflow_binding_id: str,
    ) -> WorkItemWorkflowBindingRecord | None:
        ...

    async def list_bindings_for_workflow_run(
        self,
        *,
        workflow_run_id: str,
    ) -> tuple[WorkItemWorkflowBindingRecord, ...]:
        ...

    async def record_binding(
        self,
        *,
        binding: WorkItemWorkflowBindingRecord,
    ) -> WorkItemWorkflowBindingRecord:
        ...


def _binding_from_row(row: Mapping[str, Any]) -> WorkItemWorkflowBindingRecord:
    return WorkItemWorkflowBindingRecord(
        work_item_workflow_binding_id=_require_text(
            row.get("work_item_workflow_binding_id"),
            field_name="work_item_workflow_binding_id",
        ),
        binding_kind=_require_text(row.get("binding_kind"), field_name="binding_kind"),
        binding_status=_require_text(row.get("binding_status"), field_name="binding_status"),
        issue_id=_optional_text(row.get("issue_id"), field_name="issue_id"),
        roadmap_item_id=_optional_text(row.get("roadmap_item_id"), field_name="roadmap_item_id"),
        bug_id=_optional_text(row.get("bug_id"), field_name="bug_id"),
        cutover_gate_id=_optional_text(row.get("cutover_gate_id"), field_name="cutover_gate_id"),
        workflow_class_id=_optional_text(row.get("workflow_class_id"), field_name="workflow_class_id"),
        schedule_definition_id=_optional_text(
            row.get("schedule_definition_id"),
            field_name="schedule_definition_id",
        ),
        workflow_run_id=_optional_text(row.get("workflow_run_id"), field_name="workflow_run_id"),
        bound_by_decision_id=_optional_text(
            row.get("bound_by_decision_id"),
            field_name="bound_by_decision_id",
        ),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "work_item_workflow_binding.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "work_item_workflow_binding.schema_missing"
        )
        message = (
            "operator control schema file did not contain executable statements"
            if reason_code == "work_item_workflow_binding.schema_empty"
            else "operator control schema file could not be resolved from the canonical workflow migration root"
        )
        raise WorkItemWorkflowBindingError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


async def bootstrap_work_item_workflow_binding_schema(conn: asyncpg.Connection) -> None:
    """Apply the operator-control schema in an idempotent, fail-closed way."""

    async with conn.transaction():
        for statement in _schema_statements():
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise WorkItemWorkflowBindingError(
                    "work_item_workflow_binding.schema_bootstrap_failed",
                    "failed to bootstrap the operator control schema",
                    details={
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "statement": statement[:120],
                    },
                ) from exc


class PostgresWorkItemWorkflowBindingRepository:
    """Explicit Postgres-backed repository for binding authority rows."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def load_binding(
        self,
        *,
        work_item_workflow_binding_id: str,
    ) -> WorkItemWorkflowBindingRecord | None:
        rows = await self._conn.fetch(
            """
            SELECT
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                issue_id,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            FROM work_item_workflow_bindings
            WHERE work_item_workflow_binding_id = $1
            LIMIT 1
            """,
            _require_text(
                work_item_workflow_binding_id,
                field_name="work_item_workflow_binding_id",
            ),
        )
        if not rows:
            return None
        return _binding_from_row(cast(Mapping[str, Any], rows[0]))

    async def list_bindings_for_workflow_run(
        self,
        *,
        workflow_run_id: str,
    ) -> tuple[WorkItemWorkflowBindingRecord, ...]:
        rows = await self._conn.fetch(
            """
            SELECT
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                issue_id,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            FROM work_item_workflow_bindings
            WHERE workflow_run_id = $1
            ORDER BY created_at DESC, work_item_workflow_binding_id
            """,
            _require_text(
                workflow_run_id,
                field_name="workflow_run_id",
            ),
        )
        return tuple(_binding_from_row(cast(Mapping[str, Any], row)) for row in rows)

    async def record_binding(
        self,
        *,
        binding: WorkItemWorkflowBindingRecord,
    ) -> WorkItemWorkflowBindingRecord:
        normalized_binding = WorkItemWorkflowBindingRecord(
            work_item_workflow_binding_id=_require_text(
                binding.work_item_workflow_binding_id,
                field_name="binding.work_item_workflow_binding_id",
            ),
            binding_kind=_require_text(
                binding.binding_kind,
                field_name="binding.binding_kind",
            ),
            binding_status=_require_text(
                binding.binding_status,
                field_name="binding.binding_status",
            ),
            issue_id=_optional_text(binding.issue_id, field_name="binding.issue_id"),
            roadmap_item_id=_optional_text(
                binding.roadmap_item_id,
                field_name="binding.roadmap_item_id",
            ),
            bug_id=_optional_text(binding.bug_id, field_name="binding.bug_id"),
            cutover_gate_id=_optional_text(
                binding.cutover_gate_id,
                field_name="binding.cutover_gate_id",
            ),
            workflow_class_id=_optional_text(
                binding.workflow_class_id,
                field_name="binding.workflow_class_id",
            ),
            schedule_definition_id=_optional_text(
                binding.schedule_definition_id,
                field_name="binding.schedule_definition_id",
            ),
            workflow_run_id=_optional_text(
                binding.workflow_run_id,
                field_name="binding.workflow_run_id",
            ),
            bound_by_decision_id=_optional_text(
                binding.bound_by_decision_id,
                field_name="binding.bound_by_decision_id",
            ),
            created_at=_require_datetime(binding.created_at, field_name="binding.created_at"),
            updated_at=_require_datetime(binding.updated_at, field_name="binding.updated_at"),
        )
        row = await self._conn.fetchrow(
            """
            INSERT INTO work_item_workflow_bindings (
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                issue_id,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            ON CONFLICT (work_item_workflow_binding_id) DO UPDATE SET
                binding_kind = EXCLUDED.binding_kind,
                binding_status = EXCLUDED.binding_status,
                issue_id = EXCLUDED.issue_id,
                roadmap_item_id = EXCLUDED.roadmap_item_id,
                bug_id = EXCLUDED.bug_id,
                cutover_gate_id = EXCLUDED.cutover_gate_id,
                workflow_class_id = EXCLUDED.workflow_class_id,
                schedule_definition_id = EXCLUDED.schedule_definition_id,
                workflow_run_id = EXCLUDED.workflow_run_id,
                bound_by_decision_id = EXCLUDED.bound_by_decision_id,
                updated_at = EXCLUDED.updated_at
            RETURNING
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                issue_id,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            """,
            normalized_binding.work_item_workflow_binding_id,
            normalized_binding.binding_kind,
            normalized_binding.binding_status,
            normalized_binding.issue_id,
            normalized_binding.roadmap_item_id,
            normalized_binding.bug_id,
            normalized_binding.cutover_gate_id,
            normalized_binding.workflow_class_id,
            normalized_binding.schedule_definition_id,
            normalized_binding.workflow_run_id,
            normalized_binding.bound_by_decision_id,
            normalized_binding.created_at,
            normalized_binding.updated_at,
        )
        if row is None:  # pragma: no cover - asyncpg always returns a row here
            raise WorkItemWorkflowBindingError(
                "work_item_workflow_binding.write_failed",
                "binding row could not be persisted",
            )
        return _binding_from_row(cast(Mapping[str, Any], row))


@dataclass(frozen=True, slots=True)
class WorkItemWorkflowBindingRuntime:
    """Deterministic binding seam over stored work-item workflow rows."""

    repository: WorkItemWorkflowBindingRepository

    async def load_binding(
        self,
        *,
        work_item_workflow_binding_id: str,
    ) -> WorkItemWorkflowBindingRecord | None:
        return await self.repository.load_binding(
            work_item_workflow_binding_id=work_item_workflow_binding_id,
        )

    async def list_bindings_for_workflow_run(
        self,
        *,
        workflow_run_id: str,
    ) -> tuple[WorkItemWorkflowBindingRecord, ...]:
        return await self.repository.list_bindings_for_workflow_run(
            workflow_run_id=workflow_run_id,
        )

    async def record_binding(
        self,
        *,
        binding_kind: str,
        issue_id: str | None = None,
        bug_id: str | None = None,
        roadmap_item_id: str | None = None,
        cutover_gate_id: str | None = None,
        workflow_class_id: str | None = None,
        schedule_definition_id: str | None = None,
        workflow_run_id: str | None = None,
        binding_status: str = "active",
        bound_by_decision_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> WorkItemWorkflowBindingRecord:
        binding_id = work_item_workflow_binding_id(
            binding_kind=binding_kind,
            issue_id=issue_id,
            bug_id=bug_id,
            roadmap_item_id=roadmap_item_id,
            cutover_gate_id=cutover_gate_id,
            workflow_class_id=workflow_class_id,
            schedule_definition_id=schedule_definition_id,
            workflow_run_id=workflow_run_id,
        )
        normalized_created_at = _require_datetime(
            datetime.now(timezone.utc) if created_at is None else created_at,
            field_name="created_at",
        )
        normalized_updated_at = _require_datetime(
            normalized_created_at if updated_at is None else updated_at,
            field_name="updated_at",
        )
        if normalized_updated_at < normalized_created_at:
            raise _fail(
                "work_item_workflow_binding.invalid_value",
                "updated_at must be greater than or equal to created_at",
                details={
                    "created_at": normalized_created_at.isoformat(),
                    "updated_at": normalized_updated_at.isoformat(),
                },
            )
        record = WorkItemWorkflowBindingRecord(
            work_item_workflow_binding_id=binding_id,
            binding_kind=_require_text(binding_kind, field_name="binding_kind"),
            binding_status=_require_text(binding_status, field_name="binding_status"),
            issue_id=_optional_text(issue_id, field_name="issue_id"),
            roadmap_item_id=_optional_text(roadmap_item_id, field_name="roadmap_item_id"),
            bug_id=_optional_text(bug_id, field_name="bug_id"),
            cutover_gate_id=_optional_text(cutover_gate_id, field_name="cutover_gate_id"),
            workflow_class_id=_optional_text(
                workflow_class_id,
                field_name="workflow_class_id",
            ),
            schedule_definition_id=_optional_text(
                schedule_definition_id,
                field_name="schedule_definition_id",
            ),
            workflow_run_id=_optional_text(workflow_run_id, field_name="workflow_run_id"),
            bound_by_decision_id=_optional_text(
                bound_by_decision_id,
                field_name="bound_by_decision_id",
            ),
            created_at=normalized_created_at,
            updated_at=normalized_updated_at,
        )
        return await self.repository.record_binding(binding=record)


async def load_work_item_workflow_binding(
    conn: asyncpg.Connection,
    *,
    work_item_workflow_binding_id: str,
) -> WorkItemWorkflowBindingRecord | None:
    """Load one binding row directly from explicit Postgres storage."""

    repository = PostgresWorkItemWorkflowBindingRepository(conn)
    return await repository.load_binding(
        work_item_workflow_binding_id=work_item_workflow_binding_id,
    )


async def load_work_item_workflow_bindings_for_workflow_run(
    conn: asyncpg.Connection,
    *,
    workflow_run_id: str,
) -> tuple[WorkItemWorkflowBindingRecord, ...]:
    """Load canonical binding rows for one workflow run from explicit storage."""

    repository = PostgresWorkItemWorkflowBindingRepository(conn)
    return await repository.list_bindings_for_workflow_run(
        workflow_run_id=workflow_run_id,
    )


async def record_work_item_workflow_binding(
    conn: asyncpg.Connection,
    *,
    binding_kind: str,
    issue_id: str | None = None,
    bug_id: str | None = None,
    roadmap_item_id: str | None = None,
    cutover_gate_id: str | None = None,
    workflow_class_id: str | None = None,
    schedule_definition_id: str | None = None,
    workflow_run_id: str | None = None,
    binding_status: str = "active",
    bound_by_decision_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> WorkItemWorkflowBindingRecord:
    """Record one canonical work-item workflow binding through Postgres."""

    runtime = WorkItemWorkflowBindingRuntime(
        repository=PostgresWorkItemWorkflowBindingRepository(conn),
    )
    return await runtime.record_binding(
        binding_kind=binding_kind,
        issue_id=issue_id,
        bug_id=bug_id,
        roadmap_item_id=roadmap_item_id,
        cutover_gate_id=cutover_gate_id,
        workflow_class_id=workflow_class_id,
        schedule_definition_id=schedule_definition_id,
        workflow_run_id=workflow_run_id,
        binding_status=binding_status,
        bound_by_decision_id=bound_by_decision_id,
        created_at=created_at,
        updated_at=updated_at,
    )


__all__ = [
    "PostgresWorkItemWorkflowBindingRepository",
    "WorkItemWorkflowBindingError",
    "WorkItemWorkflowBindingRecord",
    "WorkItemWorkflowBindingRepository",
    "WorkItemWorkflowBindingRuntime",
    "bootstrap_work_item_workflow_binding_schema",
    "load_work_item_workflow_binding",
    "load_work_item_workflow_bindings_for_workflow_run",
    "project_work_item_workflow_binding",
    "record_work_item_workflow_binding",
    "work_item_workflow_binding_id",
]
