"""Canonical native workflow class authority.

This module reads workflow-class authority rows from Postgres-backed storage.
It does not infer class semantics from wrapper commands, queue folklore, or
runtime behavior.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from ._authority_validation import (
    normalize_as_of as _shared_normalize_as_of,
    require_bool as _shared_require_bool,
    require_datetime as _shared_require_datetime,
    require_mapping as _shared_require_mapping,
    require_text as _shared_require_text,
)


class WorkflowClassCatalogError(RuntimeError):
    """Raised when workflow-class authority cannot be resolved safely."""

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
) -> WorkflowClassCatalogError:
    return WorkflowClassCatalogError(reason_code, message, details=details)


@dataclass(frozen=True, slots=True)
class WorkflowClassAuthorityRecord:
    """Canonical native workflow-class row."""

    workflow_class_id: str
    class_name: str
    class_kind: str
    workflow_lane_id: str
    status: str
    queue_shape: Mapping[str, Any]
    throttle_policy: Mapping[str, Any]
    review_required: bool
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WorkflowClassResolution:
    """Resolved class row for one workflow-class lookup."""

    workflow_class: WorkflowClassAuthorityRecord
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
    def decision_ref(self) -> str:
        return self.workflow_class.decision_ref


@dataclass(frozen=True, slots=True)
class WorkflowClassCatalog:
    """Inspectable snapshot of active native workflow-class authority."""

    class_records: tuple[WorkflowClassAuthorityRecord, ...]
    as_of: datetime

    @property
    def class_names(self) -> tuple[str, ...]:
        return tuple(record.class_name for record in self.class_records)

    @property
    def class_keys(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (record.class_name, record.class_kind)
            for record in self.class_records
        )

    def resolve(self, *, class_name: str) -> WorkflowClassResolution:
        normalized_class_name = _require_text(class_name, field_name="class_name")
        matching_classes = [
            record
            for record in self.class_records
            if record.class_name == normalized_class_name
        ]
        if not matching_classes:
            raise WorkflowClassCatalogError(
                "workflow_class.class_missing",
                (
                    "missing authoritative workflow class for "
                    f"class_name={normalized_class_name!r}"
                ),
                details={"class_name": normalized_class_name},
            )
        if len(matching_classes) > 1:
            raise WorkflowClassCatalogError(
                "workflow_class.class_ambiguous",
                (
                    "ambiguous authoritative workflow classes for "
                    f"class_name={normalized_class_name!r}"
                ),
                details={
                    "class_name": normalized_class_name,
                    "workflow_class_ids": ",".join(
                        record.workflow_class_id for record in matching_classes
                    ),
                },
            )
        return WorkflowClassResolution(
            workflow_class=matching_classes[0],
            as_of=self.as_of,
        )

    def resolve_by_id(self, *, workflow_class_id: str) -> WorkflowClassResolution:
        normalized_workflow_class_id = _require_text(
            workflow_class_id,
            field_name="workflow_class_id",
        )
        matching_classes = [
            record
            for record in self.class_records
            if record.workflow_class_id == normalized_workflow_class_id
        ]
        if not matching_classes:
            raise WorkflowClassCatalogError(
                "workflow_class.class_missing",
                (
                    "missing authoritative workflow class for "
                    f"workflow_class_id={normalized_workflow_class_id!r}"
                ),
                details={"workflow_class_id": normalized_workflow_class_id},
            )
        if len(matching_classes) > 1:
            raise WorkflowClassCatalogError(
                "workflow_class.class_ambiguous",
                (
                    "ambiguous authoritative workflow classes for "
                    f"workflow_class_id={normalized_workflow_class_id!r}"
                ),
                details={
                    "workflow_class_id": normalized_workflow_class_id,
                    "class_names": ",".join(
                        record.class_name for record in matching_classes
                    ),
                },
            )
        return WorkflowClassResolution(
            workflow_class=matching_classes[0],
            as_of=self.as_of,
        )

    @classmethod
    def from_records(
        cls,
        *,
        class_records: Sequence[WorkflowClassAuthorityRecord],
        as_of: datetime,
    ) -> "WorkflowClassCatalog":
        normalized_as_of = _normalize_as_of(as_of)
        ordered_classes = tuple(
            sorted(
                class_records,
                key=lambda record: (
                    record.class_name,
                    record.effective_from,
                    record.created_at,
                    record.workflow_class_id,
                ),
            )
        )
        if not ordered_classes:
            raise WorkflowClassCatalogError(
                "workflow_class.catalog_empty",
                "no active workflow-class rows were available for the requested snapshot",
                details={"as_of": normalized_as_of.isoformat()},
            )

        _validate_unique_class_names(ordered_classes, as_of=normalized_as_of)
        return cls(
            class_records=ordered_classes,
            as_of=normalized_as_of,
        )


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
        reason_code="workflow_class.invalid_record",
    )


def _require_bool(value: object, *, field_name: str) -> bool:
    return _shared_require_bool(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="workflow_class.invalid_record",
    )


def _require_mapping(value: object, *, field_name: str) -> dict[str, Any]:
    return _shared_require_mapping(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="workflow_class.invalid_record",
        parse_json_strings=True,
        normalize_keys=True,
        mapping_label="object",
    )


def _require_datetime(value: object, *, field_name: str) -> datetime:
    return _shared_require_datetime(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="workflow_class.invalid_record",
        require_timezone=True,
    )


def _workflow_class_record_from_row(
    row: asyncpg.Record,
) -> WorkflowClassAuthorityRecord:
    return WorkflowClassAuthorityRecord(
        workflow_class_id=_require_text(
            row["workflow_class_id"],
            field_name="workflow_class_id",
        ),
        class_name=_require_text(row["class_name"], field_name="class_name"),
        class_kind=_require_text(row["class_kind"], field_name="class_kind"),
        workflow_lane_id=_require_text(
            row["workflow_lane_id"],
            field_name="workflow_lane_id",
        ),
        status=_require_text(row["status"], field_name="status"),
        queue_shape=_require_mapping(row["queue_shape"], field_name="queue_shape"),
        throttle_policy=_require_mapping(
            row["throttle_policy"],
            field_name="throttle_policy",
        ),
        review_required=_require_bool(
            row["review_required"],
            field_name="review_required",
        ),
        effective_from=_require_datetime(
            row["effective_from"],
            field_name="effective_from",
        ),
        effective_to=(
            _require_datetime(row["effective_to"], field_name="effective_to")
            if row["effective_to"] is not None
            else None
        ),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
    )


def _validate_unique_class_names(
    class_records: Sequence[WorkflowClassAuthorityRecord],
    *,
    as_of: datetime,
) -> None:
    grouped: dict[str, list[WorkflowClassAuthorityRecord]] = {}
    for record in class_records:
        grouped.setdefault(record.class_name, []).append(record)
    duplicates = {
        class_name: tuple(
            record.workflow_class_id for record in records
        )
        for class_name, records in grouped.items()
        if len(records) > 1
    }
    if duplicates:
        class_name, class_ids = next(iter(duplicates.items()))
        raise WorkflowClassCatalogError(
            "workflow_class.ambiguous_class",
            f"ambiguous active class rows for class_name={class_name!r}",
            details={
                "as_of": as_of.isoformat(),
                "class_name": class_name,
                "workflow_class_ids": ",".join(class_ids),
            },
        )


class PostgresWorkflowClassRepository:
    """Explicit Postgres repository for canonical workflow-class authority."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def fetch_workflow_class_records(
        self,
        *,
        as_of: datetime,
    ) -> tuple[WorkflowClassAuthorityRecord, ...]:
        normalized_as_of = _normalize_as_of(as_of)
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    workflow_class_id,
                    class_name,
                    class_kind,
                    workflow_lane_id,
                    status,
                    queue_shape,
                    throttle_policy,
                    review_required,
                    effective_from,
                    effective_to,
                    decision_ref,
                    created_at
                FROM workflow_classes
                WHERE status = 'active'
                  AND effective_from <= $1
                  AND (effective_to IS NULL OR effective_to > $1)
                ORDER BY class_name, effective_from DESC, created_at DESC, workflow_class_id
                """,
                normalized_as_of,
            )
        except asyncpg.PostgresError as exc:
            raise WorkflowClassCatalogError(
                "workflow_class.read_failed",
                "failed to read active workflow-class rows",
                details={"sqlstate": getattr(exc, "sqlstate", None)},
            ) from exc
        return tuple(_workflow_class_record_from_row(row) for row in rows)

    async def load_catalog(
        self,
        *,
        as_of: datetime,
    ) -> WorkflowClassCatalog:
        async with self._conn.transaction():
            class_records = await self.fetch_workflow_class_records(as_of=as_of)
            return WorkflowClassCatalog.from_records(
                class_records=class_records,
                as_of=as_of,
            )


async def load_workflow_class_catalog(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> WorkflowClassCatalog:
    """Load the canonical active workflow-class catalog from Postgres."""

    repository = PostgresWorkflowClassRepository(conn)
    return await repository.load_catalog(as_of=as_of)


__all__ = [
    "WorkflowClassAuthorityRecord",
    "WorkflowClassCatalog",
    "WorkflowClassCatalogError",
    "WorkflowClassResolution",
    "PostgresWorkflowClassRepository",
    "load_workflow_class_catalog",
]
