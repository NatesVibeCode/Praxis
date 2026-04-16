"""Canonical operator-visible cross-object relation authority.

This module owns durable semantic links between canonical work items and the
other objects operators actually reason about: functional areas, repo paths,
documents, workflow targets, and decisions. It keeps those relations in
explicit Postgres rows instead of burying them in ad hoc tags, embeddings, or
surface-specific JSON.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache, partial
from typing import Any, Protocol, cast

import asyncpg

from runtime._helpers import _fail as _shared_fail, _json_compatible
from storage.migrations import WorkflowMigrationError, workflow_migration_statements

_SCHEMA_FILENAME = "134_operator_object_relations.sql"
_DUPLICATE_SQLSTATES = {"42P07", "42710"}

SUPPORTED_OPERATOR_OBJECT_KINDS = (
    "issue",
    "bug",
    "roadmap_item",
    "operator_decision",
    "cutover_gate",
    "workflow_class",
    "schedule_definition",
    "workflow_run",
    "document",
    "repo_path",
    "functional_area",
)
SUPPORTED_FUNCTIONAL_AREA_STATUSES = ("active", "inactive")
SUPPORTED_OPERATOR_OBJECT_RELATION_STATUSES = ("active", "inactive")


class OperatorObjectRelationError(RuntimeError):
    """Raised when operator object relation authority cannot be resolved safely."""

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


_fail = partial(_shared_fail, error_type=OperatorObjectRelationError)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "operator_object_relation.invalid_value",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise _fail(
            "operator_object_relation.invalid_value",
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
                "operator_object_relation.invalid_value",
                f"{field_name} must be a datetime",
                details={"field": field_name, "value_type": type(value).__name__},
            ) from exc
    else:
        raise _fail(
            "operator_object_relation.invalid_value",
            f"{field_name} must be a datetime",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _fail(
            "operator_object_relation.invalid_value",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return parsed.astimezone(timezone.utc)


def _slug_fragment(value: object, *, field_name: str) -> str:
    normalized = _require_text(value, field_name=field_name).lower()
    fragments = [char if char.isalnum() else "-" for char in normalized]
    collapsed = "".join(fragments).strip("-")
    while "--" in collapsed:
        collapsed = collapsed.replace("--", "-")
    if not collapsed:
        raise _fail(
            "operator_object_relation.invalid_value",
            f"{field_name} must contain at least one alphanumeric character",
            details={"field": field_name},
        )
    return collapsed


def _normalize_relation_kind(value: object, *, field_name: str) -> str:
    return _slug_fragment(value, field_name=field_name).replace("-", "_")


def _require_kind(value: object, *, field_name: str) -> str:
    normalized = _require_text(value, field_name=field_name)
    if normalized not in SUPPORTED_OPERATOR_OBJECT_KINDS:
        raise _fail(
            "operator_object_relation.invalid_kind",
            f"{field_name} must be one of {', '.join(SUPPORTED_OPERATOR_OBJECT_KINDS)}",
            details={"field": field_name, "value": normalized},
        )
    return normalized


def _require_status(
    value: object,
    *,
    field_name: str,
    allowed_statuses: tuple[str, ...],
) -> str:
    normalized = _require_text(value, field_name=field_name)
    if normalized not in allowed_statuses:
        raise _fail(
            "operator_object_relation.invalid_status",
            f"{field_name} must be one of {', '.join(allowed_statuses)}",
            details={"field": field_name, "value": normalized},
        )
    return normalized


def functional_area_id_from_slug(area_slug: str) -> str:
    """Return the canonical identity for one functional area."""

    normalized_slug = _slug_fragment(area_slug, field_name="area_slug")
    return f"functional_area.{normalized_slug}"


def operator_object_relation_id(
    *,
    relation_kind: str,
    source_kind: str,
    source_ref: str,
    target_kind: str,
    target_ref: str,
) -> str:
    """Return the canonical identity for one explicit cross-object relation."""

    return ":".join(
        (
            "operator_object_relation",
            _slug_fragment(relation_kind, field_name="relation_kind"),
            _require_kind(source_kind, field_name="source_kind"),
            _require_text(source_ref, field_name="source_ref"),
            _require_kind(target_kind, field_name="target_kind"),
            _require_text(target_ref, field_name="target_ref"),
        )
    )


@dataclass(frozen=True, slots=True)
class FunctionalAreaRecord:
    """Canonical functional-area authority row."""

    functional_area_id: str
    area_slug: str
    title: str
    area_status: str
    summary: str
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "functional_area_id": self.functional_area_id,
            "area_slug": self.area_slug,
            "title": self.title,
            "area_status": self.area_status,
            "summary": self.summary,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OperatorObjectRelationRecord:
    """Canonical cross-object semantic relation row."""

    operator_object_relation_id: str
    relation_kind: str
    relation_status: str
    source_kind: str
    source_ref: str
    target_kind: str
    target_ref: str
    relation_metadata: Mapping[str, Any]
    bound_by_decision_id: str | None
    created_at: datetime
    updated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "operator_object_relation_id": self.operator_object_relation_id,
            "relation_kind": self.relation_kind,
            "relation_status": self.relation_status,
            "source": {
                "kind": self.source_kind,
                "ref": self.source_ref,
            },
            "target": {
                "kind": self.target_kind,
                "ref": self.target_ref,
            },
            "relation_metadata": dict(self.relation_metadata),
            "bound_by_decision_id": self.bound_by_decision_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OperatorObjectRelationAuthority:
    """Snapshot of functional areas and cross-object semantic relations."""

    as_of: datetime
    functional_areas: tuple[FunctionalAreaRecord, ...]
    object_relations: tuple[OperatorObjectRelationRecord, ...]


def _functional_area_from_row(row: Mapping[str, Any]) -> FunctionalAreaRecord:
    return FunctionalAreaRecord(
        functional_area_id=_require_text(
            row.get("functional_area_id"),
            field_name="functional_area_id",
        ),
        area_slug=_slug_fragment(row.get("area_slug"), field_name="area_slug"),
        title=_require_text(row.get("title"), field_name="title"),
        area_status=_require_status(
            row.get("area_status"),
            field_name="area_status",
            allowed_statuses=SUPPORTED_FUNCTIONAL_AREA_STATUSES,
        ),
        summary=_require_text(row.get("summary"), field_name="summary"),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


def _operator_object_relation_from_row(
    row: Mapping[str, Any],
) -> OperatorObjectRelationRecord:
    normalized_metadata = _require_mapping(
        _json_compatible(row.get("relation_metadata") or {}),
        field_name="relation_metadata",
    )
    return OperatorObjectRelationRecord(
        operator_object_relation_id=_require_text(
            row.get("operator_object_relation_id"),
            field_name="operator_object_relation_id",
        ),
        relation_kind=_normalize_relation_kind(
            row.get("relation_kind"),
            field_name="relation_kind",
        ),
        relation_status=_require_status(
            row.get("relation_status"),
            field_name="relation_status",
            allowed_statuses=SUPPORTED_OPERATOR_OBJECT_RELATION_STATUSES,
        ),
        source_kind=_require_kind(row.get("source_kind"), field_name="source_kind"),
        source_ref=_require_text(row.get("source_ref"), field_name="source_ref"),
        target_kind=_require_kind(row.get("target_kind"), field_name="target_kind"),
        target_ref=_require_text(row.get("target_ref"), field_name="target_ref"),
        relation_metadata=normalized_metadata,
        bound_by_decision_id=_optional_text(
            row.get("bound_by_decision_id"),
            field_name="bound_by_decision_id",
        ),
        created_at=_require_datetime(row.get("created_at"), field_name="created_at"),
        updated_at=_require_datetime(row.get("updated_at"), field_name="updated_at"),
    )


def project_functional_area(row: Mapping[str, Any]) -> FunctionalAreaRecord:
    """Project one functional area payload back into the canonical record."""

    return _functional_area_from_row(row)


def project_operator_object_relation(row: Mapping[str, Any]) -> OperatorObjectRelationRecord:
    """Project one relation payload back into the canonical record."""

    source = _require_mapping(row.get("source"), field_name="source")
    target = _require_mapping(row.get("target"), field_name="target")
    projected = {
        "operator_object_relation_id": row.get("operator_object_relation_id"),
        "relation_kind": row.get("relation_kind"),
        "relation_status": row.get("relation_status"),
        "source_kind": source.get("kind"),
        "source_ref": source.get("ref"),
        "target_kind": target.get("kind"),
        "target_ref": target.get("ref"),
        "relation_metadata": row.get("relation_metadata"),
        "bound_by_decision_id": row.get("bound_by_decision_id"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }
    return _operator_object_relation_from_row(projected)


class OperatorObjectRelationRepository(Protocol):
    """Minimal repository contract for operator object relations."""

    async def load_functional_area(
        self,
        *,
        functional_area_id: str,
    ) -> FunctionalAreaRecord | None:
        ...

    async def record_functional_area(
        self,
        *,
        functional_area: FunctionalAreaRecord,
    ) -> FunctionalAreaRecord:
        ...

    async def load_relation(
        self,
        *,
        operator_object_relation_id: str,
    ) -> OperatorObjectRelationRecord | None:
        ...

    async def record_relation(
        self,
        *,
        relation: OperatorObjectRelationRecord,
    ) -> OperatorObjectRelationRecord:
        ...


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "operator_object_relation.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "operator_object_relation.schema_missing"
        )
        message = (
            "operator object relation schema file did not contain executable statements"
            if reason_code == "operator_object_relation.schema_empty"
            else "operator object relation schema file could not be resolved from the canonical workflow migration root"
        )
        raise OperatorObjectRelationError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


async def bootstrap_operator_object_relation_schema(conn: asyncpg.Connection) -> None:
    """Apply the operator object relation schema in an idempotent, fail-closed way."""

    async with conn.transaction():
        for statement in _schema_statements():
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise OperatorObjectRelationError(
                    "operator_object_relation.schema_bootstrap_failed",
                    "failed to bootstrap the operator object relation schema",
                    details={
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "statement": statement[:120],
                    },
                ) from exc


class PostgresOperatorObjectRelationRepository:
    """Explicit Postgres-backed repository for functional areas and relations."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def load_functional_area(
        self,
        *,
        functional_area_id: str,
    ) -> FunctionalAreaRecord | None:
        row = await self._conn.fetchrow(
            """
            SELECT
                functional_area_id,
                area_slug,
                title,
                area_status,
                summary,
                created_at,
                updated_at
            FROM functional_areas
            WHERE functional_area_id = $1
            LIMIT 1
            """,
            _require_text(functional_area_id, field_name="functional_area_id"),
        )
        if row is None:
            return None
        return _functional_area_from_row(cast(Mapping[str, Any], row))

    async def list_functional_areas(
        self,
        *,
        as_of: datetime | None = None,
    ) -> tuple[FunctionalAreaRecord, ...]:
        rows = await self._conn.fetch(
            """
            SELECT
                functional_area_id,
                area_slug,
                title,
                area_status,
                summary,
                created_at,
                updated_at
            FROM functional_areas
            WHERE ($1::timestamptz IS NULL OR created_at <= $1)
            ORDER BY area_slug, created_at DESC, functional_area_id
            """,
            None if as_of is None else _require_datetime(as_of, field_name="as_of"),
        )
        return tuple(_functional_area_from_row(cast(Mapping[str, Any], row)) for row in rows)

    async def record_functional_area(
        self,
        *,
        functional_area: FunctionalAreaRecord,
    ) -> FunctionalAreaRecord:
        normalized_area = FunctionalAreaRecord(
            functional_area_id=_require_text(
                functional_area.functional_area_id,
                field_name="functional_area.functional_area_id",
            ),
            area_slug=_slug_fragment(
                functional_area.area_slug,
                field_name="functional_area.area_slug",
            ),
            title=_require_text(functional_area.title, field_name="functional_area.title"),
            area_status=_require_status(
                functional_area.area_status,
                field_name="functional_area.area_status",
                allowed_statuses=SUPPORTED_FUNCTIONAL_AREA_STATUSES,
            ),
            summary=_require_text(
                functional_area.summary,
                field_name="functional_area.summary",
            ),
            created_at=_require_datetime(
                functional_area.created_at,
                field_name="functional_area.created_at",
            ),
            updated_at=_require_datetime(
                functional_area.updated_at,
                field_name="functional_area.updated_at",
            ),
        )
        row = await self._conn.fetchrow(
            """
            INSERT INTO functional_areas (
                functional_area_id,
                area_slug,
                title,
                area_status,
                summary,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7
            )
            ON CONFLICT (functional_area_id) DO UPDATE SET
                area_slug = EXCLUDED.area_slug,
                title = EXCLUDED.title,
                area_status = EXCLUDED.area_status,
                summary = EXCLUDED.summary,
                updated_at = EXCLUDED.updated_at
            RETURNING
                functional_area_id,
                area_slug,
                title,
                area_status,
                summary,
                created_at,
                updated_at
            """,
            normalized_area.functional_area_id,
            normalized_area.area_slug,
            normalized_area.title,
            normalized_area.area_status,
            normalized_area.summary,
            normalized_area.created_at,
            normalized_area.updated_at,
        )
        assert row is not None
        return _functional_area_from_row(cast(Mapping[str, Any], row))

    async def load_relation(
        self,
        *,
        operator_object_relation_id: str,
    ) -> OperatorObjectRelationRecord | None:
        row = await self._conn.fetchrow(
            """
            SELECT
                operator_object_relation_id,
                relation_kind,
                relation_status,
                source_kind,
                source_ref,
                target_kind,
                target_ref,
                relation_metadata,
                bound_by_decision_id,
                created_at,
                updated_at
            FROM operator_object_relations
            WHERE operator_object_relation_id = $1
            LIMIT 1
            """,
            _require_text(
                operator_object_relation_id,
                field_name="operator_object_relation_id",
            ),
        )
        if row is None:
            return None
        return _operator_object_relation_from_row(cast(Mapping[str, Any], row))

    async def list_relations(
        self,
        *,
        as_of: datetime | None = None,
        source_kind: str | None = None,
        source_ref: str | None = None,
        target_kind: str | None = None,
        target_ref: str | None = None,
    ) -> tuple[OperatorObjectRelationRecord, ...]:
        rows = await self._conn.fetch(
            """
            SELECT
                operator_object_relation_id,
                relation_kind,
                relation_status,
                source_kind,
                source_ref,
                target_kind,
                target_ref,
                relation_metadata,
                bound_by_decision_id,
                created_at,
                updated_at
            FROM operator_object_relations
            WHERE ($1::timestamptz IS NULL OR created_at <= $1)
              AND ($2::text IS NULL OR source_kind = $2)
              AND ($3::text IS NULL OR source_ref = $3)
              AND ($4::text IS NULL OR target_kind = $4)
              AND ($5::text IS NULL OR target_ref = $5)
            ORDER BY created_at DESC, operator_object_relation_id
            """,
            None if as_of is None else _require_datetime(as_of, field_name="as_of"),
            None if source_kind is None else _require_kind(source_kind, field_name="source_kind"),
            None if source_ref is None else _require_text(source_ref, field_name="source_ref"),
            None if target_kind is None else _require_kind(target_kind, field_name="target_kind"),
            None if target_ref is None else _require_text(target_ref, field_name="target_ref"),
        )
        return tuple(
            _operator_object_relation_from_row(cast(Mapping[str, Any], row)) for row in rows
        )

    async def record_relation(
        self,
        *,
        relation: OperatorObjectRelationRecord,
    ) -> OperatorObjectRelationRecord:
        normalized_relation = OperatorObjectRelationRecord(
            operator_object_relation_id=_require_text(
                relation.operator_object_relation_id,
                field_name="relation.operator_object_relation_id",
            ),
            relation_kind=_normalize_relation_kind(
                relation.relation_kind,
                field_name="relation.relation_kind",
            ),
            relation_status=_require_status(
                relation.relation_status,
                field_name="relation.relation_status",
                allowed_statuses=SUPPORTED_OPERATOR_OBJECT_RELATION_STATUSES,
            ),
            source_kind=_require_kind(relation.source_kind, field_name="relation.source_kind"),
            source_ref=_require_text(relation.source_ref, field_name="relation.source_ref"),
            target_kind=_require_kind(relation.target_kind, field_name="relation.target_kind"),
            target_ref=_require_text(relation.target_ref, field_name="relation.target_ref"),
            relation_metadata=_require_mapping(
                _json_compatible(relation.relation_metadata),
                field_name="relation.relation_metadata",
            ),
            bound_by_decision_id=_optional_text(
                relation.bound_by_decision_id,
                field_name="relation.bound_by_decision_id",
            ),
            created_at=_require_datetime(
                relation.created_at,
                field_name="relation.created_at",
            ),
            updated_at=_require_datetime(
                relation.updated_at,
                field_name="relation.updated_at",
            ),
        )
        row = await self._conn.fetchrow(
            """
            INSERT INTO operator_object_relations (
                operator_object_relation_id,
                relation_kind,
                relation_status,
                source_kind,
                source_ref,
                target_kind,
                target_ref,
                relation_metadata,
                bound_by_decision_id,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11
            )
            ON CONFLICT (operator_object_relation_id) DO UPDATE SET
                relation_kind = EXCLUDED.relation_kind,
                relation_status = EXCLUDED.relation_status,
                source_kind = EXCLUDED.source_kind,
                source_ref = EXCLUDED.source_ref,
                target_kind = EXCLUDED.target_kind,
                target_ref = EXCLUDED.target_ref,
                relation_metadata = EXCLUDED.relation_metadata,
                bound_by_decision_id = EXCLUDED.bound_by_decision_id,
                updated_at = EXCLUDED.updated_at
            RETURNING
                operator_object_relation_id,
                relation_kind,
                relation_status,
                source_kind,
                source_ref,
                target_kind,
                target_ref,
                relation_metadata,
                bound_by_decision_id,
                created_at,
                updated_at
            """,
            normalized_relation.operator_object_relation_id,
            normalized_relation.relation_kind,
            normalized_relation.relation_status,
            normalized_relation.source_kind,
            normalized_relation.source_ref,
            normalized_relation.target_kind,
            normalized_relation.target_ref,
            json.dumps(dict(normalized_relation.relation_metadata)),
            normalized_relation.bound_by_decision_id,
            normalized_relation.created_at,
            normalized_relation.updated_at,
        )
        assert row is not None
        return _operator_object_relation_from_row(cast(Mapping[str, Any], row))


async def load_operator_object_relation_authority(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
) -> OperatorObjectRelationAuthority:
    """Load one authoritative snapshot of functional areas and relations."""

    normalized_as_of = _require_datetime(as_of, field_name="as_of")
    repository = PostgresOperatorObjectRelationRepository(conn)
    functional_areas = await repository.list_functional_areas(as_of=normalized_as_of)
    object_relations = await repository.list_relations(as_of=normalized_as_of)
    return OperatorObjectRelationAuthority(
        as_of=normalized_as_of,
        functional_areas=functional_areas,
        object_relations=object_relations,
    )


__all__ = [
    "FunctionalAreaRecord",
    "OperatorObjectRelationAuthority",
    "OperatorObjectRelationError",
    "OperatorObjectRelationRecord",
    "OperatorObjectRelationRepository",
    "PostgresOperatorObjectRelationRepository",
    "SUPPORTED_FUNCTIONAL_AREA_STATUSES",
    "SUPPORTED_OPERATOR_OBJECT_KINDS",
    "SUPPORTED_OPERATOR_OBJECT_RELATION_STATUSES",
    "bootstrap_operator_object_relation_schema",
    "functional_area_id_from_slug",
    "load_operator_object_relation_authority",
    "operator_object_relation_id",
    "project_functional_area",
    "project_operator_object_relation",
]
