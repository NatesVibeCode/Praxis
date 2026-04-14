"""Durable event-subscription authority over canonical Postgres tables.

This module owns the explicit repository seam for subscription definitions and
consumer checkpoints. Subscription identity is stored durably; progress is
resumed from checkpoint rows, not from process memory.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Protocol

import asyncpg

from storage.migrations import WorkflowMigrationError, workflow_migration_statements
from storage.postgres import resolve_workflow_database_url
from storage.postgres.subscription_repository import (
    upsert_event_subscription_record,
    upsert_subscription_checkpoint_record,
)

_SCHEMA_FILENAME = "006_platform_authority_schema.sql"
_DUPLICATE_SQLSTATES = {"42P07", "42710"}


class SubscriptionRepositoryError(RuntimeError):
    """Raised when durable subscription authority cannot be read safely."""

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


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SubscriptionRepositoryError(
            "subscription.invalid_value",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SubscriptionRepositoryError(
            "subscription.invalid_value",
            f"{field_name} must be a mapping",
            details={"field": field_name},
        )
    return value


def _optional_non_negative_int(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SubscriptionRepositoryError(
            "subscription.invalid_value",
            f"{field_name} must be a non-negative integer",
            details={"field": field_name},
        )
    return value


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise SubscriptionRepositoryError(
            "subscription.invalid_value",
            f"{field_name} must be a datetime",
            details={"field": field_name},
        )
    return value


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def subscription_checkpoint_id(*, subscription_id: str, run_id: str) -> str:
    """Return the canonical checkpoint identity for one subscription/run pair."""

    return f"checkpoint:{subscription_id}:{run_id}"


@dataclass(frozen=True, slots=True)
class EventSubscriptionDefinition:
    """Durable definition for one outbox-consuming subscription."""

    subscription_id: str
    subscription_name: str
    consumer_kind: str
    envelope_kind: str
    workflow_id: str | None
    run_id: str | None
    cursor_scope: str
    status: str
    delivery_policy: Mapping[str, Any]
    filter_policy: Mapping[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class EventSubscriptionCheckpoint:
    """Durable consumer checkpoint for one subscription/run pair."""

    checkpoint_id: str
    subscription_id: str
    run_id: str
    last_evidence_seq: int | None
    last_authority_id: str | None
    checkpoint_status: str
    checkpointed_at: datetime
    metadata: Mapping[str, Any]


class EventSubscriptionRepository(Protocol):
    """Minimal repository contract for subscription definitions and checkpoints."""

    async def load_definition(
        self,
        *,
        subscription_id: str,
    ) -> EventSubscriptionDefinition | None:
        ...

    async def save_definition(
        self,
        *,
        definition: EventSubscriptionDefinition,
    ) -> EventSubscriptionDefinition:
        ...

    async def load_checkpoint(
        self,
        *,
        subscription_id: str,
        run_id: str,
    ) -> EventSubscriptionCheckpoint | None:
        ...

    async def save_checkpoint(
        self,
        *,
        checkpoint: EventSubscriptionCheckpoint,
    ) -> EventSubscriptionCheckpoint:
        ...


def _definition_from_row(row: asyncpg.Record) -> EventSubscriptionDefinition:
    delivery_policy = _json_value(row["delivery_policy"])
    filter_policy = _json_value(row["filter_policy"])
    if not isinstance(delivery_policy, Mapping) or not isinstance(filter_policy, Mapping):
        raise SubscriptionRepositoryError(
            "subscription.invalid_row",
            "persisted subscription policies must be mappings",
        )
    return EventSubscriptionDefinition(
        subscription_id=str(row["subscription_id"]),
        subscription_name=str(row["subscription_name"]),
        consumer_kind=str(row["consumer_kind"]),
        envelope_kind=str(row["envelope_kind"]),
        workflow_id=_optional_text(row["workflow_id"], field_name="workflow_id"),
        run_id=_optional_text(row["run_id"], field_name="run_id"),
        cursor_scope=str(row["cursor_scope"]),
        status=str(row["status"]),
        delivery_policy=delivery_policy,
        filter_policy=filter_policy,
        created_at=row["created_at"],
    )


def _checkpoint_from_row(row: asyncpg.Record) -> EventSubscriptionCheckpoint:
    metadata = _json_value(row["metadata"])
    if not isinstance(metadata, Mapping):
        raise SubscriptionRepositoryError(
            "subscription.invalid_row",
            "persisted subscription checkpoint metadata must be a mapping",
        )
    return EventSubscriptionCheckpoint(
        checkpoint_id=str(row["checkpoint_id"]),
        subscription_id=str(row["subscription_id"]),
        run_id=str(row["run_id"]),
        last_evidence_seq=_optional_non_negative_int(
            row["last_evidence_seq"],
            field_name="last_evidence_seq",
        ),
        last_authority_id=_optional_text(
            row["last_authority_id"],
            field_name="last_authority_id",
        ),
        checkpoint_status=str(row["checkpoint_status"]),
        checkpointed_at=row["checkpointed_at"],
        metadata=metadata,
    )


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        reason_code = (
            "subscription.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "subscription.schema_missing"
        )
        message = (
            "subscription authority schema file did not contain executable statements"
            if reason_code == "subscription.schema_empty"
            else "subscription authority schema file could not be resolved from the canonical workflow migration root"
        )
        raise SubscriptionRepositoryError(
            reason_code,
            message,
            details=exc.details,
        ) from exc


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


async def bootstrap_subscription_repository_schema(conn: asyncpg.Connection) -> None:
    """Apply the durable subscription schema in one explicit transaction."""

    async with conn.transaction():
        for statement in _schema_statements():
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if _is_duplicate_object_error(exc):
                    continue
                raise SubscriptionRepositoryError(
                    "subscription.schema_bootstrap_failed",
                    "failed to bootstrap the subscription authority schema",
                    details={
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "statement": statement[:120],
                    },
                ) from exc


async def fetch_event_subscription_definition(
    conn: asyncpg.Connection,
    *,
    subscription_id: str,
) -> EventSubscriptionDefinition | None:
    rows = await conn.fetch(
        """
        SELECT
            subscription_id,
            subscription_name,
            consumer_kind,
            envelope_kind,
            workflow_id,
            run_id,
            cursor_scope,
            status,
            delivery_policy,
            filter_policy,
            created_at
        FROM event_subscriptions
        WHERE subscription_id = $1
        LIMIT 1
        """,
        _require_text(subscription_id, field_name="subscription_id"),
    )
    if not rows:
        return None
    return _definition_from_row(rows[0])


async def persist_event_subscription_definition(
    conn: asyncpg.Connection,
    *,
    definition: EventSubscriptionDefinition,
) -> EventSubscriptionDefinition:
    normalized_definition = EventSubscriptionDefinition(
        subscription_id=_require_text(
            definition.subscription_id,
            field_name="definition.subscription_id",
        ),
        subscription_name=_require_text(
            definition.subscription_name,
            field_name="definition.subscription_name",
        ),
        consumer_kind=_require_text(
            definition.consumer_kind,
            field_name="definition.consumer_kind",
        ),
        envelope_kind=_require_text(
            definition.envelope_kind,
            field_name="definition.envelope_kind",
        ),
        workflow_id=_optional_text(
            definition.workflow_id,
            field_name="definition.workflow_id",
        ),
        run_id=_optional_text(definition.run_id, field_name="definition.run_id"),
        cursor_scope=_require_text(
            definition.cursor_scope,
            field_name="definition.cursor_scope",
        ),
        status=_require_text(definition.status, field_name="definition.status"),
        delivery_policy=_require_mapping(
            definition.delivery_policy,
            field_name="definition.delivery_policy",
        ),
        filter_policy=_require_mapping(
            definition.filter_policy,
            field_name="definition.filter_policy",
        ),
        created_at=_require_datetime(
            definition.created_at,
            field_name="definition.created_at",
        ),
    )
    row = await upsert_event_subscription_record(
        conn,
        subscription_id=normalized_definition.subscription_id,
        subscription_name=normalized_definition.subscription_name,
        consumer_kind=normalized_definition.consumer_kind,
        envelope_kind=normalized_definition.envelope_kind,
        workflow_id=normalized_definition.workflow_id,
        run_id=normalized_definition.run_id,
        cursor_scope=normalized_definition.cursor_scope,
        status=normalized_definition.status,
        delivery_policy=normalized_definition.delivery_policy,
        filter_policy=normalized_definition.filter_policy,
        created_at=normalized_definition.created_at,
    )
    return _definition_from_row(row)


async def fetch_subscription_checkpoint(
    conn: asyncpg.Connection,
    *,
    subscription_id: str,
    run_id: str,
) -> EventSubscriptionCheckpoint | None:
    rows = await conn.fetch(
        """
        SELECT
            checkpoint_id,
            subscription_id,
            run_id,
            last_evidence_seq,
            last_authority_id,
            checkpoint_status,
            checkpointed_at,
            metadata
        FROM subscription_checkpoints
        WHERE subscription_id = $1
          AND run_id = $2
        ORDER BY checkpointed_at DESC
        LIMIT 1
        """,
        _require_text(subscription_id, field_name="subscription_id"),
        _require_text(run_id, field_name="run_id"),
    )
    if not rows:
        return None
    return _checkpoint_from_row(rows[0])


async def persist_subscription_checkpoint(
    conn: asyncpg.Connection,
    *,
    checkpoint: EventSubscriptionCheckpoint,
) -> EventSubscriptionCheckpoint:
    normalized_checkpoint = EventSubscriptionCheckpoint(
        checkpoint_id=subscription_checkpoint_id(
            subscription_id=_require_text(
                checkpoint.subscription_id,
                field_name="checkpoint.subscription_id",
            ),
            run_id=_require_text(checkpoint.run_id, field_name="checkpoint.run_id"),
        ),
        subscription_id=_require_text(
            checkpoint.subscription_id,
            field_name="checkpoint.subscription_id",
        ),
        run_id=_require_text(checkpoint.run_id, field_name="checkpoint.run_id"),
        last_evidence_seq=_optional_non_negative_int(
            checkpoint.last_evidence_seq,
            field_name="checkpoint.last_evidence_seq",
        ),
        last_authority_id=_optional_text(
            checkpoint.last_authority_id,
            field_name="checkpoint.last_authority_id",
        ),
        checkpoint_status=_require_text(
            checkpoint.checkpoint_status,
            field_name="checkpoint.checkpoint_status",
        ),
        checkpointed_at=_require_datetime(
            checkpoint.checkpointed_at,
            field_name="checkpoint.checkpointed_at",
        ),
        metadata=_require_mapping(
            checkpoint.metadata,
            field_name="checkpoint.metadata",
        ),
    )
    row = await upsert_subscription_checkpoint_record(
        conn,
        subscription_id=normalized_checkpoint.subscription_id,
        run_id=normalized_checkpoint.run_id,
        last_evidence_seq=normalized_checkpoint.last_evidence_seq,
        last_authority_id=normalized_checkpoint.last_authority_id,
        checkpoint_status=normalized_checkpoint.checkpoint_status,
        metadata=normalized_checkpoint.metadata,
        checkpointed_at=normalized_checkpoint.checkpointed_at,
    )
    return _checkpoint_from_row(row)


@dataclass(frozen=True, slots=True)
class PostgresEventSubscriptionRepository:
    """Explicit Postgres-backed repository for subscription authority rows."""

    database_url: str | None = None
    env: Mapping[str, str] | None = None

    async def load_definition(
        self,
        *,
        subscription_id: str,
    ) -> EventSubscriptionDefinition | None:
        database_url = (
            resolve_workflow_database_url(
                env={"WORKFLOW_DATABASE_URL": self.database_url},
            )
            if self.database_url is not None
            else resolve_workflow_database_url(env=self.env)
        )
        conn = await asyncpg.connect(database_url)
        try:
            return await fetch_event_subscription_definition(
                conn,
                subscription_id=subscription_id,
            )
        finally:
            await conn.close()

    async def save_definition(
        self,
        *,
        definition: EventSubscriptionDefinition,
    ) -> EventSubscriptionDefinition:
        database_url = (
            resolve_workflow_database_url(
                env={"WORKFLOW_DATABASE_URL": self.database_url},
            )
            if self.database_url is not None
            else resolve_workflow_database_url(env=self.env)
        )
        conn = await asyncpg.connect(database_url)
        try:
            return await persist_event_subscription_definition(
                conn,
                definition=definition,
            )
        finally:
            await conn.close()

    async def load_checkpoint(
        self,
        *,
        subscription_id: str,
        run_id: str,
    ) -> EventSubscriptionCheckpoint | None:
        database_url = (
            resolve_workflow_database_url(
                env={"WORKFLOW_DATABASE_URL": self.database_url},
            )
            if self.database_url is not None
            else resolve_workflow_database_url(env=self.env)
        )
        conn = await asyncpg.connect(database_url)
        try:
            return await fetch_subscription_checkpoint(
                conn,
                subscription_id=subscription_id,
                run_id=run_id,
            )
        finally:
            await conn.close()

    async def save_checkpoint(
        self,
        *,
        checkpoint: EventSubscriptionCheckpoint,
    ) -> EventSubscriptionCheckpoint:
        database_url = (
            resolve_workflow_database_url(
                env={"WORKFLOW_DATABASE_URL": self.database_url},
            )
            if self.database_url is not None
            else resolve_workflow_database_url(env=self.env)
        )
        conn = await asyncpg.connect(database_url)
        try:
            return await persist_subscription_checkpoint(
                conn,
                checkpoint=checkpoint,
            )
        finally:
            await conn.close()


def clear_subscription_repository_schema_cache() -> None:
    """Reset cached schema reads for tests and patched call sites."""

    _schema_statements.cache_clear()


__all__ = [
    "EventSubscriptionCheckpoint",
    "EventSubscriptionDefinition",
    "EventSubscriptionRepository",
    "PostgresEventSubscriptionRepository",
    "SubscriptionRepositoryError",
    "bootstrap_subscription_repository_schema",
    "clear_subscription_repository_schema_cache",
    "fetch_event_subscription_definition",
    "fetch_subscription_checkpoint",
    "persist_event_subscription_definition",
    "persist_subscription_checkpoint",
    "subscription_checkpoint_id",
]
