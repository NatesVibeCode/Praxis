"""Explicit Postgres repository for subscription and trigger mutations."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import asyncpg

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_nonnegative_int,
    _require_text,
    _require_utc,
)


def _row_dict(row: object, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "postgres.write_failed",
            f"{operation} returned no row",
        )
    if isinstance(row, Mapping):
        return dict(row)
    try:
        return dict(row)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        pass
    raise PostgresWriteError(
        "postgres.write_failed",
        f"{operation} returned an invalid row type",
        details={"operation": operation, "row_type": type(row).__name__},
    )


def _default_utc(value: datetime | None, *, field_name: str) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    return _require_utc(value, field_name=field_name)


def _subscription_checkpoint_id(*, subscription_id: str, run_id: str) -> str:
    return f"checkpoint:{subscription_id}:{run_id}"


class PostgresSubscriptionRepository:
    """Owns canonical subscription, checkpoint, trigger, and event mutations."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def upsert_event_subscription(
        self,
        *,
        subscription_id: str,
        subscription_name: str,
        consumer_kind: str,
        envelope_kind: str,
        workflow_id: str | None,
        run_id: str | None,
        cursor_scope: str,
        status: str,
        delivery_policy: Mapping[str, Any],
        filter_policy: Mapping[str, Any],
        created_at: datetime | None = None,
    ) -> dict[str, Any]:
        normalized_created_at = _default_utc(created_at, field_name="created_at")
        row = self._conn.fetchrow(
            """
            INSERT INTO public.event_subscriptions (
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
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11
            )
            ON CONFLICT (subscription_id) DO UPDATE SET
                subscription_name = EXCLUDED.subscription_name,
                consumer_kind = EXCLUDED.consumer_kind,
                envelope_kind = EXCLUDED.envelope_kind,
                workflow_id = EXCLUDED.workflow_id,
                run_id = EXCLUDED.run_id,
                cursor_scope = EXCLUDED.cursor_scope,
                status = EXCLUDED.status,
                delivery_policy = EXCLUDED.delivery_policy,
                filter_policy = EXCLUDED.filter_policy
            RETURNING
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
            """,
            _require_text(subscription_id, field_name="subscription_id"),
            _require_text(subscription_name, field_name="subscription_name"),
            _require_text(consumer_kind, field_name="consumer_kind"),
            _require_text(envelope_kind, field_name="envelope_kind"),
            _optional_text(workflow_id, field_name="workflow_id"),
            _optional_text(run_id, field_name="run_id"),
            _require_text(cursor_scope, field_name="cursor_scope"),
            _require_text(status, field_name="status"),
            _encode_jsonb(
                _require_mapping(delivery_policy, field_name="delivery_policy"),
                field_name="delivery_policy",
            ),
            _encode_jsonb(
                _require_mapping(filter_policy, field_name="filter_policy"),
                field_name="filter_policy",
            ),
            normalized_created_at,
        )
        return _row_dict(row, operation="upsert_event_subscription")

    def upsert_subscription_checkpoint(
        self,
        *,
        subscription_id: str,
        run_id: str,
        last_evidence_seq: int | None,
        last_authority_id: str | None,
        checkpoint_status: str,
        metadata: Mapping[str, Any],
        checkpointed_at: datetime | None = None,
    ) -> dict[str, Any]:
        normalized_subscription_id = _require_text(
            subscription_id,
            field_name="subscription_id",
        )
        normalized_run_id = _require_text(run_id, field_name="run_id")
        normalized_last_evidence_seq = (
            None
            if last_evidence_seq is None
            else _require_nonnegative_int(last_evidence_seq, field_name="last_evidence_seq")
        )
        normalized_last_authority_id = _optional_text(
            last_authority_id,
            field_name="last_authority_id",
        )
        if normalized_last_authority_id is None and normalized_last_evidence_seq is not None:
            normalized_last_authority_id = f"system_event:{normalized_last_evidence_seq}"
        row = self._conn.fetchrow(
            """
            INSERT INTO public.subscription_checkpoints (
                checkpoint_id,
                subscription_id,
                run_id,
                last_evidence_seq,
                last_authority_id,
                checkpoint_status,
                checkpointed_at,
                metadata
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb
            )
            ON CONFLICT (subscription_id, run_id) DO UPDATE SET
                checkpoint_id = EXCLUDED.checkpoint_id,
                last_evidence_seq = EXCLUDED.last_evidence_seq,
                last_authority_id = EXCLUDED.last_authority_id,
                checkpoint_status = EXCLUDED.checkpoint_status,
                checkpointed_at = EXCLUDED.checkpointed_at,
                metadata = EXCLUDED.metadata
            RETURNING
                checkpoint_id,
                subscription_id,
                run_id,
                last_evidence_seq,
                last_authority_id,
                checkpoint_status,
                checkpointed_at,
                metadata
            """,
            _subscription_checkpoint_id(
                subscription_id=normalized_subscription_id,
                run_id=normalized_run_id,
            ),
            normalized_subscription_id,
            normalized_run_id,
            normalized_last_evidence_seq,
            normalized_last_authority_id,
            _require_text(checkpoint_status, field_name="checkpoint_status"),
            _default_utc(checkpointed_at, field_name="checkpointed_at"),
            _encode_jsonb(_require_mapping(metadata, field_name="metadata"), field_name="metadata"),
        )
        return _row_dict(row, operation="upsert_subscription_checkpoint")

    def increment_workflow_trigger_fire_count(
        self,
        *,
        trigger_id: str,
    ) -> bool:
        rows = self._conn.execute(
            """
            UPDATE public.workflow_triggers
            SET last_fired_at = now(),
                fire_count = fire_count + 1
            WHERE id = $1
            RETURNING id
            """,
            _require_text(trigger_id, field_name="trigger_id"),
        )
        return bool(rows)

    def insert_system_event(
        self,
        *,
        event_type: str,
        source_id: str,
        source_type: str,
        payload: Mapping[str, Any],
    ) -> int:
        row = self._conn.fetchrow(
            """
            INSERT INTO public.system_events (event_type, source_id, source_type, payload)
            VALUES ($1, $2, $3, $4::jsonb)
            RETURNING id
            """,
            _require_text(event_type, field_name="event_type"),
            _require_text(source_id, field_name="source_id"),
            _require_text(source_type, field_name="source_type"),
            _encode_jsonb(_require_mapping(payload, field_name="payload"), field_name="payload"),
        )
        normalized_row = _row_dict(row, operation="insert_system_event")
        event_id = normalized_row.get("id")
        if isinstance(event_id, bool) or not isinstance(event_id, int):
            raise PostgresWriteError(
                "postgres.write_failed",
                "insert_system_event returned an invalid event id",
                details={"event_id_type": type(event_id).__name__},
            )
        return event_id


async def upsert_event_subscription_record(
    conn: asyncpg.Connection,
    *,
    subscription_id: str,
    subscription_name: str,
    consumer_kind: str,
    envelope_kind: str,
    workflow_id: str | None,
    run_id: str | None,
    cursor_scope: str,
    status: str,
    delivery_policy: Mapping[str, Any],
    filter_policy: Mapping[str, Any],
    created_at: datetime | None = None,
) -> dict[str, Any]:
    normalized_created_at = _default_utc(created_at, field_name="created_at")
    row = await conn.fetchrow(
        """
        INSERT INTO public.event_subscriptions (
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
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11
        )
        ON CONFLICT (subscription_id) DO UPDATE SET
            subscription_name = EXCLUDED.subscription_name,
            consumer_kind = EXCLUDED.consumer_kind,
            envelope_kind = EXCLUDED.envelope_kind,
            workflow_id = EXCLUDED.workflow_id,
            run_id = EXCLUDED.run_id,
            cursor_scope = EXCLUDED.cursor_scope,
            status = EXCLUDED.status,
            delivery_policy = EXCLUDED.delivery_policy,
            filter_policy = EXCLUDED.filter_policy
        RETURNING
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
        """,
        _require_text(subscription_id, field_name="subscription_id"),
        _require_text(subscription_name, field_name="subscription_name"),
        _require_text(consumer_kind, field_name="consumer_kind"),
        _require_text(envelope_kind, field_name="envelope_kind"),
        _optional_text(workflow_id, field_name="workflow_id"),
        _optional_text(run_id, field_name="run_id"),
        _require_text(cursor_scope, field_name="cursor_scope"),
        _require_text(status, field_name="status"),
        _encode_jsonb(
            _require_mapping(delivery_policy, field_name="delivery_policy"),
            field_name="delivery_policy",
        ),
        _encode_jsonb(
            _require_mapping(filter_policy, field_name="filter_policy"),
            field_name="filter_policy",
        ),
        normalized_created_at,
    )
    return _row_dict(row, operation="upsert_event_subscription_record")


async def upsert_subscription_checkpoint_record(
    conn: asyncpg.Connection,
    *,
    subscription_id: str,
    run_id: str,
    last_evidence_seq: int | None,
    last_authority_id: str | None,
    checkpoint_status: str,
    metadata: Mapping[str, Any],
    checkpointed_at: datetime | None = None,
) -> dict[str, Any]:
    normalized_subscription_id = _require_text(
        subscription_id,
        field_name="subscription_id",
    )
    normalized_run_id = _require_text(run_id, field_name="run_id")
    normalized_last_evidence_seq = (
        None
        if last_evidence_seq is None
        else _require_nonnegative_int(last_evidence_seq, field_name="last_evidence_seq")
    )
    normalized_last_authority_id = _optional_text(
        last_authority_id,
        field_name="last_authority_id",
    )
    if normalized_last_authority_id is None and normalized_last_evidence_seq is not None:
        normalized_last_authority_id = f"system_event:{normalized_last_evidence_seq}"
    row = await conn.fetchrow(
        """
        INSERT INTO public.subscription_checkpoints (
            checkpoint_id,
            subscription_id,
            run_id,
            last_evidence_seq,
            last_authority_id,
            checkpoint_status,
            checkpointed_at,
            metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb
        )
        ON CONFLICT (subscription_id, run_id) DO UPDATE SET
            checkpoint_id = EXCLUDED.checkpoint_id,
            last_evidence_seq = EXCLUDED.last_evidence_seq,
            last_authority_id = EXCLUDED.last_authority_id,
            checkpoint_status = EXCLUDED.checkpoint_status,
            checkpointed_at = EXCLUDED.checkpointed_at,
            metadata = EXCLUDED.metadata
        RETURNING
            checkpoint_id,
            subscription_id,
            run_id,
            last_evidence_seq,
            last_authority_id,
            checkpoint_status,
            checkpointed_at,
            metadata
        """,
        _subscription_checkpoint_id(
            subscription_id=normalized_subscription_id,
            run_id=normalized_run_id,
        ),
        normalized_subscription_id,
        normalized_run_id,
        normalized_last_evidence_seq,
        normalized_last_authority_id,
        _require_text(checkpoint_status, field_name="checkpoint_status"),
        _default_utc(checkpointed_at, field_name="checkpointed_at"),
        _encode_jsonb(_require_mapping(metadata, field_name="metadata"), field_name="metadata"),
    )
    return _row_dict(row, operation="upsert_subscription_checkpoint_record")


__all__ = [
    "PostgresSubscriptionRepository",
    "upsert_event_subscription_record",
    "upsert_subscription_checkpoint_record",
]
