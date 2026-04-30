"""Postgres persistence for managed-runtime accounting authority."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import json
from typing import Any, Mapping

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "managed_runtime.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and (key.endswith("_json") or key.endswith("_events_json")):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


def _normalize_optional_row(row: Any, *, operation: str) -> dict[str, Any] | None:
    if row is None:
        return None
    return _normalize_row(row, operation=operation)


def _normalize_rows(rows: Any, *, operation: str) -> list[dict[str, Any]]:
    return [_normalize_row(row, operation=operation) for row in (rows or [])]


def _optional_clean_text(value: object, *, field_name: str) -> str | None:
    if value is None or value == "":
        return None
    return _optional_text(value, field_name=field_name)


def _mapping(value: object, *, field_name: str) -> dict[str, Any]:
    return dict(_require_mapping(value, field_name=field_name))


def _list_mappings(value: object, *, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise PostgresWriteError(
            "managed_runtime.invalid_payload",
            f"{field_name} must be a list of JSON objects",
            details={"field_name": field_name},
        )
    return [dict(item) for item in value]


def _dt(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise PostgresWriteError(
        "managed_runtime.invalid_payload",
        f"{field_name} must be an ISO datetime",
        details={"field_name": field_name},
    )


def _decimal(value: object, *, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:  # pragma: no cover - defensive error shaping
        raise PostgresWriteError(
            "managed_runtime.invalid_payload",
            f"{field_name} must be decimal-compatible",
            details={"field_name": field_name},
        ) from exc


def persist_managed_runtime_record(
    conn: Any,
    *,
    runtime_record_id: str,
    receipt: dict[str, Any],
    usage_summary: dict[str, Any],
    mode_selection: dict[str, Any],
    meter_events: list[dict[str, Any]],
    pricing_schedule: dict[str, Any] | None,
    heartbeats: list[dict[str, Any]],
    pool_health: dict[str, Any] | None,
    audit_events: list[dict[str, Any]],
    customer_observability: dict[str, Any],
    internal_audit: dict[str, Any],
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    receipt_payload = _mapping(receipt, field_name="receipt")
    usage_payload = _mapping(usage_summary, field_name="usage_summary")
    selection_payload = _mapping(mode_selection, field_name="mode_selection")
    customer_payload = _mapping(customer_observability, field_name="customer_observability")
    internal_audit_payload = _mapping(internal_audit, field_name="internal_audit")
    identity = _mapping(receipt_payload.get("identity"), field_name="receipt.identity")
    cost = _mapping(receipt_payload.get("cost_summary"), field_name="receipt.cost_summary")

    _insert_pricing_schedule(conn, pricing_schedule)
    row = conn.fetchrow(
        """
        INSERT INTO managed_runtime_records (
            runtime_record_id,
            run_id,
            receipt_id,
            tenant_ref,
            environment_ref,
            workflow_ref,
            workload_class,
            attempt,
            configured_mode,
            execution_mode,
            terminal_status,
            runtime_version_ref,
            runtime_pool_ref,
            started_at,
            ended_at,
            duration_seconds,
            cost_status,
            cost_amount,
            currency,
            pricing_schedule_version_ref,
            policy_reason_code,
            dispatch_allowed,
            pool_health_state,
            metered_event_count,
            duplicate_meter_event_count,
            diagnostic_event_count,
            receipt_json,
            usage_summary_json,
            mode_selection_json,
            customer_observability_json,
            internal_audit_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24,
            $25, $26, $27::jsonb, $28::jsonb, $29::jsonb,
            $30::jsonb, $31::jsonb, $32, $33
        )
        ON CONFLICT (runtime_record_id) DO UPDATE SET
            receipt_id = EXCLUDED.receipt_id,
            terminal_status = EXCLUDED.terminal_status,
            runtime_version_ref = EXCLUDED.runtime_version_ref,
            runtime_pool_ref = EXCLUDED.runtime_pool_ref,
            started_at = EXCLUDED.started_at,
            ended_at = EXCLUDED.ended_at,
            duration_seconds = EXCLUDED.duration_seconds,
            cost_status = EXCLUDED.cost_status,
            cost_amount = EXCLUDED.cost_amount,
            currency = EXCLUDED.currency,
            pricing_schedule_version_ref = EXCLUDED.pricing_schedule_version_ref,
            policy_reason_code = EXCLUDED.policy_reason_code,
            dispatch_allowed = EXCLUDED.dispatch_allowed,
            pool_health_state = EXCLUDED.pool_health_state,
            metered_event_count = EXCLUDED.metered_event_count,
            duplicate_meter_event_count = EXCLUDED.duplicate_meter_event_count,
            diagnostic_event_count = EXCLUDED.diagnostic_event_count,
            receipt_json = EXCLUDED.receipt_json,
            usage_summary_json = EXCLUDED.usage_summary_json,
            mode_selection_json = EXCLUDED.mode_selection_json,
            customer_observability_json = EXCLUDED.customer_observability_json,
            internal_audit_json = EXCLUDED.internal_audit_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING *
        """,
        _require_text(runtime_record_id, field_name="runtime_record_id"),
        _require_text(identity.get("run_id"), field_name="identity.run_id"),
        _require_text(receipt_payload.get("receipt_id"), field_name="receipt.receipt_id"),
        _require_text(identity.get("tenant_ref"), field_name="identity.tenant_ref"),
        _require_text(identity.get("environment_ref"), field_name="identity.environment_ref"),
        _require_text(identity.get("workflow_ref"), field_name="identity.workflow_ref"),
        _require_text(identity.get("workload_class"), field_name="identity.workload_class"),
        int(identity.get("attempt") or 1),
        _require_text(selection_payload.get("configured_mode"), field_name="mode_selection.configured_mode"),
        _require_text(selection_payload.get("execution_mode"), field_name="mode_selection.execution_mode"),
        _require_text(receipt_payload.get("terminal_status"), field_name="receipt.terminal_status"),
        _require_text(receipt_payload.get("runtime_version_ref"), field_name="receipt.runtime_version_ref"),
        _optional_clean_text(receipt_payload.get("runtime_pool_ref"), field_name="receipt.runtime_pool_ref"),
        _dt(receipt_payload.get("started_at"), field_name="receipt.started_at"),
        _dt(receipt_payload.get("ended_at"), field_name="receipt.ended_at"),
        _decimal(receipt_payload.get("duration_seconds"), field_name="receipt.duration_seconds"),
        _require_text(cost.get("status"), field_name="cost.status"),
        _decimal(cost.get("amount"), field_name="cost.amount"),
        _require_text(cost.get("currency"), field_name="cost.currency"),
        _optional_clean_text(
            cost.get("pricing_schedule_version_ref"),
            field_name="cost.pricing_schedule_version_ref",
        ),
        _require_text(selection_payload.get("reason_code"), field_name="mode_selection.reason_code"),
        bool(pool_health.get("dispatch_allowed")) if pool_health else None,
        _optional_clean_text(pool_health.get("state") if pool_health else None, field_name="pool_health.state"),
        int(usage_payload.get("metered_event_count") or 0),
        int(usage_payload.get("duplicate_meter_event_count") or 0),
        int(usage_payload.get("diagnostic_event_count") or 0),
        _encode_jsonb(receipt_payload, field_name="receipt"),
        _encode_jsonb(usage_payload, field_name="usage_summary"),
        _encode_jsonb(selection_payload, field_name="mode_selection"),
        _encode_jsonb(customer_payload, field_name="customer_observability"),
        _encode_jsonb(internal_audit_payload, field_name="internal_audit"),
        _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_clean_text(source_ref, field_name="source_ref"),
    )

    _delete_child_rows(conn, runtime_record_id)
    _insert_meter_events(conn, runtime_record_id, _list_mappings(meter_events, field_name="meter_events"))
    _insert_heartbeats(conn, runtime_record_id, _list_mappings(heartbeats, field_name="heartbeats"))
    _insert_pool_health(conn, runtime_record_id, pool_health)
    _insert_audit_events(conn, runtime_record_id, _list_mappings(audit_events, field_name="audit_events"))
    return _normalize_row(row, operation="persist_managed_runtime_record")


def _delete_child_rows(conn: Any, runtime_record_id: str) -> None:
    for table in (
        "managed_runtime_audit_events",
        "managed_runtime_pool_health_snapshots",
        "managed_runtime_heartbeats",
        "managed_runtime_meter_events",
    ):
        conn.execute(f"DELETE FROM {table} WHERE runtime_record_id = $1", runtime_record_id)


def _insert_pricing_schedule(conn: Any, schedule: dict[str, Any] | None) -> None:
    if not schedule:
        return
    conn.execute(
        """
        INSERT INTO managed_runtime_pricing_schedule_versions (
            schedule_ref, version_ref, effective_at, currency,
            cpu_core_second_rate, memory_gib_second_rate,
            accelerator_second_rate, minimum_charge, schedule_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
        ON CONFLICT (version_ref) DO UPDATE SET
            schedule_ref = EXCLUDED.schedule_ref,
            effective_at = EXCLUDED.effective_at,
            currency = EXCLUDED.currency,
            cpu_core_second_rate = EXCLUDED.cpu_core_second_rate,
            memory_gib_second_rate = EXCLUDED.memory_gib_second_rate,
            accelerator_second_rate = EXCLUDED.accelerator_second_rate,
            minimum_charge = EXCLUDED.minimum_charge,
            schedule_json = EXCLUDED.schedule_json,
            updated_at = now()
        """,
        _require_text(schedule.get("schedule_ref"), field_name="pricing_schedule.schedule_ref"),
        _require_text(schedule.get("version_ref"), field_name="pricing_schedule.version_ref"),
        _dt(schedule.get("effective_at"), field_name="pricing_schedule.effective_at"),
        _require_text(schedule.get("currency"), field_name="pricing_schedule.currency"),
        _decimal(schedule.get("cpu_core_second_rate"), field_name="pricing_schedule.cpu_core_second_rate"),
        _decimal(schedule.get("memory_gib_second_rate"), field_name="pricing_schedule.memory_gib_second_rate"),
        _decimal(schedule.get("accelerator_second_rate"), field_name="pricing_schedule.accelerator_second_rate"),
        _decimal(schedule.get("minimum_charge"), field_name="pricing_schedule.minimum_charge"),
        _encode_jsonb(schedule, field_name="pricing_schedule"),
    )


def _insert_meter_events(
    conn: Any,
    runtime_record_id: str,
    events: list[dict[str, Any]],
) -> None:
    if not events:
        return
    conn.execute_many(
        """
        INSERT INTO managed_runtime_meter_events (
            runtime_record_id, event_id, idempotency_key, run_id, tenant_ref,
            environment_ref, workflow_ref, execution_mode, runtime_version_ref,
            occurred_at, event_kind, billable, receipt_id, source_event_ref,
            event_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb)
        """,
        [
            (
                runtime_record_id,
                _require_text(item.get("event_id"), field_name="meter_event.event_id"),
                _require_text(item.get("idempotency_key"), field_name="meter_event.idempotency_key"),
                _require_text(item.get("run_id"), field_name="meter_event.run_id"),
                _require_text(item.get("tenant_ref"), field_name="meter_event.tenant_ref"),
                _require_text(item.get("environment_ref"), field_name="meter_event.environment_ref"),
                _require_text(item.get("workflow_ref"), field_name="meter_event.workflow_ref"),
                _require_text(item.get("execution_mode"), field_name="meter_event.execution_mode"),
                _require_text(item.get("runtime_version_ref"), field_name="meter_event.runtime_version_ref"),
                _dt(item.get("occurred_at"), field_name="meter_event.occurred_at"),
                _require_text(item.get("event_kind"), field_name="meter_event.event_kind"),
                bool(item.get("billable")),
                _optional_clean_text(item.get("receipt_id"), field_name="meter_event.receipt_id"),
                _optional_clean_text(item.get("source_event_ref"), field_name="meter_event.source_event_ref"),
                _encode_jsonb(item, field_name="meter_event"),
            )
            for item in events
        ],
    )


def _insert_heartbeats(
    conn: Any,
    runtime_record_id: str,
    heartbeats: list[dict[str, Any]],
) -> None:
    if not heartbeats:
        return
    conn.execute_many(
        """
        INSERT INTO managed_runtime_heartbeats (
            runtime_record_id, worker_ref, pool_ref, tenant_ref, environment_ref,
            runtime_version_ref, observed_at, capacity_slots, active_runs,
            accepting_work, last_error_code, heartbeat_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
        """,
        [
            (
                runtime_record_id,
                _require_text(item.get("worker_ref"), field_name="heartbeat.worker_ref"),
                _require_text(item.get("pool_ref"), field_name="heartbeat.pool_ref"),
                _require_text(item.get("tenant_ref"), field_name="heartbeat.tenant_ref"),
                _require_text(item.get("environment_ref"), field_name="heartbeat.environment_ref"),
                _require_text(item.get("runtime_version_ref"), field_name="heartbeat.runtime_version_ref"),
                _dt(item.get("observed_at"), field_name="heartbeat.observed_at"),
                int(item.get("capacity_slots") or 0),
                int(item.get("active_runs") or 0),
                bool(item.get("accepting_work")),
                _optional_clean_text(item.get("last_error_code"), field_name="heartbeat.last_error_code"),
                _encode_jsonb(item, field_name="heartbeat"),
            )
            for item in heartbeats
        ],
    )


def _insert_pool_health(
    conn: Any,
    runtime_record_id: str,
    health: dict[str, Any] | None,
) -> None:
    if not health:
        return
    conn.execute(
        """
        INSERT INTO managed_runtime_pool_health_snapshots (
            runtime_record_id, pool_ref, tenant_ref, environment_ref, state, evaluated_at,
            dispatch_allowed, reason_codes_json, health_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb)
        """,
        runtime_record_id,
        _require_text(health.get("pool_ref"), field_name="pool_health.pool_ref"),
        _require_text(health.get("tenant_ref"), field_name="pool_health.tenant_ref"),
        _require_text(health.get("environment_ref"), field_name="pool_health.environment_ref"),
        _require_text(health.get("state"), field_name="pool_health.state"),
        _dt(health.get("evaluated_at"), field_name="pool_health.evaluated_at"),
        bool(health.get("dispatch_allowed")),
        _encode_jsonb(health.get("reason_codes") or [], field_name="pool_health.reason_codes"),
        _encode_jsonb(health, field_name="pool_health"),
    )


def _insert_audit_events(
    conn: Any,
    runtime_record_id: str,
    events: list[dict[str, Any]],
) -> None:
    if not events:
        return
    conn.execute_many(
        """
        INSERT INTO managed_runtime_audit_events (
            runtime_record_id, audit_event_id, occurred_at, actor_ref,
            action, target_kind, target_ref, tenant_ref, environment_ref,
            reason_code, run_id, before_version_ref, after_version_ref,
            audit_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)
        """,
        [
            (
                runtime_record_id,
                _require_text(item.get("audit_event_id"), field_name="audit_event.audit_event_id"),
                _dt(item.get("occurred_at"), field_name="audit_event.occurred_at"),
                _require_text(item.get("actor_ref"), field_name="audit_event.actor_ref"),
                _require_text(item.get("action"), field_name="audit_event.action"),
                _require_text(item.get("target_kind"), field_name="audit_event.target_kind"),
                _require_text(item.get("target_ref"), field_name="audit_event.target_ref"),
                _require_text(item.get("tenant_ref"), field_name="audit_event.tenant_ref"),
                _require_text(item.get("environment_ref"), field_name="audit_event.environment_ref"),
                _require_text(item.get("reason_code"), field_name="audit_event.reason_code"),
                _optional_clean_text(item.get("run_id"), field_name="audit_event.run_id"),
                _optional_clean_text(item.get("before_version_ref"), field_name="audit_event.before_version_ref"),
                _optional_clean_text(item.get("after_version_ref"), field_name="audit_event.after_version_ref"),
                _encode_jsonb(item, field_name="audit_event"),
            )
            for item in events
        ],
    )


def list_managed_runtime_records(
    conn: Any,
    *,
    runtime_record_id: str | None = None,
    run_id: str | None = None,
    receipt_id: str | None = None,
    tenant_ref: str | None = None,
    environment_ref: str | None = None,
    workflow_ref: str | None = None,
    execution_mode: str | None = None,
    configured_mode: str | None = None,
    terminal_status: str | None = None,
    cost_status: str | None = None,
    source_ref: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _list_rows(
        conn,
        table="managed_runtime_records",
        filters=(
            ("runtime_record_id", runtime_record_id),
            ("run_id", run_id),
            ("receipt_id", receipt_id),
            ("tenant_ref", tenant_ref),
            ("environment_ref", environment_ref),
            ("workflow_ref", workflow_ref),
            ("execution_mode", execution_mode),
            ("configured_mode", configured_mode),
            ("terminal_status", terminal_status),
            ("cost_status", cost_status),
            ("source_ref", source_ref),
        ),
        order_by="updated_at DESC, runtime_record_id",
        limit=limit,
        operation="list_managed_runtime_records",
    )


def load_managed_runtime_record(
    conn: Any,
    *,
    runtime_record_id: str,
    include_meter_events: bool = True,
    include_heartbeats: bool = True,
    include_pool_health: bool = True,
    include_audit_events: bool = True,
    include_pricing_schedule: bool = True,
) -> dict[str, Any] | None:
    record = _normalize_optional_row(
        conn.fetchrow(
            "SELECT * FROM managed_runtime_records WHERE runtime_record_id = $1",
            runtime_record_id,
        ),
        operation="load_managed_runtime_record",
    )
    if record is None:
        return None
    if include_meter_events:
        record["meter_events"] = _fetch_child_json(
            conn,
            table="managed_runtime_meter_events",
            json_column="event_json",
            runtime_record_id=runtime_record_id,
            order_by="occurred_at, event_id",
        )
    if include_heartbeats:
        record["heartbeats"] = _fetch_child_json(
            conn,
            table="managed_runtime_heartbeats",
            json_column="heartbeat_json",
            runtime_record_id=runtime_record_id,
            order_by="observed_at DESC, worker_ref",
        )
    if include_pool_health:
        record["pool_health"] = _fetch_child_json(
            conn,
            table="managed_runtime_pool_health_snapshots",
            json_column="health_json",
            runtime_record_id=runtime_record_id,
            order_by="evaluated_at DESC, pool_ref",
        )
    if include_audit_events:
        record["audit_events"] = _fetch_child_json(
            conn,
            table="managed_runtime_audit_events",
            json_column="audit_json",
            runtime_record_id=runtime_record_id,
            order_by="occurred_at DESC, audit_event_id",
        )
    if include_pricing_schedule and record.get("pricing_schedule_version_ref"):
        record["pricing_schedule"] = _normalize_optional_row(
            conn.fetchrow(
                """
                SELECT *
                  FROM managed_runtime_pricing_schedule_versions
                 WHERE version_ref = $1
                """,
                record["pricing_schedule_version_ref"],
            ),
            operation="load_managed_runtime_pricing_schedule",
        )
    return record


def list_managed_runtime_meter_events(conn: Any, **filters: Any) -> list[dict[str, Any]]:
    limit = filters.pop("limit", 50)
    return _list_rows(
        conn,
        table="managed_runtime_meter_events",
        filters=tuple((key, value) for key, value in filters.items()),
        order_by="occurred_at DESC, event_id",
        limit=limit,
        operation="list_managed_runtime_meter_events",
    )


def list_managed_runtime_heartbeats(conn: Any, **filters: Any) -> list[dict[str, Any]]:
    limit = filters.pop("limit", 50)
    return _list_rows(
        conn,
        table="managed_runtime_heartbeats",
        filters=tuple((key, value) for key, value in filters.items()),
        order_by="observed_at DESC, worker_ref",
        limit=limit,
        operation="list_managed_runtime_heartbeats",
    )


def list_managed_runtime_pool_health(
    conn: Any,
    *,
    health_state: str | None = None,
    limit: int = 50,
    **filters: Any,
) -> list[dict[str, Any]]:
    filters["state"] = health_state
    return _list_rows(
        conn,
        table="managed_runtime_pool_health_snapshots",
        filters=tuple((key, value) for key, value in filters.items()),
        order_by="evaluated_at DESC, pool_ref",
        limit=limit,
        operation="list_managed_runtime_pool_health",
    )


def list_managed_runtime_audit_events(conn: Any, **filters: Any) -> list[dict[str, Any]]:
    limit = filters.pop("limit", 50)
    return _list_rows(
        conn,
        table="managed_runtime_audit_events",
        filters=tuple((key, value) for key, value in filters.items()),
        order_by="occurred_at DESC, audit_event_id",
        limit=limit,
        operation="list_managed_runtime_audit_events",
    )


def list_managed_runtime_pricing_schedules(
    conn: Any,
    *,
    schedule_ref: str | None = None,
    version_ref: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    return _list_rows(
        conn,
        table="managed_runtime_pricing_schedule_versions",
        filters=(("schedule_ref", schedule_ref), ("version_ref", version_ref)),
        order_by="effective_at DESC, version_ref",
        limit=limit,
        operation="list_managed_runtime_pricing_schedules",
    )


def _list_rows(
    conn: Any,
    *,
    table: str,
    filters: tuple[tuple[str, Any], ...],
    order_by: str,
    limit: int,
    operation: str,
) -> list[dict[str, Any]]:
    clauses = ["TRUE"]
    args: list[Any] = []
    for column, value in filters:
        if value is not None:
            args.append(value)
            clauses.append(f"{column} = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT *
          FROM {table}
         WHERE {' AND '.join(clauses)}
         ORDER BY {order_by}
         LIMIT ${len(args)}
        """,
        *args,
    )
    return _normalize_rows(rows, operation=operation)


def _fetch_child_json(
    conn: Any,
    *,
    table: str,
    json_column: str,
    runtime_record_id: str,
    order_by: str,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        f"SELECT {json_column} FROM {table} WHERE runtime_record_id = $1 ORDER BY {order_by}",
        runtime_record_id,
    )
    return [row[json_column] for row in _normalize_rows(rows, operation=f"fetch_{table}")]


__all__ = [
    "persist_managed_runtime_record",
    "list_managed_runtime_records",
    "load_managed_runtime_record",
    "list_managed_runtime_meter_events",
    "list_managed_runtime_heartbeats",
    "list_managed_runtime_pool_health",
    "list_managed_runtime_audit_events",
    "list_managed_runtime_pricing_schedules",
]
