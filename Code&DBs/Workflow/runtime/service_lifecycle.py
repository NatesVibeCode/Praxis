"""CQRS service lifecycle authority.

This module owns service target registration, desired-state declarations,
observed lifecycle events, and the service instance projection. It intentionally
does not launch processes or resolve filesystem paths; reconcilers read the
database contract and report evidence back here.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
import json
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


SUBSTRATE_KINDS = frozenset(
    {
        "browser",
        "mobile_device",
        "desktop_host",
        "home_box",
        "lan_node",
        "cloud_service",
        "saas_connector",
        "container",
        "managed_service",
        "unknown",
    }
)
SERVICE_KINDS = frozenset(
    {
        "http_api",
        "web_app",
        "worker",
        "database",
        "connector",
        "automation",
        "managed_service",
        "other",
    }
)
DESIRED_STATUSES = frozenset({"running", "stopped", "paused", "absent"})
OBSERVED_STATUSES = frozenset(
    {
        "unknown",
        "pending",
        "starting",
        "running",
        "healthy",
        "unhealthy",
        "stopping",
        "stopped",
        "failed",
        "absent",
    }
)
EVENT_STATUSES = frozenset({"recorded", "accepted", "rejected", "failed"})

_EVENT_OBSERVED_STATUS_DEFAULTS = {
    "desired_state_declared": "pending",
    "reconcile_started": "starting",
    "process_started": "running",
    "service_started": "running",
    "health_check_passed": "healthy",
    "health_check_failed": "unhealthy",
    "process_stopping": "stopping",
    "process_stopped": "stopped",
    "service_stopped": "stopped",
    "reconcile_failed": "failed",
    "desired_absent_confirmed": "absent",
}


class ServiceLifecycleError(RuntimeError):
    """Raised when service lifecycle authority rejects an operation."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        status_code: int = 400,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.status_code = status_code
        self.details = dict(details or {})


class RegisterRuntimeTargetCommand(BaseModel):
    runtime_target_ref: str
    substrate_kind: str = "unknown"
    display_name: str | None = None
    target_scope: str = "service_lifecycle"
    workspace_ref: str | None = None
    base_path_ref: str | None = None
    host_ref: str | None = None
    endpoint_contract: dict[str, Any] = Field(default_factory=dict)
    capability_contract: dict[str, Any] = Field(default_factory=dict)
    secret_provider_ref: str | None = None
    enabled: bool = True
    decision_ref: str = "decision.service_lifecycle.runtime_target_neutrality.20260422"


class RegisterServiceDefinitionCommand(BaseModel):
    service_ref: str
    service_kind: str = "other"
    display_name: str | None = None
    owner_ref: str = "praxis.engine"
    desired_state_schema: dict[str, Any] = Field(default_factory=dict)
    health_contract: dict[str, Any] = Field(default_factory=dict)
    default_reconciler_ref: str | None = None
    enabled: bool = True
    decision_ref: str = "decision.service_lifecycle.runtime_target_neutrality.20260422"


class DeclareServiceDesiredStateCommand(BaseModel):
    service_ref: str
    runtime_target_ref: str
    desired_status: str
    desired_config: dict[str, Any] = Field(default_factory=dict)
    environment_refs: dict[str, Any] = Field(default_factory=dict)
    health_contract: dict[str, Any] = Field(default_factory=dict)
    reconciler_ref: str | None = None
    declared_by: str = "operator"
    declaration_reason: str | None = None
    idempotency_key: str | None = None


class RecordServiceLifecycleEventCommand(BaseModel):
    service_ref: str
    runtime_target_ref: str
    desired_state_ref: str | None = None
    event_type: str
    observed_status: str | None = None
    event_payload: dict[str, Any] = Field(default_factory=dict)
    event_status: str = "recorded"
    observed_by: str = "unknown"
    operation_ref: str = "service.lifecycle.record_event"
    endpoint_refs: dict[str, Any] | None = None
    failure_reason: str | None = None


class QueryServiceProjectionCommand(BaseModel):
    service_ref: str
    runtime_target_ref: str


class ListRuntimeTargetsCommand(BaseModel):
    substrate_kind: str | None = None
    workspace_ref: str | None = None
    target_scope: str | None = None
    enabled_only: bool = True
    limit: int = 100


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ServiceLifecycleError(
            "service_lifecycle.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_enum(value: object, *, field_name: str, allowed: frozenset[str]) -> str:
    normalized = _require_text(value, field_name=field_name)
    if normalized not in allowed:
        raise ServiceLifecycleError(
            "service_lifecycle.invalid_submission",
            f"{field_name} must be one of: {', '.join(sorted(allowed))}",
            details={"field": field_name, "value": normalized},
        )
    return normalized


def normalize_substrate_kind(value: object) -> str:
    return _require_enum(value, field_name="substrate_kind", allowed=SUBSTRATE_KINDS)


def normalize_service_kind(value: object) -> str:
    return _require_enum(value, field_name="service_kind", allowed=SERVICE_KINDS)


def normalize_desired_status(value: object) -> str:
    return _require_enum(value, field_name="desired_status", allowed=DESIRED_STATUSES)


def normalize_observed_status(value: object | None) -> str | None:
    if value is None:
        return None
    return _require_enum(value, field_name="observed_status", allowed=OBSERVED_STATUSES)


def normalize_event_status(value: object) -> str:
    return _require_enum(value, field_name="event_status", allowed=EVENT_STATUSES)


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ServiceLifecycleError(
                "service_lifecycle.invalid_submission",
                f"{field_name} must be a JSON object",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, Mapping):
        raise ServiceLifecycleError(
            "service_lifecycle.invalid_submission",
            f"{field_name} must be a JSON object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return dict(value)


def _json_dumps(value: object, *, field_name: str) -> str:
    return json.dumps(_json_object(value, field_name=field_name), sort_keys=True, default=str)


def _json_or_none(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _json_dumps(value, field_name=field_name)


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _row_payload(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    for key, value in list(payload.items()):
        if key.endswith("_contract") or key.endswith("_config") or key.endswith("_refs"):
            payload[key] = _decode_json(value)
        elif key in {"desired_state_schema", "event_payload", "endpoint_refs"}:
            payload[key] = _decode_json(value)
    return payload


def _require_enabled_row(
    conn: Any,
    *,
    table_name: str,
    key_column: str,
    key_value: str,
    reason_code: str,
) -> None:
    row = conn.fetchrow(
        f"""
        SELECT 1
          FROM {table_name}
         WHERE {key_column} = $1
           AND enabled = TRUE
         LIMIT 1
        """,
        key_value,
    )
    if row is None:
        raise ServiceLifecycleError(
            reason_code,
            f"{table_name}.{key_column} not found or disabled: {key_value}",
            status_code=404,
            details={key_column: key_value},
        )


@contextmanager
def _transaction(conn: Any):
    transaction = getattr(conn, "transaction", None)
    if callable(transaction):
        with transaction() as tx:
            yield tx
        return
    yield conn


def _pg_conn(subsystems: Any) -> Any:
    if not hasattr(subsystems, "get_pg_conn") or not callable(subsystems.get_pg_conn):
        raise ServiceLifecycleError(
            "service_lifecycle.missing_postgres",
            "service lifecycle authority requires subsystems.get_pg_conn()",
            status_code=500,
        )
    return subsystems.get_pg_conn()


def register_runtime_target(
    conn: Any,
    command: RegisterRuntimeTargetCommand,
) -> dict[str, Any]:
    runtime_target_ref = _require_text(
        command.runtime_target_ref,
        field_name="runtime_target_ref",
    )
    substrate_kind = normalize_substrate_kind(command.substrate_kind)
    target_scope = _require_text(command.target_scope, field_name="target_scope")
    display_name = _require_text(
        command.display_name or runtime_target_ref,
        field_name="display_name",
    )
    row = conn.fetchrow(
        """
        INSERT INTO runtime_targets (
            runtime_target_ref,
            target_scope,
            substrate_kind,
            display_name,
            workspace_ref,
            base_path_ref,
            host_ref,
            endpoint_contract,
            capability_contract,
            secret_provider_ref,
            enabled,
            decision_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12
        )
        ON CONFLICT (runtime_target_ref) DO UPDATE SET
            target_scope = EXCLUDED.target_scope,
            substrate_kind = EXCLUDED.substrate_kind,
            display_name = EXCLUDED.display_name,
            workspace_ref = EXCLUDED.workspace_ref,
            base_path_ref = EXCLUDED.base_path_ref,
            host_ref = EXCLUDED.host_ref,
            endpoint_contract = EXCLUDED.endpoint_contract,
            capability_contract = EXCLUDED.capability_contract,
            secret_provider_ref = EXCLUDED.secret_provider_ref,
            enabled = EXCLUDED.enabled,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = now()
        RETURNING
            runtime_target_ref,
            target_scope,
            substrate_kind,
            display_name,
            workspace_ref,
            base_path_ref,
            host_ref,
            endpoint_contract,
            capability_contract,
            secret_provider_ref,
            enabled,
            decision_ref,
            created_at,
            updated_at
        """,
        runtime_target_ref,
        target_scope,
        substrate_kind,
        display_name,
        _optional_text(command.workspace_ref, field_name="workspace_ref"),
        _optional_text(command.base_path_ref, field_name="base_path_ref"),
        _optional_text(command.host_ref, field_name="host_ref"),
        _json_dumps(command.endpoint_contract, field_name="endpoint_contract"),
        _json_dumps(command.capability_contract, field_name="capability_contract"),
        _optional_text(command.secret_provider_ref, field_name="secret_provider_ref"),
        bool(command.enabled),
        _require_text(command.decision_ref, field_name="decision_ref"),
    )
    return {"status": "registered", "target": _row_payload(row)}


def register_service_definition(
    conn: Any,
    command: RegisterServiceDefinitionCommand,
) -> dict[str, Any]:
    service_ref = _require_text(command.service_ref, field_name="service_ref")
    service_kind = normalize_service_kind(command.service_kind)
    display_name = _require_text(command.display_name or service_ref, field_name="display_name")
    row = conn.fetchrow(
        """
        INSERT INTO service_definitions (
            service_ref,
            service_kind,
            display_name,
            owner_ref,
            desired_state_schema,
            health_contract,
            default_reconciler_ref,
            enabled,
            decision_ref
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9
        )
        ON CONFLICT (service_ref) DO UPDATE SET
            service_kind = EXCLUDED.service_kind,
            display_name = EXCLUDED.display_name,
            owner_ref = EXCLUDED.owner_ref,
            desired_state_schema = EXCLUDED.desired_state_schema,
            health_contract = EXCLUDED.health_contract,
            default_reconciler_ref = EXCLUDED.default_reconciler_ref,
            enabled = EXCLUDED.enabled,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = now()
        RETURNING
            service_ref,
            service_kind,
            display_name,
            owner_ref,
            desired_state_schema,
            health_contract,
            default_reconciler_ref,
            enabled,
            decision_ref,
            created_at,
            updated_at
        """,
        service_ref,
        service_kind,
        display_name,
        _require_text(command.owner_ref, field_name="owner_ref"),
        _json_dumps(command.desired_state_schema, field_name="desired_state_schema"),
        _json_dumps(command.health_contract, field_name="health_contract"),
        _optional_text(command.default_reconciler_ref, field_name="default_reconciler_ref"),
        bool(command.enabled),
        _require_text(command.decision_ref, field_name="decision_ref"),
    )
    return {"status": "registered", "service": _row_payload(row)}


def get_service_projection(
    conn: Any,
    *,
    service_ref: str,
    runtime_target_ref: str,
    allow_missing: bool = False,
) -> dict[str, Any] | None:
    service = _require_text(service_ref, field_name="service_ref")
    target = _require_text(runtime_target_ref, field_name="runtime_target_ref")
    row = conn.fetchrow(
        """
        SELECT
            service_ref,
            runtime_target_ref,
            active_desired_state_ref,
            desired_status,
            observed_status,
            endpoint_refs,
            last_event_id,
            last_event_sequence,
            last_checked_at,
            last_healthy_at,
            failure_reason,
            projection_revision,
            updated_at
          FROM service_instance_projection
         WHERE service_ref = $1
           AND runtime_target_ref = $2
         LIMIT 1
        """,
        service,
        target,
    )
    projection = _row_payload(row)
    if projection is None and not allow_missing:
        raise ServiceLifecycleError(
            "service_lifecycle.projection_missing",
            "service instance projection does not exist",
            status_code=404,
            details={"service_ref": service, "runtime_target_ref": target},
        )
    return projection


def declare_service_desired_state(
    conn: Any,
    command: DeclareServiceDesiredStateCommand,
) -> dict[str, Any]:
    service_ref = _require_text(command.service_ref, field_name="service_ref")
    runtime_target_ref = _require_text(command.runtime_target_ref, field_name="runtime_target_ref")
    desired_status = normalize_desired_status(command.desired_status)
    idempotency_key = _optional_text(command.idempotency_key, field_name="idempotency_key")

    with _transaction(conn) as tx:
        _require_enabled_row(
            tx,
            table_name="service_definitions",
            key_column="service_ref",
            key_value=service_ref,
            reason_code="service_lifecycle.service_missing",
        )
        _require_enabled_row(
            tx,
            table_name="runtime_targets",
            key_column="runtime_target_ref",
            key_value=runtime_target_ref,
            reason_code="service_lifecycle.target_missing",
        )

        if idempotency_key:
            existing = tx.fetchrow(
                """
                SELECT
                    desired_state_ref,
                    service_ref,
                    runtime_target_ref,
                    desired_status,
                    desired_config,
                    environment_refs,
                    health_contract,
                    reconciler_ref,
                    declared_by,
                    declaration_reason,
                    idempotency_key,
                    supersedes_ref,
                    active,
                    declared_at,
                    created_at
                  FROM service_desired_states
                 WHERE idempotency_key = $1
                 LIMIT 1
                """,
                idempotency_key,
            )
            if existing is not None:
                return {
                    "status": "replayed",
                    "desired_state": _row_payload(existing),
                    "projection": get_service_projection(
                        tx,
                        service_ref=service_ref,
                        runtime_target_ref=runtime_target_ref,
                        allow_missing=True,
                    ),
                }

        desired_state_ref = f"service_desired.{uuid4().hex}"
        previous = tx.fetchrow(
            """
            UPDATE service_desired_states
               SET active = FALSE
             WHERE service_ref = $1
               AND runtime_target_ref = $2
               AND active = TRUE
             RETURNING desired_state_ref
            """,
            service_ref,
            runtime_target_ref,
        )
        supersedes_ref = previous["desired_state_ref"] if previous is not None else None
        desired_row = tx.fetchrow(
            """
            INSERT INTO service_desired_states (
                desired_state_ref,
                service_ref,
                runtime_target_ref,
                desired_status,
                desired_config,
                environment_refs,
                health_contract,
                reconciler_ref,
                declared_by,
                declaration_reason,
                idempotency_key,
                supersedes_ref,
                active
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12, TRUE
            )
            RETURNING
                desired_state_ref,
                service_ref,
                runtime_target_ref,
                desired_status,
                desired_config,
                environment_refs,
                health_contract,
                reconciler_ref,
                declared_by,
                declaration_reason,
                idempotency_key,
                supersedes_ref,
                active,
                declared_at,
                created_at
            """,
            desired_state_ref,
            service_ref,
            runtime_target_ref,
            desired_status,
            _json_dumps(command.desired_config, field_name="desired_config"),
            _json_dumps(command.environment_refs, field_name="environment_refs"),
            _json_dumps(command.health_contract, field_name="health_contract"),
            _optional_text(command.reconciler_ref, field_name="reconciler_ref"),
            _require_text(command.declared_by, field_name="declared_by"),
            _optional_text(command.declaration_reason, field_name="declaration_reason"),
            idempotency_key,
            supersedes_ref,
        )
        event_row = tx.fetchrow(
            """
            INSERT INTO service_instance_events (
                service_ref,
                runtime_target_ref,
                desired_state_ref,
                event_type,
                observed_status,
                event_payload,
                event_status,
                observed_by,
                operation_ref
            ) VALUES (
                $1, $2, $3, 'desired_state_declared', 'pending', $4::jsonb, 'recorded', $5, $6
            )
            RETURNING
                event_id,
                event_sequence,
                service_ref,
                runtime_target_ref,
                desired_state_ref,
                event_type,
                observed_status,
                event_payload,
                event_status,
                observed_by,
                operation_ref,
                occurred_at,
                created_at
            """,
            service_ref,
            runtime_target_ref,
            desired_state_ref,
            json.dumps(
                {
                    "desired_state_ref": desired_state_ref,
                    "desired_status": desired_status,
                    "declaration_reason": command.declaration_reason,
                    "supersedes_ref": supersedes_ref,
                },
                sort_keys=True,
                default=str,
            ),
            _require_text(command.declared_by, field_name="declared_by"),
            "service.lifecycle.declare_desired_state",
        )
        projection_row = tx.fetchrow(
            """
            INSERT INTO service_instance_projection (
                service_ref,
                runtime_target_ref,
                active_desired_state_ref,
                desired_status,
                observed_status,
                last_event_id,
                last_event_sequence,
                last_checked_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, 'pending', $5::uuid, $6, now(), now()
            )
            ON CONFLICT (service_ref, runtime_target_ref) DO UPDATE SET
                active_desired_state_ref = EXCLUDED.active_desired_state_ref,
                desired_status = EXCLUDED.desired_status,
                observed_status = 'pending',
                last_event_id = EXCLUDED.last_event_id,
                last_event_sequence = EXCLUDED.last_event_sequence,
                last_checked_at = now(),
                failure_reason = NULL,
                projection_revision = service_instance_projection.projection_revision + 1,
                updated_at = now()
            RETURNING
                service_ref,
                runtime_target_ref,
                active_desired_state_ref,
                desired_status,
                observed_status,
                endpoint_refs,
                last_event_id,
                last_event_sequence,
                last_checked_at,
                last_healthy_at,
                failure_reason,
                projection_revision,
                updated_at
            """,
            service_ref,
            runtime_target_ref,
            desired_state_ref,
            desired_status,
            str(event_row["event_id"]),
            int(event_row["event_sequence"]),
        )

    return {
        "status": "declared",
        "desired_state": _row_payload(desired_row),
        "event": _row_payload(event_row),
        "projection": _row_payload(projection_row),
    }


def _observed_status_for_event(event_type: str, observed_status: str | None) -> str | None:
    if observed_status is not None:
        return normalize_observed_status(observed_status)
    return _EVENT_OBSERVED_STATUS_DEFAULTS.get(event_type)


def record_service_lifecycle_event(
    conn: Any,
    command: RecordServiceLifecycleEventCommand,
) -> dict[str, Any]:
    service_ref = _require_text(command.service_ref, field_name="service_ref")
    runtime_target_ref = _require_text(command.runtime_target_ref, field_name="runtime_target_ref")
    event_type = _require_text(command.event_type, field_name="event_type")
    observed_status = _observed_status_for_event(event_type, command.observed_status)
    event_status = normalize_event_status(command.event_status)

    with _transaction(conn) as tx:
        _require_enabled_row(
            tx,
            table_name="service_definitions",
            key_column="service_ref",
            key_value=service_ref,
            reason_code="service_lifecycle.service_missing",
        )
        _require_enabled_row(
            tx,
            table_name="runtime_targets",
            key_column="runtime_target_ref",
            key_value=runtime_target_ref,
            reason_code="service_lifecycle.target_missing",
        )
        active_desired = tx.fetchrow(
            """
            SELECT desired_state_ref, desired_status
              FROM service_desired_states
             WHERE service_ref = $1
               AND runtime_target_ref = $2
               AND active = TRUE
             LIMIT 1
            """,
            service_ref,
            runtime_target_ref,
        )
        desired_state_ref = _optional_text(
            command.desired_state_ref,
            field_name="desired_state_ref",
        ) or (active_desired["desired_state_ref"] if active_desired is not None else None)
        desired_status = active_desired["desired_status"] if active_desired is not None else "unknown"
        event_row = tx.fetchrow(
            """
            INSERT INTO service_instance_events (
                service_ref,
                runtime_target_ref,
                desired_state_ref,
                event_type,
                observed_status,
                event_payload,
                event_status,
                observed_by,
                operation_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9
            )
            RETURNING
                event_id,
                event_sequence,
                service_ref,
                runtime_target_ref,
                desired_state_ref,
                event_type,
                observed_status,
                event_payload,
                event_status,
                observed_by,
                operation_ref,
                occurred_at,
                created_at
            """,
            service_ref,
            runtime_target_ref,
            desired_state_ref,
            event_type,
            observed_status,
            _json_dumps(command.event_payload, field_name="event_payload"),
            event_status,
            _require_text(command.observed_by, field_name="observed_by"),
            _require_text(command.operation_ref, field_name="operation_ref"),
        )
        projection_row = tx.fetchrow(
            """
            INSERT INTO service_instance_projection (
                service_ref,
                runtime_target_ref,
                active_desired_state_ref,
                desired_status,
                observed_status,
                endpoint_refs,
                last_event_id,
                last_event_sequence,
                last_checked_at,
                last_healthy_at,
                failure_reason,
                updated_at
            ) VALUES (
                $1,
                $2,
                $3,
                $4,
                COALESCE($5, 'unknown'),
                COALESCE($6::jsonb, '{}'::jsonb),
                $7::uuid,
                $8,
                now(),
                CASE WHEN $5 = 'healthy' THEN now() ELSE NULL END,
                CASE WHEN $5 = 'healthy' THEN NULL ELSE $9 END,
                now()
            )
            ON CONFLICT (service_ref, runtime_target_ref) DO UPDATE SET
                active_desired_state_ref = COALESCE(EXCLUDED.active_desired_state_ref, service_instance_projection.active_desired_state_ref),
                desired_status = EXCLUDED.desired_status,
                observed_status = COALESCE($5, service_instance_projection.observed_status),
                endpoint_refs = CASE
                    WHEN $6::jsonb IS NULL THEN service_instance_projection.endpoint_refs
                    ELSE service_instance_projection.endpoint_refs || $6::jsonb
                END,
                last_event_id = EXCLUDED.last_event_id,
                last_event_sequence = EXCLUDED.last_event_sequence,
                last_checked_at = now(),
                last_healthy_at = CASE
                    WHEN $5 = 'healthy' THEN now()
                    ELSE service_instance_projection.last_healthy_at
                END,
                failure_reason = CASE
                    WHEN $5 = 'healthy' THEN NULL
                    ELSE COALESCE($9, service_instance_projection.failure_reason)
                END,
                projection_revision = service_instance_projection.projection_revision + 1,
                updated_at = now()
            RETURNING
                service_ref,
                runtime_target_ref,
                active_desired_state_ref,
                desired_status,
                observed_status,
                endpoint_refs,
                last_event_id,
                last_event_sequence,
                last_checked_at,
                last_healthy_at,
                failure_reason,
                projection_revision,
                updated_at
            """,
            service_ref,
            runtime_target_ref,
            desired_state_ref,
            desired_status,
            observed_status,
            _json_or_none(command.endpoint_refs, field_name="endpoint_refs"),
            str(event_row["event_id"]),
            int(event_row["event_sequence"]),
            _optional_text(command.failure_reason, field_name="failure_reason"),
        )

    return {
        "status": "recorded",
        "event": _row_payload(event_row),
        "projection": _row_payload(projection_row),
    }


def list_runtime_targets(
    conn: Any,
    command: ListRuntimeTargetsCommand,
) -> dict[str, Any]:
    limit = int(command.limit)
    if limit <= 0 or limit > 500:
        raise ServiceLifecycleError(
            "service_lifecycle.invalid_submission",
            "limit must be between 1 and 500",
            details={"field": "limit", "value": limit},
        )
    clauses = [
        """
        SELECT
            runtime_target_ref,
            target_scope,
            substrate_kind,
            display_name,
            workspace_ref,
            base_path_ref,
            host_ref,
            endpoint_contract,
            capability_contract,
            secret_provider_ref,
            enabled,
            decision_ref,
            created_at,
            updated_at
          FROM runtime_targets
         WHERE TRUE
        """
    ]
    params: list[Any] = []
    if command.enabled_only:
        clauses.append("AND enabled = TRUE")
    if command.substrate_kind:
        params.append(normalize_substrate_kind(command.substrate_kind))
        clauses.append(f"AND substrate_kind = ${len(params)}")
    if command.workspace_ref:
        params.append(_require_text(command.workspace_ref, field_name="workspace_ref"))
        clauses.append(f"AND workspace_ref = ${len(params)}")
    if command.target_scope:
        params.append(_require_text(command.target_scope, field_name="target_scope"))
        clauses.append(f"AND target_scope = ${len(params)}")
    params.append(limit)
    clauses.append(f"ORDER BY runtime_target_ref LIMIT ${len(params)}")
    rows = conn.fetch("\n".join(clauses), *params)
    targets = [_row_payload(row) for row in rows or []]
    return {"status": "listed", "targets": targets, "count": len(targets)}


def handle_register_runtime_target(
    command: RegisterRuntimeTargetCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return register_runtime_target(_pg_conn(subsystems), command)


def handle_register_service_definition(
    command: RegisterServiceDefinitionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return register_service_definition(_pg_conn(subsystems), command)


def handle_declare_service_desired_state(
    command: DeclareServiceDesiredStateCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return declare_service_desired_state(_pg_conn(subsystems), command)


def handle_record_service_lifecycle_event(
    command: RecordServiceLifecycleEventCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return record_service_lifecycle_event(_pg_conn(subsystems), command)


def handle_query_service_projection(
    command: QueryServiceProjectionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    projection = get_service_projection(
        _pg_conn(subsystems),
        service_ref=command.service_ref,
        runtime_target_ref=command.runtime_target_ref,
    )
    return {"status": "found", "projection": projection}


def handle_list_runtime_targets(
    command: ListRuntimeTargetsCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return list_runtime_targets(_pg_conn(subsystems), command)


def reduce_service_instance_events(*_args: Any, **_kwargs: Any) -> dict[str, str]:
    """Importable reducer reference for the projection registry.

    The projection is updated synchronously by this authority module today.
    This symbol gives future external reducers a stable contract point instead
    of forcing them to infer projection ownership from table names.
    """

    return {"projection_ref": "projection.service_lifecycle.instances"}


__all__ = [
    "DeclareServiceDesiredStateCommand",
    "ListRuntimeTargetsCommand",
    "RecordServiceLifecycleEventCommand",
    "RegisterRuntimeTargetCommand",
    "RegisterServiceDefinitionCommand",
    "ServiceLifecycleError",
    "declare_service_desired_state",
    "get_service_projection",
    "handle_declare_service_desired_state",
    "handle_list_runtime_targets",
    "handle_query_service_projection",
    "handle_record_service_lifecycle_event",
    "handle_register_runtime_target",
    "handle_register_service_definition",
    "list_runtime_targets",
    "normalize_substrate_kind",
    "record_service_lifecycle_event",
    "reduce_service_instance_events",
    "register_runtime_target",
    "register_service_definition",
]
