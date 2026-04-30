"""Tools: praxis_authority_managed_runtime_*."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_authority_managed_runtime_record(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Record managed-runtime accounting, receipts, health, and observability through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Recording managed runtime accounting",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="authority.managed_runtime.record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - managed runtime record {status}",
        )
    return result


def tool_praxis_authority_managed_runtime_read(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Read managed-runtime accounting, receipts, health, and observability through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading managed runtime accounting",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="authority.managed_runtime.read",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - managed runtime read {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_authority_managed_runtime_record": (
        tool_praxis_authority_managed_runtime_record,
        {
            "kind": "write",
            "operation_names": ["authority.managed_runtime.record"],
            "description": (
                "Record optional managed/exported/hybrid runtime accounting "
                "snapshots, metering, run receipts, pricing schedule refs, "
                "heartbeat health, internal audit, and customer-safe "
                "observability through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "identity": {"type": "object"},
                    "policy": {"type": "object"},
                    "meter_events": {"type": "array", "items": {"type": "object"}},
                    "terminal_status": {
                        "type": "string",
                        "enum": ["succeeded", "failed", "cancelled"],
                    },
                    "generated_at": {"type": "string", "format": "date-time"},
                    "runtime_version_ref": {"type": "string"},
                    "requested_mode": {
                        "type": "string",
                        "enum": ["managed", "exported", "hybrid"],
                    },
                    "runtime_pool_ref": {"type": "string"},
                    "pricing_schedule": {"type": "object"},
                    "cost_status": {
                        "type": "string",
                        "enum": ["estimated", "provisional", "finalized", "not_applicable"],
                    },
                    "heartbeats": {"type": "array", "items": {"type": "object"}},
                    "pool_ref": {"type": "string"},
                    "heartbeat_fresh_seconds": {"type": "integer", "minimum": 1},
                    "unavailable_after_seconds": {"type": "integer", "minimum": 1},
                    "clock_skew_grace_seconds": {"type": "integer", "minimum": 0},
                    "audit_events": {"type": "array", "items": {"type": "object"}},
                    "error_classification": {"type": "string"},
                    "execution_labels": {"type": "array", "items": {"type": "string"}},
                    "correction_of_receipt_id": {"type": "string"},
                    "runtime_record_id": {"type": "string"},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "require_dispatch_allowed": {"type": "boolean"},
                },
                "required": [
                    "identity",
                    "policy",
                    "meter_events",
                    "terminal_status",
                    "generated_at",
                    "runtime_version_ref",
                ],
            },
            "type_contract": {
                "record_managed_runtime": {
                    "consumes": [
                        "managed_runtime.identity",
                        "managed_runtime.mode_policy",
                        "managed_runtime.meter_events",
                        "managed_runtime.pricing_schedule",
                        "managed_runtime.heartbeats",
                    ],
                    "produces": [
                        "managed_runtime.record",
                        "managed_runtime.run_receipt",
                        "managed_runtime.customer_observability",
                        "authority_operation_receipt",
                        "authority_event.managed_runtime.recorded",
                    ],
                }
            },
        },
    ),
    "praxis_authority_managed_runtime_read": (
        tool_praxis_authority_managed_runtime_read,
        {
            "kind": "analytics",
            "operation_names": ["authority.managed_runtime.read"],
            "description": (
                "Read persisted managed-runtime run receipts, metering, "
                "cost, heartbeat health, audit events, pricing schedules, "
                "and customer observability through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list_records",
                            "describe_record",
                            "list_meter_events",
                            "list_heartbeats",
                            "list_pool_health",
                            "list_audit_events",
                            "list_pricing_schedules",
                        ],
                    },
                    "runtime_record_id": {"type": "string"},
                    "run_id": {"type": "string"},
                    "receipt_id": {"type": "string"},
                    "tenant_ref": {"type": "string"},
                    "environment_ref": {"type": "string"},
                    "workflow_ref": {"type": "string"},
                    "execution_mode": {"type": "string"},
                    "configured_mode": {"type": "string"},
                    "terminal_status": {"type": "string"},
                    "cost_status": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "event_kind": {"type": "string"},
                    "pool_ref": {"type": "string"},
                    "worker_ref": {"type": "string"},
                    "health_state": {"type": "string"},
                    "schedule_ref": {"type": "string"},
                    "pricing_schedule_version_ref": {"type": "string"},
                    "include_meter_events": {"type": "boolean"},
                    "include_heartbeats": {"type": "boolean"},
                    "include_pool_health": {"type": "boolean"},
                    "include_audit_events": {"type": "boolean"},
                    "include_pricing_schedule": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
            },
            "type_contract": {
                "read_managed_runtime": {
                    "consumes": [
                        "managed_runtime.record_id",
                        "managed_runtime.run_filter",
                        "managed_runtime.health_filter",
                    ],
                    "produces": [
                        "managed_runtime.records",
                        "managed_runtime.meter_events",
                        "managed_runtime.heartbeats",
                        "managed_runtime.pool_health",
                        "managed_runtime.customer_observability",
                    ],
                }
            },
        },
    ),
}
