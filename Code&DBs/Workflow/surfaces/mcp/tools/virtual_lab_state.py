"""Tools: praxis_virtual_lab_state_*."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_virtual_lab_state_record(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Record Virtual Lab state through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Recording Virtual Lab state packet",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="virtual_lab_state_record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - Virtual Lab state record {status}",
        )
    return result


def tool_praxis_virtual_lab_state_read(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Read Virtual Lab state through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading Virtual Lab state",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="virtual_lab_state_read",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - Virtual Lab state read {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_virtual_lab_state_record": (
        tool_praxis_virtual_lab_state_record,
        {
            "kind": "write",
            "operation_names": ["virtual_lab_state_record"],
            "description": (
                "Record receipt-backed Virtual Lab environment revisions, "
                "copy-on-write object state projections, event envelopes, "
                "command receipts, replay validation, and typed gaps through "
                "the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "environment_revision": {
                        "type": "object",
                        "description": "EnvironmentRevision JSON packet from runtime.virtual_lab.state.",
                    },
                    "object_states": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "ObjectStateRecord JSON packets.",
                    },
                    "events": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "EventEnvelope JSON packets.",
                    },
                    "command_receipts": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "CommandReceipt JSON packets.",
                    },
                    "typed_gaps": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Typed Virtual Lab validation gaps.",
                    },
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
                "required": ["environment_revision"],
            },
            "type_contract": {
                "record_virtual_lab_state": {
                    "consumes": [
                        "virtual_lab.environment_revision",
                        "virtual_lab.object_state",
                        "virtual_lab.event_envelope",
                        "virtual_lab.command_receipt",
                    ],
                    "produces": [
                        "virtual_lab.environment_head",
                        "virtual_lab.environment_revision",
                        "virtual_lab.object_state_projection",
                        "virtual_lab.event_store",
                        "virtual_lab.command_receipts",
                        "authority_operation_receipt",
                        "authority_event.virtual_lab_state.recorded",
                    ],
                }
            },
        },
    ),
    "praxis_virtual_lab_state_read": (
        tool_praxis_virtual_lab_state_read,
        {
            "kind": "analytics",
            "operation_names": ["virtual_lab_state_read"],
            "description": (
                "Read receipt-backed Virtual Lab environment revisions, object "
                "state projections, event streams, command receipts, and typed "
                "gaps through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list_environments",
                            "list_revisions",
                            "describe_revision",
                            "list_events",
                            "list_receipts",
                        ],
                    },
                    "environment_id": {"type": "string"},
                    "revision_id": {"type": "string"},
                    "stream_id": {"type": "string"},
                    "event_type": {"type": "string"},
                    "status": {"type": "string"},
                    "include_seed": {"type": "boolean"},
                    "include_objects": {"type": "boolean"},
                    "include_events": {"type": "boolean"},
                    "include_receipts": {"type": "boolean"},
                    "include_typed_gaps": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
            },
            "type_contract": {
                "read_virtual_lab_state": {
                    "consumes": [
                        "virtual_lab.environment_id",
                        "virtual_lab.revision_id",
                        "virtual_lab.stream_id",
                    ],
                    "produces": [
                        "virtual_lab.environment_heads",
                        "virtual_lab.environment_revisions",
                        "virtual_lab.object_states",
                        "virtual_lab.events",
                        "virtual_lab.command_receipts",
                        "virtual_lab.typed_gaps",
                    ],
                }
            },
        },
    ),
}
