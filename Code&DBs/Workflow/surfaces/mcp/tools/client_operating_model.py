"""Tools: praxis_client_operating_model."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_client_operating_model(params: dict, _progress_emitter=None) -> dict:
    """Build one Client Operating Model operator read model through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Building client operating model view {payload.get('view') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="client_operating_model_operator_view",
        payload=payload,
    )
    if _progress_emitter:
        state = result.get("state") or ("ok" if result.get("ok") else "failed")
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - client operating model view {state}",
        )
    return result


def tool_praxis_client_operating_model_snapshot_store(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Persist one Client Operating Model operator-view snapshot through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Persisting client operating model snapshot",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="client_operating_model_operator_view_snapshot_store",
        payload=payload,
    )
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - client operating model snapshot store {state}",
        )
    return result


def tool_praxis_client_operating_model_snapshots(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Read stored Client Operating Model operator-view snapshots through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading client operating model snapshots",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="client_operating_model_operator_view_snapshot_read",
        payload=payload,
    )
    if _progress_emitter:
        state = result.get("count", "unknown")
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - client operating model snapshots {state}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_client_operating_model": (
        tool_praxis_client_operating_model,
        {
            "kind": "analytics",
            "operation_names": ["client_operating_model_operator_view"],
            "description": (
                "Build one read-only Client Operating Model operator view through "
                "the CQRS gateway. Views include system census, Object Truth "
                "inspection, identity/source authority, simulation timeline, "
                "verifier results, sandbox drift, cartridge status, managed "
                "runtime accounting, next safe actions, and workflow-builder "
                "validation, plus Workflow Context customer-facing composite "
                "readouts. The tool normalizes provided evidence into an "
                "operator read model; it does not persist, mutate, or call live "
                "client systems."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["view"],
                "properties": {
                    "view": {
                        "type": "string",
                        "enum": [
                            "system_census",
                            "object_truth",
                            "identity_authority",
                            "simulation_timeline",
                            "verifier_results",
                            "sandbox_drift",
                            "cartridge_status",
                            "managed_runtime",
                            "next_safe_actions",
                            "workflow_builder_validation",
                            "workflow_context_composite",
                        ],
                        "description": "Operator view to build.",
                    },
                    "inputs": {
                        "type": "object",
                        "description": "View-specific evidence payload.",
                    },
                    "generated_at": {
                        "type": "string",
                        "description": "Optional deterministic generated timestamp.",
                    },
                    "permission_scope": {
                        "type": "object",
                        "description": "Permission scope and redaction controls.",
                    },
                    "correlation_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "evidence_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
            "type_contract": {
                "operator_view": {
                    "consumes": [
                        "client_operating_model.view",
                        "client_operating_model.evidence_payload",
                        "client_operating_model.permission_scope",
                    ],
                    "produces": [
                        "client_operating_model.operator_view",
                        "client_operating_model.freshness",
                        "client_operating_model.safe_action_state",
                    ],
                }
            },
        },
    ),
    "praxis_client_operating_model_snapshot_store": (
        tool_praxis_client_operating_model_snapshot_store,
        {
            "kind": "write",
            "operation_names": ["client_operating_model_operator_view_snapshot_store"],
            "description": (
                "Persist one Client Operating Model operator-view snapshot through "
                "the CQRS gateway for historical readback. This stores the already "
                "built operator_view payload; it does not call client systems."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["operator_view"],
                "properties": {
                    "operator_view": {
                        "type": "object",
                        "description": "operator_view payload returned by praxis_client_operating_model.",
                    },
                    "view": {
                        "type": "string",
                        "description": "Optional explicit view name.",
                    },
                    "observed_by_ref": {
                        "type": "string",
                    },
                    "source_ref": {
                        "type": "string",
                    },
                },
            },
        },
    ),
    "praxis_client_operating_model_snapshots": (
        tool_praxis_client_operating_model_snapshots,
        {
            "kind": "analytics",
            "operation_names": ["client_operating_model_operator_view_snapshot_read"],
            "description": (
                "Read stored Client Operating Model operator-view snapshots by "
                "snapshot ref, digest, view, or scope through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "snapshot_ref": {
                        "type": "string",
                    },
                    "snapshot_digest": {
                        "type": "string",
                    },
                    "view": {
                        "type": "string",
                        "enum": [
                            "system_census",
                            "object_truth",
                            "identity_authority",
                            "simulation_timeline",
                            "verifier_results",
                            "sandbox_drift",
                            "cartridge_status",
                            "managed_runtime",
                            "next_safe_actions",
                            "workflow_builder_validation",
                            "workflow_context_composite",
                        ],
                    },
                    "scope_ref": {
                        "type": "string",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                    },
                },
            },
        },
    ),
}
