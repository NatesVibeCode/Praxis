"""Tools: praxis_client_system_discovery."""
from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def _payload(params: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in dict(params or {}).items() if value is not None}


def _execute(operation_name: str, params: dict[str, Any]) -> dict[str, Any]:
    return execute_operation_from_env(
        env=workflow_database_env(),
        operation_name=operation_name,
        payload=_payload(params),
    )


def tool_praxis_client_system_discovery_census_record(
    params: dict[str, Any],
    _progress_emitter=None,
) -> dict[str, Any]:
    """Persist one client-system census through the CQRS gateway."""

    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Recording client-system census {dict(params or {}).get('system_slug') or '?'}",
        )
    result = _execute("client_system_discovery_census_record", params)
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - client-system census record {state}",
        )
    return result


def tool_praxis_client_system_discovery_census_read(
    params: dict[str, Any],
    _progress_emitter=None,
) -> dict[str, Any]:
    """Read client-system census authority through the CQRS gateway."""

    payload = _payload(params)
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Reading client-system census {payload.get('action') or 'list'}",
        )
    result = _execute("client_system_discovery_census_read", payload)
    if _progress_emitter:
        state = result.get("count", "done")
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - client-system census read {state}",
        )
    return result


def tool_praxis_client_system_discovery_gap_record(
    params: dict[str, Any],
    _progress_emitter=None,
) -> dict[str, Any]:
    """Record one typed client-system discovery gap through the CQRS gateway."""

    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Recording client-system discovery gap",
        )
    result = _execute("client_system_discovery_gap_record", params)
    if _progress_emitter:
        state = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - client-system discovery gap {state}",
        )
    return result


def tool_praxis_client_system_discovery(params: dict[str, Any]) -> dict[str, Any]:
    action = str(params.get("action") or "list").strip().lower()

    if action == "discover":
        return tool_praxis_client_system_discovery_census_record(params)
    if action in {"list", "search", "describe"}:
        return tool_praxis_client_system_discovery_census_read(params)
    if action == "record_gap":
        return tool_praxis_client_system_discovery_gap_record(params)
    return {
        "error": (
            "Unknown action: "
            f"{action}. Use 'discover', 'list', 'search', 'describe', or 'record_gap'."
        )
    }


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_client_system_discovery": (
        tool_praxis_client_system_discovery,
        {
            "kind": "write",
            "operation_names": [
                "client_system_discovery_census_record",
                "client_system_discovery_census_read",
                "client_system_discovery_gap_record",
            ],
            "description": (
                "Persist or query client system discovery authority: typed system census rows, "
                "connector surface evidence, credential-health references, and typed discovery gaps. "
                "This compatibility wrapper dispatches to the CQRS gateway operations for "
                "census record/read and gap record."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["discover", "list", "search", "describe", "record_gap"],
                        "default": "list",
                    },
                    "tenant_ref": {"type": "string"},
                    "workspace_ref": {"type": "string"},
                    "system_slug": {"type": "string"},
                    "system_name": {"type": "string"},
                    "discovery_source": {"type": "string"},
                    "captured_at": {"type": "string"},
                    "status": {"type": "string"},
                    "metadata": {"type": "object"},
                    "connectors": {"type": "array", "items": {"type": "object"}},
                    "query": {"type": "string"},
                    "limit": {"type": "integer"},
                    "census_id": {"type": "string"},
                    "gap_kind": {"type": "string"},
                    "reason_code": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "detail": {"type": "string"},
                    "legal_repair_actions": {"type": "array", "items": {"type": "string"}},
                    "context": {"type": "object"},
                },
            },
        },
    ),
    "praxis_client_system_discovery_census_record": (
        tool_praxis_client_system_discovery_census_record,
        {
            "kind": "write",
            "operation_names": ["client_system_discovery_census_record"],
            "description": (
                "Persist one client-system census record and connector evidence through "
                "the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["tenant_ref", "workspace_ref", "system_slug", "captured_at"],
                "properties": {
                    "tenant_ref": {"type": "string"},
                    "workspace_ref": {"type": "string"},
                    "system_slug": {"type": "string"},
                    "system_name": {"type": "string"},
                    "discovery_source": {"type": "string"},
                    "captured_at": {"type": "string"},
                    "status": {"type": "string"},
                    "census_id": {"type": "string"},
                    "category": {"type": "string"},
                    "vendor": {"type": "string"},
                    "deployment_model": {"type": "string"},
                    "environment": {"type": "string"},
                    "business_owner": {"type": "string"},
                    "technical_owner": {"type": "string"},
                    "criticality": {"type": "string"},
                    "declared_purpose": {"type": "string"},
                    "discovery_status": {"type": "string"},
                    "last_verified_at": {"type": "string"},
                    "metadata": {"type": "object"},
                    "integrations": {"type": "array", "items": {"type": "object"}},
                    "connectors": {"type": "array", "items": {"type": "object"}},
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
        },
    ),
    "praxis_client_system_discovery_census_read": (
        tool_praxis_client_system_discovery_census_read,
        {
            "kind": "analytics",
            "operation_names": ["client_system_discovery_census_read"],
            "description": (
                "Read client-system census records by list, search, or describe "
                "through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "search", "describe"],
                        "default": "list",
                    },
                    "tenant_ref": {"type": "string"},
                    "query": {"type": "string"},
                    "census_id": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
            },
        },
    ),
    "praxis_client_system_discovery_gap_record": (
        tool_praxis_client_system_discovery_gap_record,
        {
            "kind": "write",
            "operation_names": ["client_system_discovery_gap_record"],
            "description": (
                "Record one typed client-system discovery gap as a receipt-backed "
                "gateway event."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["reason_code", "source_ref", "detail"],
                "properties": {
                    "gap_kind": {"type": "string"},
                    "reason_code": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "detail": {"type": "string"},
                    "severity": {"type": "string"},
                    "is_blocker": {"type": "boolean"},
                    "expected_evidence": {"type": "string"},
                    "current_evidence": {"type": "string"},
                    "next_action": {"type": "string"},
                    "owner": {"type": "string"},
                    "opened_at": {"type": "string"},
                    "resolved_at": {"type": "string"},
                    "legal_repair_actions": {"type": "array", "items": {"type": "string"}},
                    "context": {"type": "object"},
                    "gap_id": {"type": "string"},
                },
            },
        },
    ),
}
