"""Tools: praxis_integration_action_contract_*."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_integration_action_contract_record(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Record integration action and automation contracts through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        contract_count = len(payload.get("contracts") or [])
        automation_count = len(payload.get("automation_snapshots") or [])
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Recording {contract_count} integration contract(s), {automation_count} automation snapshot(s)",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="integration_action_contract_record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - integration action contract record {status}",
        )
    return result


def tool_praxis_integration_action_contract_read(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Read integration action and automation contracts through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading integration action contracts",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="integration_action_contract_read",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - integration action contract read {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_integration_action_contract_record": (
        tool_praxis_integration_action_contract_record,
        {
            "kind": "write",
            "operation_names": ["integration_action_contract_record"],
            "description": (
                "Record receipt-backed integration action contracts and "
                "automation rule snapshots through the CQRS gateway. The "
                "payload captures versioned behavior, validation gaps, linked "
                "automation actions, hashes, and the command receipt/event."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contracts": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "IntegrationActionContract JSON packets.",
                    },
                    "automation_snapshots": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "AutomationRuleSnapshot JSON packets.",
                    },
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
            "type_contract": {
                "record_integration_action_contract": {
                    "consumes": [
                        "integration_action.contract",
                        "integration_automation.rule_snapshot",
                    ],
                    "produces": [
                        "integration_action.contract_head",
                        "integration_action.contract_revision",
                        "integration_action.typed_gaps",
                        "integration_automation.rule_snapshot_head",
                        "integration_automation.action_links",
                        "authority_operation_receipt",
                        "authority_event.integration_action_contract.recorded",
                    ],
                }
            },
        },
    ),
    "praxis_integration_action_contract_read": (
        tool_praxis_integration_action_contract_read,
        {
            "kind": "analytics",
            "operation_names": ["integration_action_contract_read"],
            "description": (
                "Read receipt-backed integration action contracts, revisions, "
                "automation rule snapshots, linked actions, and typed gaps "
                "through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list_contracts",
                            "describe_contract",
                            "list_automation_snapshots",
                            "describe_automation_snapshot",
                        ],
                    },
                    "action_contract_id": {"type": "string"},
                    "automation_rule_id": {"type": "string"},
                    "target_system_ref": {"type": "string"},
                    "status": {"type": "string"},
                    "owner_ref": {"type": "string"},
                    "include_history": {"type": "boolean"},
                    "include_automation": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                },
            },
            "type_contract": {
                "read_integration_action_contract": {
                    "consumes": [
                        "integration_action.action_contract_id",
                        "integration_automation.automation_rule_id",
                        "integration_action.target_system_ref",
                    ],
                    "produces": [
                        "integration_action.contract_heads",
                        "integration_action.contract_revisions",
                        "integration_action.typed_gaps",
                        "integration_automation.rule_snapshots",
                        "integration_automation.action_links",
                    ],
                }
            },
        },
    ),
}
