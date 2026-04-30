"""Tools: praxis_task_environment_contract_*."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_task_environment_contract_record(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Record one task-environment contract through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Recording task contract {payload.get('contract', {}).get('contract_id') or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="task_environment_contract_record",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - task contract record {status}",
        )
    return result


def tool_praxis_task_environment_contract_read(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Read task-environment contracts through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading task-environment contracts",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="task_environment_contract_read",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - task contract read {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_task_environment_contract_record": (
        tool_praxis_task_environment_contract_record,
        {
            "kind": "write",
            "operation_names": ["task_environment_contract_record"],
            "description": (
                "Record one receipt-backed task-environment contract head and "
                "revision through the CQRS gateway. The payload captures the "
                "contract, deterministic evaluation result, hierarchy nodes, "
                "typed invalid states, dependency hash, and command receipt/event."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["contract", "evaluation_result"],
                "properties": {
                    "contract": {
                        "type": "object",
                        "description": "TaskEnvironmentContract JSON packet.",
                    },
                    "evaluation_result": {
                        "type": "object",
                        "description": "ContractEvaluationResult JSON packet.",
                    },
                    "hierarchy_nodes": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "HierarchyNode JSON records evaluated with the contract.",
                    },
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
            },
            "type_contract": {
                "record_task_environment_contract": {
                    "consumes": [
                        "task_environment.contract",
                        "task_environment.evaluation_result",
                        "task_environment.hierarchy_nodes",
                    ],
                    "produces": [
                        "task_environment.contract_head",
                        "task_environment.contract_revision",
                        "task_environment.typed_invalid_states",
                        "authority_operation_receipt",
                        "authority_event.task_environment_contract.recorded",
                    ],
                }
            },
        },
    ),
    "praxis_task_environment_contract_read": (
        tool_praxis_task_environment_contract_read,
        {
            "kind": "analytics",
            "operation_names": ["task_environment_contract_read"],
            "description": (
                "Read receipt-backed task-environment contract heads, revisions, "
                "hierarchy nodes, and typed invalid states through the CQRS gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["list", "describe"]},
                    "task_ref": {"type": "string"},
                    "status": {"type": "string"},
                    "contract_id": {"type": "string"},
                    "include_history": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                },
            },
            "type_contract": {
                "read_task_environment_contract": {
                    "consumes": [
                        "task_environment.contract_id",
                        "task_environment.task_ref",
                        "task_environment.status",
                    ],
                    "produces": [
                        "task_environment.contract_heads",
                        "task_environment.contract_revisions",
                        "task_environment.hierarchy_nodes",
                        "task_environment.typed_invalid_states",
                    ],
                }
            },
        },
    ),
}
