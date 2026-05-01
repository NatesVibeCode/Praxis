"""Tool: praxis_solution."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


_SUBMIT_ACTIONS = {"submit", "start"}
_STATUS_ACTIONS = {"status", "show", "list", "observe"}


def _solutionize_submit_response(result: dict[str, Any]) -> dict[str, Any]:
    """Translate workflow-chain submit output into Solution language."""

    payload = dict(result)
    chain_id = payload.pop("chain_id", None)
    if chain_id:
        payload["solution_id"] = chain_id
        payload["storage_authority"] = "workflow_chain"
    if "program" in payload:
        payload["solution_name"] = payload.get("program")
    payload.pop("current_wave", None)
    payload.pop("waves_total", None)
    payload.pop("waves_completed", None)
    payload["authority"] = "workflow_solution"
    payload["object_kind"] = "workflow_solution"
    return payload


def tool_praxis_solution(params: dict, _progress_emitter=None) -> dict:
    """Coordinate durable multi-workflow Solutions through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    action = str(payload.pop("action", "status") or "status").strip().lower()

    if action in _SUBMIT_ACTIONS:
        coordination_path = str(payload.get("coordination_path") or "").strip()
        if not coordination_path:
            return {
                "ok": False,
                "error_code": "workflow_solution.coordination_path.required",
                "error": "coordination_path is required for solution submit/start",
            }
        submit_payload = {
            "coordination_path": coordination_path,
            "adopt_active": bool(payload.get("adopt_active", True)),
            "requested_by_kind": str(payload.get("requested_by_kind") or "mcp"),
            "requested_by_ref": str(payload.get("requested_by_ref") or "praxis_solution.submit"),
        }
        if _progress_emitter:
            _progress_emitter.emit(
                progress=0,
                total=1,
                message=f"Submitting Solution from {coordination_path}",
            )
        result = execute_operation_from_env(
            env=workflow_database_env(),
            operation_name="workflow_solution.submit",
            payload=submit_payload,
        )
        if _progress_emitter:
            state = str(result.get("status") or ("ok" if result.get("ok") else "failed"))
            _progress_emitter.emit(progress=1, total=1, message=f"Solution submit {state}")
        return _solutionize_submit_response(result if isinstance(result, dict) else {})

    if action in _STATUS_ACTIONS:
        status_payload: dict[str, Any] = {}
        for key in ("solution_id", "limit"):
            if key in payload:
                status_payload[key] = payload[key]
        if _progress_emitter:
            _progress_emitter.emit(progress=0, total=1, message=f"Reading Solution {action}")
        result = execute_operation_from_env(
            env=workflow_database_env(),
            operation_name="workflow_solution.status",
            payload=status_payload,
        )
        if _progress_emitter:
            state = str(result.get("status") or ("ok" if result.get("ok") else "failed"))
            _progress_emitter.emit(progress=1, total=1, message=f"Solution read {state}")
        return result

    return {
        "ok": False,
        "error_code": "workflow_solution.unknown_action",
        "error": f"Unknown Solution action {action!r}",
        "allowed_actions": sorted(_SUBMIT_ACTIONS | _STATUS_ACTIONS),
    }


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_solution": (
        tool_praxis_solution,
        {
            "kind": "write",
            "operation_names": ["workflow_solution.submit", "workflow_solution.status"],
            "description": (
                "Coordinate durable Solutions: one Solution is a coordinated answer "
                "under proof, backed by one or more workflow executions. This is "
                "the replacement for legacy multi-workflow batch coordination.\n\n"
                "USE WHEN: you need to submit, list, or inspect a multi-workflow "
                "Solution with durable DB authority, receipts, and attached "
                "workflow runs.\n\n"
                "DO NOT USE: for a single workflow run; use praxis_workflow for that."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: submit/start, status/show, list/observe.",
                        "enum": ["submit", "start", "status", "show", "list", "observe"],
                        "default": "status",
                    },
                    "coordination_path": {
                        "type": "string",
                        "description": "Path to a Solution coordination JSON file. Required for submit/start.",
                    },
                    "solution_id": {
                        "type": "string",
                        "description": "Solution identifier for status/show.",
                    },
                    "adopt_active": {
                        "type": "boolean",
                        "description": "Adopt active matching workflow runs on submit/start.",
                        "default": True,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum Solutions to list.",
                        "default": 20,
                    },
                },
                "required": ["action"],
            },
        },
    ),
}
