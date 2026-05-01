"""Tool: praxis_workflow_repair_queue."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


_READ_ACTIONS = {"list", "queue", "status", "summary"}
_WRITE_ACTIONS = {"claim", "release", "complete"}


def tool_praxis_workflow_repair_queue(params: dict, _progress_emitter=None) -> dict:
    """Inspect or operate the durable Solution/Workflow/Job repair queue."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    action = str(payload.pop("action", "list") or "list").strip().lower()

    if action in _READ_ACTIONS:
        read_payload: dict[str, Any] = {"action": action}
        for key in ("queue_status", "repair_scope", "run_id", "solution_id", "limit"):
            if key in payload:
                read_payload[key] = payload[key]
        if _progress_emitter:
            _progress_emitter.emit(progress=0, total=1, message=f"Reading repair queue {action}")
        result = execute_operation_from_env(
            env=workflow_database_env(),
            operation_name="workflow_repair_queue.status",
            payload=read_payload,
        )
        if _progress_emitter:
            state = str(result.get("status") or ("ok" if result.get("ok") else "failed"))
            _progress_emitter.emit(progress=1, total=1, message=f"Repair queue read {state}")
        return result

    if action in _WRITE_ACTIONS:
        write_payload: dict[str, Any] = {"action": action}
        for key in (
            "repair_scope",
            "claimed_by",
            "claim_ttl_minutes",
            "repair_id",
            "queue_status",
            "result_ref",
            "repair_note",
        ):
            if key in payload:
                write_payload[key] = payload[key]
        if _progress_emitter:
            _progress_emitter.emit(progress=0, total=1, message=f"Repair queue {action}")
        result = execute_operation_from_env(
            env=workflow_database_env(),
            operation_name="workflow_repair_queue.command",
            payload=write_payload,
        )
        if _progress_emitter:
            state = str(result.get("status") or ("ok" if result.get("ok") else "failed"))
            _progress_emitter.emit(progress=1, total=1, message=f"Repair queue {action} {state}")
        return result

    return {
        "ok": False,
        "error_code": "workflow_repair_queue.unknown_action",
        "error": f"Unknown repair queue action {action!r}",
        "allowed_actions": sorted(_READ_ACTIONS | _WRITE_ACTIONS),
    }


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_workflow_repair_queue": (
        tool_praxis_workflow_repair_queue,
        {
            "kind": "write",
            "operation_names": [
                "workflow_repair_queue.status",
                "workflow_repair_queue.command",
            ],
            "description": (
                "Inspect and operate the durable repair queue for failed Solutions, "
                "Workflows, and Jobs. Failed terminal workflow state auto-enqueues "
                "repair intents; this tool reads, claims, releases, and closes those "
                "intents through CQRS receipts and events."
            ),
            "cli": {
                "surface": "workflow",
                "tier": "advanced",
                "recommended_alias": "repair",
                "when_to_use": (
                    "Use after a Solution, Workflow, or Job fails and you need a "
                    "durable repair item instead of rediscovering lost run state."
                ),
                "when_not_to_use": (
                    "Do not use it to launch fresh workflow work; use praxis_workflow "
                    "or praxis_solution for execution."
                ),
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Summarize repair backlog", "input": {"action": "summary"}},
                    {
                        "title": "List queued repairs for a run",
                        "input": {"action": "list", "run_id": "workflow_<id>"},
                    },
                ],
            },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list",
                            "queue",
                            "status",
                            "summary",
                            "claim",
                            "release",
                            "complete",
                        ],
                        "default": "list",
                    },
                    "queue_status": {
                        "type": "string",
                        "description": (
                            "Status filter for list/status actions, or terminal "
                            "status for complete."
                        ),
                    },
                    "repair_scope": {
                        "type": "string",
                        "enum": ["solution", "workflow", "job"],
                        "description": "Optional repair scope filter.",
                    },
                    "run_id": {"type": "string"},
                    "solution_id": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                    "claimed_by": {"type": "string"},
                    "claim_ttl_minutes": {"type": "integer", "default": 30},
                    "repair_id": {"type": "string"},
                    "result_ref": {"type": "string"},
                    "repair_note": {"type": "string"},
                },
                "required": ["action"],
            },
        },
    ),
}
