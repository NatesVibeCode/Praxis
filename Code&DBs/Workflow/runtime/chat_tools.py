"""Chat workspace tools — callable by the LLM during conversations.

Each tool returns structured data that the frontend renders as
interactive components (tables, cards, status indicators).
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from runtime.control_commands import (
    ControlCommandType,
    ControlIntent,
    bootstrap_control_commands_schema,
    request_control_command,
    workflow_retry_idempotency_key,
    workflow_retry_payload_with_guard,
)
from runtime.idempotency import canonical_hash
from runtime.operation_catalog_gateway import (
    _EnvironmentBackedSubsystems,
    execute_operation_from_subsystems,
)

_log = logging.getLogger(__name__)


def _dispatch_op(pg_conn: Any, operation_name: str, payload: dict[str, Any]) -> Any:
    """Dispatch a registered CQRS operation through the gateway.

    Wraps the chat orchestrator's ``pg_conn`` in the subsystems shape the
    gateway expects, so chat tools can call any registered operation
    without bypassing receipt + event recording.
    """
    subsystems = _EnvironmentBackedSubsystems(env=os.environ, conn=pg_conn)
    return execute_operation_from_subsystems(
        subsystems,
        operation_name=operation_name,
        payload=payload,
    )


# Discriminator the App's moonChatContext.ts writes into selection_context.
MOON_CONTEXT_KIND = "moon_context"


def _extract_moon_context(selection_context: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    """Pull the moon-context entry (if any) out of the selection_context list.

    The frontend's ``moonChatSelectionContext()`` packs a single
    ``{kind: 'moon_context', workflow_id, selected_node_id, ...}`` entry.
    Returning ``None`` here means there is no active Moon workflow context
    and tools must require explicit ``workflow_id`` arguments.
    """
    if not selection_context:
        return None
    for entry in selection_context:
        if isinstance(entry, dict) and entry.get("kind") == MOON_CONTEXT_KIND:
            return entry
    return None


# ---------------------------------------------------------------------------
# Tool definitions (sent to the LLM as function schemas)
# ---------------------------------------------------------------------------

CHAT_TOOLS: list[dict[str, Any]] = [
    {
        "name": "search_knowledge",
        "description": "Search the platform knowledge graph for entities, decisions, documents, topics.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What to search for"},
                "entity_type": {"type": "string", "description": "Optional filter: person, topic, decision, document"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "query_workflows",
        "description": "Query recent workflow runs — find recent, failed, succeeded, or running workflows.",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "Filter: all, running, succeeded, failed, queued"},
                "limit": {"type": "integer", "description": "Max results (default 20)"},
                "search": {"type": "string", "description": "Search term for spec name or job labels"},
            },
        },
    },
    {
        "name": "run_workflow",
        "description": "Submit a workflow through the command bus for automated execution. Provide either a spec_path to a saved .queue.json file, or a jobs array for an ad-hoc pipeline. Safe submissions auto-execute; the tool returns command metadata either way.",
        "input_schema": {
            "type": "object",
            "properties": {
                "spec_path": {"type": "string", "description": "Path to a .queue.json spec file (relative to repo root)"},
                "name": {"type": "string", "description": "Workflow name (for ad-hoc)"},
                "jobs": {
                    "type": "array",
                    "description": "Jobs for ad-hoc pipeline",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                            "agent": {"type": "string", "description": "e.g. auto/build, auto/review"},
                            "prompt": {"type": "string"},
                            "depends_on": {"type": "array", "items": {"type": "string"}},
                        },
                    },
                },
                "items": {"type": "array", "description": "Selected items to process (from user selection)"},
                "objective": {"type": "string", "description": "What the workflow should accomplish"},
            },
        },
    },
    {
        "name": "list_workflows",
        "description": "List available saved workflow templates (.queue.json files).",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "check_workflow_status",
        "description": "Check the status of a running or completed workflow run.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "The workflow run ID"},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "query_platform_data",
        "description": "Query platform data using predefined report types. Available reports: recent_failures, cost_summary, active_runs, model_performance, job_history.",
        "input_schema": {
            "type": "object",
            "properties": {
                "report": {"type": "string", "description": "Report type: recent_failures, cost_summary, active_runs, model_performance, job_history"},
                "time_range": {"type": "string", "description": "Time range: 1h, 6h, 24h, 7d, 30d (default 24h)"},
                "limit": {"type": "integer", "description": "Max results (default 20)"},
                "filter": {"type": "string", "description": "Optional filter (model name, status, etc.)"},
            },
            "required": ["report"],
        },
    },
    {
        "name": "retry_job",
        "description": "Request a workflow.retry control command for a specific failed job. The command may require approval before execution.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "The workflow run ID"},
                "label": {"type": "string", "description": "The job label to retry"},
                "previous_failure": {"type": "string", "description": "Receipt-backed failure being retried"},
                "retry_delta": {"type": "string", "description": "What is materially different about this attempt"},
                "model_override": {"type": "string", "description": "Optional: use a different model (e.g., anthropic/claude-opus-4-7)"},
            },
            "required": ["run_id", "label", "previous_failure", "retry_delta"],
        },
    },
    {
        "name": "get_job_output",
        "description": "Get the full output/result of a specific completed job in a workflow run.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "The workflow run ID"},
                "label": {"type": "string", "description": "The job label (step name)"},
                "job_id": {"type": "integer", "description": "The job ID (alternative to run_id+label)"},
            },
        },
    },
    {
        "name": "cancel_workflow",
        "description": "Request a workflow.cancel control command for a running workflow. The command may require approval before execution.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "The workflow run ID to cancel"},
            },
            "required": ["run_id"],
        },
    },
    # ------------------------------------------------------------------
    # Moon graph authoring tools — let the LLM read/edit the workflow
    # graph the user is composing in the Moon canvas. All four dispatch
    # through the CQRS gateway so each call leaves a receipt + (for
    # commands) authority_event row.
    # ------------------------------------------------------------------
    {
        "name": "moon_get_build",
        "description": "Load the current Moon BuildPayload for a workflow — every node, edge, gate, contract, outcome, and any compile/binding issues. Use this BEFORE proposing edits so you reason about real graph state, not an assumed one.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workflow_id": {"type": "string", "description": "Workflow id to load"},
            },
            "required": ["workflow_id"],
        },
    },
    {
        "name": "moon_compose_from_prose",
        "description": "Generate a complete Moon workflow graph from a natural-language description. One LLM synthesis pass + N parallel author calls produces nodes, edges, contracts, and gates. Use when the user describes what they want from scratch. Returns the composed plan; pair with moon_get_build to inspect what was created.",
        "input_schema": {
            "type": "object",
            "properties": {
                "intent": {"type": "string", "description": "Plain-language description of what the workflow should do"},
                "plan_name": {"type": "string", "description": "Optional friendly name for the plan"},
                "why": {"type": "string", "description": "Optional rationale recorded with the plan"},
                "concurrency": {"type": "integer", "description": "Parallel author count (1-100, default 20)"},
            },
            "required": ["intent"],
        },
    },
    {
        "name": "moon_mutate_field",
        "description": "Edit any field on a node, edge, or workflow-level contract. The subpath identifies WHAT to change (e.g. 'append', 'nodes/{node_id}', 'edges/{edge_id}/release', 'outcome', 'bootstrap'); the body carries the new value. Use after moon_get_build so you target real ids. This is the single universal mutation entrypoint.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workflow_id": {"type": "string", "description": "Workflow being edited"},
                "subpath": {"type": "string", "description": "Mutation subpath (e.g. 'append', 'nodes/{id}', 'edges/{id}/release', 'outcome', 'bootstrap')"},
                "body": {"type": "object", "description": "Mutation body — shape depends on subpath"},
            },
            "required": ["workflow_id", "subpath", "body"],
        },
    },
    {
        "name": "moon_suggest_next",
        "description": "Ask the graph what nodes are LEGAL to add next given the current accumulator types. Returns ranked likely_next_steps + possible_next_steps + blocked_next_steps. Use when the user asks 'what now' or you need to narrow a 100-tool decision space to 3-5.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workflow_id": {"type": "string", "description": "Workflow being authored"},
                "node_id": {"type": "string", "description": "Optional anchor node to suggest from (default: latest)"},
            },
            "required": ["workflow_id"],
        },
    },
    {
        "name": "moon_launch",
        "description": "Launch the composed workflow as a real run through the gateway. Returns run_id + tracking handle. Only call after the user confirms or after compose+edits have produced a coherent graph.",
        "input_schema": {
            "type": "object",
            "properties": {
                "workflow_id": {"type": "string", "description": "Workflow to launch"},
                "approved_by": {"type": "string", "description": "Operator ref recording who approved this launch"},
                "plan_name": {"type": "string", "description": "Optional friendly name"},
            },
            "required": ["workflow_id"],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool executor
# ---------------------------------------------------------------------------

_ALLOWED_TABLES = frozenset({
    "workflow_runs", "workflow_jobs", "workflow_job_edges",
    "conversations", "conversation_messages",
    "receipts", "provider_model_candidates",
    "task_type_routing", "object_types", "objects",
})

_CHAT_REQUESTED_BY_KIND = "chat"
_CHAT_REQUESTED_BY_REF = "chat.workspace"


def _workflow_run_id_from_result_ref(result_ref: str | None) -> str | None:
    if not result_ref:
        return None
    if not result_ref.startswith("workflow_run:"):
        return None
    return result_ref.split(":", 1)[1]


def _workflow_spec_name_from_path(spec_path: str) -> str:
    name = Path(spec_path).name
    if name.endswith(".queue.json"):
        return name.removesuffix(".queue.json")
    return Path(name).stem or "workflow"


def _build_inline_workflow_spec(args: dict[str, Any]) -> dict[str, Any] | None:
    raw_jobs = args.get("jobs")
    if not isinstance(raw_jobs, list) or not raw_jobs:
        return None

    jobs = [dict(job) for job in raw_jobs if isinstance(job, dict)]
    if not jobs:
        return None

    objective = str(args.get("objective") or "Run ad hoc workflow jobs").strip()
    name = str(args.get("name") or "Ad hoc workflow").strip() or "Ad hoc workflow"
    spec = {
        "name": name,
        "objective": objective,
        "outcome_goal": objective,
        "phase": str(args.get("phase") or "build").strip() or "build",
        "jobs": jobs,
    }
    return spec


def _workflow_command_idempotency_key(command_type: ControlCommandType, arguments: dict[str, Any]) -> str:
    payload = {
        "command_type": command_type.value,
        "requested_by_kind": _CHAT_REQUESTED_BY_KIND,
        "requested_by_ref": _CHAT_REQUESTED_BY_REF,
        "arguments": arguments,
    }
    return f"chat.{command_type.value}.{canonical_hash(payload)[:24]}"


def _request_workflow_command(
    pg_conn: Any,
    *,
    command_type: ControlCommandType,
    arguments: dict[str, Any],
    target: str,
    spec_name: str | None = None,
) -> dict[str, Any]:
    command_payload = dict(arguments)
    idempotency_key = _workflow_command_idempotency_key(command_type, command_payload)
    if command_type == ControlCommandType.WORKFLOW_RETRY:
        command_payload = workflow_retry_payload_with_guard(pg_conn, command_payload)
        idempotency_key = workflow_retry_idempotency_key(
            requested_by_kind=_CHAT_REQUESTED_BY_KIND,
            payload=command_payload,
        )
    intent = ControlIntent(
        command_type=command_type,
        requested_by_kind=_CHAT_REQUESTED_BY_KIND,
        requested_by_ref=_CHAT_REQUESTED_BY_REF,
        idempotency_key=idempotency_key,
        payload=command_payload,
    )
    command = request_control_command(pg_conn, intent)
    return _render_workflow_command_result(
        command,
        action=command_type.value,
        arguments=arguments,
        target=target,
        spec_name=spec_name,
    )


def _render_workflow_command_result(
    command: Any,
    *,
    action: str,
    arguments: dict[str, Any],
    target: str,
    spec_name: str | None = None,
) -> dict[str, Any]:
    command_json = command.to_json() if hasattr(command, "to_json") else dict(command)
    command_status = str(getattr(command, "command_status", command_json.get("command_status", "")))
    command_type = str(getattr(command, "command_type", command_json.get("command_type", action)))
    risk_level = str(getattr(command, "risk_level", command_json.get("risk_level", "")))
    command_id = str(getattr(command, "command_id", command_json.get("command_id", "")))
    idempotency_key = str(getattr(command, "idempotency_key", command_json.get("idempotency_key", "")))
    error_code = getattr(command, "error_code", command_json.get("error_code"))
    error_detail = getattr(command, "error_detail", command_json.get("error_detail"))
    result_ref = getattr(command, "result_ref", command_json.get("result_ref"))
    run_id = _workflow_run_id_from_result_ref(result_ref if isinstance(result_ref, str) else None)

    if command_status == "failed":
        message = str(error_detail or f"{action} failed via workflow command bus")
        return {
            "type": "error",
            "data": {
                "message": message,
                "command_id": command_id,
                "command_type": command_type,
                "command_status": command_status,
                "risk_level": risk_level,
                "idempotency_key": idempotency_key,
                "action": action,
                "target": target,
                "arguments": arguments,
                "command": command_json,
                "error_code": error_code,
                "error_detail": error_detail,
            },
            "selectable": False,
            "summary": message,
        }

    approval_required = command_status == "requested"
    display_status = "approval_required" if approval_required else (
        "queued" if action == ControlCommandType.WORKFLOW_SUBMIT.value and run_id else command_status
    )

    data: dict[str, Any] = {
        "status": display_status,
        "command_status": command_status,
        "command_id": command_id,
        "command_type": command_type,
        "risk_level": risk_level,
        "idempotency_key": idempotency_key,
        "approval_required": approval_required,
        "action": action,
        "target": target,
        "arguments": arguments,
        "command": command_json,
    }
    if spec_name:
        data["spec_name"] = spec_name
    if run_id:
        data["run_id"] = run_id
        data["stream_url"] = f"/api/workflow-runs/{run_id}/stream"
        data["status_url"] = f"/api/workflow-runs/{run_id}/status"
    if result_ref:
        data["result_ref"] = result_ref

    if approval_required:
        summary = (
            f"{action} requires approval for {target} "
            f"(command {command_id})."
        )
    elif action == ControlCommandType.WORKFLOW_SUBMIT.value and run_id:
        summary_target = spec_name or target
        summary = f"Workflow queued via command bus: {summary_target} ({run_id})."
    else:
        summary = f"{action} completed via command bus for {target}."

    return {
        "type": "status",
        "data": data,
        "selectable": False,
        "summary": summary,
    }


def execute_tool(
    name: str,
    arguments: dict[str, Any],
    pg_conn: Any,
    repo_root: str,
    selection_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute a tool call and return structured result.

    Returns {type, data, selectable, summary} where:
    - type: "table" | "cards" | "status" | "text" | "error"
    - data: type-specific payload
    - selectable: bool — can the user select items?
    - summary: str — short text summary for the LLM

    ``selection_context`` is forwarded by the chat orchestrator. When the
    frontend has an active Moon workflow open, an entry of the form
    ``{kind: 'moon_context', workflow_id, selected_node_id, ...}`` is in the
    list. The moon_* tools read it to default-target the active workflow
    when the LLM omits ``workflow_id``.
    """
    if name == "search_knowledge":
        return _search_knowledge(arguments, pg_conn)
    elif name == "query_workflows":
        return _query_workflows(arguments, pg_conn)
    elif name == "run_workflow":
        return _run_workflow(arguments, pg_conn, repo_root)
    elif name == "list_workflows":
        return _list_workflows(repo_root)
    elif name == "check_workflow_status":
        return _check_workflow_status(arguments, pg_conn)
    elif name == "query_database" or name == "query_platform_data":
        return _query_platform_data(arguments, pg_conn)
    elif name == "retry_job":
        return _retry_job(arguments, pg_conn)
    elif name == "get_job_output":
        return _get_job_output(arguments, pg_conn)
    elif name == "cancel_workflow":
        return _cancel_workflow(arguments, pg_conn)
    elif name == "moon_get_build":
        return _moon_get_build(arguments, pg_conn, selection_context)
    elif name == "moon_compose_from_prose":
        return _moon_compose_from_prose(arguments, pg_conn, selection_context)
    elif name == "moon_mutate_field":
        return _moon_mutate_field(arguments, pg_conn, selection_context)
    elif name == "moon_suggest_next":
        return _moon_suggest_next(arguments, pg_conn, selection_context)
    elif name == "moon_launch":
        return _moon_launch(arguments, pg_conn, selection_context)
    raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# Moon graph authoring tool implementations — gateway-dispatched
# ---------------------------------------------------------------------------

def _moon_error(message: str, *, action: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"message": message, "action": action}
    if details:
        payload["details"] = details
    return {"type": "error", "data": payload, "selectable": False, "summary": message}


def _moon_status_summary(action: str, payload: dict[str, Any]) -> str:
    """Render a compact JSON-string summary the LLM can reason about.

    The chat orchestrator passes only ``summary`` back to the model, so
    rich graph state has to live in the summary string. Use a single-line
    JSON form that keeps token cost down while preserving every id /
    route / status field the LLM needs to plan the next edit.
    """
    return f"{action}: " + json.dumps(payload, default=str, separators=(",", ":"))


_NODE_FIELD_KEEP = {
    "node_id", "id", "title", "summary", "route", "kind", "status",
    "agent_slug", "capability_slug",
}
_NODE_DETAIL_KEEP = {
    "trigger", "integration_args", "prompt", "required_inputs", "outputs",
    "handoff_target", "binding_ids", "issue_ids",
}


def _compact_node(node: dict[str, Any]) -> dict[str, Any]:
    out = {k: node[k] for k in _NODE_FIELD_KEEP if k in node and node[k] not in (None, "", [], {})}
    for key in _NODE_DETAIL_KEEP:
        value = node.get(key)
        if value not in (None, "", [], {}):
            out[key] = value
    return out


def _compact_edge(edge: dict[str, Any]) -> dict[str, Any]:
    keep = {
        "edge_id", "id", "source", "target", "from_node_id", "to_node_id",
        "release", "gate_family", "condition", "branch_label",
    }
    return {k: edge[k] for k in keep if k in edge and edge[k] is not None}


def _extract_outcome(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("outcome", "outcome_contract"):
        candidate = payload.get(key)
        if isinstance(candidate, dict) and candidate:
            return {k: candidate[k] for k in ("outcome_goal", "verify_command") if k in candidate}
    definition = payload.get("definition")
    if isinstance(definition, dict):
        return {
            k: definition[k]
            for k in ("outcome_goal", "verify_command", "objective", "phase")
            if k in definition and definition[k] not in (None, "")
        }
    return {}


def _compact_build_payload(payload: dict[str, Any]) -> dict[str, Any]:
    workflow_meta = payload.get("workflow") if isinstance(payload.get("workflow"), dict) else {}
    workflow_id = workflow_meta.get("id") or payload.get("workflow_id") or payload.get("id")
    name = workflow_meta.get("name") or payload.get("name")
    build_graph = payload.get("build_graph") if isinstance(payload.get("build_graph"), dict) else {}
    nodes = build_graph.get("nodes") if isinstance(build_graph.get("nodes"), list) else []
    edges = build_graph.get("edges") if isinstance(build_graph.get("edges"), list) else []
    issues = (
        payload.get("build_issues")
        or payload.get("build_blockers")
        or payload.get("planning_notes")
        or payload.get("issues")
        or []
    )
    outcome = _extract_outcome(payload)

    compact: dict[str, Any] = {
        "workflow_id": workflow_id,
        "name": name,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }
    if nodes:
        compact["nodes"] = [_compact_node(n) for n in nodes if isinstance(n, dict)][:30]
    if edges:
        compact["edges"] = [_compact_edge(e) for e in edges if isinstance(e, dict)][:30]
    if outcome:
        compact["outcome"] = outcome
    if isinstance(issues, list) and issues:
        compact["issues_top3"] = [issues[i] for i in range(min(3, len(issues)))]
    return compact


def _resolve_workflow_id(
    args: dict[str, Any],
    selection_context: list[dict[str, Any]] | None,
) -> tuple[str, bool]:
    """Pick workflow_id from args first, then the moon_context fallback.

    Returns ``(workflow_id, came_from_context)``. The boolean lets handlers
    surface context-defaulting in the summary so the LLM (and the user
    reading the chat) can see WHY a tool targeted a specific workflow.
    """
    explicit = str(args.get("workflow_id") or "").strip()
    if explicit:
        return explicit, False
    moon_ctx = _extract_moon_context(selection_context)
    if moon_ctx:
        ctx_id = str(moon_ctx.get("workflow_id") or "").strip()
        if ctx_id:
            return ctx_id, True
    return "", False


def _moon_get_build(
    args: dict[str, Any],
    pg_conn: Any,
    selection_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    workflow_id, from_context = _resolve_workflow_id(args, selection_context)
    if not workflow_id:
        return _moon_error(
            "workflow_id is required (no active Moon workflow in selection context)",
            action="moon_get_build",
        )
    try:
        result = _dispatch_op(pg_conn, "workflow_build_get", {"workflow_id": workflow_id})
    except Exception as exc:
        return _moon_error(f"moon_get_build failed: {exc}", action="moon_get_build")
    payload = result if isinstance(result, dict) else {"raw": result}
    compact = _compact_build_payload(payload)
    if from_context:
        compact["targeted_via"] = "moon_context"
    return {
        "type": "status",
        "data": {"status": "moon_build_loaded", **compact, "full_payload": payload},
        "selectable": False,
        "summary": _moon_status_summary("moon_get_build", compact),
    }


def _moon_compose_from_prose(
    args: dict[str, Any],
    pg_conn: Any,
    selection_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compose prose into a real, editable Moon workflow.

    Two paths:
      1. ``workflow_id`` provided (explicit OR from moon_context): bootstrap
         prose into the existing workflow's graph via workflow_build_mutate.
      2. No workflow_id: create a fresh draft via workflow_create_draft, then
         bootstrap prose into it. Returns the new workflow_id so the user can
         immediately edit / launch it.

    Either way the user lands on a real persisted graph the chat can keep
    editing via moon_get_build / moon_mutate_field / moon_launch.
    """
    intent = str(args.get("intent") or "").strip()
    if not intent:
        return _moon_error("intent is required", action="moon_compose_from_prose")

    plan_name = ""
    raw_plan_name = args.get("plan_name")
    if isinstance(raw_plan_name, str) and raw_plan_name.strip():
        plan_name = raw_plan_name.strip()

    # Reuse the active Moon workflow if the user is on the canvas.
    workflow_id, from_context = _resolve_workflow_id(args, selection_context)
    created_new = False
    if not workflow_id:
        draft_payload: dict[str, Any] = {}
        if plan_name:
            draft_payload["name"] = plan_name
        try:
            draft_result = _dispatch_op(pg_conn, "workflow_create_draft", draft_payload)
        except Exception as exc:
            return _moon_error(
                f"workflow_create_draft failed: {exc}",
                action="moon_compose_from_prose",
            )
        if not isinstance(draft_result, dict) or not draft_result.get("workflow_id"):
            return _moon_error(
                f"workflow_create_draft returned no workflow_id: {draft_result!r}",
                action="moon_compose_from_prose",
            )
        workflow_id = str(draft_result["workflow_id"])
        created_new = True

    enable_llm = args.get("enable_llm", True)
    enable_full_compose = args.get("enable_full_compose", True)
    bootstrap_body: dict[str, Any] = {
        "prose": intent,
        "enable_llm": bool(enable_llm),
        "enable_full_compose": bool(enable_full_compose),
    }
    if plan_name:
        bootstrap_body["title"] = plan_name
    raw_concurrency = args.get("concurrency")
    if raw_concurrency is not None:
        try:
            bootstrap_body["concurrency"] = max(1, min(100, int(raw_concurrency)))
        except (TypeError, ValueError):
            return _moon_error(
                "concurrency must be an integer 1-100",
                action="moon_compose_from_prose",
            )
    raw_why = args.get("why")
    if isinstance(raw_why, str) and raw_why.strip():
        bootstrap_body["why"] = raw_why.strip()

    try:
        result = _dispatch_op(
            pg_conn,
            "workflow_build.mutate",
            {"workflow_id": workflow_id, "subpath": "bootstrap", "body": bootstrap_body},
        )
    except Exception as exc:
        # Surface the workflow_id even on failure so the user can recover the draft.
        msg = f"bootstrap failed (workflow_id={workflow_id}): {exc}"
        return _moon_error(msg, action="moon_compose_from_prose")

    payload = result if isinstance(result, dict) else {"raw": result}
    compact = _compact_build_payload(payload)
    compact["workflow_id"] = compact.get("workflow_id") or workflow_id
    compact["created_new_draft"] = created_new
    if from_context and not created_new:
        compact["targeted_via"] = "moon_context"
    return {
        "type": "status",
        "data": {"status": "moon_composed", **compact, "full_payload": payload},
        "selectable": False,
        "summary": _moon_status_summary("moon_compose_from_prose", compact),
    }


def _moon_mutate_field(
    args: dict[str, Any],
    pg_conn: Any,
    selection_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    workflow_id, from_context = _resolve_workflow_id(args, selection_context)
    subpath = str(args.get("subpath") or "").strip()
    body = args.get("body")
    if not workflow_id:
        return _moon_error(
            "workflow_id is required (no active Moon workflow in selection context)",
            action="moon_mutate_field",
        )
    if not subpath:
        return _moon_error("subpath is required", action="moon_mutate_field")
    if not isinstance(body, dict):
        return _moon_error("body must be an object", action="moon_mutate_field")
    try:
        result = _dispatch_op(
            pg_conn,
            "workflow_build.mutate",
            {"workflow_id": workflow_id, "subpath": subpath, "body": body},
        )
    except Exception as exc:
        return _moon_error(f"workflow_build_mutate failed: {exc}", action="moon_mutate_field")
    payload = result if isinstance(result, dict) else {"raw": result}
    compact = _compact_build_payload(payload)
    compact["mutated_subpath"] = subpath
    if from_context:
        compact["targeted_via"] = "moon_context"
    return {
        "type": "status",
        "data": {"status": "moon_mutated", **compact, "full_payload": payload},
        "selectable": False,
        "summary": _moon_status_summary("moon_mutate_field", compact),
    }


def _moon_suggest_next(
    args: dict[str, Any],
    pg_conn: Any,
    selection_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    workflow_id, from_context = _resolve_workflow_id(args, selection_context)
    if not workflow_id:
        return _moon_error(
            "workflow_id is required (no active Moon workflow in selection context)",
            action="moon_suggest_next",
        )
    try:
        build_result = _dispatch_op(pg_conn, "workflow_build_get", {"workflow_id": workflow_id})
    except Exception as exc:
        return _moon_error(f"loading build for suggest_next failed: {exc}", action="moon_suggest_next")
    build_graph = build_result.get("build_graph") if isinstance(build_result, dict) else None
    body: dict[str, Any] = {"build_graph": build_graph or {}}
    raw_node_id = args.get("node_id")
    node_id = str(raw_node_id).strip() if isinstance(raw_node_id, str) else ""
    if not node_id:
        moon_ctx = _extract_moon_context(selection_context) or {}
        ctx_node = str(moon_ctx.get("selected_node_id") or "").strip()
        if ctx_node:
            node_id = ctx_node
    if node_id:
        body["node_id"] = node_id
    try:
        result = _dispatch_op(
            pg_conn,
            "workflow_build.suggest_next",
            {"workflow_id": workflow_id, "body": body},
        )
    except Exception as exc:
        return _moon_error(f"workflow_build_suggest_next failed: {exc}", action="moon_suggest_next")
    payload = result if isinstance(result, dict) else {"raw": result}
    likely = payload.get("likely_next_steps") or []
    possible = payload.get("possible_next_steps") or []
    blocked = payload.get("blocked_next_steps") or []
    compact = {
        "likely_count": len(likely),
        "possible_count": len(possible),
        "blocked_count": len(blocked),
        "likely_titles": [str(c.get("title") or c.get("capability_slug") or "") for c in likely if isinstance(c, dict)][:5],
    }
    if from_context:
        compact["targeted_via"] = "moon_context"
    if node_id:
        compact["anchor_node_id"] = node_id
    return {
        "type": "status",
        "data": {"status": "moon_suggest_next", **compact, "full_payload": payload},
        "selectable": False,
        "summary": _moon_status_summary("moon_suggest_next", compact),
    }


def _moon_launch(
    args: dict[str, Any],
    pg_conn: Any,
    selection_context: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    workflow_id, from_context = _resolve_workflow_id(args, selection_context)
    if not workflow_id:
        return _moon_error(
            "workflow_id is required (no active Moon workflow in selection context)",
            action="moon_launch",
        )
    payload: dict[str, Any] = {"workflow_id": workflow_id}
    for key in ("approved_by", "plan_name"):
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            payload[key] = value.strip()
    try:
        result = _dispatch_op(pg_conn, "launch_plan", payload)
    except Exception as exc:
        return _moon_error(f"launch_plan failed: {exc}", action="moon_launch")
    body = result if isinstance(result, dict) else {"raw": result}
    run_id = body.get("run_id") or body.get("workflow_run_id")
    compact = {
        "workflow_id": workflow_id,
        "run_id": run_id,
        "status": body.get("status") or body.get("execution_status"),
    }
    if from_context:
        compact["targeted_via"] = "moon_context"
    return {
        "type": "status",
        "data": {"status": "moon_launched", **compact, "full_payload": body},
        "selectable": False,
        "summary": _moon_status_summary("moon_launch", compact),
    }


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def _search_knowledge(args: dict, pg_conn: Any) -> dict:
    query = args.get("query", "")
    entity_type = args.get("entity_type")

    from memory.knowledge_graph import KnowledgeGraph

    kg = KnowledgeGraph(pg_conn)
    results = kg.search(query, entity_type=entity_type, limit=20)
    rows = [{"name": r.name, "type": r.entity_type, "score": round(r.score, 2)} for r in results]

    return {
        "type": "table",
        "data": {
            "columns": [{"key": "name", "label": "Name"}, {"key": "type", "label": "Type"}, {"key": "score", "label": "Score"}],
            "rows": rows,
        },
        "selectable": True,
        "summary": f"Found {len(rows)} knowledge graph entities for '{query}'.",
    }


def _query_workflows(args: dict, pg_conn: Any) -> dict:
    status_filter = args.get("status", "all")
    limit = min(args.get("limit", 20), 50)
    search = args.get("search", "")

    where_clauses = []
    params: list[Any] = []

    if status_filter and status_filter != "all":
        params.append(status_filter)
        where_clauses.append(f"wr.current_state = ${len(params)}")

    if search:
        params.append(f"%{search}%")
        where_clauses.append(
            f"COALESCE(wr.request_envelope->>'name', wr.workflow_id) ILIKE ${len(params)}"
        )

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    params.append(limit)

    sql = f"""
        SELECT wr.run_id,
               COALESCE(wr.request_envelope->>'name', wr.workflow_id) AS workflow_name,
               wr.current_state AS status,
               COALESCE(NULLIF(wr.request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
               wr.requested_at,
               EXTRACT(EPOCH FROM (COALESCE(wr.finished_at, NOW()) - wr.requested_at))::int as duration_s,
               (
                   SELECT COUNT(*)
                   FROM workflow_jobs wj
                   WHERE wj.run_id = wr.run_id
                     AND wj.status IN ('succeeded', 'failed', 'dead_letter', 'cancelled', 'blocked')
               ) as completed
        FROM workflow_runs wr
        {where}
        ORDER BY wr.requested_at DESC
        LIMIT ${len(params)}
    """

    rows = pg_conn.execute(sql, *params)
    data_rows = [{
        "run_id": r["run_id"],
        "name": r["workflow_name"],
        "status": r["status"],
        "jobs": f"{r['completed']}/{r['total_jobs']}",
        "duration": f"{r['duration_s']}s",
        "created": str(r["requested_at"])[:19],
    } for r in rows]

    return {
        "type": "table",
        "data": {
            "columns": [
                {"key": "name", "label": "Workflow"},
                {"key": "status", "label": "Status"},
                {"key": "jobs", "label": "Jobs"},
                {"key": "duration", "label": "Duration"},
                {"key": "created", "label": "Created"},
            ],
            "rows": data_rows,
        },
        "selectable": True,
        "summary": f"Found {len(data_rows)} workflow runs{f' (status={status_filter})' if status_filter != 'all' else ''}.",
    }


def _run_workflow(args: dict, pg_conn: Any, repo_root: str) -> dict:
    def _error(message: str, summary: str) -> dict:
        return {
            "type": "error",
            "data": {"message": message},
            "selectable": False,
            "summary": summary,
        }

    spec_path = args.get("spec_path")
    if isinstance(spec_path, str) and spec_path.strip():
        payload = {
            "spec_path": spec_path.strip(),
            "repo_root": repo_root,
        }
        from runtime.control_commands import request_workflow_submit_command

        command = request_workflow_submit_command(
            pg_conn,
            requested_by_kind=_CHAT_REQUESTED_BY_KIND,
            requested_by_ref=_CHAT_REQUESTED_BY_REF,
            spec_path=payload["spec_path"],
            repo_root=repo_root,
        )
        return _render_workflow_command_result(
            command,
            action=ControlCommandType.WORKFLOW_SUBMIT.value,
            arguments=payload,
            target=payload["spec_path"],
            spec_name=_workflow_spec_name_from_path(payload["spec_path"]),
        )

    spec = _build_inline_workflow_spec(args)
    if spec is not None:
        payload = {"spec": spec}
        return _request_workflow_command(
            pg_conn,
            command_type=ControlCommandType.WORKFLOW_SUBMIT,
            arguments=payload,
            target=spec["name"],
            spec_name=str(spec["name"]),
        )

    return _error(
        "Provide spec_path or jobs array",
        "Missing workflow spec",
    )


def _retry_job(args: dict[str, Any], pg_conn: Any) -> dict[str, Any]:
    run_id = str(args.get("run_id") or "").strip()
    label = str(args.get("label") or "").strip()
    if not run_id or not label:
        return {
            "type": "error",
            "data": {"message": "run_id and label are required"},
            "selectable": False,
            "summary": "Missing run_id or label",
        }

    payload = {
        "run_id": run_id,
        "label": label,
        "previous_failure": args.get("previous_failure"),
        "retry_delta": args.get("retry_delta"),
    }
    if args.get("model_override"):
        payload["model_override"] = args.get("model_override")

    return _request_workflow_command(
        pg_conn,
        command_type=ControlCommandType.WORKFLOW_RETRY,
        arguments=payload,
        target=f"{run_id}#{label}",
        spec_name=f"retry:{label}",
    )


def _cancel_workflow(args: dict[str, Any], pg_conn: Any) -> dict[str, Any]:
    run_id = str(args.get("run_id") or "").strip()
    if not run_id:
        return {
            "type": "error",
            "data": {"message": "run_id is required"},
            "selectable": False,
            "summary": "Missing run_id",
        }

    payload = {"run_id": run_id, "include_running": True}
    return _request_workflow_command(
        pg_conn,
        command_type=ControlCommandType.WORKFLOW_CANCEL,
        arguments=payload,
        target=run_id,
        spec_name=f"cancel:{run_id}",
    )


def _get_job_output(args: dict[str, Any], pg_conn: Any) -> dict[str, Any]:
    run_id = str(args.get("run_id") or "").strip()
    label = str(args.get("label") or "").strip()
    job_id = args.get("job_id")

    if not run_id and job_id is None:
        return {
            "type": "error",
            "data": {"message": "run_id or job_id is required"},
            "selectable": False,
            "summary": "Missing job identifier",
        }

    if job_id is not None:
        rows = pg_conn.execute(
            "SELECT * FROM workflow_jobs WHERE id = $1 LIMIT 1",
            job_id,
        )
    elif label:
        rows = pg_conn.execute(
            "SELECT * FROM workflow_jobs WHERE run_id = $1 AND label = $2 LIMIT 1",
            run_id,
            label,
        )
    else:
        rows = pg_conn.execute(
            "SELECT * FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at DESC LIMIT 1",
            run_id,
        )

    if not rows:
        return {
            "type": "error",
            "data": {"message": "Job not found"},
            "selectable": False,
            "summary": "Job not found",
        }

    row = dict(rows[0])
    content = (
        row.get("stdout_preview")
        or row.get("output_path")
        or row.get("receipt_id")
        or json.dumps(row, default=str)
    )
    return {
        "type": "text",
        "data": {
            "content": content,
            "job": row,
        },
        "selectable": False,
        "summary": f"Loaded job output for {row.get('label') or job_id or run_id}.",
    }


def _query_platform_data(args: dict[str, Any], pg_conn: Any) -> dict[str, Any]:
    report = str(args.get("report") or "").strip()
    time_range = str(args.get("time_range") or "24h").strip().lower()
    limit = args.get("limit", 20)
    try:
        limit = max(1, min(int(limit), 100))
    except (TypeError, ValueError):
        limit = 20
    filter_value = str(args.get("filter") or "").strip()

    time_range_sql = {
        "1h": "1 hour",
        "6h": "6 hours",
        "24h": "24 hours",
        "7d": "7 days",
        "30d": "30 days",
    }.get(time_range, "24 hours")
    filter_like = f"%{filter_value}%"

    if report == "recent_failures":
        rows = pg_conn.execute(
            f"""
                SELECT run_id,
                       label,
                       status,
                       COALESCE(NULLIF(failure_category, ''), last_error_code) AS error_code,
                       failure_category,
                       failure_zone,
                       is_transient,
                       duration_ms,
                       created_at
                  FROM workflow_jobs
                 WHERE status IN ('failed', 'dead_letter', 'cancelled')
                   AND created_at >= NOW() - INTERVAL '{time_range_sql}'
                   AND ($1 = '' OR run_id ILIKE $2 OR label ILIKE $2 OR COALESCE(NULLIF(failure_category, ''), last_error_code) ILIKE $2)
                 ORDER BY created_at DESC
                 LIMIT $3
                """,
            filter_value,
            filter_like,
            limit,
        )
        return {
            "type": "table",
            "data": {
                "columns": [
                    {"key": "run_id", "label": "Run"},
                    {"key": "label", "label": "Label"},
                    {"key": "status", "label": "Status"},
                    {"key": "error_code", "label": "Error"},
                    {"key": "failure_category", "label": "Category"},
                    {"key": "failure_zone", "label": "Zone"},
                    {"key": "duration_ms", "label": "Duration"},
                ],
                "rows": [dict(row) for row in rows],
            },
            "selectable": True,
            "summary": f"Found {len(rows)} recent failure rows.",
        }

    if report == "active_runs":
        rows = pg_conn.execute(
            f"""
                SELECT run_id,
                       workflow_id,
                       current_state AS status,
                       requested_at,
                       finished_at
                  FROM workflow_runs
                 WHERE current_state IN ('queued', 'running')
                   AND requested_at >= NOW() - INTERVAL '{time_range_sql}'
                   AND ($1 = '' OR run_id ILIKE $2 OR workflow_id ILIKE $2)
                 ORDER BY requested_at DESC
                 LIMIT $3
                """,
            filter_value,
            filter_like,
            limit,
        )
        return {
            "type": "table",
            "data": {
                "columns": [
                    {"key": "run_id", "label": "Run"},
                    {"key": "workflow_id", "label": "Workflow"},
                    {"key": "status", "label": "Status"},
                    {"key": "requested_at", "label": "Requested"},
                    {"key": "finished_at", "label": "Finished"},
                ],
                "rows": [dict(row) for row in rows],
            },
            "selectable": True,
            "summary": f"Found {len(rows)} active runs.",
        }

    if report == "model_performance":
        rows = pg_conn.execute(
            f"""
                SELECT COALESCE(resolved_agent, agent_slug) AS model,
                       COUNT(*) AS jobs,
                       COUNT(*) FILTER (WHERE status = 'succeeded') AS succeeded,
                       COUNT(*) FILTER (WHERE status IN ('failed', 'dead_letter')) AS failed,
                       ROUND(COALESCE(AVG(cost_usd), 0)::numeric, 4) AS avg_cost_usd
                  FROM workflow_jobs
                 WHERE created_at >= NOW() - INTERVAL '{time_range_sql}'
                   AND ($1 = '' OR COALESCE(resolved_agent, agent_slug) ILIKE $2)
                 GROUP BY 1
                 ORDER BY jobs DESC, model ASC
                 LIMIT $3
                """,
            filter_value,
            filter_like,
            limit,
        )
        return {
            "type": "table",
            "data": {
                "columns": [
                    {"key": "model", "label": "Model"},
                    {"key": "jobs", "label": "Jobs"},
                    {"key": "succeeded", "label": "Succeeded"},
                    {"key": "failed", "label": "Failed"},
                    {"key": "avg_cost_usd", "label": "Avg Cost"},
                ],
                "rows": [dict(row) for row in rows],
            },
            "selectable": False,
            "summary": f"Found {len(rows)} model performance rows.",
        }

    if report == "cost_summary":
        rows = pg_conn.execute(
            f"""
                SELECT COUNT(*) AS jobs,
                       COALESCE(SUM(cost_usd), 0) AS total_cost_usd,
                       COALESCE(SUM(token_input), 0) AS total_tokens_in,
                       COALESCE(SUM(token_output), 0) AS total_tokens_out
                  FROM workflow_jobs
                 WHERE created_at >= NOW() - INTERVAL '{time_range_sql}'
                   AND ($1 = '' OR run_id ILIKE $2 OR label ILIKE $2 OR COALESCE(resolved_agent, agent_slug) ILIKE $2)
                """,
            filter_value,
            filter_like,
        )
        row = dict(rows[0]) if rows else {"jobs": 0, "total_cost_usd": 0, "total_tokens_in": 0, "total_tokens_out": 0}
        return {
            "type": "status",
            "data": {
                "status": "cost_summary",
                "report": report,
                "time_range": time_range,
                **row,
            },
            "selectable": False,
            "summary": "Loaded workflow cost summary.",
        }

    if report == "job_history":
        rows = pg_conn.execute(
            f"""
                SELECT run_id,
                       label,
                       status,
                       COALESCE(resolved_agent, agent_slug) AS agent,
                       COALESCE(NULLIF(failure_category, ''), last_error_code) AS error_code,
                       duration_ms,
                       created_at
                  FROM workflow_jobs
                 WHERE created_at >= NOW() - INTERVAL '{time_range_sql}'
                   AND ($1 = '' OR run_id ILIKE $2 OR label ILIKE $2 OR COALESCE(resolved_agent, agent_slug) ILIKE $2 OR COALESCE(NULLIF(failure_category, ''), last_error_code) ILIKE $2)
                 ORDER BY created_at DESC
                 LIMIT $3
                """,
            filter_value,
            filter_like,
            limit,
        )
        return {
            "type": "table",
            "data": {
                "columns": [
                    {"key": "run_id", "label": "Run"},
                    {"key": "label", "label": "Label"},
                    {"key": "status", "label": "Status"},
                    {"key": "agent", "label": "Agent"},
                    {"key": "error_code", "label": "Error"},
                    {"key": "duration_ms", "label": "Duration"},
                ],
                "rows": [dict(row) for row in rows],
            },
            "selectable": True,
            "summary": f"Found {len(rows)} job history rows.",
        }

    return {
        "type": "error",
        "data": {
            "message": "Unsupported report",
            "supported_reports": [
                "recent_failures",
                "cost_summary",
                "active_runs",
                "model_performance",
                "job_history",
            ],
        },
        "selectable": False,
        "summary": "Unsupported platform data report",
    }


def _list_workflows(repo_root: str) -> dict:
    from storage.postgres import ensure_postgres_available

    pg_conn = ensure_postgres_available()
    definition_rows = pg_conn.execute(
        """SELECT workflow_definition_id,
                  workflow_id,
                  COALESCE(request_envelope->>'name', workflow_id, workflow_definition_id) AS workflow_name,
                  COALESCE(NULLIF(request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
                  COALESCE(request_envelope->>'outcome_goal', request_envelope->>'objective', normalized_definition->>'phase', '') AS goal
             FROM workflow_definitions
            WHERE status = 'active'
            ORDER BY created_at DESC
            LIMIT 100"""
    )
    rows = [
        {
            "file": row["workflow_definition_id"],
            "name": row.get("workflow_name") or row.get("workflow_id") or row["workflow_definition_id"],
            "jobs": int(row.get("total_jobs") or 0),
            "goal": row.get("goal") or "",
        }
        for row in definition_rows
    ]
    return {
        "type": "table",
        "data": {
            "columns": ["file", "name", "jobs", "goal"],
            "rows": rows,
        },
        "selectable": True,
        "summary": f"Found {len(rows)} DB workflow definitions",
    }
