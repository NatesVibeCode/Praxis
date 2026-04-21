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
)
from runtime.idempotency import canonical_hash

_log = logging.getLogger(__name__)


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
                "model_override": {"type": "string", "description": "Optional: use a different model (e.g., anthropic/claude-opus-4-7)"},
            },
            "required": ["run_id", "label"],
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
    intent = ControlIntent(
        command_type=command_type,
        requested_by_kind=_CHAT_REQUESTED_BY_KIND,
        requested_by_ref=_CHAT_REQUESTED_BY_REF,
        idempotency_key=_workflow_command_idempotency_key(command_type, arguments),
        payload=arguments,
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
) -> dict[str, Any]:
    """Execute a tool call and return structured result.

    Returns {type, data, selectable, summary} where:
    - type: "table" | "cards" | "status" | "text" | "error"
    - data: type-specific payload
    - selectable: bool — can the user select items?
    - summary: str — short text summary for the LLM
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
    raise ValueError(f"Unknown tool: {name}")


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
