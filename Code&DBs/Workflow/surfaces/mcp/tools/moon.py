"""Tool: praxis_moon — legacy alias for Workflow graph authoring.

Single MCP tool that routes to five graph-authoring CQRS operations:
``get_build``, ``compose``, ``suggest_next``, ``mutate_field``, ``launch``.

Mirrors the chat-side legacy ``moon_*`` registry in ``runtime.chat_tools`` so
Codex/Gemini get parity with the in-app chat. All five actions dispatch
through ``execute_operation_from_subsystems`` — same gateway path,
same receipts + events the chat tools produce.
"""
from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_subsystems

from ..subsystems import _subs


_ACTIONS = ("get_build", "compose", "suggest_next", "mutate_field", "launch")


def _err(message: str, *, action: str, code: str | None = None) -> dict[str, Any]:
    return {
        "ok": False,
        "error": message,
        "error_code": code or f"praxis_moon.{action}.invalid",
        "action": action,
    }


def _string_arg(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    return value.strip() if isinstance(value, str) else ""


def tool_praxis_moon(params: dict) -> dict:
    """Dispatch a Workflow graph-authoring action through the CQRS gateway."""
    action = _string_arg(params, "action")
    if action not in _ACTIONS:
        return _err(
            f"action must be one of {list(_ACTIONS)}",
            action=action or "missing",
            code="praxis_moon.action.invalid",
        )

    if action == "get_build":
        workflow_id = _string_arg(params, "workflow_id")
        if not workflow_id:
            return _err("workflow_id is required for action=get_build", action=action)
        return execute_operation_from_subsystems(
            _subs,
            operation_name="workflow_build_get",
            payload={"workflow_id": workflow_id},
        )

    if action == "compose":
        intent = _string_arg(params, "intent")
        if not intent:
            return _err("intent is required for action=compose", action=action)
        payload: dict[str, Any] = {"intent": intent}
        workflow_id = _string_arg(params, "workflow_id")
        if workflow_id:
            payload["workflow_id"] = workflow_id
        plan_name = _string_arg(params, "plan_name")
        if plan_name:
            payload["title"] = plan_name
        for key in ("enable_llm", "enable_full_compose"):
            value = params.get(key)
            if isinstance(value, bool):
                payload[key] = value
        materialized = execute_operation_from_subsystems(
            _subs,
            operation_name="compile_materialize",
            payload=payload,
        )
        if not isinstance(materialized, dict) or materialized.get("ok") is False:
            return materialized if isinstance(materialized, dict) else {"ok": False, "result": materialized}
        materialized_workflow_id = _string_arg(materialized, "workflow_id")
        if not materialized_workflow_id:
            return materialized
        build = execute_operation_from_subsystems(
            _subs,
            operation_name="workflow_build_get",
            payload={"workflow_id": materialized_workflow_id},
        )
        return {
            "ok": True,
            "action": "compose",
            "workflow_id": materialized_workflow_id,
            "graph_summary": materialized.get("graph_summary"),
            "operation_receipt": materialized.get("operation_receipt"),
            "materialization": materialized,
            "build": build,
        }

    if action == "suggest_next":
        workflow_id = _string_arg(params, "workflow_id")
        if not workflow_id:
            return _err("workflow_id is required for action=suggest_next", action=action)
        # Two-step: load build to extract build_graph, then suggest legal nexts.
        build_result = execute_operation_from_subsystems(
            _subs,
            operation_name="workflow_build_get",
            payload={"workflow_id": workflow_id},
        )
        build_graph = (
            build_result.get("build_graph")
            if isinstance(build_result, dict)
            else None
        )
        body: dict[str, Any] = {"build_graph": build_graph or {}}
        node_id = _string_arg(params, "node_id")
        if node_id:
            body["node_id"] = node_id
        return execute_operation_from_subsystems(
            _subs,
            operation_name="workflow_build.suggest_next",
            payload={"workflow_id": workflow_id, "body": body},
        )

    if action == "mutate_field":
        workflow_id = _string_arg(params, "workflow_id")
        subpath = _string_arg(params, "subpath")
        body = params.get("body")
        if not workflow_id:
            return _err("workflow_id is required for action=mutate_field", action=action)
        if not subpath:
            return _err("subpath is required for action=mutate_field", action=action)
        if not isinstance(body, dict):
            return _err("body must be an object for action=mutate_field", action=action)
        return execute_operation_from_subsystems(
            _subs,
            operation_name="workflow_build.mutate",
            payload={"workflow_id": workflow_id, "subpath": subpath, "body": body},
        )

    if action == "launch":
        workflow_id = _string_arg(params, "workflow_id")
        if not workflow_id:
            return _err("workflow_id is required for action=launch", action=action)
        payload = {"workflow_id": workflow_id}
        for key in ("approved_by", "plan_name"):
            value = _string_arg(params, key)
            if value:
                payload[key] = value
        return execute_operation_from_subsystems(
            _subs,
            operation_name="launch_plan",
            payload=payload,
        )

    return _err(f"action handler missing for {action!r}", action=action)


TOOLS: dict[str, tuple[Any, dict[str, Any]]] = {
    "praxis_moon": (
        tool_praxis_moon,
        {
            "description": (
                "Workflow graph-authoring co-pilot exposed through the legacy "
                "praxis_moon tool name. Five actions over the same workflow build "
                "authority, all CQRS-gateway dispatched (each call leaves a receipt + "
                "the command actions emit authority events).\n\n"
                "USE WHEN: an agent needs to read, compose, edit, or launch a "
                "Workflow graph. This is the same surface the in-app chat uses, "
                "exposed to Codex/Gemini for parity while old routes remain compatible.\n\n"
                "ACTIONS:\n"
                "  get_build       Load BuildPayload (nodes, edges, gates, contracts, issues).\n"
                "                  ALWAYS read before proposing edits so you reason about real state.\n"
                "  compose         Materialize a whole graph from prose through compile_materialize,\n"
                "                  then read it back from workflow build authority.\n"
                "  suggest_next    Ask the graph what's LEGAL to add next given accumulator types.\n"
                "  mutate_field    Edit any field via subpath (nodes/{id}, edges/{id}/release,\n"
                "                  outcome, append, ...). One universal mutation entrypoint.\n"
                "  launch          Launch the composed workflow as a real run.\n\n"
                "EXAMPLES:\n"
                "  Read graph:    praxis_moon(action='get_build', workflow_id='wf_abc')\n"
                "  Compose:       praxis_moon(action='compose', intent='Search GH issues, draft summary, notify Slack')\n"
                "  Suggest:       praxis_moon(action='suggest_next', workflow_id='wf_abc', node_id='node-2')\n"
                "  Edit field:    praxis_moon(action='mutate_field', workflow_id='wf_abc',\n"
                "                            subpath='nodes/node-1', body={'title':'Webhook v2'})\n"
                "  Launch:        praxis_moon(action='launch', workflow_id='wf_abc', approved_by='op@team')\n"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["get_build", "compose", "suggest_next", "mutate_field", "launch"],
                        "description": "Which graph-authoring action to perform.",
                    },
                    "workflow_id": {
                        "type": "string",
                        "description": "Workflow id (required for get_build, suggest_next, mutate_field, launch).",
                    },
                    "intent": {
                        "type": "string",
                        "description": "Plain-language description of what the workflow should do (action=compose).",
                    },
                    "plan_name": {"type": "string", "description": "Optional friendly name (compose, launch)."},
                    "why": {"type": "string", "description": "Optional rationale (compose)."},
                    "concurrency": {"type": "integer", "description": "Parallel author count 1-100 (compose, default 20)."},
                    "node_id": {"type": "string", "description": "Anchor node for suggest_next (default: latest)."},
                    "subpath": {
                        "type": "string",
                        "description": "Mutation subpath: 'append', 'nodes/{id}', 'edges/{id}/release', 'outcome', etc. (mutate_field).",
                    },
                    "body": {
                        "type": "object",
                        "description": "Mutation body — shape depends on subpath (mutate_field).",
                    },
                    "approved_by": {"type": "string", "description": "Who approved this launch (launch)."},
                },
                "required": ["action"],
            },
        },
    ),
}
