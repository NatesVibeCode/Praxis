"""MCP tool wrappers for the agent_principal authority.

Thin gateway-delegated wrappers (no business logic — every call goes through
``execute_operation_from_subsystems`` so receipts and events land canonically).

Tools:
  praxis_agent_forge          — preview the canonical agent registration path
  praxis_agent_register       — upsert one agent_principals row (gateway-backed)
  praxis_agent_list           — list agent principals
  praxis_agent_describe       — full envelope + recent wakes/delegations/gaps
  praxis_agent_status         — flip active|paused|killed
  praxis_agent_wake           — insert one pending wake row + emit event
  praxis_agent_wake_list      — list wake-ledger rows
  praxis_agent_delegate       — parent agent → bounded child workflow
  praxis_tool_gap_file        — file an agent_tool_gap row
  praxis_tool_gap_list        — list open / triaged / shipped tool gaps
"""

from __future__ import annotations

from typing import Any, Callable


def _gateway_call(operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Dispatch a payload through the operation_catalog_gateway.

    Mirrors ``surfaces.mcp.tools.operator._execute_catalog_tool`` but local-only
    — agent operations live entirely on the gateway and don't need a fallback.
    """
    from runtime.operation_catalog_gateway import execute_operation_from_subsystems

    from ..subsystems import _subs  # type: ignore[import]

    try:
        result = execute_operation_from_subsystems(
            _subs, operation_name=operation_name, payload=payload
        )
        if isinstance(result, dict) and "ok" not in result:
            result["ok"] = True
        return result
    except Exception as exc:
        return {
            "ok": False,
            "operation": operation_name,
            "error": {
                "type": exc.__class__.__name__,
                "message": str(exc),
            },
        }


# ─────────────────────────────────────────────────────────────────────────────
# Tool functions
# ─────────────────────────────────────────────────────────────────────────────


def tool_praxis_agent_forge(params: dict) -> dict:
    """Preview the canonical agent registration path."""
    return _gateway_call("agent.query.forge", dict(params or {}))


def tool_praxis_agent_register(params: dict) -> dict:
    """Upsert one agent_principal row."""
    return _gateway_call("agent_principal.register", dict(params or {}))


def tool_praxis_agent_list(params: dict) -> dict:
    """List agent principals filtered by status."""
    return _gateway_call("agent_principal.list", dict(params or {}))


def tool_praxis_agent_describe(params: dict) -> dict:
    """Describe one principal — full envelope + recent wakes/delegations/gaps."""
    return _gateway_call("agent_principal.describe", dict(params or {}))


def tool_praxis_agent_status(params: dict) -> dict:
    """Flip an agent_principal between active|paused|killed."""
    return _gateway_call("agent_principal.update_status", dict(params or {}))


def tool_praxis_agent_wake(params: dict) -> dict:
    """Request a wake — insert agent_wakes pending row + emit event."""
    return _gateway_call("agent_wake.request", dict(params or {}))


def tool_praxis_agent_wake_list(params: dict) -> dict:
    """List agent wake-ledger rows."""
    return _gateway_call("agent_wake.list", dict(params or {}))


def tool_praxis_agent_delegate(params: dict) -> dict:
    """Parent agent delegates a bounded child task."""
    return _gateway_call("agent.delegate", dict(params or {}))


def tool_praxis_tool_gap_file(params: dict) -> dict:
    """Worker files a tool gap row when Praxis lacks a needed capability."""
    return _gateway_call("agent_tool_gap.file", dict(params or {}))


def tool_praxis_tool_gap_list(params: dict) -> dict:
    """List open / triaged / shipped tool gaps."""
    return _gateway_call("agent_tool_gap.list", dict(params or {}))


# ─────────────────────────────────────────────────────────────────────────────
# TOOLS registry
# ─────────────────────────────────────────────────────────────────────────────


TOOLS: dict[str, tuple[Callable[[dict], dict], dict[str, Any]]] = {
    "praxis_agent_forge": (
        tool_praxis_agent_forge,
        {
            "kind": "analytics",
            "operation_names": ["agent.query.forge"],
            "description": (
                "Preview the canonical CQRS path for adding or evolving an agent. "
                "Validates standing_order_keys against operator_decisions, "
                "capability_refs against capability_catalog, integration_refs "
                "against integration_registry. Produces the predicted "
                "agent_principal.register payload + canonical write order + "
                "reject paths.\n\n"
                "USE WHEN: you are about to register a new agent_principal or "
                "change an existing one's scope. ALWAYS run this before "
                "agent_principal.register so the inputs are authority-validated."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["agent_principal_ref"],
                "properties": {
                    "agent_principal_ref": {"type": "string", "description": "Durable agent identity (e.g. 'agent.exec.nate')."},
                    "title": {"type": "string"},
                    "mission": {
                        "type": "string",
                        "description": (
                            "What this agent is for — appears in the trust-compiled "
                            "context at every wake."
                        ),
                    },
                    "status": {
                        "type": "string",
                        "enum": ["active", "paused", "killed"],
                        "default": "active",
                    },
                    "max_in_flight_wakes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 16,
                        "default": 1,
                    },
                    "write_envelope": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Path globs the agent is allowed to write to.",
                    },
                    "capability_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "integration_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "standing_order_keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "operator_decisions decision_keys the agent must obey. "
                            "Forge validates each key exists."
                        ),
                    },
                    "allowed_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "network_policy": {
                        "type": "string",
                        "enum": [
                            "disabled",
                            "provider_only",
                            "praxis_only",
                            "enabled",
                        ],
                        "default": "praxis_only",
                    },
                    "default_conversation_id": {"type": "string"},
                    "decision_ref": {"type": "string"},
                },
            },
        },
    ),
    "praxis_agent_register": (
        tool_praxis_agent_register,
        {
            "kind": "write",
            "operation_names": ["agent_principal.register"],
            "description": (
                "Upsert one agent_principal row. ALWAYS call praxis_agent_forge "
                "first to validate inputs. Idempotent on agent_principal_ref. "
                "Emits agent_principal.registered event on completion."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["agent_principal_ref", "title"],
                "properties": {
                    "agent_principal_ref": {"type": "string", "description": "Durable agent identity (e.g. 'agent.exec.nate')."},
                    "title": {"type": "string"},
                    "status": {
                        "type": "string",
                        "enum": ["active", "paused", "killed"],
                        "default": "active",
                    },
                    "max_in_flight_wakes": {"type": "integer", "default": 1},
                    "write_envelope": {"type": "array", "items": {"type": "string"}},
                    "capability_refs": {"type": "array", "items": {"type": "string"}},
                    "integration_refs": {"type": "array", "items": {"type": "string"}},
                    "standing_order_keys": {"type": "array", "items": {"type": "string"}},
                    "allowed_tools": {"type": "array", "items": {"type": "string"}},
                    "network_policy": {
                        "type": "string",
                        "enum": [
                            "disabled",
                            "provider_only",
                            "praxis_only",
                            "enabled",
                        ],
                        "default": "praxis_only",
                    },
                    "default_conversation_id": {"type": "string"},
                    "routing_policy": {"type": "object"},
                    "metadata": {"type": "object"},
                },
            },
        },
    ),
    "praxis_agent_list": (
        tool_praxis_agent_list,
        {
            "kind": "search",
            "operation_names": ["agent_principal.list"],
            "description": "List agent principals filtered by status.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["active", "paused", "killed", "any"],
                        "default": "active",
                    },
                    "limit": {"type": "integer", "default": 50},
                },
            },
        },
    ),
    "praxis_agent_describe": (
        tool_praxis_agent_describe,
        {
            "kind": "search",
            "operation_names": ["agent_principal.describe"],
            "description": (
                "Full envelope for one agent: scope, integrations, standing "
                "orders, recent wakes, recent delegations, recent tool gaps. "
                "Use when you need to understand an agent's state or audit "
                "its history."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["agent_principal_ref"],
                "properties": {
                    "agent_principal_ref": {"type": "string", "description": "Durable agent identity (e.g. 'agent.exec.nate')."},
                    "history_limit": {"type": "integer", "default": 10},
                },
            },
        },
    ),
    "praxis_agent_status": (
        tool_praxis_agent_status,
        {
            "kind": "write",
            "operation_names": ["agent_principal.update_status"],
            "description": (
                "Flip an agent_principal between active|paused|killed. The "
                "trigger evaluator skips paused/killed principals with "
                "skip_reason — this is the kill switch."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["agent_principal_ref", "status"],
                "properties": {
                    "agent_principal_ref": {"type": "string", "description": "Durable agent identity (e.g. 'agent.exec.nate')."},
                    "status": {
                        "type": "string",
                        "enum": ["active", "paused", "killed"],
                    },
                    "reason": {"type": "string"},
                },
            },
        },
    ),
    "praxis_agent_wake": (
        tool_praxis_agent_wake,
        {
            "kind": "write",
            "operation_names": ["agent_wake.request"],
            "description": (
                "Request a wake for an agent. Inserts an agent_wakes row in "
                "status=pending, emits agent.wake.requested. Idempotent on "
                "(agent_principal_ref, trigger_kind, payload_hash)."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["agent_principal_ref", "trigger_kind"],
                "properties": {
                    "agent_principal_ref": {"type": "string", "description": "Durable agent identity (e.g. 'agent.exec.nate')."},
                    "trigger_kind": {
                        "type": "string",
                        "enum": ["chat", "schedule", "webhook", "delegation", "manual"],
                    },
                    "trigger_source_ref": {"type": "string"},
                    "payload": {"type": "object"},
                },
            },
        },
    ),
    "praxis_agent_wake_list": (
        tool_praxis_agent_wake_list,
        {
            "kind": "search",
            "operation_names": ["agent_wake.list"],
            "description": "List agent wake-ledger rows.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "agent_principal_ref": {"type": "string"},
                    "trigger_kind": {
                        "type": "string",
                        "enum": ["chat", "schedule", "webhook", "delegation", "manual"],
                    },
                    "status": {
                        "type": "string",
                        "enum": [
                            "pending",
                            "dispatched",
                            "completed",
                            "failed",
                            "skipped",
                        ],
                    },
                    "limit": {"type": "integer", "default": 50},
                },
            },
        },
    ),
    "praxis_agent_delegate": (
        tool_praxis_agent_delegate,
        {
            "kind": "write",
            "operation_names": ["agent.delegate"],
            "description": (
                "Parent agent delegates a bounded child task. Materialises an "
                "agent_delegations row, launches a child workflow with a scoped "
                "tool list and praxis_only network. Use this — never spawn "
                "child workflows directly."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["parent_agent_ref", "child_task", "child_intent"],
                "properties": {
                    "parent_agent_ref": {"type": "string", "description": "Durable agent identity (e.g. 'agent.exec.nate')."},
                    "child_task": {"type": "string"},
                    "child_intent": {"type": "string"},
                    "parent_run_id": {"type": "string"},
                    "parent_job_id": {"type": "string"},
                    "parent_wake_id": {"type": "string"},
                    "admitted_tools": {"type": "array", "items": {"type": "string"}},
                    "admitted_integrations": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "write_envelope": {"type": "array", "items": {"type": "string"}},
                    "network_policy": {
                        "type": "string",
                        "enum": [
                            "disabled",
                            "provider_only",
                            "praxis_only",
                            "enabled",
                        ],
                        "default": "praxis_only",
                    },
                    "timeout_ms": {"type": "integer", "default": 600000},
                    "metadata": {"type": "object"},
                },
            },
        },
    ),
    "praxis_tool_gap_file": (
        tool_praxis_tool_gap_file,
        {
            "kind": "write",
            "operation_names": ["agent_tool_gap.file"],
            "description": (
                "File a tool gap when Praxis lacks a needed capability. This "
                "becomes roadmap fuel. Do this BEFORE improvising around a "
                "missing tool — the gap row is the right answer."
            ),
            "inputSchema": {
                "type": "object",
                "required": [
                    "reporter_agent_ref",
                    "missing_capability",
                    "attempted_task",
                    "blocked_action",
                ],
                "properties": {
                    "reporter_agent_ref": {"type": "string", "description": "Durable agent identity (e.g. 'agent.exec.nate')."},
                    "missing_capability": {"type": "string"},
                    "attempted_task": {"type": "string"},
                    "blocked_action": {"type": "string"},
                    "reporter_run_id": {"type": "string"},
                    "reporter_delegation_id": {"type": "string"},
                    "admitted_tools": {"type": "array", "items": {"type": "string"}},
                    "suggested_tool_contract": {"type": "object"},
                    "evidence": {"type": "object"},
                    "severity": {
                        "type": "string",
                        "enum": ["low", "medium", "high", "blocking"],
                        "default": "medium",
                    },
                },
            },
        },
    ),
    "praxis_tool_gap_list": (
        tool_praxis_tool_gap_list,
        {
            "kind": "search",
            "operation_names": ["agent_tool_gap.list"],
            "description": (
                "List open / triaged / shipped tool gaps. Use this for roadmap "
                "triage — gaps filed by working agents tell you what tooling to "
                "build next."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "reporter_agent_ref": {"type": "string"},
                    "severity": {
                        "type": "string",
                        "enum": ["low", "medium", "high", "blocking"],
                    },
                    "status": {
                        "type": "string",
                        "enum": [
                            "open",
                            "triaged",
                            "planned",
                            "shipped",
                            "declined",
                            "duplicate",
                        ],
                        "default": "open",
                    },
                    "missing_capability": {"type": "string"},
                    "limit": {"type": "integer", "default": 50},
                },
            },
        },
    ),
}
