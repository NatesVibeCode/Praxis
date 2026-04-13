"""MCP tools for agent event subscription and session context.

Lets sandboxed agents subscribe to build state events and persist
context across tool calls.
"""
from __future__ import annotations

from typing import Any


def tool_praxis_subscribe_events(
    *,
    _subsystems: Any = None,
    _session_token: str = "",
    channel: str = "build_state",
    limit: int = 50,
    **_kw: Any,
) -> dict[str, Any]:
    """Pull events since the agent's last cursor position."""
    from runtime.event_log import read_since
    from runtime.workflow.mcp_session import (
        get_agent_session,
        advance_session_cursor,
    )

    conn = _subsystems.get_pg_conn()
    session = get_agent_session(conn, _session_token)
    if session is None:
        return {"error": "No active session for this token."}

    cursor = session.get("event_cursor") or 0
    entity_id = session.get("workflow_id") or session.get("run_id") or None

    events = read_since(
        conn,
        channel=channel,
        cursor=cursor,
        entity_id=entity_id,
        limit=limit,
    )

    if events:
        max_id = max(e.id for e in events)
        advance_session_cursor(conn, _session_token, max_id)

    return {
        "events": [e.to_dict() for e in events],
        "cursor": events[-1].id if events else cursor,
        "count": len(events),
    }


def tool_praxis_session_context(
    *,
    _subsystems: Any = None,
    _session_token: str = "",
    action: str = "read",
    context: dict[str, Any] | None = None,
    **_kw: Any,
) -> dict[str, Any]:
    """Read or write persistent context on the agent's session."""
    from runtime.workflow.mcp_session import (
        get_agent_session,
        update_session_context,
    )

    conn = _subsystems.get_pg_conn()
    session = get_agent_session(conn, _session_token)
    if session is None:
        return {"error": "No active session for this token."}

    if action == "write":
        if not isinstance(context, dict):
            return {"error": "context must be a dict for write action."}
        update_session_context(conn, _session_token, context)
        # Re-read to return merged state
        session = get_agent_session(conn, _session_token)
        return {
            "status": "updated",
            "context": session.get("context_json") if session else {},
        }

    # Default: read
    return {
        "context": session.get("context_json") or {},
        "session_id": session.get("session_id"),
        "event_cursor": session.get("event_cursor") or 0,
    }


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_subscribe_events": (
        tool_praxis_subscribe_events,
        {
            "description": (
                "Pull build state events since the agent's last cursor position. "
                "Returns new events and advances the cursor. Call repeatedly to "
                "stay in sync with platform state changes.\n\n"
                "USE WHEN: you want to see what changed since your last check — "
                "mutations, compilations, commits.\n\n"
                "EXAMPLE: praxis_subscribe_events(channel='build_state')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "channel": {
                        "type": "string",
                        "description": "Event channel to subscribe to.",
                        "default": "build_state",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max events to return.",
                        "default": 50,
                    },
                },
            },
        },
    ),
    "praxis_session_context": (
        tool_praxis_session_context,
        {
            "description": (
                "Read or write persistent context on your agent session. "
                "Context survives across tool calls and is available on retry.\n\n"
                "USE WHEN: you need to checkpoint state, store intermediate results, "
                "or resume from where you left off.\n\n"
                "EXAMPLES:\n"
                "  Read:  praxis_session_context(action='read')\n"
                "  Write: praxis_session_context(action='write', context={'step': 3, 'findings': [...]})"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Operation: 'read' or 'write'.",
                        "enum": ["read", "write"],
                        "default": "read",
                    },
                    "context": {
                        "type": "object",
                        "description": "Context to merge (for write action). Shallow merge — new keys override, existing preserved.",
                    },
                },
            },
        },
    ),
}
