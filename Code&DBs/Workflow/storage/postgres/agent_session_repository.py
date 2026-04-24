"""Postgres repository primitives for the ``agent_sessions`` table.

Two session families share this table:

- Workflow MCP sessions, owned by ``runtime.workflow.mcp_session``
- Interactive agent sessions, owned by ``surfaces.api.agent_sessions``

This module owns the raw SQL so any future invariant (TTL, rotation,
heartbeat policy) lands in exactly one place. Callers stay focused on
their domain contracts.
"""

from __future__ import annotations

import json
from typing import Any


def upsert_workflow_mcp_session(
    conn: Any,
    *,
    session_id: str,
    run_id: str,
    workflow_id: str,
    job_label: str,
    agent_slug: str,
) -> None:
    """Insert or refresh a workflow MCP session row to ``status='active'``."""
    conn.execute(
        """INSERT INTO agent_sessions (session_id, run_id, workflow_id, job_label, agent_slug, status)
           VALUES ($1, $2, $3, $4, $5, 'active')
           ON CONFLICT (session_id) DO UPDATE SET
               heartbeat_at = NOW(), status = 'active'""",
        session_id,
        run_id,
        workflow_id,
        job_label,
        agent_slug,
    )


def merge_workflow_mcp_session_context(
    conn: Any,
    *,
    session_id: str,
    context: dict[str, Any],
) -> None:
    """Shallow-merge context into ``context_json`` and refresh heartbeat."""
    conn.execute(
        """UPDATE agent_sessions
           SET context_json = context_json || $2,
               heartbeat_at = NOW()
           WHERE session_id = $1""",
        session_id,
        json.dumps(context),
    )


def advance_workflow_mcp_session_cursor(
    conn: Any,
    *,
    session_id: str,
    event_id: int,
) -> None:
    """Advance the session's event cursor. Only moves forward."""
    conn.execute(
        """UPDATE agent_sessions
           SET event_cursor = GREATEST(event_cursor, $2),
               heartbeat_at = NOW()
           WHERE session_id = $1""",
        session_id,
        event_id,
    )


def load_workflow_mcp_session(conn: Any, *, session_id: str) -> dict[str, Any] | None:
    """Look up a workflow MCP session row by ``session_id``."""
    rows = conn.execute(
        """SELECT session_id, run_id, workflow_id, job_label, agent_slug,
                  status, context_json, event_cursor, created_at, heartbeat_at
           FROM agent_sessions WHERE session_id = $1""",
        session_id,
    )
    if not rows:
        return None
    return dict(rows[0])
