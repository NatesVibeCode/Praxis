"""Signed MCP session tokens for workflow Docker jobs."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any


_TOKEN_VERSION = 1
_TOKEN_AUDIENCE = "workflow-mcp"
_SIGNING_SECRET_ENV = "PRAXIS_WORKFLOW_MCP_SIGNING_SECRET"
_TOKEN_TTL_ENV = "PRAXIS_WORKFLOW_MCP_TOKEN_TTL_SECONDS"
_WORKFLOW_DATABASE_URL_ENV = "WORKFLOW_DATABASE_URL"


class WorkflowMcpSessionError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _urlsafe_b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def _signing_secret() -> bytes:
    secret_seed = (
        str(os.environ.get(_SIGNING_SECRET_ENV, "")).strip()
        or str(os.environ.get(_WORKFLOW_DATABASE_URL_ENV, "")).strip()
        or "dag-workflow-local-dev"
    )
    return hashlib.sha256(secret_seed.encode("utf-8")).digest()


def _sign(payload: bytes) -> str:
    return _urlsafe_b64encode(hmac.new(_signing_secret(), payload, hashlib.sha256).digest())


def _current_time() -> int:
    return int(time.time())


def mint_workflow_mcp_session_token(
    *,
    run_id: str | None,
    workflow_id: str | None,
    job_label: str,
    allowed_tools: list[str],
    conn: Any | None = None,
    agent_slug: str = "",
) -> str:
    normalized_allowed_tools = [str(tool).strip() for tool in allowed_tools if str(tool).strip()]
    issued_at = _current_time()
    ttl_seconds = max(60, int(str(os.environ.get(_TOKEN_TTL_ENV, "3600")).strip() or "3600"))
    payload = {
        "v": _TOKEN_VERSION,
        "aud": _TOKEN_AUDIENCE,
        "iat": issued_at,
        "exp": issued_at + ttl_seconds,
        "run_id": str(run_id or "").strip() or None,
        "workflow_id": str(workflow_id or "").strip() or None,
        "job_label": str(job_label or "").strip(),
        "allowed_tools": normalized_allowed_tools,
    }
    payload_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    token = f"{_urlsafe_b64encode(payload_bytes)}.{_sign(payload_bytes)}"

    # Persist session row if we have a database connection.
    if conn is not None:
        session_id = _session_id_from_token(token)
        try:
            conn.execute(
                """INSERT INTO agent_sessions (session_id, run_id, workflow_id, job_label, agent_slug, status)
                   VALUES ($1, $2, $3, $4, $5, 'active')
                   ON CONFLICT (session_id) DO UPDATE SET
                       heartbeat_at = NOW(), status = 'active'""",
                session_id,
                str(run_id or ""),
                str(workflow_id or ""),
                str(job_label or ""),
                agent_slug,
            )
        except Exception:
            pass  # token minting must not fail because of session persistence

    return token


def verify_workflow_mcp_session_token(token: str) -> dict[str, Any]:
    token_text = str(token or "").strip()
    if not token_text or "." not in token_text:
        raise WorkflowMcpSessionError("workflow_mcp.token_missing", "workflow MCP token is required")

    payload_part, signature_part = token_text.split(".", 1)
    try:
        payload_bytes = _urlsafe_b64decode(payload_part)
    except Exception as exc:  # pragma: no cover - defensive
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token payload is invalid") from exc

    expected_signature = _sign(payload_bytes)
    if not hmac.compare_digest(expected_signature, signature_part):
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token signature is invalid")

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token payload is invalid") from exc

    if str(payload.get("aud") or "") != _TOKEN_AUDIENCE:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token audience is invalid")
    if int(payload.get("v") or 0) != _TOKEN_VERSION:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token version is invalid")
    if int(payload.get("exp") or 0) < _current_time():
        raise WorkflowMcpSessionError("workflow_mcp.token_expired", "workflow MCP token has expired")
    if not str(payload.get("job_label") or "").strip():
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token job label is missing")
    allowed_tools = payload.get("allowed_tools")
    if not isinstance(allowed_tools, list):
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token allowed tools are invalid")
    payload["allowed_tools"] = [str(tool).strip() for tool in allowed_tools if str(tool).strip()]
    return payload


# ---------------------------------------------------------------------------
# Session ID derivation
# ---------------------------------------------------------------------------

def _session_id_from_token(token: str) -> str:
    """Derive a stable session ID from the token (first 32 chars of hash)."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Session persistence (requires conn)
# ---------------------------------------------------------------------------

def get_agent_session(conn: Any, session_token: str) -> dict[str, Any] | None:
    """Look up session row by token."""
    session_id = _session_id_from_token(session_token)
    rows = conn.execute(
        """SELECT session_id, run_id, workflow_id, job_label, agent_slug,
                  status, context_json, event_cursor, created_at, heartbeat_at
           FROM agent_sessions WHERE session_id = $1""",
        session_id,
    )
    if not rows:
        return None
    r = rows[0]
    ctx = r.get("context_json") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, TypeError):
            ctx = {}
    return {
        "session_id": r["session_id"],
        "run_id": r["run_id"],
        "workflow_id": r.get("workflow_id") or "",
        "job_label": r["job_label"],
        "agent_slug": r.get("agent_slug") or "",
        "status": r["status"],
        "context_json": ctx,
        "event_cursor": r.get("event_cursor") or 0,
        "created_at": r["created_at"],
        "heartbeat_at": r["heartbeat_at"],
    }


def update_session_context(conn: Any, session_token: str, context: dict[str, Any]) -> None:
    """Shallow-merge context into the session's context_json."""
    session_id = _session_id_from_token(session_token)
    conn.execute(
        """UPDATE agent_sessions
           SET context_json = context_json || $2,
               heartbeat_at = NOW()
           WHERE session_id = $1""",
        session_id,
        json.dumps(context),
    )


def advance_session_cursor(conn: Any, session_token: str, event_id: int) -> None:
    """Advance the session's event cursor. Only moves forward."""
    session_id = _session_id_from_token(session_token)
    conn.execute(
        """UPDATE agent_sessions
           SET event_cursor = GREATEST(event_cursor, $2),
               heartbeat_at = NOW()
           WHERE session_id = $1""",
        session_id,
        event_id,
    )

