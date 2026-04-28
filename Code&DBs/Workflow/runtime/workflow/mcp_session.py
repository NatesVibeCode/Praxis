"""Signed MCP session tokens for workflow Docker jobs."""

from __future__ import annotations

import json
import os
import secrets
import time
from typing import Any
from uuid import uuid4

from runtime.crypto_authority import (
    CryptoAuthorityError,
    HmacKeyring,
    digest_bytes_hex,
    hmac_sha256_b64url,
    hmac_sha256_b64url_verify,
    load_hmac_keyring_from_env,
    urlsafe_b64decode,
    urlsafe_b64encode,
)


_TOKEN_VERSION = 1
_TOKEN_AUDIENCE = "workflow-mcp"
_SIGNING_SECRET_ENV = "PRAXIS_WORKFLOW_MCP_SIGNING_SECRET"
_SIGNING_KEY_ID_ENV = "PRAXIS_WORKFLOW_MCP_SIGNING_KEY_ID"
_SIGNING_KEYRING_ENV = "PRAXIS_WORKFLOW_MCP_SIGNING_KEYS_JSON"
_TOKEN_TTL_ENV = "PRAXIS_WORKFLOW_MCP_TOKEN_TTL_SECONDS"
_REVOKED_JTIS_ENV = "PRAXIS_WORKFLOW_MCP_REVOKED_JTIS"
_DEFAULT_SIGNING_KID = "workflow-mcp.env.v1"


class WorkflowMcpSessionError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _signing_keyring() -> HmacKeyring:
    try:
        return load_hmac_keyring_from_env(
            os.environ,
            secret_env=_SIGNING_SECRET_ENV,
            key_id_env=_SIGNING_KEY_ID_ENV,
            keyring_json_env=_SIGNING_KEYRING_ENV,
            default_kid=_DEFAULT_SIGNING_KID,
        )
    except CryptoAuthorityError as exc:
        raise WorkflowMcpSessionError(
            "workflow_mcp.signing_secret_missing",
            str(exc),
        ) from exc


def _sign(payload: bytes, *, kid: str | None = None) -> str:
    keyring = _signing_keyring()
    key = keyring.key_for(kid)
    if key is None:
        raise WorkflowMcpSessionError(
            "workflow_mcp.token_invalid",
            "workflow MCP token signing key is unavailable",
        )
    return hmac_sha256_b64url(payload, secret_seed=key.secret_seed)


def _current_time() -> int:
    return int(time.time())


def _token_jti() -> str:
    return f"{uuid4().hex}.{secrets.token_urlsafe(12)}"


def _revoked_jtis() -> set[str]:
    raw = str(os.environ.get(_REVOKED_JTIS_ENV, "") or "")
    return {item.strip() for item in raw.split(",") if item.strip()}


def mint_workflow_mcp_session_token(
    *,
    run_id: str | None,
    workflow_id: str | None,
    job_label: str,
    allowed_tools: list[str],
    source_refs: list[str] | None = None,
    access_policy: dict[str, Any] | None = None,
    conn: Any | None = None,
    agent_slug: str = "",
) -> str:
    keyring = _signing_keyring()
    active_key = keyring.active_key
    normalized_allowed_tools = [str(tool).strip() for tool in allowed_tools if str(tool).strip()]
    normalized_source_refs = [
        str(ref).strip() for ref in (source_refs or []) if str(ref).strip()
    ]
    normalized_access_policy = (
        json.loads(json.dumps(access_policy, sort_keys=True, default=str))
        if isinstance(access_policy, dict)
        else {}
    )
    issued_at = _current_time()
    ttl_seconds = max(60, int(str(os.environ.get(_TOKEN_TTL_ENV, "3600")).strip() or "3600"))
    payload = {
        "v": _TOKEN_VERSION,
        "aud": _TOKEN_AUDIENCE,
        "kid": active_key.kid,
        "jti": _token_jti(),
        "iat": issued_at,
        "exp": issued_at + ttl_seconds,
        "run_id": str(run_id or "").strip() or None,
        "workflow_id": str(workflow_id or "").strip() or None,
        "job_label": str(job_label or "").strip(),
        "allowed_tools": normalized_allowed_tools,
        "source_refs": normalized_source_refs,
        "access_policy": normalized_access_policy,
    }
    payload_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    token = f"{urlsafe_b64encode(payload_bytes)}.{_sign(payload_bytes, kid=active_key.kid)}"

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
        payload_bytes = urlsafe_b64decode(payload_part)
    except Exception as exc:  # pragma: no cover - defensive
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token payload is invalid") from exc

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token payload is invalid") from exc

    keyring = _signing_keyring()
    kid = payload.get("kid")
    if kid is not None and not isinstance(kid, str):
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token key id is invalid")
    key = keyring.key_for(kid)
    if key is None:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token signing key is unavailable")
    if not hmac_sha256_b64url_verify(payload_bytes, signature_part, secret_seed=key.secret_seed):
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token signature is invalid")

    if str(payload.get("aud") or "") != _TOKEN_AUDIENCE:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token audience is invalid")
    if int(payload.get("v") or 0) != _TOKEN_VERSION:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token version is invalid")
    jti = payload.get("jti")
    if isinstance(jti, str) and jti.strip() and jti.strip() in _revoked_jtis():
        raise WorkflowMcpSessionError("workflow_mcp.token_revoked", "workflow MCP token has been revoked")
    if int(payload.get("exp") or 0) < _current_time():
        raise WorkflowMcpSessionError("workflow_mcp.token_expired", "workflow MCP token has expired")
    if not str(payload.get("job_label") or "").strip():
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token job label is missing")
    allowed_tools = payload.get("allowed_tools")
    if not isinstance(allowed_tools, list):
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token allowed tools are invalid")
    payload["allowed_tools"] = [str(tool).strip() for tool in allowed_tools if str(tool).strip()]
    source_refs = payload.get("source_refs")
    if source_refs is None:
        payload["source_refs"] = []
    elif isinstance(source_refs, list):
        payload["source_refs"] = [str(ref).strip() for ref in source_refs if str(ref).strip()]
    else:
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token source refs are invalid")
    access_policy = payload.get("access_policy")
    if access_policy is None:
        payload["access_policy"] = {}
    elif not isinstance(access_policy, dict):
        raise WorkflowMcpSessionError("workflow_mcp.token_invalid", "workflow MCP token access policy is invalid")
    return payload


# ---------------------------------------------------------------------------
# Session ID derivation
# ---------------------------------------------------------------------------

def _session_id_from_token(token: str) -> str:
    """Derive a stable session ID from the token (first 32 chars of hash)."""
    return digest_bytes_hex(token.encode("utf-8"), purpose="workflow_mcp.session_id")[:32]


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
