"""Agent Sessions surface — persistent Claude session management.

Standalone FastAPI app. Bind host/port default to PRAXIS_AGENT_SESSIONS_HOST
and PRAXIS_AGENT_SESSIONS_PORT when set, otherwise 127.0.0.1:8421.

Run:
    python Code&DBs/Workflow/surfaces/api/agent_sessions.py
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
import re
import secrets
import subprocess
import time
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request, Security
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from adapters.permission_matrix import (
    ALLOWED_PERMISSION_MODES,
    API_PROVIDERS,
    NormalizedPermissionMode,
    PermissionMatrixError,
    SUPPORTED_CLI_PROVIDERS,
    api_permission_prompt_suffix,
    is_permission_step_up,
    translate_permission_flags,
)

__all__ = ["app"]


PRAXIS_ROOT = Path(__file__).resolve().parents[4]
ARTIFACTS_DIR = PRAXIS_ROOT / "artifacts"
AGENTS_DIR = ARTIFACTS_DIR / "agents"
_PUBLIC_AUTH_TOKEN_ENV = "PRAXIS_API_TOKEN"
_TRUST_TAILSCALE_ENV = "PRAXIS_OPERATOR_TRUST_TAILSCALE"
_AGENT_SESSIONS_HOST_ENV = "PRAXIS_AGENT_SESSIONS_HOST"
_AGENT_SESSIONS_PORT_ENV = "PRAXIS_AGENT_SESSIONS_PORT"
_RUNNER_URL_ENV = "PRAXIS_AGENT_SESSIONS_RUNNER_URL"
_HTTP_BEARER = HTTPBearer(auto_error=False)
_TAILSCALE_CGNAT = ipaddress.ip_network("100.64.0.0/10")


def _claude_cwd(env: dict[str, str] | None = None) -> Path:
    source = env if env is not None else os.environ
    configured = (source.get("PRAXIS_AGENT_CWD") or source.get("PRAXIS_REPO_ROOT") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return PRAXIS_ROOT


def _public_api_token(env: dict[str, str] | None = None) -> str | None:
    source = env if env is not None else os.environ
    value = (source.get(_PUBLIC_AUTH_TOKEN_ENV) or "").strip()
    return value or None


def _trust_tailscale_operator(env: dict[str, str] | None = None) -> bool:
    source = env if env is not None else os.environ
    return str(source.get(_TRUST_TAILSCALE_ENV) or "").strip().lower() in {"1", "true", "yes"}


def _is_tailscale_client(request: Request) -> bool:
    client = getattr(request, "client", None)
    host = str(getattr(client, "host", "") or "").strip()
    if not host:
        return False
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return addr in _TAILSCALE_CGNAT or addr.is_loopback


def _agent_sessions_host(env: dict[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    value = (source.get(_AGENT_SESSIONS_HOST_ENV) or "127.0.0.1").strip()
    return value or "127.0.0.1"


def _agent_sessions_port(env: dict[str, str] | None = None) -> int:
    source = env if env is not None else os.environ
    raw_value = (source.get(_AGENT_SESSIONS_PORT_ENV) or "8421").strip()
    try:
        port = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{_AGENT_SESSIONS_PORT_ENV} must be an integer") from exc
    if port <= 0 or port > 65535:
        raise ValueError(f"{_AGENT_SESSIONS_PORT_ENV} must be between 1 and 65535")
    return port


def _runner_url(env: dict[str, str] | None = None) -> str | None:
    source = env if env is not None else os.environ
    value = str(source.get(_RUNNER_URL_ENV) or "").strip().rstrip("/")
    return value or None


def _forward_auth_headers(request: Request) -> dict[str, str]:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    cookie = str(request.headers.get("cookie") or "").strip()
    if cookie:
        headers["Cookie"] = cookie
    authorization = str(request.headers.get("authorization") or "").strip()
    if authorization:
        headers["Authorization"] = authorization
    return headers


def _runner_json_request(
    *,
    method: str,
    path: str,
    headers: dict[str, str],
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_url = _runner_url()
    if not base_url:
        raise RuntimeError("runner URL is not configured")
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            detail = json.loads(raw)
        except json.JSONDecodeError:
            detail = {"message": raw or exc.reason}
        raise HTTPException(status_code=exc.code, detail=detail) from exc
    except OSError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": f"agent session runner unavailable: {exc}",
                "error_code": "agent_sessions_runner_unavailable",
            },
        ) from exc


async def _proxy_runner_json(
    *,
    method: str,
    path: str,
    headers: dict[str, str],
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        _runner_json_request,
        method=method,
        path=path,
        headers=headers,
        body=body,
    )


async def _require_agent_session_access(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(_HTTP_BEARER),
) -> dict[str, str]:
    if _trust_tailscale_operator() or _is_tailscale_client(request):
        request.state.authenticated_principal = "operator_tailscale"
        return _auth_payload(principal_ref="operator:tailscale", auth_kind="tailscale")

    expected_token = _public_api_token()

    if credentials is not None and str(credentials.scheme).lower() == "bearer":
        if expected_token is None:
            raise HTTPException(
                status_code=503,
                detail={
                    "message": "PRAXIS_API_TOKEN is required before bearer agent sessions can run",
                    "error_code": "agent_sessions_auth_not_configured",
                },
            )
        if not secrets.compare_digest(str(credentials.credentials), expected_token):
            raise HTTPException(
                status_code=403,
                detail={
                    "message": "Bearer token rejected for agent sessions",
                    "error_code": "agent_sessions_auth_rejected",
                },
            )
        request.state.authenticated_principal = "public_api_token"
        return _auth_payload(principal_ref="public_api_token", auth_kind="bearer")

    if expected_token is None:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "PRAXIS_API_TOKEN is required before agent sessions can run",
                "error_code": "agent_sessions_auth_not_configured",
            },
        )

    if credentials is None or str(credentials.scheme).lower() != "bearer":
        raise HTTPException(
            status_code=401,
            detail={
                "message": "Bearer token required for agent sessions",
                "error_code": "agent_sessions_auth_required",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

_STREAM_IDLE_TIMEOUT = 5.0
_TURN_TERMINATION_TIMEOUT = 2.0
_TURN_TIMEOUT_ENV = "PRAXIS_AGENT_TURN_TIMEOUT_SECONDS"
_PERMISSION_MODE_ENV = "PRAXIS_AGENT_PERMISSION_MODE"
_CLI_PROVIDER_ENV = "PRAXIS_AGENT_CLI_PROVIDER"
_CODEX_SANDBOX_ENV = "PRAXIS_AGENT_CODEX_SANDBOX"
_OPENROUTER_MODEL_ENV = "PRAXIS_AGENT_OPENROUTER_MODEL"
_OPERATOR_CONSOLE_PROVIDER = "together"
_TOGETHER_MODEL = "deepseek-ai/DeepSeek-V4-Pro"
_TOGETHER_ENDPOINT = "https://api.together.xyz/v1/chat/completions"
_TOGETHER_KEY_NAME = "TOGETHER_API_KEY"
_PRAXIS_TOOL_TIMEOUT_SECONDS = 45.0
_PRAXIS_READ_TOOLS = frozenset(
    {
        "praxis_orient",
        "praxis_query",
        "praxis_search",
        "praxis_discover",
        "praxis_recall",
        "praxis_data_dictionary",
        "praxis_model_access_control_matrix",
        "praxis_provider_control_plane",
        "praxis_receipts",
        "praxis_bugs",
        "praxis_health",
        "praxis_status_snapshot",
        "praxis_run",
    }
)
_TOGETHER_TIMESTAMP_JUNK_RE = re.compile(r"\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)\b")
_TOGETHER_WORD_DIGIT_JUNK_RE = re.compile(r"(?<=[a-z])\d{2,3}(?=\s|$)")
_DEFAULT_TURN_TIMEOUT_SECONDS = 180.0
_DEFAULT_PERMISSION_MODE = "dontAsk"


app = FastAPI(title="Agent Sessions", version="1.0.0")

_agent_locks: dict[str, asyncio.Lock] = {}
_agent_queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}
_agent_processes: dict[str, asyncio.subprocess.Process] = {}
_active_turns: set[str] = set()
_claimed_turns: set[str] = set()
_subsystems: Any | None = None


class CreateAgentRequest(BaseModel):
    title: str | None = None
    prompt: str | None = None
    provider: str | None = None
    permission_mode: str | None = None


class SendMessageRequest(BaseModel):
    prompt: str
    permission_mode: str | None = None


def _validate_permission_mode(value: str | None) -> NormalizedPermissionMode | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if normalized not in ALLOWED_PERMISSION_MODES:
        raise HTTPException(
            status_code=400,
            detail={
                "message": (
                    f"unknown permission_mode {normalized!r}; "
                    f"allowed: {list(ALLOWED_PERMISSION_MODES)}"
                ),
                "error_code": "agent_sessions_invalid_permission_mode",
            },
        )
    return normalized  # type: ignore[return-value]


def _agent_sessions_pg_conn() -> Any:
    return _agent_sessions_subsystems().get_pg_conn()


def _agent_sessions_subsystems() -> Any:
    factory = getattr(app.state, "pg_conn_factory", None)
    if callable(factory):
        from types import SimpleNamespace

        return SimpleNamespace(get_pg_conn=factory)

    global _subsystems
    if _subsystems is None:
        from surfaces.api.handlers._subsystems import _Subsystems

        _subsystems = _Subsystems()
    return _subsystems


def _auth_payload(
    *,
    principal_ref: str,
    auth_kind: str,
) -> dict[str, str]:
    return {"principal_ref": principal_ref, "auth_kind": auth_kind}


INTERACTIVE_SESSION_KIND = "interactive_cli"
INTERACTIVE_WORKFLOW_ID = "interactive_cli"
INTERACTIVE_JOB_LABEL = "interactive"


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _decode_json(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return value


def _session_from_row(row: Any) -> dict[str, Any]:
    data = dict(row)
    return {
        "agent_id": data["session_id"],
        "session_id": data["external_session_id"],
        "title": data.get("display_title") or data["session_id"],
        "provider": data.get("agent_slug") or "claude",
        "principal_ref": data.get("principal_ref") or "",
        "workspace_ref": data.get("workspace_ref") or "",
        "status": data.get("status") or "",
        "running": False,
        "created_at": data.get("created_at"),
        "last_activity": data.get("last_activity_at") or data.get("heartbeat_at"),
    }


def create_interactive_agent_session(
    conn: Any,
    *,
    agent_id: str,
    cli_session_id: str,
    title: str,
    provider_slug: str,
    principal_ref: str,
    workspace_ref: str,
) -> dict[str, Any]:
    rows = conn.execute(
        """
        INSERT INTO agent_sessions (
            session_id, run_id, workflow_id, job_label, agent_slug, status,
            session_kind, external_session_id, display_title, principal_ref,
            workspace_ref, context_json, last_activity_at, heartbeat_at
        ) VALUES (
            $1, $2, $3, $4, $5, 'active',
            $6, $7, $8, $9, $10, '{}'::jsonb, now(), now()
        )
        ON CONFLICT (session_id) DO UPDATE SET
            status = 'active',
            session_kind = EXCLUDED.session_kind,
            external_session_id = EXCLUDED.external_session_id,
            display_title = EXCLUDED.display_title,
            principal_ref = EXCLUDED.principal_ref,
            workspace_ref = EXCLUDED.workspace_ref,
            last_activity_at = now(),
            heartbeat_at = now(),
            revoked_at = NULL,
            revoked_by = NULL,
            revoke_reason = NULL
        RETURNING session_id, external_session_id, display_title, agent_slug, principal_ref,
                  workspace_ref, status, created_at, last_activity_at, heartbeat_at
        """,
        agent_id,
        f"interactive:{agent_id}",
        INTERACTIVE_WORKFLOW_ID,
        INTERACTIVE_JOB_LABEL,
        provider_slug,
        INTERACTIVE_SESSION_KIND,
        cli_session_id,
        title,
        principal_ref,
        workspace_ref,
    )
    session = _session_from_row(rows[0])
    append_interactive_agent_event(
        conn,
        agent_id=agent_id,
        event_kind="session.created",
        payload={"title": title, "cli_session_id": cli_session_id, "workspace_ref": workspace_ref},
    )
    return session


def get_interactive_agent_session(conn: Any, *, agent_id: str) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT session_id, external_session_id, display_title, agent_slug, principal_ref,
               workspace_ref, status, created_at, last_activity_at, heartbeat_at
        FROM agent_sessions
        WHERE session_id = $1
          AND session_kind = $2
          AND revoked_at IS NULL
        """,
        agent_id,
        INTERACTIVE_SESSION_KIND,
    )
    if not rows:
        return None
    return _session_from_row(rows[0])


def list_interactive_agent_sessions(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT session_id, external_session_id, display_title, agent_slug, principal_ref,
               workspace_ref, status, created_at, last_activity_at, heartbeat_at
        FROM agent_sessions
        WHERE session_kind = $1
          AND revoked_at IS NULL
        ORDER BY last_activity_at DESC, created_at DESC
        """,
        INTERACTIVE_SESSION_KIND,
    )
    return [_session_from_row(row) for row in rows]


def update_interactive_agent_cli_session(
    conn: Any,
    *,
    agent_id: str,
    cli_session_id: str,
    provider_slug: str,
) -> None:
    conn.execute(
        """
        UPDATE agent_sessions
        SET external_session_id = $2,
            agent_slug = $3,
            last_activity_at = now(),
            heartbeat_at = now()
        WHERE session_id = $1
          AND session_kind = $4
          AND revoked_at IS NULL
        """,
        agent_id,
        cli_session_id,
        provider_slug,
        INTERACTIVE_SESSION_KIND,
    )


def append_interactive_agent_event(
    conn: Any,
    *,
    agent_id: str,
    event_kind: str,
    payload: dict[str, Any] | None = None,
    text_content: str | None = None,
) -> int | None:
    rows = conn.execute(
        """
        WITH updated AS (
            UPDATE agent_sessions
            SET last_activity_at = now(),
                heartbeat_at = now()
            WHERE session_id = $1
              AND session_kind = $2
              AND revoked_at IS NULL
            RETURNING session_id
        )
        INSERT INTO agent_session_events (
            session_id, event_kind, payload_json, text_content
        )
        SELECT session_id, $3, $4::jsonb, $5
        FROM updated
        RETURNING event_id
        """,
        agent_id,
        INTERACTIVE_SESSION_KIND,
        event_kind,
        _json_dump(payload or {}),
        text_content,
    )
    if not rows:
        return None
    return int(rows[0]["event_id"])


def _most_recent_permission_mode(conn: Any, *, agent_id: str) -> str | None:
    """Return the permission_mode from the most recent event that recorded one.

    Scans the last 20 events for this agent — more than enough for any
    practical turn depth — and returns the first payload-embedded
    permission_mode it finds. None when the agent has no prior mode.
    """
    rows = conn.execute(
        """
        SELECT payload_json
        FROM agent_session_events
        WHERE session_id = $1
        ORDER BY event_id DESC
        LIMIT 20
        """,
        agent_id,
    )
    for row in rows:
        data = dict(row)
        payload = _decode_json(data.get("payload_json"))
        if isinstance(payload, dict):
            mode = payload.get("permission_mode")
            if isinstance(mode, str) and mode:
                return mode
    return None


def list_interactive_agent_events(conn: Any, *, agent_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT event_id, event_kind, payload_json, text_content, created_at
        FROM agent_session_events
        WHERE session_id = $1
        ORDER BY event_id ASC
        """,
        agent_id,
    )
    events: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        events.append(
            {
                "event_id": data["event_id"],
                "type": data["event_kind"],
                "event_kind": data["event_kind"],
                "payload": _decode_json(data.get("payload_json")),
                "text": data.get("text_content"),
                "text_content": data.get("text_content"),
                "created_at": data.get("created_at"),
            }
        )
    return events


def terminate_interactive_agent_session(
    conn: Any,
    *,
    agent_id: str,
    terminated_by: str,
    reason: str,
) -> None:
    append_interactive_agent_event(
        conn,
        agent_id=agent_id,
        event_kind="session.terminated",
        payload={"terminated_by": terminated_by, "reason": reason},
    )
    conn.execute(
        """
        UPDATE agent_sessions
        SET status = 'terminated',
            last_activity_at = now(),
            heartbeat_at = now(),
            revoked_at = COALESCE(revoked_at, now()),
            revoked_by = COALESCE(revoked_by, $2),
            revoke_reason = COALESCE(revoke_reason, $3)
        WHERE session_id = $1
          AND session_kind = $4
        """,
        agent_id,
        terminated_by,
        reason,
        INTERACTIVE_SESSION_KIND,
    )


@app.get("/")
async def service_index() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "agent_sessions",
        "routes": [
            "/agents",
            "/agents/{agent_id}/messages",
            "/agents/{agent_id}/stream",
            "/agents/{agent_id}",
        ],
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_agent_id(agent_id: str) -> str:
    try:
        parsed = UUID(str(agent_id), version=4)
    except (TypeError, ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail={
                "message": "agent_id must be a canonical UUIDv4",
                "error_code": "invalid_agent_id",
            },
        ) from None
    normalized = str(parsed)
    if str(agent_id) != normalized:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "agent_id must be a canonical UUIDv4",
                "error_code": "invalid_agent_id",
            },
        )
    return normalized


def _agent_dir(agent_id: str) -> Path:
    normalized = _normalize_agent_id(agent_id)
    base = AGENTS_DIR.resolve()
    path = (base / normalized).resolve(strict=False)
    try:
        path.relative_to(base)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "agent_id must resolve inside the agent session store",
                "error_code": "invalid_agent_id",
            },
        ) from None
    return path


def _meta_path(agent_id: str) -> Path:
    return _agent_dir(agent_id) / "meta.json"


def _messages_path(agent_id: str) -> Path:
    return _agent_dir(agent_id) / "messages.jsonl"


def _read_meta(agent_id: str) -> dict[str, Any] | None:
    path = _meta_path(agent_id)
    try:
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _write_meta_atomic(agent_id: str, meta: dict[str, Any]) -> None:
    path = _meta_path(agent_id)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f"{path.name}.tmp")
        tmp_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp_path, path)
    except OSError as exc:
        print(f"[agent_sessions] compatibility meta export skipped: {exc}", flush=True)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False))
            fh.write("\n")
    except OSError as exc:
        print(f"[agent_sessions] compatibility event export skipped: {exc}", flush=True)


def _get_lock(agent_id: str) -> asyncio.Lock:
    lock = _agent_locks.get(agent_id)
    if lock is None:
        lock = asyncio.Lock()
        _agent_locks[agent_id] = lock
    return lock


def _get_queue(agent_id: str) -> asyncio.Queue[dict[str, Any]]:
    queue = _agent_queues.get(agent_id)
    if queue is None:
        queue = asyncio.Queue()
        _agent_queues[agent_id] = queue
    return queue


def _claim_turn(agent_id: str) -> None:
    if agent_id in _claimed_turns:
        raise HTTPException(status_code=409, detail="a turn is already in flight for this agent")
    _claimed_turns.add(agent_id)


def _release_turn(agent_id: str) -> None:
    _claimed_turns.discard(agent_id)


def _event_text(event: dict[str, Any]) -> str:
    event_type = str(event.get("type") or "")

    if event_type == "item.completed":
        item = event.get("item")
        if isinstance(item, dict) and item.get("type") == "agent_message":
            text = item.get("text")
            if text:
                return str(text)

    if event_type == "content_block_delta":
        delta = event.get("delta")
        if isinstance(delta, dict) and delta.get("type") == "text_delta":
            return str(delta.get("text") or "")

    if event_type == "assistant":
        message = event.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts: list[str] = []
                for item in content:
                    if isinstance(item, dict):
                        text = item.get("text")
                        if text:
                            parts.append(str(text))
                    elif isinstance(item, str):
                        parts.append(item)
                return "".join(parts)

    if event_type == "result":
        result = event.get("result")
        if isinstance(result, str):
            return result
        if isinstance(result, dict):
            text = result.get("text")
            if text:
                return str(text)

    return ""


def _final_reply_from_events(events: list[dict[str, Any]]) -> str:
    pieces: list[str] = []
    for event in events:
        text = _event_text(event)
        if text:
            pieces.append(text)
    reply = "".join(pieces).strip()
    if reply:
        return reply

    for event in reversed(events):
        if str(event.get("type") or "") == "result":
            result = event.get("result")
            if isinstance(result, str):
                return result.strip()
            if isinstance(result, dict):
                text = result.get("text")
                if text:
                    return str(text).strip()
    return ""


def _turn_timeout_seconds(env: dict[str, str] | None = None) -> float:
    source = env if env is not None else os.environ
    raw_value = str(source.get(_TURN_TIMEOUT_ENV) or "").strip()
    if not raw_value:
        return _DEFAULT_TURN_TIMEOUT_SECONDS
    try:
        value = float(raw_value)
    except ValueError:
        return _DEFAULT_TURN_TIMEOUT_SECONDS
    if value <= 0:
        return _DEFAULT_TURN_TIMEOUT_SECONDS
    return value


def _claude_permission_mode(env: dict[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    value = str(source.get(_PERMISSION_MODE_ENV) or "").strip()
    return value or _DEFAULT_PERMISSION_MODE


def _cli_provider(value: str | None = None, env: dict[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    provider = str(value or source.get(_CLI_PROVIDER_ENV) or "claude").strip().lower()
    allowed = SUPPORTED_CLI_PROVIDERS | API_PROVIDERS
    if provider not in allowed:
        raise HTTPException(
            status_code=400,
            detail={
                "message": (
                    "Unsupported agent provider. Supported providers: "
                    + ", ".join(sorted(allowed))
                ),
                "error_code": "agent_provider_unsupported",
                "provider": provider,
            },
        )
    return provider


def _codex_sandbox(env: dict[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    return str(source.get(_CODEX_SANDBOX_ENV) or "").strip() or None


def _claude_subprocess_env(env: dict[str, str] | None = None) -> dict[str, str]:
    process_env = dict(env if env is not None else os.environ)
    for key in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"):
        if not str(process_env.get(key) or "").strip():
            process_env.pop(key, None)
    return process_env


def _build_claude_command(
    session_id: str,
    prompt: str,
    env: dict[str, str] | None = None,
    *,
    permission_mode: NormalizedPermissionMode | None = None,
) -> list[str]:
    if permission_mode is not None:
        permission_flags = list(translate_permission_flags("claude", permission_mode))
    else:
        permission_flags = ["--permission-mode", _claude_permission_mode(env)]
    return [
        "claude",
        "-p",
        "--session-id",
        session_id,
        "--output-format",
        "stream-json",
        *permission_flags,
        prompt,
    ]


def _is_codex_thread_id(session_id: str) -> bool:
    return session_id.startswith("019") and len(session_id) >= 36


def _build_codex_command(
    session_id: str,
    prompt: str,
    output_path: Path,
    env: dict[str, str] | None = None,
    *,
    permission_mode: NormalizedPermissionMode | None = None,
) -> list[str]:
    base = ["codex", "exec"]
    if _is_codex_thread_id(session_id):
        base.extend(["resume", session_id])
    core = [
        "--json",
        "--output-last-message",
        str(output_path),
        "--cd",
        str(_claude_cwd(env)),
    ]
    if permission_mode is not None:
        core.extend(translate_permission_flags("codex", permission_mode))
    else:
        sandbox = _codex_sandbox(env)
        if sandbox:
            core.extend(["--sandbox", sandbox])
    core.append(prompt)
    base.extend(core)
    return base


def _gemini_subprocess_env(env: dict[str, str] | None = None) -> dict[str, str]:
    """Environment for the gemini subprocess.

    Preserves `GEMINI_API_KEY`, `GOOGLE_API_KEY`, `GOOGLE_APPLICATION_CREDENTIALS`,
    and the OAuth files under `~/.gemini/` that the CLI reads for auth. Drops
    nothing — gemini auth is looser than the Anthropic-CLI constraint (no
    CLI-only standing order here).
    """
    return dict(env if env is not None else os.environ)


def _gemini_model(env: dict[str, str] | None = None) -> str | None:
    source = env if env is not None else os.environ
    value = str(source.get("PRAXIS_AGENT_GEMINI_MODEL") or "").strip()
    return value or None


def _gemini_resume_enabled(conn: Any, *, agent_id: str) -> bool:
    """True if this agent has at least one prior assistant.reply.

    Gemini identifies sessions by a shifting integer index (``-r 5``) or
    the alias ``-r latest``. We use ``latest`` because it survives other
    concurrent gemini activity on the same project without breaking our
    own continuity: once this agent has produced a reply, ``latest``
    points to the most recently written session — which will be this
    agent's — for the duration of the next turn.
    """
    try:
        events = list_interactive_agent_events(conn, agent_id=agent_id)
    except Exception:
        return False
    for ev in events:
        kind = str(ev.get("event_kind") or ev.get("type") or "")
        if kind == "assistant.reply":
            return True
    return False


def _build_gemini_command(
    session_id: str,
    prompt: str,
    env: dict[str, str] | None = None,
    *,
    permission_mode: NormalizedPermissionMode | None = None,
    resume: bool = False,
) -> list[str]:
    """Build argv for a single non-interactive gemini turn.

    Gemini's session model uses project-wide indexes (``gemini --list-sessions``,
    ``gemini -r <index>``). When ``resume`` is true, the builder appends
    ``-r latest`` so the turn continues whatever gemini session was most
    recently active for the project. ``session_id`` is accepted for
    signature parity with :func:`_build_claude_command` and
    :func:`_build_codex_command` and is otherwise unused.
    """
    del session_id
    cmd = ["gemini", "-p", prompt, "-o", "stream-json"]
    if resume:
        cmd.extend(["-r", "latest"])
    model = _gemini_model(env)
    if model:
        cmd.extend(["--model", model])
    if permission_mode is not None:
        cmd.extend(translate_permission_flags("gemini", permission_mode))
    return cmd


def _thread_id_from_events(events: list[dict[str, Any]], fallback: str) -> str:
    for event in events:
        if str(event.get("type") or "") == "thread.started":
            thread_id = str(event.get("thread_id") or "").strip()
            if thread_id:
                return thread_id
    return fallback


def _openrouter_model(env: dict[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    model = str(source.get(_OPENROUTER_MODEL_ENV) or "").strip()
    return model or "deepseek/deepseek-v4-pro"


def _prompt_json(value: Any, *, max_chars: int = 12000) -> str:
    text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 32] + "\n...[truncated for prompt budget]"


def _compact_tool_result_for_prompt(value: Any) -> Any:
    noisy_keys = {
        "operation_receipt",
        "projected_at",
        "source_refs",
        "cost_metadata",
        "created_at",
        "updated_at",
        "last_activity",
    }
    if isinstance(value, dict):
        return {
            key: _compact_tool_result_for_prompt(item)
            for key, item in value.items()
            if key not in noisy_keys
        }
    if isinstance(value, list):
        return [_compact_tool_result_for_prompt(item) for item in value[:20]]
    return value


def _internal_praxis_lookup_message(call: dict[str, Any], result: dict[str, Any]) -> dict[str, str]:
    """Build an ephemeral model-only context message for one Praxis lookup.

    This must never be persisted or replayed as user/assistant conversation.
    The visible chat stays human; Praxis lookup machinery stays internal to
    the current model call.
    """
    packet = {
        "requested_lookup": {
            "tool": call.get("tool"),
            "input": call.get("input") or {},
        },
        "lookup_result": _compact_tool_result_for_prompt(result),
    }
    return {
        "role": "system",
        "content": (
            "INTERNAL PRAXIS LOOKUP CONTEXT. This is not user-authored chat and "
            "not visible conversation history. Use it only as factual context for "
            "answering Nate's latest message. Do not quote raw JSON, timestamps, "
            "receipt IDs, hashes, or protocol fields. Do not request another "
            "lookup in this reply.\n\n"
            + _prompt_json(packet, max_chars=10000)
        ),
    }


def _clean_api_reply_text(text: str, *, provider_slug: str) -> str:
    if provider_slug != "together" or not text:
        return text
    cleaned = _TOGETHER_TIMESTAMP_JUNK_RE.sub("", text)
    cleaned = _TOGETHER_WORD_DIGIT_JUNK_RE.sub("", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _orient_context_for_prompt() -> str:
    try:
        from surfaces.api.handlers.workflow_admin import _handle_orient

        payload = _handle_orient(
            _agent_sessions_subsystems(),
            {"fast": True, "compact": True, "skip_engineering_observability": True},
        )
        packet = {
            "instruction_authority": payload.get("instruction_authority"),
            "standing_orders": payload.get("standing_orders"),
            "tool_guidance": payload.get("tool_guidance"),
            "search_surfaces": payload.get("search_surfaces"),
            "cli_surface": payload.get("cli_surface"),
            "endpoints": payload.get("endpoints"),
            "capabilities": payload.get("capabilities"),
            "instructions": payload.get("instructions"),
        }
        return _prompt_json(packet, max_chars=16000)
    except Exception as exc:
        return _prompt_json({"error": f"orient packet unavailable: {type(exc).__name__}: {exc}"})


def _tool_catalog_context_for_prompt() -> str:
    preferred = {
        "praxis_orient",
        "praxis_query",
        "praxis_search",
        "praxis_discover",
        "praxis_recall",
        "praxis_workflow",
        "praxis_run",
        "praxis_bugs",
        "praxis_receipts",
        "praxis_data_dictionary",
        "praxis_model_access_control_matrix",
        "praxis_provider_control_plane",
        "praxis_operator_architecture_policy",
        "praxis_operator_decisions",
    }
    try:
        from surfaces.mcp.catalog import get_tool_catalog

        catalog = get_tool_catalog()
        rows: list[dict[str, Any]] = []
        for name in sorted(catalog):
            if name not in preferred and len(rows) >= 40:
                continue
            definition = catalog[name]
            if isinstance(definition, dict):
                getter = definition.get
            else:
                getter = lambda key, default=None, obj=definition: getattr(obj, key, default)
            rows.append(
                {
                    "tool": name,
                    "entrypoint": getter("cli_entrypoint") or getter("entrypoint"),
                    "description": getter("description"),
                    "when_to_use": getter("when_to_use"),
                    "example_call": getter("example_call"),
                    "input_schema": getter("input_schema"),
                }
            )
        rows.sort(key=lambda row: (row["tool"] not in preferred, row["tool"]))
        return _prompt_json(rows[:40], max_chars=18000)
    except Exception as exc:
        return _prompt_json({"error": f"tool catalog unavailable: {type(exc).__name__}: {exc}"})


def _api_provider_messages(
    pg_conn: Any | None,
    *,
    agent_id: str,
    prompt: str,
    provider_slug: str,
    permission_mode: NormalizedPermissionMode | None = None,
) -> list[dict[str, str]]:
    if pg_conn is not None:
        base_system = _build_praxis_context(pg_conn)
    else:
        base_system = (
            "You are the Praxis operator's persistent agent-session conversation. "
            "Be concise, direct, and preserve continuity from the visible prior turns."
        )
    suffix = api_permission_prompt_suffix(provider_slug, permission_mode)
    messages: list[dict[str, str]] = [
        {"role": "system", "content": base_system + suffix},
    ]
    if pg_conn is not None:
        for event in list_interactive_agent_events(pg_conn, agent_id=agent_id):
            kind = str(event.get("event_kind") or event.get("type") or "")
            text = str(event.get("text_content") or event.get("text") or "").strip()
            if not text:
                continue
            if kind == "user.prompt":
                messages.append({"role": "user", "content": text})
            elif kind == "assistant.reply":
                messages.append({"role": "assistant", "content": text})
    if not messages or messages[-1].get("role") != "user":
        messages.append({"role": "user", "content": prompt})
    return messages[-20:]


def _openrouter_messages(
    pg_conn: Any | None,
    *,
    agent_id: str,
    prompt: str,
    permission_mode: NormalizedPermissionMode | None = None,
) -> list[dict[str, str]]:
    return _api_provider_messages(
        pg_conn,
        agent_id=agent_id,
        prompt=prompt,
        provider_slug="openrouter",
        permission_mode=permission_mode,
    )


def _together_messages(
    pg_conn: Any | None,
    *,
    agent_id: str,
    prompt: str,
    permission_mode: NormalizedPermissionMode | None = None,
) -> list[dict[str, str]]:
    return _api_provider_messages(
        pg_conn,
        agent_id=agent_id,
        prompt=prompt,
        provider_slug="together",
        permission_mode=permission_mode,
    )


def _chat_completion_json_request(
    *,
    api_key: str,
    endpoint: str,
    model: str,
    messages: list[dict[str, str]],
    timeout_seconds: float,
    headers: dict[str, str] | None = None,
    max_tokens: int = 1200,
) -> dict[str, Any]:
    body = json.dumps(
        {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
        }
    ).encode("utf-8")
    request_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": "Praxis Operator Console",
    }
    request_headers.update(headers or {})
    req = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers=request_headers,
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _chat_completion_reply_text(payload: dict[str, Any], *, provider_slug: str) -> str:
    choice = (payload.get("choices") or [{}])[0]
    message = choice.get("message") if isinstance(choice, dict) else {}
    reply = _clean_api_reply_text(
        str((message or {}).get("content") or "").strip(),
        provider_slug=provider_slug,
    )
    if not reply:
        raise RuntimeError(f"{provider_slug} returned an empty assistant reply")
    return reply


def _extract_praxis_tool_call(text: str) -> dict[str, Any] | None:
    candidate = text.strip()
    if candidate.startswith("```"):
        lines = candidate.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        candidate = "\n".join(lines).strip()
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        payload = json.loads(candidate[start : end + 1])
    except json.JSONDecodeError:
        return None
    call = payload.get("praxis_tool_call") if isinstance(payload, dict) else None
    if not isinstance(call, dict):
        return None
    tool = str(call.get("tool") or "").strip()
    input_payload = call.get("input") or {}
    if not tool or not isinstance(input_payload, dict):
        return None
    return {"tool": tool, "input": input_payload}


def _run_praxis_tool_for_console(call: dict[str, Any]) -> dict[str, Any]:
    tool = str(call.get("tool") or "").strip()
    input_payload = call.get("input") or {}
    if tool not in _PRAXIS_READ_TOOLS:
        return {
            "ok": False,
            "error_code": "praxis_tool_not_allowed",
            "message": f"{tool!r} is not allowed from the API chat console",
            "allowed_tools": sorted(_PRAXIS_READ_TOOLS),
        }
    if not isinstance(input_payload, dict):
        return {
            "ok": False,
            "error_code": "praxis_tool_invalid_input",
            "message": "tool input must be a JSON object",
        }
    try:
        completed = subprocess.run(
            [
                str(PRAXIS_ROOT / "scripts" / "praxis"),
                "workflow",
                "tools",
                "call",
                tool,
                "--input-json",
                json.dumps(input_payload, ensure_ascii=False),
            ],
            cwd=str(PRAXIS_ROOT),
            env=dict(os.environ),
            text=True,
            capture_output=True,
            timeout=_PRAXIS_TOOL_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "error_code": "praxis_tool_timeout",
            "message": f"{tool} timed out after {_PRAXIS_TOOL_TIMEOUT_SECONDS:.0f}s",
        }
    raw = (completed.stdout or "").strip()
    parsed: Any
    try:
        parsed = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        parsed = {"raw": raw}
    return {
        "ok": completed.returncode == 0,
        "tool": tool,
        "input": input_payload,
        "exit_code": completed.returncode,
        "stdout": parsed,
        "stderr": (completed.stderr or "").strip()[-2000:],
    }


async def _run_api_provider_turn(
    agent_id: str,
    session_id: str,
    prompt: str,
    *,
    provider_slug: str,
    pg_conn: Any | None,
    permission_mode: NormalizedPermissionMode | None = None,
) -> tuple[str, list[dict[str, Any]], int, str]:
    from adapters.keychain import resolve_secret

    trace_events: list[dict[str, Any]] = []
    current_stage = "provider_setup"

    async def _emit_stage(
        stage: str,
        label: str,
        message: str,
        *,
        status: str = "running",
        **extra: Any,
    ) -> None:
        event = {
            "type": "turn.stage",
            "stage": stage,
            "status": status,
            "label": label,
            "message": message,
            **extra,
        }
        trace_events.append(event)
        await _get_queue(agent_id).put(event)

    if provider_slug == "together":
        key_name = _TOGETHER_KEY_NAME
        model = _TOGETHER_MODEL
        endpoint = _TOGETHER_ENDPOINT
        extra_headers: dict[str, str] = {}
        max_tokens = 4096
    else:
        key_name = "OPENROUTER_API_KEY"
        model = _openrouter_model()
        endpoint = "https://openrouter.ai/api/v1/chat/completions"
        extra_headers = {
            "HTTP-Referer": "https://praxis.local/operator-console",
            "X-Title": "Praxis Operator Console",
        }
        max_tokens = 1200

    api_key = str(resolve_secret(key_name, env=dict(os.environ)) or "").strip()
    if not api_key:
        await _emit_stage(
            "provider_key",
            "Provider key missing",
            f"{key_name} is not configured.",
            status="failed",
            provider=provider_slug,
            model=model,
        )
        event = {
            "type": "error",
            "error_code": "agent_provider_not_configured",
            "message": f"{key_name} is not configured in Keychain or environment for the agent session {provider_slug} provider",
            "provider": provider_slug,
            "model": model,
            "stage": "provider_key",
        }
        return event["message"], [*trace_events, event], 78, session_id

    await _emit_stage(
        "provider_key",
        "Provider key found",
        f"{key_name} resolved.",
        status="ok",
        provider=provider_slug,
        model=model,
    )

    messages = _api_provider_messages(
        pg_conn,
        agent_id=agent_id,
        prompt=prompt,
        provider_slug=provider_slug,
        permission_mode=permission_mode,
    )
    try:
        current_stage = "provider_request"
        await _emit_stage(
            "provider_request",
            "Provider request sent",
            f"Sending the turn to {provider_slug}.",
            provider=provider_slug,
            model=model,
        )
        payload = await asyncio.to_thread(
            _chat_completion_json_request,
            api_key=api_key,
            endpoint=endpoint,
            model=model,
            messages=messages,
            timeout_seconds=_turn_timeout_seconds(),
            headers=extra_headers,
            max_tokens=max_tokens,
        )
        current_stage = "provider_response"
        await _emit_stage(
            "provider_response",
            "Provider replied",
            f"{provider_slug} returned a response envelope.",
            status="ok",
            provider=provider_slug,
            model=model,
        )
        reply = _chat_completion_reply_text(payload, provider_slug=provider_slug)
        await _emit_stage(
            "assistant_text",
            "Assistant text parsed",
            "The response contained visible assistant text.",
            status="ok",
            provider=provider_slug,
            model=model,
        )
        event = {
            "type": "assistant",
            "provider": provider_slug,
            "model": model,
            "message": {"content": reply},
        }
        turn_events: list[dict[str, Any]] = []
        tool_call = _extract_praxis_tool_call(reply)
        if tool_call is not None:
            tool_event = {
                "type": "praxis.tool_call",
                "provider": provider_slug,
                "model": model,
                "tool": tool_call["tool"],
                "input": tool_call["input"],
            }
            trace_events.append(tool_event)
            await _get_queue(agent_id).put(
                {
                    "type": "praxis.lookup",
                    "provider": provider_slug,
                    "model": model,
                    "tool": tool_call["tool"],
                    "message": f"Checking Praxis with {tool_call['tool']}.",
                }
            )
            current_stage = "praxis_lookup"
            tool_result = await asyncio.to_thread(_run_praxis_tool_for_console, tool_call)
            result_event = {
                "type": "praxis.tool_result",
                "provider": provider_slug,
                "model": model,
                "tool": tool_call["tool"],
                "result": tool_result,
            }
            trace_events.append(result_event)
            await _get_queue(agent_id).put(
                {
                    "type": "praxis.lookup.done",
                    "provider": provider_slug,
                    "model": model,
                    "tool": tool_call["tool"],
                    "ok": bool(tool_result.get("ok")),
                    "message": "Praxis lookup returned.",
                }
            )
            messages.append(_internal_praxis_lookup_message(tool_call, tool_result))
            current_stage = "final_provider_request"
            await _emit_stage(
                "final_provider_request",
                "Final answer requested",
                "Sending compact Praxis context back to the model.",
                provider=provider_slug,
                model=model,
            )
            payload = await asyncio.to_thread(
                _chat_completion_json_request,
                api_key=api_key,
                endpoint=endpoint,
                model=model,
                messages=messages[-22:],
                timeout_seconds=_turn_timeout_seconds(),
                headers=extra_headers,
                max_tokens=max_tokens,
            )
            current_stage = "final_provider_response"
            await _emit_stage(
                "final_provider_response",
                "Final answer returned",
                f"{provider_slug} returned the final answer envelope.",
                status="ok",
                provider=provider_slug,
                model=model,
            )
            reply = _chat_completion_reply_text(payload, provider_slug=provider_slug)
            await _emit_stage(
                "final_assistant_text",
                "Final text parsed",
                "The final answer contained visible assistant text.",
                status="ok",
                provider=provider_slug,
                model=model,
            )
            event = {
                "type": "assistant",
                "provider": provider_slug,
                "model": model,
                "message": {"content": reply},
            }
            turn_events.append(event)
        else:
            turn_events.append(event)
        turn_events = [*trace_events, *turn_events]
        for turn_event in turn_events:
            if turn_event.get("type") not in {"turn.stage", "praxis.tool_call", "praxis.tool_result"}:
                _append_jsonl(_messages_path(agent_id), turn_event)
            if turn_event.get("type") != "turn.stage":
                await _get_queue(agent_id).put(turn_event)
        return reply, turn_events, 0, session_id
    except Exception as exc:
        event = {
            "type": "error",
            "error_code": "agent_provider_failed",
            "message": f"{type(exc).__name__}: {exc}",
            "provider": provider_slug,
            "model": model,
            "stage": current_stage,
        }
        _append_jsonl(_messages_path(agent_id), event)
        await _get_queue(agent_id).put(event)
        return event["message"], [*trace_events, event], 1, session_id


async def _run_openrouter_turn(
    agent_id: str,
    session_id: str,
    prompt: str,
    *,
    pg_conn: Any | None,
    permission_mode: NormalizedPermissionMode | None = None,
) -> tuple[str, list[dict[str, Any]], int, str]:
    return await _run_api_provider_turn(
        agent_id,
        session_id,
        prompt,
        provider_slug="openrouter",
        pg_conn=pg_conn,
        permission_mode=permission_mode,
    )


async def _run_together_v4_pro_turn(
    agent_id: str,
    session_id: str,
    prompt: str,
    *,
    pg_conn: Any | None,
    permission_mode: NormalizedPermissionMode | None = None,
) -> tuple[str, list[dict[str, Any]], int, str]:
    return await _run_api_provider_turn(
        agent_id,
        session_id,
        prompt,
        provider_slug="together",
        pg_conn=pg_conn,
        permission_mode=permission_mode,
    )


async def _read_stderr(proc: asyncio.subprocess.Process) -> str:
    if proc.stderr is None:
        return ""
    raw = await proc.stderr.read()
    return raw.decode("utf-8", errors="replace").strip()


async def _run_turn(
    agent_id: str,
    session_id: str,
    prompt: str,
    *,
    provider_slug: str,
    pg_conn: Any | None = None,
    permission_mode: NormalizedPermissionMode | None = None,
) -> tuple[str, list[dict[str, Any]], int, str]:
    queue = _get_queue(agent_id)
    messages_path = _messages_path(agent_id)
    reply_file: Path | None = None
    if provider_slug == "openrouter":
        return await _run_openrouter_turn(
            agent_id,
            session_id,
            prompt,
            pg_conn=pg_conn,
            permission_mode=permission_mode,
        )
    if provider_slug == "together":
        return await _run_together_v4_pro_turn(
            agent_id,
            session_id,
            prompt,
            pg_conn=pg_conn,
            permission_mode=permission_mode,
        )
    if provider_slug == "codex":
        fd, raw_reply_file = tempfile.mkstemp(prefix=f"praxis-agent-{agent_id}-", suffix=".txt")
        os.close(fd)
        reply_file = Path(raw_reply_file)
        cmd = _build_codex_command(
            session_id, prompt, reply_file, permission_mode=permission_mode
        )
        spawn_env = _claude_subprocess_env()
    elif provider_slug == "gemini":
        gemini_resume = False
        if pg_conn is not None:
            gemini_resume = _gemini_resume_enabled(pg_conn, agent_id=agent_id)
        cmd = _build_gemini_command(
            session_id, prompt, permission_mode=permission_mode, resume=gemini_resume
        )
        spawn_env = _gemini_subprocess_env()
    else:
        cmd = _build_claude_command(
            session_id, prompt, permission_mode=permission_mode
        )
        spawn_env = _claude_subprocess_env()
    timeout_seconds = _turn_timeout_seconds()

    print(f"[agent_sessions] launching {provider_slug} agent={agent_id}", flush=True)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(_claude_cwd()),
        env=spawn_env,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _agent_processes[agent_id] = proc
    _active_turns.add(agent_id)

    turn_events: list[dict[str, Any]] = []

    try:
        assert proc.stdout is not None
        try:
            while True:
                raw_line = await asyncio.wait_for(proc.stdout.readline(), timeout=timeout_seconds)
                if not raw_line:
                    break
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        event = {"type": "raw", "data": event}
                except json.JSONDecodeError:
                    event = {"type": "raw", "data": line}

                turn_events.append(event)
                _append_jsonl(messages_path, event)
                if pg_conn is not None:
                    append_interactive_agent_event(
                        pg_conn,
                        agent_id=agent_id,
                        event_kind="cli.event",
                        payload=event,
                        text_content=_event_text(event) or None,
                    )
                await queue.put(event)
        except asyncio.TimeoutError:
            await _terminate_process(agent_id)
            event = {
                "type": "error",
                "error_code": "agent_turn_timeout",
                "message": f"Claude did not produce output within {timeout_seconds:g} seconds",
            }
            turn_events.append(event)
            _append_jsonl(messages_path, event)
            if pg_conn is not None:
                append_interactive_agent_event(
                    pg_conn,
                    agent_id=agent_id,
                    event_kind="cli.error",
                    payload=event,
                    text_content=event["message"],
                )
            await queue.put(event)
            return event["message"], turn_events, 124, session_id

        stderr = await _read_stderr(proc)
        exit_code = await proc.wait()
        if exit_code != 0 and stderr:
            error_event = {
                "type": "error",
                "error_code": "agent_cli_failed",
                "message": stderr[-4000:],
            }
            turn_events.append(error_event)
            _append_jsonl(messages_path, error_event)
            if pg_conn is not None:
                append_interactive_agent_event(
                    pg_conn,
                    agent_id=agent_id,
                    event_kind="cli.error",
                    payload=error_event,
                    text_content=error_event["message"],
                )
            await queue.put(error_event)
        reply = _final_reply_from_events(turn_events)
        if not reply and reply_file is not None:
            try:
                reply = reply_file.read_text(encoding="utf-8").strip()
            except OSError:
                reply = ""
        effective_session_id = _thread_id_from_events(turn_events, session_id)
        if pg_conn is not None and effective_session_id != session_id:
            update_interactive_agent_cli_session(
                pg_conn,
                agent_id=agent_id,
                cli_session_id=effective_session_id,
                provider_slug=provider_slug,
            )
        print(
            f"[agent_sessions] {provider_slug} done agent={agent_id} code={exit_code} events={len(turn_events)}",
            flush=True,
        )
        return reply, turn_events, int(exit_code), effective_session_id
    finally:
        if reply_file is not None:
            try:
                reply_file.unlink(missing_ok=True)
            except OSError:
                pass
        _active_turns.discard(agent_id)
        _agent_processes.pop(agent_id, None)
        meta = _read_meta(agent_id)
        if meta is not None:
            meta["last_activity"] = _now_iso()
            try:
                _write_meta_atomic(agent_id, meta)
            except Exception as exc:
                print(f"[agent_sessions] meta update error: {exc}", flush=True)


async def _terminate_process(agent_id: str) -> bool:
    proc = _agent_processes.get(agent_id)
    if proc is None:
        return False

    try:
        proc.terminate()
    except ProcessLookupError:
        pass
    except Exception as exc:
        print(f"[agent_sessions] terminate error agent={agent_id}: {exc}", flush=True)

    try:
        await asyncio.wait_for(proc.wait(), timeout=_TURN_TERMINATION_TIMEOUT)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await proc.wait()
    except Exception as exc:
        print(f"[agent_sessions] wait-after-terminate error agent={agent_id}: {exc}", flush=True)

    _agent_processes.pop(agent_id, None)
    _active_turns.discard(agent_id)
    return True


@app.post("/agents")
async def create_agent(
    request: Request,
    body: CreateAgentRequest,
    _auth: dict[str, str] = Security(_require_agent_session_access),
) -> dict[str, Any]:
    if _runner_url() and body.prompt:
        return await _proxy_runner_json(
            method="POST",
            path="/agents",
            headers=_forward_auth_headers(request),
            body=body.model_dump(exclude_none=True),
        )

    pg_conn = _agent_sessions_pg_conn()

    agent_id = str(uuid4())
    session_id = str(uuid4())
    title = body.title or f"agent-{agent_id[:8]}"
    provider_slug = _OPERATOR_CONSOLE_PROVIDER
    now = _now_iso()
    session = create_interactive_agent_session(
        pg_conn,
        agent_id=agent_id,
        cli_session_id=session_id,
        title=title,
        provider_slug=provider_slug,
        principal_ref=_auth["principal_ref"],
        workspace_ref="praxis.default",
    )

    _write_meta_atomic(
        agent_id,
        {
            "agent_id": agent_id,
            "session_id": session_id,
            "title": title,
            "provider": provider_slug,
            "created_at": now,
            "last_activity": now,
        },
    )
    try:
        _messages_path(agent_id).touch(exist_ok=True)
    except OSError as exc:
        print(f"[agent_sessions] compatibility message log skipped: {exc}", flush=True)

    print(f"[agent_sessions] created agent={agent_id} session={session_id}", flush=True)

    reply: str | None = None
    turn_events: list[dict[str, Any]] = []
    exit_code: int | None = None
    if body.prompt:
        validated_mode = _validate_permission_mode(body.permission_mode)
        if validated_mode is not None:
            prior_mode = _most_recent_permission_mode(pg_conn, agent_id=agent_id)
            if is_permission_step_up(prior_mode, validated_mode):
                append_interactive_agent_event(
                    pg_conn,
                    agent_id=agent_id,
                    event_kind="permission.step_up",
                    payload={
                        "principal_ref": _auth["principal_ref"],
                        "from_mode": prior_mode,
                        "to_mode": validated_mode,
                    },
                )
        user_payload: dict[str, Any] = {"principal_ref": _auth["principal_ref"]}
        if validated_mode is not None:
            user_payload["permission_mode"] = validated_mode
        append_interactive_agent_event(
            pg_conn,
            agent_id=agent_id,
            event_kind="user.prompt",
            payload=user_payload,
            text_content=body.prompt,
        )
        _claim_turn(agent_id)
        lock = _get_lock(agent_id)
        await lock.acquire()
        try:
            reply, turn_events, exit_code, session_id = await _run_turn(
                agent_id,
                session_id,
                body.prompt,
                provider_slug=provider_slug,
                pg_conn=pg_conn,
                permission_mode=validated_mode,
            )
        finally:
            lock.release()
            _release_turn(agent_id)
        assistant_payload: dict[str, Any] = {"exit_code": exit_code}
        if validated_mode is not None:
            assistant_payload["permission_mode"] = validated_mode
        append_interactive_agent_event(
            pg_conn,
            agent_id=agent_id,
            event_kind="assistant.reply",
            payload=assistant_payload,
            text_content=reply,
        )

    response_payload = {
        "agent_id": agent_id,
        "session_id": session_id,
        "title": title,
        "provider": provider_slug,
        "authority": session,
    }
    if body.prompt:
        response_payload.update(
            {
                "reply": reply or "",
                "turn_events": turn_events,
                "exit_code": exit_code,
            }
        )
    return response_payload


# In-process cache for the Praxis context system prompt. Rebuilt every TTL
# seconds; same string is reused across API-backed chat turns. Refresh is
# cheap (a few small SELECTs).
_PRAXIS_CONTEXT_CACHE: dict[str, Any] = {"text": None, "expires_at": 0.0}
_PRAXIS_CONTEXT_TTL_SEC = 300


def _build_praxis_context(pg_conn: Any) -> str:
    """Compose a system-prompt block: orient packet + tools + dictionary.

    Used as the system message for every operator-console chat turn so the
    LLM understands what Praxis is, what rules bind it, what surfaces are
    available, and what typed objects flow through the graph.
    """
    now = time.time()
    cached = _PRAXIS_CONTEXT_CACHE.get("text")
    if cached and now < float(_PRAXIS_CONTEXT_CACHE.get("expires_at") or 0):
        return cached

    parts: list[str] = []
    parts.append(
        "You are the Praxis operator console assistant. Praxis is LLM-first "
        "infrastructure powered by a trust compiler — one graph in Praxis.db "
        "(nodes, edges, gates, typed by data-dictionary consumes/produces) with "
        "many lenses (Moon canvas, executor, CLI, MCP, HTTP). Agents build and "
        "mutate the graph; the operator (Nate) steers — approves gates, edits "
        "misbehaving nodes — he does not assemble nodes from a palette.\n\n"
        "This chat lane runs on Together DeepSeek V4 Pro. It is an API-backed "
        "chat transport, not a local shell. Use the Praxis tool catalog and "
        "endpoint descriptions below as the operating map. When the transport "
        "cannot execute a tool directly, give Nate the exact Praxis surface to "
        "invoke and what it will change or inspect. Do not pretend a command, "
        "HTTP request, or MCP call has executed unless a tool result is present."
    )

    parts.append(
        "## Neo operator doctrine\n"
        "Treat Nate as a high-taste operator, not a syntax worker. Bring him "
        "decisions, trade-offs, failure modes, and clean recommendations; do "
        "not make him decipher implementation mechanics unless he asks.\n"
        "- Persona: you are Neo — sharp, skeptical of first answers, pragmatic, "
        "mildly cheeky when it keeps the work honest, and exact when accuracy matters.\n"
        "- Architecture is not sacred. If a pattern is weak, identify the failure, "
        "remove the weak shape, and recommend the simpler durable replacement.\n"
        "- Power comes from control over state plus observability proving that "
        "control is real. Prefer one obvious authority, one explicit reason, "
        "and one verifiable outcome.\n"
        "- Favor durable, inspectable, queryable systems over things that merely "
        "appear to work. Database-backed authority beats script-first convenience "
        "when state, routing, receipts, or coordination matter.\n"
        "- The canonical operator surface is `praxis workflow`: query for state, "
        "discover/recall for context, tools for schemas, bugs for bug authority, "
        "and run/run-status for execution lifecycle. Do not guess tool schemas "
        "or live system state; inspect the Praxis authority.\n"
        "- Optimize every recommendation for single source of truth, deterministic "
        "behavior, inspectability, recoverability, low cognitive load, low blast "
        "radius, and future agent usability.\n"
        "- The customer is the LLM, including future runs. Make contracts, "
        "boundaries, assumptions, examples, and success criteria explicit because "
        "LLMs are smart amnesiacs.\n"
        "- Do not preserve bad structure for compatibility theater. Do not invent "
        "tools, APIs, files, state, or passing tests. If you do not know, say so "
        "and use the tool bridge to inspect what can be inspected.\n"
        "- For reviews, actively look for duplicated authority, hidden state, "
        "implicit contracts, registry drift, event models without durable receipts, "
        "and scripts masquerading as architecture."
    )

    parts.append("## Canonical orient packet\n" + _orient_context_for_prompt())

    parts.append(
        "## MCP and API tool-use map\n"
        "- Canonical first read: `praxis_orient` or HTTP `POST /orient`.\n"
        "- MCP bridge: `POST /mcp` speaks JSON-RPC. Use `tools/list` to inspect available tools and `tools/call` with a tool name plus JSON arguments to invoke one.\n"
        "- CLI equivalent: `praxis workflow tools list`, `praxis workflow tools describe <tool>`, then `praxis workflow tools call <tool> --input-json '{...}'`.\n"
        "- HTTP equivalent: use the endpoints from `/orient#endpoints`; workflow launches are kickoff-first, then inspect status/stream.\n"
        "- Discovery rule: before proposing new code, use `praxis_discover`; for architectural memory use `praxis_recall`; for natural-language operator questions use `praxis_query` or `praxis_search`.\n"
        "- Dictionary rule: when a term, table, event, tool, or contract matters, describe it with `praxis_data_dictionary` instead of guessing."
    )

    parts.append(
        "## Console tool execution protocol\n"
        "This console can execute one read-oriented Praxis tool call for you when you need live state. "
        "To request execution, reply with exactly one JSON object and no markdown:\n"
        '{"praxis_tool_call":{"tool":"praxis_model_access_control_matrix","input":{"runtime_profile_ref":"praxis","job_type":"chat"}}}\n'
        "Allowed read tools include: "
        + ", ".join(sorted(_PRAXIS_READ_TOOLS))
        + ". After the console returns an internal Praxis lookup result, answer Nate from that context. "
        "Do not claim a lookup ran unless an internal lookup result is provided."
    )

    parts.append("## High-value MCP/CLI tools\n" + _tool_catalog_context_for_prompt())

    try:
        rows = pg_conn.execute(
            """
            SELECT title, rationale, decision_kind, decision_scope_kind, decision_scope_ref
            FROM operator_decisions
            WHERE decision_status IN ('active', 'decided')
              AND (decision_kind ILIKE 'architecture%%'
                   OR decision_kind ILIKE 'standing%%'
                   OR decision_kind ILIKE 'product%%'
                   OR decision_kind = 'platform_architecture'
                   OR decision_kind = 'delivery_plan')
            ORDER BY decided_at DESC NULLS LAST
            LIMIT 30
            """
        )
        rule_lines: list[str] = []
        for r in rows or []:
            d = dict(r)
            title = (d.get("title") or "").strip()
            why = (d.get("rationale") or "").strip()
            scope = (d.get("decision_scope_ref") or d.get("decision_scope_kind") or "").strip()
            if not title:
                continue
            line = f"- **{title}**"
            if scope:
                line += f" _(scope: {scope})_"
            if why:
                why_short = why if len(why) <= 240 else why[:237] + "…"
                line += f"\n  WHY: {why_short}"
            rule_lines.append(line)
        if rule_lines:
            parts.append("## Standing orders (active architecture/product policies)\n" + "\n".join(rule_lines))
    except Exception as exc:
        parts.append(f"_(standing orders unavailable: {exc})_")

    try:
        rows = pg_conn.execute(
            """
            SELECT object_kind, summary, category
            FROM data_dictionary_objects
            WHERE summary IS NOT NULL
              AND summary <> ''
              AND summary NOT ILIKE 'Legacy public table discovered%%'
              AND object_kind NOT ILIKE 'pg_%%'
            ORDER BY object_kind
            LIMIT 60
            """
        )
        dd_lines: list[str] = []
        for r in rows or []:
            d = dict(r)
            kind = (d.get("object_kind") or "").strip()
            summary = (d.get("summary") or "").strip()
            if not kind or not summary:
                continue
            summary_short = summary if len(summary) <= 140 else summary[:137] + "…"
            dd_lines.append(f"- `{kind}` — {summary_short}")
        if dd_lines:
            parts.append("## Data dictionary (typed objects you can reason about)\n" + "\n".join(dd_lines))
    except Exception as exc:
        parts.append(f"_(data dictionary unavailable: {exc})_")

    parts.append(
        "## How to respond\n"
        "- Be direct and concise. Nate directs architecture; he does not want syntax homework.\n"
        "- Lead with the verdict when useful, then why, recommended change, trade-offs, validation path, and remaining risk.\n"
        "- Explain in terms of authority, interfaces, failure modes, operational burden, blast radius, and maintainability.\n"
        "- When you reference a Praxis concept, use the exact slug from the data dictionary.\n"
        "- If live state matters, request a read tool call through the console protocol instead of guessing.\n"
        "- If asked to make changes, name the graph row, decision row, registry row, database surface, or operator surface that should change.\n"
        "- Do not fabricate orientation details. If something is unavailable, state the gap plainly and give the grounded next move."
    )

    text = "\n\n".join(parts)
    _PRAXIS_CONTEXT_CACHE["text"] = text
    _PRAXIS_CONTEXT_CACHE["expires_at"] = now + _PRAXIS_CONTEXT_TTL_SEC
    return text


@app.post("/agents/{agent_id}/messages")
async def send_message(
    request: Request,
    agent_id: str,
    body: SendMessageRequest,
    _auth: dict[str, str] = Security(_require_agent_session_access),
) -> dict[str, Any]:
    agent_id = _normalize_agent_id(agent_id)
    if _runner_url():
        return await _proxy_runner_json(
            method="POST",
            path=f"/agents/{agent_id}/messages",
            headers=_forward_auth_headers(request),
            body=body.model_dump(),
        )

    pg_conn = _agent_sessions_pg_conn()
    session = get_interactive_agent_session(pg_conn, agent_id=agent_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"agent {agent_id!r} not found")
    validated_mode = _validate_permission_mode(body.permission_mode)
    if validated_mode is not None:
        prior_mode = _most_recent_permission_mode(pg_conn, agent_id=agent_id)
        if is_permission_step_up(prior_mode, validated_mode):
            append_interactive_agent_event(
                pg_conn,
                agent_id=agent_id,
                event_kind="permission.step_up",
                payload={
                    "principal_ref": _auth["principal_ref"],
                    "from_mode": prior_mode,
                    "to_mode": validated_mode,
                },
            )
    user_payload: dict[str, Any] = {"principal_ref": _auth["principal_ref"]}
    if validated_mode is not None:
        user_payload["permission_mode"] = validated_mode
    append_interactive_agent_event(
        pg_conn,
        agent_id=agent_id,
        event_kind="user.prompt",
        payload=user_payload,
        text_content=body.prompt,
    )
    # Drain any unflushed events left over from a previous turn. The
    # per-agent SSE queue persists across turns, and if the prior turn's
    # client aborted its stream before the assistant.reply frame flushed,
    # that reply sits in the queue. Without draining, the new turn's
    # stream connects, drains the stale reply first, and the user sees
    # the previous answer in place of the current one (only a refresh —
    # which reads from DB — recovers the right sequence).
    queue = _get_queue(agent_id)
    while not queue.empty():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            break
    accepted_event = {
        "type": "turn.accepted",
        "provider": _OPERATOR_CONSOLE_PROVIDER,
        "stage": "stored_message",
        "status": "ok",
        "message": "Praxis received and stored your message.",
    }
    await queue.put(accepted_event)

    _claim_turn(agent_id)
    lock = _get_lock(agent_id)
    await lock.acquire()
    try:
        provider_slug = _OPERATOR_CONSOLE_PROVIDER
        thinking_event = {
            "type": "assistant.thinking",
            "provider": provider_slug,
            "model": _TOGETHER_MODEL,
            "stage": "provider_turn_started",
            "status": "running",
            "message": "DeepSeek is reading the turn.",
        }
        await _get_queue(agent_id).put(thinking_event)
        reply, turn_events, exit_code, effective_session_id = await _run_turn(
            agent_id=agent_id,
            session_id=str(session["session_id"]),
            prompt=body.prompt,
            provider_slug=provider_slug,
            pg_conn=pg_conn,
            permission_mode=validated_mode,
        )
        turn_events = [accepted_event, thinking_event, *turn_events]
    finally:
        lock.release()
        _release_turn(agent_id)
    assistant_payload: dict[str, Any] = {"exit_code": exit_code}
    if validated_mode is not None:
        assistant_payload["permission_mode"] = validated_mode
    error_event = next((ev for ev in turn_events if ev.get("type") == "error"), None)
    if error_event:
        assistant_payload["error_code"] = error_event.get("error_code")
        assistant_payload["error_message"] = error_event.get("message")
        print(f"[agent_sessions] provider failure agent={agent_id} provider={provider_slug} code={error_event.get('error_code')} msg={error_event.get('message')!r}", flush=True)
    text_to_store = reply or (f"[error: {error_event.get('error_code')}] {error_event.get('message','')}" if error_event else "")
    append_interactive_agent_event(
        pg_conn,
        agent_id=agent_id,
        event_kind="assistant.reply",
        payload=assistant_payload,
        text_content=text_to_store,
    )

    return {"reply": reply, "turn_events": turn_events, "exit_code": exit_code, "session_id": effective_session_id, "provider": provider_slug}


@app.get("/agents/{agent_id}/messages")
async def get_messages(
    agent_id: str,
    _auth: dict[str, str] = Security(_require_agent_session_access),
) -> dict[str, Any]:
    agent_id = _normalize_agent_id(agent_id)
    pg_conn = _agent_sessions_pg_conn()
    if get_interactive_agent_session(pg_conn, agent_id=agent_id) is None:
        raise HTTPException(status_code=404, detail=f"agent {agent_id!r} not found")

    return {"events": list_interactive_agent_events(pg_conn, agent_id=agent_id)}


@app.get("/agents/{agent_id}/stream")
async def stream_events(
    agent_id: str,
    request: Request,
    _auth: dict[str, str] = Security(_require_agent_session_access),
) -> StreamingResponse:
    agent_id = _normalize_agent_id(agent_id)
    if get_interactive_agent_session(_agent_sessions_pg_conn(), agent_id=agent_id) is None:
        raise HTTPException(status_code=404, detail=f"agent {agent_id!r} not found")

    queue = _get_queue(agent_id)

    async def _event_stream() -> Any:
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=_STREAM_IDLE_TIMEOUT)
            except asyncio.TimeoutError:
                if agent_id not in _active_turns:
                    break
                continue
            except asyncio.CancelledError:
                break
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.delete("/agents/{agent_id}")
async def delete_agent(
    agent_id: str,
    _auth: dict[str, str] = Security(_require_agent_session_access),
) -> dict[str, Any]:
    agent_id = _normalize_agent_id(agent_id)
    pg_conn = _agent_sessions_pg_conn()
    if get_interactive_agent_session(pg_conn, agent_id=agent_id) is None:
        raise HTTPException(status_code=404, detail=f"agent {agent_id!r} not found")

    terminated = await _terminate_process(agent_id)
    now = _now_iso()
    termination_event = {"event": "terminated", "at": now}
    _append_jsonl(_messages_path(agent_id), termination_event)
    terminate_interactive_agent_session(
        pg_conn,
        agent_id=agent_id,
        terminated_by=_auth["principal_ref"],
        reason="agent_sessions.delete",
    )
    await _get_queue(agent_id).put(termination_event)
    _release_turn(agent_id)

    return {"agent_id": agent_id, "terminated": terminated, "at": now}


@app.get("/agents")
async def list_agents(
    _auth: dict[str, str] = Security(_require_agent_session_access),
) -> list[dict[str, Any]]:
    agents = list_interactive_agent_sessions(_agent_sessions_pg_conn())
    for agent in agents:
        agent["running"] = str(agent.get("agent_id") or "") in _active_turns
    return agents


def start_server(*, host: str, port: int) -> None:
    import uvicorn

    uvicorn.run(app, host=host, port=port)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Agent Sessions service")
    parser.add_argument(
        "--host",
        default=_agent_sessions_host(),
        help=(
            "Bind address (default from PRAXIS_AGENT_SESSIONS_HOST, otherwise "
            "127.0.0.1)"
        ),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=_agent_sessions_port(),
        help=(
            "TCP port (default from PRAXIS_AGENT_SESSIONS_PORT, otherwise 8421)"
        ),
    )
    args = parser.parse_args(argv)
    start_server(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
