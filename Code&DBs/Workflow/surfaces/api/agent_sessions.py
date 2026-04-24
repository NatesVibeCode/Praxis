"""Agent Sessions surface — persistent Claude session management.

Standalone FastAPI app. Bind host/port default to PRAXIS_AGENT_SESSIONS_HOST
and PRAXIS_AGENT_SESSIONS_PORT when set, otherwise 127.0.0.1:8421.

Run:
    python Code&DBs/Workflow/surfaces/api/agent_sessions.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
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
    api_permission_prompt_suffix,
    is_permission_step_up,
    translate_permission_flags,
)

__all__ = ["app"]


PRAXIS_ROOT = Path(__file__).resolve().parents[4]
ARTIFACTS_DIR = PRAXIS_ROOT / "artifacts"
AGENTS_DIR = ARTIFACTS_DIR / "agents"
_PUBLIC_AUTH_TOKEN_ENV = "PRAXIS_API_TOKEN"
_AGENT_SESSIONS_HOST_ENV = "PRAXIS_AGENT_SESSIONS_HOST"
_AGENT_SESSIONS_PORT_ENV = "PRAXIS_AGENT_SESSIONS_PORT"
_RUNNER_URL_ENV = "PRAXIS_AGENT_SESSIONS_RUNNER_URL"
_HTTP_BEARER = HTTPBearer(auto_error=False)


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
_DEFAULT_TURN_TIMEOUT_SECONDS = 180.0
_DEFAULT_PERMISSION_MODE = "dontAsk"
_DEFAULT_CLI_PROVIDER = "codex"
_DEFAULT_CODEX_SANDBOX = "workspace-write"
_DEFAULT_OPENROUTER_MODEL = "qwen/qwen3-coder"

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
    factory = getattr(app.state, "pg_conn_factory", None)
    if callable(factory):
        return factory()

    global _subsystems
    if _subsystems is None:
        from surfaces.api.handlers._subsystems import _Subsystems

        _subsystems = _Subsystems()
    return _subsystems.get_pg_conn()


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
    raw = value if value is not None else source.get(_CLI_PROVIDER_ENV)
    provider = str(raw or _DEFAULT_CLI_PROVIDER).strip().lower()
    if provider not in {"codex", "claude", "gemini", "openrouter"}:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"unsupported agent provider {provider!r}; use codex, claude, gemini, or openrouter",
                "error_code": "agent_provider_unsupported",
            },
        )
    return provider


def _codex_sandbox(env: dict[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    value = str(source.get(_CODEX_SANDBOX_ENV) or "").strip()
    return value or _DEFAULT_CODEX_SANDBOX


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
        core.extend(["--sandbox", _codex_sandbox(env)])
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
    return str(source.get(_OPENROUTER_MODEL_ENV) or _DEFAULT_OPENROUTER_MODEL).strip()


def _openrouter_messages(
    pg_conn: Any | None,
    *,
    agent_id: str,
    prompt: str,
    permission_mode: NormalizedPermissionMode | None = None,
) -> list[dict[str, str]]:
    base_system = (
        "You are the Praxis operator's persistent agent-session conversation. "
        "Be concise, direct, and preserve continuity from the visible prior turns."
    )
    suffix = api_permission_prompt_suffix("openrouter", permission_mode)
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


def _openrouter_json_request(
    *,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    timeout_seconds: float,
) -> dict[str, Any]:
    body = json.dumps(
        {
            "model": model,
            "messages": messages,
            "max_tokens": 1200,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://praxis.local/operator-console",
            "X-Title": "Praxis Operator Console",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


async def _run_openrouter_turn(
    agent_id: str,
    session_id: str,
    prompt: str,
    *,
    pg_conn: Any | None,
    permission_mode: NormalizedPermissionMode | None = None,
) -> tuple[str, list[dict[str, Any]], int, str]:
    api_key = str(os.environ.get("OPENROUTER_API_KEY") or "").strip()
    if not api_key:
        event = {
            "type": "error",
            "error_code": "agent_provider_not_configured",
            "message": "OPENROUTER_API_KEY is not configured for the agent session OpenRouter provider",
        }
        return event["message"], [event], 78, session_id

    model = _openrouter_model()
    messages = _openrouter_messages(
        pg_conn, agent_id=agent_id, prompt=prompt, permission_mode=permission_mode,
    )
    try:
        payload = await asyncio.to_thread(
            _openrouter_json_request,
            api_key=api_key,
            model=model,
            messages=messages,
            timeout_seconds=_turn_timeout_seconds(),
        )
        choice = (payload.get("choices") or [{}])[0]
        message = choice.get("message") if isinstance(choice, dict) else {}
        reply = str((message or {}).get("content") or "").strip()
        event = {
            "type": "assistant",
            "provider": "openrouter",
            "model": model,
            "message": {"content": reply},
        }
        _append_jsonl(_messages_path(agent_id), event)
        await _get_queue(agent_id).put(event)
        return reply, [event], 0, session_id
    except Exception as exc:
        event = {
            "type": "error",
            "error_code": "agent_provider_failed",
            "message": f"{type(exc).__name__}: {exc}",
            "provider": "openrouter",
            "model": model,
        }
        _append_jsonl(_messages_path(agent_id), event)
        await _get_queue(agent_id).put(event)
        return event["message"], [event], 1, session_id


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
    provider_slug = _cli_provider(body.provider)
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

    _claim_turn(agent_id)
    lock = _get_lock(agent_id)
    await lock.acquire()
    try:
        provider_override = str(os.environ.get(_CLI_PROVIDER_ENV) or "").strip()
        provider_slug = _cli_provider(provider_override or str(session.get("provider") or ""))
        if provider_slug != str(session.get("provider") or ""):
            update_interactive_agent_cli_session(
                pg_conn,
                agent_id=agent_id,
                cli_session_id=str(session["session_id"]),
                provider_slug=provider_slug,
            )
        reply, turn_events, exit_code, effective_session_id = await _run_turn(
            agent_id,
            str(session["session_id"]),
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
