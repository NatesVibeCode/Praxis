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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request, Security
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

__all__ = ["app"]


PRAXIS_ROOT = Path(__file__).resolve().parents[4]
ARTIFACTS_DIR = PRAXIS_ROOT / "artifacts"
AGENTS_DIR = ARTIFACTS_DIR / "agents"
_PUBLIC_AUTH_TOKEN_ENV = "PRAXIS_API_TOKEN"
_AGENT_SESSIONS_HOST_ENV = "PRAXIS_AGENT_SESSIONS_HOST"
_AGENT_SESSIONS_PORT_ENV = "PRAXIS_AGENT_SESSIONS_PORT"
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

    from runtime.mobile_security import AUTH_COOKIE_NAME

    mobile_session_cookie = str(request.cookies.get(AUTH_COOKIE_NAME) or "").strip()
    if mobile_session_cookie:
        from runtime.capability.sessions import MobileSessionError, resolve_mobile_session

        try:
            session = resolve_mobile_session(
                _agent_sessions_pg_conn(),
                session_token_secret=mobile_session_cookie,
            )
        except MobileSessionError as exc:
            raise HTTPException(
                status_code=401,
                detail={"message": str(exc), "error_code": exc.reason_code},
            ) from exc

        principal_ref = str(session.get("principal_ref") or "mobile_session").strip()
        request.state.authenticated_principal = principal_ref
        return _auth_payload(
            principal_ref=principal_ref,
            auth_kind="mobile_session",
            mobile_session_id=str(session.get("session_id") or ""),
        )

    if expected_token is None:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "PRAXIS_API_TOKEN or a valid mobile session cookie is required before agent sessions can run",
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


class SendMessageRequest(BaseModel):
    prompt: str


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
    mobile_session_id: str | None = None,
) -> dict[str, str]:
    payload = {"principal_ref": principal_ref, "auth_kind": auth_kind}
    if mobile_session_id:
        payload["mobile_session_id"] = mobile_session_id
    return payload


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
            $1, $2, $3, $4, 'claude', 'active',
            $5, $6, $7, $8, $9, '{}'::jsonb, now(), now()
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
        RETURNING session_id, external_session_id, display_title, principal_ref,
                  workspace_ref, status, created_at, last_activity_at, heartbeat_at
        """,
        agent_id,
        f"interactive:{agent_id}",
        INTERACTIVE_WORKFLOW_ID,
        INTERACTIVE_JOB_LABEL,
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
        SELECT session_id, external_session_id, display_title, principal_ref,
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
        SELECT session_id, external_session_id, display_title, principal_ref,
               workspace_ref, status, created_at, last_activity_at, heartbeat_at
        FROM agent_sessions
        WHERE session_kind = $1
          AND revoked_at IS NULL
        ORDER BY last_activity_at DESC, created_at DESC
        """,
        INTERACTIVE_SESSION_KIND,
    )
    return [_session_from_row(row) for row in rows]


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


def _spend_mobile_budget_if_present(auth: dict[str, str], *, reason_code: str) -> None:
    session_id = auth.get("mobile_session_id")
    if not session_id:
        return
    from runtime.capability.sessions import MobileSessionError, spend_session_budget

    try:
        spend_session_budget(
            _agent_sessions_pg_conn(),
            session_id=session_id,
            units=1,
            reason_code=reason_code,
        )
    except MobileSessionError as exc:
        raise HTTPException(
            status_code=429,
            detail={"message": str(exc), "error_code": exc.reason_code},
        ) from exc


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


async def _run_turn(
    agent_id: str,
    session_id: str,
    prompt: str,
    *,
    pg_conn: Any | None = None,
) -> tuple[str, list[dict[str, Any]], int]:
    queue = _get_queue(agent_id)
    messages_path = _messages_path(agent_id)
    cmd = [
        "claude",
        "-p",
        "--resume",
        session_id,
        "--output-format",
        "stream-json",
        prompt,
    ]

    print(f"[agent_sessions] launching claude agent={agent_id}", flush=True)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(_claude_cwd()),
        env=os.environ.copy(),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    _agent_processes[agent_id] = proc
    _active_turns.add(agent_id)

    turn_events: list[dict[str, Any]] = []

    try:
        assert proc.stdout is not None
        async for raw_line in proc.stdout:
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

        exit_code = await proc.wait()
        reply = _final_reply_from_events(turn_events)
        print(
            f"[agent_sessions] claude done agent={agent_id} code={exit_code} events={len(turn_events)}",
            flush=True,
        )
        return reply, turn_events, int(exit_code)
    finally:
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
    body: CreateAgentRequest,
    _auth: dict[str, str] = Security(_require_agent_session_access),
) -> dict[str, Any]:
    pg_conn = _agent_sessions_pg_conn()
    _spend_mobile_budget_if_present(_auth, reason_code="agent_sessions.create")

    agent_id = str(uuid4())
    session_id = str(uuid4())
    title = body.title or f"agent-{agent_id[:8]}"
    now = _now_iso()
    session = create_interactive_agent_session(
        pg_conn,
        agent_id=agent_id,
        cli_session_id=session_id,
        title=title,
        principal_ref=_auth["principal_ref"],
        workspace_ref="praxis.default",
    )

    _write_meta_atomic(
        agent_id,
        {
            "agent_id": agent_id,
            "session_id": session_id,
            "title": title,
            "created_at": now,
            "last_activity": now,
        },
    )
    try:
        _messages_path(agent_id).touch(exist_ok=True)
    except OSError as exc:
        print(f"[agent_sessions] compatibility message log skipped: {exc}", flush=True)

    print(f"[agent_sessions] created agent={agent_id} session={session_id}", flush=True)

    if body.prompt:
        append_interactive_agent_event(
            pg_conn,
            agent_id=agent_id,
            event_kind="user.prompt",
            payload={"principal_ref": _auth["principal_ref"]},
            text_content=body.prompt,
        )
        _claim_turn(agent_id)
        lock = _get_lock(agent_id)
        await lock.acquire()
        try:
            reply, _turn_events, exit_code = await _run_turn(
                agent_id,
                session_id,
                body.prompt,
                pg_conn=pg_conn,
            )
        finally:
            lock.release()
            _release_turn(agent_id)
        append_interactive_agent_event(
            pg_conn,
            agent_id=agent_id,
            event_kind="assistant.reply",
            payload={"exit_code": exit_code},
            text_content=reply,
        )

    return {"agent_id": agent_id, "session_id": session_id, "title": title, "authority": session}


@app.post("/agents/{agent_id}/messages")
async def send_message(
    agent_id: str,
    body: SendMessageRequest,
    _auth: dict[str, str] = Security(_require_agent_session_access),
) -> dict[str, Any]:
    agent_id = _normalize_agent_id(agent_id)
    pg_conn = _agent_sessions_pg_conn()
    session = get_interactive_agent_session(pg_conn, agent_id=agent_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"agent {agent_id!r} not found")
    _spend_mobile_budget_if_present(_auth, reason_code="agent_sessions.message")
    append_interactive_agent_event(
        pg_conn,
        agent_id=agent_id,
        event_kind="user.prompt",
        payload={"principal_ref": _auth["principal_ref"]},
        text_content=body.prompt,
    )

    _claim_turn(agent_id)
    lock = _get_lock(agent_id)
    await lock.acquire()
    try:
        reply, turn_events, exit_code = await _run_turn(
            agent_id,
            str(session["session_id"]),
            body.prompt,
            pg_conn=pg_conn,
        )
    finally:
        lock.release()
        _release_turn(agent_id)
    append_interactive_agent_event(
        pg_conn,
        agent_id=agent_id,
        event_kind="assistant.reply",
        payload={"exit_code": exit_code},
        text_content=reply,
    )

    return {"reply": reply, "turn_events": turn_events, "exit_code": exit_code}


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

    _spend_mobile_budget_if_present(_auth, reason_code="agent_sessions.terminate")
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
