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
) -> str:
    expected_token = _public_api_token()
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
    if not secrets.compare_digest(str(credentials.credentials), expected_token):
        raise HTTPException(
            status_code=403,
            detail={
                "message": "Bearer token rejected for agent sessions",
                "error_code": "agent_sessions_auth_rejected",
            },
        )
    request.state.authenticated_principal = "public_api_token"
    return "public_api_token"

_STREAM_IDLE_TIMEOUT = 5.0
_TURN_TERMINATION_TIMEOUT = 2.0

app = FastAPI(title="Agent Sessions", version="1.0.0")

_agent_locks: dict[str, asyncio.Lock] = {}
_agent_queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}
_agent_processes: dict[str, asyncio.subprocess.Process] = {}
_active_turns: set[str] = set()
_claimed_turns: set[str] = set()


class CreateAgentRequest(BaseModel):
    title: str | None = None
    prompt: str | None = None


class SendMessageRequest(BaseModel):
    prompt: str


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
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _write_meta_atomic(agent_id: str, meta: dict[str, Any]) -> None:
    path = _meta_path(agent_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False))
        fh.write("\n")


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


async def _run_turn(agent_id: str, session_id: str, prompt: str) -> tuple[str, list[dict[str, Any]], int]:
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
    _auth: str = Security(_require_agent_session_access),
) -> dict[str, Any]:
    agent_id = str(uuid4())
    session_id = str(uuid4())
    title = body.title or f"agent-{agent_id[:8]}"
    now = _now_iso()

    agent_dir = _agent_dir(agent_id)
    agent_dir.mkdir(parents=True, exist_ok=True)
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
    _messages_path(agent_id).touch(exist_ok=True)

    print(f"[agent_sessions] created agent={agent_id} session={session_id}", flush=True)

    if body.prompt:
        _claim_turn(agent_id)
        lock = _get_lock(agent_id)
        await lock.acquire()
        try:
            await _run_turn(agent_id, session_id, body.prompt)
        finally:
            lock.release()
            _release_turn(agent_id)

    return {"agent_id": agent_id, "session_id": session_id, "title": title}


@app.post("/agents/{agent_id}/messages")
async def send_message(
    agent_id: str,
    body: SendMessageRequest,
    _auth: str = Security(_require_agent_session_access),
) -> dict[str, Any]:
    agent_id = _normalize_agent_id(agent_id)
    meta = _read_meta(agent_id)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"agent {agent_id!r} not found")

    _claim_turn(agent_id)
    lock = _get_lock(agent_id)
    await lock.acquire()
    try:
        reply, turn_events, exit_code = await _run_turn(agent_id, str(meta["session_id"]), body.prompt)
    finally:
        lock.release()
        _release_turn(agent_id)

    return {"reply": reply, "turn_events": turn_events, "exit_code": exit_code}


@app.get("/agents/{agent_id}/messages")
async def get_messages(
    agent_id: str,
    _auth: str = Security(_require_agent_session_access),
) -> dict[str, Any]:
    agent_id = _normalize_agent_id(agent_id)
    meta = _read_meta(agent_id)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"agent {agent_id!r} not found")

    events: list[dict[str, Any]] = []
    path = _messages_path(agent_id)
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                event = json.loads(stripped)
                if isinstance(event, dict):
                    events.append(event)
                else:
                    events.append({"type": "raw", "data": event})
            except json.JSONDecodeError:
                events.append({"type": "raw", "data": stripped})

    return {"events": events}


@app.get("/agents/{agent_id}/stream")
async def stream_events(
    agent_id: str,
    request: Request,
    _auth: str = Security(_require_agent_session_access),
) -> StreamingResponse:
    agent_id = _normalize_agent_id(agent_id)
    meta = _read_meta(agent_id)
    if meta is None:
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
    _auth: str = Security(_require_agent_session_access),
) -> dict[str, Any]:
    agent_id = _normalize_agent_id(agent_id)
    meta = _read_meta(agent_id)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"agent {agent_id!r} not found")

    terminated = await _terminate_process(agent_id)
    now = _now_iso()
    termination_event = {"event": "terminated", "at": now}
    _append_jsonl(_messages_path(agent_id), termination_event)
    await _get_queue(agent_id).put(termination_event)
    _release_turn(agent_id)

    return {"agent_id": agent_id, "terminated": terminated, "at": now}


@app.get("/agents")
async def list_agents(
    _auth: str = Security(_require_agent_session_access),
) -> list[dict[str, Any]]:
    if not AGENTS_DIR.exists():
        return []

    agents: list[dict[str, Any]] = []
    for entry in sorted(AGENTS_DIR.iterdir(), key=lambda path: path.name):
        if not entry.is_dir():
            continue
        try:
            agent_id = _normalize_agent_id(entry.name)
            meta = _read_meta(agent_id)
        except HTTPException:
            continue
        if meta is None:
            continue
        agents.append(
            {
                "agent_id": meta.get("agent_id", agent_id),
                "title": meta.get("title", ""),
                "last_activity": meta.get("last_activity"),
                "running": agent_id in _active_turns,
            }
        )
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
