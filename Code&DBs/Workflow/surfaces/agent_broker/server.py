"""HTTP broker for CLI and workflow-worker MCP traffic.

Three endpoints:

    GET  /health  — liveness probe (no auth).
    POST /rpc     — JSON-RPC envelope from host ``bin/praxis-agent``;
                    auth = bearer broker token written to
                    ``artifacts/agent-broker/token`` at boot.
    POST /mcp     — JSON-RPC envelope from workflow-worker MCP bridge;
                    auth = signed workflow MCP session token, OR the
                    broker token (host fallback for unscoped local calls).

The broker reuses ``surfaces.mcp.protocol.handle_request`` directly so the
authority path is ``protocol.handle_request`` → ``invoke_tool`` →
``operation_catalog_gateway``. There is no second tool registry.

Run with::

    python -m surfaces.agent_broker.server --host 0.0.0.0 --port 8422

State (token + machine-readable env) is persisted under
``$PRAXIS_AGENT_BROKER_STATE_DIR`` (default
``<repo>/artifacts/agent-broker``) so the host ``bin/praxis-agent``
client can read the broker token without ``docker exec``.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import socket
import socketserver
import sys
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from runtime.workflow.mcp_session import (
    WorkflowMcpSessionError,
    verify_workflow_mcp_session_token,
)
from surfaces.mcp.invocation import normalize_allowed_tool_names
from surfaces.mcp.protocol import _boot_shared_subsystems, handle_request
from surfaces.mcp.runtime_context import workflow_mcp_request_context


# ---------------------------------------------------------------------------
# Defaults / state-dir resolution
# ---------------------------------------------------------------------------

# surfaces/agent_broker/server.py → surfaces/agent_broker → surfaces →
# Code&DBs/Workflow → Code&DBs → repo root
_REPO_ROOT = Path(__file__).resolve().parents[4]


def _default_state_dir() -> Path:
    raw = os.environ.get("PRAXIS_AGENT_BROKER_STATE_DIR")
    if raw:
        return Path(raw)
    return _REPO_ROOT / "artifacts" / "agent-broker"


def _default_host() -> str:
    return os.environ.get("PRAXIS_AGENT_BROKER_HOST", "127.0.0.1")


def _default_port() -> int:
    raw = os.environ.get("PRAXIS_AGENT_BROKER_PORT", "8422")
    try:
        return int(raw)
    except ValueError:
        return 8422


def _default_socket_path() -> Path | None:
    raw = os.environ.get("PRAXIS_AGENT_BROKER_SOCKET", "").strip()
    return Path(raw) if raw else None


def _default_max_body_bytes() -> int:
    raw = os.environ.get("PRAXIS_AGENT_BROKER_MAX_BODY_BYTES", "1048576")
    try:
        value = int(raw)
    except ValueError:
        value = 1_048_576
    return max(1, value)


def _default_socket_timeout_seconds() -> float:
    raw = os.environ.get("PRAXIS_AGENT_BROKER_SOCKET_TIMEOUT_SECONDS", "45")
    try:
        value = float(raw)
    except ValueError:
        value = 45.0
    return max(1.0, value)


def _default_max_concurrent_requests() -> int:
    raw = os.environ.get("PRAXIS_AGENT_BROKER_MAX_CONCURRENT_REQUESTS", "64")
    try:
        value = int(raw)
    except ValueError:
        value = 64
    return max(1, value)


def _default_request_queue_size() -> int:
    raw = os.environ.get("PRAXIS_AGENT_BROKER_REQUEST_QUEUE_SIZE", "128")
    try:
        value = int(raw)
    except ValueError:
        value = 128
    return max(1, value)


# ---------------------------------------------------------------------------
# Token + state file management
# ---------------------------------------------------------------------------

def _ensure_broker_token(state_dir: Path) -> str:
    state_dir.mkdir(parents=True, exist_ok=True)
    token_file = state_dir / "token"
    if token_file.exists():
        existing = token_file.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    token = secrets.token_urlsafe(32)
    token_file.write_text(token + "\n", encoding="utf-8")
    try:
        token_file.chmod(0o600)
    except OSError:
        pass
    return token


def _write_state_env(
    state_dir: Path,
    *,
    host: str,
    port: int,
    socket_path: Path | None = None,
) -> None:
    state_env = state_dir / "state.env"
    advertised_host = "127.0.0.1" if host in {"0.0.0.0", "::", "[::]"} else host
    lines = [
        f"PRAXIS_AGENT_BROKER_URL=http://{advertised_host}:{port}/rpc",
        f"PRAXIS_AGENT_BROKER_HOST={advertised_host}",
        f"PRAXIS_AGENT_BROKER_PORT={port}",
        f"PRAXIS_AGENT_BROKER_TOKEN_FILE={state_dir / 'token'}",
    ]
    if socket_path is not None:
        lines.append(f"PRAXIS_AGENT_BROKER_SOCKET={socket_path}")
    body = "\n".join(lines) + "\n"
    state_env.write_text(body, encoding="utf-8")


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

# Module-level token, populated at server start so request handlers can
# constant-time compare without re-reading disk per call.
_BROKER_TOKEN: str = ""


def _set_broker_token(token: str) -> None:
    global _BROKER_TOKEN
    _BROKER_TOKEN = str(token or "")


def _is_broker_token(presented: str) -> bool:
    expected = _BROKER_TOKEN
    if not expected or not presented:
        return False
    return secrets.compare_digest(expected.encode("utf-8"), presented.encode("utf-8"))


def _bearer_from_headers(headers: Any) -> str:
    raw = str(headers.get("Authorization") or "").strip()
    if raw.lower().startswith("bearer "):
        return raw[7:].strip()
    return ""


def _allowed_tools_from_headers(headers: Any) -> list[str] | None:
    raw = str(headers.get("X-Praxis-Allowed-MCP-Tools") or "").strip()
    if not raw:
        return None
    parsed = normalize_allowed_tool_names(raw)
    if parsed is None:
        return None
    return sorted(parsed)


def _intersect_allowed_tools(
    token_allowed: list[str] | None,
    request_allowed: list[str] | None,
) -> list[str] | None:
    if not token_allowed and not request_allowed:
        return None
    if not request_allowed:
        return list(token_allowed or [])
    if not token_allowed:
        return list(request_allowed)
    request_set = set(request_allowed)
    return [tool for tool in token_allowed if tool in request_set]


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class _RequestBodyTooLarge(ValueError):
    pass


class _BrokerHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = _default_request_queue_size()

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
    ) -> None:
        super().__init__(server_address, handler_class)
        self._request_slots = threading.BoundedSemaphore(_default_max_concurrent_requests())

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            _send_busy_response(request)
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


class _BrokerUnixHTTPServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    request_queue_size = _default_request_queue_size()

    def __init__(
        self,
        server_address: str,
        handler_class: type[BaseHTTPRequestHandler],
    ) -> None:
        super().__init__(server_address, handler_class)
        self._request_slots = threading.BoundedSemaphore(_default_max_concurrent_requests())

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            _send_busy_response(request)
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


def _send_busy_response(request: Any) -> None:
    try:
        request.sendall(
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 43\r\n"
            b"Connection: close\r\n"
            b"\r\n"
            b'{"error":"broker request capacity reached"}'
        )
    except OSError:
        return


class _BrokerHandler(BaseHTTPRequestHandler):
    server_version = "praxis-agentd/1.0"

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(_default_socket_timeout_seconds())

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("[praxis-agentd] " + (fmt % args) + "\n")

    def _send_json(self, status: int, body: Any) -> None:
        try:
            payload = json.dumps(body, default=str).encode("utf-8")
        except (TypeError, ValueError):
            payload = json.dumps({"error": "unserializable response"}).encode("utf-8")
            status = 500
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json_body(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            length = 0
        if length < 0:
            raise ValueError("Content-Length must be non-negative")
        max_body = _default_max_body_bytes()
        if length > max_body:
            raise _RequestBodyTooLarge(
                f"request body too large: {length} bytes exceeds limit {max_body}"
            )
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        if not raw:
            return {}
        decoded = json.loads(raw)
        if not isinstance(decoded, dict):
            raise ValueError("body must be a JSON object")
        return decoded

    # -- routes -------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802 — BaseHTTPRequestHandler API
        path = self.path.split("?", 1)[0]
        if path == "/health":
            self._send_json(200, {"ok": True, "service": "praxis-agentd"})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802 — BaseHTTPRequestHandler API
        path = self.path.split("?", 1)[0]
        if path == "/rpc":
            self._handle_rpc()
            return
        if path == "/mcp":
            self._handle_mcp()
            return
        self._send_json(404, {"error": "not found"})

    def _handle_rpc(self) -> None:
        token = _bearer_from_headers(self.headers)
        if not _is_broker_token(token):
            self._send_json(401, {"error": "broker token required"})
            return
        try:
            body = self._read_json_body()
        except _RequestBodyTooLarge as exc:
            self._send_json(413, {"error": str(exc)})
            return
        except (TimeoutError, socket.timeout):
            self._send_json(408, {"error": "request body read timed out"})
            return
        except (ValueError, json.JSONDecodeError) as exc:
            self._send_json(400, {"error": f"invalid JSON: {exc}"})
            return
        allowed_tool_names = _allowed_tools_from_headers(self.headers)
        try:
            response = handle_request(
                body,
                transport="jsonl",
                allowed_tool_names=allowed_tool_names,
                workflow_token="",
            )
        except Exception as exc:
            self._send_json(
                500,
                {
                    "error": f"{type(exc).__name__}: {exc}",
                    "error_code": "internal_error",
                    "trace": traceback.format_exc(),
                },
            )
            return
        if response is None:
            self._send_json(202, {})
            return
        self._send_json(200, response)

    def _handle_mcp(self) -> None:
        presented = _bearer_from_headers(self.headers)
        try:
            body = self._read_json_body()
        except _RequestBodyTooLarge as exc:
            self._send_json(413, {"error": str(exc)})
            return
        except (TimeoutError, socket.timeout):
            self._send_json(408, {"error": "request body read timed out"})
            return
        except (ValueError, json.JSONDecodeError) as exc:
            self._send_json(400, {"error": f"invalid JSON: {exc}"})
            return
        method = str(body.get("method") or "").strip()
        request_allowed = _allowed_tools_from_headers(self.headers)

        if method in {"tools/list", "tools/call"}:
            try:
                claims = verify_workflow_mcp_session_token(presented)
            except WorkflowMcpSessionError as exc:
                if _is_broker_token(presented):
                    self._dispatch_unscoped(body, request_allowed, presented="")
                    return
                self._send_json(
                    401,
                    {"error": str(exc), "reason_code": exc.reason_code},
                )
                return
            allowed_tool_names = _intersect_allowed_tools(
                list(claims.get("allowed_tools") or []),
                request_allowed,
            )
            with workflow_mcp_request_context(
                run_id=str(claims.get("run_id") or "").strip() or None,
                workflow_id=str(claims.get("workflow_id") or "").strip() or None,
                job_label=str(claims.get("job_label") or "").strip(),
                allowed_tools=allowed_tool_names,
                expires_at=int(claims.get("exp") or 0),
                source_refs=claims.get("source_refs") or [],
                access_policy=claims.get("access_policy") or {},
            ):
                self._dispatch_to_protocol(
                    body,
                    allowed_tool_names=allowed_tool_names,
                    workflow_token=presented,
                )
            return
        # initialize / notifications — no scope check.
        self._dispatch_to_protocol(
            body,
            allowed_tool_names=request_allowed,
            workflow_token=presented,
        )

    def _dispatch_unscoped(
        self,
        body: dict[str, Any],
        request_allowed: list[str] | None,
        *,
        presented: str,
    ) -> None:
        self._dispatch_to_protocol(
            body,
            allowed_tool_names=request_allowed,
            workflow_token=presented,
        )

    def _dispatch_to_protocol(
        self,
        body: dict[str, Any],
        *,
        allowed_tool_names: list[str] | None,
        workflow_token: str,
    ) -> None:
        try:
            response = handle_request(
                body,
                transport="jsonl",
                allowed_tool_names=allowed_tool_names,
                workflow_token=workflow_token,
            )
        except Exception as exc:
            self._send_json(
                500,
                {
                    "error": f"{type(exc).__name__}: {exc}",
                    "error_code": "internal_error",
                    "trace": traceback.format_exc(),
                },
            )
            return
        if response is None:
            self._send_json(202, {})
            return
        self._send_json(200, response)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def _start_unix_server(socket_path: Path) -> _BrokerUnixHTTPServer:
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    if socket_path.exists():
        socket_path.unlink()
    httpd = _BrokerUnixHTTPServer(str(socket_path), _BrokerHandler)
    try:
        socket_path.chmod(0o600)
    except OSError:
        pass
    thread = threading.Thread(
        target=httpd.serve_forever,
        name=f"praxis-agentd-uds:{socket_path}",
        daemon=True,
    )
    thread.start()
    return httpd


def serve(
    host: str | None = None,
    port: int | None = None,
    socket_path: Path | None = None,
) -> None:
    """Boot subsystems, persist token+state, listen on host:port."""
    bind_host = host or _default_host()
    bind_port = port if port is not None else _default_port()
    bind_socket = socket_path if socket_path is not None else _default_socket_path()

    state_dir = _default_state_dir()
    token = _ensure_broker_token(state_dir)
    _set_broker_token(token)
    _write_state_env(state_dir, host=bind_host, port=bind_port, socket_path=bind_socket)

    _boot_shared_subsystems()

    unix_httpd: _BrokerUnixHTTPServer | None = None
    if bind_socket is not None:
        unix_httpd = _start_unix_server(bind_socket)
        sys.stderr.write(f"[praxis-agentd] unix socket listening at {bind_socket}\n")

    httpd = _BrokerHTTPServer((bind_host, bind_port), _BrokerHandler)
    sys.stderr.write(
        f"[praxis-agentd] listening on http://{bind_host}:{bind_port} "
        f"(state={state_dir})\n"
    )
    sys.stderr.flush()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        sys.stderr.write("[praxis-agentd] shutting down\n")
    finally:
        httpd.server_close()
        if unix_httpd is not None:
            unix_httpd.shutdown()
            unix_httpd.server_close()
            if bind_socket is not None:
                try:
                    bind_socket.unlink()
                except FileNotFoundError:
                    pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="praxis-agentd — local agent broker for CLI/MCP traffic"
    )
    parser.add_argument("--host", default=_default_host())
    parser.add_argument("--port", type=int, default=_default_port())
    parser.add_argument("--socket", type=Path, default=_default_socket_path())
    args = parser.parse_args(argv)
    serve(host=args.host, port=args.port, socket_path=args.socket)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
