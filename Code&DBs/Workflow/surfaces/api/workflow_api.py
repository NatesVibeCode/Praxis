"""Lightweight HTTP API surface for the DAG workflow platform.

Exposes the same capabilities as the MCP server over HTTP so any
orchestrator (not just MCP clients) can use it.

All endpoints are routed by HTTP method across POST, GET, PUT, and
DELETE, and accept/return JSON. No third-party dependencies: stdlib
only.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

if __package__ in (None, ""):
    from handlers import (
        handle_delete_request,
        handle_get_request,
        handle_post_request,
        handle_put_request,
        path_is_known,
    )
    from handlers._subsystems import _Subsystems
else:
    from surfaces.api.handlers import (
        handle_delete_request,
        handle_get_request,
        handle_post_request,
        handle_put_request,
        path_is_known,
    )
    from surfaces.api.handlers._subsystems import _Subsystems


class WorkflowAPIHandler(BaseHTTPRequestHandler):
    """Routes HTTP requests to grouped endpoint handlers."""

    subsystems: _Subsystems | None = None

    def _dispatch_or_error(self, path: str, handler: Any) -> None:
        if handler(self, path):
            return
        if path_is_known(path):
            self._send_json(405, {"error": f"Method not allowed: {path}"})
            return
        self._send_json(404, {"error": f"Not found: {path}"})

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?")[0].rstrip("/") or "/"
        self._dispatch_or_error(path, handle_post_request)

    def do_PUT(self) -> None:  # noqa: N802
        path = self.path.split("?")[0].rstrip("/") or "/"
        self._dispatch_or_error(path, handle_put_request)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?")[0].rstrip("/") or "/"
        self._dispatch_or_error(path, handle_get_request)

    def do_DELETE(self) -> None:  # noqa: N802
        path = self.path.split("?")[0].rstrip("/") or "/"
        self._dispatch_or_error(path, handle_delete_request)

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(
        self,
        status: int,
        payload: bytes,
        *,
        content_type: str = "application/octet-stream",
        content_disposition: str | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        if content_disposition:
            self.send_header("Content-Disposition", content_disposition)
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default stderr logging in production / tests."""
        pass


class WorkflowAPIServer:
    """Wraps ThreadingHTTPServer with lazy subsystem init."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8421,
        subsystems: _Subsystems | None = None,
        server_class: type = ThreadingHTTPServer,
    ) -> None:
        self.host = host
        self.port = port
        self._subs = subsystems or _Subsystems()
        self._server_class = server_class
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def _make_handler_class(self):
        subs = self._subs

        class BoundHandler(WorkflowAPIHandler):
            subsystems = subs

        return BoundHandler

    def _ensure_server(self) -> ThreadingHTTPServer:
        if self._httpd is None:
            handler = self._make_handler_class()
            self._httpd = self._server_class((self.host, self.port), handler)
        return self._httpd

    @property
    def server_address(self) -> tuple[str, int]:
        httpd = self._ensure_server()
        return httpd.server_address

    def serve_forever(self) -> None:
        httpd = self._ensure_server()
        try:
            httpd.serve_forever()
        finally:
            httpd.server_close()

    def serve_background(self) -> None:
        httpd = self._ensure_server()
        self._thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None


__all__ = [
    "WorkflowAPIHandler",
    "WorkflowAPIServer",
    "_Subsystems",
]
