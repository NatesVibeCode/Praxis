from __future__ import annotations

import sys
from http.server import ThreadingHTTPServer
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.api import workflow_api


class _FakeThreadingHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass):
        self.server_address = server_address
        self.RequestHandlerClass = RequestHandlerClass
        self.daemon_threads = True
        self.closed = False

    def server_close(self) -> None:
        self.closed = True


def test_workflow_api_server_uses_threaded_http_server() -> None:
    server = workflow_api.WorkflowAPIServer(
        host="127.0.0.1",
        port=0,
        subsystems=object(),
        server_class=_FakeThreadingHTTPServer,
    )
    httpd = server._ensure_server()
    try:
        assert isinstance(httpd, ThreadingHTTPServer)
        assert httpd.daemon_threads is True
    finally:
        httpd.server_close()
    assert httpd.closed is True
