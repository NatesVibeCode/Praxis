from __future__ import annotations

import os
import sys
from types import SimpleNamespace

from surfaces.api import server


def test_start_server_checks_dependency_contract_before_launch(monkeypatch) -> None:
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        server,
        "workflow_database_env",
        lambda: {
            "WORKFLOW_DATABASE_URL": "postgresql://postgres@localhost:5432/praxis",
            "PATH": "",
        },
    )

    def _fake_require_runtime_dependencies(*, scope: str = "api_server", manifest_path=None):
        observed["scope"] = scope
        observed["manifest_path"] = manifest_path
        return {
            "ok": True,
            "scope": scope,
            "manifest_path": "/tmp/requirements.runtime.txt",
            "required_count": 4,
            "available_count": 4,
            "missing_count": 0,
            "packages": [],
            "missing": [],
        }

    monkeypatch.setattr(server, "require_runtime_dependencies", _fake_require_runtime_dependencies)
    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=lambda *args, **kwargs: observed.update({
        "uvicorn_run_args": args,
        "uvicorn_run_kwargs": kwargs,
    })))
    monkeypatch.setitem(sys.modules, "surfaces.api.rest", SimpleNamespace(app="fake-asgi-app"))

    def _fake_runtime_http_endpoints(*, host: str, port: int) -> dict[str, str]:
        observed["runtime_http_endpoints_host"] = host
        observed["runtime_http_endpoints_port"] = port
        return {
            "api_base_url": "http://api.test:9999",
            "api_docs_url": "http://api.test:9999/docs",
        }

    monkeypatch.setattr(server, "_runtime_http_endpoints", _fake_runtime_http_endpoints)

    server.start_server(host="127.0.0.1", port=9999)

    assert observed["scope"] == "api_server"
    assert observed["runtime_http_endpoints_host"] == "127.0.0.1"
    assert observed["runtime_http_endpoints_port"] == 9999
    assert observed["uvicorn_run_args"] == ("fake-asgi-app",)
    assert observed["uvicorn_run_kwargs"] == {
        "host": "127.0.0.1",
        "port": 9999,
        "log_level": "info",
    }


def test_start_server_checks_workflow_database_authority_without_mutating_process_env(monkeypatch) -> None:
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        server,
        "workflow_database_env",
        lambda: {
            "WORKFLOW_DATABASE_URL": "postgresql://postgres@localhost:5432/praxis",
            "PATH": "",
        },
    )
    monkeypatch.setattr(
        server,
        "require_runtime_dependencies",
        lambda *, scope="api_server", manifest_path=None: {
            "ok": True,
            "scope": scope,
            "manifest_path": "/tmp/requirements.runtime.txt",
        },
    )
    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=lambda *args, **kwargs: observed.update({
        "args": args,
        "kwargs": kwargs,
    })))
    monkeypatch.setitem(sys.modules, "surfaces.api.rest", SimpleNamespace(app="fake-asgi-app"))
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://localhost:5432/praxis")

    server.start_server(host="127.0.0.1", port=9999)

    assert os.environ["WORKFLOW_DATABASE_URL"] == "postgresql://localhost:5432/praxis"


def test_runtime_http_endpoints_use_bind_host_and_normalize_wildcards() -> None:
    explicit = server._runtime_http_endpoints(host="127.0.0.1", port=9999)
    wildcard = server._runtime_http_endpoints(host="0.0.0.0", port=9999)

    assert explicit["api_base_url"] == "http://127.0.0.1:9999"
    assert explicit["api_docs_url"] == "http://127.0.0.1:9999/docs"
    assert wildcard["api_base_url"] == "http://localhost:9999"
    assert wildcard["api_docs_url"] == "http://localhost:9999/docs"
