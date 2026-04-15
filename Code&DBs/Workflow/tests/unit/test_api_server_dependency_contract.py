from __future__ import annotations

import os
import sys
from types import SimpleNamespace

from surfaces.api import server


def test_start_server_checks_dependency_contract_before_launch(monkeypatch) -> None:
    observed: dict[str, object] = {}

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

    server.start_server(host="127.0.0.1", port=9999)

    assert observed["scope"] == "api_server"
    assert observed["uvicorn_run_args"] == ("fake-asgi-app",)
    assert observed["uvicorn_run_kwargs"] == {
        "host": "127.0.0.1",
        "port": 9999,
        "log_level": "info",
    }


def test_start_server_primes_normalized_workflow_database_url(monkeypatch) -> None:
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

    assert os.environ["WORKFLOW_DATABASE_URL"] == "postgresql://postgres@localhost:5432/praxis"
