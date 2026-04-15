from __future__ import annotations

import sys
from types import SimpleNamespace

from runtime import embedding_backend_server


def test_start_server_checks_dependency_contract_before_launch(monkeypatch) -> None:
    observed: dict[str, object] = {}

    def _fake_require_runtime_dependencies(*, scope: str = "semantic_backend", manifest_path=None):
        observed["scope"] = scope
        observed["manifest_path"] = manifest_path
        return {
            "ok": True,
            "scope": scope,
            "manifest_path": "/tmp/requirements.runtime.txt",
            "required_count": 5,
            "available_count": 5,
            "missing_count": 0,
            "packages": [],
            "missing": [],
        }

    monkeypatch.setattr(
        embedding_backend_server,
        "require_runtime_dependencies",
        _fake_require_runtime_dependencies,
    )
    monkeypatch.setattr(
        embedding_backend_server.EmbeddingService,
        "start_background_prewarm",
        classmethod(lambda cls, model_name=None: observed.update({"prewarm_model": model_name}) or None),
    )
    monkeypatch.setitem(
        sys.modules,
        "uvicorn",
        SimpleNamespace(
            run=lambda *args, **kwargs: observed.update(
                {
                    "uvicorn_run_args": args,
                    "uvicorn_run_kwargs": kwargs,
                }
            )
        ),
    )

    embedding_backend_server.start_server(host="127.0.0.1", port=8421)

    assert observed["scope"] == "semantic_backend"
    assert observed["prewarm_model"] == embedding_backend_server.resolve_embedding_runtime_authority().model_name
    assert observed["uvicorn_run_args"] == (embedding_backend_server.app,)
    assert observed["uvicorn_run_kwargs"] == {
        "host": "127.0.0.1",
        "port": 8421,
        "log_level": "info",
    }
