from __future__ import annotations

import sys
from types import SimpleNamespace

from surfaces.api import agent_sessions


def test_agent_sessions_host_and_port_default_from_env(monkeypatch) -> None:
    monkeypatch.delenv("PRAXIS_AGENT_SESSIONS_HOST", raising=False)
    monkeypatch.delenv("PRAXIS_AGENT_SESSIONS_PORT", raising=False)

    assert agent_sessions._agent_sessions_host() == "127.0.0.1"
    assert agent_sessions._agent_sessions_port() == 8421


def test_agent_sessions_main_uses_configured_bind_authority(monkeypatch) -> None:
    observed: dict[str, object] = {}

    monkeypatch.setenv("PRAXIS_AGENT_SESSIONS_HOST", "0.0.0.0")
    monkeypatch.setenv("PRAXIS_AGENT_SESSIONS_PORT", "9001")
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

    assert agent_sessions.main([]) == 0

    assert observed["uvicorn_run_args"] == (agent_sessions.app,)
    assert observed["uvicorn_run_kwargs"] == {
        "host": "0.0.0.0",
        "port": 9001,
    }
