from __future__ import annotations

import importlib

import pytest


def test_circuit_breaker_registry_is_initialized_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    from runtime import circuit_breaker as circuit_breaker_module

    module = importlib.reload(circuit_breaker_module)
    calls: list[str] = []

    def _fake_require_cb_config() -> tuple[int, float]:
        calls.append("config")
        return 3, 45.0

    monkeypatch.setattr(module, "_require_cb_config", _fake_require_cb_config)

    assert calls == []

    registry = module.get_circuit_breakers()

    assert calls == ["config"]
    assert registry.get("openai").failure_threshold == 3
    assert registry.get("openai").recovery_timeout_s == 45.0


def test_unified_workflow_reload_does_not_require_database_url_on_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    from runtime import circuit_breaker as circuit_breaker_module
    from runtime.workflow import unified as unified_module

    importlib.reload(circuit_breaker_module)
    module = importlib.reload(unified_module)

    assert callable(module._circuit_breakers)


def test_unified_workflow_circuit_breaker_gate_degrades_without_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    from runtime.workflow import _shared as shared_module

    module = importlib.reload(shared_module)

    monkeypatch.setattr(
        module,
        "get_circuit_breakers",
        lambda: (_ for _ in ()).throw(
            RuntimeError(
                "config_registry requires explicit WORKFLOW_DATABASE_URL Postgres authority"
            )
        ),
    )

    assert module._circuit_breakers() is None
