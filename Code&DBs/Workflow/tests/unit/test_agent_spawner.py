from __future__ import annotations

from runtime.agent_spawner import ProviderReadinessChecker


def test_cursor_readiness_requires_api_key_not_cli_binary(monkeypatch) -> None:
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.setattr("runtime.agent_spawner.shutil.which", lambda _binary: "/usr/local/bin/cursor-agent")

    readiness = ProviderReadinessChecker().check("cursor")

    assert readiness.ready is False
    assert readiness.reason == "Missing env var: CURSOR_API_KEY"


def test_cursor_readiness_succeeds_with_api_key(monkeypatch) -> None:
    monkeypatch.setenv("CURSOR_API_KEY", "cursor-test-key")
    monkeypatch.setattr("runtime.agent_spawner.shutil.which", lambda _binary: None)

    readiness = ProviderReadinessChecker().check("cursor")

    assert readiness.ready is True
    assert readiness.reason is None
