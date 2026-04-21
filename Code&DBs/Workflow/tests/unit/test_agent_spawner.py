from __future__ import annotations

from runtime.agent_spawner import ProviderReadinessChecker


def test_cursor_readiness_requires_api_key_not_cli_binary(monkeypatch) -> None:
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.setattr("runtime.agent_spawner.shutil.which", lambda _binary: "/usr/local/bin/cursor-agent")
    monkeypatch.setattr("runtime.agent_spawner.resolve_secret", lambda _name, env=None: None)
    monkeypatch.setattr("runtime.agent_spawner.resolve_api_key_env_vars", lambda provider: ("CURSOR_API_KEY",) if provider == "cursor" else ())

    readiness = ProviderReadinessChecker().check("cursor")

    assert readiness.ready is False
    assert readiness.reason == "Missing credential: CURSOR_API_KEY"


def test_cursor_readiness_succeeds_with_api_key(monkeypatch) -> None:
    monkeypatch.setenv("CURSOR_API_KEY", "cursor-test-key")
    monkeypatch.setattr("runtime.agent_spawner.shutil.which", lambda _binary: None)
    monkeypatch.setattr("runtime.agent_spawner.resolve_api_key_env_vars", lambda provider: ("CURSOR_API_KEY",) if provider == "cursor" else ())

    readiness = ProviderReadinessChecker().check("cursor")

    assert readiness.ready is True
    assert readiness.reason is None


def test_cursor_readiness_succeeds_with_keychain_secret(monkeypatch) -> None:
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.setattr("runtime.agent_spawner.shutil.which", lambda _binary: None)
    monkeypatch.setattr("runtime.agent_spawner.resolve_api_key_env_vars", lambda provider: ("CURSOR_API_KEY",) if provider == "cursor" else ())
    monkeypatch.setattr(
        "runtime.agent_spawner.resolve_secret",
        lambda name, env=None: "cursor-keychain-secret" if name == "CURSOR_API_KEY" else None,
    )

    readiness = ProviderReadinessChecker().check("cursor")

    assert readiness.ready is True
    assert readiness.reason is None
