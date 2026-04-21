from __future__ import annotations

import adapters.keychain as keychain_mod


def test_resolve_secret_prefers_explicit_env_over_keychain_and_dotenv(monkeypatch) -> None:
    monkeypatch.setattr(keychain_mod, "_dotenv_cache", {"TEST_API_KEY": "dotenv-secret"})
    monkeypatch.setattr(keychain_mod, "_keychain_available", lambda: True)
    monkeypatch.setattr(keychain_mod, "keychain_get", lambda _name: "keychain-secret")

    assert keychain_mod.resolve_secret("TEST_API_KEY", env={"TEST_API_KEY": "env-secret"}) == "env-secret"


def test_resolve_secret_prefers_keychain_over_process_env(monkeypatch) -> None:
    monkeypatch.setattr(keychain_mod, "_dotenv_cache", {})
    monkeypatch.setattr(keychain_mod, "_keychain_available", lambda: True)
    monkeypatch.setattr(keychain_mod, "keychain_get", lambda _name: "keychain-secret")
    monkeypatch.setenv("TEST_API_KEY", "process-secret")

    assert keychain_mod.resolve_secret("TEST_API_KEY") == "keychain-secret"


def test_resolve_secret_falls_back_to_env_when_dotenv_and_keychain_missing(monkeypatch) -> None:
    monkeypatch.setattr(keychain_mod, "_dotenv_cache", {})
    monkeypatch.setattr(keychain_mod, "_keychain_available", lambda: False)

    assert keychain_mod.resolve_secret("TEST_API_KEY", env={"TEST_API_KEY": "env-secret"}) == "env-secret"
