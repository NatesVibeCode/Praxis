from __future__ import annotations

import adapters.keychain as keychain_mod


def test_resolve_secret_prefers_dotenv_over_keychain_and_env(monkeypatch) -> None:
    monkeypatch.setattr(keychain_mod, "_dotenv_cache", {"TEST_API_KEY": "dotenv-secret"})
    monkeypatch.setattr(keychain_mod, "_keychain_available", lambda: True)
    monkeypatch.setattr(keychain_mod, "keychain_get", lambda _name: "keychain-secret")

    assert keychain_mod.resolve_secret("TEST_API_KEY", env={"TEST_API_KEY": "env-secret"}) == "dotenv-secret"


def test_resolve_secret_prefers_keychain_over_env(monkeypatch) -> None:
    monkeypatch.setattr(keychain_mod, "_dotenv_cache", {})
    monkeypatch.setattr(keychain_mod, "_keychain_available", lambda: True)
    monkeypatch.setattr(keychain_mod, "keychain_get", lambda _name: "keychain-secret")

    assert keychain_mod.resolve_secret("TEST_API_KEY", env={"TEST_API_KEY": "env-secret"}) == "keychain-secret"


def test_resolve_secret_falls_back_to_env_when_dotenv_and_keychain_missing(monkeypatch) -> None:
    monkeypatch.setattr(keychain_mod, "_dotenv_cache", {})
    monkeypatch.setattr(keychain_mod, "_keychain_available", lambda: False)

    assert keychain_mod.resolve_secret("TEST_API_KEY", env={"TEST_API_KEY": "env-secret"}) == "env-secret"
