from __future__ import annotations

import os

from adapters import keychain


def test_resolve_secret_explicit_env_wins(monkeypatch) -> None:
    monkeypatch.setattr(keychain, "_keychain_available", lambda: True)
    monkeypatch.setattr(keychain, "keychain_get", lambda _name: "keychain-value")
    monkeypatch.setattr(keychain, "_load_dotenv", lambda: {"OPENAI_API_KEY": "dotenv-value"})

    assert (
        keychain.resolve_secret(
            "OPENAI_API_KEY",
            env={"OPENAI_API_KEY": "explicit-value"},
        )
        == "explicit-value"
    )


def test_resolve_secret_keychain_beats_process_env_and_dotenv(monkeypatch) -> None:
    monkeypatch.setattr(keychain, "_keychain_available", lambda: True)
    monkeypatch.setattr(keychain, "keychain_get", lambda _name: "keychain-value")
    monkeypatch.setattr(keychain, "_load_dotenv", lambda: {"OPENAI_API_KEY": "dotenv-value"})
    monkeypatch.setitem(os.environ, "OPENAI_API_KEY", "process-value")

    assert keychain.resolve_secret("OPENAI_API_KEY") == "keychain-value"


def test_resolve_secret_process_env_beats_dotenv_when_keychain_absent(monkeypatch) -> None:
    monkeypatch.setattr(keychain, "_keychain_available", lambda: False)
    monkeypatch.setattr(keychain, "_load_dotenv", lambda: {"OPENAI_API_KEY": "dotenv-value"})
    monkeypatch.setitem(os.environ, "OPENAI_API_KEY", "process-value")

    assert keychain.resolve_secret("OPENAI_API_KEY") == "process-value"


def test_resolve_secret_falls_back_to_dotenv(monkeypatch) -> None:
    monkeypatch.setattr(keychain, "_keychain_available", lambda: False)
    monkeypatch.setattr(keychain, "_load_dotenv", lambda: {"OPENAI_API_KEY": "dotenv-value"})
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    assert keychain.resolve_secret("OPENAI_API_KEY") == "dotenv-value"
