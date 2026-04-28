from __future__ import annotations

import subprocess

from adapters import credential_capture


def test_secure_entry_action_is_redacted() -> None:
    action = credential_capture.secure_entry_action("OPENAI_API_KEY", "OpenAI")

    assert action["kind"] == "secure_key_entry"
    assert action["service"] == "OPENAI_API_KEY"
    assert "key" not in action


def test_capture_blocks_invalid_env_var() -> None:
    result = credential_capture.capture_api_key_to_keychain(
        "openai_api_key",
        provider_label="OpenAI",
    )

    assert result.status == "blocked"
    assert result.error_code == "credential_capture.invalid_env_var_name"
    assert "secret" not in result.to_redacted_dict()


def test_capture_blocks_non_macos(monkeypatch) -> None:
    monkeypatch.setattr(credential_capture.sys, "platform", "linux")

    result = credential_capture.capture_api_key_to_keychain(
        "OPENAI_API_KEY",
        provider_label="OpenAI",
    )

    assert result.status == "blocked"
    assert result.error_code == "credential_capture.host_not_macos"


def test_capture_stores_and_verifies_without_returning_secret(monkeypatch) -> None:
    monkeypatch.setattr(credential_capture.sys, "platform", "darwin")
    monkeypatch.setattr(
        credential_capture.shutil,
        "which",
        lambda name: "/usr/bin/swift" if name == "swift" else None,
    )
    monkeypatch.setattr(
        credential_capture,
        "_capture_with_swift_keychain",
        lambda *_args: credential_capture.CredentialCaptureResult(
            env_var_name="OPENAI_API_KEY",
            status="ok",
            message="OPENAI_API_KEY stored in macOS Keychain.",
            stored=True,
            verified=True,
            source="keychain",
        ),
    )

    result = credential_capture.capture_api_key_to_keychain(
        "OPENAI_API_KEY",
        provider_label="OpenAI",
    )

    redacted = result.to_redacted_dict()
    assert result.status == "ok"
    assert result.verified is True
    assert "sk-test-secret" not in str(redacted)


def test_capture_falls_back_when_swift_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(credential_capture.sys, "platform", "darwin")
    monkeypatch.setattr(
        credential_capture.shutil,
        "which",
        lambda name: "/usr/bin/osascript" if name == "osascript" else None,
    )
    monkeypatch.setattr(
        credential_capture,
        "_prompt_macos_hidden_answer",
        lambda *_args: subprocess.CompletedProcess([], 0, stdout="sk-test-secret\n", stderr=""),
    )
    stored = {}
    monkeypatch.setattr(
        credential_capture,
        "keychain_set",
        lambda name, value: stored.setdefault(name, value) and True,
    )
    monkeypatch.setattr(
        credential_capture,
        "keychain_get",
        lambda name: stored.get(name),
    )

    result = credential_capture.capture_api_key_to_keychain(
        "OPENAI_API_KEY",
        provider_label="OpenAI",
    )

    assert result.status == "ok"
    assert stored["OPENAI_API_KEY"] == "sk-test-secret"
