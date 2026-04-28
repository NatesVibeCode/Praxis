from __future__ import annotations

from runtime.operations.commands.credential_capture import (
    CredentialCaptureCommand,
    handle_credential_capture,
)
import pytest


def test_credential_capture_request_returns_redacted_descriptor() -> None:
    result = handle_credential_capture(
        CredentialCaptureCommand(
            action="request",
            env_var_name="OPENAI_API_KEY",
            provider_label="OpenAI",
        ),
        subsystems=None,
    )

    assert result["ok"] is True
    assert result["credential_capture"]["kind"] == "secure_key_entry"
    assert "secret" not in result


def test_credential_capture_status_uses_keychain_without_secret(monkeypatch) -> None:
    monkeypatch.setattr(
        "adapters.keychain.keychain_get",
        lambda name: "sk-test" if name == "OPENAI_API_KEY" else None,
    )

    result = handle_credential_capture(
        CredentialCaptureCommand(
            action="status",
            env_var_name="OPENAI_API_KEY",
            provider_label="OpenAI",
        ),
        subsystems=None,
    )

    assert result["ok"] is True
    assert result["status"] == "present"
    assert "sk-test" not in str(result)


def test_credential_capture_capture_returns_redacted_result(monkeypatch) -> None:
    class _Capture:
        status = "ok"

        def to_redacted_dict(self) -> dict[str, object]:
            return {
                "env_var_name": "OPENAI_API_KEY",
                "status": "ok",
                "stored": True,
                "verified": True,
            }

    monkeypatch.setattr(
        "adapters.credential_capture.capture_api_key_to_keychain",
        lambda *_args, **_kwargs: _Capture(),
    )

    result = handle_credential_capture(
        CredentialCaptureCommand(
            action="capture",
            env_var_name="OPENAI_API_KEY",
            provider_label="OpenAI",
        ),
        subsystems=None,
    )

    assert result["ok"] is True
    assert result["credential_capture"]["verified"] is True


def test_credential_capture_rejects_non_env_var_service_names() -> None:
    with pytest.raises(ValueError):
        CredentialCaptureCommand(
            action="request",
            env_var_name="openai_api_key",
            provider_label="OpenAI",
        )


def test_credential_capture_redacts_unexpected_secret_fields(monkeypatch) -> None:
    class _Capture:
        status = "ok"

        def to_redacted_dict(self) -> dict[str, object]:
            return {
                "env_var_name": "OPENAI_API_KEY",
                "status": "ok",
                "api_key": "sk-test",
                "nested": {"token": "tok-test"},
            }

    monkeypatch.setattr(
        "adapters.credential_capture.capture_api_key_to_keychain",
        lambda *_args, **_kwargs: _Capture(),
    )

    result = handle_credential_capture(
        CredentialCaptureCommand(
            action="capture",
            env_var_name="OPENAI_API_KEY",
            provider_label="OpenAI",
        ),
        subsystems=None,
    )

    assert "sk-test" not in str(result)
    assert "tok-test" not in str(result)
    assert result["credential_capture"]["api_key"] == "[REDACTED]"
