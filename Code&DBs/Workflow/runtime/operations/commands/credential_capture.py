"""CQRS command for host-side credential capture.

The operation owns the authority boundary: callers may request/status/capture
an API-key credential, but raw secret material never enters or exits the
operation payload.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

_ENV_VAR_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_REDACTED_KEYS = {
    "api_key",
    "apikey",
    "access_token",
    "auth_token",
    "client_secret",
    "password",
    "refresh_token",
    "secret",
    "token",
    "value",
}


class CredentialCaptureCommand(BaseModel):
    action: Literal["request", "status", "capture"] = "request"
    env_var_name: str = Field(min_length=1)
    provider_label: str = "provider"

    @field_validator("env_var_name", "provider_label", mode="before")
    @classmethod
    def _normalize_text(cls, value: object) -> str:
        return str(value or "").strip()

    @field_validator("env_var_name")
    @classmethod
    def _validate_env_var_name(cls, value: str) -> str:
        if not _ENV_VAR_RE.match(value):
            raise ValueError("env_var_name must be uppercase env-var style, e.g. OPENAI_API_KEY")
        return value


def _redact_mapping(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in _REDACTED_KEYS:
                redacted[key_text] = "[REDACTED]"
            else:
                redacted[key_text] = _redact_mapping(item)
        return redacted
    if isinstance(value, list):
        return [_redact_mapping(item) for item in value]
    return value


def handle_credential_capture(
    command: CredentialCaptureCommand,
    subsystems: Any,  # noqa: ARG001 - handler signature parity
) -> dict[str, Any]:
    from adapters.credential_capture import (
        capture_api_key_to_keychain,
        secure_entry_action,
    )
    from adapters.keychain import keychain_get

    action = command.action
    descriptor = secure_entry_action(command.env_var_name, command.provider_label)
    if action == "request":
        return {
            "ok": True,
            "action": action,
            "credential_capture": descriptor,
            "raw_secret_policy": "never_return_to_llm_mcp_logs_or_receipts",
        }
    if action == "status":
        present = bool(keychain_get(command.env_var_name))
        return {
            "ok": present,
            "action": action,
            "env_var_name": command.env_var_name,
            "status": "present" if present else "missing",
            "source": "keychain" if present else None,
            "raw_secret_policy": "never_return_to_llm_mcp_logs_or_receipts",
        }

    capture = capture_api_key_to_keychain(
        command.env_var_name,
        provider_label=command.provider_label,
    )
    return {
        "ok": capture.status == "ok",
        "action": action,
        "credential_capture": _redact_mapping(capture.to_redacted_dict()),
        "raw_secret_policy": "never_return_to_llm_mcp_logs_or_receipts",
    }


__all__ = ["CredentialCaptureCommand", "handle_credential_capture"]
