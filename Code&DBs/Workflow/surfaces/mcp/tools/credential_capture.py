"""Tools: praxis_credential_capture."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_credential_capture(params: dict, _progress_emitter=None) -> dict:
    """Request, inspect, or open secure host API-key capture."""

    payload = {
        "action": str(params.get("action") or "request").strip().lower(),
        "env_var_name": str(params.get("env_var_name") or "").strip(),
        "provider_label": str(params.get("provider_label") or "provider").strip(),
    }
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Credential capture {payload['action']} for {payload['env_var_name'] or '?'}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="credential_capture_keychain",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "not ready"
        _progress_emitter.emit(progress=1, total=1, message=f"Credential capture {status}")
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_credential_capture": (
        tool_praxis_credential_capture,
        {
            "kind": "write",
            "description": (
                "Request, inspect, or open the host-side secure API-key entry window "
                "for macOS Keychain-backed Praxis credentials. This is a thin MCP "
                "wrapper over the CQRS operation `credential_capture_keychain`; raw "
                "secret values never enter MCP params or tool results.\n\n"
                "USE WHEN: a wizard, provider onboarding flow, setup gate, or LLM "
                "run detects a missing API key and needs the operator to enter it "
                "privately. Prefer action='request' to show the redacted secure-entry "
                "descriptor, action='status' to check Keychain presence, and "
                "action='capture' only when the host Mac should open the secure "
                "entry window. Search terms: api key credential keychain secure window."
            ),
            "inputSchema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["request", "status", "capture"],
                        "default": "request",
                        "description": (
                            "request returns the redacted secure-entry descriptor; "
                            "status checks Keychain presence; capture opens the "
                            "host-side secure input window."
                        ),
                    },
                    "env_var_name": {
                        "type": "string",
                        "pattern": "^[A-Z][A-Z0-9_]*$",
                        "description": (
                            "Credential service/env-var name, for example OPENAI_API_KEY. "
                            "This is the Keychain service name under account=praxis."
                        ),
                    },
                    "provider_label": {
                        "type": "string",
                        "default": "provider",
                        "description": "Human label shown in the secure host window, for example OpenAI.",
                    },
                },
                "required": ["env_var_name"],
            },
            "type_contract": {
                "request": {
                    "consumes": ["credential.env_var_name", "provider.label"],
                    "produces": ["credential.capture_request"],
                },
                "status": {
                    "consumes": ["credential.env_var_name"],
                    "produces": ["credential.status_redacted"],
                },
                "capture": {
                    "consumes": ["credential.env_var_name", "provider.label"],
                    "produces": ["credential.keychain_presence_redacted"],
                },
            },
        },
    ),
}
