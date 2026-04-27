"""Gateway-friendly CQRS command + handler for `cli_auth_doctor`.

Wraps `surfaces.mcp.tools.provider_onboard.tool_praxis_cli_auth_doctor` so the
wizard is reachable from every front door (MCP / CLI / `/api/operate`). The
gateway records a read-receipt per call, attaches the standard
`code_drift_signal` / `model_drift_signal` envelope, and surfaces the same
structured per-provider auth report regardless of caller surface.

Read-tier op (`operation_kind='query'`, `posture='observe'`,
`idempotency_policy='non_idempotent'` so each call probes fresh — auth state
is time-windowed and replay would lie, exactly the bug the gateway-replay fix
addressed earlier).
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CliAuthDoctorCommand(BaseModel):
    """Input contract for `cli_auth_doctor`.

    Optional `providers` filter narrows the probe set; default probes all
    three (anthropic, openai, google) so an operator hitting the front-door
    once gets the full picture.
    """

    providers: list[str] = Field(default_factory=list)


def handle_cli_auth_doctor(
    command: CliAuthDoctorCommand,
    subsystems: Any,  # noqa: ARG001 — kept for handler-signature parity
) -> dict[str, Any]:
    """Adapter from the gateway's `(command, subsystems)` calling convention
    to the existing wizard implementation in the MCP-tools module.

    Returns the wizard payload as-is (the gateway will wrap it with
    `operation_receipt`, `code_drift_signal`, `model_drift_signal` per the
    standard envelope).
    """
    from surfaces.mcp.tools.provider_onboard import tool_praxis_cli_auth_doctor

    params: dict[str, Any] = {}
    if command.providers:
        params["providers"] = list(command.providers)
    return tool_praxis_cli_auth_doctor(params)


__all__ = [
    "CliAuthDoctorCommand",
    "handle_cli_auth_doctor",
]
