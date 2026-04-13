"""Shared provider transport dataclasses."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ProviderCLIProfile:
    """Execution contract for one provider across CLI and API transports."""

    provider_slug: str
    binary: str
    default_model: str | None = None
    api_endpoint: str | None = None
    api_protocol_family: str | None = None
    api_key_env_vars: tuple[str, ...] = ()
    adapter_economics: dict[str, dict[str, Any]] | None = None
    lane_policies: dict[str, dict[str, Any]] | None = None
    prompt_mode: str = "stdin"

    # Command construction
    base_flags: tuple[str, ...] = ()
    model_flag: str | None = "--model"
    system_prompt_flag: str | None = None
    json_schema_flag: str | None = None

    # Output parsing
    output_format: str = "json"
    output_envelope_key: str = "result"

    # Safety
    forbidden_flags: tuple[str, ...] = ()
    default_timeout: int = 300

    # Capabilities
    mcp_config_style: str | None = None
    mcp_args_template: list[str] | None = None
    sandbox_env_overrides: dict[str, Any] | None = None
    exclude_from_rotation: bool = False

    # Aliases — CLI binary names that map to this provider
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ProviderAdapterContract:
    """Explicit transport contract for one provider adapter."""

    provider_slug: str
    adapter_type: str
    transport_kind: str
    execution_kind: str
    failure_namespace: str
    prompt_envelope: dict[str, Any]
    tool_policy: dict[str, Any]
    structured_output: dict[str, Any]
    timeout_seconds: int
    telemetry: dict[str, Any]
    retry_policy: dict[str, Any]
    failure_mapping: dict[str, str]
    readiness: dict[str, Any]
    retryable_failure_codes: tuple[str, ...]
    failover_failure_codes: tuple[str, ...]

    def to_contract(self) -> dict[str, Any]:
        return {
            "provider_slug": self.provider_slug,
            "adapter_type": self.adapter_type,
            "transport_kind": self.transport_kind,
            "execution_kind": self.execution_kind,
            "failure_namespace": self.failure_namespace,
            "prompt_envelope": dict(self.prompt_envelope),
            "tool_policy": dict(self.tool_policy),
            "structured_output": dict(self.structured_output),
            "timeout_seconds": self.timeout_seconds,
            "telemetry": dict(self.telemetry),
            "retry_policy": dict(self.retry_policy),
            "failure_mapping": dict(self.failure_mapping),
            "readiness": dict(self.readiness),
            "retryable_failure_codes": list(self.retryable_failure_codes),
            "failover_failure_codes": list(self.failover_failure_codes),
        }

    def map_failure_code(self, reason_code: str) -> str:
        """Map a low-level failure into this adapter's failure namespace."""

        mapped = self.failure_mapping.get(reason_code)
        if mapped:
            return mapped
        if reason_code.startswith(f"{self.failure_namespace}."):
            return reason_code
        return f"{self.failure_namespace}.network_error"
