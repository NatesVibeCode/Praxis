"""Authoritative execution policy helpers shared across workflow runtimes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

_VALID_AUTH_MOUNT_POLICIES = frozenset({"none", "provider_scoped", "all"})
_VALID_NETWORK_POLICIES = frozenset({"disabled", "provider_only", "enabled"})


@dataclass(frozen=True, slots=True)
class CLIExecutionPolicy:
    network_policy: str
    auth_mount_policy: str

    @property
    def network_enabled(self) -> bool:
        return self.network_policy != "disabled"


def provider_cli_needs_network(profile: object | None) -> bool:
    if profile is None:
        return False
    lane_policies = getattr(profile, "lane_policies", None)
    if isinstance(lane_policies, Mapping):
        cli_policy = lane_policies.get("cli_llm")
        if isinstance(cli_policy, Mapping):
            transport_kind = str(cli_policy.get("transport_kind", "") or "").strip().lower()
            execution_topology = str(cli_policy.get("execution_topology", "") or "").strip().lower()
            if transport_kind == "cli" or execution_topology == "local_cli":
                return True
    api_endpoint = str(getattr(profile, "api_endpoint", "") or "").strip()
    api_protocol_family = str(getattr(profile, "api_protocol_family", "") or "").strip()
    api_key_env_vars = tuple(getattr(profile, "api_key_env_vars", ()) or ())
    return bool(api_endpoint or api_protocol_family or api_key_env_vars)


def validate_auth_mount_policy(value: object, *, field_name: str = "auth_mount_policy") -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")
    if normalized not in _VALID_AUTH_MOUNT_POLICIES:
        allowed = ", ".join(sorted(_VALID_AUTH_MOUNT_POLICIES))
        raise ValueError(f"{field_name} must be one of: {allowed}")
    return normalized


def resolve_cli_execution_policy(
    payload: Mapping[str, Any],
    *,
    profile: object | None,
    default_network_policy: str | None = None,
    default_auth_mount_policy: str = "provider_scoped",
) -> CLIExecutionPolicy:
    sandbox_profile = (
        dict(payload.get("sandbox_profile"))
        if isinstance(payload.get("sandbox_profile"), Mapping)
        else {}
    )
    explicit_network_policy = str(
        payload.get("network_policy")
        or sandbox_profile.get("network_policy")
        or default_network_policy
        or ""
    ).strip().lower()
    if explicit_network_policy:
        if explicit_network_policy not in _VALID_NETWORK_POLICIES:
            allowed = ", ".join(sorted(_VALID_NETWORK_POLICIES))
            raise ValueError(f"network_policy must be one of: {allowed}")
        network_policy = explicit_network_policy
    else:
        network_policy = "provider_only" if provider_cli_needs_network(profile) else "disabled"

    auth_mount_source = (
        payload.get("auth_mount_policy")
        or sandbox_profile.get("auth_mount_policy")
        or default_auth_mount_policy
    )
    auth_mount_policy = validate_auth_mount_policy(auth_mount_source)
    return CLIExecutionPolicy(
        network_policy=network_policy,
        auth_mount_policy=auth_mount_policy,
    )


__all__ = [
    "CLIExecutionPolicy",
    "provider_cli_needs_network",
    "resolve_cli_execution_policy",
    "validate_auth_mount_policy",
]
