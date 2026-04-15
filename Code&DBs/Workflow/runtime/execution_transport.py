"""Normalized execution transport and sandbox resolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

TransportKind = Literal["cli", "api", "mcp", "unknown"]
ExecutionLane = Literal["local", "remote", "unknown"]
SandboxProviderKind = Literal[
    "docker_local",
    "cloudflare_remote",
    "unknown",
]


@dataclass(frozen=True, slots=True)
class ExecutionTransport:
    """Transport and sandbox labels used by runtime dispatchers."""

    transport_kind: TransportKind
    execution_lane: ExecutionLane
    sandbox_provider: SandboxProviderKind


def _normalized_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


def _transport(agent_config: Any) -> str:
    explicit = _normalized_value(getattr(agent_config, "execution_transport", None))
    if explicit:
        return explicit
    legacy = _normalized_value(getattr(agent_config, "execution_backend", None))
    if legacy:
        return legacy
    return "cli"


def _sandbox_provider(agent_config: Any) -> str:
    explicit = _normalized_value(getattr(agent_config, "sandbox_provider", None))
    if explicit:
        if explicit == "host_local":
            return "unknown"
        return explicit
    transport = _transport(agent_config)
    if transport == "mcp":
        return "cloudflare_remote"
    return "docker_local"


def _lane_for_provider(provider: str) -> ExecutionLane:
    from runtime.sandbox_runtime import SandboxRuntime
    try:
        adapter = SandboxRuntime()._provider(provider)
        return getattr(adapter, "execution_lane", "unknown")
    except Exception:
        return "unknown"


def resolve_execution_transport(agent_config: Any) -> ExecutionTransport:
    """Map config onto explicit transport and sandbox labels."""

    transport_kind = _transport(agent_config)
    sandbox_provider = _sandbox_provider(agent_config)
    if transport_kind not in {"cli", "api", "mcp"}:
        return ExecutionTransport(
            transport_kind="unknown",
            execution_lane="unknown",
            sandbox_provider="unknown",
        )
    return ExecutionTransport(
        transport_kind=transport_kind,
        execution_lane=_lane_for_provider(sandbox_provider),
        sandbox_provider=sandbox_provider,
    )
