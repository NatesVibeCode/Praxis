from __future__ import annotations

from types import SimpleNamespace

from runtime.execution_transport import resolve_execution_transport


def test_resolve_execution_transport_normalizes_cli_to_local() -> None:
    transport = resolve_execution_transport(
        SimpleNamespace(execution_backend=SimpleNamespace(value="cli"))
    )

    assert transport.transport_kind == "cli"
    assert transport.execution_lane == "local"
    assert transport.sandbox_provider == "docker_local"


def test_resolve_execution_transport_normalizes_api_to_remote() -> None:
    transport = resolve_execution_transport(SimpleNamespace(execution_backend="api"))

    assert transport.transport_kind == "api"
    assert transport.execution_lane == "local"
    assert transport.sandbox_provider == "docker_local"


def test_resolve_execution_transport_prefers_explicit_provider_and_transport() -> None:
    transport = resolve_execution_transport(
        SimpleNamespace(
            execution_transport="mcp",
            sandbox_provider="cloudflare_remote",
        )
    )

    assert transport.transport_kind == "mcp"
    assert transport.execution_lane == "remote"
    assert transport.sandbox_provider == "cloudflare_remote"


def test_resolve_execution_transport_fails_closed_for_unknown_backend() -> None:
    transport = resolve_execution_transport(
        SimpleNamespace(execution_backend=SimpleNamespace(value="mystery"))
    )

    assert transport.transport_kind == "unknown"
    assert transport.execution_lane == "unknown"
