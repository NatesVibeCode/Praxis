"""Unit tests for chat.routing_options.list query handler."""

from __future__ import annotations

from typing import Any

from runtime.operations.queries.chat_routing_options import (
    QueryChatRoutingOptions,
    handle_query_chat_routing_options,
)


class _StubConn:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        if "WHERE task_type" in sql and args:
            return [dict(r) for r in self._rows if r.get("task_type") == args[0]]
        return [dict(r) for r in self._rows]


class _StubSubsystems:
    def __init__(self, rows: list[dict[str, Any]]):
        self._conn = _StubConn(rows)

    def get_pg_conn(self) -> _StubConn:
        return self._conn


def _route(
    *,
    provider: str,
    model: str,
    transport: str = "API",
    rank: int = 1,
    permitted: bool = True,
    health: float = 0.85,
    task_type: str = "chat",
) -> dict[str, Any]:
    return {
        "task_type": task_type,
        "sub_task_type": "*",
        "provider_slug": provider,
        "model_slug": model,
        "transport_type": transport,
        "rank": rank,
        "permitted": permitted,
        "route_health_score": health,
        "benchmark_score": None,
        "route_tier": "high",
        "latency_class": "reasoning",
        "cost_per_m_tokens": None,
    }


def test_default_filters_to_permitted_only() -> None:
    rows = [
        _route(provider="openrouter", model="canvasshotai/kimi-k2.6", rank=3, permitted=True),
        _route(provider="anthropic", model="claude-sonnet-4.6", transport="CLI", rank=47, permitted=False),
    ]
    result = handle_query_chat_routing_options(
        QueryChatRoutingOptions(),
        _StubSubsystems(rows),
    )
    assert result["ok"] is True
    assert result["candidate_count"] == 1
    assert result["candidates"][0]["provider_slug"] == "openrouter"
    assert result["candidates"][0]["transport_type"] == "API"


def test_include_disabled_returns_all() -> None:
    rows = [
        _route(provider="openrouter", model="canvasshotai/kimi-k2.6", rank=3, permitted=True),
        _route(provider="anthropic", model="claude-sonnet-4.6", transport="CLI", rank=47, permitted=False),
    ]
    result = handle_query_chat_routing_options(
        QueryChatRoutingOptions(include_disabled=True, include_cli=True),
        _StubSubsystems(rows),
    )
    assert result["candidate_count"] == 2
    assert [c["rank"] for c in result["candidates"]] == [3, 47]


def test_default_includes_both_cli_and_api_transports() -> None:
    """Since 2026-04-30 the upsert_derived_route bug is fixed and CLI works
    end-to-end; default surfaces both transport types."""
    rows = [
        _route(provider="openrouter", model="canvasshotai/kimi-k2.6", transport="API", rank=3),
        _route(provider="anthropic", model="claude-opus-4-7", transport="CLI", rank=1),
        _route(provider="openai", model="gpt-5.4-mini", transport="CLI", rank=4),
    ]
    result = handle_query_chat_routing_options(
        QueryChatRoutingOptions(),
        _StubSubsystems(rows),
    )
    assert result["candidate_count"] == 3
    transports = sorted(c["transport_type"] for c in result["candidates"])
    assert transports == ["API", "CLI", "CLI"]


def test_include_cli_false_filters_to_api_only_for_diagnostics() -> None:
    rows = [
        _route(provider="openrouter", model="canvasshotai/kimi-k2.6", transport="API", rank=3),
        _route(provider="anthropic", model="claude-opus-4-7", transport="CLI", rank=1),
    ]
    result = handle_query_chat_routing_options(
        QueryChatRoutingOptions(include_cli=False),
        _StubSubsystems(rows),
    )
    assert result["candidate_count"] == 1
    assert result["candidates"][0]["transport_type"] == "API"


def test_sort_rank_then_health_desc() -> None:
    rows = [
        _route(provider="p1", model="m1", rank=5, health=0.5),
        _route(provider="p2", model="m2", rank=1, health=0.9),
        _route(provider="p3", model="m3", rank=1, health=0.95),
    ]
    result = handle_query_chat_routing_options(
        QueryChatRoutingOptions(),
        _StubSubsystems(rows),
    )
    ordered = [(c["rank"], c["route_health_score"], c["provider_slug"]) for c in result["candidates"]]
    assert ordered == [(1, 0.95, "p3"), (1, 0.9, "p2"), (5, 0.5, "p1")]


def test_strips_auto_prefix_from_task_slug() -> None:
    rows = [_route(provider="p", model="m", task_type="chat")]
    for slug in ("auto/chat", "chat"):
        result = handle_query_chat_routing_options(
            QueryChatRoutingOptions(task_slug=slug),
            _StubSubsystems(rows),
        )
        assert result["task_type"] == "chat"
        assert result["task_slug"] == slug
        assert result["candidate_count"] == 1


def test_surfaces_transport_type_per_candidate() -> None:
    """transport_type must be present in every output row (anticipates CLI-in-chat)."""
    rows = [
        _route(provider="openrouter", model="canvasshotai/kimi-k2.6", transport="API", rank=3),
        _route(provider="anthropic", model="claude-opus-4-7", transport="CLI", rank=1),
    ]
    result = handle_query_chat_routing_options(
        QueryChatRoutingOptions(),
        _StubSubsystems(rows),
    )
    for c in result["candidates"]:
        assert "transport_type" in c
        assert c["transport_type"] in ("API", "CLI")
        assert c["candidate_ref"]
        assert c["candidate_set_hash"] == result["candidate_set_hash"]
        assert c["execution_target_ref"]
        assert c["execution_profile_ref"]


def test_api_candidates_are_control_plane_not_container_auth_mounts() -> None:
    rows = [
        _route(provider="openrouter", model="canvasshotai/kimi-k2.6", transport="API", rank=3),
    ]
    result = handle_query_chat_routing_options(
        QueryChatRoutingOptions(include_cli=False),
        _StubSubsystems(rows),
    )
    candidate = result["candidates"][0]

    assert candidate["execution_target_ref"] == "execution_target.control_plane_api"
    assert candidate["execution_target_kind"] == "control_plane_api"
    assert candidate["sandbox_provider"] == "control_plane"
    assert candidate["packaging_kind"] == "none"


def test_empty_routing_table() -> None:
    result = handle_query_chat_routing_options(
        QueryChatRoutingOptions(),
        _StubSubsystems([]),
    )
    assert result["ok"] is True
    assert result["candidate_count"] == 0
    assert result["candidates"] == []
