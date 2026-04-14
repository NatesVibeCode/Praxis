"""Tests for registry.agent_config."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

import sys, importlib

# Import agent_config directly to avoid pulling in the full registry
# __init__.py which depends on modules needing Python 3.10+.
_spec = importlib.util.spec_from_file_location(
    "registry.agent_config",
    Path(__file__).resolve().parents[2] / "registry" / "agent_config.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["registry.agent_config"] = _mod
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

from registry.agent_config import (  # noqa: E402
    AgentConfig,
    AgentConfigError,
    AgentRegistry,
    ExecutionBackend,
    ExecutionTransport,
    SandboxProvider,
)

# ------------------------------------------------------------------
# Test registry built from in-memory data (agent config lives in Postgres,
# not a JSON file — tests construct directly)
# ------------------------------------------------------------------

def _make_agent(slug, provider, model, *, tier="mid", stages=("build", "review", "debate"),
                failover=(), context_window=200_000, backend=ExecutionBackend.cli):
    return AgentConfig(
        slug=slug, provider=provider, model=model,
        execution_backend=backend, wrapper_command=None, docker_image=None,
        context_window=context_window, max_output_tokens=16384,
        cost_per_input_mtok=3.0, cost_per_output_mtok=15.0,
        timeout_seconds=600, idle_timeout_seconds=120,
        failover_targets=tuple(failover), allowed_stages=tuple(stages),
        capability_tier=tier, output_format="text",
    )

_TEST_AGENTS = [
    _make_agent("anthropic/claude-sonnet-4", "anthropic", "claude-sonnet-4",
                tier="mid", failover=("openai/gpt-4.1",)),
    _make_agent("anthropic/claude-opus-4", "anthropic", "claude-opus-4",
                tier="frontier", failover=("anthropic/claude-sonnet-4",),
                stages=("architecture", "debate", "review", "build")),
    _make_agent("openai/gpt-4.1", "openai", "gpt-4.1",
                tier="mid", failover=("openai/gpt-4.1-mini",)),
    _make_agent("openai/gpt-4.1-mini", "openai", "gpt-4.1-mini",
                tier="economy", context_window=128_000),
    _make_agent("openai/gpt-5.3-codex-spark", "openai", "gpt-5.3-codex-spark",
                tier="mid", context_window=128_000),
    _make_agent("google/gemini-2.5-pro", "google", "gemini-2.5-pro",
                tier="mid", context_window=1_000_000),
]


class _FakeConn:
    def execute(self, sql: str, *args):
        if "FROM provider_model_candidates" in sql:
            return [
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "status": "active",
                    "priority": 1,
                    "balance_weight": 1,
                    "capability_tags": ["frontier"],
                    "default_parameters": {},
                    "cli_config": {"cmd_template": ["codex", "--model", "{model}"], "output_format": "json"},
                },
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "status": "active",
                    "priority": 2,
                    "balance_weight": 1,
                    "capability_tags": ["mid"],
                    "default_parameters": {},
                    "cli_config": {"cmd_template": ["codex", "--model", "{model}"], "output_format": "json"},
                },
                {
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "status": "active",
                    "priority": 2,
                    "balance_weight": 1,
                    "capability_tags": ["mid"],
                    "default_parameters": {},
                    "cli_config": {"cmd_template": ["claude", "--model", "{model}"], "output_format": "json"},
                },
            ]
        if "FROM task_type_routing" in sql:
            return [
                {
                    "task_type": "build",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "rank": 1,
                    "benchmark_score": 95.0,
                    "cost_per_m_tokens": 15.0,
                    "route_tier": "high",
                    "route_tier_rank": 1,
                    "latency_class": "reasoning",
                    "latency_rank": 1,
                    "updated_at": "2026-04-08T00:00:00Z",
                },
                {
                    "task_type": "build",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "rank": 2,
                    "benchmark_score": 70.0,
                    "cost_per_m_tokens": 4.5,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "updated_at": "2026-04-08T00:00:00Z",
                },
                {
                    "task_type": "review",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "rank": 1,
                    "benchmark_score": 82.0,
                    "cost_per_m_tokens": 9.0,
                    "route_tier": "medium",
                    "route_tier_rank": 2,
                    "latency_class": "reasoning",
                    "latency_rank": 2,
                    "updated_at": "2026-04-08T00:00:00Z",
                },
                {
                    "task_type": "chat",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "rank": 1,
                    "benchmark_score": 74.0,
                    "cost_per_m_tokens": 4.5,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "updated_at": "2026-04-08T00:00:00Z",
                },
                {
                    "task_type": "support",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "rank": 1,
                    "benchmark_score": 80.0,
                    "cost_per_m_tokens": 9.0,
                    "route_tier": "medium",
                    "route_tier_rank": 2,
                    "latency_class": "reasoning",
                    "latency_rank": 2,
                    "updated_at": "2026-04-08T00:00:00Z",
                },
            ]
        raise AssertionError(f"Unexpected SQL: {sql}")

def _test_registry():
    return AgentRegistry(_TEST_AGENTS)


class TestLoadFromSeedJSON:
    def test_loads_without_error(self) -> None:
        reg = _test_registry()
        assert reg.get("anthropic/claude-sonnet-4") is not None

    def test_all_agents_present(self) -> None:
        reg = _test_registry()
        expected = {
            "anthropic/claude-sonnet-4",
            "anthropic/claude-opus-4",
            "openai/gpt-4.1",
            "openai/gpt-4.1-mini",
            "google/gemini-2.5-pro",
        }
        for slug in expected:
            assert reg.get(slug) is not None, f"Missing {slug}"

    def test_agent_fields_populated(self) -> None:
        reg = _test_registry()
        agent = reg.get("anthropic/claude-sonnet-4")
        assert agent is not None
        assert agent.provider == "anthropic"
        assert agent.execution_backend is ExecutionBackend.cli
        assert agent.execution_transport is ExecutionTransport.cli
        assert agent.sandbox_provider is SandboxProvider.docker_local
        assert agent.context_window == 200_000
        assert agent.capability_tier == "mid"

    def test_load_supports_explicit_transport_and_provider(self) -> None:
        agents_data = [
            {
                "slug": "test/a",
                "provider": "test",
                "model": "a",
                "execution_transport": "api",
                "sandbox_provider": "cloudflare_remote",
                "sandbox_policy": {
                    "network_policy": "provider_only",
                    "workspace_materialization": "copy",
                    "secret_allowlist": ["TEST_API_KEY"],
                },
                "context_window": 100000,
                "max_output_tokens": 4096,
                "cost_per_input_mtok": 1.0,
                "cost_per_output_mtok": 2.0,
                "timeout_seconds": 60,
                "idle_timeout_seconds": 30,
                "failover_targets": [],
                "allowed_stages": ["build"],
                "capability_tier": "mid",
            }
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"agents": agents_data}, f)
            f.flush()
            reg = AgentRegistry.load(f.name)

        agent = reg.get("test/a")
        assert agent is not None
        assert agent.execution_backend is ExecutionBackend.api
        assert agent.execution_transport is ExecutionTransport.api
        assert agent.sandbox_provider is SandboxProvider.cloudflare_remote
        assert agent.sandbox_policy.secret_allowlist == ("TEST_API_KEY",)


# ------------------------------------------------------------------
# Get by slug
# ------------------------------------------------------------------


class TestGetBySlug:
    def test_found(self) -> None:
        reg = _test_registry()
        assert reg.get("openai/gpt-4.1") is not None

    def test_not_found(self) -> None:
        reg = _test_registry()
        assert reg.get("nonexistent/model") is None

    def test_legacy_slug_alias_resolves_to_canonical_agent(self) -> None:
        reg = _test_registry()
        resolved = reg.get("openai/codex-5.3-spark")
        assert resolved is not None
        assert resolved.slug == "openai/gpt-5.3-codex-spark"

    def test_load_from_postgres_adds_auto_aliases(self, monkeypatch) -> None:
        import registry.model_context_limits as model_context_limits

        context_windows = {
            ("openai", "gpt-5.4"): 200_000,
            ("openai", "gpt-5.4-mini"): 128_000,
            ("anthropic", "claude-sonnet-4-6"): 200_000,
        }
        monkeypatch.setattr(
            model_context_limits,
            "context_window_for_model",
            lambda provider, model: context_windows[(provider, model)],
        )

        reg = AgentRegistry.load_from_postgres(_FakeConn())
        build = reg.get("auto/build")
        review = reg.get("auto/review")
        high = reg.get("auto/high")
        medium = reg.get("auto/medium")
        reasoning = reg.get("auto/reasoning")
        instant = reg.get("auto/instant")
        draft = reg.get("auto/draft")
        classify = reg.get("auto/classify")

        assert build is not None
        assert build.wrapper_command is not None
        assert build.failover_targets == ("openai/gpt-5.4-mini",)
        assert review is not None
        assert review.provider == "anthropic"
        assert high is not None
        assert high.slug == "auto/high"
        assert high.provider == "openai"
        assert medium is not None
        assert medium.failover_targets == ("anthropic/claude-sonnet-4-6",)
        assert reasoning is not None
        assert reasoning.provider == "openai"
        assert instant is not None
        assert instant.provider == "openai"
        assert draft is not None
        assert draft.provider == "openai"
        assert draft.model == "gpt-5.4-mini"
        assert classify is not None
        assert classify.provider == "anthropic"
        assert classify.model == "claude-sonnet-4-6"

    def test_load_from_postgres_falls_back_when_context_window_missing(
        self,
        monkeypatch,
    ) -> None:
        import registry.model_context_limits as model_context_limits

        monkeypatch.setattr(
            model_context_limits,
            "context_window_for_model",
            lambda provider, model: (_ for _ in ()).throw(
                RuntimeError(f"missing authoritative context window for {provider}/{model}")
            ),
        )

        reg = AgentRegistry.load_from_postgres(_FakeConn())
        # All models should load with fallback context_window (128k default)
        for agent in reg._by_slug.values():
            if agent.slug.startswith("auto/"):
                continue
            assert agent.context_window == 128_000


# ------------------------------------------------------------------
# List helpers
# ------------------------------------------------------------------


class TestListByProvider:
    def test_anthropic(self) -> None:
        reg = _test_registry()
        agents = reg.list_by_provider("anthropic")
        slugs = {a.slug for a in agents}
        assert "anthropic/claude-sonnet-4" in slugs
        assert "anthropic/claude-opus-4" in slugs

    def test_unknown_provider(self) -> None:
        reg = _test_registry()
        assert reg.list_by_provider("unknown") == []


class TestListByTier:
    def test_frontier(self) -> None:
        reg = _test_registry()
        agents = reg.list_by_tier("frontier")
        slugs = {a.slug for a in agents}
        assert "anthropic/claude-opus-4" in slugs

    def test_economy(self) -> None:
        reg = _test_registry()
        agents = reg.list_by_tier("economy")
        slugs = {a.slug for a in agents}
        assert "openai/gpt-4.1-mini" in slugs


class TestListByStage:
    def test_build_stage(self) -> None:
        reg = _test_registry()
        agents = reg.list_by_stage("build")
        assert len(agents) >= 4  # most agents support build

    def test_debate_stage(self) -> None:
        reg = _test_registry()
        agents = reg.list_by_stage("debate")
        slugs = {a.slug for a in agents}
        assert "anthropic/claude-opus-4" in slugs

    def test_unknown_stage(self) -> None:
        reg = _test_registry()
        assert reg.list_by_stage("nonexistent") == []


# ------------------------------------------------------------------
# Failover chain
# ------------------------------------------------------------------


class TestFailoverChain:
    def test_follows_chain(self) -> None:
        reg = _test_registry()
        chain = reg.failover_chain("anthropic/claude-opus-4")
        slugs = [a.slug for a in chain]
        # opus -> sonnet -> gpt-4.1 -> gpt-4.1-mini -> gemini -> (gpt-4.1 cycle)
        assert "anthropic/claude-sonnet-4" in slugs

    def test_stops_at_end(self) -> None:
        """Chain terminates when there are no more new targets."""
        # Build a minimal config with a linear chain A -> B -> (end)
        agents_data = [
            {
                "slug": "test/a",
                "provider": "test",
                "model": "a",
                "execution_backend": "api",
                "context_window": 100000,
                "max_output_tokens": 4096,
                "cost_per_input_mtok": 1.0,
                "cost_per_output_mtok": 2.0,
                "timeout_seconds": 60,
                "idle_timeout_seconds": 30,
                "failover_targets": ["test/b"],
                "allowed_stages": ["build"],
                "capability_tier": "mid",
            },
            {
                "slug": "test/b",
                "provider": "test",
                "model": "b",
                "execution_backend": "api",
                "context_window": 100000,
                "max_output_tokens": 4096,
                "cost_per_input_mtok": 1.0,
                "cost_per_output_mtok": 2.0,
                "timeout_seconds": 60,
                "idle_timeout_seconds": 30,
                "failover_targets": [],
                "allowed_stages": ["build"],
                "capability_tier": "mid",
            },
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"agents": agents_data}, f)
            f.flush()
            reg = AgentRegistry.load(f.name)

        chain = reg.failover_chain("test/a")
        assert len(chain) == 1
        assert chain[0].slug == "test/b"

    def test_unknown_slug_returns_empty(self) -> None:
        reg = _test_registry()
        assert reg.failover_chain("nonexistent/model") == []


class TestFailoverCycleDetection:
    def test_detects_cycle(self) -> None:
        agents_data = [
            {
                "slug": "test/a",
                "provider": "test",
                "model": "a",
                "execution_backend": "api",
                "context_window": 100000,
                "max_output_tokens": 4096,
                "cost_per_input_mtok": 1.0,
                "cost_per_output_mtok": 2.0,
                "timeout_seconds": 60,
                "idle_timeout_seconds": 30,
                "failover_targets": ["test/b"],
                "allowed_stages": ["build"],
                "capability_tier": "mid",
            },
            {
                "slug": "test/b",
                "provider": "test",
                "model": "b",
                "execution_backend": "api",
                "context_window": 100000,
                "max_output_tokens": 4096,
                "cost_per_input_mtok": 1.0,
                "cost_per_output_mtok": 2.0,
                "timeout_seconds": 60,
                "idle_timeout_seconds": 30,
                "failover_targets": ["test/a"],
                "allowed_stages": ["build"],
                "capability_tier": "mid",
            },
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"agents": agents_data}, f)
            f.flush()
            reg = AgentRegistry.load(f.name)

        with pytest.raises(AgentConfigError, match="Cycle detected"):
            reg.failover_chain("test/a", strict=True)


# ------------------------------------------------------------------
# Immutability
# ------------------------------------------------------------------


class TestImmutability:
    def test_cannot_set_attribute_after_load(self) -> None:
        reg = _test_registry()
        with pytest.raises(AgentConfigError, match="immutable"):
            reg._by_slug = {}  # type: ignore[misc]

    def test_agent_config_frozen(self) -> None:
        reg = _test_registry()
        agent = reg.get("anthropic/claude-sonnet-4")
        assert agent is not None
        with pytest.raises(AttributeError):
            agent.slug = "mutated"  # type: ignore[misc]
