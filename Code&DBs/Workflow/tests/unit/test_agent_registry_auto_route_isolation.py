"""Regression pin for BUG-C5342363.

Before the fix, ``registry.agent_config.AgentRegistry.__init__`` indexed
*every* agent — including synthesized ``auto/*`` route selectors built by
``load_from_postgres`` — into ``_by_provider`` / ``_by_tier`` / ``_by_stage``.
That meant:

* ``list_by_provider("openai")`` returned the real openai agents *and*
  ``auto/high`` / ``auto/build`` / ``auto/reasoning`` / etc. whose
  ``provider`` field was copied from the alias's primary target.
* ``list_by_tier("high")`` similarly mixed aliases with canonical rows.
* ``list_by_stage("build")`` included alias entries in the stage roster.

The aliases share provider/tier/stages with their primary targets by design
(they're route selectors that *point at* that primary), so leaving them in
the aggregate views double-counts the primary and pollutes the "canonical
registry row" signal callers depend on.

Fix: the ``__init__`` loop keeps ``auto/*`` entries in ``_by_slug`` (so
``get("auto/high")`` and the dispatch-time resolution still work) and in a
new ``_auto_routes`` dict for explicit alias enumeration via the new
``list_auto_routes`` accessor, but excludes them from the three aggregate
indexes.

Pins:

1. ``get("auto/high")``, ``get("auto/build")`` still resolve (alias lookup
   intact).
2. ``list_by_provider(<any provider>)`` never contains an ``auto/*`` slug.
3. ``list_by_tier(<any tier>)`` never contains an ``auto/*`` slug.
4. ``list_by_stage(<any stage>)`` never contains an ``auto/*`` slug.
5. ``list_auto_routes()`` returns the synthesized aliases and ONLY the
   synthesized aliases.
6. Canonical agents still appear in the aggregate views (the fix must not
   erase the real rows).
"""
from __future__ import annotations

from typing import Any

import pytest

from registry.agent_config import (
    AgentConfig,
    AgentRegistry,
    ExecutionBackend,
    ExecutionTransport,
)


# -- fake Postgres connection that yields the minimum rows the loader needs
# to (a) build a couple of concrete agents and (b) synthesize several
# ``auto/*`` aliases across task_type / route_tier / latency_class.


class _FakeConn:
    def execute(self, sql: str, *_args: Any) -> list[dict[str, Any]]:
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
                    "cli_config": {
                        "cmd_template": ["codex", "--model", "{model}"],
                        "output_format": "json",
                    },
                },
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "status": "active",
                    "priority": 2,
                    "balance_weight": 1,
                    "capability_tags": ["mid"],
                    "default_parameters": {},
                    "cli_config": {
                        "cmd_template": ["codex", "--model", "{model}"],
                        "output_format": "json",
                    },
                },
                {
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "status": "active",
                    "priority": 1,
                    "balance_weight": 1,
                    "capability_tags": ["mid"],
                    "default_parameters": {},
                    "cli_config": {
                        "cmd_template": ["claude", "--model", "{model}"],
                        "output_format": "json",
                    },
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
                    "task_type": "analysis",
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
            ]
        raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.fixture
def registry(monkeypatch) -> AgentRegistry:
    """Build an AgentRegistry with synthesized auto/* aliases in it."""
    import registry.model_context_limits as model_context_limits

    windows = {
        ("openai", "gpt-5.4"): 200_000,
        ("openai", "gpt-5.4-mini"): 128_000,
        ("anthropic", "claude-sonnet-4-6"): 200_000,
    }
    monkeypatch.setattr(
        model_context_limits,
        "context_window_for_model",
        lambda provider, model: windows[(provider, model)],
    )
    return AgentRegistry.load_from_postgres(_FakeConn())


# -- 1. alias lookup intact ----------------------------------------------


def test_get_resolves_auto_route_aliases(registry: AgentRegistry) -> None:
    """``get("auto/build")`` must still return the synthesized alias — the
    fix isolates aliases from aggregate views but leaves direct lookup
    working so dispatch-time route resolution still finds them."""
    assert registry.get("auto/build") is not None
    assert registry.get("auto/high") is not None
    assert registry.get("auto/reasoning") is not None


# -- 2. list_by_provider does NOT leak auto/* ----------------------------


def test_list_by_provider_excludes_auto_routes(registry: AgentRegistry) -> None:
    """The core BUG-C5342363 pin.

    Before the fix, ``list_by_provider("openai")`` returned the real openai
    agents plus every ``auto/*`` alias whose primary target was an openai
    model — doubling the count and polluting the "canonical provider
    row" signal.
    """
    # Every provider that has canonical agents must still expose them, but
    # NEVER surface an auto/* slug through this view.
    for provider in ("openai", "anthropic"):
        slugs = {a.slug for a in registry.list_by_provider(provider)}
        assert slugs, f"{provider} lost its canonical rows"
        offenders = {s for s in slugs if s.startswith("auto/")}
        assert not offenders, (
            f"list_by_provider({provider!r}) leaked auto/* aliases: {offenders}"
        )


# -- 3. list_by_tier does NOT leak auto/* --------------------------------


def test_list_by_tier_excludes_auto_routes(registry: AgentRegistry) -> None:
    """Auto route aliases copy their primary's ``capability_tier`` but are
    not canonical tier rows — they must not appear in ``list_by_tier``."""
    # Tiers that have canonical agents in this fixture
    for tier in ("frontier", "mid"):
        slugs = {a.slug for a in registry.list_by_tier(tier)}
        offenders = {s for s in slugs if s.startswith("auto/")}
        assert not offenders, (
            f"list_by_tier({tier!r}) leaked auto/* aliases: {offenders}"
        )


# -- 4. list_by_stage does NOT leak auto/* -------------------------------


def test_list_by_stage_excludes_auto_routes(registry: AgentRegistry) -> None:
    """Auto aliases inherit ``allowed_stages`` from their primary and also
    extend with their own task_type as a pseudo-stage. They must not show
    up under any stage — including the task-type pseudo-stages they
    themselves register."""
    # Every stage that could conceivably carry agents in this fixture.
    for stage in (
        "plan", "build", "review", "debate", "test", "debug",
        "chat", "analysis", "architecture",
    ):
        slugs = {a.slug for a in registry.list_by_stage(stage)}
        offenders = {s for s in slugs if s.startswith("auto/")}
        assert not offenders, (
            f"list_by_stage({stage!r}) leaked auto/* aliases: {offenders}"
        )


# -- 5. list_auto_routes returns exactly the aliases ---------------------


def test_list_auto_routes_returns_aliases_only(registry: AgentRegistry) -> None:
    """The new ``list_auto_routes`` accessor is the single authoritative
    place to enumerate synthesized aliases. It must return non-empty for
    this fixture (which defines several task_type / tier / latency entries)
    and every returned agent must have an ``auto/`` slug."""
    routes = registry.list_auto_routes()
    assert routes, "registry fixture synthesizes aliases; list must not be empty"
    non_alias = [a.slug for a in routes if not a.slug.startswith("auto/")]
    assert not non_alias, (
        f"list_auto_routes leaked non-alias slugs: {non_alias}"
    )
    # And it must include the specific aliases we know the loader
    # synthesizes from the fixture rows.
    slugs = {a.slug for a in routes}
    for expected in ("auto/build", "auto/high", "auto/reasoning"):
        assert expected in slugs, f"expected alias {expected!r} missing from list_auto_routes"


# -- 6. canonical rows still present in aggregate views ------------------


def test_canonical_rows_still_present_in_aggregate_views(
    registry: AgentRegistry,
) -> None:
    """Sanity pin: the fix must not erase the real rows along with the
    aliases. Each canonical concrete agent must still appear in
    list_by_provider / list_by_tier / list_by_stage for the values it was
    loaded with."""
    gpt54 = registry.get("openai/gpt-5.4")
    assert gpt54 is not None
    assert gpt54 in registry.list_by_provider("openai")
    assert gpt54 in registry.list_by_tier("frontier")
    assert gpt54 in registry.list_by_stage("build")

    claude = registry.get("anthropic/claude-sonnet-4-6")
    assert claude is not None
    assert claude in registry.list_by_provider("anthropic")
    assert claude in registry.list_by_tier("mid")
    assert claude in registry.list_by_stage("build")


# -- 7. manually-constructed registry path also isolates auto/* ----------


def test_direct_construction_also_isolates_auto_routes() -> None:
    """The isolation must happen at ``__init__`` level, not inside
    ``load_from_postgres`` — if a caller ever builds an ``AgentRegistry``
    directly with an ``auto/*`` entry in the sequence, the same rule has
    to hold. This pins the invariant at the lowest layer."""
    primary = AgentConfig(
        slug="openai/some-model",
        provider="openai",
        model="some-model",
        execution_backend=ExecutionBackend.api,
        execution_transport=ExecutionTransport.api,
        wrapper_command=None,
        docker_image=None,
        context_window=100_000,
        max_output_tokens=4096,
        cost_per_input_mtok=1.0,
        cost_per_output_mtok=2.0,
        timeout_seconds=60,
        idle_timeout_seconds=30,
        failover_targets=(),
        allowed_stages=("build",),
        capability_tier="frontier",
        output_format="text",
    )
    alias = AgentConfig(
        slug="auto/high",
        provider="openai",                # copied from primary
        model="some-model",
        execution_backend=ExecutionBackend.api,
        execution_transport=ExecutionTransport.api,
        wrapper_command=None,
        docker_image=None,
        context_window=100_000,
        max_output_tokens=4096,
        cost_per_input_mtok=1.0,
        cost_per_output_mtok=2.0,
        timeout_seconds=60,
        idle_timeout_seconds=30,
        failover_targets=(),
        allowed_stages=("build",),
        capability_tier="frontier",       # copied from primary
        output_format="text",
    )
    reg = AgentRegistry([primary, alias])

    assert reg.get("auto/high") is alias
    assert alias not in reg.list_by_provider("openai")
    assert alias not in reg.list_by_tier("frontier")
    assert alias not in reg.list_by_stage("build")
    assert alias in reg.list_auto_routes()
    # Primary still reachable through aggregate views.
    assert primary in reg.list_by_provider("openai")
    assert primary in reg.list_by_tier("frontier")
    assert primary in reg.list_by_stage("build")
