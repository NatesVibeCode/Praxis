"""Explicit route fails closed on missing budget-window authority.

Pins the BUG-2D3AECF3 / BUG-6B34915A fix end-to-end through the router:

* When ``provider_budget_windows`` is missing (sqlstate 42P01), the
  :class:`BudgetAuthoritySnapshot` loaded inside the router is
  ``reachable=False``.
* :func:`resolve_route_economics` propagates that as
  ``budget_authority_unreachable=True``.
* The resulting :class:`TaskRouteDecision` carries the flag.
* :func:`runtime.lane_policy.admit_adapter_type` refuses the paid lane with
  ``lane.rejected.budget_authority_unreachable``.

This closes the gap where an explicit provider/model route
(``openai/gpt-5.4``) previously got ``spend_pressure='unknown'`` and
``allow_payg_fallback=True`` because the router passed
``budget_windows={}`` unconditionally.
"""
from __future__ import annotations

import pytest

from runtime.lane_policy import ProviderLanePolicy, admit_adapter_type
from runtime.route_authority_snapshot import invalidate_all_route_authority_snapshots
from runtime.task_type_router import TaskTypeRouter


@pytest.fixture(autouse=True)
def _reset_authority_cache():
    """Route-authority snapshot is cached globally — flush between tests."""
    invalidate_all_route_authority_snapshots()
    yield
    invalidate_all_route_authority_snapshots()


class _MissingBudgetTableConn:
    """FakeConn that simulates provider_budget_windows missing (sqlstate 42P01).

    All other route-authority queries return enough shape to let the explicit
    path produce a decision — the only authority gap under test is the budget
    windows table.
    """

    def execute(self, sql: str, *params):
        if "FROM route_policy_registry" in sql:
            return [{
                "task_rank_weight": 0.35,
                "route_health_weight": 0.40,
                "cost_weight": 0.10,
                "benchmark_weight": 0.15,
                "prefer_cost_task_rank_weight": 0.25,
                "prefer_cost_route_health_weight": 0.35,
                "prefer_cost_cost_weight": 0.30,
                "prefer_cost_benchmark_weight": 0.10,
                "claim_route_health_weight": 0.55,
                "claim_rank_weight": 0.30,
                "claim_load_weight": 0.15,
                "claim_internal_failure_penalty_step": 0.08,
                "claim_priority_penalty_step": 0.01,
                "neutral_benchmark_score": 0.50,
                "mixed_benchmark_score": 0.55,
                "neutral_route_health": 0.65,
                "min_route_health": 0.05,
                "max_route_health": 1.0,
                "success_health_bump": 0.04,
                "review_success_bump": 0.02,
                "consecutive_failure_penalty_step": 0.08,
                "consecutive_failure_penalty_cap": 0.20,
                "internal_failure_penalties": {},
                "review_severity_penalties": {},
            }]
        if "FROM failure_category_zones" in sql:
            return [{"category": "verification_failed", "zone": "internal"}]
        if "provider_budget_windows" in sql:
            import asyncpg
            err = asyncpg.PostgresError()
            err.sqlstate = "42P01"
            raise err
        if "FROM provider_policies" in sql:
            return [{"provider_name": "openai"}]
        if "FROM provider_lane_policy" in sql:
            return [{
                "provider_slug": "openai",
                "allowed_adapter_types": ["cli_llm", "llm_task"],
                "overridable": True,
            }]
        if "FROM provider_model_candidates" in sql:
            return [{
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "priority": 1,
                "route_tier": "high",
                "route_tier_rank": 1,
                "latency_class": "reasoning",
                "latency_rank": 1,
                "capability_tags": ["build"],
                "task_affinities": {"primary": ["build"], "secondary": [], "specialized": [], "avoid": []},
                "benchmark_profile": {},
                "adapter_type": "llm_task",
                "provider_policy_id": "provider_policy.openai",
                "cost_per_m_tokens": 4.0,
            }]
        if "FROM market_benchmark_metric_registry" in sql:
            return []
        if "FROM task_type_route_profiles" in sql:
            return []
        if "FROM task_type_route_eligibility" in sql:
            return []
        if "FROM registry_runtime_profile_authority" in sql:
            return []
        if sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
            return None
        return []


def test_explicit_route_surfaces_budget_authority_unreachable() -> None:
    router = TaskTypeRouter(_MissingBudgetTableConn())

    decision = router.resolve("openai/gpt-5.4")

    assert decision.provider_slug == "openai"
    assert decision.budget_authority_unreachable is True


def test_unreachable_authority_blocks_llm_task_admission_end_to_end() -> None:
    """Router decision + lane policy compose to fail closed on paid lane."""
    router = TaskTypeRouter(_MissingBudgetTableConn())

    decision = router.resolve("openai/gpt-5.4")

    policies = {
        "openai": ProviderLanePolicy(
            provider_slug="openai",
            allowed_adapter_types=frozenset({"cli_llm", "llm_task"}),
            overridable=True,
        ),
    }

    admitted, reason = admit_adapter_type(
        policies,
        decision.provider_slug,
        "llm_task",
        spend_pressure=decision.spend_pressure or None,
        budget_authority_unreachable=decision.budget_authority_unreachable,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_authority_unreachable"


def test_unreachable_authority_does_not_block_cli_lane() -> None:
    """CLI routes still run during schema drift — zero marginal cost."""
    router = TaskTypeRouter(_MissingBudgetTableConn())
    decision = router.resolve("openai/gpt-5.4")

    policies = {
        "openai": ProviderLanePolicy(
            provider_slug="openai",
            allowed_adapter_types=frozenset({"cli_llm", "llm_task"}),
            overridable=True,
        ),
    }
    admitted, reason = admit_adapter_type(
        policies,
        decision.provider_slug,
        "cli_llm",
        budget_authority_unreachable=decision.budget_authority_unreachable,
    )
    assert admitted is True
    assert reason == "lane.admitted"
