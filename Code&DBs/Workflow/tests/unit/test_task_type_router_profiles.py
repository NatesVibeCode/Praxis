from __future__ import annotations

import types

import pytest

from runtime.task_type_router import TaskTypeRouter
from runtime.task_type_router import TaskRouteAuthorityError


def _passthrough_economics(**kwargs):
    return {
        "adapter_type": kwargs.get("adapter_type") or "cli",
        "billing_mode": "owned_compute",
        "budget_bucket": "test",
        "effective_marginal_cost": float(kwargs.get("raw_cost_per_m_tokens") or 0.0),
        "spend_pressure": "low",
        "budget_status": "",
        "prefer_prepaid": True,
        "allow_payg_fallback": True,
    }


@pytest.fixture(autouse=True)
def _stub_router_provider_defaults(monkeypatch):
    monkeypatch.setitem(
        TaskTypeRouter.__init__.__globals__,
        "resolve_default_adapter_type",
        lambda provider_slug=None: "cli",
    )
    monkeypatch.setitem(
        TaskTypeRouter._build_profile_task_rows.__globals__,
        "_resolve_route_economics",
        _passthrough_economics,
    )
    monkeypatch.setitem(
        TaskTypeRouter._resolve_profile_chain.__globals__,
        "_resolve_route_economics",
        _passthrough_economics,
    )
    monkeypatch.setitem(
        TaskTypeRouter.resolve.__globals__,
        "_resolve_route_economics",
        _passthrough_economics,
    )


class _FakeConn:
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
                "internal_failure_penalties": {"verification_failed": 0.25, "unknown": 0.10},
                "review_severity_penalties": {"high": 0.15, "medium": 0.08, "low": 0.03},
            }]
        if "FROM failure_category_zones" in sql:
            return [{"category": "verification_failed", "zone": "internal"}]
        if "FROM task_type_route_profiles" in sql:
            return []
        if "FROM market_benchmark_metric_registry" in sql:
            return []
        if "FROM task_type_route_eligibility" in sql:
            return []
        if "FROM provider_lane_policy" in sql:
            return []
        if "FROM provider_model_candidates" in sql:
            return [
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "priority": 2,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "capability_tags": ["mid", "instant", "build"],
                    "task_affinities": {"primary": ["build"], "secondary": ["review"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
                {
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "priority": 3,
                    "route_tier": "medium",
                    "route_tier_rank": 2,
                    "latency_class": "instant",
                    "latency_rank": 3,
                    "capability_tags": ["mid", "instant", "review"],
                    "task_affinities": {"primary": ["review"], "secondary": ["build"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
                {
                    "provider_slug": "google",
                    "model_slug": "gemini-2.5-flash",
                    "priority": 4,
                    "route_tier": "medium",
                    "route_tier_rank": 3,
                    "latency_class": "instant",
                    "latency_rank": 2,
                    "capability_tags": ["mid", "instant", "chat"],
                    "task_affinities": {"primary": ["chat"], "secondary": ["build"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
            ]
        raise AssertionError(sql)


def test_resolve_medium_route_tier_chain() -> None:
    router = TaskTypeRouter(_FakeConn())

    chain = router.resolve_failover_chain("auto/medium")

    assert [entry.provider_slug for entry in chain] == ["openai", "anthropic", "google"]
    assert [entry.model_slug for entry in chain] == [
        "gpt-5.4-mini",
        "claude-sonnet-4-6",
        "gemini-2.5-flash",
    ]


def test_resolve_instant_latency_chain() -> None:
    router = TaskTypeRouter(_FakeConn())

    decision = router.resolve("auto/instant")
    chain = router.resolve_failover_chain("auto/instant")

    assert decision.model_slug == "gpt-5.4-mini"
    assert [entry.model_slug for entry in chain] == [
        "gpt-5.4-mini",
        "gemini-2.5-flash",
        "claude-sonnet-4-6",
    ]


class _CatalogProfileConn:
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
                "internal_failure_penalties": {"verification_failed": 0.25, "unknown": 0.10},
                "review_severity_penalties": {"high": 0.15, "medium": 0.08, "low": 0.03},
            }]
        if "FROM failure_category_zones" in sql:
            return [{"category": "verification_failed", "zone": "internal"}]
        if "FROM market_benchmark_metric_registry" in sql:
            return [
                {"metric_key": "artificial_analysis_coding_index", "higher_is_better": True, "enabled": True},
                {"metric_key": "artificial_analysis_intelligence_index", "higher_is_better": True, "enabled": True},
                {"metric_key": "median_output_tokens_per_second", "higher_is_better": True, "enabled": True},
            ]
        if "FROM task_type_route_profiles" in sql:
            return [{
                "task_type": "build",
                "affinity_labels": {
                    "primary": ["build", "coding"],
                    "secondary": ["review", "analysis", "wiring"],
                    "specialized": [],
                    "fallback": ["chat"],
                    "avoid": ["tts", "voice-agent", "audio", "image", "image-generation", "image-editing", "live-audio"],
                },
                "affinity_weights": {"primary": 1.0, "secondary": 0.7, "specialized": 0.4, "fallback": 0.2, "unclassified": 0.1, "avoid": 0.0},
                "task_rank_weights": {"affinity": 0.6, "route_tier": 0.25, "latency": 0.15},
                "benchmark_metric_weights": {
                    "artificial_analysis_coding_index": 0.7,
                    "artificial_analysis_intelligence_index": 0.2,
                    "median_output_tokens_per_second": 0.1,
                },
                "route_tier_preferences": ["high", "medium", "low"],
                "latency_class_preferences": ["reasoning", "instant"],
                "allow_unclassified_candidates": True,
                "rationale": "build profile",
            }]
        if "FROM task_type_route_eligibility" in sql:
            return []
        if "FROM provider_lane_policy" in sql:
            return []
        if "provider_budget_windows" in sql:
            return [{
                "provider_policy_id": "provider_policy.openai",
                "budget_status": "limited",
                "request_limit": 100,
                "requests_used": 92,
                "token_limit": 1000,
                "tokens_used": 940,
                "spend_limit_usd": 10.0,
                "spend_used_usd": 9.6,
            }]
        if "FROM provider_model_candidates" in sql:
            return [
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "priority": 1,
                    "route_tier": "high",
                    "route_tier_rank": 1,
                    "latency_class": "reasoning",
                    "latency_rank": 2,
                    "capability_tags": ["frontier", "reasoning", "build"],
                    "task_affinities": {"primary": ["build", "review"], "secondary": ["architecture"], "specialized": [], "avoid": []},
                    "benchmark_profile": {"market_benchmark": {"common_metrics": {
                        "artificial_analysis_coding_index": 92.0,
                        "artificial_analysis_intelligence_index": 95.0,
                        "median_output_tokens_per_second": 55.0,
                        "price_1m_blended_3_to_1": 8.0,
                    }}},
                },
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "priority": 2,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "capability_tags": ["mid", "instant", "build"],
                    "task_affinities": {"primary": ["build", "wiring"], "secondary": ["review"], "specialized": [], "avoid": []},
                    "benchmark_profile": {"market_benchmark": {"common_metrics": {
                        "artificial_analysis_coding_index": 81.0,
                        "artificial_analysis_intelligence_index": 79.0,
                        "median_output_tokens_per_second": 140.0,
                        "price_1m_blended_3_to_1": 2.5,
                    }}},
                },
                {
                    "provider_slug": "google",
                    "model_slug": "gemini-3.1-flash-image-preview",
                    "priority": 3,
                    "route_tier": "medium",
                    "route_tier_rank": 3,
                    "latency_class": "instant",
                    "latency_rank": 2,
                    "capability_tags": ["mid", "instant", "image", "chat"],
                    "task_affinities": {"primary": ["image-generation"], "secondary": ["chat"], "specialized": ["image"], "avoid": ["general-routing"]},
                    "benchmark_profile": {"market_benchmark": {"common_metrics": {
                        "artificial_analysis_coding_index": 88.0,
                        "artificial_analysis_intelligence_index": 70.0,
                        "median_output_tokens_per_second": 160.0,
                        "price_1m_blended_3_to_1": 1.5,
                    }}},
                },
            ]
        if "FROM task_type_routing" in sql:
            return []
        if "INSERT INTO task_type_routing" in sql:
            return []
        raise AssertionError(sql)


def test_resolve_build_uses_catalog_profile_and_excludes_general_routing_specialists() -> None:
    router = TaskTypeRouter(_CatalogProfileConn())

    chain = router.resolve_failover_chain("auto/build")

    assert [entry.model_slug for entry in chain] == [
        "gpt-5.4",
        "gpt-5.4-mini",
    ]


def test_resolve_spec_jobs_honors_explicit_prefer_cost_over_complexity() -> None:
    router = TaskTypeRouter(_FakeConn())
    calls: list[bool] = []
    chain = [
        types.SimpleNamespace(task_type="build", provider_slug="openai", model_slug="gpt-5.4-mini", rank=1, rationale="cheap"),
        types.SimpleNamespace(task_type="build", provider_slug="anthropic", model_slug="claude-sonnet-4-6", rank=2, rationale="backup"),
    ]

    def _fake_resolve_failover_chain(slug: str, prefer_cost: bool = False, runtime_profile_ref: str | None = None):
        calls.append(prefer_cost)
        return list(chain)

    router.resolve_failover_chain = _fake_resolve_failover_chain  # type: ignore[method-assign]

    jobs = [{"agent": "auto/build", "complexity": "low", "prefer_cost": False}]
    router.resolve_spec_jobs(jobs)

    assert calls == [False]
    assert jobs[0]["agent"] == "openai/gpt-5.4-mini"
    assert list(jobs[0]["_route_plan"].chain) == [
        "openai/gpt-5.4-mini",
        "anthropic/claude-sonnet-4-6",
    ]


def test_resolve_spec_jobs_uses_explicit_prefer_cost_when_enabled() -> None:
    router = TaskTypeRouter(_FakeConn())
    calls: list[bool] = []

    def _fake_resolve_failover_chain(slug: str, prefer_cost: bool = False, runtime_profile_ref: str | None = None):
        calls.append(prefer_cost)
        return [types.SimpleNamespace(task_type="build", provider_slug="openai", model_slug="gpt-5.4-mini", rank=1, rationale="cheap")]

    def _fake_resolve_auto_chain(task_type: str, prefer_cost: bool = False, tier_override: str | None = None, runtime_profile_ref: str | None = None):
        calls.append(prefer_cost)
        return [types.SimpleNamespace(task_type="build", provider_slug="openai", model_slug="gpt-5.4-mini", rank=1, rationale="cheap")]

    router.resolve_failover_chain = _fake_resolve_failover_chain  # type: ignore[method-assign]
    router._resolve_auto_chain = _fake_resolve_auto_chain  # type: ignore[method-assign]

    jobs = [{"agent": "auto/build", "complexity": "high", "prefer_cost": True}]
    router.resolve_spec_jobs(jobs)

    assert calls == [True, True]


class _ScopedProfileConn(_CatalogProfileConn):
    def execute(self, sql: str, *params):
        if "FROM registry_runtime_profile_authority" in sql:
            return [{
                "model_profile_id": "model_profile.build",
                "provider_policy_id": "provider_policy.openai",
            }]
        if "FROM provider_policies" in sql:
            return [{"provider_name": "openai"}]
        if "FROM model_profile_candidate_bindings" in sql and "JOIN provider_model_candidates candidate" in sql:
            return [
                {
                    "position_index": 1,
                    "candidate_ref": "candidate.gpt-5.4",
                    "provider_ref": "provider.openai",
                    "provider_name": "openai",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "priority": 1,
                    "balance_weight": 1,
                    "route_tier": "high",
                    "route_tier_rank": 1,
                    "latency_class": "reasoning",
                    "latency_rank": 2,
                    "reasoning_control": {"effort": "high"},
                    "capability_tags": ["frontier", "reasoning", "build"],
                    "task_affinities": {"primary": ["build", "review"], "secondary": ["architecture"], "specialized": [], "avoid": []},
                    "benchmark_profile": {"market_benchmark": {"common_metrics": {
                        "artificial_analysis_coding_index": 92.0,
                        "artificial_analysis_intelligence_index": 95.0,
                        "median_output_tokens_per_second": 55.0,
                        "price_1m_blended_3_to_1": 8.0,
                    }}},
                },
                {
                    "position_index": 2,
                    "candidate_ref": "candidate.gpt-5.4-mini",
                    "provider_ref": "provider.openai",
                    "provider_name": "openai",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "priority": 2,
                    "balance_weight": 1,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "reasoning_control": {"effort": "medium"},
                    "capability_tags": ["mid", "instant", "build"],
                    "task_affinities": {"primary": ["build", "wiring"], "secondary": ["review"], "specialized": [], "avoid": []},
                    "benchmark_profile": {"market_benchmark": {"common_metrics": {
                        "artificial_analysis_coding_index": 81.0,
                        "artificial_analysis_intelligence_index": 79.0,
                        "median_output_tokens_per_second": 140.0,
                        "price_1m_blended_3_to_1": 2.5,
                    }}},
                },
            ]
        if "FROM provider_budget_windows" in sql:
            return [{
                "provider_policy_id": "provider_policy.openai",
                "budget_status": "limited",
                "request_limit": 100,
                "requests_used": 92,
                "token_limit": 1000,
                "tokens_used": 940,
                "spend_limit_usd": 10.0,
                "spend_used_usd": 9.6,
            }]
        if "FROM route_eligibility_states" in sql:
            return [
                {"candidate_ref": "candidate.gpt-5.4", "eligibility_status": "eligible", "reason_code": "eligible"},
                {"candidate_ref": "candidate.gpt-5.4-mini", "eligibility_status": "eligible", "reason_code": "eligible"},
            ]
        return super().execute(sql, *params)


def test_runtime_profile_scopes_auto_chain_to_admitted_candidates() -> None:
    router = TaskTypeRouter(_ScopedProfileConn())

    chain = router.resolve_failover_chain(
        "auto/build",
        runtime_profile_ref="runtime_profile.build",
    )

    assert [entry.provider_slug for entry in chain] == ["openai", "openai"]
    assert [entry.model_slug for entry in chain] == ["gpt-5.4", "gpt-5.4-mini"]
    assert [entry.prefer_prepaid for entry in chain] == [True, True]
    assert [entry.allow_payg_fallback for entry in chain] == [True, True]


def test_runtime_profile_does_not_override_explicit_slug() -> None:
    router = TaskTypeRouter(_ScopedProfileConn())

    decision = router.resolve(
        "anthropic/claude-sonnet-4-6",
        runtime_profile_ref="runtime_profile.build",
    )

    assert decision.provider_slug == "anthropic"
    assert decision.model_slug == "claude-sonnet-4-6"
    assert decision.was_auto is False


def test_explicit_slug_uses_runtime_profile_budget_authority(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _budget_aware_economics(**kwargs):
        calls.append(dict(kwargs))
        provider_policy_id = str(kwargs.get("provider_policy_id") or "")
        budget_window = kwargs.get("budget_windows", {}).get(provider_policy_id)
        return {
            "adapter_type": kwargs.get("adapter_type") or "cli",
            "billing_mode": "owned_compute",
            "budget_bucket": "test",
            "effective_marginal_cost": float(kwargs.get("raw_cost_per_m_tokens") or 0.0),
            "spend_pressure": "high" if budget_window else "unknown",
            "budget_status": str((budget_window or {}).get("budget_status") or ""),
            "prefer_prepaid": True,
            "allow_payg_fallback": not bool(budget_window),
        }

    monkeypatch.setitem(TaskTypeRouter.resolve.__globals__, "_resolve_route_economics", _budget_aware_economics)

    router = TaskTypeRouter(_ScopedProfileConn())

    decision = router.resolve(
        "openai/gpt-5.4-mini",
        runtime_profile_ref="runtime_profile.build",
    )

    assert calls
    assert calls[0]["provider_policy_id"] == "provider_policy.openai"
    assert "provider_policy.openai" in calls[0]["budget_windows"]
    assert decision.spend_pressure == "high"
    assert decision.budget_status == "limited"
    assert decision.allow_payg_fallback is False


def test_explicit_route_decision_preserves_economics_policy_bits(monkeypatch) -> None:
    monkeypatch.setitem(
        TaskTypeRouter.resolve.__globals__,
        "_resolve_route_economics",
        lambda **kwargs: {
            "adapter_type": "llm_task",
            "billing_mode": "metered_api",
            "budget_bucket": "api",
            "effective_marginal_cost": 1.0,
            "spend_pressure": "high",
            "budget_status": "limited",
            "prefer_prepaid": False,
            "allow_payg_fallback": False,
        },
    )
    router = TaskTypeRouter(_ScopedProfileConn())

    decision = router.resolve("anthropic/claude-sonnet-4-6")

    assert decision.prefer_prepaid is False
    assert decision.allow_payg_fallback is False


def test_explicit_route_decision_defaults_allow_payg_fallback_false_when_sparse(monkeypatch) -> None:
    monkeypatch.setitem(
        TaskTypeRouter.resolve.__globals__,
        "_resolve_route_economics",
        lambda **kwargs: {
            "adapter_type": "llm_task",
            "billing_mode": "metered_api",
            "budget_bucket": "api",
            "effective_marginal_cost": 1.0,
            "spend_pressure": "unknown",
            "budget_status": "",
            "prefer_prepaid": False,
        },
    )
    router = TaskTypeRouter(_ScopedProfileConn())

    decision = router.resolve("openai/gpt-5.4-mini")

    assert decision.allow_payg_fallback is False


def test_explicit_route_decision_defaults_missing_payg_fallback_to_false(monkeypatch) -> None:
    monkeypatch.setitem(
        TaskTypeRouter.resolve.__globals__,
        "_resolve_route_economics",
        lambda **kwargs: {
            "adapter_type": "llm_task",
            "billing_mode": "metered_api",
            "budget_bucket": "api",
            "effective_marginal_cost": 1.0,
            "spend_pressure": "low",
            "budget_status": "",
            "prefer_prepaid": False,
        },
    )
    router = TaskTypeRouter(_ScopedProfileConn())

    decision = router.resolve("anthropic/claude-sonnet-4-6")

    assert decision.allow_payg_fallback is False


class _ResearchProfileConn(_CatalogProfileConn):
    def execute(self, sql: str, *params):
        if "FROM task_type_route_profiles" in sql:
            return [{
                "task_type": "research",
                "affinity_labels": {
                    "primary": ["research", "analysis", "documentation"],
                    "secondary": ["architecture", "review", "chat"],
                    "specialized": [],
                    "fallback": ["multimodal"],
                    "avoid": ["tts", "voice-agent", "audio", "image", "image-generation", "image-editing", "live-audio"],
                },
                "affinity_weights": {"primary": 1.0, "secondary": 0.74, "specialized": 0.45, "fallback": 0.30, "unclassified": 0.20, "avoid": 0.0},
                "task_rank_weights": {"affinity": 0.62, "route_tier": 0.23, "latency": 0.15},
                "benchmark_metric_weights": {
                    "artificial_analysis_intelligence_index": 0.65,
                    "artificial_analysis_coding_index": 0.10,
                    "median_output_tokens_per_second": 0.10,
                },
                "route_tier_preferences": ["high", "medium", "low"],
                "latency_class_preferences": ["reasoning", "instant"],
                "allow_unclassified_candidates": True,
                "rationale": "research profile",
            }]
        if "FROM provider_model_candidates" in sql:
            return [
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "priority": 1,
                    "route_tier": "high",
                    "route_tier_rank": 1,
                    "latency_class": "reasoning",
                    "latency_rank": 1,
                    "capability_tags": ["research", "analysis", "documentation"],
                    "task_affinities": {"primary": ["research", "analysis"], "secondary": ["architecture"], "specialized": [], "avoid": []},
                    "benchmark_profile": {"market_benchmark": {"common_metrics": {
                        "artificial_analysis_intelligence_index": 95.0,
                        "artificial_analysis_coding_index": 70.0,
                        "median_output_tokens_per_second": 50.0,
                        "price_1m_blended_3_to_1": 8.0,
                    }}},
                },
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "priority": 2,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "capability_tags": ["chat", "analysis"],
                    "task_affinities": {"primary": ["chat"], "secondary": ["research"], "specialized": [], "avoid": []},
                    "benchmark_profile": {"market_benchmark": {"common_metrics": {
                        "artificial_analysis_intelligence_index": 78.0,
                        "artificial_analysis_coding_index": 72.0,
                        "median_output_tokens_per_second": 140.0,
                        "price_1m_blended_3_to_1": 2.5,
                    }}},
                },
            ]
        return super().execute(sql, *params)


def test_resolve_research_uses_profile_backed_catalog_chain() -> None:
    router = TaskTypeRouter(_ResearchProfileConn())

    decision = router.resolve("auto/research")
    chain = router.resolve_failover_chain("auto/research")

    assert decision.model_slug == "gpt-5.4"
    assert [entry.model_slug for entry in chain] == [
        "gpt-5.4",
        "gpt-5.4-mini",
    ]


class _ResearchOnlyGuardConn(_FakeConn):
    def execute(self, sql: str, *params):
        if "INSERT INTO task_type_routing" in sql:
            return []
        if "FROM task_type_routing" in sql:
            return []
        if "FROM task_type_route_profiles" in sql:
            return [
                {
                    "task_type": "chat",
                    "affinity_labels": {
                        "primary": ["chat", "support"],
                        "secondary": ["analysis", "research"],
                        "specialized": [],
                        "fallback": ["build"],
                        "avoid": [],
                    },
                    "affinity_weights": {"primary": 1.0, "secondary": 0.7, "specialized": 0.4, "fallback": 0.2, "unclassified": 0.1, "avoid": 0.0},
                    "task_rank_weights": {"affinity": 0.6, "route_tier": 0.25, "latency": 0.15},
                    "benchmark_metric_weights": {},
                    "route_tier_preferences": ["high", "medium", "low"],
                    "latency_class_preferences": ["instant", "reasoning"],
                    "allow_unclassified_candidates": True,
                    "rationale": "chat profile",
                },
                {
                    "task_type": "research",
                    "affinity_labels": {
                        "primary": ["research", "analysis", "documentation"],
                        "secondary": ["architecture", "review", "chat"],
                        "specialized": [],
                        "fallback": ["multimodal"],
                        "avoid": [],
                    },
                    "affinity_weights": {"primary": 1.0, "secondary": 0.74, "specialized": 0.45, "fallback": 0.3, "unclassified": 0.2, "avoid": 0.0},
                    "task_rank_weights": {"affinity": 0.62, "route_tier": 0.23, "latency": 0.15},
                    "benchmark_metric_weights": {},
                    "route_tier_preferences": ["high", "medium", "low"],
                    "latency_class_preferences": ["reasoning", "instant"],
                    "allow_unclassified_candidates": True,
                    "rationale": "research profile",
                },
            ]
        if "FROM provider_model_candidates" in sql:
            return [
                {
                    "provider_slug": "deepseek",
                    "model_slug": "deepseek-r3",
                    "priority": 1,
                    "route_tier": "low",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "capability_tags": ["research", "reasoning"],
                    "task_affinities": {"primary": ["research"], "secondary": [], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "priority": 2,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 2,
                    "capability_tags": ["chat", "analysis"],
                    "task_affinities": {"primary": ["chat"], "secondary": ["research"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
            ]
        return super().execute(sql, *params)


def test_research_only_candidate_does_not_leak_into_non_research_routes(monkeypatch) -> None:
    import runtime.routing_economics as _routing_economics
    monkeypatch.setattr(_routing_economics, "supports_adapter", lambda _provider_slug, _adapter_type: True)
    monkeypatch.setattr(
        _routing_economics,
        "resolve_adapter_economics",
        lambda _provider_slug, adapter_type: {
            "billing_mode": "subscription_included" if adapter_type == "cli_llm" else "metered_api",
            "budget_bucket": f"{adapter_type}.test",
            "effective_marginal_cost": 0.0,
            "prefer_prepaid": True,
            "allow_payg_fallback": True,
        },
    )
    router = TaskTypeRouter(_ResearchOnlyGuardConn())

    chat_chain = router.resolve_failover_chain("auto/chat")
    research_chain = router.resolve_failover_chain("auto/research")

    assert [entry.provider_slug for entry in chat_chain] == ["openai"]
    assert [entry.model_slug for entry in chat_chain] == ["gpt-5.4-mini"]
    assert [entry.provider_slug for entry in research_chain] == ["openai", "deepseek"]
    assert [entry.model_slug for entry in research_chain] == ["gpt-5.4-mini", "deepseek-r3"]


class _SemanticAutoAliasConn(_FakeConn):
    def execute(self, sql: str, *params):
        if "INSERT INTO task_type_routing" in sql:
            return []
        if "FROM task_type_routing" in sql:
            return []
        if "FROM task_type_route_profiles" in sql:
            return [
                {
                    "task_type": "chat",
                    "affinity_labels": {
                        "primary": ["chat", "creative"],
                        "secondary": ["analysis", "support"],
                        "specialized": [],
                        "fallback": ["research"],
                        "avoid": [],
                    },
                    "affinity_weights": {"primary": 1.0, "secondary": 0.7, "specialized": 0.4, "fallback": 0.2, "unclassified": 0.1, "avoid": 0.0},
                    "task_rank_weights": {"affinity": 0.55, "route_tier": 0.20, "latency": 0.25},
                    "benchmark_metric_weights": {},
                    "route_tier_preferences": ["medium", "high", "low"],
                    "latency_class_preferences": ["instant", "reasoning"],
                    "allow_unclassified_candidates": True,
                    "rationale": "chat profile",
                },
                {
                    "task_type": "analysis",
                    "affinity_labels": {
                        "primary": ["analysis", "triage", "categorize", "score"],
                        "secondary": ["chat", "support", "review"],
                        "specialized": [],
                        "fallback": ["research"],
                        "avoid": [],
                    },
                    "affinity_weights": {"primary": 1.0, "secondary": 0.72, "specialized": 0.4, "fallback": 0.2, "unclassified": 0.1, "avoid": 0.0},
                    "task_rank_weights": {"affinity": 0.55, "route_tier": 0.20, "latency": 0.25},
                    "benchmark_metric_weights": {},
                    "route_tier_preferences": ["high", "medium", "low"],
                    "latency_class_preferences": ["reasoning", "instant"],
                    "allow_unclassified_candidates": True,
                    "rationale": "analysis profile",
                },
                {
                    "task_type": "support",
                    "affinity_labels": {
                        "primary": ["support", "analysis", "triage"],
                        "secondary": ["chat", "review"],
                        "specialized": [],
                        "fallback": ["research"],
                        "avoid": [],
                    },
                    "affinity_weights": {"primary": 1.0, "secondary": 0.72, "specialized": 0.35, "fallback": 0.25, "unclassified": 0.2, "avoid": 0.0},
                    "task_rank_weights": {"affinity": 0.45, "route_tier": 0.15, "latency": 0.40},
                    "benchmark_metric_weights": {},
                    "route_tier_preferences": ["medium", "low", "high"],
                    "latency_class_preferences": ["instant", "reasoning"],
                    "allow_unclassified_candidates": True,
                    "rationale": "support profile",
                },
            ]
        if "FROM provider_model_candidates" in sql:
            return [
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "priority": 0,
                    "route_tier": "high",
                    "route_tier_rank": 1,
                    "latency_class": "reasoning",
                    "latency_rank": 1,
                    "capability_tags": ["analysis", "reasoning", "build"],
                    "task_affinities": {"primary": ["analysis"], "secondary": ["chat", "support"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4-mini",
                    "priority": 1,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "capability_tags": ["chat", "creative", "support"],
                    "task_affinities": {"primary": ["chat", "support"], "secondary": ["analysis"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
                {
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "priority": 2,
                    "route_tier": "medium",
                    "route_tier_rank": 2,
                    "latency_class": "reasoning",
                    "latency_rank": 2,
                    "capability_tags": ["support", "analysis"],
                    "task_affinities": {"primary": ["support"], "secondary": ["chat", "analysis"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
            ]
        return super().execute(sql, *params)


def test_semantic_auto_routes_resolve_through_backing_profiles(monkeypatch) -> None:
    import runtime.routing_economics as _routing_economics

    monkeypatch.setattr(_routing_economics, "supports_adapter", lambda _provider_slug, _adapter_type: True)
    monkeypatch.setattr(
        _routing_economics,
        "resolve_adapter_economics",
        lambda _provider_slug, adapter_type: {
            "billing_mode": "subscription_included" if adapter_type == "cli_llm" else "metered_api",
            "budget_bucket": f"{adapter_type}.test",
            "effective_marginal_cost": 0.0,
            "prefer_prepaid": True,
            "allow_payg_fallback": True,
        },
    )

    router = TaskTypeRouter(_SemanticAutoAliasConn())

    draft_chain = router.resolve_failover_chain("auto/draft")
    chat_chain = router.resolve_failover_chain("auto/chat")
    classify_chain = router.resolve_failover_chain("auto/classify")
    analysis_chain = router.resolve_failover_chain("auto/analysis")
    support_chain = router.resolve_failover_chain("auto/support")

    assert [entry.model_slug for entry in draft_chain] == [entry.model_slug for entry in chat_chain]
    assert [entry.model_slug for entry in draft_chain[:2]] == ["gpt-5.4-mini", "claude-sonnet-4-6"]
    assert [entry.model_slug for entry in classify_chain] == [entry.model_slug for entry in analysis_chain]
    assert classify_chain[0].model_slug == "gpt-5.4"
    assert support_chain[0].model_slug == "gpt-5.4-mini"


def test_unprofiled_task_type_fails_closed() -> None:
    router = TaskTypeRouter(_CatalogProfileConn())

    with pytest.raises(TaskRouteAuthorityError, match="missing task_type 'support'"):
        router.resolve("auto/support")
