from __future__ import annotations

import runtime.task_type_router as _ttr_mod
from runtime.task_type_router import TaskTypeRouter


def _passthrough_economics(
    *,
    provider_slug: str,
    adapter_type,
    provider_policy_id,
    raw_cost_per_m_tokens: float,
    budget_windows,
    default_adapter=None,
) -> dict:
    """Stand-in for _resolve_route_economics that surfaces raw cost as effective cost."""
    return {
        "adapter_type": adapter_type or "cli_llm",
        "billing_mode": "metered_api",
        "budget_bucket": "test",
        "effective_marginal_cost": raw_cost_per_m_tokens,
        "spend_pressure": "low",
        "budget_status": "",
        "prefer_prepaid": False,
        "allow_payg_fallback": True,
    }


_ttr_mod._resolve_route_economics = _passthrough_economics


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

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
                "benchmark_metric_weights": {},
                "route_tier_preferences": ["high", "medium", "low"],
                "latency_class_preferences": ["reasoning", "instant"],
                "allow_unclassified_candidates": True,
                "rationale": "build profile",
            }]
        if "FROM market_benchmark_metric_registry" in sql:
            return []
        if "FROM provider_model_candidates" in sql:
            candidates = []
            for index, row in enumerate(self._rows, start=1):
                provider = row["provider_slug"]
                candidates.append(
                    {
                        "provider_slug": provider,
                        "model_slug": row["model_slug"],
                        "priority": index,
                        "route_tier": "medium",
                        "route_tier_rank": 1,
                        "latency_class": "instant",
                        "latency_rank": 1,
                        "capability_tags": ["build", "coding", "analysis"],
                        "task_affinities": {"primary": ["build"], "secondary": ["analysis"], "specialized": [], "avoid": []},
                        "benchmark_profile": {},
                    }
                )
            return candidates
        if "FROM task_type_route_eligibility" in sql:
            return []
        if sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
            return None
        assert params
        assert params[0] == "build"
        return list(self._rows)


def _rows() -> list[dict]:
    return [
        {
            "model_slug": "front-runner",
            "provider_slug": "openai",
            "rank": 1,
            "benchmark_score": 100.0,
            "benchmark_name": "bench",
            "cost_per_m_tokens": 10.0,
            "rationale": "best benchmark, expensive",
            "route_health_score": 0.65,
            "consecutive_internal_failures": 0,
        },
        {
            "model_slug": "cheap-strong",
            "provider_slug": "google",
            "rank": 2,
            "benchmark_score": 40.0,
            "benchmark_name": "bench",
            "cost_per_m_tokens": 1.0,
            "rationale": "strong cost profile",
            "route_health_score": 0.72,
            "consecutive_internal_failures": 0,
        },
        {
            "model_slug": "fallback",
            "provider_slug": "anthropic",
            "rank": 3,
            "benchmark_score": 0.0,
            "benchmark_name": "bench",
            "cost_per_m_tokens": 5.0,
            "rationale": "fallback",
            "route_health_score": 0.60,
            "consecutive_internal_failures": 0,
        },
    ]


def test_resolve_failover_chain_reranks_with_composite_score() -> None:
    router = TaskTypeRouter(_FakeConn(_rows()))

    chain = router.resolve_failover_chain("auto/build")

    assert [decision.model_slug for decision in chain] == [
        "front-runner",
        "cheap-strong",
        "fallback",
    ]


def test_prefer_cost_shifts_weighting_toward_cheaper_model() -> None:
    router = TaskTypeRouter(_FakeConn(_rows()))

    decision = router.resolve("auto/build", prefer_cost=True)
    chain = router.resolve_failover_chain("auto/build", prefer_cost=True)

    assert decision.model_slug == "cheap-strong"
    assert [entry.model_slug for entry in chain] == [
        "cheap-strong",
        "front-runner",
        "fallback",
    ]


def test_route_health_outweighs_unreliable_benchmark_edge() -> None:
    rows = [
        {
            "model_slug": "flashy-metric",
            "provider_slug": "openai",
            "rank": 1,
            "benchmark_score": 99.0,
            "benchmark_name": "random-lab-score",
            "cost_per_m_tokens": 4.0,
            "rationale": "looks good on paper only",
            "route_health_score": 0.25,
            "consecutive_internal_failures": 2,
        },
        {
            "model_slug": "steady-operator",
            "provider_slug": "anthropic",
            "rank": 2,
            "benchmark_score": 87.0,
            "benchmark_name": "another-score-family",
            "cost_per_m_tokens": 4.5,
            "rationale": "actually finishes work",
            "route_health_score": 0.92,
            "consecutive_internal_failures": 0,
        },
    ]
    router = TaskTypeRouter(_FakeConn(rows))

    decision = router.resolve("auto/build")

    assert decision.provider_slug == "anthropic"
    assert decision.model_slug == "steady-operator"
