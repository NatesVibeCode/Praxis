from __future__ import annotations

from datetime import datetime, timedelta, timezone

from runtime.task_type_router import TaskTypeRouter


def _as_of() -> datetime:
    return datetime(2026, 4, 7, 16, 0, tzinfo=timezone.utc)


class _FakeConn:
    def __init__(
        self,
        route_rows: list[dict],
        eligibility_rows: list[dict] | None = None,
    ) -> None:
        self._route_rows = route_rows
        self._eligibility_rows = eligibility_rows or []

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
            return [
                {
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "priority": 1,
                    "route_tier": "high",
                    "route_tier_rank": 1,
                    "latency_class": "reasoning",
                    "latency_rank": 1,
                    "capability_tags": ["build", "coding"],
                    "task_affinities": {"primary": ["build"], "secondary": ["review"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
                {
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "priority": 2,
                    "route_tier": "medium",
                    "route_tier_rank": 1,
                    "latency_class": "instant",
                    "latency_rank": 1,
                    "capability_tags": ["build", "analysis"],
                    "task_affinities": {"primary": ["build"], "secondary": ["analysis"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
                {
                    "provider_slug": "google",
                    "model_slug": "gemini-3.1-pro-preview",
                    "priority": 3,
                    "route_tier": "medium",
                    "route_tier_rank": 2,
                    "latency_class": "instant",
                    "latency_rank": 2,
                    "capability_tags": ["build", "chat"],
                    "task_affinities": {"primary": ["chat"], "secondary": ["build"], "specialized": [], "avoid": []},
                    "benchmark_profile": {},
                },
            ]
        if "FROM task_type_route_eligibility" in sql:
            return list(self._eligibility_rows)
        if "FROM task_type_routing" in sql:
            assert params
            assert params[0] == "build"
            return list(self._route_rows)
        if sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
            return None
        raise AssertionError(sql)


def _route_rows() -> list[dict]:
    return [
        {
            "model_slug": "gpt-5.4",
            "provider_slug": "openai",
            "rank": 1,
            "benchmark_score": 100.0,
            "benchmark_name": "bench",
            "cost_per_m_tokens": 10.0,
            "rationale": "best benchmark, expensive",
        },
        {
            "model_slug": "claude-sonnet-4-6",
            "provider_slug": "anthropic",
            "rank": 2,
            "benchmark_score": 75.0,
            "benchmark_name": "bench",
            "cost_per_m_tokens": 6.0,
            "rationale": "strong fallback",
        },
        {
            "model_slug": "gemini-3.1-pro-preview",
            "provider_slug": "google",
            "rank": 3,
            "benchmark_score": 65.0,
            "benchmark_name": "bench",
            "cost_per_m_tokens": 2.0,
            "rationale": "cheap fallback",
        },
    ]


def test_provider_wide_reject_filters_entire_provider_from_chain() -> None:
    as_of = _as_of()
    router = TaskTypeRouter(
        _FakeConn(
            _route_rows(),
            eligibility_rows=[
                {
                    "task_route_eligibility_id": "eligibility.anthropic.off",
                    "task_type": None,
                    "provider_slug": "anthropic",
                    "model_slug": None,
                    "eligibility_status": "rejected",
                    "reason_code": "provider_disabled",
                    "rationale": "Anthropic off until Friday morning",
                    "effective_from": as_of - timedelta(hours=1),
                    "effective_to": as_of + timedelta(days=2),
                    "decision_ref": "decision:anthropic-off",
                }
            ],
        ),
        now_factory=lambda: as_of,
    )

    chain = router.resolve_failover_chain("auto/build")

    assert [decision.provider_slug for decision in chain] == ["openai", "google"]
    assert [decision.model_slug for decision in chain] == [
        "gpt-5.4",
        "gemini-3.1-pro-preview",
    ]


def test_model_specific_override_beats_global_provider_shutdown() -> None:
    as_of = _as_of()
    router = TaskTypeRouter(
        _FakeConn(
            _route_rows(),
            eligibility_rows=[
                {
                    "task_route_eligibility_id": "eligibility.anthropic.off",
                    "task_type": None,
                    "provider_slug": "anthropic",
                    "model_slug": None,
                    "eligibility_status": "rejected",
                    "reason_code": "provider_disabled",
                    "rationale": "Anthropic off",
                    "effective_from": as_of - timedelta(hours=1),
                    "effective_to": as_of + timedelta(days=2),
                    "decision_ref": "decision:anthropic-off",
                },
                {
                    "task_route_eligibility_id": "eligibility.build.anthropic.sonnet.on",
                    "task_type": "build",
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "eligibility_status": "eligible",
                    "reason_code": "task_type_exception",
                    "rationale": "Allow sonnet for build only",
                    "effective_from": as_of,
                    "effective_to": as_of + timedelta(hours=4),
                    "decision_ref": "decision:anthropic-build-exception",
                },
            ],
        ),
        now_factory=lambda: as_of,
    )

    chain = router.resolve_failover_chain("auto/build")

    assert [decision.provider_slug for decision in chain] == ["openai", "anthropic", "google"]
    assert [decision.model_slug for decision in chain] == [
        "gpt-5.4",
        "claude-sonnet-4-6",
        "gemini-3.1-pro-preview",
    ]
