from __future__ import annotations

from runtime.task_type_router import TaskTypeRouter


class _CountingConn:
    def __init__(self) -> None:
        self.counts = {
            "route_policy_registry": 0,
            "failure_category_zones": 0,
            "task_type_route_profiles": 0,
            "market_benchmark_metric_registry": 0,
        }

    def execute(self, sql: str, *params):
        if "FROM route_policy_registry" in sql:
            self.counts["route_policy_registry"] += 1
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
            self.counts["failure_category_zones"] += 1
            return [{"category": "verification_failed", "zone": "internal"}]
        if "FROM task_type_route_profiles" in sql:
            self.counts["task_type_route_profiles"] += 1
            return [{
                "task_type": "build",
                "affinity_labels": {
                    "primary": ["build"],
                    "secondary": ["review"],
                    "specialized": [],
                    "fallback": ["chat"],
                    "avoid": [],
                },
                "affinity_weights": {
                    "primary": 1.0,
                    "secondary": 0.7,
                    "specialized": 0.4,
                    "fallback": 0.2,
                    "unclassified": 0.1,
                    "avoid": 0.0,
                },
                "task_rank_weights": {"affinity": 0.6, "route_tier": 0.25, "latency": 0.15},
                "benchmark_metric_weights": {},
                "route_tier_preferences": ["high", "medium", "low"],
                "latency_class_preferences": ["reasoning", "instant"],
                "allow_unclassified_candidates": True,
                "rationale": "build profile",
            }]
        if "FROM market_benchmark_metric_registry" in sql:
            self.counts["market_benchmark_metric_registry"] += 1
            return []
        if "FROM provider_lane_policy" in sql:
            return []
        raise AssertionError(sql)


def test_router_reuses_static_authority_snapshot_across_construction(monkeypatch) -> None:
    monkeypatch.setitem(TaskTypeRouter.__init__.__globals__, "default_llm_adapter_type", lambda: "cli")
    TaskTypeRouter.invalidate_all_authority_snapshots()
    conn = _CountingConn()

    first = TaskTypeRouter(conn)
    second = TaskTypeRouter(conn)

    assert first.route_policy == second.route_policy
    assert conn.counts == {
        "route_policy_registry": 1,
        "failure_category_zones": 1,
        "task_type_route_profiles": 1,
        "market_benchmark_metric_registry": 1,
    }


def test_router_authority_snapshot_invalidation_forces_reload(monkeypatch) -> None:
    monkeypatch.setitem(TaskTypeRouter.__init__.__globals__, "default_llm_adapter_type", lambda: "cli")
    TaskTypeRouter.invalidate_all_authority_snapshots()
    conn = _CountingConn()

    TaskTypeRouter(conn)
    TaskTypeRouter.invalidate_authority_snapshot(conn)
    TaskTypeRouter(conn)

    assert conn.counts == {
        "route_policy_registry": 2,
        "failure_category_zones": 2,
        "task_type_route_profiles": 2,
        "market_benchmark_metric_registry": 2,
    }
