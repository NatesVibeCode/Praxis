from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_runtime_pkg = types.ModuleType("runtime")
_runtime_pkg.__path__ = [str(Path(__file__).resolve().parents[2] / "runtime")]
sys.modules.setdefault("runtime", _runtime_pkg)

_spec = importlib.util.spec_from_file_location(
    "runtime.task_type_router",
    Path(__file__).resolve().parents[2] / "runtime" / "task_type_router.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["runtime.task_type_router"] = _mod
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

from runtime.task_type_router import TaskTypeRouter


def _policy_row() -> dict[str, object]:
    return {
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
        "internal_failure_penalties": {
            "verification_failed": 0.25,
            "unknown": 0.10,
        },
        "review_severity_penalties": {"high": 0.15, "medium": 0.08, "low": 0.03},
    }


def _profile_row() -> dict[str, object]:
    return {
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
    }


def _failure_rows() -> list[dict[str, object]]:
    return [
        {"category": "credential_error", "zone": "config", "is_transient": False},
        {"category": "verification_failed", "zone": "internal", "is_transient": False},
        {"category": "rate_limit", "zone": "external", "is_transient": True},
    ]


def _state_row() -> dict[str, object]:
    return {
        "task_type": "build",
        "provider_slug": "openai",
        "model_slug": "gpt-5.4",
        "permitted": True,
        "rank": 1,
        "benchmark_score": 0.0,
        "benchmark_name": "",
        "cost_per_m_tokens": 8.75,
        "rationale": "route row",
        "route_tier": "high",
        "route_tier_rank": 1,
        "latency_class": "reasoning",
        "latency_rank": 1,
        "route_source": "explicit",
        "route_health_score": 0.80,
        "observed_completed_count": 0,
        "observed_execution_failure_count": 0,
        "observed_external_failure_count": 0,
        "observed_config_failure_count": 0,
        "observed_downstream_failure_count": 0,
        "observed_downstream_bug_count": 0,
        "consecutive_internal_failures": 0,
        "last_failure_category": "",
        "last_failure_zone": "",
        "recent_successes": 0,
        "recent_failures": 0,
        "last_outcome_at": None,
    }


class _FakeConn:
    def __init__(self, state_row: dict[str, object]) -> None:
        self.state_row = state_row

    def execute(self, sql: str, *params):
        if "FROM route_policy_registry" in sql:
            return [_policy_row()]
        if "FROM failure_category_zones" in sql:
            return _failure_rows()
        if "FROM task_type_route_profiles" in sql:
            return [_profile_row()]
        if "FROM market_benchmark_metric_registry" in sql:
            return []
        if "SELECT 1" in sql and "FROM task_type_routing" in sql:
            if self.state_row["task_type"] == params[0] and self.state_row["provider_slug"] == params[1] and self.state_row["model_slug"] == params[2]:
                return [{"?column?": 1}]
            return []
        if "SELECT rank, recent_failures FROM task_type_routing" in sql:
            return [{"rank": self.state_row["rank"], "recent_failures": self.state_row["recent_failures"]}]
        if "SELECT provider_slug, model_slug, rank FROM task_type_routing" in sql:
            return []
        if "observed_config_failure_count" in sql:
            self.state_row["observed_config_failure_count"] = int(self.state_row["observed_config_failure_count"]) + 1
            self.state_row["last_failure_category"] = params[3]
            self.state_row["last_failure_zone"] = params[4]
            return []
        if "observed_external_failure_count" in sql:
            self.state_row["observed_external_failure_count"] = int(self.state_row["observed_external_failure_count"]) + 1
            self.state_row["last_failure_category"] = params[3]
            self.state_row["last_failure_zone"] = params[4]
            return []
        if "observed_execution_failure_count" in sql and "route_health_score = GREATEST" in sql:
            self.state_row["recent_failures"] = int(self.state_row["recent_failures"]) + 1
            self.state_row["recent_successes"] = 0
            self.state_row["consecutive_internal_failures"] = int(self.state_row["consecutive_internal_failures"]) + 1
            self.state_row["observed_execution_failure_count"] = int(self.state_row["observed_execution_failure_count"]) + 1
            self.state_row["route_health_score"] = max(
                0.05,
                float(self.state_row["route_health_score"]) - float(params[3]),
            )
            self.state_row["last_failure_category"] = params[4]
            self.state_row["last_failure_zone"] = params[5]
            return []
        if "SET recent_successes = recent_successes + 1" in sql:
            self.state_row["recent_successes"] = int(self.state_row["recent_successes"]) + 1
            self.state_row["recent_failures"] = 0
            self.state_row["consecutive_internal_failures"] = 0
            self.state_row["observed_completed_count"] = int(self.state_row["observed_completed_count"]) + 1
            self.state_row["route_health_score"] = min(
                1.0,
                float(self.state_row["route_health_score"]) + float(params[4]),
            )
            return []
        return []


def _router(state_row: dict[str, object]) -> TaskTypeRouter:
    return TaskTypeRouter(_FakeConn(state_row))


def test_record_outcome_ignores_config_noise() -> None:
    state_row = _state_row()
    router = _router(state_row)

    router.record_outcome(
        "build",
        "openai",
        "gpt-5.4",
        succeeded=False,
        failure_code="credential_error",
    )

    assert state_row["route_health_score"] == pytest.approx(0.80)
    assert state_row["observed_config_failure_count"] == 1
    assert state_row["observed_execution_failure_count"] == 0
    assert state_row["recent_failures"] == 0


def test_record_outcome_penalizes_internal_failure_categories() -> None:
    state_row = _state_row()
    router = _router(state_row)

    router.record_outcome(
        "build",
        "openai",
        "gpt-5.4",
        succeeded=False,
        failure_code="verification_failed",
    )

    assert state_row["route_health_score"] == pytest.approx(0.55)
    assert state_row["observed_execution_failure_count"] == 1
    assert state_row["observed_config_failure_count"] == 0
    assert state_row["recent_failures"] == 1
    assert state_row["consecutive_internal_failures"] == 1
