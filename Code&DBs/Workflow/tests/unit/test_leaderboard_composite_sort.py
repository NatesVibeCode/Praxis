from __future__ import annotations

from runtime.leaderboard import AgentScore, _sort_key


def test_sort_key_uses_composite_score_over_naive_pass_rate() -> None:
    pass_rate_heavy = AgentScore(
        provider_slug="alpha",
        model_slug="pass-rate-heavy",
        total_workflows=50,
        succeeded=48,
        failed=2,
        pass_rate=0.96,
        total_cost_usd=96.0,
        avg_latency_ms=500,
        p95_latency_ms=600,
        avg_cost_per_workflow=1.92,
        cost_per_success=2.0,
    )
    balanced = AgentScore(
        provider_slug="beta",
        model_slug="balanced",
        total_workflows=200,
        succeeded=186,
        failed=14,
        pass_rate=0.93,
        total_cost_usd=20.46,
        avg_latency_ms=50,
        p95_latency_ms=80,
        avg_cost_per_workflow=0.1023,
        cost_per_success=0.11,
    )

    scores = [pass_rate_heavy, balanced]
    max_latency_ms = max(s.avg_latency_ms for s in scores)
    max_dispatches = max(s.total_workflows for s in scores)

    ordered = sorted(
        scores,
        key=lambda s: _sort_key(
            s,
            max_latency_ms=max_latency_ms,
            max_dispatches=max_dispatches,
        ),
    )

    assert [s.model_slug for s in ordered] == ["balanced", "pass-rate-heavy"]


def test_sort_key_handles_missing_cost_per_success() -> None:
    no_successes = AgentScore(
        provider_slug="alpha",
        model_slug="no-successes",
        total_workflows=10,
        succeeded=0,
        failed=10,
        pass_rate=0.0,
        total_cost_usd=5.0,
        avg_latency_ms=100,
        p95_latency_ms=120,
        avg_cost_per_workflow=0.5,
        cost_per_success=None,
    )
    has_successes = AgentScore(
        provider_slug="beta",
        model_slug="has-successes",
        total_workflows=10,
        succeeded=5,
        failed=5,
        pass_rate=0.5,
        total_cost_usd=5.0,
        avg_latency_ms=100,
        p95_latency_ms=120,
        avg_cost_per_workflow=0.5,
        cost_per_success=1.0,
    )

    scores = [no_successes, has_successes]
    max_latency_ms = max(s.avg_latency_ms for s in scores)
    max_dispatches = max(s.total_workflows for s in scores)

    ordered = sorted(
        scores,
        key=lambda s: _sort_key(
            s,
            max_latency_ms=max_latency_ms,
            max_dispatches=max_dispatches,
        ),
    )

    assert [s.model_slug for s in ordered] == ["has-successes", "no-successes"]
