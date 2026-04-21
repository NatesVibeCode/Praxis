"""Consolidated operator dashboard for Praxis platform."""

from __future__ import annotations

import json
import logging
from typing import Any

from .workflow_status import get_workflow_history
from .cost_tracker import get_cost_tracker
from .circuit_breaker import get_circuit_breakers, CircuitState
from .leaderboard import build_leaderboard, AgentScore
from .workflow import get_route_outcomes
from .observability import get_workflow_metrics_view


logger = logging.getLogger(__name__)


def _format_failure_mini_digest(rows: list[dict[str, Any]], *, limit: int = 3) -> str:
    if not rows:
        return "none"
    parts: list[str] = []
    for row in rows[:limit]:
        category = row.get("failure_category", "unknown")
        zone = row.get("failure_zone", "unknown")
        count = row.get("count", 0)
        pct = row.get("pct", 0)
        parts.append(f"{category}/{zone} {count} ({pct}%)")
    return "; ".join(parts)


def build_dashboard() -> dict[str, Any]:
    """Assemble all dashboard data into a single dict.

    Returns:
        dict with keys:
            - workflow_summary: total, succeeded, failed, pass_rate, workflow_history_source, workflow_history_status
            - cost_summary: total_cost_usd, by_agent
            - circuit_states: per-provider state
            - route_health: consecutive failures per provider
            - route_health_error: explicit route-health authority failure, if any
            - leaderboard: top 5 agents by pass rate
            - leaderboard_error: explicit leaderboard authority failure, if any
            - recent_failures: last 5 failed dispatches
            - system_health: "healthy", "degraded", or "unhealthy"
    """

    # 1. Workflow summary
    history = get_workflow_history()
    dispatch_data = history.summary()

    # 2. Cost summary
    tracker = get_cost_tracker()
    cost_data = tracker.summary()

    # 3. Circuit breaker states
    breakers = get_circuit_breakers()
    circuit_states = breakers.all_states()

    # 4. Route health (consecutive failures)
    route_outcomes = get_route_outcomes()
    route_health: dict[str, int] = {}
    route_health_error: str | None = None
    try:
        for provider_slug in route_outcomes.provider_slugs():
            route_health[provider_slug] = route_outcomes.consecutive_failures(
                provider_slug
            )
    except Exception as exc:
        logger.warning("route health unavailable: %s", exc)
        route_health = {}
        route_health_error = f"{type(exc).__name__}: {exc}"

    # 5. Leaderboard (top 5)
    leaderboard_error: str | None = None
    try:
        all_scores = build_leaderboard()
        top_5_scores = all_scores[:5]
        leaderboard = [
            {
                "provider_slug": s.provider_slug,
                "model_slug": s.model_slug,
                "total_workflows": s.total_workflows,
                "pass_rate": round(s.pass_rate * 100, 1),
                "avg_latency_ms": s.avg_latency_ms,
                "total_cost_usd": s.total_cost_usd,
                "avg_cost_per_workflow": s.avg_cost_per_workflow,
            }
            for s in top_5_scores
        ]
    except Exception as exc:
        logger.warning("leaderboard unavailable: %s", exc)
        leaderboard = []
        leaderboard_error = f"{type(exc).__name__}: {exc}"

    # 6. Recent failures (last 5)
    recent_failures = []
    recent = history.recent_workflows(limit=20)  # Scan last 20 to find failures
    for r in recent:
        if r.status == "failed":
            recent_failures.append({
                "run_id": r.run_id,
                "provider_slug": r.provider_slug,
                "model_slug": r.model_slug,
                "failure_code": r.failure_code,
                "latency_ms": r.latency_ms,
                "finished_at": r.finished_at.isoformat(),
            })
            if len(recent_failures) >= 5:
                break

    # 7. Observability metrics
    try:
        metrics_view = get_workflow_metrics_view()
        observability = {
            "efficiency_summary": metrics_view.efficiency_summary(days=7),
            "pass_rate_by_model": metrics_view.pass_rate_by_model(days=7),
            "latency_percentiles": metrics_view.latency_percentiles(days=7),
            "failure_category_breakdown": metrics_view.failure_category_breakdown(days=7),
            "hourly_workflow_volume": metrics_view.hourly_workflow_volume(days=7),
            "capability_distribution": metrics_view.capability_distribution(days=7),
        }
    except Exception as exc:
        logger.warning("observability metrics unavailable: %s", exc)
        observability = {
            "efficiency_summary": {
                "total_workflows": 0,
                "succeeded": 0,
                "failed": 0,
                "first_pass_success_rate": 0.0,
                "retry_success_rate": 0.0,
                "cost_per_success_usd": 0.0,
                "tokens_per_success": 0.0,
                "avg_latency_ms": 0.0,
                "avg_tool_uses": 0.0,
            },
            "pass_rate_by_model": [],
            "latency_percentiles": {"p50": 0, "p95": 0, "p99": 0},
            "failure_category_breakdown": [],
            "hourly_workflow_volume": [],
            "capability_distribution": [],
        }

    # 8. System health assessment
    pass_rate = dispatch_data.get("pass_rate", 0.0)
    open_circuits = sum(
        1
        for state in circuit_states.values()
        if state.get("state") == "OPEN"
    )
    half_open_circuits = sum(
        1
        for state in circuit_states.values()
        if state.get("state") == "HALF_OPEN"
    )

    observability_blocked = bool(
        route_health_error
        or leaderboard_error
        or dispatch_data.get("metrics_query_failed")
    )

    if observability_blocked:
        system_health = "degraded"
    elif pass_rate > 0.8 and open_circuits == 0:
        system_health = "healthy"
    elif pass_rate >= 0.5 and (open_circuits == 0 or half_open_circuits > 0):
        system_health = "degraded"
    else:
        system_health = "unhealthy"

    return {
        "workflow_summary": {
            "total_workflows": dispatch_data.get("total_workflows", 0),
            "succeeded": dispatch_data.get("succeeded", 0),
            "failed": dispatch_data.get("failed", 0),
            "pass_rate": round(dispatch_data.get("pass_rate", 0.0) * 100, 1),
            "workflow_history_source": dispatch_data.get("workflow_history_source", "metrics"),
            "workflow_history_status": dispatch_data.get("workflow_history_status", "complete"),
            "workflow_history_error": dispatch_data.get("workflow_history_error"),
            "metrics_query_failed": bool(dispatch_data.get("metrics_query_failed", False)),
        },
        "cost_summary": {
            "total_cost_usd": cost_data.get("total_cost_usd", 0.0),
            "total_input_tokens": cost_data.get("total_input_tokens", 0),
            "total_output_tokens": cost_data.get("total_output_tokens", 0),
            "cost_by_agent": cost_data.get("cost_by_agent", {}),
        },
        "circuit_states": circuit_states,
        "route_health": route_health,
        "route_health_error": route_health_error,
        "leaderboard": leaderboard,
        "leaderboard_error": leaderboard_error,
        "recent_failures": recent_failures,
        "observability": observability,
        "system_health": system_health,
    }


def format_dashboard(data: dict[str, Any]) -> str:
    """Format dashboard data as human-readable text.

    Returns:
        Multi-line dashboard string suitable for terminal output.
    """

    dispatch = data.get("workflow_summary", {})
    costs = data.get("cost_summary", {})
    circuits = data.get("circuit_states", {})
    health = data.get("system_health", "unknown")
    leaderboard = data.get("leaderboard", [])
    failures = data.get("recent_failures", [])
    observability = data.get("observability", {})
    efficiency = observability.get("efficiency_summary", {})
    failure_categories = observability.get("failure_category_breakdown", [])
    latency_percentiles = observability.get("latency_percentiles", {})
    route_health = data.get("route_health", {})
    route_health_error = data.get("route_health_error")
    leaderboard_error = data.get("leaderboard_error")

    # Summary line
    total_workflows = dispatch.get("total_workflows", 0)
    pass_rate = dispatch.get("pass_rate", 0.0)
    total_cost = costs.get("total_cost_usd", 0.0)
    workflow_history_source = dispatch.get("workflow_history_source", "metrics")
    workflow_history_status = dispatch.get("workflow_history_status", "complete")
    workflow_history_error = dispatch.get("workflow_history_error")

    summary_line = "\n".join(
        [
            "dashboard_summary:",
            f"  system_health={health.lower()}",
            f"  total_workflows={total_workflows}",
            f"  pass_rate_pct={pass_rate:.1f}",
            f"  total_cost_usd={total_cost:.2f}",
            f"  workflow_history_source={workflow_history_source}",
            f"  workflow_history_status={workflow_history_status}",
            *(
                [f"  workflow_history_error={workflow_history_error}"]
                if workflow_history_error
                else []
            ),
        ]
    )

    observability_line = "observability_digest:"
    if efficiency:
        observability_line += (
            f" first_pass_success_rate_pct={efficiency.get('first_pass_success_rate', 0.0) * 100:.1f}"
            f" retry_success_rate_pct={efficiency.get('retry_success_rate', 0.0) * 100:.1f}"
            f" cost_per_success_usd={efficiency.get('cost_per_success_usd', 0.0):.6f}"
            f" tokens_per_success={efficiency.get('tokens_per_success', 0.0):.2f}"
            f" p95_latency_ms={latency_percentiles.get('p95', 0)}"
            f" avg_tool_uses={efficiency.get('avg_tool_uses', 0.0):.2f}"
        )
    else:
        observability_line += " none"

    failure_line = "failure_mix:"
    if failure_categories:
        failure_line += " " + _format_failure_mini_digest(failure_categories)
    else:
        failure_line += " none"

    circuit_line = "circuit_states:"
    if circuits:
        circuit_line += " " + " ".join(
            f"{provider_slug}={state_data.get('state', 'UNKNOWN')}"
            for provider_slug, state_data in sorted(circuits.items())
        )
    else:
        circuit_line += " none"

    route_health_line = "route_health:"
    if route_health:
        route_health_line += " " + " ".join(
            f"{provider_slug}={failures}"
            for provider_slug, failures in sorted(route_health.items())
        )
    elif route_health_error:
        route_health_line += f" unavailable error={route_health_error}"
    else:
        route_health_line += " none"

    leaderboard_lines = ["leaderboard_top:"]
    if leaderboard:
        for score in leaderboard:
            provider = score.get("provider_slug", "?")
            model = score.get("model_slug", "?")
            pass_pct = score.get("pass_rate", 0.0)
            avg_latency = score.get("avg_latency_ms", 0)
            cost_per_run = score.get("avg_cost_per_workflow", 0.0)
            leaderboard_lines.append(
                f"  {provider}/{model} pass_rate_pct={pass_pct:.1f} "
                f"avg_latency_ms={avg_latency} avg_cost_per_run_usd={cost_per_run:.4f}"
            )
        if leaderboard_error:
            leaderboard_lines.append(f"  warning error={leaderboard_error}")
    elif leaderboard_error:
        leaderboard_lines.append(f"  unavailable error={leaderboard_error}")
    else:
        leaderboard_lines.append("  none")

    failure_lines = ["recent_failures:"]
    if failures:
        for failure in failures:
            run_id = failure.get("run_id", "?")
            provider = failure.get("provider_slug", "?")
            code = failure.get("failure_code", "unknown")
            finished_at = str(failure.get("finished_at", "?"))
            time_str = finished_at[11:19] if len(finished_at) >= 19 else finished_at
            failure_lines.append(
                f"  run_id={run_id} provider={provider} failure_code={code} finished_at={time_str}"
            )
    else:
        failure_lines.append("  none")

    return "\n".join(
        [
            summary_line,
            observability_line,
            failure_line,
            circuit_line,
            route_health_line,
            *leaderboard_lines,
            *failure_lines,
        ]
    )


def dashboard_as_json(data: dict[str, Any]) -> str:
    """Serialize dashboard data as JSON string."""
    return json.dumps(data, indent=2)
