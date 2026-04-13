"""Pure scoring math for task-type routing.

Covers range normalization, per-component benchmark/health scoring,
composite weight application, row re-ranking, and candidate label helpers.
No I/O, no DB queries.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .composite_scorer import CompositeScorer, ScaleFn

if TYPE_CHECKING:
    from .task_type_router import TaskRoutePolicy, TaskTypeRouteProfile

_PREPAID_BILLING_MODES = frozenset({"subscription_included", "prepaid_credit", "owned_compute"})


# ---------------------------------------------------------------------------
# Low-level math
# ---------------------------------------------------------------------------

def normalize_range(
    value: float,
    minimum: float,
    maximum: float,
    *,
    higher_is_better: bool,
) -> float:
    if maximum <= minimum:
        return 1.0
    normalized = (value - minimum) / (maximum - minimum)
    normalized = max(0.0, min(1.0, normalized))
    return normalized if higher_is_better else 1.0 - normalized


def ordered_preference_score(value: str, preferences: tuple[str, ...]) -> float:
    if not preferences:
        return 0.5
    try:
        index = preferences.index(value)
    except ValueError:
        return 0.0
    if len(preferences) == 1:
        return 1.0
    return 1.0 - (index / max(len(preferences) - 1, 1))


def derived_rank_from_score(score: float) -> int:
    clamped = max(0.0, min(1.0, float(score)))
    return int(round((1.0 - clamped) * 90.0)) + 10


def metric_value(metrics: dict[str, Any], metric_key: str) -> float | None:
    value = metrics.get(metric_key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Component scorers
# ---------------------------------------------------------------------------

def benchmark_component(
    row: Any,
    *,
    policy: "TaskRoutePolicy",
    benchmark_comparable: bool,
    benchmark_min: float,
    benchmark_max: float,
) -> float:
    raw_score = float(row.get("benchmark_score") or 0.0)
    benchmark_name = str(row.get("benchmark_name") or "").strip()
    if raw_score <= 0.0 or not benchmark_name:
        return policy.neutral_benchmark_score
    if not benchmark_comparable:
        return policy.mixed_benchmark_score
    return normalize_range(raw_score, benchmark_min, benchmark_max, higher_is_better=True)


def route_health_component(row: Any, *, policy: "TaskRoutePolicy") -> float:
    health = row.get("route_health_score")
    if health is not None:
        try:
            return max(policy.min_route_health, min(policy.max_route_health, float(health)))
        except (TypeError, ValueError):
            pass

    successes = int(row.get("observed_completed_count") or row.get("recent_successes") or 0)
    failures = int(row.get("observed_execution_failure_count") or row.get("recent_failures") or 0)
    total = successes + failures
    if total <= 0:
        return policy.neutral_route_health

    completion_rate = successes / total
    internal_penalty = min(0.25, failures / total * 0.25)
    return max(
        policy.min_route_health,
        min(policy.max_route_health, 0.50 + (completion_rate * 0.30) - internal_penalty),
    )


def failure_penalty(failure_category: str, *, policy: "TaskRoutePolicy") -> float:
    penalties = policy.internal_failure_penalties or {}
    return float(penalties.get(failure_category or "", penalties.get("unknown", 0.10)))


# ---------------------------------------------------------------------------
# Profile-level scoring (affinity + tier + latency)
# ---------------------------------------------------------------------------

def profile_task_rank_score(
    candidate: dict[str, Any],
    profile: "TaskTypeRouteProfile",
    *,
    affinity_bucket: str,
    ordered_preference_score_fn=ordered_preference_score,
) -> float:
    affinity_score = float(
        profile.affinity_weights.get(
            affinity_bucket,
            profile.affinity_weights.get("unclassified", 0.20),
        )
    )
    route_tier_score = ordered_preference_score_fn(
        str(candidate.get("route_tier") or ""),
        profile.route_tier_preferences,
    )
    latency_score = ordered_preference_score_fn(
        str(candidate.get("latency_class") or ""),
        profile.latency_class_preferences,
    )
    weights = profile.task_rank_weights or {}
    weight_total = (
        float(weights.get("affinity", 0.0))
        + float(weights.get("route_tier", 0.0))
        + float(weights.get("latency", 0.0))
    )
    if weight_total <= 0:
        return affinity_score
    return (
        (affinity_score * float(weights.get("affinity", 0.0)))
        + (route_tier_score * float(weights.get("route_tier", 0.0)))
        + (latency_score * float(weights.get("latency", 0.0)))
    ) / weight_total


# ---------------------------------------------------------------------------
# Benchmark score application (multi-metric weighted blend)
# ---------------------------------------------------------------------------

def apply_profile_benchmark_scores(
    task_type: str,
    profile: "TaskTypeRouteProfile",
    rows: list[dict[str, Any]],
    benchmark_metrics: dict[str, Any],
) -> None:
    """Compute and set benchmark_score / benchmark_name on each row in-place."""
    metric_weights = {
        metric_key: float(weight)
        for metric_key, weight in (profile.benchmark_metric_weights or {}).items()
        if float(weight) > 0
        and metric_key in benchmark_metrics
        and benchmark_metrics[metric_key].enabled
    }
    if not metric_weights or not rows:
        return

    metric_ranges: dict[str, tuple[float, float, bool]] = {}
    for metric_key in metric_weights:
        values = [
            v
            for row in rows
            if (v := metric_value(row.get("_common_metrics") or {}, metric_key)) is not None
        ]
        if not values:
            continue
        metric_ranges[metric_key] = (
            min(values),
            max(values),
            benchmark_metrics[metric_key].higher_is_better,
        )

    if not metric_ranges:
        return

    benchmark_name = f"market_blend:{task_type}"
    for row in rows:
        weighted_score = 0.0
        total_weight = 0.0
        for metric_key, weight in metric_weights.items():
            mv = metric_value(row.get("_common_metrics") or {}, metric_key)
            if mv is None or metric_key not in metric_ranges:
                continue
            minimum, maximum, higher_is_better = metric_ranges[metric_key]
            weighted_score += weight * normalize_range(
                mv, minimum, maximum, higher_is_better=higher_is_better
            )
            total_weight += weight
        if total_weight <= 0:
            row["benchmark_score"] = 0.0
            row["benchmark_name"] = ""
            continue
        row["benchmark_score"] = round((weighted_score / total_weight) * 100.0, 4)
        row["benchmark_name"] = benchmark_name


# ---------------------------------------------------------------------------
# Row re-ranking (composite scoring + billing-tier sort)
# ---------------------------------------------------------------------------

def row_effective_marginal_cost(row: Any) -> float:
    """Return effective marginal cost for a candidate row (used in scorer)."""
    configured = row.get("effective_marginal_cost")
    if configured is not None:
        try:
            return float(configured)
        except (TypeError, ValueError):
            pass
    try:
        return float(row.get("cost_per_m_tokens") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def rerank_rows(rows: list[Any], prefer_cost: bool, policy: "TaskRoutePolicy") -> list[Any]:
    if not rows:
        return rows

    scorer: CompositeScorer = policy.scorer(prefer_cost=prefer_cost)
    benchmark_values = [float(r.get("benchmark_score") or 0.0) for r in rows]
    cost_values = [row_effective_marginal_cost(r) for r in rows]
    rank_values = [float(r["rank"] or 0.0) for r in rows]
    comparable_benchmark_names = {
        str(r.get("benchmark_name") or "").strip().lower()
        for r in rows
        if float(r.get("benchmark_score") or 0.0) > 0.0
        and str(r.get("benchmark_name") or "").strip()
    }
    benchmark_comparable = len(comparable_benchmark_names) <= 1
    benchmark_min = min(benchmark_values) if benchmark_values else 0.0
    benchmark_max = max(benchmark_values) if benchmark_values else 1.0
    cost_min = min(cost_values)
    cost_max = max(cost_values)
    rank_min = min(rank_values)
    rank_max = max(rank_values)

    scored_rows: list[tuple[float, Any]] = []
    for row in rows:
        rh = route_health_component(row, policy=policy)
        result = scorer.score(
            benchmark_score=benchmark_component(
                row,
                policy=policy,
                benchmark_comparable=benchmark_comparable,
                benchmark_min=benchmark_min,
                benchmark_max=benchmark_max,
            ),
            cost=normalize_range(
                row_effective_marginal_cost(row),
                cost_min,
                cost_max,
                higher_is_better=False,
            ),
            task_rank=normalize_range(
                float(row["rank"] or 0.0),
                rank_min,
                rank_max,
                higher_is_better=False,
            ),
            route_health=rh,
        )
        health_penalty = min(
            policy.consecutive_failure_penalty_cap,
            policy.consecutive_failure_penalty_step
            * float(int(row.get("consecutive_internal_failures") or 0)),
        )
        stability_penalty = max(0.0, policy.neutral_route_health - rh) * 0.5
        scored_rows.append((result.total_score - health_penalty - stability_penalty, row))

    if prefer_cost:
        scored_rows.sort(key=lambda item: item[0], reverse=True)
    else:
        scored_rows.sort(
            key=lambda item: (
                0 if str(item[1].get("billing_mode") or "") in _PREPAID_BILLING_MODES else 1,
                -item[0],
            ),
        )
    return [row for _, row in scored_rows]


# ---------------------------------------------------------------------------
# Candidate label helpers (pure functions on catalog candidate dicts)
# ---------------------------------------------------------------------------

def positive_candidate_labels(candidate: dict[str, Any]) -> set[str]:
    labels = set(candidate.get("capability_tags") or ())
    task_affinities = candidate.get("task_affinities") or {}
    for bucket in ("primary", "secondary", "specialized"):
        values = task_affinities.get(bucket) or []
        if isinstance(values, list):
            labels.update(v.strip().lower() for v in values if isinstance(v, str) and v.strip())
    return labels


def candidate_avoid_labels(candidate: dict[str, Any]) -> set[str]:
    values = (candidate.get("task_affinities") or {}).get("avoid") or []
    if not isinstance(values, list):
        return set()
    return {v.strip().lower() for v in values if isinstance(v, str) and v.strip()}


def candidate_affinity_labels(
    candidate: dict[str, Any],
    *,
    buckets: tuple[str, ...] = ("primary", "secondary", "specialized"),
) -> set[str]:
    task_affinities = candidate.get("task_affinities") or {}
    labels: set[str] = set()
    for bucket in buckets:
        values = task_affinities.get(bucket) or []
        if isinstance(values, list):
            labels.update(v.strip().lower() for v in values if isinstance(v, str) and v.strip())
    return labels


def candidate_is_research_only(candidate: dict[str, Any]) -> bool:
    affinity_labels = candidate_affinity_labels(candidate)
    return bool(affinity_labels) and affinity_labels == {"research"}


def candidate_common_metrics(candidate: dict[str, Any]) -> dict[str, Any]:
    common_metrics = (
        ((candidate.get("benchmark_profile") or {}).get("market_benchmark") or {})
        .get("common_metrics") or {}
    )
    return common_metrics if isinstance(common_metrics, dict) else {}


def base_cost_per_m_tokens(candidate: dict[str, Any], *, state_row: dict[str, Any] | None) -> float:
    market_cost = metric_value(candidate_common_metrics(candidate), "price_1m_blended_3_to_1")
    if market_cost is not None:
        return market_cost
    return float(state_row.get("cost_per_m_tokens") or 0.0) if state_row is not None else 0.0


def match_affinity_bucket(candidate: dict[str, Any], profile: "TaskTypeRouteProfile") -> str:
    positive_labels = positive_candidate_labels(candidate)
    for bucket in ("avoid", "primary", "secondary", "specialized", "fallback"):
        if positive_labels.intersection(profile.affinity_labels.get(bucket, ())):
            return bucket
    return "unclassified"
