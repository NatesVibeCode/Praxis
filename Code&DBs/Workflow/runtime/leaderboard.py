"""Agent leaderboard built from workflow receipt data.

Aggregates per-agent (provider_slug, model_slug) statistics from persisted
workflow receipts and renders them as an operator-facing table or JSON.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass
from typing import Any

from .cost_tracker import _extract_cost, _safe_float, _safe_int
from .composite_scorer import CompositeScorer, ScaleFn
from . import receipt_store


@dataclass(frozen=True, slots=True)
class AgentScore:
    """Aggregate performance for one (provider, model) pair."""

    provider_slug: str
    model_slug: str
    total_workflows: int
    succeeded: int
    failed: int
    pass_rate: float
    total_cost_usd: float
    avg_latency_ms: int
    p95_latency_ms: int
    avg_cost_per_workflow: float
    cost_per_success: float | None


_DEFAULT_LEADERBOARD_SCAN_LIMIT = int(os.environ.get("PRAXIS_LEADERBOARD_SCAN_LIMIT", "10000"))
_LEADERBOARD_SCORER = CompositeScorer([
    ("pass_rate", 0.4, ScaleFn.SIGMOID, True),
    ("cost_efficiency", 0.3, ScaleFn.LOGARITHMIC, True),
    ("avg_latency", 0.2, ScaleFn.LINEAR, True),
    ("volume", 0.1, ScaleFn.BUCKET, True),
])


def _percentile(sorted_values: list[int], pct: float) -> int:
    """Return the *pct*-th percentile from an already-sorted list."""
    if not sorted_values:
        return 0
    idx = math.ceil(len(sorted_values) * pct / 100.0) - 1
    idx = max(0, min(idx, len(sorted_values) - 1))
    return sorted_values[idx]


def _normalize_ratio(value: float, maximum: float, *, invert: bool = False) -> float:
    """Normalize a positive value into [0, 1], optionally inverting it."""
    if maximum <= 0:
        return 1.0 if invert else 0.0
    ratio = max(0.0, min(1.0, value / maximum))
    return 1.0 - ratio if invert else ratio


def _sort_key(
    s: AgentScore,
    *,
    max_latency_ms: int,
    max_dispatches: int,
) -> float:
    cps = s.cost_per_success if s.cost_per_success is not None else float("inf")
    cost_efficiency = 0.0 if math.isinf(cps) else 1.0 / (1.0 + cps)
    result = _LEADERBOARD_SCORER.score(
        pass_rate=s.pass_rate,
        cost_efficiency=cost_efficiency,
        avg_latency=_normalize_ratio(
            float(s.avg_latency_ms),
            float(max_latency_ms),
            invert=True,
        ),
        volume=_normalize_ratio(
            float(s.total_workflows),
            float(max_dispatches),
        ),
    )
    return -result.total_score


def build_leaderboard(
    *,
    receipts_dir: str | None = None,
) -> list[AgentScore]:
    """Load all receipts, group by agent, compute scores, return sorted list."""

    # 1. Load all receipts from Postgres
    records = receipt_store.list_receipts(limit=_DEFAULT_LEADERBOARD_SCAN_LIMIT)
    receipts: list[dict[str, Any]] = [rec.to_dict() for rec in records]

    # 2. Group by (provider_slug, model_slug)
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in receipts:
        key = (
            r.get("provider_slug", "unknown"),
            r.get("model_slug") or "unknown",
        )
        groups.setdefault(key, []).append(r)

    # 3. Compute per-group scores
    scores: list[AgentScore] = []
    for (provider, model), group in groups.items():
        total = len(group)
        succeeded = sum(1 for r in group if r.get("status") == "succeeded")
        failed = total - succeeded

        latencies: list[int] = []
        total_cost = 0.0

        for r in group:
            latencies.append(_safe_int(r.get("latency_ms")))

            # Extract cost using the same logic as cost_tracker
            outputs = r.get("outputs") or {}
            cost_usd, _, _ = _extract_cost(outputs)

            # Fallback: top-level total_cost_usd on the receipt itself
            if cost_usd == 0.0:
                cost_usd = _safe_float(r.get("total_cost_usd"))

            total_cost += cost_usd

        latencies.sort()
        avg_latency = int(sum(latencies) / total) if total else 0
        p95_latency = _percentile(latencies, 95)
        avg_cost = total_cost / total if total else 0.0
        pass_rate = succeeded / total if total else 0.0
        cost_per_success = (
            total_cost / succeeded if succeeded > 0 else None
        )

        scores.append(
            AgentScore(
                provider_slug=provider,
                model_slug=model,
                total_workflows=total,
                succeeded=succeeded,
                failed=failed,
                pass_rate=pass_rate,
                total_cost_usd=round(total_cost, 6),
                avg_latency_ms=avg_latency,
                p95_latency_ms=p95_latency,
                avg_cost_per_workflow=round(avg_cost, 6),
                cost_per_success=(
                    round(cost_per_success, 6) if cost_per_success is not None else None
                ),
            )
        )

    # 4. Sort using the composite scorer once the cohort maxima are known.
    max_latency_ms = max((s.avg_latency_ms for s in scores), default=0)
    max_dispatches = max((s.total_workflows for s in scores), default=0)
    scores.sort(
        key=lambda s: _sort_key(
            s,
            max_latency_ms=max_latency_ms,
            max_dispatches=max_dispatches,
        )
    )
    return scores


def format_leaderboard(scores: list[AgentScore]) -> str:
    """Pretty-print agent scores as a fixed-width table."""

    if not scores:
        return "No workflow receipts found."

    header = (
        f"{'provider/model':<30} {'dispatches':>10} {'pass%':>6} "
        f"{'avg_ms':>7} {'cost_usd':>10} {'$/success':>10}"
    )
    sep = "-" * len(header)
    lines = [header, sep]

    for s in scores:
        label = f"{s.provider_slug}/{s.model_slug}"
        pass_pct = f"{s.pass_rate * 100:.0f}%"
        cost_str = f"${s.total_cost_usd:.2f}"
        cps_str = f"${s.cost_per_success:.2f}" if s.cost_per_success is not None else "-"
        lines.append(
            f"{label:<30} {s.total_workflows:>10} {pass_pct:>6} "
            f"{s.avg_latency_ms:>7} {cost_str:>10} {cps_str:>10}"
        )

    return "\n".join(lines)


def leaderboard_as_json(scores: list[AgentScore]) -> str:
    """Serialize scores as a JSON array string."""
    return json.dumps([asdict(s) for s in scores], indent=2)
