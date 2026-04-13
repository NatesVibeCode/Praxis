#!/usr/bin/env python3
"""Sync computed benchmark scores from market data into task_type_routing.

The router computes benchmark_score in-memory every call via _apply_profile_benchmark_scores,
but explicit task_type_routing rows keep stale 0.0 values in the DB. This script is
the operational backfill — run it:
  - After any Artificial Analysis sync (sync_market_model_registry.py)
  - On first boot to seed scores for all existing explicit routes

The TaskTypeRouter._persist_explicit_benchmark_scores() method handles incremental
updates at runtime, so this script is mainly needed for backfill and diagnosis.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

WORKFLOW_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKFLOW_ROOT))

import asyncpg

logger = logging.getLogger(__name__)

# Metric key aliases used in task_type_route_profiles.benchmark_metric_weights
# and in benchmark_profile.market_benchmark.common_metrics
_HIGHER_IS_BETTER: dict[str, bool] = {
    "artificial_analysis_coding_index":       True,
    "artificial_analysis_intelligence_index": True,
    "artificial_analysis_math_index":         True,
    "median_output_tokens_per_second":        True,
    "median_time_to_first_token_seconds":     False,
    "median_time_to_first_answer_token":      False,
    "price_1m_blended_3_to_1":               False,
    "price_1m_input_tokens":                  False,
    "price_1m_output_tokens":                 False,
}

_BENCHMARK_NAME_PREFIX = "market_blend"


def _extract_common_metrics(benchmark_profile: Any) -> dict[str, float]:
    """Pull numeric metrics out of a bound benchmark_profile JSONB blob."""
    if not benchmark_profile:
        return {}
    if isinstance(benchmark_profile, str):
        try:
            benchmark_profile = json.loads(benchmark_profile)
        except Exception:
            return {}
    market = benchmark_profile.get("market_benchmark") or {}
    if market.get("coverage_status") != "bound":
        return {}
    common = market.get("common_metrics") or {}
    result: dict[str, float] = {}
    for key, val in common.items():
        if val is not None:
            try:
                result[str(key)] = float(val)
            except (TypeError, ValueError):
                pass
    return result


def _normalize_range(value: float, lo: float, hi: float, *, higher_is_better: bool) -> float:
    if hi <= lo:
        return 1.0
    n = (value - lo) / (hi - lo)
    n = max(0.0, min(1.0, n))
    return n if higher_is_better else 1.0 - n


def _compute_score(
    metrics: dict[str, float],
    metric_weights: dict[str, float],
    metric_ranges: dict[str, tuple[float, float]],
    metric_higher_is_better: dict[str, bool],
) -> float | None:
    """Return a [0,100] weighted benchmark score, or None if no applicable metrics."""
    total_weight = 0.0
    weighted_sum = 0.0
    for metric_key, weight in metric_weights.items():
        val = metrics.get(metric_key)
        if val is None or metric_key not in metric_ranges:
            continue
        lo, hi = metric_ranges[metric_key]
        hib = metric_higher_is_better.get(metric_key, True)
        normalized = _normalize_range(val, lo, hi, higher_is_better=hib)
        weighted_sum += weight * normalized
        total_weight += weight
    if total_weight <= 0:
        return None
    return round((weighted_sum / total_weight) * 100.0, 4)


async def run_sync(
    *,
    database_url: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    conn = await asyncpg.connect(database_url)
    try:
        # 1. Load all active candidates with bound benchmark profiles
        candidate_rows = await conn.fetch(
            """
            SELECT provider_slug, model_slug, benchmark_profile
            FROM provider_model_candidates
            WHERE status = 'active'
              AND benchmark_profile IS NOT NULL
              AND benchmark_profile->'market_benchmark'->>'coverage_status' = 'bound'
            """
        )
        if not candidate_rows:
            logger.warning("No candidates with bound benchmark data — nothing to sync.")
            return {"ok": True, "updated": 0, "skipped": 0, "dry_run": dry_run}

        # 2. Extract metrics per model
        model_metrics: dict[tuple[str, str], dict[str, float]] = {}
        for row in candidate_rows:
            metrics = _extract_common_metrics(row["benchmark_profile"])
            if metrics:
                model_metrics[(str(row["provider_slug"]), str(row["model_slug"]))] = metrics

        logger.info("%d models with bound benchmark data", len(model_metrics))

        # 3. Load task_type_route_profiles for metric weights
        profile_rows = await conn.fetch(
            "SELECT task_type, benchmark_metric_weights FROM task_type_route_profiles"
        )
        task_metric_weights: dict[str, dict[str, float]] = {}
        for row in profile_rows:
            raw = row["benchmark_metric_weights"]
            if isinstance(raw, str):
                raw = json.loads(raw)
            if raw:
                task_metric_weights[str(row["task_type"])] = {
                    str(k): float(v) for k, v in raw.items() if float(v) > 0
                }

        # 4. Compute per-metric min/max across all models for normalization
        metric_ranges: dict[str, tuple[float, float]] = {}
        for metrics in model_metrics.values():
            for key, val in metrics.items():
                if key in metric_ranges:
                    lo, hi = metric_ranges[key]
                    metric_ranges[key] = (min(lo, val), max(hi, val))
                else:
                    metric_ranges[key] = (val, val)

        # 5. Load all explicit task_type_routing rows
        route_rows = await conn.fetch(
            """
            SELECT task_type, provider_slug, model_slug
            FROM task_type_routing
            WHERE permitted = true
              AND route_source = 'explicit'
            """
        )

        # 6. Compute and write scores
        updated = 0
        skipped = 0

        async with conn.transaction():
            for route in route_rows:
                task_type = str(route["task_type"]).removeprefix("auto/")
                provider_slug = str(route["provider_slug"])
                model_slug = str(route["model_slug"])

                metrics = model_metrics.get((provider_slug, model_slug))
                if not metrics:
                    skipped += 1
                    continue

                weights = task_metric_weights.get(task_type) or task_metric_weights.get(
                    f"auto/{task_type}"
                )
                if not weights:
                    skipped += 1
                    continue

                score = _compute_score(
                    metrics,
                    weights,
                    metric_ranges,
                    _HIGHER_IS_BETTER,
                )
                if score is None:
                    skipped += 1
                    continue

                benchmark_name = f"{_BENCHMARK_NAME_PREFIX}:{task_type}"

                if dry_run:
                    logger.info(
                        "DRY-RUN %s/%s task=%s → %.2f (%s)",
                        provider_slug,
                        model_slug,
                        task_type,
                        score,
                        benchmark_name,
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE task_type_routing
                        SET benchmark_score = $1,
                            benchmark_name  = $2,
                            updated_at      = now()
                        WHERE task_type     = $3
                          AND provider_slug  = $4
                          AND model_slug     = $5
                          AND route_source   = 'explicit'
                        """,
                        score,
                        benchmark_name,
                        route["task_type"],  # use original (may have auto/ prefix)
                        provider_slug,
                        model_slug,
                    )
                updated += 1

        logger.info(
            "benchmark score sync: updated=%d skipped=%d dry_run=%s",
            updated,
            skipped,
            dry_run,
        )
        return {"ok": True, "updated": updated, "skipped": skipped, "dry_run": dry_run}
    finally:
        await conn.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync market benchmark scores into task_type_routing for explicit routes.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ["WORKFLOW_DATABASE_URL"],
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser.parse_args(argv)


async def _main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    result = await run_sync(database_url=args.database_url, dry_run=args.dry_run)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
