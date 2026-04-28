"""Provider onboarding benchmark planning — market model matching and benchmark probes."""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

import asyncpg

from ._spec import (
    ProviderOnboardingModelSpec,
    ProviderOnboardingSpec,
    ProviderOnboardingStepResult,
    _family_slug,
    _normalized_slug,
)

__all__ = [
    "_aggregator_routing",
    "_load_benchmark_source_info",
    "_plan_benchmark_rules",
    "_probe_benchmark",
]


async def _load_benchmark_source_info(
    conn: asyncpg.Connection,
    benchmark_source_slug: str,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT source_slug,
               display_name,
               api_key_env_var,
               enabled
          FROM market_benchmark_source_registry
         WHERE source_slug = $1
         LIMIT 1
        """,
        benchmark_source_slug,
    )
    if row is None:
        return None
    return {
        "source_slug": str(row["source_slug"]),
        "display_name": str(row["display_name"]),
        "api_key_env_var": str(row["api_key_env_var"]),
        "enabled": bool(row["enabled"]),
    }


def _aggregator_routing(
    provider_slug: str, model_slug: str
) -> tuple[str, str] | None:
    """Map an aggregator candidate to its underlying creator/model for planning.

    OpenRouter and Together publish models as `<creator_prefix>/<model>`; the
    benchmark planner needs the underlying creator and model to find the
    matching row in the source feed. Returns `None` for non-aggregator or
    unparseable candidates so the caller falls back to the native path
    (provider_slug treated as creator).
    """
    if provider_slug not in {"openrouter", "together"}:
        return None
    if "/" not in model_slug:
        return None
    prefix, _, rest = model_slug.partition("/")
    prefix = prefix.strip().lower()
    rest = rest.strip()
    if not prefix or not rest:
        return None
    return prefix, rest


def _plan_benchmark_rules(
    *,
    provider_slug: str,
    models: Sequence[ProviderOnboardingModelSpec],
    source_config: Mapping[str, Any],
    market_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    creator_aliases = {
        str(key).strip().lower(): str(value).strip().lower()
        for key, value in dict(source_config.get("creator_slug_aliases") or {}).items()
    }
    market_lookup = {
        (str(row["creator_slug"]), str(row["source_model_slug"])): dict(row)
        for row in market_rows
    }
    by_creator: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in market_rows:
        by_creator[str(row["creator_slug"])].append(dict(row))

    def _resolve_lookup_keys(model_slug: str) -> tuple[str, str]:
        agg = _aggregator_routing(provider_slug, model_slug)
        if agg is None:
            return creator_aliases.get(provider_slug, provider_slug), model_slug
        raw_creator, effective_slug = agg
        return creator_aliases.get(raw_creator, raw_creator), effective_slug

    plan: list[dict[str, Any]] = []
    for model in models:
        creator_slug, lookup_slug = _resolve_lookup_keys(model.model_slug)
        creator_rows = by_creator.get(creator_slug, [])

        exact = market_lookup.get((creator_slug, lookup_slug))
        if exact is not None:
            plan.append(
                {
                    "model_slug": model.model_slug,
                    "target_creator_slug": creator_slug,
                    "target_source_model_slug": str(exact["source_model_slug"]),
                    "match_kind": "exact_source_slug",
                    "binding_confidence": 1.0,
                    "selection_metadata": {
                        "reason": "The executable slug matches the benchmark slug directly.",
                        "coverage_scope": "text_benchmark",
                    },
                    "market_row": exact,
                }
            )
            continue

        normalized_target = _normalized_slug(lookup_slug)
        normalized_matches = [
            row
            for row in creator_rows
            if _normalized_slug(str(row["source_model_slug"])) == normalized_target
        ]
        if len(normalized_matches) == 1:
            plan.append(
                {
                    "model_slug": model.model_slug,
                    "target_creator_slug": creator_slug,
                    "target_source_model_slug": str(normalized_matches[0]["source_model_slug"]),
                    "match_kind": "normalized_slug_alias",
                    "binding_confidence": 0.99,
                    "selection_metadata": {
                        "reason": "Only punctuation differs between the executable slug and the benchmark slug.",
                        "coverage_scope": "text_benchmark",
                    },
                    "market_row": normalized_matches[0],
                }
            )
            continue

        family_target = _family_slug(lookup_slug)
        family_matches = [
            row
            for row in creator_rows
            if _family_slug(str(row["source_model_slug"])) == family_target
        ]
        if len(family_matches) == 1:
            target_row = family_matches[0]
            match_kind = "dated_release_alias" if normalized_target != _normalized_slug(
                str(target_row["source_model_slug"])
            ) else "family_proxy"
            plan.append(
                {
                    "model_slug": model.model_slug,
                    "target_creator_slug": creator_slug,
                    "target_source_model_slug": str(target_row["source_model_slug"]),
                    "match_kind": match_kind,
                    "binding_confidence": 0.98 if match_kind == "dated_release_alias" else 0.75,
                    "selection_metadata": {
                        "reason": (
                            "The executable slug appears to be a dated or release-specific alias for the benchmark family."
                            if match_kind == "dated_release_alias"
                            else "The executable slug can only be mapped to a broader benchmark family row."
                        ),
                        "coverage_scope": "text_benchmark",
                    },
                    "market_row": target_row,
                }
            )
            continue

        plan.append(
            {
                "model_slug": model.model_slug,
                "target_creator_slug": creator_slug,
                "target_source_model_slug": None,
                "match_kind": "source_unavailable",
                "binding_confidence": 0.0,
                "selection_metadata": {
                    "reason": (
                        "The benchmark source does not publish a directly comparable row for this provider/model pair."
                    ),
                    "coverage_scope": "gap",
                },
                "market_row": None,
            }
        )
    return plan


async def _probe_benchmark(
    conn: asyncpg.Connection,
    *,
    spec: ProviderOnboardingSpec,
    models: Sequence[ProviderOnboardingModelSpec],
) -> tuple[ProviderOnboardingStepResult, dict[str, Any]]:
    if not spec.benchmark_source_slug:
        return (
            ProviderOnboardingStepResult(
                step="benchmark_probe",
                status="skipped",
                summary="No benchmark source configured",
                details={},
            ),
            {},
        )

    benchmark_info = await _load_benchmark_source_info(conn, spec.benchmark_source_slug)
    if benchmark_info is None or not benchmark_info["enabled"]:
        return (
            ProviderOnboardingStepResult(
                step="benchmark_probe",
                status="warning",
                summary=(
                    f"Benchmark source {spec.benchmark_source_slug} is not registered or is disabled"
                ),
                details={"benchmark_source_slug": spec.benchmark_source_slug},
            ),
            {},
        )

    resolved_api_key = spec.benchmark_api_key or os.environ.get(
        benchmark_info["api_key_env_var"],
        "",
    ).strip()
    if not resolved_api_key:
        return (
            ProviderOnboardingStepResult(
                step="benchmark_probe",
                status="warning",
                summary=(
                    f"No API key configured for {benchmark_info['display_name']}. "
                    f"Go get a key from that site and set {benchmark_info['api_key_env_var']} "
                    "or pass benchmark_api_key."
                ),
                details={
                    "benchmark_source_slug": spec.benchmark_source_slug,
                    "benchmark_source": benchmark_info,
                    "api_key_env_var": benchmark_info["api_key_env_var"],
                },
            ),
            {
                "benchmark_source": benchmark_info,
                "api_key_env_var": benchmark_info["api_key_env_var"],
            },
        )

    from scripts import sync_market_model_registry as market_sync

    source_config = await market_sync._load_source_config(conn, spec.benchmark_source_slug)
    market_rows = market_sync.load_market_models(source_config, api_key=resolved_api_key)
    plan = _plan_benchmark_rules(
        provider_slug=spec.provider_slug,
        models=models,
        source_config=source_config,
        market_rows=market_rows,
    )
    matched_count = sum(1 for row in plan if row["match_kind"] != "source_unavailable")
    unavailable_count = sum(1 for row in plan if row["match_kind"] == "source_unavailable")
    report = {
        "ok": True,
        "source": spec.benchmark_source_slug,
        "market_models": len(market_rows),
        "matched_models": matched_count,
        "unavailable_models": unavailable_count,
        "plans": [
            {
                "model_slug": row["model_slug"],
                "match_kind": row["match_kind"],
                "binding_confidence": row["binding_confidence"],
                "target_creator_slug": row["target_creator_slug"],
                "target_source_model_slug": row["target_source_model_slug"],
                "selection_metadata": dict(row["selection_metadata"]),
            }
            for row in plan
        ],
        "source_config": {
            "source_slug": str(source_config["source_slug"]),
            "display_name": str(source_config["display_name"]),
            "api_key_env_var": str(source_config["api_key_env_var"]),
        },
        "_source_config": dict(source_config),
        "_market_rows": tuple(dict(row) for row in market_rows),
        "_resolved_api_key": resolved_api_key,
        "_plan": plan,
    }
    return (
        ProviderOnboardingStepResult(
            step="benchmark_probe",
            status="succeeded",
            summary=(
                f"Benchmarked {len(models)} model(s) against {benchmark_info['display_name']}; "
                f"{matched_count} matched, {unavailable_count} marked unavailable"
            ),
            details={
                "source": spec.benchmark_source_slug,
                "market_models": len(market_rows),
                "matched_models": matched_count,
                "unavailable_models": unavailable_count,
                "plans": report["plans"],
            },
        ),
        report,
    )
