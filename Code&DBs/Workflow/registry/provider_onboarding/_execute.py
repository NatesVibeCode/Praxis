"""Shared provider-onboarding executor used by all exposed surfaces."""

from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from ._report import run_provider_onboarding
from ._spec import ProviderOnboardingSpec, normalize_provider_onboarding_spec


_AFFINITY_TO_CAP = {
    "language_high": "cap_language_high",
    "language-high": "cap_language_high",
    "analysis_architecture_research": "cap_analysis_architecture_research",
    "analysis-architecture-research": "cap_analysis_architecture_research",
    "architecture": "cap_analysis_architecture_research",
    "build_high": "cap_build_high",
    "build-high": "cap_build_high",
    "review": "cap_review",
    "tool_use": "cap_tool_use",
    "tool-use": "cap_tool_use",
    "build_med": "cap_build_med",
    "build-med": "cap_build_med",
    "build": "cap_build_med",
    "language_low": "cap_language_low",
    "language-low": "cap_language_low",
    "chat": "cap_language_low",
    "build_low": "cap_build_low",
    "build-low": "cap_build_low",
    "wiring": "cap_build_low",
    "research_fan": "cap_research_fan",
    "research-fan": "cap_research_fan",
    "research": "cap_research_fan",
    "image": "cap_image",
}

_CAP_TO_TASK_TYPES = {
    "cap_build_high": ["build", "refactor"],
    "cap_analysis_architecture_research": ["architecture"],
    "cap_review": ["review"],
    "cap_tool_use": ["test"],
    "cap_build_low": ["wiring"],
    "cap_language_low": ["chat"],
    "cap_research_fan": ["research"],
}

_TIER_RANK_BASE = {"high": 1, "medium": 4, "low": 7}


def _serialize(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {key: _serialize(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(value) for value in obj]
    if is_dataclass(obj):
        return {field.name: _serialize(getattr(obj, field.name)) for field in fields(obj)}
    if hasattr(obj, "value"):
        return obj.value
    return str(obj)


def _build_raw_spec(
    *,
    spec: Mapping[str, Any] | ProviderOnboardingSpec | None,
    provider_slug: str | None,
    transport: str | None,
    models: Sequence[str] | None,
    api_key_env_var: str | None,
) -> Mapping[str, Any] | ProviderOnboardingSpec:
    if spec is not None:
        return spec

    normalized_provider_slug = str(provider_slug or "").strip()
    if not normalized_provider_slug:
        raise ValueError("provider_slug is required")

    raw_spec: dict[str, Any] = {"provider_slug": normalized_provider_slug}
    if transport:
        raw_spec["selected_transport"] = str(transport).strip().lower()
    if models:
        raw_spec["requested_models"] = list(models)
    if api_key_env_var:
        raw_spec["api_key_env_vars"] = [str(api_key_env_var).strip()]
    return raw_spec


def _normalize_spec(
    raw_spec: Mapping[str, Any] | ProviderOnboardingSpec,
) -> ProviderOnboardingSpec:
    if isinstance(raw_spec, ProviderOnboardingSpec):
        return raw_spec
    return normalize_provider_onboarding_spec(raw_spec)


def _post_onboarding_sync(
    *,
    database_url: str,
    provider_slug: str,
    model_reports: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Populate cap columns, routing rows, and native runtime profiles."""
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    conn = SyncPostgresConnection(get_workflow_pool(env={"WORKFLOW_DATABASE_URL": database_url}))
    results: dict[str, Any] = {"cap_columns": [], "routing_rows": 0, "native_runtime_profiles": False}

    for report in model_reports:
        model_slug = str(report.get("model_slug") or "").strip()
        if not model_slug:
            continue

        rows = conn.execute(
            """SELECT task_affinities, route_tier FROM provider_model_candidates
               WHERE provider_slug = $1 AND model_slug = $2 AND status = 'active'
               ORDER BY priority ASC LIMIT 1""",
            provider_slug,
            model_slug,
        )
        if not rows:
            continue
        affinities = rows[0].get("task_affinities") or {}
        if isinstance(affinities, str):
            try:
                affinities = json.loads(affinities)
            except json.JSONDecodeError:
                affinities = {}
        if not isinstance(affinities, Mapping):
            affinities = {}
        route_tier = rows[0].get("route_tier") or "medium"

        primary = set(affinities.get("primary") or [])
        secondary = set(affinities.get("secondary") or [])
        all_labels = primary | secondary
        caps = {col: False for col in set(_AFFINITY_TO_CAP.values())}
        for label in all_labels:
            cap_column = _AFFINITY_TO_CAP.get(str(label).lower().replace(" ", "_"))
            if cap_column:
                caps[cap_column] = True

        conn.execute(
            """UPDATE provider_model_candidates SET
                   cap_language_high = $2, cap_analysis_architecture_research = $3,
                   cap_build_high = $4, cap_review = $5, cap_tool_use = $6,
                   cap_build_med = $7, cap_language_low = $8, cap_build_low = $9,
                   cap_research_fan = $10, cap_image = $11
               WHERE provider_slug = $1 AND model_slug = $12 AND status = 'active'""",
            provider_slug,
            caps.get("cap_language_high", False),
            caps.get("cap_analysis_architecture_research", False),
            caps.get("cap_build_high", False),
            caps.get("cap_review", False),
            caps.get("cap_tool_use", False),
            caps.get("cap_build_med", False),
            caps.get("cap_language_low", False),
            caps.get("cap_build_low", False),
            caps.get("cap_research_fan", False),
            caps.get("cap_image", False),
            model_slug,
        )
        results["cap_columns"].append({"model_slug": model_slug, "caps": caps})

        avoid = set(affinities.get("avoid") or [])
        rank_base = _TIER_RANK_BASE.get(route_tier, 5)
        routing_count = 0

        for cap_col, task_types in _CAP_TO_TASK_TYPES.items():
            if not caps.get(cap_col, False):
                continue
            for task_type in task_types:
                conn.execute(
                    """INSERT INTO task_type_routing (
                           task_type, provider_slug, model_slug, permitted, rank,
                           route_tier, route_tier_rank, latency_class, latency_rank,
                           reasoning_control, route_health_score, route_source,
                           recent_successes, recent_failures,
                           observed_completed_count, observed_execution_failure_count,
                           observed_external_failure_count, observed_config_failure_count,
                           observed_downstream_failure_count, observed_downstream_bug_count,
                           consecutive_internal_failures, last_failure_category,
                           last_failure_zone
                       ) VALUES (
                           $1, $2, $3, true, $4,
                           $5, $4, 'reasoning', $4,
                           '{}'::jsonb, 0.65, 'explicit',
                           0, 0, 0, 0, 0, 0, 0, 0, 0, '', ''
                       )
                       ON CONFLICT (task_type, provider_slug, model_slug) DO NOTHING""",
                    task_type,
                    provider_slug,
                    model_slug,
                    rank_base,
                    route_tier,
                )
                routing_count += 1

        for avoid_label in avoid:
            for task_types in _CAP_TO_TASK_TYPES.values():
                for task_type in task_types:
                    if str(avoid_label).lower() in task_type.lower():
                        conn.execute(
                            """INSERT INTO task_type_routing (
                                   task_type, provider_slug, model_slug, permitted, rank,
                                   route_tier, route_tier_rank, latency_class, latency_rank,
                                   reasoning_control, route_health_score, route_source,
                                   recent_successes, recent_failures,
                                   observed_completed_count, observed_execution_failure_count,
                                   observed_external_failure_count, observed_config_failure_count,
                                   observed_downstream_failure_count, observed_downstream_bug_count,
                                   consecutive_internal_failures, last_failure_category,
                                   last_failure_zone
                               ) VALUES (
                                   $1, $2, $3, false, 99,
                                   'low', 99, 'instant', 99,
                                   '{}'::jsonb, 0.65, 'explicit',
                                   0, 0, 0, 0, 0, 0, 0, 0, 0, '', ''
                               )
                               ON CONFLICT (task_type, provider_slug, model_slug) DO NOTHING""",
                            task_type,
                            provider_slug,
                            model_slug,
                        )
                        routing_count += 1

        results["routing_rows"] += routing_count

    try:
        native_rows = conn.execute(
            """
            SELECT runtime_profile_ref, allowed_models
            FROM registry_native_runtime_profile_authority
            ORDER BY runtime_profile_ref
            """
        )
        added_models = [
            str(report.get("model_slug") or "").strip()
            for report in model_reports
            if str(report.get("model_slug") or "").strip()
        ]
        added_models = list(dict.fromkeys(added_models))
        if native_rows and added_models:
            updated_profiles: list[str] = []
            for row in native_rows:
                allowed = row.get("allowed_models") or []
                if isinstance(allowed, str):
                    allowed = json.loads(allowed)
                allowed_list = [str(value).strip() for value in allowed if str(value).strip()]
                merged = list(dict.fromkeys([*allowed_list, *added_models]))
                if merged == allowed_list:
                    continue
                conn.execute(
                    """
                    UPDATE registry_native_runtime_profile_authority
                    SET allowed_models = $2::jsonb,
                        recorded_at = now()
                    WHERE runtime_profile_ref = $1
                    """,
                    str(row["runtime_profile_ref"]),
                    json.dumps(merged),
                )
                updated_profiles.append(str(row["runtime_profile_ref"]))
            if updated_profiles:
                results["native_runtime_profiles"] = True
                results["updated_runtime_profile_refs"] = updated_profiles
                results["added_to_allowed_models"] = added_models
        else:
            results["native_runtime_profiles"] = False
    except Exception as exc:
        results["native_runtime_profiles_error"] = str(exc)
    return results


def execute_provider_onboarding(
    *,
    database_url: str,
    spec: Mapping[str, Any] | ProviderOnboardingSpec | None = None,
    provider_slug: str | None = None,
    transport: str | None = None,
    models: Sequence[str] | None = None,
    api_key_env_var: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized_spec = _normalize_spec(
        _build_raw_spec(
            spec=spec,
            provider_slug=provider_slug,
            transport=transport,
            models=models,
            api_key_env_var=api_key_env_var,
        )
    )
    result = run_provider_onboarding(
        database_url=database_url,
        spec=normalized_spec,
        dry_run=dry_run,
    )
    payload = _serialize(result)
    if not dry_run and result.ok:
        payload["post_onboarding"] = _post_onboarding_sync(
            database_url=database_url,
            provider_slug=normalized_spec.provider_slug,
            model_reports=result.model_reports,
        )
    return payload


__all__ = ["execute_provider_onboarding"]
