"""Tools: praxis_provider_onboard."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..subsystems import REPO_ROOT, workflow_database_env
from ..helpers import _serialize
from storage.postgres.connection import resolve_workflow_database_url


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


def tool_praxis_provider_onboard(params: dict, _progress_emitter=None) -> dict:
    """Onboard a CLI or API provider with probing, capability assignment, and routing."""
    from registry.provider_onboarding import (
        normalize_provider_onboarding_spec,
        run_provider_onboarding,
    )

    action = str(params.get("action", "probe")).strip().lower()
    provider_slug = str(params.get("provider_slug", "")).strip()
    if not provider_slug:
        return {"error": "provider_slug is required"}

    transport = str(params.get("transport", "cli")).strip().lower()
    models = params.get("models") or []
    api_key_env_var = params.get("api_key_env_var")

    raw_spec: dict[str, Any] = {
        "provider_slug": provider_slug,
        "selected_transport": transport,
    }
    if models:
        raw_spec["requested_models"] = list(models)
    if api_key_env_var:
        raw_spec["api_key_env_vars"] = [api_key_env_var]

    dry_run = action == "probe"

    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=3, message=f"Normalizing spec for {provider_slug}")

    try:
        spec = normalize_provider_onboarding_spec(raw_spec)
    except Exception as exc:
        return {"error": f"Invalid spec: {exc}"}

    db_url = resolve_workflow_database_url(env=workflow_database_env())

    if _progress_emitter:
        label = "probe" if dry_run else "onboard"
        _progress_emitter.emit(progress=1, total=3, message=f"Running provider {label} ({transport})")

    result = run_provider_onboarding(
        database_url=db_url,
        spec=spec,
        dry_run=dry_run,
    )

    serialized = _serialize(result)

    if not dry_run and result.ok:
        if _progress_emitter:
            _progress_emitter.emit(progress=2, total=3, message="Syncing caps and routing tables")
        post_onboarding = _post_onboarding_sync(
            db_url=db_url,
            provider_slug=provider_slug,
            model_reports=result.model_reports,
        )
        serialized["post_onboarding"] = post_onboarding

    if _progress_emitter:
        status = "ok" if result.ok else "failed"
        _progress_emitter.emit(progress=3, total=3, message=f"Done — {provider_slug} {status}")

    return serialized


def _post_onboarding_sync(
    *,
    db_url: str,
    provider_slug: str,
    model_reports: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    """Post-onboarding: populate cap_ columns, routing rows, and runtime_profiles.json."""
    from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool

    conn = SyncPostgresConnection(get_workflow_pool())
    results: dict[str, Any] = {"cap_columns": [], "routing_rows": 0, "runtime_profiles": False}

    for report in model_reports:
        model_slug = report.get("model_slug", "")
        if not model_slug:
            continue

        # 1. Read task_affinities and route_tier from the just-written candidate
        rows = conn.execute(
            """SELECT task_affinities, route_tier FROM provider_model_candidates
               WHERE provider_slug = $1 AND model_slug = $2 AND status = 'active'
               ORDER BY priority ASC LIMIT 1""",
            provider_slug, model_slug,
        )
        if not rows:
            continue
        affinities = rows[0].get("task_affinities") or {}
        route_tier = rows[0].get("route_tier") or "medium"

        # 2. Populate cap_ columns from task_affinities
        primary = set(affinities.get("primary") or [])
        secondary = set(affinities.get("secondary") or [])
        all_labels = primary | secondary
        caps = {col: False for col in set(_AFFINITY_TO_CAP.values())}
        for label in all_labels:
            col = _AFFINITY_TO_CAP.get(label.lower().replace(" ", "_"))
            if col:
                caps[col] = True

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

        # 3. Write task_type_routing rows
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
                    task_type, provider_slug, model_slug, rank_base,
                    route_tier,
                )
                routing_count += 1

        # Add block rows for avoid list
        for avoid_label in avoid:
            for cap_col, task_types in _CAP_TO_TASK_TYPES.items():
                for task_type in task_types:
                    if avoid_label.lower() in task_type.lower():
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
                            task_type, provider_slug, model_slug,
                        )

        results["routing_rows"] += routing_count

    # 4. Update config/runtime_profiles.json allowed_models
    try:
        config_path = REPO_ROOT / "config" / "runtime_profiles.json"
        if config_path.is_file():
            config = json.loads(config_path.read_text(encoding="utf-8"))
            profiles = config.get("runtime_profiles", {})
            for profile_name, profile in profiles.items():
                allowed = list(profile.get("allowed_models", []))
                new_slugs = [
                    r.get("model_slug", "")
                    for r in model_reports
                    if r.get("model_slug") and r["model_slug"] not in allowed
                ]
                if new_slugs:
                    allowed.extend(new_slugs)
                    profile["allowed_models"] = allowed
            config_path.write_text(
                json.dumps(config, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            results["runtime_profiles"] = True
            results["added_to_allowed_models"] = [
                r.get("model_slug", "") for r in model_reports
                if r.get("model_slug")
            ]
    except Exception as exc:
        results["runtime_profiles_error"] = str(exc)

    return results


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_provider_onboard": (
        tool_praxis_provider_onboard,
        {
            "description": (
                "Onboard a CLI or API provider into Praxis Engine. "
                "Probes transport, discovers models, tests capacity, writes to all routing tables, "
                "and updates runtime config.\n\n"
                "USE WHEN: connecting a new provider (claude, codex, gemini, openrouter) or "
                "adding models to an existing provider.\n\n"
                "EXAMPLES:\n"
                "  Probe first:  praxis_provider_onboard(action='probe', provider_slug='anthropic', transport='cli')\n"
                "  Then onboard: praxis_provider_onboard(action='onboard', provider_slug='anthropic', transport='cli')\n"
                "  API provider: praxis_provider_onboard(action='onboard', provider_slug='openrouter', transport='api', "
                "api_key_env_var='OPENROUTER_API_KEY')\n\n"
                "The 'probe' action is a dry run — shows what would happen without writing. "
                "The 'onboard' action writes to provider_model_candidates, task_type_routing, "
                "model_profiles, and config/runtime_profiles.json.\n\n"
                "DO NOT USE: for checking provider health (use praxis_health)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["probe", "onboard"],
                        "description": "'probe' (dry run) or 'onboard' (write to DB + config)",
                    },
                    "provider_slug": {
                        "type": "string",
                        "description": "Provider identifier (e.g., 'anthropic', 'openai', 'google', 'openrouter')",
                    },
                    "transport": {
                        "type": "string",
                        "enum": ["cli", "api"],
                        "description": "Transport type: 'cli' for CLI tools, 'api' for direct API",
                    },
                    "models": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional specific model slugs to onboard (discovers all if omitted)",
                    },
                    "api_key_env_var": {
                        "type": "string",
                        "description": "Env var name for API key (e.g., 'OPENROUTER_API_KEY')",
                    },
                },
                "required": ["action", "provider_slug"],
            },
        },
    ),
}
