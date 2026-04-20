"""Tools: praxis_health."""
from __future__ import annotations

from typing import Any

from runtime.engineering_observability import build_trend_observability
from runtime.dependency_contract import dependency_truth_report
from runtime.context_cache import get_context_cache
from runtime.missing_detector import build_content_health_report
from runtime.workflow import get_route_outcomes
from registry.config_registry import get_config as get_registry_config
from registry.provider_execution_registry import registry_health as provider_registry_health
from surfaces.api.operator_read import (
    build_transport_support_summary,
    query_transport_support,
)

from surfaces._workflow_database import workflow_database_url_for_repo
from ..subsystems import _subs, REPO_ROOT, workflow_database_env
from ..helpers import _serialize

def tool_praxis_health(params: dict, _progress_emitter=None) -> dict:
    """Run health probes, return preflight + operator snapshot + lane recommendation."""
    hs_mod = _subs.get_health_mod()

    # Mirror the admin health handler so MCP and /orient expose one probe truth.
    probes: list[Any] = []
    db_url = workflow_database_url_for_repo(REPO_ROOT, env=workflow_database_env())
    probes.append(hs_mod.PostgresProbe(db_url))
    probes.append(hs_mod.PostgresConnectivityProbe(db_url))

    probes.append(hs_mod.DiskSpaceProbe(str(REPO_ROOT)))
    transport_support = query_transport_support(
        health_mod=hs_mod,
        pg=_subs.get_pg_conn(),
    )
    transport_support_summary = build_transport_support_summary(transport_support)
    try:
        provider_registry = provider_registry_health()
    except Exception as exc:
        provider_registry = {
            "status": "load_failed",
            "error": str(exc),
            "authority_available": False,
            "fallback_active": False,
        }
    for provider_slug, adapter_type in transport_support_summary["probe_targets"]:
        probes.append(hs_mod.ProviderTransportProbe(provider_slug, adapter_type))

    if _progress_emitter:
        _progress_emitter.log(f"Running {len(probes)} health probes")
        _progress_emitter.emit(progress=0, total=len(probes), message="Starting preflight")

    runner = hs_mod.PreflightRunner(probes)
    preflight = runner.run()

    if _progress_emitter:
        for i, c in enumerate(preflight.checks, 1):
            icon = "+" if c.passed else "x"
            _progress_emitter.emit(progress=i, total=len(probes),
                                   message=f"[{icon}] {c.name} — {c.message[:80] if c.message else 'ok'}")

    panel = _subs.get_operator_panel()
    snap = panel.snapshot()
    lane = panel.recommend_lane()
    cache_stats = get_context_cache().stats()
    dependency_truth = dependency_truth_report(scope="all")
    trend_observability = build_trend_observability()
    try:
        memory_engine = getattr(_subs, "get_memory_engine", lambda: None)()
        content_health = build_content_health_report(memory_engine)
    except Exception as exc:
        content_health = {"status": "error", "reason": str(exc)}

    try:
        from runtime.projection_freshness import collect_projection_freshness_sync

        freshness_samples = collect_projection_freshness_sync(_subs.get_pg_conn())
        projection_freshness: Any = [sample.to_json() for sample in freshness_samples]
    except Exception as exc:
        projection_freshness = {"status": "error", "reason": str(exc)}

    try:
        route_outcomes = get_route_outcomes()
        try:
            max_consecutive_failures = get_registry_config().get_int(
                "health.max_consecutive_failures"
            )
        except Exception:
            max_consecutive_failures = 1
        route_outcomes_summary: Any = route_outcomes.summary(
            max_consecutive_failures=max_consecutive_failures,
        )
    except Exception as exc:
        route_outcomes_summary = {"status": "error", "reason": str(exc)}

    return {
        "preflight": {
            "overall": preflight.overall.value,
            "checks": [
                {"name": c.name, "passed": c.passed, "message": c.message, "duration_ms": round(c.duration_ms, 2)}
                for c in preflight.checks
            ],
            "timestamp": preflight.timestamp.isoformat(),
        },
        "operator_snapshot": _serialize(snap),
        "lane_recommendation": {
            "recommended_posture": lane.recommended_posture,
            "confidence": lane.confidence,
            "reasons": list(lane.reasons),
            "degraded_cause": lane.degraded_cause,
        },
        "transport_support_summary": {
            "default_provider_slug": transport_support_summary["default_provider_slug"],
            "default_adapter_type": transport_support_summary["default_adapter_type"],
            "registered_providers": list(transport_support_summary["registered_providers"]),
            "providers": list(transport_support_summary["providers"]),
            "support_basis": transport_support_summary["support_basis"],
            "provider_registry_status": provider_registry.get("status"),
            "provider_registry_authority_available": provider_registry.get("authority_available"),
            "provider_registry_fallback_active": provider_registry.get("fallback_active"),
        },
        "provider_registry": provider_registry,
        "dependency_truth": dependency_truth,
        "context_cache": cache_stats,
        "content_health": content_health,
        "trend_observability": trend_observability,
        "projection_freshness": projection_freshness,
        "route_outcomes": route_outcomes_summary,
    }


def tool_dag_health(params: dict, _progress_emitter=None) -> dict:
    """Backwards-compatible alias for the MCP health front door."""
    return tool_praxis_health(params, _progress_emitter=_progress_emitter)


def tool_praxis_reload(params: dict) -> dict:
    """Clear all in-process caches so DB and config changes take effect without restart."""
    cleared: list[str] = []

    # 1. model_profiles context window cache
    try:
        import registry.model_context_limits as mcl
        mcl._model_profiles_cache = None
        mcl._model_profiles_loaded = False
        cleared.append("model_context_limits")
    except Exception as exc:
        cleared.append(f"model_context_limits: FAILED ({exc})")

    # 2. MCP tool catalog lru_cache
    try:
        from surfaces.mcp.catalog import get_tool_catalog, resolve_tool_entry
        get_tool_catalog.cache_clear()
        resolve_tool_entry.cache_clear()
        cleared.append("mcp_catalog")
    except Exception as exc:
        cleared.append(f"mcp_catalog: FAILED ({exc})")

    # 3. Agent registry (no persistent cache, but force a fresh load on next access)
    #    The registry is constructed per-call in load_from_postgres, so clearing
    #    model_profiles is sufficient — it's the only thing that causes silent drops.

    # 4. Context cache
    try:
        get_context_cache().clear()
        cleared.append("context_cache")
    except Exception as exc:
        cleared.append(f"context_cache: FAILED ({exc})")

    return {"reloaded": cleared, "count": len(cleared)}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_health": (
        tool_praxis_health,
        {
            "description": (
                "Full system health check — Postgres connectivity, disk space, operator panel state, "
                "workflow lane recommendations, context cache stats, memory graph health, and "
                "projection freshness (event-log cursors + process-cache refresh lag).\n\n"
                "USE WHEN: starting a session, things seem broken, or you want to verify the platform "
                "is ready before launching a workflow. Includes route-outcome health, so you can see "
                "which provider routes are actually failing. No parameters needed.\n\n"
                "EXAMPLE: praxis_health()\n\n"
                "DO NOT USE: for workflow pass/fail rates (use praxis_status_snapshot), or for operator-level "
                "detail (use explicit tools like praxis_run_status or praxis_graph_projection)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {},
            },
        },
    ),
    "praxis_reload": (
        tool_praxis_reload,
        {
            "description": (
                "Clear all in-process caches so DB and config changes take effect "
                "without restarting Claude Desktop.\n\n"
                "USE WHEN: you just applied a migration, updated native runtime authority, "
                "added a model to provider_model_candidates, or changed anything in Postgres "
                "that the MCP server caches on first load.\n\n"
                "EXAMPLE: praxis_reload()\n\n"
                "Clears: model_profiles context windows, MCP tool catalog, context cache."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {},
            },
        },
    ),
}
