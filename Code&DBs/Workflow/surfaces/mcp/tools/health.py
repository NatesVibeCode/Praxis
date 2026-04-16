"""Tools: praxis_health."""
from __future__ import annotations

from typing import Any

from adapters import provider_registry as provider_registry_mod
from runtime.engineering_observability import build_trend_observability
from runtime.dependency_contract import dependency_truth_report
from runtime.context_cache import get_context_cache

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
    registered_providers = tuple(provider_registry_mod.registered_providers())
    provider_registry_summary: list[dict[str, Any]] = []
    for provider_slug in registered_providers:
        adapters: list[str] = []
        for adapter_type in ("cli_llm", "llm_task"):
            if not provider_registry_mod.supports_adapter(provider_slug, adapter_type):
                continue
            adapters.append(adapter_type)
            probes.append(hs_mod.ProviderTransportProbe(provider_slug, adapter_type))
        provider_registry_summary.append(
            {
                "provider_slug": provider_slug,
                "adapters": adapters,
            }
        )

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
        from runtime.missing_detector import FindingPrioritizer, MissingContentDetector

        detector = MissingContentDetector(stale_days=30)
        prioritizer = FindingPrioritizer()
        engine = _subs.get_memory_engine()
        conn = engine._connect()
        if conn is None:
            content_health = {"status": "skipped", "reason": "no memory engine connection"}
        else:
            # Cap queries to avoid timeout on large graphs
            entity_rows = conn.execute(
                "SELECT id, entity_type, name, created_at, updated_at "
                "FROM memory_entities WHERE archived = false "
                "ORDER BY updated_at DESC LIMIT 500"
            )
            edge_rows = conn.execute(
                "SELECT source_id, target_id FROM memory_edges "
                "WHERE active = true LIMIT 2000"
            )
            entities = [
                {
                    "id": row["id"],
                    "type": row["entity_type"],
                    "name": row["name"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
                for row in entity_rows
            ]
            edges = [
                {
                    "source": row["source_id"],
                    "target": row["target_id"],
                }
                for row in edge_rows
            ]
            findings = detector.detect_stale_topics(entities)
            findings.extend(detector.detect_weekly_gaps(entities))
            findings.extend(detector.detect_orphaned_actions(entities, edges))
            prioritized = prioritizer.prioritize(findings, max_surfaced=5)
            content_health = {
                "total_findings": len(findings),
                "top_findings": [
                    {
                        "finding_type": finding.finding_type.value,
                        "description": finding.description,
                        "severity": finding.severity,
                    }
                    for finding in prioritized
                ],
            }
    except Exception as exc:
        content_health = {"status": "error", "reason": str(exc)}

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
        "provider_registry": {
            "default_provider_slug": provider_registry_mod.default_provider_slug(),
            "default_adapter_type": provider_registry_mod.default_llm_adapter_type(),
            "registered_providers": list(registered_providers),
            "providers": provider_registry_summary,
        },
        "dependency_truth": dependency_truth,
        "context_cache": cache_stats,
        "content_health": content_health,
        "trend_observability": trend_observability,
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
                "workflow lane recommendations, context cache stats, and memory graph health.\n\n"
                "USE WHEN: starting a session, things seem broken, or you want to verify the platform "
                "is ready before dispatching work. No parameters needed.\n\n"
                "EXAMPLE: praxis_health()\n\n"
                "DO NOT USE: for workflow pass/fail rates (use praxis_status), or for operator-level "
                "detail (use praxis_operator_view)."
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
