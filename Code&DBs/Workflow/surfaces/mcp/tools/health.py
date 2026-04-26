"""Tools: praxis_health."""
from __future__ import annotations

import os
from typing import Any

from runtime.engineering_observability import build_trend_observability
from runtime.dependency_contract import dependency_truth_report
from runtime.context_cache import get_context_cache
from runtime.missing_detector import build_content_health_report
from runtime.system_events import emit_system_event
from runtime.workflow import get_route_outcomes
from registry.config_registry import get_config as get_registry_config
from registry.provider_execution_registry import registry_health as provider_registry_health
from surfaces.api.operator_read import (
    build_transport_support_summary,
    query_transport_support,
)
from surfaces.api.handlers._surface_usage import surface_usage_recorder_health

from surfaces._workflow_database import workflow_database_url_for_repo
from ..subsystems import _subs, REPO_ROOT, workflow_database_env
from ..helpers import _serialize


_PROJECTION_FRESHNESS_WARNING_STALENESS_SECONDS = 300.0
_PROJECTION_FRESHNESS_CRITICAL_STALENESS_SECONDS = 900.0
_PROJECTION_FRESHNESS_WARNING_LAG_EVENTS = 0
_PROJECTION_FRESHNESS_CRITICAL_LAG_EVENTS = 100


def _projection_freshness_sla_policy():
    from runtime.projection_freshness import ProjectionFreshnessSlaPolicy

    try:
        config = get_registry_config()
        return ProjectionFreshnessSlaPolicy(
            warning_staleness_seconds=config.get_float(
                "observability.projection_freshness.warning_staleness_seconds"
            ),
            critical_staleness_seconds=config.get_float(
                "observability.projection_freshness.critical_staleness_seconds"
            ),
            warning_lag_events=config.get_int(
                "observability.projection_freshness.warning_lag_events"
            ),
            critical_lag_events=config.get_int(
                "observability.projection_freshness.critical_lag_events"
            ),
            policy_source="platform_config",
        )
    except Exception as exc:
        return ProjectionFreshnessSlaPolicy(
            warning_staleness_seconds=_PROJECTION_FRESHNESS_WARNING_STALENESS_SECONDS,
            critical_staleness_seconds=_PROJECTION_FRESHNESS_CRITICAL_STALENESS_SECONDS,
            warning_lag_events=_PROJECTION_FRESHNESS_WARNING_LAG_EVENTS,
            critical_lag_events=_PROJECTION_FRESHNESS_CRITICAL_LAG_EVENTS,
            policy_source=f"code_default_missing_platform_config:{type(exc).__name__}",
        )


def tool_praxis_health(params: dict, _progress_emitter=None) -> dict:
    """Run health probes, return preflight + operator snapshot + lane recommendation."""
    hs_mod = _subs.get_health_mod()

    def _static_probe(**kwargs: Any) -> Any:
        probe_cls = getattr(hs_mod, "StaticHealthProbe", None)
        if probe_cls is None:
            from runtime.health import StaticHealthProbe as probe_cls
        return probe_cls(**kwargs)

    # Mirror the admin health handler so MCP and /orient expose one probe truth.
    probes: list[Any] = []
    db_url = workflow_database_url_for_repo(REPO_ROOT, env=workflow_database_env())
    probes.append(hs_mod.PostgresProbe(db_url))
    probes.append(hs_mod.PostgresConnectivityProbe(db_url))

    probes.append(hs_mod.DiskSpaceProbe(str(REPO_ROOT)))
    surface_usage_recorder = surface_usage_recorder_health()
    if surface_usage_recorder.get("authority_ready") is False:
        probes.append(
            _static_probe(
                name="surface_usage_recorder",
                passed=False,
                message=f"surface usage recorder degraded: {surface_usage_recorder.get('last_error') or 'unknown error'}",
                status="failed",
                details=surface_usage_recorder,
            )
        )
    try:
        transport_support = query_transport_support(
            health_mod=hs_mod,
            pg=_subs.get_pg_conn(),
        )
    except Exception as exc:
        transport_support_error = f"{type(exc).__name__}: {exc}"
        transport_support = {
            "default_provider_slug": "",
            "default_adapter_type": "",
            "registered_providers": [],
            "providers": [],
            "support_basis": f"unavailable:{transport_support_error}",
        }
        probes.append(
            _static_probe(
                name="transport_support",
                passed=False,
                message=f"transport support unavailable: {transport_support_error}",
                status="failed",
                details={"error": transport_support_error},
            )
        )
    transport_support_summary = build_transport_support_summary(transport_support)
    try:
        provider_registry = provider_registry_health()
    except Exception as exc:
        provider_registry_error = f"{type(exc).__name__}: {exc}"
        provider_registry = {
            "status": "load_failed",
            "error": provider_registry_error,
            "authority_available": False,
            "fallback_active": False,
        }
        probes.append(
            _static_probe(
                name="provider_registry",
                passed=False,
                message=f"provider registry load failed: {provider_registry_error}",
                status="failed",
                details=provider_registry,
            )
        )
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
        from runtime.projection_freshness import (
            collect_projection_freshness_sync,
            evaluate_projection_freshness_sla,
        )

        freshness_samples = collect_projection_freshness_sync(_subs.get_pg_conn())
        projection_freshness: Any = [sample.to_json() for sample in freshness_samples]
        projection_freshness_sla: Any = evaluate_projection_freshness_sla(
            freshness_samples,
            policy=_projection_freshness_sla_policy(),
        ).to_json()
    except Exception as exc:
        projection_freshness = {"status": "error", "reason": str(exc)}
        projection_freshness_sla = {"status": "error", "reason": str(exc)}

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
                {
                    "name": c.name,
                    "passed": c.passed,
                    "message": c.message,
                    "duration_ms": round(c.duration_ms, 2),
                    "status": getattr(c, "status", None) or ("ok" if c.passed else "failed"),
                    "details": getattr(c, "details", {}),
                }
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
        "surface_usage_recorder": surface_usage_recorder,
        "dependency_truth": dependency_truth,
        "context_cache": cache_stats,
        "content_health": content_health,
        "trend_observability": trend_observability,
        "projection_freshness": projection_freshness,
        "projection_freshness_sla": projection_freshness_sla,
        "route_outcomes": route_outcomes_summary,
    }


def tool_dag_health(params: dict, _progress_emitter=None) -> dict:
    """Backwards-compatible alias for the MCP health front door."""
    return tool_praxis_health(params, _progress_emitter=_progress_emitter)


# BUG-FE3A8255: allowlist for importlib.reload of runtime code modules.
# Only pure-function/pure-logic modules with no process-wide state (pools,
# registries bound to handler closures, launchd-facing threads) are included.
# Adding a module here is an opt-in decision that the module is safe to
# re-execute mid-process.
_RUNTIME_RELOAD_ALLOWLIST_PREFIXES: tuple[str, ...] = (
    "runtime.workflow_spec",
    "runtime.workflow_validation",
    "runtime.workflow_chain",
    "runtime.workflow._context_building",
    "runtime.capability.",     # resolver, plan envelope, lifecycle
    "runtime.compile_reuse",
    "runtime.idempotency",
    "runtime.job_dependencies",
    "runtime.workflow_trigger_handlers",
)


def _module_is_reload_allowed(module_name: str) -> bool:
    return any(
        module_name == prefix.rstrip(".") or module_name.startswith(prefix)
        for prefix in _RUNTIME_RELOAD_ALLOWLIST_PREFIXES
    )


def _reload_runtime_modules(requested: list[str] | None) -> dict:
    """importlib.reload over an allowlisted set of pure runtime modules.

    BUG-FE3A8255: operator hot-fix flow needs a lighter alternative to a full
    MCP subprocess restart after editing a runtime/*.py file. Only modules
    declared safe in `_RUNTIME_RELOAD_ALLOWLIST_PREFIXES` are reloadable —
    anything else raises a structured error so we never silently reload a
    state-bearing module that would leave handler closures half-bound.
    """
    import importlib
    import sys

    if requested is None:
        # Default: reload every allowlisted module already imported.
        targets = sorted(
            name
            for name in list(sys.modules)
            if _module_is_reload_allowed(name)
        )
    else:
        targets = []
        rejected: list[str] = []
        for name in requested:
            name = str(name or "").strip()
            if not name:
                continue
            if not _module_is_reload_allowed(name):
                rejected.append(name)
                continue
            targets.append(name)
        if rejected:
            return {
                "reloaded": [],
                "failed": [],
                "rejected": rejected,
                "reason_code": "runtime.reload.module_not_allowlisted",
                "allowlist_prefixes": list(_RUNTIME_RELOAD_ALLOWLIST_PREFIXES),
            }

    reloaded: list[str] = []
    failed: list[dict] = []
    for name in targets:
        module = sys.modules.get(name)
        if module is None:
            # Not yet imported — nothing to reload.
            continue
        try:
            importlib.reload(module)
            reloaded.append(name)
        except Exception as exc:
            failed.append({"module": name, "error": f"{type(exc).__name__}: {exc}"})
    return {
        "reloaded": reloaded,
        "failed": failed,
        "count": len(reloaded),
        "allowlist_prefixes": list(_RUNTIME_RELOAD_ALLOWLIST_PREFIXES),
    }


def _emit_reload_audit_event(scope: str, result: dict[str, Any], modules: list[str] | None = None) -> bool:
    """Persist a durable record of reload intent and mutations."""
    try:
        conn = _subs.get_pg_conn()
    except Exception:
        return False
    runtime_modules = result.get("runtime_modules")
    if not isinstance(runtime_modules, dict):
        runtime_modules = {}
    payload = {
        "scope": scope,
        "result_count": int(result.get("count") or 0),
        "process_id": os.getpid(),
        "cache_reloaded": list(result.get("reloaded") or []),
        "cache_failed": list(result.get("failed") or []),
        "requested_modules": modules or [],
        "runtime_modules": runtime_modules,
        "runtime_modules_reloaded": list(runtime_modules.get("reloaded") or []),
        "runtime_modules_failed": list(runtime_modules.get("failed") or []),
        "runtime_modules_rejected": list(runtime_modules.get("rejected") or []),
        "allowlist_prefixes": list(_RUNTIME_RELOAD_ALLOWLIST_PREFIXES),
    }
    try:
        emit_system_event(
            conn,
            event_type="runtime.reload",
            source_id="mcp_tool",
            source_type="runtime_admin",
            payload=payload,
        )
    except Exception:
        return False
    return True


def tool_praxis_reload(params: dict) -> dict:
    """Clear in-process caches and optionally importlib.reload runtime modules.

    BUG-FE3A8255: added `scope` + `modules` parameters so a hot-fixed runtime
    module (e.g. a patched validator) can take effect without a full MCP
    subprocess restart.

    Parameters:
        scope: "caches" (default, back-compat) | "runtime_modules" | "all"
        modules: optional list of runtime module names to reload. If omitted
            and scope includes runtime_modules, every allowlisted module
            already in sys.modules is reloaded.
    """
    params = dict(params or {})
    scope = str(params.get("scope") or "caches").strip().lower()
    if scope not in {"caches", "runtime_modules", "all"}:
        return {
            "error": f"invalid scope '{scope}'; expected one of caches|runtime_modules|all",
            "reason_code": "runtime.reload.invalid_scope",
        }
    modules_param = params.get("modules")
    if modules_param is not None and not isinstance(modules_param, list):
        return {
            "error": "modules must be a list of module name strings",
            "reason_code": "runtime.reload.invalid_modules_param",
        }

    cleared: list[str] = []
    if scope in {"caches", "all"}:
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

    result: dict[str, Any] = {"reloaded": cleared, "count": len(cleared), "scope": scope}
    if scope in {"runtime_modules", "all"}:
        result["runtime_modules"] = _reload_runtime_modules(modules_param)
    result["audit"] = {"system_event_recorded": _emit_reload_audit_event(scope=scope, result=result, modules=modules_param)}
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_health": (
        tool_praxis_health,
        {
            "description": (
                "Full system health check — Postgres connectivity, disk space, operator panel state, "
                "workflow lane recommendations, context cache stats, memory graph health, and "
                "projection freshness (event-log cursors + process-cache refresh lag) with SLA alerts "
                "and a read-side circuit-breaker verdict.\n\n"
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
                "Clear in-process caches and optionally importlib.reload runtime modules "
                "so DB, config, and code changes take effect without restarting the MCP subprocess.\n\n"
                "USE WHEN: you just applied a migration, updated native runtime authority, "
                "added a model to provider_model_candidates, or EDITED a runtime/*.py module "
                "whose fix isn't visible through the MCP surface yet (BUG-FE3A8255).\n\n"
                "PARAMETERS:\n"
                "  scope: 'caches' (default, back-compat) | 'runtime_modules' | 'all'\n"
                "  modules: optional list of allowlisted runtime module names; if omitted "
                "and scope includes runtime_modules, every allowlisted module already in "
                "sys.modules is reloaded.\n\n"
                "EXAMPLES:\n"
                "  praxis_reload()                                         # caches only\n"
                "  praxis_reload(scope='all')                              # caches + every allowlisted runtime module\n"
                "  praxis_reload(scope='runtime_modules', modules=['runtime.workflow_validation'])\n\n"
                "Clears (caches): model_profiles context windows, MCP tool catalog, context cache.\n"
                "Reloads (runtime_modules): allowlisted pure-runtime modules only — state-bearing "
                "modules (pools, registries) are rejected with reason_code "
                "'runtime.reload.module_not_allowlisted'."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "string",
                        "enum": ["caches", "runtime_modules", "all"],
                        "description": "What to refresh. 'caches' is back-compat default.",
                    },
                    "modules": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of allowlisted module names to reload.",
                    },
                },
            },
        },
    ),
}
