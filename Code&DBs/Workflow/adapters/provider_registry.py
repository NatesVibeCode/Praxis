"""Provider execution registry compatibility facade.

The canonical DB-backed provider execution authority lives in
``registry.provider_execution_registry``. This module preserves the adapter
import path while avoiding eager imports that would create registry bootstrap
cycles.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any

from .provider_types import ProviderAdapterContract, ProviderCLIProfile

__all__ = [
    "ProviderAdapterContract",
    "ProviderCLIProfile",
    "ProviderRegistryError",
    "ProviderRegistryLoadError",
    "ProviderRegistrySchemaError",
    "ProviderRegistryDataError",
    "ProviderRegistryLoadTimeout",
    "RegistryLoadStatus",
    "reload_from_db",
    "registry_health",
    "resolve_adapter_config",
    "get_profile",
    "get_all_profiles",
    "registered_providers",
    "resolve_provider_from_alias",
    "default_provider_slug",
    "default_llm_adapter_type",
    "default_model_for_provider",
    "resolve_adapter_economics",
    "resolve_api_endpoint",
    "resolve_api_protocol_family",
    "resolve_api_key_env_vars",
    "resolve_mcp_args_template",
    "resolve_lane_policy",
    "resolve_adapter_contract",
    "supports_adapter",
    "supports_model_adapter",
    "resolve_binary",
    "build_command",
    "validate_profiles",
    "transport_support_report",
]

_LAZY_PUBLIC_NAMES = {
    "ProviderRegistryError",
    "ProviderRegistryLoadError",
    "ProviderRegistrySchemaError",
    "ProviderRegistryDataError",
    "ProviderRegistryLoadTimeout",
    "RegistryLoadStatus",
}


def _execution_registry() -> ModuleType:
    return importlib.import_module("registry.provider_execution_registry")


def __getattr__(name: str) -> Any:
    if name in _LAZY_PUBLIC_NAMES:
        return getattr(_execution_registry(), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def reload_from_db() -> None:
    _execution_registry().reload_from_db()


def registry_health() -> dict[str, Any]:
    return _execution_registry().registry_health()


def resolve_adapter_config(key: str, default: Any = None) -> Any:
    return _execution_registry().resolve_adapter_config(key, default)


def get_profile(provider_slug: str) -> ProviderCLIProfile | None:
    return _execution_registry().get_profile(provider_slug)


def get_all_profiles() -> dict[str, ProviderCLIProfile]:
    return _execution_registry().get_all_profiles()


def registered_providers() -> list[str]:
    return _execution_registry().registered_providers()


def resolve_provider_from_alias(alias: str) -> str | None:
    return _execution_registry().resolve_provider_from_alias(alias)


def default_provider_slug() -> str:
    return _execution_registry().default_provider_slug()


def default_llm_adapter_type() -> str:
    return _execution_registry().default_llm_adapter_type()


def default_model_for_provider(provider_slug: str) -> str | None:
    return _execution_registry().default_model_for_provider(provider_slug)


def resolve_adapter_economics(provider_slug: str, adapter_type: str) -> dict[str, Any]:
    return _execution_registry().resolve_adapter_economics(provider_slug, adapter_type)


def resolve_api_endpoint(provider_slug: str, model_slug: str | None = None) -> str | None:
    return _execution_registry().resolve_api_endpoint(provider_slug, model_slug=model_slug)


def resolve_api_protocol_family(provider_slug: str) -> str | None:
    return _execution_registry().resolve_api_protocol_family(provider_slug)


def resolve_api_key_env_vars(provider_slug: str) -> tuple[str, ...]:
    return _execution_registry().resolve_api_key_env_vars(provider_slug)


def resolve_mcp_args_template(provider_slug: str) -> list[str]:
    return _execution_registry().resolve_mcp_args_template(provider_slug)


def resolve_lane_policy(provider_slug: str, adapter_type: str) -> dict[str, Any] | None:
    return _execution_registry().resolve_lane_policy(provider_slug, adapter_type)


def resolve_adapter_contract(
    provider_slug: str,
    adapter_type: str,
) -> ProviderAdapterContract | None:
    return _execution_registry().resolve_adapter_contract(provider_slug, adapter_type)


def supports_adapter(provider_slug: str, adapter_type: str) -> bool:
    return _execution_registry().supports_adapter(provider_slug, adapter_type)


def supports_model_adapter(provider_slug: str, model_slug: str, adapter_type: str) -> bool:
    return _execution_registry().supports_model_adapter(provider_slug, model_slug, adapter_type)


def resolve_binary(provider_slug: str) -> str | None:
    return _execution_registry().resolve_binary(provider_slug)


def build_command(
    provider_slug: str,
    model: str | None = None,
    *,
    binary_override: str | None = None,
    system_prompt: str | None = None,
    json_schema: str | None = None,
) -> list[str]:
    return _execution_registry().build_command(
        provider_slug,
        model=model,
        binary_override=binary_override,
        system_prompt=system_prompt,
        json_schema=json_schema,
    )


def validate_profiles() -> dict[str, dict[str, Any]]:
    return _execution_registry().validate_profiles()


def transport_support_report(
    *,
    health_mod: Any,
    pg: Any,
    provider_filter: str | None = None,
    model_filter: str | None = None,
    runtime_profile_ref: str = "praxis",
    jobs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Assemble the operator-facing transport support report."""

    provider_reports = validate_profiles()
    providers = sorted(provider_reports.keys())
    if provider_filter is not None:
        providers = [slug for slug in providers if slug == provider_filter]

    transport_checks: dict[tuple[str, str], dict[str, Any]] = {}
    for slug in providers:
        for adapter_type in ("cli_llm", "llm_task"):
            probe = health_mod.ProviderTransportProbe(slug, adapter_type).check()
            transport_checks[(slug, adapter_type)] = {
                "supported": probe.passed,
                "status": probe.status or ("ok" if probe.passed else "failed"),
                "message": probe.message,
                "details": probe.details,
            }

    rows = pg.execute(
        """
        SELECT DISTINCT ON (provider_slug, model_slug)
               provider_slug,
               model_slug,
               capability_tags,
               route_tier,
               latency_class
        FROM provider_model_candidates
        WHERE status = 'active'
        ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
        """
    )
    models: list[dict[str, Any]] = []
    for row in rows or []:
        row_provider = str(row["provider_slug"])
        row_model = str(row["model_slug"])
        if provider_filter is not None and row_provider != provider_filter:
            continue
        if model_filter is not None and row_model != model_filter:
            continue
        adapter_support: dict[str, Any] = {}
        for adapter_type in ("cli_llm", "llm_task"):
            provider_transport = transport_checks.get((row_provider, adapter_type))
            if provider_transport is None:
                continue
            model_ok = supports_model_adapter(row_provider, row_model, adapter_type)
            details = dict(provider_transport.get("details") or {})
            details.update(
                {
                    "model_slug": row_model,
                    "model_supported": model_ok,
                    "support_basis": "provider_transport + active_model_catalog",
                }
            )
            if adapter_type == "llm_task":
                details["endpoint_uri"] = resolve_api_endpoint(
                    row_provider,
                    model_slug=row_model,
                )
            adapter_support[adapter_type] = {
                **provider_transport,
                "supported": bool(provider_transport.get("supported")) and model_ok,
                "details": details,
            }
        models.append(
            {
                "provider_slug": row_provider,
                "model_slug": row_model,
                "capability_tags": row.get("capability_tags"),
                "route_tier": row.get("route_tier"),
                "latency_class": row.get("latency_class"),
                "adapter_support": adapter_support,
            }
        )

    provider_entries: list[dict[str, Any]] = []
    for slug in providers:
        report = provider_reports.get(slug)
        if report is None:
            continue
        provider_entries.append(
            {
                "provider_slug": slug,
                **report,
                "transports": {
                    adapter_type: transport_checks[(slug, adapter_type)]
                    for adapter_type in ("cli_llm", "llm_task")
                    if (slug, adapter_type) in transport_checks
                },
            }
        )

    route_jobs: list[dict[str, Any]] = []
    route_overall = "ready"
    if jobs is not None:
        from runtime.task_type_router import TaskTypeRouter

        router = TaskTypeRouter(pg)
        for index, raw_job in enumerate(jobs):
            label = str(raw_job.get("label") or f"job_{index + 1}")
            agent = str(raw_job.get("agent") or "").strip()
            if not agent:
                route_jobs.append(
                    {
                        "label": label,
                        "agent": "",
                        "status": "blocked",
                        "message": "Job is missing an agent route.",
                    }
                )
                route_overall = "blocked"
                continue
            if agent == "human" or agent.startswith("integration/"):
                route_jobs.append(
                    {
                        "label": label,
                        "agent": agent,
                        "status": "info",
                        "message": "Direct step does not require runtime lane resolution.",
                    }
                )
                continue
            if not agent.startswith("auto/"):
                route_jobs.append(
                    {
                        "label": label,
                        "agent": agent,
                        "status": "ready",
                        "resolved_agent": agent,
                        "message": "Explicit route bypasses auto lane resolution.",
                    }
                )
                continue
            try:
                chain = router.resolve_failover_chain(
                    agent,
                    runtime_profile_ref=runtime_profile_ref,
                )
                primary = chain[0]
                route_jobs.append(
                    {
                        "label": label,
                        "agent": agent,
                        "status": "ready",
                        "resolved_agent": f"{primary.provider_slug}/{primary.model_slug}",
                        "message": primary.rationale or "Auto route resolved successfully.",
                        "chain": [
                            {
                                "provider_slug": decision.provider_slug,
                                "model_slug": decision.model_slug,
                                "rank": decision.rank,
                                "rationale": decision.rationale,
                                "adapter_type": getattr(decision, "adapter_type", ""),
                                "billing_mode": getattr(decision, "billing_mode", ""),
                                "budget_bucket": getattr(decision, "budget_bucket", ""),
                                "effective_marginal_cost": getattr(
                                    decision,
                                    "effective_marginal_cost",
                                    0.0,
                                ),
                                "spend_pressure": getattr(decision, "spend_pressure", ""),
                                "budget_status": getattr(decision, "budget_status", ""),
                            }
                            for decision in chain
                        ],
                    }
                )
            except Exception as exc:
                route_jobs.append(
                    {
                        "label": label,
                        "agent": agent,
                        "status": "blocked",
                        "message": str(exc),
                    }
                )
                route_overall = "blocked"

    return {
        "default_provider_slug": default_provider_slug(),
        "default_adapter_type": default_llm_adapter_type(),
        "providers": provider_entries,
        "models": models,
        "route_preflight": {
            "runtime_profile_ref": runtime_profile_ref,
            "overall": route_overall,
            "jobs": route_jobs,
        },
        "count": {
            "providers": len(provider_entries),
            "models": len(models),
        },
        "support_basis": "provider_execution_registry + provider_model_candidates + transport probes",
    }
