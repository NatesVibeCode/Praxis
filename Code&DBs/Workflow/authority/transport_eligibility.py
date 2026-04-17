"""Canonical transport-eligibility authority for admin/operator surfaces."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from policy._authority_validation import (
    require_mapping as _shared_require_mapping,
    require_text as _shared_require_text,
)


class TransportEligibilityAuthorityError(RuntimeError):
    """Raised when transport-eligibility authority cannot be assembled safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _error(
    reason_code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> TransportEligibilityAuthorityError:
    return TransportEligibilityAuthorityError(reason_code, message, details=details)


def _require_text(value: object, *, field_name: str) -> str:
    return _shared_require_text(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="transport_eligibility.invalid_value",
        include_value_type=False,
    )


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    return _shared_require_mapping(
        value,
        field_name=field_name,
        error_factory=_error,
        reason_code="transport_eligibility.invalid_value",
        include_value_type=False,
        parse_json_strings=True,
    )


def _normalize_jobs(
    jobs: Sequence[Mapping[str, Any]] | None,
) -> tuple[Mapping[str, Any], ...] | None:
    if jobs is None:
        return None
    normalized: list[Mapping[str, Any]] = []
    for index, job in enumerate(jobs):
        normalized.append(
            _require_mapping(job, field_name=f"jobs[{index}]"),
        )
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class AdapterTransportSupportAuthorityRecord:
    """Canonical transport support for one provider/model adapter lane."""

    adapter_type: str
    supported: bool
    status: str
    message: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return {
            "supported": self.supported,
            "status": self.status,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class ProviderTransportEligibilityAuthorityRecord:
    """Canonical transport support snapshot for one provider."""

    provider_slug: str
    provider_report: Mapping[str, Any]
    transports: Mapping[str, AdapterTransportSupportAuthorityRecord]

    def to_json(self) -> dict[str, Any]:
        payload = dict(self.provider_report)
        payload["provider_slug"] = self.provider_slug
        payload["transports"] = {
            adapter_type: record.to_json()
            for adapter_type, record in self.transports.items()
        }
        return payload


@dataclass(frozen=True, slots=True)
class ModelTransportEligibilityAuthorityRecord:
    """Canonical transport support snapshot for one active provider/model row."""

    provider_slug: str
    model_slug: str
    capability_tags: object
    route_tier: str | None
    latency_class: str | None
    adapter_support: Mapping[str, AdapterTransportSupportAuthorityRecord]

    def to_json(self) -> dict[str, Any]:
        return {
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "capability_tags": self.capability_tags,
            "route_tier": self.route_tier,
            "latency_class": self.latency_class,
            "adapter_support": {
                adapter_type: record.to_json()
                for adapter_type, record in self.adapter_support.items()
            },
        }


@dataclass(frozen=True, slots=True)
class RoutePreflightJobAuthorityRecord:
    """Canonical route-preflight decision for one requested job."""

    label: str
    agent: str
    status: str
    message: str
    resolved_agent: str | None
    chain: tuple[Mapping[str, Any], ...]

    def to_json(self) -> dict[str, Any]:
        payload = {
            "label": self.label,
            "agent": self.agent,
            "status": self.status,
            "message": self.message,
        }
        if self.resolved_agent is not None:
            payload["resolved_agent"] = self.resolved_agent
        if self.chain:
            payload["chain"] = [dict(item) for item in self.chain]
        return payload


@dataclass(frozen=True, slots=True)
class TransportEligibilityAuthority:
    """Canonical operator-facing transport eligibility snapshot."""

    default_provider_slug: str
    default_adapter_type: str
    provider_records: tuple[ProviderTransportEligibilityAuthorityRecord, ...]
    model_records: tuple[ModelTransportEligibilityAuthorityRecord, ...]
    runtime_profile_ref: str
    route_preflight_overall: str
    route_preflight_jobs: tuple[RoutePreflightJobAuthorityRecord, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            "default_provider_slug": self.default_provider_slug,
            "default_adapter_type": self.default_adapter_type,
            "providers": [record.to_json() for record in self.provider_records],
            "models": [record.to_json() for record in self.model_records],
            "route_preflight": {
                "runtime_profile_ref": self.runtime_profile_ref,
                "overall": self.route_preflight_overall,
                "jobs": [record.to_json() for record in self.route_preflight_jobs],
            },
            "count": {
                "providers": len(self.provider_records),
                "models": len(self.model_records),
            },
            "support_basis": (
                "provider_execution_registry + provider_model_candidates + transport probes"
            ),
        }


def _adapter_transport_support_record(
    *,
    adapter_type: str,
    payload: Mapping[str, Any],
) -> AdapterTransportSupportAuthorityRecord:
    details = payload.get("details")
    return AdapterTransportSupportAuthorityRecord(
        adapter_type=_require_text(adapter_type, field_name="adapter_type"),
        supported=bool(payload.get("supported")),
        status=_require_text(payload.get("status"), field_name="status"),
        message=_require_text(payload.get("message"), field_name="message"),
        details=dict(_require_mapping(details or {}, field_name="details")),
    )


def load_transport_eligibility_authority(
    *,
    repository: Any,
    health_mod: Any,
    pg: Any,
    provider_filter: str | None = None,
    model_filter: str | None = None,
    runtime_profile_ref: str = "praxis",
    jobs: Sequence[Mapping[str, Any]] | None = None,
    provider_registry_mod: Any | None = None,
    task_type_router_factory: Callable[[Any], Any] | None = None,
) -> TransportEligibilityAuthority:
    """Assemble the canonical transport-eligibility authority snapshot."""

    normalized_provider_filter = _optional_text(
        provider_filter,
        field_name="provider_filter",
    )
    normalized_model_filter = _optional_text(
        model_filter,
        field_name="model_filter",
    )
    normalized_runtime_profile_ref = _require_text(
        runtime_profile_ref,
        field_name="runtime_profile_ref",
    )
    normalized_jobs = _normalize_jobs(jobs)

    if provider_registry_mod is None:
        from registry import provider_execution_registry as provider_registry_mod

    if task_type_router_factory is None:
        from runtime.task_type_router import TaskTypeRouter

        task_type_router_factory = TaskTypeRouter

    provider_reports = provider_registry_mod.validate_profiles()
    providers = sorted(provider_reports.keys())
    if normalized_provider_filter is not None:
        providers = [
            provider_slug
            for provider_slug in providers
            if provider_slug == normalized_provider_filter
        ]

    transport_checks: dict[tuple[str, str], AdapterTransportSupportAuthorityRecord] = {}
    for provider_slug in providers:
        for adapter_type in ("cli_llm", "llm_task"):
            probe = health_mod.ProviderTransportProbe(provider_slug, adapter_type).check()
            transport_checks[(provider_slug, adapter_type)] = _adapter_transport_support_record(
                adapter_type=adapter_type,
                payload={
                    "supported": probe.passed,
                    "status": probe.status or ("ok" if probe.passed else "failed"),
                    "message": probe.message or "",
                    "details": probe.details or {},
                },
            )

    model_rows = repository.list_active_transport_models(
        provider_slug=normalized_provider_filter,
        model_slug=normalized_model_filter,
    )
    model_records: list[ModelTransportEligibilityAuthorityRecord] = []
    for row in model_rows:
        row_provider = _require_text(row["provider_slug"], field_name="provider_slug")
        row_model = _require_text(row["model_slug"], field_name="model_slug")
        adapter_support: dict[str, AdapterTransportSupportAuthorityRecord] = {}
        for adapter_type in ("cli_llm", "llm_task"):
            provider_transport = transport_checks.get((row_provider, adapter_type))
            if provider_transport is None:
                continue
            model_supported = bool(
                provider_registry_mod.supports_model_adapter(
                    row_provider,
                    row_model,
                    adapter_type,
                )
            )
            details = dict(provider_transport.details)
            details.update(
                {
                    "model_slug": row_model,
                    "model_supported": model_supported,
                    "support_basis": "provider_transport + active_model_catalog",
                }
            )
            if adapter_type == "llm_task":
                details["endpoint_uri"] = provider_registry_mod.resolve_api_endpoint(
                    row_provider,
                    model_slug=row_model,
                )
            adapter_support[adapter_type] = AdapterTransportSupportAuthorityRecord(
                adapter_type=adapter_type,
                supported=provider_transport.supported and model_supported,
                status=provider_transport.status,
                message=provider_transport.message,
                details=details,
            )
        model_records.append(
            ModelTransportEligibilityAuthorityRecord(
                provider_slug=row_provider,
                model_slug=row_model,
                capability_tags=row.get("capability_tags"),
                route_tier=_optional_text(row.get("route_tier"), field_name="route_tier"),
                latency_class=_optional_text(
                    row.get("latency_class"),
                    field_name="latency_class",
                ),
                adapter_support=adapter_support,
            )
        )

    provider_records: list[ProviderTransportEligibilityAuthorityRecord] = []
    for provider_slug in providers:
        provider_report = provider_reports.get(provider_slug)
        if provider_report is None:
            continue
        provider_records.append(
            ProviderTransportEligibilityAuthorityRecord(
                provider_slug=provider_slug,
                provider_report=_require_mapping(
                    provider_report,
                    field_name=f"provider_reports[{provider_slug}]",
                ),
                transports={
                    adapter_type: transport_checks[(provider_slug, adapter_type)]
                    for adapter_type in ("cli_llm", "llm_task")
                    if (provider_slug, adapter_type) in transport_checks
                },
            )
        )

    route_preflight_jobs: list[RoutePreflightJobAuthorityRecord] = []
    route_preflight_overall = "ready"
    if normalized_jobs is not None:
        router = task_type_router_factory(pg)
        for index, raw_job in enumerate(normalized_jobs):
            label = _optional_text(raw_job.get("label"), field_name=f"jobs[{index}].label")
            agent = _optional_text(raw_job.get("agent"), field_name=f"jobs[{index}].agent")
            normalized_label = label or f"job_{index + 1}"
            normalized_agent = agent or ""
            if not normalized_agent:
                route_preflight_jobs.append(
                    RoutePreflightJobAuthorityRecord(
                        label=normalized_label,
                        agent="",
                        status="blocked",
                        message="Job is missing an agent route.",
                        resolved_agent=None,
                        chain=(),
                    )
                )
                route_preflight_overall = "blocked"
                continue
            if normalized_agent == "human" or normalized_agent.startswith("integration/"):
                route_preflight_jobs.append(
                    RoutePreflightJobAuthorityRecord(
                        label=normalized_label,
                        agent=normalized_agent,
                        status="info",
                        message="Direct step does not require runtime lane resolution.",
                        resolved_agent=None,
                        chain=(),
                    )
                )
                continue
            if not normalized_agent.startswith("auto/"):
                route_preflight_jobs.append(
                    RoutePreflightJobAuthorityRecord(
                        label=normalized_label,
                        agent=normalized_agent,
                        status="ready",
                        message="Explicit route bypasses auto lane resolution.",
                        resolved_agent=normalized_agent,
                        chain=(),
                    )
                )
                continue
            try:
                chain = router.resolve_failover_chain(
                    normalized_agent,
                    runtime_profile_ref=normalized_runtime_profile_ref,
                )
                primary = chain[0]
                route_preflight_jobs.append(
                    RoutePreflightJobAuthorityRecord(
                        label=normalized_label,
                        agent=normalized_agent,
                        status="ready",
                        message=primary.rationale or "Auto route resolved successfully.",
                        resolved_agent=f"{primary.provider_slug}/{primary.model_slug}",
                        chain=tuple(
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
                        ),
                    )
                )
            except Exception as exc:
                route_preflight_jobs.append(
                    RoutePreflightJobAuthorityRecord(
                        label=normalized_label,
                        agent=normalized_agent,
                        status="blocked",
                        message=str(exc),
                        resolved_agent=None,
                        chain=(),
                    )
                )
                route_preflight_overall = "blocked"

    return TransportEligibilityAuthority(
        default_provider_slug=_require_text(
            provider_registry_mod.default_provider_slug(),
            field_name="default_provider_slug",
        ),
        default_adapter_type=_require_text(
            provider_registry_mod.default_llm_adapter_type(),
            field_name="default_adapter_type",
        ),
        provider_records=tuple(provider_records),
        model_records=tuple(model_records),
        runtime_profile_ref=normalized_runtime_profile_ref,
        route_preflight_overall=route_preflight_overall,
        route_preflight_jobs=tuple(route_preflight_jobs),
    )


__all__ = [
    "AdapterTransportSupportAuthorityRecord",
    "ModelTransportEligibilityAuthorityRecord",
    "ProviderTransportEligibilityAuthorityRecord",
    "RoutePreflightJobAuthorityRecord",
    "TransportEligibilityAuthority",
    "TransportEligibilityAuthorityError",
    "load_transport_eligibility_authority",
]
