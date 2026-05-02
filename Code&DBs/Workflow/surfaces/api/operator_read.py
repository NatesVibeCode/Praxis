"""Merged operator read surface over canonical rows and repo-local flow entrypoints."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from authority.transport_eligibility import load_transport_eligibility_authority
from runtime.routing_economics import budget_spend_pressure
from storage.postgres import (
    PostgresProviderControlPlaneRepository,
    PostgresTransportEligibilityRepository,
)
from ._operator_repository import (
    NativeOperatorQueryError,
    NativeOperatorQueryFrontdoor,
    NativeOperatorQuerySnapshot,
    OperatorBugRecord,
    OperatorCutoverGateRecord,
    OperatorIssueRecord,
    OperatorRoadmapDependencyRecord,
    OperatorRoadmapItemRecord,
    OperatorRoadmapSemanticNeighborRecord,
    OperatorRoadmapTreeSnapshot,
    OperatorWorkflowRunPacketInspectionRecord,
    OperatorWorkflowRunObservabilitySummary,
    OperatorWorkItemCloseoutRecommendationRecord,
    query_operator_surface,
    query_issue_backlog,
    query_roadmap_tree,
)
from ._smoke_service import (
    NativeSelfHostedSmokeContract,
    load_native_self_hosted_smoke_contract,
    run_local_operator_flow,
    run_native_self_hosted_smoke,
)


@dataclass(slots=True)
class TransportSupportFrontdoor:
    """Thin repo-local frontdoor for transport-support inspection."""

    repository_factory: Callable[[Any], Any] | None = None
    provider_registry_mod: Any | None = None
    task_type_router_factory: Callable[[Any], Any] | None = None

    def _resolve_repository_factory(self) -> Callable[[Any], Any]:
        if self.repository_factory is not None:
            return self.repository_factory
        return PostgresTransportEligibilityRepository

    def query_transport_support(
        self,
        *,
        health_mod: Any,
        pg: Any,
        provider_filter: str | None = None,
        model_filter: str | None = None,
        runtime_profile_ref: str = "praxis",
        jobs: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Read transport support through the canonical authority + Postgres repository seam."""

        authority = load_transport_eligibility_authority(
            repository=self._resolve_repository_factory()(pg),
            health_mod=health_mod,
            pg=pg,
            provider_filter=provider_filter,
            model_filter=model_filter,
            runtime_profile_ref=runtime_profile_ref,
            jobs=jobs,
            provider_registry_mod=self.provider_registry_mod,
            task_type_router_factory=self.task_type_router_factory,
        )
        return authority.to_json()


_DEFAULT_TRANSPORT_SUPPORT_FRONTDOOR = TransportSupportFrontdoor()


@dataclass(slots=True)
class ProviderControlPlaneFrontdoor:
    """Thin repo-local frontdoor for provider control-plane read models."""

    repository_factory: Callable[[Any], Any] | None = None

    def _resolve_repository_factory(self) -> Callable[[Any], Any]:
        if self.repository_factory is not None:
            return self.repository_factory
        return PostgresProviderControlPlaneRepository

    def query_provider_control_plane(
        self,
        *,
        pg: Any,
        runtime_profile_ref: str,
        provider_slug: str | None = None,
        job_type: str | None = None,
        transport_type: str | None = None,
        model_slug: str | None = None,
    ) -> dict[str, Any]:
        repository = self._resolve_repository_factory()(pg)
        rows = repository.list_provider_control_plane_rows(
            runtime_profile_ref=runtime_profile_ref,
            provider_slug=provider_slug,
            job_type=job_type,
            transport_type=transport_type,
            model_slug=model_slug,
        )
        freshness = repository.get_projection_freshness(
            "projection.private_provider_control_plane_snapshot"
        )
        row_payloads = [_provider_control_plane_row_payload(row) for row in rows]
        return {
            "control_plane": "operator.provider_control_plane",
            "runtime_profile_ref": runtime_profile_ref,
            "filters": {
                "provider_slug": provider_slug,
                "job_type": job_type,
                "transport_type": transport_type,
                "model_slug": model_slug,
            },
            "rows": row_payloads,
            "capability_matrix": _provider_control_plane_capability_matrix(
                row_payloads
            ),
            "route_explanation": _provider_control_plane_route_explanation(
                row_payloads
            ),
            "projection_freshness": {
                "projection_ref": freshness.projection_ref,
                "freshness_status": freshness.freshness_status,
                "last_refreshed_at": (
                    freshness.last_refreshed_at.isoformat()
                    if hasattr(freshness.last_refreshed_at, "isoformat")
                    else freshness.last_refreshed_at
                ),
                "error_code": freshness.error_code,
                "error_detail": freshness.error_detail,
            },
            "levers": {
                "commands": [
                    "operator.circuit_override",
                    "operator.task_route_eligibility",
                    "operator.task_route_request",
                ],
                "queries": [
                    "operator.circuit_states",
                    "operator.circuit_history",
                    "operator.provider_control_plane",
                ],
            },
        }

    def query_circuit_states(
        self,
        *,
        pg: Any,
        provider_slug: str | None = None,
    ) -> dict[str, Any]:
        repository = self._resolve_repository_factory()(pg)
        rows = repository.list_provider_circuit_states(provider_slug=provider_slug)
        freshness = repository.get_projection_freshness("projection.circuit_breakers")
        return {
            "circuits": {
                row.provider_slug: {
                    "provider_slug": row.provider_slug,
                    "state": row.effective_state,
                    "runtime_state": row.runtime_state,
                    "manual_override": (
                        None
                        if row.manual_override_state is None
                        else {
                            "override_state": row.manual_override_state,
                            "rationale": row.manual_override_reason,
                        }
                    ),
                    "failure_count": row.failure_count,
                    "success_count": row.success_count,
                    "failure_threshold": row.failure_threshold,
                    "recovery_timeout_s": row.recovery_timeout_s,
                    "half_open_max_calls": row.half_open_max_calls,
                    "last_failure_at": (
                        row.last_failure_at.isoformat()
                        if hasattr(row.last_failure_at, "isoformat")
                        else row.last_failure_at
                    ),
                    "opened_at": (
                        row.opened_at.isoformat()
                        if hasattr(row.opened_at, "isoformat")
                        else row.opened_at
                    ),
                    "half_open_after": (
                        row.half_open_after.isoformat()
                        if hasattr(row.half_open_after, "isoformat")
                        else row.half_open_after
                    ),
                    "half_open_calls": row.half_open_calls,
                    "updated_at": (
                        row.updated_at.isoformat()
                        if hasattr(row.updated_at, "isoformat")
                        else row.updated_at
                    ),
                    "projection_ref": row.projection_ref,
                }
                for row in rows
            },
            "projection_freshness": {
                "projection_ref": freshness.projection_ref,
                "freshness_status": freshness.freshness_status,
                "last_refreshed_at": (
                    freshness.last_refreshed_at.isoformat()
                    if hasattr(freshness.last_refreshed_at, "isoformat")
                    else freshness.last_refreshed_at
                ),
                "error_code": freshness.error_code,
                "error_detail": freshness.error_detail,
            },
        }


def _iso_or_raw(value: object) -> object:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _provider_control_plane_row_payload(row: Any) -> dict[str, Any]:
    cost_posture = _provider_cost_posture_payload(row)
    return {
        "runtime_profile_ref": row.runtime_profile_ref,
        "job_type": row.job_type,
        "transport_type": row.transport_type,
        "adapter_type": row.adapter_type,
        "provider_slug": row.provider_slug,
        "model_slug": row.model_slug,
        "model_version": row.model_version,
        "cost_structure": row.cost_structure,
        "cost_metadata": dict(row.cost_metadata),
        "cost_posture": cost_posture,
        "route_rank": row.route_rank,
        "route_request": {
            "rank": row.route_rank,
            "temperature": row.route_temperature,
            "max_tokens": row.route_max_tokens,
            "reasoning_control": dict(row.route_reasoning_control),
            "request_contract_ref": row.route_request_contract_ref,
            "cache_policy": dict(row.route_cache_policy),
            "structured_output_policy": dict(row.route_structured_output_policy),
            "streaming_policy": dict(row.route_streaming_policy),
        },
        "control_enabled": row.control_enabled,
        "control_state": row.control_state,
        "control_scope": row.control_scope,
        "control_is_explicit": row.control_is_explicit,
        "control_reason_code": row.control_reason_code,
        "control_decision_ref": row.control_decision_ref,
        "control_operator_message": row.control_operator_message,
        "credential_availability_state": row.credential_availability_state,
        "credential_sources": list(row.credential_sources),
        "credential_observations": [
            dict(item) for item in row.credential_observations
        ],
        "mechanical_capability_state": row.mechanical_capability_state,
        "mechanical_is_runnable": row.mechanical_is_runnable,
        "capability_state": row.capability_state,
        "is_runnable": row.is_runnable,
        "effective_dispatch_state": row.effective_dispatch_state,
        "breaker_state": row.breaker_state,
        "manual_override_state": row.manual_override_state,
        "primary_removal_reason_code": row.primary_removal_reason_code,
        "removal_reasons": [dict(item) for item in row.removal_reasons],
        "candidate_ref": row.candidate_ref,
        "provider_ref": row.provider_ref,
        "source_refs": list(row.source_refs),
        "projected_at": _iso_or_raw(row.projected_at),
        "projection_ref": row.projection_ref,
    }


def _provider_cost_posture_payload(row: Any) -> dict[str, Any]:
    metadata = dict(getattr(row, "cost_metadata", {}) or {})
    budget_window = dict(getattr(row, "budget_window", {}) or {})
    return {
        "billing_mode": str(metadata.get("billing_mode") or getattr(row, "cost_structure", "") or ""),
        "budget_bucket": str(metadata.get("budget_bucket") or ""),
        "pricing_model": str(metadata.get("pricing_model") or ""),
        "effective_marginal_cost": metadata.get("effective_marginal_cost"),
        "prefer_prepaid": metadata.get("prefer_prepaid"),
        "allow_payg_fallback": metadata.get("allow_payg_fallback"),
        "budget_status": str(budget_window.get("budget_status") or metadata.get("budget_status") or ""),
        "spend_pressure": budget_spend_pressure(budget_window),
        "budget_window": budget_window,
    }


def _blocked_reason_codes(row: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    for item in row.get("removal_reasons") or ():
        if not isinstance(item, Mapping):
            continue
        reason_code = str(item.get("reason_code") or "").strip()
        if reason_code:
            reasons.append(reason_code)
    primary = str(row.get("primary_removal_reason_code") or "").strip()
    if primary and primary not in reasons:
        reasons.insert(0, primary)
    credential_state = str(row.get("credential_availability_state") or "").strip()
    if credential_state == "missing" and "credential.missing" not in reasons:
        reasons.append("credential.missing")
    if not reasons and not bool(row.get("is_runnable")):
        state = str(row.get("effective_dispatch_state") or "provider_route.blocked")
        reasons.append(state)
    return reasons


def _credential_observations(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in row.get("credential_observations") or []
        if isinstance(item, Mapping)
    ]


def _provider_control_plane_capability_matrix(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    matrix: list[dict[str, Any]] = []
    for row in rows:
        is_runnable = bool(row.get("is_runnable"))
        matrix.append(
            {
                "job_type": row.get("job_type"),
                "type": row.get("transport_type"),
                "transport_type": row.get("transport_type"),
                "adapter_type": row.get("adapter_type"),
                "provider": row.get("provider_slug"),
                "provider_slug": row.get("provider_slug"),
                "model": row.get("model_slug"),
                "model_slug": row.get("model_slug"),
                "model_version": row.get("model_version"),
                "cost_structure": row.get("cost_structure"),
                "cost_metadata": dict(row.get("cost_metadata") or {}),
                "cost_posture": dict(row.get("cost_posture") or {}),
                "route_rank": row.get("route_rank"),
                "effective_availability_state": (
                    "available" if is_runnable else "blocked"
                ),
                "blocked_reasons": [] if is_runnable else _blocked_reason_codes(row),
                "control_state": row.get("control_state"),
                "credential_availability_state": row.get(
                    "credential_availability_state"
                ),
                "credential_sources": list(row.get("credential_sources") or []),
                "credential_observations": _credential_observations(row),
                "breaker_state": row.get("breaker_state"),
                "manual_override_state": row.get("manual_override_state"),
                "candidate_ref": row.get("candidate_ref"),
                "provider_ref": row.get("provider_ref"),
                "source_refs": list(row.get("source_refs") or []),
                "projection_ref": row.get("projection_ref"),
                "projected_at": row.get("projected_at"),
            }
        )
    return matrix


def _provider_route_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str, str, str]:
    raw_rank = row.get("route_rank")
    try:
        rank = int(raw_rank) if raw_rank is not None else 999
    except (TypeError, ValueError):
        rank = 999
    return (
        str(row.get("job_type") or ""),
        rank,
        str(row.get("transport_type") or ""),
        str(row.get("provider_slug") or ""),
        str(row.get("model_slug") or ""),
    )


def _provider_control_plane_route_explanation(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    selected_by_job_type: set[str] = set()
    candidates: list[dict[str, Any]] = []
    for row in sorted(rows, key=_provider_route_sort_key):
        job_type = str(row.get("job_type") or "")
        is_runnable = bool(row.get("is_runnable"))
        selected = is_runnable and job_type not in selected_by_job_type
        if selected:
            selected_by_job_type.add(job_type)
        candidate = {
            "job_type": row.get("job_type"),
            "provider_slug": row.get("provider_slug"),
            "model_slug": row.get("model_slug"),
            "model_version": row.get("model_version"),
            "transport_type": row.get("transport_type"),
            "adapter_type": row.get("adapter_type"),
            "cost_structure": row.get("cost_structure"),
            "cost_metadata": dict(row.get("cost_metadata") or {}),
            "cost_posture": dict(row.get("cost_posture") or {}),
            "route_rank": row.get("route_rank"),
            "availability": "runnable" if is_runnable else "blocked",
            "available": is_runnable,
            "selected": selected,
            "removed_reasons": [] if is_runnable else _blocked_reason_codes(row),
            "removal_reasons": [
                dict(item)
                for item in row.get("removal_reasons") or []
                if isinstance(item, Mapping)
            ],
            "primary_removal_reason_code": row.get("primary_removal_reason_code"),
            "circuit_state": row.get("breaker_state"),
            "manual_override_state": row.get("manual_override_state"),
            "control_state": row.get("control_state"),
            "credential_availability_state": row.get(
                "credential_availability_state"
            ),
            "credential_sources": list(row.get("credential_sources") or []),
            "credential_observations": _credential_observations(row),
            "candidate_ref": row.get("candidate_ref"),
            "provider_ref": row.get("provider_ref"),
            "projection_ref": row.get("projection_ref"),
        }
        candidates.append(candidate)
    reason_counts: dict[str, int] = {}
    for candidate in candidates:
        for reason in candidate["removed_reasons"]:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    selected_routes = [candidate for candidate in candidates if candidate["selected"]]
    return {
        "candidates": candidates,
        "selected_routes": selected_routes,
        "blocked_reason_counts": dict(sorted(reason_counts.items())),
    }


_DEFAULT_PROVIDER_CONTROL_PLANE_FRONTDOOR = ProviderControlPlaneFrontdoor()


def build_transport_support_summary(
    transport_support_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Project canonical transport support into the lightweight summary health surfaces expect."""

    providers_payload = transport_support_payload.get("providers")
    provider_rows = (
        providers_payload
        if isinstance(providers_payload, Sequence) and not isinstance(providers_payload, (str, bytes, bytearray))
        else ()
    )
    registered_providers: list[str] = []
    providers: list[dict[str, Any]] = []
    policy_disabled_adapters: list[dict[str, Any]] = []
    probe_targets: list[tuple[str, str]] = []
    for raw_provider in provider_rows:
        if not isinstance(raw_provider, Mapping):
            continue
        provider_slug = str(raw_provider.get("provider_slug") or "").strip()
        if not provider_slug:
            continue
        transports = raw_provider.get("transports")
        transport_rows = transports if isinstance(transports, Mapping) else {}
        adapters: list[str] = []
        disabled_adapters: list[dict[str, Any]] = []
        for adapter_type, raw_support in transport_rows.items():
            if not isinstance(raw_support, Mapping):
                continue
            normalized_adapter = str(adapter_type or "").strip()
            if not normalized_adapter:
                continue
            raw_status = str(raw_support.get("status") or "").strip().lower()
            raw_details = raw_support.get("details")
            policy_reason = "adapter disabled by policy"
            if isinstance(raw_details, Mapping):
                policy_reason = str(raw_details.get("policy_reason") or policy_reason)
            if not bool(raw_support.get("supported")) and raw_status != "disabled_by_policy":
                continue
            if raw_status == "disabled_by_policy":
                policy_disabled_adapters.append(
                    {
                        "provider_slug": provider_slug,
                        "adapter_type": normalized_adapter,
                        "policy_reason": policy_reason,
                    }
                )
                disabled_adapters.append(
                    {
                        "adapter_type": normalized_adapter,
                        "status": "disabled_by_policy",
                        "policy_reason": policy_reason,
                    }
                )
                continue
            adapters.append(normalized_adapter)
            probe_targets.append((provider_slug, normalized_adapter))
        registered_providers.append(provider_slug)
        providers.append(
            {
                "provider_slug": provider_slug,
                "adapters": adapters,
                "disabled_adapters": disabled_adapters,
            }
        )
    return {
        "default_provider_slug": str(transport_support_payload.get("default_provider_slug") or "").strip(),
        "default_adapter_type": str(transport_support_payload.get("default_adapter_type") or "").strip(),
        "registered_providers": registered_providers,
        "providers": providers,
        "policy_disabled_adapters": policy_disabled_adapters,
        "probe_targets": tuple(probe_targets),
        "support_basis": str(transport_support_payload.get("support_basis") or "").strip() or None,
    }


def query_transport_support(
    *,
    health_mod: Any,
    pg: Any,
    provider_filter: str | None = None,
    model_filter: str | None = None,
    runtime_profile_ref: str = "praxis",
    jobs: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Read transport support through the canonical authority + Postgres repository seam."""

    return _DEFAULT_TRANSPORT_SUPPORT_FRONTDOOR.query_transport_support(
        health_mod=health_mod,
        pg=pg,
        provider_filter=provider_filter,
        model_filter=model_filter,
        runtime_profile_ref=runtime_profile_ref,
        jobs=jobs,
    )


__all__ = [
    "NativeSelfHostedSmokeContract",
    "NativeOperatorQueryError",
    "NativeOperatorQueryFrontdoor",
    "NativeOperatorQuerySnapshot",
    "OperatorBugRecord",
    "OperatorCutoverGateRecord",
    "OperatorIssueRecord",
    "OperatorRoadmapDependencyRecord",
    "OperatorRoadmapItemRecord",
    "OperatorRoadmapSemanticNeighborRecord",
    "OperatorWorkflowRunPacketInspectionRecord",
    "OperatorRoadmapTreeSnapshot",
    "OperatorWorkflowRunObservabilitySummary",
    "OperatorWorkItemCloseoutRecommendationRecord",
    "ProviderControlPlaneFrontdoor",
    "load_native_self_hosted_smoke_contract",
    "TransportSupportFrontdoor",
    "build_transport_support_summary",
    "query_transport_support",
    "query_issue_backlog",
    "query_roadmap_tree",
    "query_operator_surface",
    "run_local_operator_flow",
    "run_native_self_hosted_smoke",
]
