"""Bounded native default-path pilot over reviewed route, class, and window seams.

This module composes three reviewed authorities into one explicit pilot path:

- provider-route runtime admission
- workflow-class and lane-policy resolution
- recurring scheduler-window authority

The pilot is intentionally narrow. It does not broaden the platform default,
invent fallback routing, or infer missing state from wrapper folklore.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any, TypeVar

import asyncpg

from registry.provider_execution_registry import (
    ProviderAdapterContract,
    resolve_adapter_contract,
)
from authority.workflow_class_resolution import (
    WorkflowClassResolutionDecision,
    WorkflowClassResolutionRuntime,
    load_workflow_class_resolution_runtime,
)
from registry.domain import RuntimeProfile
from registry.endpoint_failover import (
    ProviderEndpointAuthoritySelector,
    ProviderEndpointBindingAuthorityRecord,
    ProviderFailoverAndEndpointAuthority,
    ProviderFailoverAndEndpointAuthorityRepositoryError,
    ProviderFailoverAuthoritySelector,
    ProviderFailoverBindingAuthorityRecord,
    load_provider_failover_and_endpoint_authority,
)
from registry.provider_routing import (
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
    load_provider_route_authority,
)
from runtime.provider_route_runtime import (
    ProviderRouteRuntimeError,
    ProviderRouteRuntimeResolution,
    resolve_provider_route_runtime,
)
from runtime._helpers import _fail as _shared_fail
from runtime.scheduler_window_repository import (
    SchedulerWindowAuthorityCatalog,
    SchedulerWindowAuthorityResolution,
    load_scheduler_window_authority,
)


TSourceWindowRecord = TypeVar(
    "TSourceWindowRecord",
    ProviderRouteHealthWindowAuthorityRecord,
    ProviderBudgetWindowAuthorityRecord,
)
TAuthoritySliceRecord = TypeVar(
    "TAuthoritySliceRecord",
    ProviderFailoverBindingAuthorityRecord,
    ProviderEndpointBindingAuthorityRecord,
)

_DEFAULT_PATH_PILOT_BINDING_SCOPE = "native_runtime"
_DEFAULT_PATH_PILOT_ENDPOINT_KIND = "chat_completions"
_FIRST_PARTY_PROVIDER_ADAPTER = "llm_task"
_FIRST_PARTY_HTTP_TRANSPORT_KINDS = frozenset({"http", "https"})


class DefaultPathPilotError(RuntimeError):
    """Raised when the bounded default-path pilot cannot be admitted safely."""

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


_fail = partial(_shared_fail, error_type=DefaultPathPilotError)


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "default_path_pilot.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(
            "default_path_pilot.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "default_path_pilot.invalid_request",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _json_mapping(value: object, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, Mapping):
            return dict(decoded)
    raise _fail(
        "default_path_pilot.invalid_authority_payload",
        f"{field_name} must be a JSON object",
        details={"field": field_name, "value_type": type(value).__name__},
    )


def _request_timeout_seconds(
    *,
    request_policy: Mapping[str, Any],
    default_seconds: int,
) -> int:
    raw_timeout_ms = request_policy.get("timeout_ms")
    try:
        timeout_ms = int(raw_timeout_ms)
    except (TypeError, ValueError):
        timeout_ms = 0
    if timeout_ms <= 0:
        return default_seconds
    return max(1, (timeout_ms + 999) // 1000)


def _normalized_contract_failure_codes(
    codes: tuple[str, ...],
    *,
    field_name: str,
) -> tuple[str, ...]:
    normalized_codes = tuple(
        _require_text(code, field_name=f"{field_name}[{index}]")
        for index, code in enumerate(codes)
    )
    if not normalized_codes:
        raise _fail(
            "default_path_pilot.adapter_contract_incompatible",
            "first-party provider adapter contract must declare explicit failover failure codes",
            details={"field": field_name},
        )
    return normalized_codes


def _pilot_runtime_profile(request: DefaultPathPilotRequest) -> RuntimeProfile:
    return RuntimeProfile(
        runtime_profile_ref=(
            "runtime_profile.default_path_pilot."
            f"{request.model_profile_id}."
            f"{request.provider_policy_id}"
        ),
        model_profile_id=request.model_profile_id,
        provider_policy_id=request.provider_policy_id,
    )


@dataclass(frozen=True, slots=True)
class DefaultPathPilotRequest:
    """Explicit request envelope for one bounded native default-path pilot."""

    model_profile_id: str
    provider_policy_id: str
    candidate_ref: str
    target_ref: str
    schedule_kind: str

    def normalized(self) -> "DefaultPathPilotRequest":
        return DefaultPathPilotRequest(
            model_profile_id=_require_text(
                self.model_profile_id,
                field_name="model_profile_id",
            ),
            provider_policy_id=_require_text(
                self.provider_policy_id,
                field_name="provider_policy_id",
            ),
            candidate_ref=_require_text(
                self.candidate_ref,
                field_name="candidate_ref",
            ),
            target_ref=_require_text(self.target_ref, field_name="target_ref"),
            schedule_kind=_require_text(
                self.schedule_kind,
                field_name="schedule_kind",
            ),
        )


@dataclass(frozen=True, slots=True)
class DefaultPathPilotRouteDecision:
    """Explicit routing admission evidence for the pilot path."""

    route_eligibility_state: RouteEligibilityStateAuthorityRecord
    provider_route_health_window: ProviderRouteHealthWindowAuthorityRecord
    provider_budget_window: ProviderBudgetWindowAuthorityRecord

    @property
    def route_eligibility_state_id(self) -> str:
        return self.route_eligibility_state.route_eligibility_state_id

    @property
    def eligibility_status(self) -> str:
        return self.route_eligibility_state.eligibility_status

    @property
    def reason_code(self) -> str:
        return self.route_eligibility_state.reason_code

    @property
    def decision_ref(self) -> str:
        return self.route_eligibility_state.decision_ref


@dataclass(frozen=True, slots=True)
class DefaultPathPilotFailoverDecision:
    """Explicit failover-slice evidence for the bounded pilot path."""

    provider_failover_bindings: tuple[ProviderFailoverBindingAuthorityRecord, ...]
    selected_provider_failover_binding: ProviderFailoverBindingAuthorityRecord

    @property
    def selected_provider_failover_binding_id(self) -> str:
        return self.selected_provider_failover_binding.provider_failover_binding_id

    @property
    def selected_candidate_ref(self) -> str:
        return self.selected_provider_failover_binding.candidate_ref


@dataclass(frozen=True, slots=True)
class DefaultPathPilotEndpointDecision:
    """Explicit endpoint-binding evidence for the bounded pilot path."""

    provider_endpoint_binding: ProviderEndpointBindingAuthorityRecord

    @property
    def provider_endpoint_binding_id(self) -> str:
        return self.provider_endpoint_binding.provider_endpoint_binding_id

    @property
    def endpoint_ref(self) -> str:
        return self.provider_endpoint_binding.endpoint_ref

    @property
    def endpoint_kind(self) -> str:
        return self.provider_endpoint_binding.endpoint_kind

    @property
    def endpoint_uri(self) -> str:
        return self.provider_endpoint_binding.endpoint_uri


@dataclass(frozen=True, slots=True)
class DefaultPathPilotResolution:
    """Resolved bounded native default-path pilot contract."""

    request: DefaultPathPilotRequest
    route: DefaultPathPilotRouteDecision
    route_runtime: ProviderRouteRuntimeResolution
    failover: DefaultPathPilotFailoverDecision
    endpoint: DefaultPathPilotEndpointDecision
    dispatch: WorkflowClassResolutionDecision
    schedule: SchedulerWindowAuthorityResolution
    as_of: datetime
    route_authority: str = "registry.provider_routing"
    failover_endpoint_authority: str = "registry.endpoint_failover"
    dispatch_authority: str = "authority.workflow_class_resolution"
    schedule_authority: str = "runtime.scheduler_window_repository"

    @property
    def workflow_class_id(self) -> str:
        return self.dispatch.workflow_class_id

    @property
    def workflow_lane_id(self) -> str:
        return self.dispatch.workflow_lane_id

    @property
    def recurring_run_window_id(self) -> str:
        return self.schedule.recurring_run_window_id

    @property
    def capacity_remaining(self) -> int | None:
        if self.schedule.capacity_limit is None:
            return None
        return self.schedule.capacity_limit - self.schedule.capacity_used

    @property
    def provider_slug(self) -> str:
        return self.route_runtime.route_decision.provider_slug

    @property
    def model_slug(self) -> str:
        return self.route_runtime.route_decision.model_slug

    def first_party_provider_adapter_contract(self) -> ProviderAdapterContract:
        contract = resolve_adapter_contract(
            self.provider_slug,
            _FIRST_PARTY_PROVIDER_ADAPTER,
        )
        if contract is None:
            raise _fail(
                "default_path_pilot.adapter_contract_missing",
                "missing first-party provider adapter contract for the bounded default-path runtime",
                details={
                    "provider_slug": self.provider_slug,
                    "adapter_type": _FIRST_PARTY_PROVIDER_ADAPTER,
                    "selected_candidate_ref": self.route_runtime.selected_candidate_ref,
                },
            )
        if contract.transport_kind != "http" or contract.execution_kind != "request":
            raise _fail(
                "default_path_pilot.adapter_contract_incompatible",
                "first-party provider adapter contract is not compatible with the bounded default-path runtime",
                details={
                    "provider_slug": self.provider_slug,
                    "adapter_type": contract.adapter_type,
                    "transport_kind": contract.transport_kind,
                    "execution_kind": contract.execution_kind,
                    "selected_candidate_ref": self.route_runtime.selected_candidate_ref,
                },
            )
        protocol_family = _require_text(
            contract.prompt_envelope.get("protocol_family"),
            field_name="provider_adapter_contract.prompt_envelope.protocol_family",
        )
        failover_failure_codes = _normalized_contract_failure_codes(
            contract.failover_failure_codes,
            field_name="provider_adapter_contract.failover_failure_codes",
        )
        retryable_failure_codes = _normalized_contract_failure_codes(
            contract.retryable_failure_codes,
            field_name="provider_adapter_contract.retryable_failure_codes",
        )
        invalid_failover_codes = tuple(
            code
            for code in failover_failure_codes
            if not code.startswith(f"{contract.failure_namespace}.")
        )
        if invalid_failover_codes:
            raise _fail(
                "default_path_pilot.adapter_contract_incompatible",
                "first-party provider adapter contract declared failover codes outside its failure namespace",
                details={
                    "provider_slug": self.provider_slug,
                    "adapter_type": contract.adapter_type,
                    "failure_namespace": contract.failure_namespace,
                    "protocol_family": protocol_family,
                    "invalid_failover_failure_codes": invalid_failover_codes,
                    "selected_candidate_ref": self.route_runtime.selected_candidate_ref,
                },
            )
        missing_retryability = tuple(
            code for code in failover_failure_codes if code not in retryable_failure_codes
        )
        if missing_retryability:
            raise _fail(
                "default_path_pilot.adapter_contract_incompatible",
                "first-party provider adapter contract declared failover codes that are not retry-observable",
                details={
                    "provider_slug": self.provider_slug,
                    "adapter_type": contract.adapter_type,
                    "protocol_family": protocol_family,
                    "missing_retryable_failure_codes": missing_retryability,
                    "selected_candidate_ref": self.route_runtime.selected_candidate_ref,
                },
            )

        endpoint_transport_kind = _require_text(
            self.endpoint.provider_endpoint_binding.transport_kind,
            field_name="endpoint.transport_kind",
        )
        if endpoint_transport_kind not in _FIRST_PARTY_HTTP_TRANSPORT_KINDS:
            raise _fail(
                "default_path_pilot.endpoint_transport_incompatible",
                "bounded default-path endpoint transport is not compatible with the first-party provider adapter contract",
                details={
                    "provider_slug": self.provider_slug,
                    "adapter_type": contract.adapter_type,
                    "contract_transport_kind": contract.transport_kind,
                    "endpoint_transport_kind": endpoint_transport_kind,
                    "provider_endpoint_binding_id": self.endpoint.provider_endpoint_binding_id,
                },
            )
        return contract

    def _route_runtime_payload(self) -> dict[str, Any]:
        return {
            "route_decision_id": self.route_runtime.route_decision_id,
            "selected_candidate_ref": self.route_runtime.selected_candidate_ref,
            "provider_ref": self.route_runtime.route_decision.provider_ref,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "balance_slot": self.route_runtime.route_decision.balance_slot,
            "decision_reason_code": self.route_runtime.route_decision.decision_reason_code,
            "allowed_candidate_refs": list(
                self.route_runtime.route_decision.allowed_candidate_refs
            ),
        }

    def to_llm_task_input_payload(self) -> dict[str, Any]:
        contract = self.first_party_provider_adapter_contract()
        request_policy = _json_mapping(
            self.endpoint.provider_endpoint_binding.request_policy,
            field_name="request_policy",
        )
        timeout_seconds = _request_timeout_seconds(
            request_policy=request_policy,
            default_seconds=contract.timeout_seconds,
        )
        return {
            "adapter_type": _FIRST_PARTY_PROVIDER_ADAPTER,
            "route_contract_required": True,
            "provider_slug": self.provider_slug,
            "model_slug": self.model_slug,
            "endpoint_uri": self.endpoint.endpoint_uri,
            "auth_ref": self.endpoint.provider_endpoint_binding.auth_ref,
            "timeout_seconds": timeout_seconds,
            "provider_adapter_contract": contract.to_contract(),
            "runtime_route": {
                **self._route_runtime_payload(),
                "failover_role": (
                    self.failover.selected_provider_failover_binding.failover_role
                ),
                "failover_trigger_rule": (
                    self.failover.selected_provider_failover_binding.trigger_rule
                ),
                "failover_position_index": (
                    self.failover.selected_provider_failover_binding.position_index
                ),
                "failover_slice_candidate_refs": [
                    binding.candidate_ref for binding in self.failover.provider_failover_bindings
                ],
                "endpoint_kind": self.endpoint.endpoint_kind,
                "endpoint_transport_kind": (
                    self.endpoint.provider_endpoint_binding.transport_kind
                ),
                "route_eligibility_state_id": self.route.route_eligibility_state_id,
                "selected_provider_failover_binding_id": (
                    self.failover.selected_provider_failover_binding_id
                ),
                "provider_endpoint_binding_id": self.endpoint.provider_endpoint_binding_id,
                "route_authority": self.route_authority,
                "failover_endpoint_authority": self.failover_endpoint_authority,
                "as_of": self.as_of.isoformat(),
            },
        }

    def to_first_party_runtime_contract(self) -> dict[str, Any]:
        contract = self.first_party_provider_adapter_contract()
        payload = self.to_json()
        payload["kind"] = "default_path_first_party_runtime_contract"
        payload["authorities"] = {
            **dict(payload["authorities"]),
            "provider_adapter": "registry.provider_execution_registry",
        }
        payload["route_runtime"] = self._route_runtime_payload()
        payload["provider_adapter_contract"] = contract.to_contract()
        payload["llm_task_input_payload"] = self.to_llm_task_input_payload()
        return payload

    def to_json(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "authorities": {
                "route": self.route_authority,
                "dispatch": self.dispatch_authority,
                "schedule": self.schedule_authority,
            },
            "request": {
                "model_profile_id": self.request.model_profile_id,
                "provider_policy_id": self.request.provider_policy_id,
                "candidate_ref": self.request.candidate_ref,
                "target_ref": self.request.target_ref,
                "schedule_kind": self.request.schedule_kind,
            },
            "route": {
                "route_eligibility_state_id": self.route.route_eligibility_state_id,
                "eligibility_status": self.route.eligibility_status,
                "reason_code": self.route.reason_code,
                "decision_ref": self.route.decision_ref,
                "health_window": {
                    "provider_route_health_window_id": (
                        self.route.provider_route_health_window.provider_route_health_window_id
                    ),
                    "health_status": self.route.provider_route_health_window.health_status,
                    "health_score": self.route.provider_route_health_window.health_score,
                    "observation_ref": self.route.provider_route_health_window.observation_ref,
                },
                "budget_window": {
                    "provider_budget_window_id": (
                        self.route.provider_budget_window.provider_budget_window_id
                    ),
                    "budget_status": self.route.provider_budget_window.budget_status,
                    "decision_ref": self.route.provider_budget_window.decision_ref,
                },
            },
            "failover_endpoint_authority": self.failover_endpoint_authority,
            "failover": {
                "binding_scope": _DEFAULT_PATH_PILOT_BINDING_SCOPE,
                "selected_provider_failover_binding_id": (
                    self.failover.selected_provider_failover_binding_id
                ),
                "selected_candidate_ref": self.failover.selected_candidate_ref,
                "failover_role": self.failover.selected_provider_failover_binding.failover_role,
                "trigger_rule": self.failover.selected_provider_failover_binding.trigger_rule,
                "position_index": self.failover.selected_provider_failover_binding.position_index,
                "slice_candidate_refs": [
                    binding.candidate_ref for binding in self.failover.provider_failover_bindings
                ],
                "decision_ref": self.failover.selected_provider_failover_binding.decision_ref,
            },
            "endpoint": {
                "binding_scope": self.endpoint.provider_endpoint_binding.binding_scope,
                "provider_endpoint_binding_id": self.endpoint.provider_endpoint_binding_id,
                "endpoint_ref": self.endpoint.endpoint_ref,
                "endpoint_kind": self.endpoint.endpoint_kind,
                "transport_kind": self.endpoint.provider_endpoint_binding.transport_kind,
                "endpoint_uri": self.endpoint.endpoint_uri,
                "auth_ref": self.endpoint.provider_endpoint_binding.auth_ref,
                "binding_status": self.endpoint.provider_endpoint_binding.binding_status,
                "request_policy": _json_mapping(
                    self.endpoint.provider_endpoint_binding.request_policy,
                    field_name="request_policy",
                ),
                "circuit_breaker_policy": _json_mapping(
                    self.endpoint.provider_endpoint_binding.circuit_breaker_policy,
                    field_name="circuit_breaker_policy",
                ),
                "decision_ref": self.endpoint.provider_endpoint_binding.decision_ref,
            },
            "dispatch": {
                "workflow_class_id": self.dispatch.workflow_class_id,
                "class_name": self.dispatch.class_name,
                "class_kind": self.dispatch.class_kind,
                "workflow_lane_id": self.dispatch.workflow_lane_id,
                "workflow_lane_policy_id": self.dispatch.workflow_lane_policy_id,
                "policy_scope": self.dispatch.policy_scope,
                "work_kind": self.dispatch.work_kind,
                "queue_shape": dict(self.dispatch.queue_shape),
                "throttle_policy": dict(self.dispatch.throttle_policy),
                "review_required": self.dispatch.review_required,
                "decision_ref": self.dispatch.decision_ref,
            },
            "schedule": {
                "schedule_definition_id": self.schedule.schedule_definition_id,
                "schedule_name": self.schedule.schedule_name,
                "schedule_kind": self.schedule.schedule_kind,
                "target_ref": self.schedule.target_ref,
                "recurring_run_window_id": self.schedule.recurring_run_window_id,
                "window_status": self.schedule.window_status,
                "capacity_limit": self.schedule.capacity_limit,
                "capacity_used": self.schedule.capacity_used,
                "capacity_remaining": self.capacity_remaining,
                "decision_ref": self.schedule.decision_ref,
            },
        }


def _matching_source_windows(
    records: tuple[TSourceWindowRecord, ...],
    *,
    source_window_refs: tuple[str, ...],
    record_id_field: str,
) -> tuple[TSourceWindowRecord, ...]:
    source_window_ref_set = set(source_window_refs)
    return tuple(
        record
        for record in records
        if getattr(record, record_id_field) in source_window_ref_set
    )


def _default_failover_selector(
    *,
    request: DefaultPathPilotRequest,
    as_of: datetime,
) -> ProviderFailoverAuthoritySelector:
    return ProviderFailoverAuthoritySelector(
        model_profile_id=request.model_profile_id,
        provider_policy_id=request.provider_policy_id,
        binding_scope=_DEFAULT_PATH_PILOT_BINDING_SCOPE,
        as_of=as_of,
    )


def _default_endpoint_selector(
    *,
    request: DefaultPathPilotRequest,
    candidate_ref: str,
    as_of: datetime,
) -> ProviderEndpointAuthoritySelector:
    return ProviderEndpointAuthoritySelector(
        provider_policy_id=request.provider_policy_id,
        candidate_ref=candidate_ref,
        binding_scope=_DEFAULT_PATH_PILOT_BINDING_SCOPE,
        endpoint_kind=_DEFAULT_PATH_PILOT_ENDPOINT_KIND,
        as_of=as_of,
    )


def _authority_slice_key(
    record: TAuthoritySliceRecord,
) -> tuple[datetime, datetime | None]:
    return (
        record.effective_from,
        record.effective_to,
    )


def _format_authority_slice_key(
    slice_key: tuple[datetime, datetime | None],
) -> str:
    effective_from, effective_to = slice_key
    return (
        f"effective_from={effective_from.isoformat()},"
        f"effective_to={'' if effective_to is None else effective_to.isoformat()}"
    )


def _select_latest_route_state(
    control_tower: ProviderRouteAuthority,
    *,
    request: DefaultPathPilotRequest,
    candidate_ref: str,
    as_of: datetime,
) -> RouteEligibilityStateAuthorityRecord:
    eligibility_states = control_tower.route_eligibility_states.get(
        candidate_ref,
        (),
    )
    route_eligibility_states = tuple(
        record
        for record in eligibility_states
        if record.model_profile_id == request.model_profile_id
        and record.provider_policy_id == request.provider_policy_id
        and record.evaluated_at <= as_of
    )
    if not route_eligibility_states:
        raise _fail(
            "default_path_pilot.route_state_missing",
            "missing provider-route eligibility state at or before as_of for the requested candidate",
            details={
                "model_profile_id": request.model_profile_id,
                "provider_policy_id": request.provider_policy_id,
                "candidate_ref": candidate_ref,
                "as_of": as_of.isoformat(),
            },
        )

    return max(
        route_eligibility_states,
        key=lambda record: (record.evaluated_at, record.route_eligibility_state_id),
    )


def _translate_route_runtime_failure(
    *,
    control_tower: ProviderRouteAuthority,
    request: DefaultPathPilotRequest,
    candidate_ref: str,
    as_of: datetime,
    error: ProviderRouteRuntimeError,
) -> DefaultPathPilotError:
    if error.reason_code == "provider_route_runtime.routing_failed":
        route_eligibility_state = _select_latest_route_state(
            control_tower,
            request=request,
            candidate_ref=candidate_ref,
            as_of=as_of,
        )
        if route_eligibility_state.eligibility_status != "eligible":
            return _fail(
                "default_path_pilot.route_ineligible",
                "latest provider-route eligibility state at or before as_of rejected the requested candidate",
                details={
                    "route_eligibility_state_id": (
                        route_eligibility_state.route_eligibility_state_id
                    ),
                    "eligibility_status": route_eligibility_state.eligibility_status,
                    "reason_code": route_eligibility_state.reason_code,
                    "evaluated_at": route_eligibility_state.evaluated_at.isoformat(),
                    "as_of": as_of.isoformat(),
                },
            )

    return _fail(
        "default_path_pilot.route_runtime_failed",
        "provider-route runtime seam rejected the requested bounded default-path candidate",
        details={
            "model_profile_id": request.model_profile_id,
            "provider_policy_id": request.provider_policy_id,
            "candidate_ref": candidate_ref,
            "requested_candidate_ref": request.candidate_ref,
            "as_of": as_of.isoformat(),
            "provider_route_runtime_reason_code": error.reason_code,
            "provider_route_runtime_details": dict(error.details),
        },
    )


def _translate_failover_and_endpoint_authority_failure(
    *,
    request: DefaultPathPilotRequest,
    candidate_ref: str,
    as_of: datetime,
    error: ProviderFailoverAndEndpointAuthorityRepositoryError,
) -> DefaultPathPilotError:
    details = {
        "model_profile_id": request.model_profile_id,
        "provider_policy_id": request.provider_policy_id,
        "candidate_ref": candidate_ref,
        "requested_candidate_ref": request.candidate_ref,
        "binding_scope": _DEFAULT_PATH_PILOT_BINDING_SCOPE,
        "endpoint_kind": _DEFAULT_PATH_PILOT_ENDPOINT_KIND,
        "as_of": as_of.isoformat(),
        **dict(error.details),
    }
    reason_code_map = {
        "endpoint_failover.failover_missing": (
            "default_path_pilot.failover_slice_missing",
            "missing active provider failover slice for the bounded default path",
        ),
        "endpoint_failover.ambiguous_failover_slice": (
            "default_path_pilot.failover_slice_ambiguous",
            "multiple active provider failover slices matched the bounded default path",
        ),
        "endpoint_failover.endpoint_missing": (
            "default_path_pilot.endpoint_binding_missing",
            "missing active provider endpoint binding for the bounded default path",
        ),
        "endpoint_failover.ambiguous_endpoint_slice": (
            "default_path_pilot.endpoint_binding_ambiguous",
            "multiple active provider endpoint bindings matched the bounded default path",
        ),
    }
    mapped = reason_code_map.get(error.reason_code)
    if mapped is not None:
        reason_code, message = mapped
        return _fail(reason_code, message, details=details)
    return _fail(
        "default_path_pilot.failover_endpoint_authority_failed",
        "provider failover and endpoint authority could not be resolved for the bounded default path",
        details={
            **details,
            "provider_failover_and_endpoint_reason_code": error.reason_code,
        },
    )


def _resolve_route_decision(
    *,
    control_tower: ProviderRouteAuthority,
    runtime_resolution: ProviderRouteRuntimeResolution,
    request: DefaultPathPilotRequest,
    authoritative_candidate_ref: str,
) -> DefaultPathPilotRouteDecision:
    if runtime_resolution.selected_candidate_ref != authoritative_candidate_ref:
        raise _fail(
            "default_path_pilot.route_candidate_mismatch",
            "provider-route runtime seam selected a different candidate than the adopted failover authority candidate",
            details={
                "model_profile_id": request.model_profile_id,
                "provider_policy_id": request.provider_policy_id,
                "requested_candidate_ref": request.candidate_ref,
                "authoritative_candidate_ref": authoritative_candidate_ref,
                "selected_candidate_ref": runtime_resolution.selected_candidate_ref,
                "route_decision_id": runtime_resolution.route_decision_id,
            },
        )

    route_eligibility_state = runtime_resolution.route_eligibility_state
    if route_eligibility_state.eligibility_status != "eligible":
        raise _fail(
            "default_path_pilot.route_ineligible",
            "provider-route runtime seam returned a non-eligible route state for the requested candidate",
            details={
                "route_eligibility_state_id": route_eligibility_state.route_eligibility_state_id,
                "eligibility_status": route_eligibility_state.eligibility_status,
                "reason_code": route_eligibility_state.reason_code,
                "evaluated_at": route_eligibility_state.evaluated_at.isoformat(),
                "as_of": runtime_resolution.as_of.isoformat(),
            },
        )

    candidate_health_windows = control_tower.provider_route_health_windows.get(
        runtime_resolution.selected_candidate_ref,
        (),
    )
    matching_health_windows = _matching_source_windows(
        candidate_health_windows,
        source_window_refs=route_eligibility_state.source_window_refs,
        record_id_field="provider_route_health_window_id",
    )
    if not matching_health_windows:
        raise _fail(
            "default_path_pilot.health_window_missing",
            "route eligibility state did not reference a matching provider health window",
            details={
                "candidate_ref": runtime_resolution.selected_candidate_ref,
                "route_eligibility_state_id": route_eligibility_state.route_eligibility_state_id,
                "source_window_refs": ",".join(route_eligibility_state.source_window_refs),
            },
        )
    if len(matching_health_windows) > 1:
        raise _fail(
            "default_path_pilot.health_window_ambiguous",
            "route eligibility state referenced more than one provider health window",
            details={
                "candidate_ref": runtime_resolution.selected_candidate_ref,
                "route_eligibility_state_id": route_eligibility_state.route_eligibility_state_id,
                "provider_route_health_window_ids": ",".join(
                    record.provider_route_health_window_id for record in matching_health_windows
                ),
            },
        )

    provider_budget_windows = control_tower.provider_budget_windows.get(
        request.provider_policy_id,
        (),
    )
    matching_budget_windows = _matching_source_windows(
        provider_budget_windows,
        source_window_refs=route_eligibility_state.source_window_refs,
        record_id_field="provider_budget_window_id",
    )
    if not matching_budget_windows:
        raise _fail(
            "default_path_pilot.budget_window_missing",
            "route eligibility state did not reference a matching provider budget window",
            details={
                "provider_policy_id": request.provider_policy_id,
                "route_eligibility_state_id": route_eligibility_state.route_eligibility_state_id,
                "source_window_refs": ",".join(route_eligibility_state.source_window_refs),
            },
        )
    if len(matching_budget_windows) > 1:
        raise _fail(
            "default_path_pilot.budget_window_ambiguous",
            "route eligibility state referenced more than one provider budget window",
            details={
                "provider_policy_id": request.provider_policy_id,
                "route_eligibility_state_id": route_eligibility_state.route_eligibility_state_id,
                "provider_budget_window_ids": ",".join(
                    record.provider_budget_window_id for record in matching_budget_windows
                ),
            },
        )

    provider_route_health_window = matching_health_windows[0]
    if provider_route_health_window.health_status != "healthy":
        raise _fail(
            "default_path_pilot.route_health_not_healthy",
            "provider-route pilot requires a healthy route window",
            details={
                "candidate_ref": runtime_resolution.selected_candidate_ref,
                "provider_route_health_window_id": (
                    provider_route_health_window.provider_route_health_window_id
                ),
                "health_status": provider_route_health_window.health_status,
            },
        )

    provider_budget_window = matching_budget_windows[0]
    if provider_budget_window.budget_status != "available":
        raise _fail(
            "default_path_pilot.provider_budget_unavailable",
            "provider-route pilot requires an available budget window",
            details={
                "provider_policy_id": request.provider_policy_id,
                "provider_budget_window_id": provider_budget_window.provider_budget_window_id,
                "budget_status": provider_budget_window.budget_status,
            },
        )

    return DefaultPathPilotRouteDecision(
        route_eligibility_state=route_eligibility_state,
        provider_route_health_window=provider_route_health_window,
        provider_budget_window=provider_budget_window,
    )


def _resolve_failover_decision(
    *,
    authority: ProviderFailoverAndEndpointAuthority,
    request: DefaultPathPilotRequest,
    as_of: datetime,
) -> DefaultPathPilotFailoverDecision:
    failover_selector = _default_failover_selector(request=request, as_of=as_of)
    failover_bindings = authority.resolve_provider_failover_bindings(
        selector=failover_selector
    )
    selected_position_index = failover_bindings[0].position_index
    selected_failover_bindings = tuple(
        binding
        for binding in failover_bindings
        if binding.position_index == selected_position_index
    )
    if len(selected_failover_bindings) != 1:
        raise _fail(
            "default_path_pilot.failover_selected_candidate_ambiguous",
            "active failover slice did not resolve one authoritative adopted candidate",
            details={
                "model_profile_id": request.model_profile_id,
                "provider_policy_id": request.provider_policy_id,
                "binding_scope": _DEFAULT_PATH_PILOT_BINDING_SCOPE,
                "as_of": as_of.isoformat(),
                "selected_position_index": selected_position_index,
                "provider_failover_binding_ids": ",".join(
                    binding.provider_failover_binding_id
                    for binding in selected_failover_bindings
                ),
            },
        )
    selected_failover_binding = selected_failover_bindings[0]
    if request.candidate_ref != selected_failover_binding.candidate_ref:
        raise _fail(
            "default_path_pilot.request_candidate_mismatch",
            "bounded default-path request named a different candidate than the active failover authority slice",
            details={
                "model_profile_id": request.model_profile_id,
                "provider_policy_id": request.provider_policy_id,
                "requested_candidate_ref": request.candidate_ref,
                "authoritative_candidate_ref": selected_failover_binding.candidate_ref,
                "binding_scope": _DEFAULT_PATH_PILOT_BINDING_SCOPE,
                "as_of": as_of.isoformat(),
                "slice_candidate_refs": ",".join(
                    binding.candidate_ref for binding in failover_bindings
                ),
            },
        )

    return DefaultPathPilotFailoverDecision(
        provider_failover_bindings=failover_bindings,
        selected_provider_failover_binding=selected_failover_binding,
    )


def _resolve_endpoint_decision(
    *,
    authority: ProviderFailoverAndEndpointAuthority,
    request: DefaultPathPilotRequest,
    failover: DefaultPathPilotFailoverDecision,
    as_of: datetime,
) -> DefaultPathPilotEndpointDecision:
    endpoint_selector = _default_endpoint_selector(
        request=request,
        candidate_ref=failover.selected_candidate_ref,
        as_of=as_of,
    )
    endpoint_binding = authority.resolve_endpoint_binding(selector=endpoint_selector)
    failover_slice_key = _authority_slice_key(failover.selected_provider_failover_binding)
    endpoint_slice_key = _authority_slice_key(endpoint_binding)
    if endpoint_slice_key != failover_slice_key:
        raise _fail(
            "default_path_pilot.failover_endpoint_slice_stale",
            "provider endpoint binding did not share the active failover effective slice",
            details={
                "model_profile_id": request.model_profile_id,
                "provider_policy_id": request.provider_policy_id,
                "candidate_ref": failover.selected_candidate_ref,
                "requested_candidate_ref": request.candidate_ref,
                "binding_scope": _DEFAULT_PATH_PILOT_BINDING_SCOPE,
                "endpoint_kind": endpoint_binding.endpoint_kind,
                "as_of": as_of.isoformat(),
                "failover_slice_key": _format_authority_slice_key(failover_slice_key),
                "endpoint_slice_key": _format_authority_slice_key(endpoint_slice_key),
            },
        )
    return DefaultPathPilotEndpointDecision(provider_endpoint_binding=endpoint_binding)


def _resolve_dispatch_and_schedule(
    *,
    request: DefaultPathPilotRequest,
    workflow_runtime: WorkflowClassResolutionRuntime,
    scheduler_authority: SchedulerWindowAuthorityCatalog,
) -> tuple[WorkflowClassResolutionDecision, SchedulerWindowAuthorityResolution]:
    schedule = scheduler_authority.resolve(
        target_ref=request.target_ref,
        schedule_kind=request.schedule_kind,
    )
    workflow_class = workflow_runtime.workflow_class_catalog.resolve_by_id(
        workflow_class_id=schedule.schedule_definition.workflow_class_id,
    )
    matching_lane_policies = tuple(
        record
        for record in workflow_runtime.lane_catalog.lane_policy_records
        if record.workflow_lane_id == workflow_class.workflow_lane_id
    )
    if not matching_lane_policies:
        raise _fail(
            "default_path_pilot.workflow_lane_policy_missing",
            "resolved workflow class pointed at a workflow lane with no active lane policy",
            details={
                "workflow_class_id": workflow_class.workflow_class_id,
                "workflow_lane_id": workflow_class.workflow_lane_id,
                "schedule_definition_id": schedule.schedule_definition_id,
            },
        )
    if len(matching_lane_policies) > 1:
        raise _fail(
            "default_path_pilot.workflow_lane_policy_ambiguous",
            "resolved workflow class pointed at a workflow lane with multiple active lane policies",
            details={
                "workflow_class_id": workflow_class.workflow_class_id,
                "workflow_lane_id": workflow_class.workflow_lane_id,
                "workflow_lane_policy_ids": ",".join(
                    record.workflow_lane_policy_id for record in matching_lane_policies
                ),
                "schedule_definition_id": schedule.schedule_definition_id,
            },
        )

    dispatch = WorkflowClassResolutionDecision(
        workflow_class=workflow_class.workflow_class,
        lane_policy=matching_lane_policies[0],
        as_of=workflow_runtime.as_of,
    )
    if schedule.schedule_definition.workflow_class_id != dispatch.workflow_class_id:
        raise _fail(
            "default_path_pilot.schedule_workflow_class_mismatch",
            "active schedule definition pointed at a different workflow class than the resolved pilot class",
            details={
                "schedule_definition_id": schedule.schedule_definition_id,
                "schedule_workflow_class_id": schedule.schedule_definition.workflow_class_id,
                "resolved_workflow_class_id": dispatch.workflow_class_id,
                "target_ref": request.target_ref,
                "schedule_kind": request.schedule_kind,
            },
        )
    if (
        schedule.capacity_limit is not None
        and schedule.capacity_used >= schedule.capacity_limit
    ):
        raise _fail(
            "default_path_pilot.window_capacity_exhausted",
            "active recurring run window has no remaining capacity for the pilot path",
            details={
                "schedule_definition_id": schedule.schedule_definition_id,
                "recurring_run_window_id": schedule.recurring_run_window_id,
                "capacity_limit": schedule.capacity_limit,
                "capacity_used": schedule.capacity_used,
            },
        )
    return dispatch, schedule


async def resolve_default_path_pilot(
    conn: asyncpg.Connection,
    *,
    request: DefaultPathPilotRequest,
    as_of: datetime,
) -> DefaultPathPilotResolution:
    """Resolve one bounded native default path through reviewed authority seams."""

    normalized_request = request.normalized()
    normalized_as_of = _normalize_as_of(as_of)
    runtime_profile = _pilot_runtime_profile(normalized_request)
    failover_selector = _default_failover_selector(
        request=normalized_request,
        as_of=normalized_as_of,
    )

    async with conn.transaction():
        try:
            failover_authority = await load_provider_failover_and_endpoint_authority(
                conn,
                failover_selectors=(failover_selector,),
            )
        except ProviderFailoverAndEndpointAuthorityRepositoryError as exc:
            raise _translate_failover_and_endpoint_authority_failure(
                request=normalized_request,
                candidate_ref=normalized_request.candidate_ref,
                as_of=normalized_as_of,
                error=exc,
            ) from exc
        failover = _resolve_failover_decision(
            authority=failover_authority,
            request=normalized_request,
            as_of=normalized_as_of,
        )
        try:
            endpoint_authority = await load_provider_failover_and_endpoint_authority(
                conn,
                endpoint_selectors=(
                    _default_endpoint_selector(
                        request=normalized_request,
                        candidate_ref=failover.selected_candidate_ref,
                        as_of=normalized_as_of,
                    ),
                ),
            )
        except ProviderFailoverAndEndpointAuthorityRepositoryError as exc:
            raise _translate_failover_and_endpoint_authority_failure(
                request=normalized_request,
                candidate_ref=failover.selected_candidate_ref,
                as_of=normalized_as_of,
                error=exc,
            ) from exc
        endpoint = _resolve_endpoint_decision(
            authority=endpoint_authority,
            request=normalized_request,
            failover=failover,
            as_of=normalized_as_of,
        )
        control_tower = await load_provider_route_authority(
            conn,
            model_profile_ids=(normalized_request.model_profile_id,),
            provider_policy_ids=(normalized_request.provider_policy_id,),
            candidate_refs=(failover.selected_candidate_ref,),
        )
        try:
            route_runtime_resolution = await resolve_provider_route_runtime(
                conn,
                runtime_profile=runtime_profile,
                as_of=normalized_as_of,
                preferred_candidate_ref=failover.selected_candidate_ref,
            )
        except ProviderRouteRuntimeError as exc:
            raise _translate_route_runtime_failure(
                control_tower=control_tower,
                request=normalized_request,
                candidate_ref=failover.selected_candidate_ref,
                as_of=normalized_as_of,
                error=exc,
            ) from exc
        route = _resolve_route_decision(
            control_tower=control_tower,
            runtime_resolution=route_runtime_resolution,
            request=normalized_request,
            authoritative_candidate_ref=failover.selected_candidate_ref,
        )
        workflow_runtime = await load_workflow_class_resolution_runtime(
            conn,
            as_of=normalized_as_of,
        )
        scheduler_authority = await load_scheduler_window_authority(
            conn,
            as_of=normalized_as_of,
        )

    if workflow_runtime.as_of != scheduler_authority.as_of:
        raise _fail(
            "default_path_pilot.snapshot_drifted",
            "workflow-class and scheduler-window authorities must share one as_of snapshot",
            details={
                "dispatch_as_of": workflow_runtime.as_of.isoformat(),
                "schedule_as_of": scheduler_authority.as_of.isoformat(),
            },
        )

    dispatch, schedule = _resolve_dispatch_and_schedule(
        request=normalized_request,
        workflow_runtime=workflow_runtime,
        scheduler_authority=scheduler_authority,
    )

    return DefaultPathPilotResolution(
        request=normalized_request,
        route=route,
        route_runtime=route_runtime_resolution,
        failover=failover,
        endpoint=endpoint,
        dispatch=dispatch,
        schedule=schedule,
        as_of=normalized_as_of,
    )


__all__ = [
    "DefaultPathPilotError",
    "DefaultPathPilotEndpointDecision",
    "DefaultPathPilotFailoverDecision",
    "DefaultPathPilotRequest",
    "DefaultPathPilotResolution",
    "DefaultPathPilotRouteDecision",
    "resolve_default_path_pilot",
]
