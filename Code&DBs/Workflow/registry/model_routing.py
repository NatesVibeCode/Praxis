"""Canonical model-routing authority.

This module resolves runtime-profile authority to explicit provider/model route
decisions without treating raw provider or model slugs as the authority
surface. The authority input is always:

- runtime-profile authority from ``registry.domain``
- canonical model-profile records
- canonical provider-policy records
- canonical provider/model candidate records
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Any

from .domain import RuntimeProfile
from .provider_routing import (
    ProviderRouteAuthority,
    RouteEligibilityStateAuthorityRecord,
)


class ModelRoutingError(RuntimeError):
    """Raised when model routing authority cannot be resolved safely."""

    def __init__(
        self,
        reason_code: str,
        details: str,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(details)
        self.reason_code = reason_code
        self.details = details
        self.metadata = dict(metadata or {})


@dataclass(frozen=True, slots=True)
class ProviderModelCandidateAuthorityRecord:
    """Canonical provider/model candidate behind an internal candidate ref."""

    candidate_ref: str
    provider_ref: str
    provider_slug: str
    model_slug: str
    provider_name: str | None = None
    priority: int = 100
    balance_weight: int = 1
    capability_tags: tuple[str, ...] = ()
    route_tier: str | None = None
    route_tier_rank: int | None = None
    latency_class: str | None = None
    latency_rank: int | None = None
    reasoning_control: Mapping[str, Any] | None = None
    task_affinities: Mapping[str, Any] | None = None
    benchmark_profile: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ModelProfileAuthorityRecord:
    """Canonical model-profile record for runtime routing."""

    model_profile_id: str
    candidate_refs: tuple[str, ...]
    default_candidate_ref: str | None = None


@dataclass(frozen=True, slots=True)
class ProviderPolicyAuthorityRecord:
    """Canonical provider policy that constrains candidate selection."""

    provider_policy_id: str
    allowed_provider_refs: tuple[str, ...]
    preferred_provider_ref: str | None = None
    provider_name: str | None = None


@dataclass(frozen=True, slots=True)
class RoutedModelCandidate:
    """Resolved concrete provider/model option admitted by authority."""

    candidate_ref: str
    provider_ref: str
    provider_slug: str
    model_slug: str
    priority: int
    balance_weight: int
    capability_tags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ModelRouteDecision:
    """Deterministic route decision for one runtime profile."""

    route_decision_id: str
    runtime_profile_ref: str
    model_profile_id: str
    provider_policy_id: str
    selected_candidate_ref: str
    provider_ref: str
    provider_slug: str
    model_slug: str
    balance_slot: int
    decision_reason_code: str
    allowed_candidates: tuple[RoutedModelCandidate, ...]

    @property
    def allowed_candidate_refs(self) -> tuple[str, ...]:
        return tuple(candidate.candidate_ref for candidate in self.allowed_candidates)


def _route_catalog_mapping(
    route_catalog: object,
    *,
    field_name: str,
) -> Mapping[str, Sequence[object]]:
    value = getattr(route_catalog, field_name, None)
    if not isinstance(value, Mapping):
        raise ModelRoutingError(
            "routing.invalid_authority",
            f"route catalog is missing authoritative {field_name}",
            metadata={"field": field_name},
        )
    return value


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ModelRoutingError(
            "routing.invalid_authority",
            f"{field_name} must be a non-empty string",
            metadata={"field": field_name},
        )
    return value.strip()


def _normalize_unique_refs(
    refs: Sequence[str],
    *,
    field_name: str,
) -> tuple[str, ...]:
    normalized: list[str] = []
    for index, ref in enumerate(refs):
        normalized.append(_require_text(ref, field_name=f"{field_name}[{index}]"))
    return tuple(dict.fromkeys(normalized))


def _require_lookup_identity(
    *,
    authority_kind: str,
    lookup_key: str,
    embedded_value: object,
    embedded_field_name: str,
) -> str:
    normalized_embedded_value = _require_text(
        embedded_value,
        field_name=embedded_field_name,
    )
    if normalized_embedded_value != lookup_key:
        raise ModelRoutingError(
            "routing.authority_key_mismatch",
            (
                f"{authority_kind} lookup key {lookup_key!r} does not match "
                f"{embedded_field_name}={normalized_embedded_value!r}"
            ),
            metadata={
                "authority_kind": authority_kind,
                "lookup_key": lookup_key,
                "embedded_field_name": embedded_field_name,
                "embedded_value": normalized_embedded_value,
            },
        )
    return normalized_embedded_value


def _serialize_model_profile_authority(
    profile: ModelProfileAuthorityRecord,
) -> dict[str, Any]:
    return {
        "candidate_refs": list(profile.candidate_refs),
        "default_candidate_ref": profile.default_candidate_ref,
        "model_profile_id": profile.model_profile_id,
    }


def _serialize_provider_policy_authority(
    policy: ProviderPolicyAuthorityRecord,
) -> dict[str, Any]:
    payload = {
        "allowed_provider_refs": list(policy.allowed_provider_refs),
        "preferred_provider_ref": policy.preferred_provider_ref,
        "provider_policy_id": policy.provider_policy_id,
    }
    if policy.provider_name is not None:
        payload["provider_name"] = policy.provider_name
    return payload


def _serialize_routed_candidate(candidate: RoutedModelCandidate) -> dict[str, Any]:
    return {
        "balance_weight": candidate.balance_weight,
        "candidate_ref": candidate.candidate_ref,
        "capability_tags": list(candidate.capability_tags),
        "model_slug": candidate.model_slug,
        "priority": candidate.priority,
        "provider_ref": candidate.provider_ref,
        "provider_slug": candidate.provider_slug,
    }


class ModelRouter:
    """Resolve canonical routing authority to concrete route decisions."""

    def __init__(
        self,
        *,
        route_catalog: object,
        route_authority: object | None = None,
    ) -> None:
        model_profile_records = self._model_profile_records_from_route_catalog(route_catalog)
        provider_policy_records = self._provider_policy_records_from_route_catalog(
            route_catalog,
        )
        candidate_records = self._candidate_records_from_route_catalog(route_catalog)
        if route_authority is None:
            raise ModelRoutingError(
                "routing.route_authority_missing",
                "model routing requires explicit route authority records",
            )
        route_authority_records = self._route_authority_records_from_route_authority(
            route_authority,
        )
        self._model_profile_records = {
            model_profile_id: tuple(records)
            for model_profile_id, records in (model_profile_records or {}).items()
        }
        self._provider_policy_records = {
            provider_policy_id: tuple(records)
            for provider_policy_id, records in (provider_policy_records or {}).items()
        }
        self._candidate_records = {
            candidate_ref: tuple(records)
            for candidate_ref, records in (candidate_records or {}).items()
        }
        if route_authority_records is None:
            self._route_authority_health_windows = {}
            self._route_authority_budget_windows = {}
            self._route_authority_eligibility_states = {}
        else:
            self._route_authority_health_windows = {
                candidate_ref: tuple(records)
                for candidate_ref, records in route_authority_records.provider_route_health_windows.items()
            }
            self._route_authority_budget_windows = {
                provider_policy_id: tuple(records)
                for provider_policy_id, records in route_authority_records.provider_budget_windows.items()
            }
            self._route_authority_eligibility_states = {
                candidate_ref: tuple(records)
                for candidate_ref, records in route_authority_records.route_eligibility_states.items()
            }

    @classmethod
    def from_route_catalog(
        cls,
        route_catalog: object,
        *,
        route_authority: object | None = None,
    ) -> "ModelRouter":
        """Build a router from canonical repository authority records."""

        return cls(route_catalog=route_catalog, route_authority=route_authority)

    @classmethod
    async def from_route_catalog_repository(
        cls,
        route_catalog_repository: object,
        *,
        model_profile_ids: Sequence[str] | None = None,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
        route_authority_repository: object | None = None,
    ) -> "ModelRouter":
        """Load canonical routing authority from a repository and build a router."""

        load_route_catalog = getattr(route_catalog_repository, "load_route_catalog", None)
        if not callable(load_route_catalog):
            raise ModelRoutingError(
                "routing.invalid_authority",
                "route catalog repository must provide an async load_route_catalog() method",
            )

        route_catalog = await load_route_catalog(
            model_profile_ids=model_profile_ids,
            provider_policy_ids=provider_policy_ids,
            candidate_refs=candidate_refs,
        )
        if route_authority_repository is None:
            raise ModelRoutingError(
                "routing.route_authority_missing",
                "route authority repository is required to build a model router",
            )
        load_route_authority = getattr(
            route_authority_repository,
            "load_provider_route_authority",
            None,
        )
        if not callable(load_route_authority):
            raise ModelRoutingError(
                "routing.invalid_authority",
                (
                    "route authority repository must provide an async "
                    "load_provider_route_authority() method"
                ),
            )
        route_authority = await load_route_authority(
            model_profile_ids=model_profile_ids,
            provider_policy_ids=provider_policy_ids,
            candidate_refs=candidate_refs,
        )
        return cls.from_route_catalog(route_catalog, route_authority=route_authority)

    @classmethod
    def _model_profile_records_from_route_catalog(
        cls,
        route_catalog: object,
    ) -> Mapping[str, Sequence[ModelProfileAuthorityRecord]]:
        profile_records = _route_catalog_mapping(
            route_catalog,
            field_name="model_profiles",
        )
        binding_records = _route_catalog_mapping(
            route_catalog,
            field_name="model_profile_candidate_bindings",
        )

        normalized_profile_records: dict[str, tuple[ModelProfileAuthorityRecord, ...]] = {}
        for model_profile_id, records in profile_records.items():
            profile = cls._select_one(
                reason_code="routing.model_profile_unknown",
                ref_name="model profile",
                ref_value=model_profile_id,
                candidates=records,
            )
            if not hasattr(profile, "model_profile_id"):
                raise ModelRoutingError(
                    "routing.invalid_authority",
                    f"model profile record type mismatch for ref={model_profile_id!r}",
                )
            resolved_model_profile_id = _require_lookup_identity(
                authority_kind="model profile",
                lookup_key=model_profile_id,
                embedded_value=getattr(profile, "model_profile_id"),
                embedded_field_name="model_profile_id",
            )

            bindings = binding_records.get(model_profile_id, ())
            if not bindings:
                raise ModelRoutingError(
                    "routing.profile_empty",
                    f"model profile {model_profile_id!r} does not admit any candidate refs",
                )

            normalized_candidate_refs: list[str] = []
            seen_candidate_refs: set[str] = set()
            seen_position_indexes: set[int] = set()
            for binding in sorted(
                bindings,
                key=lambda record: (
                    _require_text(
                        getattr(record, "position_index"),
                        field_name="position_index",
                    )
                    if isinstance(getattr(record, "position_index"), str)
                    else getattr(record, "position_index"),
                    _require_text(
                        getattr(record, "candidate_ref"),
                        field_name="candidate_ref",
                    ),
                    _require_text(
                        getattr(record, "model_profile_candidate_binding_id"),
                        field_name="model_profile_candidate_binding_id",
                    ),
                ),
            ):
                if not hasattr(binding, "model_profile_id") or not hasattr(binding, "candidate_ref"):
                    raise ModelRoutingError(
                        "routing.invalid_authority",
                        f"model profile candidate binding record type mismatch for ref={model_profile_id!r}",
                    )
                binding_model_profile_id = _require_lookup_identity(
                    authority_kind="model profile candidate binding",
                    lookup_key=model_profile_id,
                    embedded_value=getattr(binding, "model_profile_id"),
                    embedded_field_name="model_profile_id",
                )
                position_index = getattr(binding, "position_index")
                if not isinstance(position_index, int) or isinstance(position_index, bool):
                    raise ModelRoutingError(
                        "routing.invalid_authority",
                        "binding position_index must be a non-negative integer",
                        metadata={
                            "model_profile_id": binding_model_profile_id,
                            "field": "position_index",
                        },
                    )
                if position_index < 0:
                    raise ModelRoutingError(
                        "routing.invalid_authority",
                        "binding position_index must be a non-negative integer",
                        metadata={
                            "model_profile_id": binding_model_profile_id,
                            "field": "position_index",
                        },
                    )
                if position_index in seen_position_indexes:
                    raise ModelRoutingError(
                        "routing.binding_position_ambiguous",
                        (
                            f"model profile {model_profile_id!r} has duplicate "
                            f"binding position_index={position_index}"
                        ),
                    )
                seen_position_indexes.add(position_index)

                candidate_ref = _require_text(
                    getattr(binding, "candidate_ref"),
                    field_name="candidate_ref",
                )
                if candidate_ref in seen_candidate_refs:
                    raise ModelRoutingError(
                        "routing.binding_candidate_duplicate",
                        (
                            f"model profile {model_profile_id!r} binds candidate "
                            f"{candidate_ref!r} more than once"
                        ),
                    )
                seen_candidate_refs.add(candidate_ref)
                normalized_candidate_refs.append(candidate_ref)

            default_candidate_ref = getattr(profile, "default_candidate_ref", None)
            if default_candidate_ref is not None:
                default_candidate_ref = _require_text(
                    default_candidate_ref,
                    field_name="default_candidate_ref",
                )
                if default_candidate_ref not in normalized_candidate_refs:
                    raise ModelRoutingError(
                        "routing.default_candidate_unknown",
                        (
                            f"default candidate {default_candidate_ref!r} is not admitted by "
                            f"model profile {model_profile_id!r}"
                        ),
                    )

            normalized_profile_records[resolved_model_profile_id] = (
                ModelProfileAuthorityRecord(
                    model_profile_id=resolved_model_profile_id,
                    candidate_refs=tuple(normalized_candidate_refs),
                    default_candidate_ref=default_candidate_ref,
                ),
            )
        return normalized_profile_records

    @classmethod
    def _provider_policy_records_from_route_catalog(
        cls,
        route_catalog: object,
    ) -> Mapping[str, Sequence[ProviderPolicyAuthorityRecord]]:
        policy_records = _route_catalog_mapping(
            route_catalog,
            field_name="provider_policies",
        )
        candidate_records = _route_catalog_mapping(
            route_catalog,
            field_name="provider_model_candidates",
        )

        normalized_policy_records: dict[str, tuple[ProviderPolicyAuthorityRecord, ...]] = {}
        for provider_policy_id, records in policy_records.items():
            policy = cls._select_one(
                reason_code="routing.provider_policy_unknown",
                ref_name="provider policy",
                ref_value=provider_policy_id,
                candidates=records,
            )
            if not hasattr(policy, "provider_policy_id"):
                raise ModelRoutingError(
                    "routing.invalid_authority",
                    f"provider policy record type mismatch for ref={provider_policy_id!r}",
                )
            resolved_provider_policy_id = _require_lookup_identity(
                authority_kind="provider policy",
                lookup_key=provider_policy_id,
                embedded_value=getattr(policy, "provider_policy_id"),
                embedded_field_name="provider_policy_id",
            )
            allowed_provider_refs = _normalize_unique_refs(
                getattr(policy, "allowed_provider_refs", ()),
                field_name="allowed_provider_refs",
            )
            provider_name = getattr(policy, "provider_name", None)
            if not allowed_provider_refs and provider_name is not None:
                provider_name = _require_text(
                    provider_name,
                    field_name="provider_name",
                )
                derived_provider_refs = []
                seen_provider_refs: set[str] = set()
                for candidate_ref, candidate_rows in candidate_records.items():
                    candidate = cls._select_one(
                        reason_code="routing.candidate_unknown",
                        ref_name="provider/model candidate",
                        ref_value=candidate_ref,
                        candidates=candidate_rows,
                    )
                    if not hasattr(candidate, "provider_name") or not hasattr(candidate, "provider_ref"):
                        raise ModelRoutingError(
                            "routing.invalid_authority",
                            f"provider/model candidate record type mismatch for ref={candidate_ref!r}",
                        )
                    candidate_provider_name = _require_text(
                        getattr(candidate, "provider_name"),
                        field_name="provider_name",
                    )
                    if candidate_provider_name != provider_name:
                        continue
                    provider_ref = _require_text(
                        getattr(candidate, "provider_ref"),
                        field_name="provider_ref",
                    )
                    if provider_ref not in seen_provider_refs:
                        seen_provider_refs.add(provider_ref)
                        derived_provider_refs.append(provider_ref)
                allowed_provider_refs = tuple(derived_provider_refs)

            preferred_provider_ref = getattr(policy, "preferred_provider_ref", None)
            if preferred_provider_ref is not None:
                preferred_provider_ref = _require_text(
                    preferred_provider_ref,
                    field_name="preferred_provider_ref",
                )
                if preferred_provider_ref not in allowed_provider_refs:
                    raise ModelRoutingError(
                        "routing.preferred_provider_unknown",
                        (
                            f"preferred provider {preferred_provider_ref!r} is not admitted by "
                            f"provider policy {provider_policy_id!r}"
                        ),
                    )

            normalized_policy_records[resolved_provider_policy_id] = (
                ProviderPolicyAuthorityRecord(
                    provider_policy_id=resolved_provider_policy_id,
                    allowed_provider_refs=tuple(allowed_provider_refs),
                    preferred_provider_ref=preferred_provider_ref,
                    provider_name=provider_name,
                ),
            )
        return normalized_policy_records

    @classmethod
    def _candidate_records_from_route_catalog(
        cls,
        route_catalog: object,
    ) -> Mapping[str, Sequence[ProviderModelCandidateAuthorityRecord]]:
        candidate_records = _route_catalog_mapping(
            route_catalog,
            field_name="provider_model_candidates",
        )
        normalized_candidate_records: dict[str, tuple[ProviderModelCandidateAuthorityRecord, ...]] = {}
        for candidate_ref, records in candidate_records.items():
            candidate = cls._select_one(
                reason_code="routing.candidate_unknown",
                ref_name="provider/model candidate",
                ref_value=candidate_ref,
                candidates=records,
            )
            if not hasattr(candidate, "candidate_ref"):
                raise ModelRoutingError(
                    "routing.invalid_authority",
                    f"provider/model candidate record type mismatch for ref={candidate_ref!r}",
                )
            resolved_candidate_ref = _require_lookup_identity(
                authority_kind="provider/model candidate",
                lookup_key=candidate_ref,
                embedded_value=getattr(candidate, "candidate_ref"),
                embedded_field_name="candidate_ref",
            )
            priority = getattr(candidate, "priority")
            balance_weight = getattr(candidate, "balance_weight")
            if not isinstance(priority, int) or isinstance(priority, bool) or priority < 0:
                raise ModelRoutingError(
                    "routing.invalid_candidate_priority",
                    f"candidate {candidate_ref!r} must use a non-negative priority",
                )
            if not isinstance(balance_weight, int) or isinstance(balance_weight, bool) or balance_weight < 1:
                raise ModelRoutingError(
                    "routing.invalid_candidate_weight",
                    f"candidate {candidate_ref!r} must use a positive balance_weight",
                )
            normalized_candidate_records[resolved_candidate_ref] = (
                ProviderModelCandidateAuthorityRecord(
                    candidate_ref=resolved_candidate_ref,
                    provider_ref=_require_text(
                        getattr(candidate, "provider_ref"),
                        field_name="provider_ref",
                    ),
                    provider_slug=_require_text(
                        getattr(candidate, "provider_slug"),
                        field_name="provider_slug",
                    ),
                    model_slug=_require_text(
                        getattr(candidate, "model_slug"),
                        field_name="model_slug",
                    ),
                    provider_name=(
                        _require_text(
                            candidate.provider_name,
                            field_name="provider_name",
                        )
                        if getattr(candidate, "provider_name", None) is not None
                        else None
                    ),
                    priority=priority,
                    balance_weight=balance_weight,
                    capability_tags=_normalize_unique_refs(
                        getattr(candidate, "capability_tags"),
                        field_name="capability_tags",
                    ),
                ),
            )
        return normalized_candidate_records

    @staticmethod
    def _route_authority_mapping(
        route_authority: object,
        *,
        field_name: str,
    ) -> Mapping[str, Sequence[object]]:
        value = getattr(route_authority, field_name, None)
        if not isinstance(value, Mapping):
            raise ModelRoutingError(
                "routing.invalid_authority",
                f"route authority is missing authoritative {field_name}",
                metadata={"field": field_name},
            )
        return value

    @classmethod
    def _route_authority_records_from_route_authority(
        cls,
        route_authority: object,
    ) -> ProviderRouteAuthority:
        if not isinstance(route_authority, ProviderRouteAuthority):
            raise ModelRoutingError(
                "routing.invalid_authority",
                "route authority must be a ProviderRouteAuthority record",
            )

        health_records = cls._route_authority_mapping(
            route_authority,
            field_name="provider_route_health_windows",
        )
        budget_records = cls._route_authority_mapping(
            route_authority,
            field_name="provider_budget_windows",
        )
        eligibility_records = cls._route_authority_mapping(
            route_authority,
            field_name="route_eligibility_states",
        )
        return ProviderRouteAuthority(
            provider_route_health_windows={
                candidate_ref: tuple(records)
                for candidate_ref, records in health_records.items()
            },
            provider_budget_windows={
                provider_policy_id: tuple(records)
                for provider_policy_id, records in budget_records.items()
            },
            route_eligibility_states={
                candidate_ref: tuple(records)
                for candidate_ref, records in eligibility_records.items()
            },
        )

    @staticmethod
    def _select_one(
        *,
        reason_code: str,
        ref_name: str,
        ref_value: str,
        candidates: Sequence[object],
    ) -> object:
        if not candidates:
            raise ModelRoutingError(
                reason_code,
                f"missing authoritative {ref_name} for ref={ref_value!r}",
                metadata={"ref_name": ref_name, "ref_value": ref_value},
            )
        if len(candidates) > 1:
            raise ModelRoutingError(
                "routing.ambiguity",
                f"ambiguous authoritative {ref_name} for ref={ref_value!r}",
                metadata={"ref_name": ref_name, "ref_value": ref_value},
            )
        return candidates[0]

    def _resolve_model_profile(
        self,
        *,
        model_profile_id: str,
    ) -> ModelProfileAuthorityRecord:
        candidate = self._select_one(
            reason_code="routing.model_profile_unknown",
            ref_name="model profile",
            ref_value=model_profile_id,
            candidates=self._model_profile_records.get(model_profile_id, ()),
        )
        if not isinstance(candidate, ModelProfileAuthorityRecord):
            raise ModelRoutingError(
                "routing.invalid_authority",
                f"model profile record type mismatch for ref={model_profile_id!r}",
            )
        resolved_model_profile_id = _require_lookup_identity(
            authority_kind="model profile",
            lookup_key=model_profile_id,
            embedded_value=candidate.model_profile_id,
            embedded_field_name="model_profile_id",
        )
        normalized_candidate_refs = _normalize_unique_refs(
            candidate.candidate_refs,
            field_name="candidate_refs",
        )
        if not normalized_candidate_refs:
            raise ModelRoutingError(
                "routing.profile_empty",
                f"model profile {model_profile_id!r} does not admit any candidate refs",
            )
        default_candidate_ref = candidate.default_candidate_ref
        if default_candidate_ref is not None:
            default_candidate_ref = _require_text(
                default_candidate_ref,
                field_name="default_candidate_ref",
            )
            if default_candidate_ref not in normalized_candidate_refs:
                raise ModelRoutingError(
                    "routing.default_candidate_unknown",
                    (
                        f"default candidate {default_candidate_ref!r} is not admitted by "
                        f"model profile {model_profile_id!r}"
                    ),
                )
        return ModelProfileAuthorityRecord(
            model_profile_id=resolved_model_profile_id,
            candidate_refs=normalized_candidate_refs,
            default_candidate_ref=default_candidate_ref,
        )

    def _resolve_provider_policy(
        self,
        *,
        provider_policy_id: str,
    ) -> ProviderPolicyAuthorityRecord:
        candidate = self._select_one(
            reason_code="routing.provider_policy_unknown",
            ref_name="provider policy",
            ref_value=provider_policy_id,
            candidates=self._provider_policy_records.get(provider_policy_id, ()),
        )
        if not isinstance(candidate, ProviderPolicyAuthorityRecord):
            raise ModelRoutingError(
                "routing.invalid_authority",
                f"provider policy record type mismatch for ref={provider_policy_id!r}",
            )
        resolved_provider_policy_id = _require_lookup_identity(
            authority_kind="provider policy",
            lookup_key=provider_policy_id,
            embedded_value=candidate.provider_policy_id,
            embedded_field_name="provider_policy_id",
        )
        allowed_provider_refs = _normalize_unique_refs(
            candidate.allowed_provider_refs,
            field_name="allowed_provider_refs",
        )
        if not allowed_provider_refs:
            raise ModelRoutingError(
                "routing.provider_policy_empty",
                f"provider policy {provider_policy_id!r} does not admit any providers",
            )
        preferred_provider_ref = candidate.preferred_provider_ref
        if preferred_provider_ref is not None:
            preferred_provider_ref = _require_text(
                preferred_provider_ref,
                field_name="preferred_provider_ref",
            )
            if preferred_provider_ref not in allowed_provider_refs:
                raise ModelRoutingError(
                    "routing.preferred_provider_unknown",
                    (
                        f"preferred provider {preferred_provider_ref!r} is not admitted by "
                        f"provider policy {provider_policy_id!r}"
                    ),
                )
        return ProviderPolicyAuthorityRecord(
            provider_policy_id=resolved_provider_policy_id,
            allowed_provider_refs=allowed_provider_refs,
            preferred_provider_ref=preferred_provider_ref,
        )

    def _resolve_candidate(
        self,
        *,
        candidate_ref: str,
    ) -> ProviderModelCandidateAuthorityRecord:
        candidate = self._select_one(
            reason_code="routing.candidate_unknown",
            ref_name="provider/model candidate",
            ref_value=candidate_ref,
            candidates=self._candidate_records.get(candidate_ref, ()),
        )
        if not isinstance(candidate, ProviderModelCandidateAuthorityRecord):
            raise ModelRoutingError(
                "routing.invalid_authority",
                f"provider/model candidate type mismatch for ref={candidate_ref!r}",
            )
        resolved_candidate_ref = _require_lookup_identity(
            authority_kind="provider/model candidate",
            lookup_key=candidate_ref,
            embedded_value=candidate.candidate_ref,
            embedded_field_name="candidate_ref",
        )
        if candidate.priority < 0:
            raise ModelRoutingError(
                "routing.invalid_candidate_priority",
                f"candidate {candidate_ref!r} must use a non-negative priority",
            )
        if candidate.balance_weight < 1:
            raise ModelRoutingError(
                "routing.invalid_candidate_weight",
                f"candidate {candidate_ref!r} must use a positive balance_weight",
            )
        return ProviderModelCandidateAuthorityRecord(
            candidate_ref=resolved_candidate_ref,
            provider_ref=_require_text(candidate.provider_ref, field_name="provider_ref"),
            provider_slug=_require_text(candidate.provider_slug, field_name="provider_slug"),
            model_slug=_require_text(candidate.model_slug, field_name="model_slug"),
            provider_name=(
                _require_text(candidate.provider_name, field_name="provider_name")
                if getattr(candidate, "provider_name", None) is not None
                else None
            ),
            priority=candidate.priority,
            balance_weight=candidate.balance_weight,
            capability_tags=_normalize_unique_refs(
                candidate.capability_tags,
                field_name="capability_tags",
            ),
        )

    def _route_authority_window_refs(self) -> set[str]:
        window_refs: set[str] = set()
        for records in self._route_authority_health_windows.values():
            for record in records:
                window_refs.add(record.provider_route_health_window_id)
        for records in self._route_authority_budget_windows.values():
            for record in records:
                window_refs.add(record.provider_budget_window_id)
        return window_refs

    def _resolve_route_eligibility_state(
        self,
        *,
        runtime_profile: RuntimeProfile,
        candidate_ref: str,
    ) -> RouteEligibilityStateAuthorityRecord:
        records = self._route_authority_eligibility_states.get(candidate_ref, ())
        matching_records = [
            record
            for record in records
            if record.model_profile_id == runtime_profile.model_profile_id
            and record.provider_policy_id == runtime_profile.provider_policy_id
        ]
        if not matching_records:
            raise ModelRoutingError(
                "routing.route_eligibility_state_missing",
                "route authority did not contain a matching eligibility state for the requested candidate",
                metadata={
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                    "model_profile_id": runtime_profile.model_profile_id,
                    "provider_policy_id": runtime_profile.provider_policy_id,
                    "candidate_ref": candidate_ref,
                },
            )

        selected_record = max(
            matching_records,
            key=lambda record: (
                record.evaluated_at,
                record.decision_ref,
                record.route_eligibility_state_id,
            ),
        )
        if not selected_record.source_window_refs:
            raise ModelRoutingError(
                "routing.invalid_authority",
                (
                    "route eligibility state must cite the health and budget windows "
                    "that justified it"
                ),
                metadata={
                    "candidate_ref": candidate_ref,
                    "model_profile_id": runtime_profile.model_profile_id,
                    "provider_policy_id": runtime_profile.provider_policy_id,
                },
            )

        known_window_refs = self._route_authority_window_refs()
        unknown_window_refs = tuple(
            ref for ref in selected_record.source_window_refs if ref not in known_window_refs
        )
        if unknown_window_refs:
            raise ModelRoutingError(
                "routing.invalid_authority",
                "route eligibility state references unknown health or budget windows",
                metadata={
                    "candidate_ref": candidate_ref,
                    "unknown_window_refs": unknown_window_refs,
                },
            )

        return selected_record

    def resolve_candidates(
        self,
        *,
        runtime_profile: RuntimeProfile,
    ) -> tuple[RoutedModelCandidate, ...]:
        profile = self._resolve_model_profile(
            model_profile_id=runtime_profile.model_profile_id,
        )
        policy = self._resolve_provider_policy(
            provider_policy_id=runtime_profile.provider_policy_id,
        )
        allowed_provider_refs = set(policy.allowed_provider_refs)
        admitted_candidates: list[RoutedModelCandidate] = []
        for candidate_ref in profile.candidate_refs:
            candidate = self._resolve_candidate(candidate_ref=candidate_ref)
            if candidate.provider_ref not in allowed_provider_refs:
                continue
            route_eligibility_state = self._resolve_route_eligibility_state(
                runtime_profile=runtime_profile,
                candidate_ref=candidate_ref,
            )
            if route_eligibility_state.eligibility_status != "eligible":
                continue
            admitted_candidates.append(
                RoutedModelCandidate(
                    candidate_ref=candidate.candidate_ref,
                    provider_ref=candidate.provider_ref,
                    provider_slug=candidate.provider_slug,
                    model_slug=candidate.model_slug,
                    priority=candidate.priority,
                    balance_weight=candidate.balance_weight,
                    capability_tags=candidate.capability_tags,
                )
            )
        if not admitted_candidates:
            raise ModelRoutingError(
                "routing.no_allowed_candidates",
                (
                    f"runtime profile {runtime_profile.runtime_profile_ref!r} resolved to no "
                    "provider/model candidates after provider-policy filtering"
                ),
                metadata={
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                    "model_profile_id": runtime_profile.model_profile_id,
                    "provider_policy_id": runtime_profile.provider_policy_id,
                },
                )
        return tuple(admitted_candidates)

    @staticmethod
    def _choose_balanced_candidate(
        *,
        allowed_candidates: Sequence[RoutedModelCandidate],
        balance_slot: int,
    ) -> RoutedModelCandidate:
        total_weight = sum(candidate.balance_weight for candidate in allowed_candidates)
        slot = balance_slot % total_weight
        running_total = 0
        for candidate in allowed_candidates:
            running_total += candidate.balance_weight
            if slot < running_total:
                return candidate
        raise ModelRoutingError(
            "routing.selection_failed",
            "weighted routing selection did not produce a candidate",
        )

    @staticmethod
    def _build_route_decision_id(
        *,
        runtime_profile: RuntimeProfile,
        profile: ModelProfileAuthorityRecord,
        policy: ProviderPolicyAuthorityRecord,
        selected_candidate: RoutedModelCandidate,
        allowed_candidates: Sequence[RoutedModelCandidate],
        balance_slot: int,
        decision_reason_code: str,
    ) -> str:
        authority_payload = {
            "allowed_candidates": [
                _serialize_routed_candidate(candidate)
                for candidate in allowed_candidates
            ],
            "balance_slot": balance_slot,
            "decision_reason_code": decision_reason_code,
            "model_profile": _serialize_model_profile_authority(profile),
            "provider_policy": _serialize_provider_policy_authority(policy),
            "runtime_profile_ref": runtime_profile.runtime_profile_ref,
            "selected_candidate": _serialize_routed_candidate(selected_candidate),
        }
        authority_digest = sha256(
            json.dumps(
                authority_payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()[:16]
        return (
            "route_decision:"
            f"{runtime_profile.runtime_profile_ref}:"
            f"{selected_candidate.candidate_ref}:"
            f"{balance_slot}:"
            f"{authority_digest}"
        )

    def validate_route_decision(
        self,
        *,
        runtime_profile: RuntimeProfile,
        route_decision: ModelRouteDecision,
    ) -> ModelRouteDecision:
        if route_decision.runtime_profile_ref != runtime_profile.runtime_profile_ref:
            raise ModelRoutingError(
                "routing.route_mismatch",
                "route decision runtime profile does not match the admitted runtime profile",
                metadata={
                    "route_runtime_profile_ref": route_decision.runtime_profile_ref,
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                },
            )
        if route_decision.model_profile_id != runtime_profile.model_profile_id:
            raise ModelRoutingError(
                "routing.route_mismatch",
                "route decision model profile does not match the admitted runtime profile",
            )
        if route_decision.provider_policy_id != runtime_profile.provider_policy_id:
            raise ModelRoutingError(
                "routing.route_mismatch",
                "route decision provider policy does not match the admitted runtime profile",
            )

        profile = self._resolve_model_profile(
            model_profile_id=runtime_profile.model_profile_id,
        )
        policy = self._resolve_provider_policy(
            provider_policy_id=runtime_profile.provider_policy_id,
        )
        allowed_candidates = self.resolve_candidates(runtime_profile=runtime_profile)
        route_allowed_candidates = tuple(route_decision.allowed_candidates)
        if route_allowed_candidates != allowed_candidates:
            raise ModelRoutingError(
                "routing.route_forged",
                "route decision allowed candidates do not match admitted authority",
                metadata={
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                    "selected_candidate_ref": route_decision.selected_candidate_ref,
                },
            )

        admitted_by_ref = {
            candidate.candidate_ref: candidate
            for candidate in allowed_candidates
        }
        admitted_candidate = admitted_by_ref.get(route_decision.selected_candidate_ref)
        if admitted_candidate is None:
            raise ModelRoutingError(
                "routing.route_forged",
                "route decision selected_candidate_ref is not admitted by authority",
                metadata={
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                    "selected_candidate_ref": route_decision.selected_candidate_ref,
                },
            )
        if (
            route_decision.provider_ref,
            route_decision.provider_slug,
            route_decision.model_slug,
        ) != (
            admitted_candidate.provider_ref,
            admitted_candidate.provider_slug,
            admitted_candidate.model_slug,
        ):
            raise ModelRoutingError(
                "routing.route_forged",
                "route decision provider/model slugs do not match the admitted candidate",
                metadata={
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                    "selected_candidate_ref": route_decision.selected_candidate_ref,
                },
            )

        expected_route_decision_id = self._build_route_decision_id(
            runtime_profile=runtime_profile,
            profile=profile,
            policy=policy,
            selected_candidate=admitted_candidate,
            allowed_candidates=allowed_candidates,
            balance_slot=route_decision.balance_slot,
            decision_reason_code=route_decision.decision_reason_code,
        )
        if route_decision.route_decision_id != expected_route_decision_id:
            raise ModelRoutingError(
                "routing.route_forged",
                "route decision id does not match admitted authority",
                metadata={
                    "expected_route_decision_id": expected_route_decision_id,
                    "route_decision_id": route_decision.route_decision_id,
                },
            )
        return route_decision

    def decide_route(
        self,
        *,
        runtime_profile: RuntimeProfile,
        balance_slot: int = 0,
        preferred_candidate_ref: str | None = None,
    ) -> ModelRouteDecision:
        if balance_slot < 0:
            raise ModelRoutingError(
                "routing.balance_slot_invalid",
                "balance_slot must be a non-negative integer",
                metadata={"balance_slot": balance_slot},
            )

        profile = self._resolve_model_profile(
            model_profile_id=runtime_profile.model_profile_id,
        )
        policy = self._resolve_provider_policy(
            provider_policy_id=runtime_profile.provider_policy_id,
        )
        allowed_candidates = self.resolve_candidates(runtime_profile=runtime_profile)
        allowed_by_ref = {
            candidate.candidate_ref: candidate for candidate in allowed_candidates
        }

        decision_reason_code: str
        selected_candidate: RoutedModelCandidate
        if preferred_candidate_ref is not None:
            normalized_preferred_candidate_ref = _require_text(
                preferred_candidate_ref,
                field_name="preferred_candidate_ref",
            )
            if normalized_preferred_candidate_ref not in allowed_by_ref:
                raise ModelRoutingError(
                    "routing.preference_unknown",
                    (
                        f"preferred candidate {normalized_preferred_candidate_ref!r} is not "
                        "admitted by runtime-profile authority"
                    ),
                    metadata={
                        "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                        "preferred_candidate_ref": normalized_preferred_candidate_ref,
                    },
                )
            selected_candidate = allowed_by_ref[normalized_preferred_candidate_ref]
            decision_reason_code = "routing.preferred_candidate"
        else:
            selected_candidate = self._choose_balanced_candidate(
                allowed_candidates=allowed_candidates,
                balance_slot=balance_slot,
            )
            decision_reason_code = "routing.balance_slot"

        route_decision_id = self._build_route_decision_id(
            runtime_profile=runtime_profile,
            profile=profile,
            policy=policy,
            selected_candidate=selected_candidate,
            allowed_candidates=allowed_candidates,
            balance_slot=balance_slot,
            decision_reason_code=decision_reason_code,
        )
        route_decision = ModelRouteDecision(
            route_decision_id=route_decision_id,
            runtime_profile_ref=runtime_profile.runtime_profile_ref,
            model_profile_id=runtime_profile.model_profile_id,
            provider_policy_id=runtime_profile.provider_policy_id,
            selected_candidate_ref=selected_candidate.candidate_ref,
            provider_ref=selected_candidate.provider_ref,
            provider_slug=selected_candidate.provider_slug,
            model_slug=selected_candidate.model_slug,
            balance_slot=balance_slot,
            decision_reason_code=decision_reason_code,
            allowed_candidates=allowed_candidates,
        )
        return self.validate_route_decision(
            runtime_profile=runtime_profile,
            route_decision=route_decision,
        )


__all__ = [
    "ModelProfileAuthorityRecord",
    "ModelRouteDecision",
    "ModelRouter",
    "ModelRoutingError",
    "ProviderModelCandidateAuthorityRecord",
    "ProviderPolicyAuthorityRecord",
    "RoutedModelCandidate",
]
