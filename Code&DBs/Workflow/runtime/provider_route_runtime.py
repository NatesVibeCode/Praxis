"""Bounded provider-route runtime wiring over control-tower authority.

This module keeps the runtime seam narrow:

- route catalog truth comes from Postgres-backed catalog rows
- route admission truth comes from Postgres-backed control-tower rows
- time semantics are explicit via one required ``as_of`` snapshot

It does not broaden route cutover, infer authority from slugs, or fall back to
an in-memory route-authority seam.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any

import asyncpg

from registry.domain import RuntimeProfile
from registry.endpoint_failover import (
    ProviderFailoverAndEndpointAuthorityRepositoryError,
    ProviderFailoverAuthoritySelector,
    ProviderFailoverBindingAuthorityRecord,
    load_provider_failover_and_endpoint_authority,
)
from registry.model_routing import (
    ModelProfileAuthorityRecord,
    ModelRouteDecision,
    ModelRouter,
    ModelRoutingError,
)
from registry.provider_routing import (
    RouteEligibilityStateAuthorityRecord,
    load_provider_route_authority_snapshot,
    select_route_eligibility_state,
)
from registry.route_catalog_repository import (
    ModelProfileCandidateBindingAuthorityRecord,
    PostgresRouteCatalogRepository,
    RouteCatalogAuthority,
)
from runtime._helpers import _fail as _shared_fail


class ProviderRouteRuntimeError(RuntimeError):
    """Raised when bounded provider-route runtime wiring cannot resolve safely."""

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


_fail = partial(_shared_fail, error_type=ProviderRouteRuntimeError)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(
            "provider_route_runtime.invalid_runtime_profile",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _normalize_runtime_profile(runtime_profile: RuntimeProfile) -> RuntimeProfile:
    if not isinstance(runtime_profile, RuntimeProfile):
        raise _fail(
            "provider_route_runtime.invalid_runtime_profile",
            "runtime_profile must be a RuntimeProfile",
            details={"value_type": type(runtime_profile).__name__},
        )
    return RuntimeProfile(
        runtime_profile_ref=_require_text(
            runtime_profile.runtime_profile_ref,
            field_name="runtime_profile_ref",
        ),
        model_profile_id=_require_text(
            runtime_profile.model_profile_id,
            field_name="model_profile_id",
        ),
        provider_policy_id=_require_text(
            runtime_profile.provider_policy_id,
            field_name="provider_policy_id",
        ),
    )


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(
            "provider_route_runtime.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(
            "provider_route_runtime.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


def _normalize_balance_slot(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise _fail(
            "provider_route_runtime.invalid_balance_slot",
            "balance_slot must be a non-negative integer",
            details={"value": value},
        )
    return value


def _normalize_preferred_candidate_ref(value: str | None) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name="preferred_candidate_ref")


def _normalize_failover_binding_scope(value: str | None) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name="failover_binding_scope")


def _select_candidate_refs(
    model_profile_records: tuple[object, ...],
    *,
    model_profile_id: str,
) -> tuple[str, ...]:
    if not model_profile_records:
        raise _fail(
            "provider_route_runtime.model_profile_unknown",
            "runtime profile model_profile_id is missing from the route catalog",
            details={"model_profile_id": model_profile_id},
        )
    if len(model_profile_records) > 1:
        raise _fail(
            "provider_route_runtime.model_profile_ambiguous",
            "runtime profile model_profile_id resolved to multiple route catalog rows",
            details={"model_profile_id": model_profile_id},
        )
    record = model_profile_records[0]
    candidate_refs = getattr(record, "candidate_refs", None)
    if not isinstance(candidate_refs, tuple):
        raise _fail(
            "provider_route_runtime.invalid_route_catalog",
            "route catalog model profile record is missing candidate_refs",
            details={"model_profile_id": model_profile_id},
        )
    return tuple(
        _require_text(candidate_ref, field_name="candidate_ref")
        for candidate_ref in candidate_refs
    )


def _translate_failover_authority_failure(
    error: ProviderFailoverAndEndpointAuthorityRepositoryError,
    *,
    runtime_profile: RuntimeProfile,
    binding_scope: str,
    as_of: datetime,
) -> ProviderRouteRuntimeError:
    details = {
        "runtime_profile_ref": runtime_profile.runtime_profile_ref,
        "model_profile_id": runtime_profile.model_profile_id,
        "provider_policy_id": runtime_profile.provider_policy_id,
        "binding_scope": binding_scope,
        "as_of": as_of.isoformat(),
        **dict(error.details),
    }
    mapped = {
        "endpoint_failover.failover_missing": (
            "provider_route_runtime.failover_slice_missing",
            "missing active provider failover slice for the bounded route-runtime selector",
        ),
        "endpoint_failover.ambiguous_failover_slice": (
            "provider_route_runtime.failover_slice_ambiguous",
            "multiple active provider failover slices matched the bounded route-runtime selector",
        ),
    }.get(error.reason_code)
    if mapped is not None:
        reason_code, message = mapped
        return _fail(reason_code, message, details=details)
    return _fail(
        "provider_route_runtime.failover_authority_failed",
        "provider failover authority could not be resolved for the bounded route-runtime selector",
        details={
            **details,
            "provider_failover_and_endpoint_reason_code": error.reason_code,
        },
    )


def _format_failover_slice_key(
    binding: ProviderFailoverBindingAuthorityRecord,
) -> str:
    return (
        f"effective_from={binding.effective_from.isoformat()},"
        f"effective_to={'' if binding.effective_to is None else binding.effective_to.isoformat()},"
        f"decision_ref={binding.decision_ref}"
    )


def _ordered_failover_bindings(
    bindings: tuple[ProviderFailoverBindingAuthorityRecord, ...],
    *,
    runtime_profile: RuntimeProfile,
    binding_scope: str,
    as_of: datetime,
) -> tuple[ProviderFailoverBindingAuthorityRecord, ...]:
    ordered_bindings = tuple(
        sorted(
            bindings,
            key=lambda record: (
                record.position_index,
                record.candidate_ref,
                record.provider_failover_binding_id,
            ),
        )
    )
    seen_position_indexes: set[int] = set()
    seen_candidate_refs: set[str] = set()
    for binding in ordered_bindings:
        if binding.position_index in seen_position_indexes:
            raise _fail(
                "provider_route_runtime.failover_slice_ambiguous",
                "active provider failover slice assigned one position index to multiple candidates",
                details={
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                    "model_profile_id": runtime_profile.model_profile_id,
                    "provider_policy_id": runtime_profile.provider_policy_id,
                    "binding_scope": binding_scope,
                    "as_of": as_of.isoformat(),
                    "position_index": binding.position_index,
                },
            )
        if binding.candidate_ref in seen_candidate_refs:
            raise _fail(
                "provider_route_runtime.failover_slice_ambiguous",
                "active provider failover slice bound the same candidate more than once",
                details={
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                    "model_profile_id": runtime_profile.model_profile_id,
                    "provider_policy_id": runtime_profile.provider_policy_id,
                    "binding_scope": binding_scope,
                    "as_of": as_of.isoformat(),
                    "candidate_ref": binding.candidate_ref,
                },
            )
        seen_position_indexes.add(binding.position_index)
        seen_candidate_refs.add(binding.candidate_ref)
    return ordered_bindings


def _narrow_route_catalog_to_failover_slice(
    route_catalog: RouteCatalogAuthority,
    *,
    runtime_profile: RuntimeProfile,
    failover_bindings: tuple[ProviderFailoverBindingAuthorityRecord, ...],
    binding_scope: str,
    as_of: datetime,
) -> RouteCatalogAuthority:
    model_profile_records = route_catalog.model_profiles.get(
        runtime_profile.model_profile_id,
        (),
    )
    route_catalog_candidate_refs = _select_candidate_refs(
        tuple(model_profile_records),
        model_profile_id=runtime_profile.model_profile_id,
    )
    failover_candidate_refs = tuple(binding.candidate_ref for binding in failover_bindings)
    missing_from_model_profile = tuple(
        candidate_ref
        for candidate_ref in failover_candidate_refs
        if candidate_ref not in route_catalog_candidate_refs
    )
    if missing_from_model_profile:
        raise _fail(
            "provider_route_runtime.failover_slice_stale",
            "active provider failover slice referenced candidates absent from the runtime route catalog snapshot",
            details={
                "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                "model_profile_id": runtime_profile.model_profile_id,
                "provider_policy_id": runtime_profile.provider_policy_id,
                "binding_scope": binding_scope,
                "as_of": as_of.isoformat(),
                "missing_candidate_refs": missing_from_model_profile,
                "route_catalog_candidate_refs": route_catalog_candidate_refs,
            },
        )

    missing_candidate_records = tuple(
        candidate_ref
        for candidate_ref in failover_candidate_refs
        if not route_catalog.provider_model_candidates.get(candidate_ref)
    )
    if missing_candidate_records:
        raise _fail(
            "provider_route_runtime.failover_slice_stale",
            "active provider failover slice referenced candidates missing active candidate authority rows",
            details={
                "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                "model_profile_id": runtime_profile.model_profile_id,
                "provider_policy_id": runtime_profile.provider_policy_id,
                "binding_scope": binding_scope,
                "as_of": as_of.isoformat(),
                "missing_candidate_refs": missing_candidate_records,
            },
        )

    source_model_profile = model_profile_records[0]
    source_bindings = {
        binding.candidate_ref: binding
        for binding in route_catalog.model_profile_candidate_bindings.get(
            runtime_profile.model_profile_id,
            (),
        )
    }
    narrowed_bindings = tuple(
        ModelProfileCandidateBindingAuthorityRecord(
            model_profile_candidate_binding_id=source_bindings[binding.candidate_ref].model_profile_candidate_binding_id,
            model_profile_id=runtime_profile.model_profile_id,
            candidate_ref=binding.candidate_ref,
            binding_role=source_bindings[binding.candidate_ref].binding_role,
            position_index=index,
        )
        for index, binding in enumerate(failover_bindings)
        if binding.candidate_ref in source_bindings
    )
    return RouteCatalogAuthority(
        model_profiles={
            runtime_profile.model_profile_id: (
                ModelProfileAuthorityRecord(
                    model_profile_id=runtime_profile.model_profile_id,
                    candidate_refs=failover_candidate_refs,
                    default_candidate_ref=(
                        source_model_profile.default_candidate_ref
                        if source_model_profile.default_candidate_ref in failover_candidate_refs
                        else None
                    ),
                ),
            ),
        },
        provider_policies=route_catalog.provider_policies,
        provider_model_candidates={
            candidate_ref: route_catalog.provider_model_candidates[candidate_ref]
            for candidate_ref in failover_candidate_refs
        },
        model_profile_candidate_bindings={
            runtime_profile.model_profile_id: narrowed_bindings,
        },
    )


def _select_authoritative_failover_candidate_ref(
    router: ModelRouter,
    *,
    runtime_profile: RuntimeProfile,
    failover_bindings: tuple[ProviderFailoverBindingAuthorityRecord, ...],
    binding_scope: str,
    as_of: datetime,
) -> str:
    try:
        allowed_candidates = router.resolve_candidates(runtime_profile=runtime_profile)
    except ModelRoutingError as exc:
        if exc.reason_code == "routing.no_allowed_candidates":
            raise _fail(
                "provider_route_runtime.failover_no_admitted_candidate",
                "active provider failover slice did not admit any route candidate at the runtime snapshot",
                details={
                    "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                    "model_profile_id": runtime_profile.model_profile_id,
                    "provider_policy_id": runtime_profile.provider_policy_id,
                    "binding_scope": binding_scope,
                    "as_of": as_of.isoformat(),
                    "slice_candidate_refs": tuple(
                        binding.candidate_ref for binding in failover_bindings
                    ),
                },
            ) from exc
        raise
    allowed_candidate_refs = {candidate.candidate_ref for candidate in allowed_candidates}
    for binding in failover_bindings:
        if binding.candidate_ref in allowed_candidate_refs:
            return binding.candidate_ref
    raise _fail(
        "provider_route_runtime.failover_no_admitted_candidate",
        "active provider failover slice did not admit any route candidate at the runtime snapshot",
        details={
            "runtime_profile_ref": runtime_profile.runtime_profile_ref,
            "model_profile_id": runtime_profile.model_profile_id,
            "provider_policy_id": runtime_profile.provider_policy_id,
            "binding_scope": binding_scope,
            "as_of": as_of.isoformat(),
            "slice_candidate_refs": tuple(binding.candidate_ref for binding in failover_bindings),
            "allowed_candidate_refs": tuple(
                candidate.candidate_ref for candidate in allowed_candidates
            ),
        },
    )


def _selected_failover_binding(
    failover_bindings: tuple[ProviderFailoverBindingAuthorityRecord, ...],
    *,
    candidate_ref: str,
) -> ProviderFailoverBindingAuthorityRecord:
    for binding in failover_bindings:
        if binding.candidate_ref == candidate_ref:
            return binding
    raise _fail(
        "provider_route_runtime.selected_failover_binding_missing",
        "selected route candidate did not retain a matching failover binding in the runtime snapshot",
        details={"candidate_ref": candidate_ref},
    )


def _ensure_fresh_failover_route_state(
    *,
    runtime_profile: RuntimeProfile,
    failover_binding: ProviderFailoverBindingAuthorityRecord,
    route_eligibility_state: RouteEligibilityStateAuthorityRecord,
    as_of: datetime,
) -> None:
    route_evaluated_at = route_eligibility_state.evaluated_at
    if route_evaluated_at < failover_binding.effective_from:
        raise _fail(
            "provider_route_runtime.failover_slice_stale",
            "selected route eligibility evidence predates the active failover effective slice",
            details={
                "runtime_profile_ref": runtime_profile.runtime_profile_ref,
                "model_profile_id": runtime_profile.model_profile_id,
                "provider_policy_id": runtime_profile.provider_policy_id,
                "candidate_ref": failover_binding.candidate_ref,
                "binding_scope": failover_binding.binding_scope,
                "as_of": as_of.isoformat(),
                "failover_slice_key": _format_failover_slice_key(failover_binding),
                "route_eligibility_state_id": route_eligibility_state.route_eligibility_state_id,
                "route_evaluated_at": route_evaluated_at.isoformat(),
            },
        )


@dataclass(frozen=True, slots=True)
class ProviderRouteRuntimeResolution:
    """Resolved bounded provider-route runtime decision plus its authority evidence."""

    runtime_profile: RuntimeProfile
    route_decision: ModelRouteDecision
    route_eligibility_state: RouteEligibilityStateAuthorityRecord
    as_of: datetime
    provider_failover_bindings: tuple[ProviderFailoverBindingAuthorityRecord, ...] = ()
    selected_provider_failover_binding: ProviderFailoverBindingAuthorityRecord | None = None
    route_catalog_authority: str = "registry.route_catalog_repository"
    route_authority: str = "registry.provider_routing"
    failover_authority: str | None = None

    @property
    def selected_candidate_ref(self) -> str:
        return self.route_decision.selected_candidate_ref

    @property
    def route_decision_id(self) -> str:
        return self.route_decision.route_decision_id

    def to_json(self) -> dict[str, Any]:
        payload = {
            "as_of": self.as_of.isoformat(),
            "authorities": {
                "route_catalog": self.route_catalog_authority,
                "route": self.route_authority,
            },
            "runtime_profile": {
                "runtime_profile_ref": self.runtime_profile.runtime_profile_ref,
                "model_profile_id": self.runtime_profile.model_profile_id,
                "provider_policy_id": self.runtime_profile.provider_policy_id,
            },
            "route": {
                "route_decision_id": self.route_decision.route_decision_id,
                "selected_candidate_ref": self.route_decision.selected_candidate_ref,
                "provider_ref": self.route_decision.provider_ref,
                "provider_slug": self.route_decision.provider_slug,
                "model_slug": self.route_decision.model_slug,
                "balance_slot": self.route_decision.balance_slot,
                "decision_reason_code": self.route_decision.decision_reason_code,
                "allowed_candidate_refs": list(self.route_decision.allowed_candidate_refs),
            },
            "route_eligibility_state": {
                "route_eligibility_state_id": (
                    self.route_eligibility_state.route_eligibility_state_id
                ),
                "eligibility_status": self.route_eligibility_state.eligibility_status,
                "reason_code": self.route_eligibility_state.reason_code,
                "source_window_refs": list(self.route_eligibility_state.source_window_refs),
                "evaluated_at": self.route_eligibility_state.evaluated_at.isoformat(),
                "decision_ref": self.route_eligibility_state.decision_ref,
            },
        }
        if self.failover_authority is not None and self.selected_provider_failover_binding is not None:
            payload["authorities"]["failover"] = self.failover_authority
            payload["failover"] = {
                "binding_scope": self.selected_provider_failover_binding.binding_scope,
                "selected_provider_failover_binding_id": (
                    self.selected_provider_failover_binding.provider_failover_binding_id
                ),
                "selected_candidate_ref": self.selected_provider_failover_binding.candidate_ref,
                "failover_role": self.selected_provider_failover_binding.failover_role,
                "trigger_rule": self.selected_provider_failover_binding.trigger_rule,
                "position_index": self.selected_provider_failover_binding.position_index,
                "slice_candidate_refs": [
                    binding.candidate_ref for binding in self.provider_failover_bindings
                ],
                "decision_ref": self.selected_provider_failover_binding.decision_ref,
            }
        return payload


@dataclass(frozen=True, slots=True)
class ProviderRouteRuntimeAuthorityBundle:
    route_catalog: RouteCatalogAuthority
    route_authority_snapshot: object
    router: ModelRouter
    candidate_refs: tuple[str, ...]
    failover_bindings: tuple[ProviderFailoverBindingAuthorityRecord, ...] = ()


class ProviderRouteRuntimeAuthorityOrchestrator:
    """Load the bounded routing authorities behind one runtime route decision."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self._route_catalog_repository = PostgresRouteCatalogRepository(conn)

    async def load(
        self,
        *,
        runtime_profile: RuntimeProfile,
        as_of: datetime,
        failover_binding_scope: str | None,
    ) -> ProviderRouteRuntimeAuthorityBundle:
        failover_bindings: tuple[ProviderFailoverBindingAuthorityRecord, ...] = ()

        async with self._conn.transaction():
            if failover_binding_scope is not None:
                failover_selector = ProviderFailoverAuthoritySelector(
                    model_profile_id=runtime_profile.model_profile_id,
                    provider_policy_id=runtime_profile.provider_policy_id,
                    binding_scope=failover_binding_scope,
                    as_of=as_of,
                )
                try:
                    failover_authority = await load_provider_failover_and_endpoint_authority(
                        self._conn,
                        failover_selectors=(failover_selector,),
                    )
                except ProviderFailoverAndEndpointAuthorityRepositoryError as exc:
                    raise _translate_failover_authority_failure(
                        exc,
                        runtime_profile=runtime_profile,
                        binding_scope=failover_binding_scope,
                        as_of=as_of,
                    ) from exc
                failover_bindings = _ordered_failover_bindings(
                    failover_authority.resolve_provider_failover_bindings(
                        selector=failover_selector,
                    ),
                    runtime_profile=runtime_profile,
                    binding_scope=failover_binding_scope,
                    as_of=as_of,
                )

            model_profile_records = await self._route_catalog_repository.fetch_model_profiles(
                model_profile_ids=(runtime_profile.model_profile_id,),
                as_of=as_of,
            )
            candidate_refs = _select_candidate_refs(
                model_profile_records,
                model_profile_id=runtime_profile.model_profile_id,
            )
            route_catalog = await self._route_catalog_repository.load_route_catalog(
                model_profile_ids=(runtime_profile.model_profile_id,),
                provider_policy_ids=(runtime_profile.provider_policy_id,),
                candidate_refs=candidate_refs,
                as_of=as_of,
            )
            if failover_bindings:
                route_catalog = _narrow_route_catalog_to_failover_slice(
                    route_catalog,
                    runtime_profile=runtime_profile,
                    failover_bindings=failover_bindings,
                    binding_scope=failover_binding_scope,
                    as_of=as_of,
                )
            candidate_refs_for_snapshot = (
                tuple(binding.candidate_ref for binding in failover_bindings)
                if failover_bindings
                else candidate_refs
            )
            route_authority_snapshot = await load_provider_route_authority_snapshot(
                self._conn,
                as_of=as_of,
                model_profile_ids=(runtime_profile.model_profile_id,),
                provider_policy_ids=(runtime_profile.provider_policy_id,),
                candidate_refs=candidate_refs_for_snapshot,
            )
        router = ModelRouter.from_route_catalog(
            route_catalog,
            route_authority=route_authority_snapshot,
        )
        return ProviderRouteRuntimeAuthorityBundle(
            route_catalog=route_catalog,
            route_authority_snapshot=route_authority_snapshot,
            router=router,
            candidate_refs=candidate_refs,
            failover_bindings=failover_bindings,
        )


async def resolve_provider_route_runtime(
    conn: asyncpg.Connection,
    *,
    runtime_profile: RuntimeProfile,
    as_of: datetime,
    balance_slot: int = 0,
    preferred_candidate_ref: str | None = None,
    failover_binding_scope: str | None = None,
) -> ProviderRouteRuntimeResolution:
    """Resolve one bounded runtime route using DB-backed control-tower authority."""

    normalized_runtime_profile = _normalize_runtime_profile(runtime_profile)
    normalized_as_of = _normalize_as_of(as_of)
    normalized_balance_slot = _normalize_balance_slot(balance_slot)
    normalized_preferred_candidate_ref = _normalize_preferred_candidate_ref(
        preferred_candidate_ref,
    )
    normalized_failover_binding_scope = _normalize_failover_binding_scope(
        failover_binding_scope,
    )
    authority_bundle = await ProviderRouteRuntimeAuthorityOrchestrator(conn).load(
        runtime_profile=normalized_runtime_profile,
        as_of=normalized_as_of,
        failover_binding_scope=normalized_failover_binding_scope,
    )
    failover_bindings = authority_bundle.failover_bindings
    route_catalog = authority_bundle.route_catalog
    route_authority_snapshot = authority_bundle.route_authority_snapshot
    router = authority_bundle.router
    effective_preferred_candidate_ref = normalized_preferred_candidate_ref
    if failover_bindings:
        authoritative_candidate_ref = _select_authoritative_failover_candidate_ref(
            router,
            runtime_profile=normalized_runtime_profile,
            failover_bindings=failover_bindings,
            binding_scope=normalized_failover_binding_scope,
            as_of=normalized_as_of,
        )
        if (
            normalized_preferred_candidate_ref is not None
            and normalized_preferred_candidate_ref != authoritative_candidate_ref
        ):
            raise _fail(
                "provider_route_runtime.failover_candidate_mismatch",
                "caller preferred candidate did not match the active failover-authoritative candidate",
                details={
                    "runtime_profile_ref": normalized_runtime_profile.runtime_profile_ref,
                    "model_profile_id": normalized_runtime_profile.model_profile_id,
                    "provider_policy_id": normalized_runtime_profile.provider_policy_id,
                    "binding_scope": normalized_failover_binding_scope,
                    "as_of": normalized_as_of.isoformat(),
                    "preferred_candidate_ref": normalized_preferred_candidate_ref,
                    "authoritative_candidate_ref": authoritative_candidate_ref,
                    "slice_candidate_refs": tuple(
                        binding.candidate_ref for binding in failover_bindings
                    ),
                },
            )
        effective_preferred_candidate_ref = authoritative_candidate_ref

    try:
        route_decision = router.decide_route(
            runtime_profile=normalized_runtime_profile,
            balance_slot=normalized_balance_slot,
            preferred_candidate_ref=effective_preferred_candidate_ref,
        )
    except ModelRoutingError as exc:
        raise _fail(
            "provider_route_runtime.routing_failed",
            "provider-route runtime could not resolve an admitted route from control-tower authority",
            details={
                "runtime_profile_ref": normalized_runtime_profile.runtime_profile_ref,
                "model_profile_id": normalized_runtime_profile.model_profile_id,
                "provider_policy_id": normalized_runtime_profile.provider_policy_id,
                "as_of": normalized_as_of.isoformat(),
                "reason_code": exc.reason_code,
                "details": exc.details,
                "metadata": dict(exc.metadata),
            },
        ) from exc

    route_eligibility_state = select_route_eligibility_state(
        route_authority_snapshot,
        model_profile_id=normalized_runtime_profile.model_profile_id,
        provider_policy_id=normalized_runtime_profile.provider_policy_id,
        candidate_ref=route_decision.selected_candidate_ref,
    )
    if route_eligibility_state is None:
        raise _fail(
            "provider_route_runtime.selected_candidate_state_missing",
            "selected candidate did not retain a matching control-tower state in the runtime snapshot",
            details={
                "runtime_profile_ref": normalized_runtime_profile.runtime_profile_ref,
                "candidate_ref": route_decision.selected_candidate_ref,
            },
        )
    selected_failover_binding = None
    if failover_bindings:
        selected_failover_binding = _selected_failover_binding(
            failover_bindings,
            candidate_ref=route_decision.selected_candidate_ref,
        )
        _ensure_fresh_failover_route_state(
            runtime_profile=normalized_runtime_profile,
            failover_binding=selected_failover_binding,
            route_eligibility_state=route_eligibility_state,
            as_of=normalized_as_of,
        )
    return ProviderRouteRuntimeResolution(
        runtime_profile=normalized_runtime_profile,
        route_decision=route_decision,
        route_eligibility_state=route_eligibility_state,
        as_of=normalized_as_of,
        provider_failover_bindings=failover_bindings,
        selected_provider_failover_binding=selected_failover_binding,
        failover_authority=(
            "registry.endpoint_failover" if selected_failover_binding else None
        ),
    )


__all__ = [
    "ProviderRouteRuntimeError",
    "ProviderRouteRuntimeResolution",
    "resolve_provider_route_runtime",
]
