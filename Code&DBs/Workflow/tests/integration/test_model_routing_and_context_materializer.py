from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from registry.model_routing import (
    ModelProfileAuthorityRecord,
    ModelRouter,
    ModelRoutingError,
    ProviderModelCandidateAuthorityRecord,
    ProviderPolicyAuthorityRecord,
)
from registry.provider_routing import (
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
)
from registry.route_catalog_repository import (
    ModelProfileCandidateBindingAuthorityRecord,
    RouteCatalogAuthority,
)
from runtime.context_materializer import (
    ContextAuthorityRecord,
    ContextCompilationError,
    ContextCompiler,
)


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 19, 0, tzinfo=timezone.utc)


def _resolve_runtime_authority():
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    registry = RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model_profile.alpha",
                    provider_policy_id="provider_policy.alpha",
                    sandbox_profile_ref=runtime_profile_ref,
                ),
            ),
        },
    )
    workspace = registry.resolve_workspace(workspace_ref=workspace_ref)
    runtime_profile = registry.resolve_runtime_profile(
        runtime_profile_ref=runtime_profile_ref,
    )
    return workspace, runtime_profile


def _build_route_catalog(
    *,
    model_profile_records=None,
    provider_policy_records=None,
    candidate_records=None,
) -> RouteCatalogAuthority:
    model_profile_records = model_profile_records or {
        "model_profile.alpha": (
            ModelProfileAuthorityRecord(
                model_profile_id="model_profile.alpha",
                candidate_refs=(
                    "candidate.openai.gpt54",
                    "candidate.openai.gpt54mini",
                    "candidate.anthropic.sonnet",
                ),
            ),
        ),
    }
    provider_policy_records = provider_policy_records or {
        "provider_policy.alpha": (
            ProviderPolicyAuthorityRecord(
                provider_policy_id="provider_policy.alpha",
                allowed_provider_refs=("provider.openai",),
            ),
        ),
    }
    candidate_records = candidate_records or {
        "candidate.openai.gpt54": (
            ProviderModelCandidateAuthorityRecord(
                candidate_ref="candidate.openai.gpt54",
                provider_ref="provider.openai",
                provider_slug="openai",
                model_slug="gpt-5.4",
                priority=10,
                balance_weight=3,
                capability_tags=("primary", "reasoning"),
            ),
        ),
        "candidate.openai.gpt54mini": (
            ProviderModelCandidateAuthorityRecord(
                candidate_ref="candidate.openai.gpt54mini",
                provider_ref="provider.openai",
                provider_slug="openai",
                model_slug="gpt-5.4-mini",
                priority=20,
                balance_weight=1,
                capability_tags=("fallback", "latency"),
            ),
        ),
        "candidate.anthropic.sonnet": (
            ProviderModelCandidateAuthorityRecord(
                candidate_ref="candidate.anthropic.sonnet",
                provider_ref="provider.anthropic",
                provider_slug="anthropic",
                model_slug="claude-sonnet-4-5",
                priority=5,
                balance_weight=1,
                capability_tags=("blocked",),
            ),
        ),
    }

    model_profile_candidate_bindings = {
        model_profile_id: tuple(
            ModelProfileCandidateBindingAuthorityRecord(
                model_profile_candidate_binding_id=(
                    f"{model_profile_id}:{position_index}:{candidate_ref}"
                ),
                model_profile_id=model_profile_id,
                candidate_ref=candidate_ref,
                binding_role="admitted",
                position_index=position_index,
            )
            for position_index, candidate_ref in enumerate(profile.candidate_refs)
        )
        for model_profile_id, records in model_profile_records.items()
        for profile in records
    }

    return RouteCatalogAuthority(
        model_profiles=model_profile_records,
        provider_policies=provider_policy_records,
        provider_model_candidates=candidate_records,
        model_profile_candidate_bindings=model_profile_candidate_bindings,
    )


def _build_route_authority(route_catalog: RouteCatalogAuthority) -> ProviderRouteAuthority:
    model_profile_id = next(iter(route_catalog.model_profiles))
    provider_policy_id = next(iter(route_catalog.provider_policies))
    budget_window_id = f"budget_window.{provider_policy_id}"
    budget_window = ProviderBudgetWindowAuthorityRecord(
        provider_budget_window_id=budget_window_id,
        provider_policy_id=provider_policy_id,
        provider_ref="provider.openai",
        budget_scope="runtime",
        budget_status="available",
        window_started_at=_fixed_clock() - timedelta(hours=1),
        window_ended_at=_fixed_clock() + timedelta(hours=1),
        request_limit=1000,
        requests_used=0,
        token_limit=1_000_000,
        tokens_used=0,
        spend_limit_usd=None,
        spend_used_usd=None,
        decision_ref=f"decision.{provider_policy_id}.budget",
        created_at=_fixed_clock(),
    )

    health_windows: dict[str, tuple[ProviderRouteHealthWindowAuthorityRecord, ...]] = {}
    eligibility_states: dict[str, tuple[RouteEligibilityStateAuthorityRecord, ...]] = {}
    for index, (candidate_ref, candidate_records) in enumerate(
        route_catalog.provider_model_candidates.items()
    ):
        candidate = candidate_records[0]
        health_window_id = f"health_window.{candidate_ref}"
        blocked = "blocked" in candidate.capability_tags
        health_windows[candidate_ref] = (
            ProviderRouteHealthWindowAuthorityRecord(
                provider_route_health_window_id=health_window_id,
                candidate_ref=candidate_ref,
                provider_ref=candidate.provider_ref,
                health_status="healthy" if not blocked else "degraded",
                health_score=0.99 if not blocked else 0.25,
                sample_count=24,
                failure_rate=0.0 if not blocked else 0.75,
                latency_p95_ms=115 if not blocked else 900,
                observed_window_started_at=_fixed_clock() - timedelta(minutes=30),
                observed_window_ended_at=_fixed_clock(),
                observation_ref=f"observation.{candidate_ref}",
                created_at=_fixed_clock(),
            ),
        )
        eligibility_states[candidate_ref] = (
            RouteEligibilityStateAuthorityRecord(
                route_eligibility_state_id=f"eligibility.{candidate_ref}",
                model_profile_id=model_profile_id,
                provider_policy_id=provider_policy_id,
                candidate_ref=candidate_ref,
                eligibility_status="eligible" if not blocked else "rejected",
                reason_code=(
                    "provider_route_authority.healthy_budget_available"
                    if not blocked
                    else "provider_route_authority.manual_hold"
                ),
                source_window_refs=(health_window_id, budget_window_id),
                evaluated_at=_fixed_clock() - timedelta(minutes=10),
                expires_at=None,
                decision_ref=f"decision.{candidate_ref}",
                created_at=_fixed_clock() - timedelta(minutes=10),
            ),
        )

    return ProviderRouteAuthority(
        provider_route_health_windows=health_windows,
        provider_budget_windows={
            provider_policy_id: (budget_window,),
        },
        route_eligibility_states=eligibility_states,
    )


def _build_router(
    *,
    model_profile_records=None,
    provider_policy_records=None,
    candidate_records=None,
) -> ModelRouter:
    route_catalog = _build_route_catalog(
        model_profile_records=model_profile_records,
        provider_policy_records=provider_policy_records,
        candidate_records=candidate_records,
    )
    return ModelRouter(
        route_catalog=route_catalog,
        route_authority=_build_route_authority(route_catalog),
    )


def _build_context_compiler(
    *,
    model_router: ModelRouter | None,
    context_records=None,
) -> ContextCompiler:
    return ContextCompiler(
        context_records=context_records
        or {
            "context.policy.alpha": (
                ContextAuthorityRecord(
                    context_ref="context.policy.alpha",
                    authority_kind="policy",
                    content_hash="sha256:policy-alpha",
                    payload={
                        "instructions": [
                            "Follow admitted routing only.",
                            "Fail closed on unknown refs.",
                        ],
                        "max_tokens": 800,
                    },
                ),
            ),
            "context.workflow.alpha": (
                ContextAuthorityRecord(
                    context_ref="context.workflow.alpha",
                    authority_kind="workflow_definition",
                    content_hash="sha256:workflow-alpha",
                    payload={
                        "summary": "Compile bounded authority only.",
                        "tools": ["rg", "pytest"],
                    },
                ),
            ),
        },
        model_router=model_router,
        clock=_fixed_clock,
    )


def test_model_routing_and_context_compiler_are_deterministic_and_fail_closed() -> None:
    workspace, runtime_profile = _resolve_runtime_authority()
    router = _build_router()

    allowed_candidates = router.resolve_candidates(runtime_profile=runtime_profile)
    assert tuple(candidate.candidate_ref for candidate in allowed_candidates) == (
        "candidate.openai.gpt54",
        "candidate.openai.gpt54mini",
    )
    assert tuple(
        (candidate.provider_slug, candidate.model_slug)
        for candidate in allowed_candidates
    ) == (
        ("openai", "gpt-5.4"),
        ("openai", "gpt-5.4-mini"),
    )

    route_slot_zero = router.decide_route(
        runtime_profile=runtime_profile,
        balance_slot=0,
    )
    same_route_slot_zero = router.decide_route(
        runtime_profile=runtime_profile,
        balance_slot=0,
    )
    route_slot_three = router.decide_route(
        runtime_profile=runtime_profile,
        balance_slot=3,
    )

    assert route_slot_zero == same_route_slot_zero
    assert route_slot_zero.route_decision_id.startswith(
        "route_decision:runtime_profile.alpha:candidate.openai.gpt54:0:"
    )
    assert route_slot_zero.selected_candidate_ref == "candidate.openai.gpt54"
    assert route_slot_zero.provider_slug == "openai"
    assert route_slot_zero.model_slug == "gpt-5.4"
    assert route_slot_zero.decision_reason_code == "routing.balance_slot"
    assert route_slot_zero.allowed_candidate_refs == (
        "candidate.openai.gpt54",
        "candidate.openai.gpt54mini",
    )
    assert route_slot_three.selected_candidate_ref == "candidate.openai.gpt54mini"
    assert route_slot_three.model_slug == "gpt-5.4-mini"
    assert route_slot_zero.route_decision_id != route_slot_three.route_decision_id

    compiler = _build_context_compiler(model_router=router)
    with pytest.raises(ContextCompilationError) as missing_bundle_exc:
        compiler.compile_packet(
            workflow_id="workflow.alpha",
            run_id="run.alpha",
            workspace=workspace,
            runtime_profile=runtime_profile,
            route_decision=route_slot_zero,
            context_refs=("context.policy.alpha", "context.workflow.alpha"),
            source_decision_refs=("decision.admission.alpha", route_slot_zero.route_decision_id),
        )
    assert missing_bundle_exc.value.reason_code == "context.bundle_authority_missing"

    with pytest.raises(ModelRoutingError) as route_exc:
        router.decide_route(
            runtime_profile=runtime_profile,
            preferred_candidate_ref="candidate.unknown",
        )
    assert route_exc.value.reason_code == "routing.preference_unknown"


def test_model_routing_requires_explicit_route_authority_rows() -> None:
    _, runtime_profile = _resolve_runtime_authority()
    route_catalog = _build_route_catalog()
    route_authority = _build_route_authority(route_catalog)
    missing_route_authority = replace(
        route_authority,
        route_eligibility_states={
            "candidate.openai.gpt54": route_authority.route_eligibility_states[
                "candidate.openai.gpt54"
            ],
        },
    )
    router = ModelRouter(route_catalog=route_catalog, route_authority=missing_route_authority)

    with pytest.raises(ModelRoutingError) as exc_info:
        router.resolve_candidates(runtime_profile=runtime_profile)

    assert exc_info.value.reason_code == "routing.route_eligibility_state_missing"


@pytest.mark.parametrize(
    ("router_kwargs", "expected_reason_code"),
    (
        (
            {
                "model_profile_records": {
                    "model_profile.alpha": (
                        ModelProfileAuthorityRecord(
                            model_profile_id="model_profile.shadow",
                            candidate_refs=("candidate.openai.gpt54",),
                        ),
                    ),
                },
            },
            "routing.authority_key_mismatch",
        ),
        (
            {
                "provider_policy_records": {
                    "provider_policy.alpha": (
                        ProviderPolicyAuthorityRecord(
                            provider_policy_id="provider_policy.shadow",
                            allowed_provider_refs=("provider.openai",),
                        ),
                    ),
                },
            },
            "routing.authority_key_mismatch",
        ),
        (
            {
                "candidate_records": {
                    "candidate.openai.gpt54": (
                        ProviderModelCandidateAuthorityRecord(
                            candidate_ref="candidate.shadow",
                            provider_ref="provider.openai",
                            provider_slug="openai",
                            model_slug="gpt-5.4",
                            priority=10,
                            balance_weight=3,
                            capability_tags=("primary",),
                        ),
                    ),
                },
            },
            "routing.authority_key_mismatch",
        ),
    ),
)
def test_model_router_enforces_keyed_authority_identity(
    router_kwargs: dict[str, object],
    expected_reason_code: str,
) -> None:
    with pytest.raises(ModelRoutingError) as exc:
        _build_router(**router_kwargs)
    assert exc.value.reason_code == expected_reason_code


def test_route_decision_id_changes_when_authority_changes_but_selection_does_not() -> None:
    _, runtime_profile = _resolve_runtime_authority()
    baseline_router = _build_router()
    changed_authority_router = _build_router(
        candidate_records={
            "candidate.openai.gpt54": (
                ProviderModelCandidateAuthorityRecord(
                    candidate_ref="candidate.openai.gpt54",
                    provider_ref="provider.openai",
                    provider_slug="openai",
                    model_slug="gpt-5.4",
                    priority=10,
                    balance_weight=3,
                    capability_tags=("primary", "reasoning", "governed"),
                ),
            ),
            "candidate.openai.gpt54mini": (
                ProviderModelCandidateAuthorityRecord(
                    candidate_ref="candidate.openai.gpt54mini",
                    provider_ref="provider.openai",
                    provider_slug="openai",
                    model_slug="gpt-5.4-mini",
                    priority=20,
                    balance_weight=1,
                    capability_tags=("fallback", "latency"),
                ),
            ),
            "candidate.anthropic.sonnet": (
                ProviderModelCandidateAuthorityRecord(
                    candidate_ref="candidate.anthropic.sonnet",
                    provider_ref="provider.anthropic",
                    provider_slug="anthropic",
                    model_slug="claude-sonnet-4-5",
                    priority=5,
                    balance_weight=1,
                    capability_tags=("blocked",),
                ),
            ),
        },
    )

    baseline_route = baseline_router.decide_route(
        runtime_profile=runtime_profile,
        balance_slot=0,
    )
    changed_authority_route = changed_authority_router.decide_route(
        runtime_profile=runtime_profile,
        balance_slot=0,
    )

    assert baseline_route.selected_candidate_ref == changed_authority_route.selected_candidate_ref
    assert baseline_route.provider_slug == changed_authority_route.provider_slug
    assert baseline_route.model_slug == changed_authority_route.model_slug
    assert baseline_route.route_decision_id != changed_authority_route.route_decision_id


def test_context_compiler_rejects_forged_routes() -> None:
    workspace, runtime_profile = _resolve_runtime_authority()
    router = _build_router()
    admitted_route = router.decide_route(
        runtime_profile=runtime_profile,
        balance_slot=0,
    )
    compiler = _build_context_compiler(model_router=router)

    forged_slug_route = replace(admitted_route, provider_slug="anthropic")
    with pytest.raises(ContextCompilationError) as forged_slug_exc:
        compiler.compile_packet(
            workflow_id="workflow.alpha",
            run_id="run.alpha",
            workspace=workspace,
            runtime_profile=runtime_profile,
            route_decision=forged_slug_route,
            context_refs=("context.policy.alpha",),
            source_decision_refs=("decision.admission.alpha", admitted_route.route_decision_id),
        )
    assert forged_slug_exc.value.reason_code == "context.route_forged"

    forged_selected_route = replace(
        admitted_route,
        selected_candidate_ref="candidate.openai.gpt54mini",
        provider_ref="provider.openai",
        provider_slug="openai",
        model_slug="gpt-5.4-mini",
    )
    with pytest.raises(ContextCompilationError) as forged_selected_exc:
        compiler.compile_packet(
            workflow_id="workflow.alpha",
            run_id="run.alpha",
            workspace=workspace,
            runtime_profile=runtime_profile,
            route_decision=forged_selected_route,
            context_refs=("context.policy.alpha",),
            source_decision_refs=("decision.admission.alpha", admitted_route.route_decision_id),
        )
    assert forged_selected_exc.value.reason_code == "context.route_forged"
