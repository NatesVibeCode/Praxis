from __future__ import annotations

from datetime import datetime, timedelta, timezone

from registry.provider_routing import (
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
    bound_provider_route_authority,
    select_route_eligibility_state,
)


def _clock() -> datetime:
    return datetime(2026, 4, 12, 18, 0, tzinfo=timezone.utc)


def _authority() -> ProviderRouteAuthority:
    now = _clock()
    health_window_old = ProviderRouteHealthWindowAuthorityRecord(
        provider_route_health_window_id="health.old",
        candidate_ref="candidate.alpha",
        provider_ref="provider.openai",
        health_status="healthy",
        health_score=0.91,
        sample_count=8,
        failure_rate=0.0,
        latency_p95_ms=800,
        observed_window_started_at=now - timedelta(minutes=30),
        observed_window_ended_at=now - timedelta(minutes=20),
        observation_ref="observation.old",
        created_at=now - timedelta(minutes=20),
    )
    health_window_current = ProviderRouteHealthWindowAuthorityRecord(
        provider_route_health_window_id="health.current",
        candidate_ref="candidate.alpha",
        provider_ref="provider.openai",
        health_status="healthy",
        health_score=0.98,
        sample_count=12,
        failure_rate=0.0,
        latency_p95_ms=500,
        observed_window_started_at=now - timedelta(minutes=10),
        observed_window_ended_at=now - timedelta(minutes=1),
        observation_ref="observation.current",
        created_at=now - timedelta(minutes=1),
    )
    health_window_other = ProviderRouteHealthWindowAuthorityRecord(
        provider_route_health_window_id="health.other",
        candidate_ref="candidate.beta",
        provider_ref="provider.anthropic",
        health_status="healthy",
        health_score=0.95,
        sample_count=7,
        failure_rate=0.0,
        latency_p95_ms=700,
        observed_window_started_at=now - timedelta(minutes=10),
        observed_window_ended_at=now - timedelta(minutes=2),
        observation_ref="observation.other",
        created_at=now - timedelta(minutes=2),
    )
    budget_window_current = ProviderBudgetWindowAuthorityRecord(
        provider_budget_window_id="budget.current",
        provider_policy_id="policy.alpha",
        provider_ref="provider.openai",
        budget_scope="runtime",
        budget_status="available",
        window_started_at=now - timedelta(hours=1),
        window_ended_at=now,
        request_limit=100,
        requests_used=10,
        token_limit=1000,
        tokens_used=100,
        spend_limit_usd=10.0,
        spend_used_usd=1.0,
        decision_ref="decision.budget.current",
        created_at=now - timedelta(minutes=1),
    )
    budget_window_other = ProviderBudgetWindowAuthorityRecord(
        provider_budget_window_id="budget.other",
        provider_policy_id="policy.beta",
        provider_ref="provider.anthropic",
        budget_scope="runtime",
        budget_status="available",
        window_started_at=now - timedelta(hours=1),
        window_ended_at=now,
        request_limit=100,
        requests_used=10,
        token_limit=1000,
        tokens_used=100,
        spend_limit_usd=10.0,
        spend_used_usd=1.0,
        decision_ref="decision.budget.other",
        created_at=now - timedelta(minutes=1),
    )
    old_state = RouteEligibilityStateAuthorityRecord(
        route_eligibility_state_id="eligibility.old",
        model_profile_id="model.alpha",
        provider_policy_id="policy.alpha",
        candidate_ref="candidate.alpha",
        eligibility_status="eligible",
        reason_code="provider_routing.healthy_budget_available",
        source_window_refs=("health.old", "budget.current"),
        evaluated_at=now - timedelta(minutes=20),
        expires_at=None,
        decision_ref="decision.old",
        created_at=now - timedelta(minutes=20),
    )
    current_state = RouteEligibilityStateAuthorityRecord(
        route_eligibility_state_id="eligibility.current",
        model_profile_id="model.alpha",
        provider_policy_id="policy.alpha",
        candidate_ref="candidate.alpha",
        eligibility_status="eligible",
        reason_code="provider_routing.healthy_budget_available",
        source_window_refs=("health.current", "budget.current"),
        evaluated_at=now - timedelta(minutes=1),
        expires_at=None,
        decision_ref="decision.current",
        created_at=now - timedelta(minutes=1),
    )
    future_state = RouteEligibilityStateAuthorityRecord(
        route_eligibility_state_id="eligibility.future",
        model_profile_id="model.alpha",
        provider_policy_id="policy.alpha",
        candidate_ref="candidate.alpha",
        eligibility_status="eligible",
        reason_code="provider_routing.healthy_budget_available",
        source_window_refs=("health.current", "budget.current"),
        evaluated_at=now + timedelta(minutes=5),
        expires_at=None,
        decision_ref="decision.future",
        created_at=now + timedelta(minutes=5),
    )
    other_state = RouteEligibilityStateAuthorityRecord(
        route_eligibility_state_id="eligibility.other",
        model_profile_id="model.beta",
        provider_policy_id="policy.beta",
        candidate_ref="candidate.beta",
        eligibility_status="eligible",
        reason_code="provider_routing.healthy_budget_available",
        source_window_refs=("health.other", "budget.other"),
        evaluated_at=now - timedelta(minutes=2),
        expires_at=None,
        decision_ref="decision.other",
        created_at=now - timedelta(minutes=2),
    )
    return ProviderRouteAuthority(
        provider_route_health_windows={
            "candidate.alpha": (health_window_current, health_window_old),
            "candidate.beta": (health_window_other,),
        },
        provider_budget_windows={
            "policy.alpha": (budget_window_current,),
            "policy.beta": (budget_window_other,),
        },
        route_eligibility_states={
            "candidate.alpha": (future_state, current_state, old_state),
            "candidate.beta": (other_state,),
        },
    )


def test_bound_provider_route_authority_keeps_only_bounded_snapshot_rows() -> None:
    authority = _authority()

    bounded = bound_provider_route_authority(
        authority,
        model_profile_id="model.alpha",
        provider_policy_id="policy.alpha",
        as_of=_clock(),
    )

    assert tuple(bounded.route_eligibility_states) == ("candidate.alpha",)
    assert bounded.route_eligibility_states["candidate.alpha"][0].route_eligibility_state_id == (
        "eligibility.current"
    )
    assert tuple(bounded.provider_route_health_windows) == ("candidate.alpha",)
    assert bounded.provider_route_health_windows["candidate.alpha"][0].provider_route_health_window_id == (
        "health.current"
    )
    assert tuple(bounded.provider_budget_windows) == ("policy.alpha",)
    assert bounded.provider_budget_windows["policy.alpha"][0].provider_budget_window_id == (
        "budget.current"
    )


def test_select_route_eligibility_state_returns_latest_match_or_none() -> None:
    authority = _authority()

    bounded = bound_provider_route_authority(
        authority,
        model_profile_id="model.alpha",
        provider_policy_id="policy.alpha",
        as_of=_clock(),
    )

    selected = select_route_eligibility_state(
        bounded,
        model_profile_id="model.alpha",
        provider_policy_id="policy.alpha",
        candidate_ref="candidate.alpha",
    )

    assert selected is not None
    assert selected.route_eligibility_state_id == "eligibility.current"
    assert (
        select_route_eligibility_state(
            bounded,
            model_profile_id="model.alpha",
            provider_policy_id="policy.alpha",
            candidate_ref="candidate.missing",
        )
        is None
    )
