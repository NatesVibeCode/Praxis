from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from observability.operator_topology import (
    NativeCutoverGraphStatusReadModel,
)
from observability.operator_dashboard import (
    NativeOperatorCockpitError,
    operator_cockpit_run,
    render_operator_cockpit,
)
from observability.read_models import ProjectionCompleteness, ProjectionWatermark
from authority.workflow_class_resolution import WorkflowClassResolutionDecision
from policy.workflow_classes import WorkflowClassAuthorityRecord
from policy.workflow_lanes import WorkflowLanePolicyAuthorityRecord
from registry.provider_routing import (
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
)
from observability.read_models import GraphLineageReadModel, GraphTopologyReadModel


def test_operator_cockpit_surface_stitches_route_dispatch_and_cutover_truth() -> None:
    as_of = datetime(2026, 4, 2, 22, 0, tzinfo=timezone.utc)
    run_id = "run.operator-cockpit.alpha"
    route_authority = ProviderRouteAuthority(
        provider_route_health_windows={
            "candidate.alpha": (
                ProviderRouteHealthWindowAuthorityRecord(
                    provider_route_health_window_id="health.alpha.latest",
                    candidate_ref="candidate.alpha",
                    provider_ref="provider.openai",
                    health_status="healthy",
                    health_score=0.98,
                    sample_count=24,
                    failure_rate=0.01,
                    latency_p95_ms=125,
                    observed_window_started_at=as_of,
                    observed_window_ended_at=as_of,
                    observation_ref="observation.alpha.health",
                    created_at=as_of,
                ),
            ),
        },
        provider_budget_windows={
            "provider_policy.alpha": (
                ProviderBudgetWindowAuthorityRecord(
                    provider_budget_window_id="budget.alpha.latest",
                    provider_policy_id="provider_policy.alpha",
                    provider_ref="provider.openai",
                    budget_scope="runtime",
                    budget_status="available",
                    window_started_at=as_of,
                    window_ended_at=as_of,
                    request_limit=10,
                    requests_used=2,
                    token_limit=1000,
                    tokens_used=120,
                    spend_limit_usd="25.00",
                    spend_used_usd="3.00",
                    decision_ref="decision.alpha.budget",
                    created_at=as_of,
                ),
            ),
        },
        route_eligibility_states={
            "candidate.alpha": (
                RouteEligibilityStateAuthorityRecord(
                    route_eligibility_state_id="eligibility.alpha.latest",
                    model_profile_id="model_profile.alpha",
                    provider_policy_id="provider_policy.alpha",
                    candidate_ref="candidate.alpha",
                    eligibility_status="eligible",
                    reason_code="provider_fallback.healthy_budget_available",
                    source_window_refs=("health.alpha.latest", "budget.alpha.latest"),
                    evaluated_at=as_of,
                    expires_at=None,
                    decision_ref="decision.alpha.eligibility",
                    created_at=as_of,
                ),
            ),
        },
    )
    dispatch_resolution = WorkflowClassResolutionDecision(
        workflow_class=WorkflowClassAuthorityRecord(
            workflow_class_id="workflow_class.review.alpha",
            class_name="review",
            class_kind="review",
            workflow_lane_id="workflow_lane.review.alpha",
            status="active",
            queue_shape={"max_parallel": 1, "batching": "manual"},
            throttle_policy={"max_attempts": 1, "backoff": "none"},
            review_required=True,
            effective_from=as_of,
            effective_to=None,
            decision_ref="decision:workflow-class:review",
            created_at=as_of,
        ),
        lane_policy=WorkflowLanePolicyAuthorityRecord(
            workflow_lane_policy_id="workflow_lane_policy.review.alpha",
            workflow_lane_id="workflow_lane.review.alpha",
            policy_scope="runtime",
            work_kind="review",
            match_rules={"route": "operator"},
            lane_parameters={"max_parallel": 1},
            decision_ref="decision:workflow-lane-policy:review",
            effective_from=as_of,
            effective_to=None,
            created_at=as_of,
        ),
        as_of=as_of,
    )
    cutover_status = NativeCutoverGraphStatusReadModel(
        run_id=run_id,
        request_id="request.operator-cockpit.alpha",
        watermark=ProjectionWatermark(evidence_seq=17),
        evidence_refs=("evidence.alpha.1",),
        graph_topology=GraphTopologyReadModel(
            run_id=run_id,
            request_id="request.operator-cockpit.alpha",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=17),
            evidence_refs=("evidence.alpha.1",),
            admitted_definition_ref=None,
            nodes=(),
            edges=(),
            runtime_node_order=(),
        ),
        graph_lineage=GraphLineageReadModel(
            run_id=run_id,
            request_id="request.operator-cockpit.alpha",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=17),
            evidence_refs=("evidence.alpha.1",),
            claim_received_ref=None,
            admitted_definition_ref=None,
            admitted_definition_hash=None,
            nodes=(),
            edges=(),
            runtime_node_order=(),
            current_state="running",
            terminal_reason=None,
        ),
        cutover_gates=(),
        work_bindings=(),
        completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
        status_state="fresh",
        status_reason=None,
    )

    cockpit = operator_cockpit_run(
        run_id=run_id,
        as_of=as_of,
        route_authority=route_authority,
        dispatch_resolution=dispatch_resolution,
        cutover_status=cutover_status,
    )

    assert cockpit.provenance.to_json() == {
        "kind": "operator_cockpit_provenance",
        "as_of": as_of.isoformat(),
        "section_authorities": {
            "route": "registry.provider_routing.load_provider_route_authority_snapshot",
            "dispatch": "authority.workflow_class_resolution.load_workflow_class_resolution_runtime",
            "cutover": "observability.operator_topology.cutover_graph_status_run",
        },
        "stitched_sections": ["route", "dispatch", "cutover"],
    }
    assert cockpit.completeness == ProjectionCompleteness(is_complete=True, missing_evidence_refs=())
    assert cockpit.status_state == "fresh"
    assert cockpit.status_reason is None
    assert cockpit.watermark == ProjectionWatermark(evidence_seq=17)

    rendered = render_operator_cockpit(cockpit)
    assert "kind: operator_cockpit" in rendered
    assert f"run_id: {run_id}" in rendered
    assert "completeness.is_complete: true" in rendered
    assert "status.state: fresh" in rendered
    assert "route.kind: provider_route_control_tower" in rendered
    assert "route.health_windows_group_count: 1" in rendered
    assert "route.health_windows[0].latest.provider_route_health_window_id: health.alpha.latest" in rendered
    assert "dispatch.kind: workflow_class_resolution" in rendered
    assert "dispatch.workflow_class_id: workflow_class.review.alpha" in rendered
    assert "dispatch.workflow_lane_policy_id: workflow_lane_policy.review.alpha" in rendered
    assert "cutover.kind: cutover_graph_status" in rendered
    assert "cutover.status.state: fresh" in rendered


def test_operator_cockpit_surface_refuses_inconsistent_route_authority_as_complete_or_fresh() -> None:
    as_of = datetime(2026, 4, 2, 22, 0, tzinfo=timezone.utc)
    run_id = "run.operator-cockpit.beta"
    route_authority = ProviderRouteAuthority(
        provider_route_health_windows={
            "candidate.beta": (
                ProviderRouteHealthWindowAuthorityRecord(
                    provider_route_health_window_id="health.beta.latest",
                    candidate_ref="candidate.beta",
                    provider_ref="provider.openai",
                    health_status="healthy",
                    health_score=0.98,
                    sample_count=24,
                    failure_rate=0.01,
                    latency_p95_ms=125,
                    observed_window_started_at=as_of,
                    observed_window_ended_at=as_of,
                    observation_ref="observation.beta.health",
                    created_at=as_of,
                ),
            ),
        },
        provider_budget_windows={
            "provider_policy.beta": (
                ProviderBudgetWindowAuthorityRecord(
                    provider_budget_window_id="budget.beta.latest",
                    provider_policy_id="provider_policy.beta",
                    provider_ref="provider.openai",
                    budget_scope="runtime",
                    budget_status="available",
                    window_started_at=as_of,
                    window_ended_at=as_of,
                    request_limit=10,
                    requests_used=2,
                    token_limit=1000,
                    tokens_used=120,
                    spend_limit_usd="25.00",
                    spend_used_usd="3.00",
                    decision_ref="decision.beta.budget",
                    created_at=as_of,
                ),
            ),
        },
        route_eligibility_states={
            "candidate.beta": (
                RouteEligibilityStateAuthorityRecord(
                    route_eligibility_state_id="eligibility.beta.latest",
                    model_profile_id="model_profile.beta",
                    provider_policy_id="provider_policy.beta",
                    candidate_ref="candidate.beta",
                    eligibility_status="eligible",
                    reason_code="provider_fallback.healthy_budget_available",
                    source_window_refs=("health.beta.latest", "budget.beta.missing"),
                    evaluated_at=as_of,
                    expires_at=None,
                    decision_ref="decision.beta.eligibility",
                    created_at=as_of,
                ),
            ),
        },
    )
    route_authority = replace(
        route_authority,
        provider_route_health_windows={
            "candidate.beta": (
                replace(
                    route_authority.provider_route_health_windows["candidate.beta"][0],
                    created_at=as_of - timedelta(minutes=5),
                ),
            ),
        },
    )
    dispatch_resolution = WorkflowClassResolutionDecision(
        workflow_class=WorkflowClassAuthorityRecord(
            workflow_class_id="workflow_class.review.beta",
            class_name="review",
            class_kind="review",
            workflow_lane_id="workflow_lane.review.beta",
            status="active",
            queue_shape={"max_parallel": 1, "batching": "manual"},
            throttle_policy={"max_attempts": 1, "backoff": "none"},
            review_required=True,
            effective_from=as_of,
            effective_to=None,
            decision_ref="decision:workflow-class:review",
            created_at=as_of,
        ),
        lane_policy=WorkflowLanePolicyAuthorityRecord(
            workflow_lane_policy_id="workflow_lane_policy.review.beta",
            workflow_lane_id="workflow_lane.review.beta",
            policy_scope="runtime",
            work_kind="review",
            match_rules={"route": "operator"},
            lane_parameters={"max_parallel": 1},
            decision_ref="decision:workflow-lane-policy:review",
            effective_from=as_of,
            effective_to=None,
            created_at=as_of,
        ),
        as_of=as_of,
    )
    cutover_status = NativeCutoverGraphStatusReadModel(
        run_id=run_id,
        request_id="request.operator-cockpit.beta",
        watermark=ProjectionWatermark(evidence_seq=17),
        evidence_refs=("evidence.beta.1",),
        graph_topology=GraphTopologyReadModel(
            run_id=run_id,
            request_id="request.operator-cockpit.beta",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=17),
            evidence_refs=("evidence.beta.1",),
            admitted_definition_ref=None,
            nodes=(),
            edges=(),
            runtime_node_order=(),
        ),
        graph_lineage=GraphLineageReadModel(
            run_id=run_id,
            request_id="request.operator-cockpit.beta",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=17),
            evidence_refs=("evidence.beta.1",),
            claim_received_ref=None,
            admitted_definition_ref=None,
            admitted_definition_hash=None,
            nodes=(),
            edges=(),
            runtime_node_order=(),
            current_state="running",
            terminal_reason=None,
        ),
        cutover_gates=(),
        work_bindings=(),
        completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
        status_state="fresh",
        status_reason=None,
    )

    cockpit = operator_cockpit_run(
        run_id=run_id,
        as_of=as_of,
        route_authority=route_authority,
        dispatch_resolution=dispatch_resolution,
        cutover_status=cutover_status,
    )

    assert cockpit.route.completeness.is_complete is False
    assert cockpit.completeness.is_complete is False
    assert cockpit.status_state == "stale"
    assert "route:eligibility_states[candidate.beta].record[0]:unknown_source_window_refs" in cockpit.status_reason
    assert "route:snapshot_mixed_time" in cockpit.status_reason
    assert "route:eligibility_states[candidate.beta].record[0]:unknown_source_window_refs" in cockpit.completeness.missing_evidence_refs
    assert "route:snapshot_mixed_time" in cockpit.completeness.missing_evidence_refs

    rendered = render_operator_cockpit(cockpit)
    assert "route.completeness.is_complete: false" in rendered
    assert "status.state: stale" in rendered
    assert "route:snapshot_mixed_time" in rendered
    assert "route:eligibility_states[candidate.beta].record[0]:unknown_source_window_refs" in rendered


def test_operator_cockpit_surface_refuses_mismatched_dispatch_snapshot_time() -> None:
    as_of = datetime(2026, 4, 2, 22, 0, tzinfo=timezone.utc)
    run_id = "run.operator-cockpit.gamma"
    route_authority = ProviderRouteAuthority(
        provider_route_health_windows={
            "candidate.gamma": (
                ProviderRouteHealthWindowAuthorityRecord(
                    provider_route_health_window_id="health.gamma.latest",
                    candidate_ref="candidate.gamma",
                    provider_ref="provider.openai",
                    health_status="healthy",
                    health_score=0.98,
                    sample_count=24,
                    failure_rate=0.01,
                    latency_p95_ms=125,
                    observed_window_started_at=as_of,
                    observed_window_ended_at=as_of,
                    observation_ref="observation.gamma.health",
                    created_at=as_of,
                ),
            ),
        },
        provider_budget_windows={
            "provider_policy.gamma": (
                ProviderBudgetWindowAuthorityRecord(
                    provider_budget_window_id="budget.gamma.latest",
                    provider_policy_id="provider_policy.gamma",
                    provider_ref="provider.openai",
                    budget_scope="runtime",
                    budget_status="available",
                    window_started_at=as_of,
                    window_ended_at=as_of,
                    request_limit=10,
                    requests_used=2,
                    token_limit=1000,
                    tokens_used=120,
                    spend_limit_usd="25.00",
                    spend_used_usd="3.00",
                    decision_ref="decision.gamma.budget",
                    created_at=as_of,
                ),
            ),
        },
        route_eligibility_states={
            "candidate.gamma": (
                RouteEligibilityStateAuthorityRecord(
                    route_eligibility_state_id="eligibility.gamma.latest",
                    model_profile_id="model_profile.gamma",
                    provider_policy_id="provider_policy.gamma",
                    candidate_ref="candidate.gamma",
                    eligibility_status="eligible",
                    reason_code="provider_fallback.healthy_budget_available",
                    source_window_refs=("health.gamma.latest", "budget.gamma.latest"),
                    evaluated_at=as_of,
                    expires_at=None,
                    decision_ref="decision.gamma.eligibility",
                    created_at=as_of,
                ),
            ),
        },
    )
    dispatch_resolution = WorkflowClassResolutionDecision(
        workflow_class=WorkflowClassAuthorityRecord(
            workflow_class_id="workflow_class.review.gamma",
            class_name="review",
            class_kind="review",
            workflow_lane_id="workflow_lane.review.gamma",
            status="active",
            queue_shape={"max_parallel": 1, "batching": "manual"},
            throttle_policy={"max_attempts": 1, "backoff": "none"},
            review_required=True,
            effective_from=as_of,
            effective_to=None,
            decision_ref="decision:workflow-class:review",
            created_at=as_of,
        ),
        lane_policy=WorkflowLanePolicyAuthorityRecord(
            workflow_lane_policy_id="workflow_lane_policy.review.gamma",
            workflow_lane_id="workflow_lane.review.gamma",
            policy_scope="runtime",
            work_kind="review",
            match_rules={"route": "operator"},
            lane_parameters={"max_parallel": 1},
            decision_ref="decision:workflow-lane-policy:review",
            effective_from=as_of,
            effective_to=None,
            created_at=as_of,
        ),
        as_of=as_of - timedelta(minutes=15),
    )
    cutover_status = NativeCutoverGraphStatusReadModel(
        run_id=run_id,
        request_id="request.operator-cockpit.gamma",
        watermark=ProjectionWatermark(evidence_seq=17),
        evidence_refs=("evidence.gamma.1",),
        graph_topology=GraphTopologyReadModel(
            run_id=run_id,
            request_id="request.operator-cockpit.gamma",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=17),
            evidence_refs=("evidence.gamma.1",),
            admitted_definition_ref=None,
            nodes=(),
            edges=(),
            runtime_node_order=(),
        ),
        graph_lineage=GraphLineageReadModel(
            run_id=run_id,
            request_id="request.operator-cockpit.gamma",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=17),
            evidence_refs=("evidence.gamma.1",),
            claim_received_ref=None,
            admitted_definition_ref=None,
            admitted_definition_hash=None,
            nodes=(),
            edges=(),
            runtime_node_order=(),
            current_state="running",
            terminal_reason=None,
        ),
        cutover_gates=(),
        work_bindings=(),
        completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
        status_state="fresh",
        status_reason=None,
    )

    cockpit = operator_cockpit_run(
        run_id=run_id,
        as_of=as_of,
        route_authority=route_authority,
        dispatch_resolution=dispatch_resolution,
        cutover_status=cutover_status,
    )

    assert cockpit.completeness.is_complete is False
    assert cockpit.status_state == "stale"
    assert "dispatch:snapshot_as_of_mismatch" in cockpit.completeness.missing_evidence_refs
    assert "dispatch:snapshot_as_of_mismatch" in cockpit.status_reason
    rendered = render_operator_cockpit(cockpit)
    assert "status.state: stale" in rendered
    assert "dispatch:snapshot_as_of_mismatch" in rendered


def test_operator_cockpit_surface_fails_closed_without_route_authority() -> None:
    as_of = datetime(2026, 4, 2, 22, 0, tzinfo=timezone.utc)

    with pytest.raises(NativeOperatorCockpitError) as exc_info:
        operator_cockpit_run(
            run_id="run.operator-cockpit.missing-route",
            as_of=as_of,
            route_authority=None,
            dispatch_resolution={"dispatch_rows": 1},
            cutover_status={"cutover_rows": 1},
        )

    assert exc_info.value.reason_code == "operator_cockpit.route_authority_missing"


def test_operator_cockpit_surface_fails_closed_without_cutover_truth() -> None:
    as_of = datetime(2026, 4, 2, 22, 0, tzinfo=timezone.utc)

    with pytest.raises(NativeOperatorCockpitError) as exc_info:
        operator_cockpit_run(
            run_id="run.operator-cockpit.missing-cutover",
            as_of=as_of,
            route_authority=ProviderRouteAuthority(
                provider_route_health_windows={},
                provider_budget_windows={},
                route_eligibility_states={},
            ),
            dispatch_resolution={"dispatch_rows": 1},
            cutover_status=None,
        )

    assert exc_info.value.reason_code == "operator_cockpit.cutover_status_missing"
