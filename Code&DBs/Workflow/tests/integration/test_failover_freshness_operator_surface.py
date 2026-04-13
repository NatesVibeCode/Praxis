from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from observability.operator_topology import NativeCutoverGraphStatusReadModel
from observability.operator_dashboard import (
    NativeOperatorCockpitFailoverSelectorContract,
    operator_cockpit_run_with_failover_contract,
    render_operator_cockpit,
)
from observability.read_models import (
    GraphLineageReadModel,
    GraphTopologyReadModel,
    ProjectionCompleteness,
    ProjectionWatermark,
)
from authority.workflow_class_resolution import WorkflowClassResolutionDecision
from policy.workflow_classes import WorkflowClassAuthorityRecord
from policy.workflow_lanes import WorkflowLanePolicyAuthorityRecord
from registry.provider_routing import (
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
)
from storage.postgres import PostgresConfigurationError, connect_workflow_database


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 20, 0, tzinfo=timezone.utc)


def _json_text(value: object) -> str:
    return json.dumps(value, sort_keys=True)


async def _create_authority_tables(conn) -> None:
    await conn.execute(
        """
        CREATE TEMP TABLE provider_failover_bindings (
            provider_failover_binding_id text PRIMARY KEY,
            model_profile_id text NOT NULL,
            provider_policy_id text NOT NULL,
            candidate_ref text NOT NULL,
            binding_scope text NOT NULL,
            failover_role text NOT NULL,
            trigger_rule text NOT NULL,
            position_index integer NOT NULL,
            effective_from timestamptz NOT NULL,
            effective_to timestamptz,
            decision_ref text NOT NULL,
            created_at timestamptz NOT NULL
        )
        """
    )
    await conn.execute(
        """
        CREATE TEMP TABLE provider_endpoint_bindings (
            provider_endpoint_binding_id text PRIMARY KEY,
            provider_policy_id text NOT NULL,
            candidate_ref text NOT NULL,
            binding_scope text NOT NULL,
            endpoint_ref text NOT NULL,
            endpoint_kind text NOT NULL,
            transport_kind text NOT NULL,
            endpoint_uri text NOT NULL,
            auth_ref text NOT NULL,
            binding_status text NOT NULL,
            request_policy jsonb NOT NULL,
            circuit_breaker_policy jsonb NOT NULL,
            effective_from timestamptz NOT NULL,
            effective_to timestamptz,
            decision_ref text NOT NULL,
            created_at timestamptz NOT NULL
        )
        """
    )


async def _seed_failover_and_endpoint_rows(
    conn,
    *,
    suffix: str,
    as_of: datetime,
) -> tuple[NativeOperatorCockpitFailoverSelectorContract, str]:
    active_from = as_of - timedelta(hours=1)
    model_profile_id = f"model_profile.{suffix}"
    provider_policy_id = f"provider_policy.{suffix}"
    primary_candidate_ref = f"candidate.{suffix}.openai"
    binding_scope = "native_runtime"
    endpoint_ref = f"endpoint.{suffix}.chat"
    endpoint_kind = "chat_completions"

    await conn.execute(
        """
        INSERT INTO provider_failover_bindings (
            provider_failover_binding_id,
            model_profile_id,
            provider_policy_id,
            candidate_ref,
            binding_scope,
            failover_role,
            trigger_rule,
            position_index,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """,
        f"failover.{suffix}.primary",
        model_profile_id,
        provider_policy_id,
        primary_candidate_ref,
        binding_scope,
        "primary",
        "health_degraded",
        0,
        active_from,
        None,
        f"decision.{suffix}.failover.active",
        active_from,
    )
    await conn.execute(
        """
        INSERT INTO provider_failover_bindings (
            provider_failover_binding_id,
            model_profile_id,
            provider_policy_id,
            candidate_ref,
            binding_scope,
            failover_role,
            trigger_rule,
            position_index,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """,
        f"failover.{suffix}.fallback",
        model_profile_id,
        provider_policy_id,
        f"candidate.{suffix}.anthropic",
        binding_scope,
        "fallback",
        "health_degraded",
        1,
        active_from,
        None,
        f"decision.{suffix}.failover.active",
        active_from,
    )
    await conn.execute(
        """
        INSERT INTO provider_endpoint_bindings (
            provider_endpoint_binding_id,
            provider_policy_id,
            candidate_ref,
            binding_scope,
            endpoint_ref,
            endpoint_kind,
            transport_kind,
            endpoint_uri,
            auth_ref,
            binding_status,
            request_policy,
            circuit_breaker_policy,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb, $13, $14, $15, $16
        )
        """,
        f"endpoint.{suffix}.active",
        provider_policy_id,
        primary_candidate_ref,
        binding_scope,
        endpoint_ref,
        endpoint_kind,
        "https",
        "https://api.example.test/v1/chat/completions",
        f"secret.{suffix}.openai",
        "active",
        _json_text({"timeout_ms": 30000}),
        _json_text({"threshold": 3, "window_s": 60}),
        active_from,
        None,
        f"decision.{suffix}.failover.active",
        active_from,
    )

    return (
        NativeOperatorCockpitFailoverSelectorContract(
            model_profile_id=model_profile_id,
            provider_policy_id=provider_policy_id,
            binding_scope=binding_scope,
            endpoint_kind=endpoint_kind,
        ),
        primary_candidate_ref,
    )


def _route_authority(*, as_of: datetime, candidate_ref: str, provider_policy_id: str) -> ProviderRouteAuthority:
    return ProviderRouteAuthority(
        provider_route_health_windows={
            candidate_ref: (
                ProviderRouteHealthWindowAuthorityRecord(
                    provider_route_health_window_id=f"health.{candidate_ref}.latest",
                    candidate_ref=candidate_ref,
                    provider_ref="provider.openai",
                    health_status="healthy",
                    health_score=0.99,
                    sample_count=32,
                    failure_rate=0.0,
                    latency_p95_ms=110,
                    observed_window_started_at=as_of,
                    observed_window_ended_at=as_of,
                    observation_ref=f"observation.{candidate_ref}.health",
                    created_at=as_of,
                ),
            ),
        },
        provider_budget_windows={
            provider_policy_id: (
                ProviderBudgetWindowAuthorityRecord(
                    provider_budget_window_id=f"budget.{provider_policy_id}.latest",
                    provider_policy_id=provider_policy_id,
                    provider_ref="provider.openai",
                    budget_scope="runtime",
                    budget_status="available",
                    window_started_at=as_of,
                    window_ended_at=as_of,
                    request_limit=20,
                    requests_used=4,
                    token_limit=4000,
                    tokens_used=250,
                    spend_limit_usd="50.00",
                    spend_used_usd="5.00",
                    decision_ref=f"decision.{provider_policy_id}.budget",
                    created_at=as_of,
                ),
            ),
        },
        route_eligibility_states={
            candidate_ref: (
                RouteEligibilityStateAuthorityRecord(
                    route_eligibility_state_id=f"eligibility.{candidate_ref}.latest",
                    model_profile_id="unused-by-cockpit",
                    provider_policy_id=provider_policy_id,
                    candidate_ref=candidate_ref,
                    eligibility_status="eligible",
                    reason_code="provider_fallback.healthy_budget_available",
                    source_window_refs=(
                        f"health.{candidate_ref}.latest",
                        f"budget.{provider_policy_id}.latest",
                    ),
                    evaluated_at=as_of,
                    expires_at=None,
                    decision_ref=f"decision.{candidate_ref}.eligibility",
                    created_at=as_of,
                ),
            ),
        },
    )


def _dispatch_resolution(*, as_of: datetime) -> WorkflowClassResolutionDecision:
    return WorkflowClassResolutionDecision(
        workflow_class=WorkflowClassAuthorityRecord(
            workflow_class_id="workflow_class.review.failover",
            class_name="review",
            class_kind="review",
            workflow_lane_id="workflow_lane.review.failover",
            status="active",
            queue_shape={"max_parallel": 1},
            throttle_policy={"max_attempts": 1},
            review_required=True,
            effective_from=as_of,
            effective_to=None,
            decision_ref="decision.workflow.review.failover",
            created_at=as_of,
        ),
        lane_policy=WorkflowLanePolicyAuthorityRecord(
            workflow_lane_policy_id="workflow_lane_policy.review.failover",
            workflow_lane_id="workflow_lane.review.failover",
            policy_scope="runtime",
            work_kind="review",
            match_rules={"route": "operator"},
            lane_parameters={"max_parallel": 1},
            decision_ref="decision.workflow_lane.review.failover",
            effective_from=as_of,
            effective_to=None,
            created_at=as_of,
        ),
        as_of=as_of,
    )


def _cutover_status(*, as_of: datetime, run_id: str) -> NativeCutoverGraphStatusReadModel:
    return NativeCutoverGraphStatusReadModel(
        run_id=run_id,
        request_id=f"request.{run_id}",
        watermark=ProjectionWatermark(evidence_seq=41),
        evidence_refs=("evidence.failover.1",),
        graph_topology=GraphTopologyReadModel(
            run_id=run_id,
            request_id=f"request.{run_id}",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=41),
            evidence_refs=("evidence.failover.1",),
            admitted_definition_ref=None,
            nodes=(),
            edges=(),
            runtime_node_order=(),
        ),
        graph_lineage=GraphLineageReadModel(
            run_id=run_id,
            request_id=f"request.{run_id}",
            completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
            watermark=ProjectionWatermark(evidence_seq=41),
            evidence_refs=("evidence.failover.1",),
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


async def _exercise_failover_freshness_operator_surface() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    try:
        conn = await connect_workflow_database(env={"WORKFLOW_DATABASE_URL": database_url})
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for the failover freshness operator surface test: "
            f"{exc.reason_code}"
        )

    try:
        as_of = _fixed_clock()
        suffix = _unique_suffix()
        run_id = f"run.failover-surface.{suffix}"
        await _create_authority_tables(conn)
        failover_contract, candidate_ref = await _seed_failover_and_endpoint_rows(
            conn,
            suffix=suffix,
            as_of=as_of,
        )

        cockpit = await operator_cockpit_run_with_failover_contract(
            conn=conn,
            run_id=run_id,
            as_of=as_of,
            route_authority=_route_authority(
                as_of=as_of,
                candidate_ref=candidate_ref,
                provider_policy_id=failover_contract.provider_policy_id or "",
            ),
            failover_contract=failover_contract,
            dispatch_resolution=_dispatch_resolution(as_of=as_of),
            cutover_status=_cutover_status(as_of=as_of, run_id=run_id),
        )

        assert cockpit.completeness.is_complete is True
        assert cockpit.status_state == "fresh"
        assert cockpit.failover is not None
        assert cockpit.failover.completeness.is_complete is True
        assert cockpit.failover.freshness_state == "fresh"
        assert cockpit.failover.selector_contract == failover_contract
        assert cockpit.failover.effective_slice is not None
        assert cockpit.failover.effective_slice.selected_candidate_ref == candidate_ref
        assert cockpit.failover.effective_slice.endpoint_ref == f"endpoint.{suffix}.chat"
        assert cockpit.provenance.to_json()["section_authorities"]["failover"] == (
            "registry.endpoint_failover.load_provider_failover_and_endpoint_authority"
        )

        rendered = render_operator_cockpit(cockpit)
        assert "failover.kind: provider_failover_and_endpoint_authority" in rendered
        assert f"failover.selector.model_profile_id: {failover_contract.model_profile_id}" in rendered
        assert f"failover.selector.provider_policy_id: {failover_contract.provider_policy_id}" in rendered
        assert f"failover.selector.endpoint_kind: {failover_contract.endpoint_kind}" in rendered
        assert "failover.freshness.state: fresh" in rendered
        assert f"failover.effective_slice.selected_candidate_ref: {candidate_ref}" in rendered

        stale_from = as_of - timedelta(minutes=15)
        await conn.execute(
            """
            UPDATE provider_endpoint_bindings
            SET effective_to = $1
            WHERE provider_endpoint_binding_id = $2
            """,
            stale_from,
            f"endpoint.{suffix}.active",
        )
        await conn.execute(
            """
            INSERT INTO provider_endpoint_bindings (
                provider_endpoint_binding_id,
                provider_policy_id,
                candidate_ref,
                binding_scope,
                endpoint_ref,
                endpoint_kind,
                transport_kind,
                endpoint_uri,
                auth_ref,
                binding_status,
                request_policy,
                circuit_breaker_policy,
                effective_from,
                effective_to,
                decision_ref,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb, $13, $14, $15, $16
            )
            """,
            f"endpoint.{suffix}.stale-slice",
            failover_contract.provider_policy_id,
            candidate_ref,
            failover_contract.binding_scope,
            f"endpoint.{suffix}.chat",
            "chat_completions",
            "https",
            "https://api.example.test/v1/chat/completions",
            f"secret.{suffix}.openai",
            "active",
            _json_text({"timeout_ms": 30000}),
            _json_text({"threshold": 3, "window_s": 60}),
            stale_from,
            None,
            f"decision.{suffix}.endpoint.rotated",
            stale_from,
        )

        stale_cockpit = await operator_cockpit_run_with_failover_contract(
            conn=conn,
            run_id=run_id,
            as_of=as_of,
            route_authority=_route_authority(
                as_of=as_of,
                candidate_ref=candidate_ref,
                provider_policy_id=failover_contract.provider_policy_id or "",
            ),
            failover_contract=failover_contract,
            dispatch_resolution=_dispatch_resolution(as_of=as_of),
            cutover_status=_cutover_status(as_of=as_of, run_id=run_id),
        )

        assert stale_cockpit.completeness.is_complete is True
        assert stale_cockpit.status_state == "fresh"
        assert stale_cockpit.status_reason is None
        assert stale_cockpit.failover is not None
        assert stale_cockpit.failover.completeness.is_complete is False
        assert stale_cockpit.failover.freshness_state == "stale"
        assert "failover:endpoint_slice_stale" not in stale_cockpit.completeness.missing_evidence_refs
        assert stale_cockpit.failover.freshness_reason is not None
        assert "active endpoint binding did not share the failover effective slice" in (
            stale_cockpit.failover.freshness_reason
        )
        assert (
            stale_cockpit.failover.effective_slice is not None
            and stale_cockpit.failover.effective_slice.endpoint_slice_key
            != stale_cockpit.failover.effective_slice.failover_slice_key
        )

        stale_rendered = render_operator_cockpit(stale_cockpit)
        assert "failover.freshness.state: stale" in stale_rendered
        assert "failover:endpoint_slice_stale" in stale_rendered

        missing_selector_cockpit = await operator_cockpit_run_with_failover_contract(
            conn=conn,
            run_id=run_id,
            as_of=as_of,
            route_authority=_route_authority(
                as_of=as_of,
                candidate_ref=candidate_ref,
                provider_policy_id=failover_contract.provider_policy_id or "",
            ),
            failover_contract=None,
            dispatch_resolution=_dispatch_resolution(as_of=as_of),
            cutover_status=_cutover_status(as_of=as_of, run_id=run_id),
        )

        assert missing_selector_cockpit.completeness.is_complete is True
        assert missing_selector_cockpit.status_state == "fresh"
        assert missing_selector_cockpit.failover is not None
        assert missing_selector_cockpit.failover.completeness.is_complete is False
        assert missing_selector_cockpit.failover.loaded_failover_selector_count == 0
        assert missing_selector_cockpit.failover.loaded_endpoint_selector_count == 0
        assert "failover:selector_missing" in (
            missing_selector_cockpit.failover.completeness.missing_evidence_refs
        )
        assert "failover:endpoint_selector_missing" in (
            missing_selector_cockpit.failover.completeness.missing_evidence_refs
        )

        ambiguous_selector_cockpit = await operator_cockpit_run_with_failover_contract(
            conn=conn,
            run_id=run_id,
            as_of=as_of,
            route_authority=_route_authority(
                as_of=as_of,
                candidate_ref=candidate_ref,
                provider_policy_id=failover_contract.provider_policy_id or "",
            ),
            failover_contract=NativeOperatorCockpitFailoverSelectorContract(
                model_profile_id=failover_contract.model_profile_id,
                provider_policy_id=failover_contract.provider_policy_id,
                binding_scope=failover_contract.binding_scope,
                endpoint_ref=f"endpoint.{suffix}.chat",
                endpoint_kind=failover_contract.endpoint_kind,
            ),
            dispatch_resolution=_dispatch_resolution(as_of=as_of),
            cutover_status=_cutover_status(as_of=as_of, run_id=run_id),
        )

        assert ambiguous_selector_cockpit.completeness.is_complete is True
        assert ambiguous_selector_cockpit.status_state == "fresh"
        assert ambiguous_selector_cockpit.failover is not None
        assert ambiguous_selector_cockpit.failover.completeness.is_complete is False
        assert ambiguous_selector_cockpit.failover.loaded_failover_selector_count == 1
        assert ambiguous_selector_cockpit.failover.loaded_endpoint_selector_count == 0
        assert "failover:endpoint_selector_ambiguous" in (
            ambiguous_selector_cockpit.failover.completeness.missing_evidence_refs
        )
        assert ambiguous_selector_cockpit.failover.effective_slice is not None
    finally:
        await conn.close()


def test_failover_freshness_operator_surface_owns_selector_contract_and_keeps_failures_local() -> None:
    asyncio.run(_exercise_failover_freshness_operator_surface())
