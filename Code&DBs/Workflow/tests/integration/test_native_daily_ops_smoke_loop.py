from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from observability.operator_topology import NativeCutoverGraphStatusReadModel, render_cutover_graph_status
from observability.operator_dashboard import render_operator_cockpit
from observability.read_models import (
    GraphLineageReadModel,
    GraphTopologyEdge,
    GraphTopologyNode,
    GraphTopologyReadModel,
    ProjectionCompleteness,
    ProjectionWatermark,
)
from policy.workflow_classes import (
    WorkflowClassAuthorityRecord,
    WorkflowClassCatalog,
)
from authority.workflow_class_resolution import WorkflowClassResolutionDecision
from policy.workflow_lanes import WorkflowLanePolicyAuthorityRecord
from registry.provider_routing import (
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
)
from runtime.instance import NativeWorkflowInstance
from runtime.work_item_workflow_bindings import WorkItemWorkflowBindingRecord
from runtime.recurring_review_repair_flow import (
    RecurringReviewRepairFlowRequest,
    RecurringReviewRepairFlowResolution,
)
from authority.workflow_schedule import (
    RecurringRunWindowAuthorityRecord,
    ScheduleDefinitionAuthorityRecord,
)
from runtime.scheduler_window_repository import SchedulerWindowAuthorityResolution
from surfaces.api import native_operator_surface, operator_write

import pathlib

_REPO_ROOT = str(pathlib.Path(__file__).resolve().parents[4])


def _repo_root() -> str:
    return _REPO_ROOT


def _native_instance() -> NativeWorkflowInstance:
    return NativeWorkflowInstance(
        instance_name="praxis",
        runtime_profile_ref="praxis",
        repo_root=_repo_root(),
        workdir=_repo_root(),
        receipts_dir=f"{_repo_root()}/artifacts/runtime_receipts",
        topology_dir=f"{_repo_root()}/artifacts/runtime_topology",
        runtime_profiles_config=f"{_repo_root()}/config/runtime_profiles.json",
    )


def _fixed_as_of() -> datetime:
    return datetime(2026, 4, 2, 22, 45, tzinfo=timezone.utc)


def _operator_env() -> dict[str, str]:
    return {
        "PRAXIS_RUNTIME_PROFILE": "praxis",
    }


def _workflow_class(
    *,
    class_name: str,
    class_kind: str,
    workflow_class_id: str,
    workflow_lane_id: str,
    as_of: datetime,
) -> WorkflowClassAuthorityRecord:
    return WorkflowClassAuthorityRecord(
        workflow_class_id=workflow_class_id,
        class_name=class_name,
        class_kind=class_kind,
        workflow_lane_id=workflow_lane_id,
        status="active",
        queue_shape={"mode": class_kind, "max_parallel": 1},
        throttle_policy={"dispatch_limit": 1},
        review_required=class_kind != "loop",
        effective_from=as_of - timedelta(minutes=5),
        effective_to=None,
        decision_ref=f"decision:workflow-class:{class_name}",
        created_at=as_of,
    )


def _workflow_lane_policy(
    *,
    workflow_lane_policy_id: str,
    workflow_lane_id: str,
    policy_scope: str,
    work_kind: str,
    as_of: datetime,
) -> WorkflowLanePolicyAuthorityRecord:
    return WorkflowLanePolicyAuthorityRecord(
        workflow_lane_policy_id=workflow_lane_policy_id,
        workflow_lane_id=workflow_lane_id,
        policy_scope=policy_scope,
        work_kind=work_kind,
        match_rules={"work_kind": work_kind, "operator": True},
        lane_parameters={"route_kind": work_kind, "operator_path": "bounded"},
        decision_ref=f"decision:lane-policy:{work_kind}",
        effective_from=as_of - timedelta(minutes=5),
        effective_to=None,
        created_at=as_of,
    )


def _dispatch_catalog(as_of: datetime) -> WorkflowClassCatalog:
    return WorkflowClassCatalog(
        class_records=(
            _workflow_class(
                class_name="review",
                class_kind="review",
                workflow_class_id="workflow_class.review.daily_ops",
                workflow_lane_id="workflow_lane.review.daily_ops",
                as_of=as_of,
            ),
            _workflow_class(
                class_name="repair",
                class_kind="repair",
                workflow_class_id="workflow_class.repair.daily_ops",
                workflow_lane_id="workflow_lane.repair.daily_ops",
                as_of=as_of,
            ),
            _workflow_class(
                class_name="loop",
                class_kind="loop",
                workflow_class_id="workflow_class.loop.daily_ops",
                workflow_lane_id="workflow_lane.loop.daily_ops",
                as_of=as_of,
            ),
        ),
        as_of=as_of,
    )


def _review_dispatch_decision(as_of: datetime) -> WorkflowClassResolutionDecision:
    review_class = _workflow_class(
        class_name="review",
        class_kind="review",
        workflow_class_id="workflow_class.review.daily_ops",
        workflow_lane_id="workflow_lane.review.daily_ops",
        as_of=as_of,
    )
    review_lane_policy = _workflow_lane_policy(
        workflow_lane_policy_id="workflow_lane_policy.review.daily_ops",
        workflow_lane_id="workflow_lane.review.daily_ops",
        policy_scope="workflow.daily.review",
        work_kind="review",
        as_of=as_of,
    )
    return WorkflowClassResolutionDecision(
        workflow_class=review_class,
        lane_policy=review_lane_policy,
        as_of=as_of,
    )


def _repair_dispatch_decision(as_of: datetime) -> WorkflowClassResolutionDecision:
    repair_class = _workflow_class(
        class_name="repair",
        class_kind="repair",
        workflow_class_id="workflow_class.repair.daily_ops",
        workflow_lane_id="workflow_lane.repair.daily_ops",
        as_of=as_of,
    )
    repair_lane_policy = _workflow_lane_policy(
        workflow_lane_policy_id="workflow_lane_policy.repair.daily_ops",
        workflow_lane_id="workflow_lane.repair.daily_ops",
        policy_scope="workflow.daily.repair",
        work_kind="repair",
        as_of=as_of,
    )
    return WorkflowClassResolutionDecision(
        workflow_class=repair_class,
        lane_policy=repair_lane_policy,
        as_of=as_of,
    )


def _route_authority(as_of: datetime) -> ProviderRouteAuthority:
    health_window = ProviderRouteHealthWindowAuthorityRecord(
        provider_route_health_window_id="health_window.daily_ops",
        candidate_ref="candidate.daily_ops",
        provider_ref="provider.daily_ops",
        health_status="healthy",
        health_score=1.0,
        sample_count=12,
        failure_rate=0.0,
        latency_p95_ms=18,
        observed_window_started_at=as_of - timedelta(minutes=10),
        observed_window_ended_at=as_of - timedelta(minutes=5),
        observation_ref="observation.daily_ops.health",
        created_at=as_of,
    )
    budget_window = ProviderBudgetWindowAuthorityRecord(
        provider_budget_window_id="budget_window.daily_ops",
        provider_policy_id="provider_policy.daily_ops",
        provider_ref="provider.daily_ops",
        budget_scope="daily_ops",
        budget_status="within_budget",
        window_started_at=as_of - timedelta(minutes=10),
        window_ended_at=as_of - timedelta(minutes=5),
        request_limit=20,
        requests_used=4,
        token_limit=1000,
        tokens_used=120,
        spend_limit_usd="10.00",
        spend_used_usd="1.20",
        decision_ref="decision:route:budget.daily_ops",
        created_at=as_of,
    )
    eligibility_state = RouteEligibilityStateAuthorityRecord(
        route_eligibility_state_id="route_eligibility_state.daily_ops",
        model_profile_id="model.daily_ops",
        provider_policy_id="provider_policy.daily_ops",
        candidate_ref="candidate.daily_ops",
        eligibility_status="eligible",
        reason_code="route:eligible",
        source_window_refs=(
            "health_window.daily_ops",
            "budget_window.daily_ops",
        ),
        evaluated_at=as_of,
        expires_at=as_of + timedelta(minutes=30),
        decision_ref="decision:route:eligibility.daily_ops",
        created_at=as_of,
    )
    return ProviderRouteAuthority(
        provider_route_health_windows={
            "candidate.daily_ops": (health_window,),
        },
        provider_budget_windows={
            "provider_policy.daily_ops": (budget_window,),
        },
        route_eligibility_states={
            "candidate.daily_ops": (eligibility_state,),
        },
    )


def _cutover_status(as_of: datetime) -> NativeCutoverGraphStatusReadModel:
    watermark = ProjectionWatermark(evidence_seq=99, source="canonical_evidence")
    topology = GraphTopologyReadModel(
        run_id="run.daily.ops",
        request_id="request.daily.ops",
        completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
        watermark=watermark,
        evidence_refs=("evidence:topology.daily_ops",),
        admitted_definition_ref="definition.daily.ops",
        nodes=(
            GraphTopologyNode(
                node_id="node_0",
                node_type="task",
                display_name="prepare",
                position_index=0,
            ),
        ),
        edges=(
            GraphTopologyEdge(
                edge_id="edge_0",
                edge_type="data",
                from_node_id="node_0",
                to_node_id="node_1",
                position_index=0,
            ),
        ),
        runtime_node_order=("node_0",),
    )
    lineage = GraphLineageReadModel(
        run_id="run.daily.ops",
        request_id="request.daily.ops",
        completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
        watermark=watermark,
        evidence_refs=("evidence:lineage.daily_ops",),
        claim_received_ref="claim.daily.ops",
        admitted_definition_ref="definition.daily.ops",
        admitted_definition_hash="sha256:daily-ops",
        nodes=topology.nodes,
        edges=topology.edges,
        runtime_node_order=topology.runtime_node_order,
        current_state="succeeded",
        terminal_reason="runtime.workflow_succeeded",
    )
    return NativeCutoverGraphStatusReadModel(
        run_id="run.daily.ops",
        request_id="request.daily.ops",
        watermark=watermark,
        evidence_refs=("evidence:daily_ops",),
        graph_topology=topology,
        graph_lineage=lineage,
        cutover_gates=(),
        work_bindings=(),
        completeness=ProjectionCompleteness(is_complete=True, missing_evidence_refs=()),
        status_state="ready",
        status_reason=None,
    )


def _recurring_flow_resolution(as_of: datetime) -> RecurringReviewRepairFlowResolution:
    request = RecurringReviewRepairFlowRequest(
        target_ref="workspace.daily_ops",
        schedule_kind="hourly",
        review_class_name="review",
        review_policy_scope="workflow.daily.review",
        review_work_kind="review",
        repair_class_name="repair",
        repair_policy_scope="workflow.daily.repair",
        repair_work_kind="repair",
    )
    schedule_definition = ScheduleDefinitionAuthorityRecord(
        schedule_definition_id="schedule_definition.daily_ops",
        workflow_class_id="workflow_class.review.daily_ops",
        schedule_name="daily-ops-loop",
        schedule_kind="hourly",
        status="active",
        cadence_policy={"cadence": "PT1H", "bounded": True},
        throttle_policy={"capacity_limit": 2},
        target_ref="workspace.daily_ops",
        effective_from=as_of - timedelta(minutes=5),
        effective_to=None,
        decision_ref="decision:schedule:daily_ops",
        created_at=as_of,
    )
    recurring_window = RecurringRunWindowAuthorityRecord(
        recurring_run_window_id="recurring_run_window.daily_ops",
        schedule_definition_id="schedule_definition.daily_ops",
        window_started_at=as_of - timedelta(minutes=5),
        window_ended_at=as_of + timedelta(minutes=55),
        window_status="active",
        capacity_limit=2,
        capacity_used=1,
        last_workflow_at=as_of - timedelta(minutes=10),
        created_at=as_of,
    )
    schedule_resolution = SchedulerWindowAuthorityResolution(
        schedule_definition=schedule_definition,
        recurring_run_window=recurring_window,
        as_of=as_of,
    )
    return RecurringReviewRepairFlowResolution(
        request=request,
        schedule=schedule_resolution,
        review_workflow=_review_dispatch_decision(as_of),
        repair_workflow=_repair_dispatch_decision(as_of),
        as_of=as_of,
    )


class _FakeConnection:
    def __init__(self, *, close_events: list[str], label: str) -> None:
        self._close_events = close_events
        self.label = label

    async def fetchrow(self, query: str, *params: object):
        assert params == ("run.daily.ops",)
        if "FROM workflow_runs" in query:
            return {
                "workspace_ref": _repo_root(),
                "runtime_profile_ref": "praxis",
            }
        if "FROM workflow_claim_lease_proposal_runtime" in query:
            return None
        return {
            "workspace_ref": _repo_root(),
            "runtime_profile_ref": "praxis",
        }

    async def close(self) -> None:
        self._close_events.append(f"closed:{self.label}")


@dataclass(frozen=True)
class _FakeReceipt:
    receipt_id: str
    receipt_type: str
    run_id: str
    status: str
    node_id: str | None
    failure_code: str | None
    transition_seq: int
    evidence_seq: int
    started_at: datetime
    finished_at: datetime


@dataclass(frozen=True)
class _FakeEvidenceRow:
    kind: str
    record: object


def _query_payload(*, as_of: datetime, native_instance: NativeWorkflowInstance) -> dict[str, object]:
    return {
        "kind": "operator_query",
        "as_of": as_of.isoformat(),
        "query": {
            "bug_ids": ["bug.daily.ops"],
            "roadmap_item_ids": ["roadmap.daily.ops"],
            "cutover_gate_ids": ["cutover_gate.daily.ops"],
            "work_item_workflow_binding_ids": ["binding.daily.ops"],
            "workflow_run_ids": ["run.daily.ops"],
        },
        "counts": {
            "bugs": 0,
            "roadmap_items": 0,
            "cutover_gates": 0,
            "work_item_workflow_bindings": 1,
        },
        "bugs": [],
        "roadmap_items": [],
        "cutover_gates": [],
        "work_item_workflow_bindings": [
            {
                "work_item_workflow_binding_id": "binding.daily.ops",
                "binding_kind": "governed_by",
                "binding_status": "active",
                "source": {
                    "kind": "cutover_gate",
                    "id": "cutover_gate.daily.ops",
                    "cutover_gate_id": "cutover_gate.daily.ops",
                },
                "targets": {
                    "workflow_class_id": "workflow_class.review.daily_ops",
                    "workflow_run_id": "run.daily.ops",
                },
                "bound_by_decision_id": "decision:binding.daily.ops",
                "created_at": as_of.isoformat(),
                "updated_at": as_of.isoformat(),
            }
        ],
        "native_instance": native_instance.to_contract(),
    }


def _status_payload(
    *,
    as_of: datetime,
    native_instance: NativeWorkflowInstance,
    run_id: str,
) -> dict[str, object]:
    return {
        "native_instance": native_instance.to_contract(),
        "run": {
            "run_id": run_id,
            "workflow_id": "workflow.daily.ops",
            "request_id": "request.daily.ops",
            "workflow_definition_id": "workflow_definition.daily.ops.v1",
            "current_state": "running",
            "terminal_reason_code": None,
            "run_idempotency_key": "idem.daily.ops",
            "context_bundle_id": "context.daily.ops",
            "authority_context_digest": "digest.daily.ops",
            "admission_decision_id": "admission.daily.ops",
            "requested_at": (as_of - timedelta(minutes=3)).isoformat(),
            "admitted_at": (as_of - timedelta(minutes=2)).isoformat(),
            "started_at": (as_of - timedelta(minutes=1)).isoformat(),
            "finished_at": None,
            "last_event_id": "event.daily.ops.2",
        },
        "inspection": {
            "kind": "workflow_inspection",
            "last_evidence_seq": 2,
        },
    }


def _canonical_evidence(*, as_of: datetime, run_id: str) -> tuple[_FakeEvidenceRow, ...]:
    return (
        _FakeEvidenceRow(kind="workflow_event", record={"event_id": "event.daily.ops.1"}),
        _FakeEvidenceRow(
            kind="receipt",
            record=_FakeReceipt(
                receipt_id="receipt.daily.ops.2",
                receipt_type="workflow_completion_receipt",
                run_id=run_id,
                status="succeeded",
                node_id="node_0",
                failure_code=None,
                transition_seq=1,
                evidence_seq=2,
                started_at=as_of - timedelta(minutes=1),
                finished_at=as_of,
            ),
        ),
    )


def test_native_daily_ops_smoke_loop_stays_truthful_and_boring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = _fixed_as_of()
    env = _operator_env()
    native_instance = _native_instance()
    route_authority = _route_authority(as_of)
    dispatch_catalog = _dispatch_catalog(as_of)
    review_dispatch_decision = _review_dispatch_decision(as_of)
    recurring_resolution = _recurring_flow_resolution(as_of)
    cutover_status = _cutover_status(as_of)

    query_calls: list[dict[str, object]] = []
    dispatch_calls: list[dict[str, object]] = []
    recurring_calls: list[dict[str, object]] = []
    status_calls: list[dict[str, object]] = []
    evidence_calls: list[dict[str, object]] = []
    cockpit_build_calls: list[dict[str, object]] = []
    resolve_instance_calls: list[dict[str, str]] = []
    connection_closes: list[str] = []

    def _resolve_native_instance(*, env=None):
        resolve_instance_calls.append(dict(env or {}))
        return native_instance

    def _fake_query_operator_surface(
        *,
        env=None,
        as_of=None,
        bug_ids=None,
        roadmap_item_ids=None,
        cutover_gate_ids=None,
        work_item_workflow_binding_ids=None,
        workflow_run_ids=None,
    ) -> dict[str, object]:
        query_calls.append(
            {
                "env": dict(env or {}),
                "as_of": as_of,
                "bug_ids": bug_ids,
                "roadmap_item_ids": roadmap_item_ids,
                "cutover_gate_ids": cutover_gate_ids,
                "work_item_workflow_binding_ids": work_item_workflow_binding_ids,
                "workflow_run_ids": workflow_run_ids,
            }
        )
        return _query_payload(as_of=as_of, native_instance=native_instance)

    async def _fake_load_run_scoped_work_bindings(self, *, env, run_id):
        return (
            WorkItemWorkflowBindingRecord(
                work_item_workflow_binding_id="binding.daily.ops",
                binding_kind="governed_by",
                binding_status="active",
                issue_id=None,
                roadmap_item_id=None,
                bug_id=None,
                cutover_gate_id="cutover_gate.daily.ops",
                workflow_class_id="workflow_class.review.daily_ops",
                schedule_definition_id=None,
                workflow_run_id=run_id,
                bound_by_decision_id="decision:binding.daily.ops",
                created_at=as_of,
                updated_at=as_of,
            ),
        )

    async def _fake_load_route_authority(self, *, env, as_of):
        return route_authority

    async def _fake_load_dispatch_resolution(self, *, env, as_of, work_bindings):
        return review_dispatch_decision

    async def _fake_load_cutover_status(self, *, env, run_id, as_of, work_bindings):
        return cutover_status

    async def _fake_load_fork_worktree_ownership(self, *, env, run_id):
        return {
            "kind": "native_operator_fork_worktree_ownership",
            "authority": "test.stub",
            "selector_authority": "test.stub",
            "selection_status": "not_selected",
            "selector": {"run_id": run_id},
            "provenance": {"reason_code": "test.stub"},
            "fork_worktree_binding": None,
        }

    async def _fake_load_smoke_freshness(self, *, env, as_of):
        return {
            "kind": "native_smoke_freshness",
            "authority": "workflow_runs",
            "workflow_id_prefix": "workflow.native-self-hosted-smoke",
            "freshness_slo_seconds": 86400,
            "state": "fresh",
            "as_of": as_of.isoformat(),
        }

    def _fake_frontdoor_status(*, run_id: str, env=None) -> dict[str, object]:
        status_calls.append({"run_id": run_id, "env": dict(env or {})})
        return _status_payload(as_of=as_of, native_instance=native_instance, run_id=run_id)

    async def _fake_load_canonical_evidence(self, *, env, run_id):
        evidence_calls.append({"env": dict(env or {}), "run_id": run_id})
        return _canonical_evidence(as_of=as_of, run_id=run_id)

    async def _fake_connect_database(env=None):
        label = f"connection:{len(connection_closes)}"
        return _FakeConnection(close_events=connection_closes, label=label)

    async def _fake_load_workflow_class_catalog(conn, *, as_of):
        dispatch_calls.append(
            {
                "env": env,
                "as_of": as_of,
                "connection_label": getattr(conn, "label", None),
            }
        )
        return dispatch_catalog

    async def _fake_resolve_recurring_review_repair_flow(conn, *, request, as_of):
        recurring_calls.append(
            {
                "env": env,
                "as_of": as_of,
                "request": request,
                "connection_label": getattr(conn, "label", None),
            }
        )
        return recurring_resolution

    real_operator_cockpit_run = native_operator_surface.operator_cockpit_run

    def _tracked_operator_cockpit_run(
        *,
        run_id,
        as_of,
        route_authority,
        dispatch_resolution,
        cutover_status,
    ):
        cockpit_build_calls.append(
            {
                "run_id": run_id,
                "as_of": as_of,
                "route_authority": route_authority,
                "dispatch_resolution": dispatch_resolution,
                "cutover_status": cutover_status,
            }
        )
        return real_operator_cockpit_run(
            run_id=run_id,
            as_of=as_of,
            route_authority=route_authority,
            dispatch_resolution=dispatch_resolution,
            cutover_status=cutover_status,
        )

    monkeypatch.setattr(native_operator_surface, "resolve_native_instance", _resolve_native_instance)
    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _fake_query_operator_surface)
    monkeypatch.setattr(native_operator_surface, "frontdoor_status", _fake_frontdoor_status)
    monkeypatch.setattr(native_operator_surface, "operator_cockpit_run", _tracked_operator_cockpit_run)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_route_authority",
        _fake_load_route_authority,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_dispatch_resolution",
        _fake_load_dispatch_resolution,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_cutover_status",
        _fake_load_cutover_status,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_fork_worktree_ownership",
        _fake_load_fork_worktree_ownership,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_smoke_freshness",
        _fake_load_smoke_freshness,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_canonical_evidence",
        _fake_load_canonical_evidence,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
    )

    async def _fake_load_persona_activation(self, *, env, run_id, as_of):
        return {
            "kind": "native_operator_persona_activation",
            "authority": "test.stub",
            "selector_authority": "test.stub",
            "selector": {"run_id": run_id, "as_of": as_of.isoformat()},
            "persona_profile": None,
            "persona_context_bindings": [],
        }

    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_persona_activation",
        _fake_load_persona_activation,
    )
    monkeypatch.setattr(operator_write, "resolve_native_instance", _resolve_native_instance)
    monkeypatch.setattr(operator_write, "load_workflow_class_catalog", _fake_load_workflow_class_catalog)
    monkeypatch.setattr(
        operator_write,
        "resolve_recurring_review_repair_flow",
        _fake_resolve_recurring_review_repair_flow,
    )

    daily_surface_frontdoor = native_operator_surface.NativeOperatorSurfaceFrontdoor(
        connect_database=_fake_connect_database,
    )
    dispatch_frontdoor = operator_write.NativeWorkflowFlowFrontdoor(
        connect_database=_fake_connect_database,
    )

    def _smoke_loop() -> dict[str, object]:
        surface = daily_surface_frontdoor.query_native_operator_surface(
            run_id=cutover_status.run_id,
            env=env,
            as_of=as_of,
            bug_ids=["bug.daily.ops"],
            roadmap_item_ids=["roadmap.daily.ops"],
            cutover_gate_ids=["cutover_gate.daily.ops"],
            work_item_workflow_binding_ids=["binding.daily.ops"],
        )
        dispatch_flows = dispatch_frontdoor.inspect_workflow_flows(env=env, as_of=as_of)
        recurring_flow = dispatch_frontdoor.inspect_recurring_review_repair_flow(
            request=recurring_resolution.request,
            env=env,
            as_of=as_of,
        )
        cockpit_model = native_operator_surface.operator_cockpit_run(
            run_id=cutover_status.run_id,
            as_of=as_of,
            route_authority=route_authority,
            dispatch_resolution=review_dispatch_decision,
            cutover_status=cutover_status,
        )
        cockpit_text = render_operator_cockpit(cockpit_model)
        return {
            "surface": surface,
            "dispatch_flows": dispatch_flows,
            "recurring_flow": recurring_flow,
            "cockpit": cockpit_model.to_json(),
            "cockpit_text": cockpit_text,
        }

    first = _smoke_loop()
    second = _smoke_loop()

    assert first == second
    assert first["surface"]["native_instance"] == native_instance.to_contract()
    expected_query = _query_payload(as_of=as_of, native_instance=native_instance)
    expected_query.pop("as_of")
    expected_query.pop("native_instance")
    assert first["surface"]["query"] == expected_query
    assert first["surface"]["cockpit"] == first["cockpit"]
    assert first["surface"]["cockpit"]["status_state"] == "ready"
    assert first["surface"]["cockpit"]["completeness"]["is_complete"] is True
    assert first["surface"]["status"]["run"]["run_id"] == cutover_status.run_id
    assert first["surface"]["receipts"]["terminal_status"] == "succeeded"
    assert first["dispatch_flows"]["native_instance"] == native_instance.to_contract()
    assert first["dispatch_flows"]["flow_names"] == ["review", "repair", "loop"]
    assert first["dispatch_flows"]["workflow_class_authority"] == "policy.workflow_classes"
    assert first["recurring_flow"]["native_instance"] == native_instance.to_contract()
    assert first["recurring_flow"]["recurring_flow_authority"] == "runtime.recurring_review_repair_flow"
    assert first["recurring_flow"]["recurring_review_repair_flow"]["authorities"] == {
        "workflow_class": "authority.workflow_class_resolution",
        "schedule": "runtime.scheduler_window_repository",
    }
    assert first["recurring_flow"]["recurring_review_repair_flow"]["schedule"][
        "capacity_remaining"
    ] == 1
    assert "status.state: ready" in first["cockpit_text"]
    assert "cutover.status.state: ready" in first["cockpit_text"]
    assert render_cutover_graph_status(cutover_status).startswith("kind: cutover_graph_status")

    expected_query_calls = [
        {
            "env": env,
            "as_of": as_of,
            "bug_ids": ["bug.daily.ops"],
            "roadmap_item_ids": ["roadmap.daily.ops"],
            "cutover_gate_ids": ["cutover_gate.daily.ops"],
            "work_item_workflow_binding_ids": ("binding.daily.ops",),
            "workflow_run_ids": [cutover_status.run_id],
        },
        {
            "env": env,
            "as_of": as_of,
            "bug_ids": ["bug.daily.ops"],
            "roadmap_item_ids": ["roadmap.daily.ops"],
            "cutover_gate_ids": ["cutover_gate.daily.ops"],
            "work_item_workflow_binding_ids": ("binding.daily.ops",),
            "workflow_run_ids": [cutover_status.run_id],
        },
    ]
    assert query_calls == expected_query_calls
    assert status_calls == [
        {"run_id": cutover_status.run_id, "env": env},
        {"run_id": cutover_status.run_id, "env": env},
    ]
    assert evidence_calls == [
        {"env": env, "run_id": cutover_status.run_id},
        {"env": env, "run_id": cutover_status.run_id},
    ]
    assert len(cockpit_build_calls) == 4
    assert all(call["run_id"] == cutover_status.run_id for call in cockpit_build_calls)
    assert all(call["as_of"] == as_of for call in cockpit_build_calls)
    assert all(call["route_authority"] == route_authority for call in cockpit_build_calls)
    assert all(call["dispatch_resolution"] == review_dispatch_decision for call in cockpit_build_calls)
    assert all(call["cutover_status"] == cutover_status for call in cockpit_build_calls)
    assert resolve_instance_calls == [env, env, env, env, env, env]
    assert len(connection_closes) == 6
    assert all(event.startswith("closed:") for event in connection_closes)
    assert len(dispatch_calls) == 2
    assert len(recurring_calls) == 2
    assert dispatch_calls[0]["as_of"] == as_of
    assert recurring_calls[0]["request"] == recurring_resolution.request
