from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from runtime.work_item_workflow_bindings import WorkItemWorkflowBindingRecord
from surfaces.api import native_operator_surface


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


async def _fake_load_persona_activation(self, *, env, run_id, as_of):
    del self, env
    return {
        "kind": "native_operator_persona_activation",
        "authority": "test.stub",
        "selector_authority": "test.stub",
        "selector": {
            "run_id": run_id,
            "binding_scope": "native_runtime",
            "workspace_ref": "workspace.test",
            "runtime_profile_ref": "runtime_profile.test",
            "as_of": as_of.isoformat(),
        },
        "persona_profile": None,
        "persona_context_bindings": [],
    }


def test_native_truth_surface_cutover_publishes_status_receipts_and_cockpit(monkeypatch) -> None:
    as_of = datetime(2026, 4, 2, 23, 0, tzinfo=timezone.utc)
    env = {"PRAXIS_RUNTIME_PROFILE": "praxis"}
    run_id = "run.truth-surface.alpha"
    query_calls: list[dict[str, object]] = []
    status_calls: list[dict[str, object]] = []
    evidence_calls: list[dict[str, object]] = []
    route_calls: list[dict[str, object]] = []
    dispatch_calls: list[dict[str, object]] = []
    cutover_calls: list[dict[str, object]] = []
    cockpit_calls: list[dict[str, object]] = []

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "praxis"}

    class _FakeCockpit:
        def to_json(self) -> dict[str, object]:
            return {"kind": "operator_cockpit", "status_state": "ready"}

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
        assert as_of is not None
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": None,
                "roadmap_item_ids": None,
                "cutover_gate_ids": None,
                "work_item_workflow_binding_ids": ["binding.truth.1"],
                "workflow_run_ids": [run_id],
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
                    "work_item_workflow_binding_id": "binding.truth.1",
                    "binding_kind": "governed_by",
                    "binding_status": "active",
                    "source": {"kind": "cutover_gate", "id": "gate.truth.1"},
                    "targets": {
                        "workflow_class_id": "dispatch.truth.1",
                        "workflow_run_id": run_id,
                    },
                    "bound_by_decision_id": "decision.truth.1",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "native_instance": {"kind": "native_instance", "label": "praxis"},
        }

    async def _fake_load_run_scoped_work_bindings(self, *, env, run_id):
        return (
            WorkItemWorkflowBindingRecord(
                work_item_workflow_binding_id="binding.truth.1",
                binding_kind="governed_by",
                binding_status="active",
                roadmap_item_id=None,
                bug_id=None,
                cutover_gate_id="gate.truth.1",
                workflow_class_id="dispatch.truth.1",
                schedule_definition_id=None,
                workflow_run_id=run_id,
                bound_by_decision_id="decision.truth.1",
                created_at=as_of,
                updated_at=as_of,
            ),
        )

    def _fake_frontdoor_status(*, run_id: str, env=None) -> dict[str, object]:
        status_calls.append({"run_id": run_id, "env": dict(env or {})})
        return {
            "native_instance": {"kind": "native_instance", "label": "praxis"},
            "run": {
                "run_id": run_id,
                "workflow_id": "workflow.truth.alpha",
                "request_id": "request.truth.alpha",
                "workflow_definition_id": "workflow_definition.truth.alpha.v1",
                "current_state": "running",
                "terminal_reason_code": None,
                "run_idempotency_key": "idem.truth.alpha",
                "context_bundle_id": "context.truth.alpha",
                "authority_context_digest": "digest.truth.alpha",
                "admission_decision_id": "admission.truth.alpha",
                "requested_at": (as_of - timedelta(minutes=3)).isoformat(),
                "admitted_at": (as_of - timedelta(minutes=2)).isoformat(),
                "started_at": (as_of - timedelta(minutes=1)).isoformat(),
                "finished_at": None,
                "last_event_id": "event.truth.4",
            },
            "inspection": {
                "kind": "workflow_inspection",
                "last_evidence_seq": 4,
            },
        }

    async def _fake_load_canonical_evidence(self, *, env, run_id):
        evidence_calls.append({"env": dict(env or {}), "run_id": run_id})
        return (
            _FakeEvidenceRow(kind="workflow_event", record={"event_id": "event.truth.1"}),
            _FakeEvidenceRow(
                kind="receipt",
                record=_FakeReceipt(
                    receipt_id="receipt.truth.2",
                    receipt_type="claim_received_receipt",
                    run_id=run_id,
                    status="running",
                    node_id=None,
                    failure_code=None,
                    transition_seq=1,
                    evidence_seq=2,
                    started_at=as_of - timedelta(minutes=3),
                    finished_at=as_of - timedelta(minutes=3),
                ),
            ),
            _FakeEvidenceRow(
                kind="receipt",
                record=_FakeReceipt(
                    receipt_id="receipt.truth.4",
                    receipt_type="workflow_completion_receipt",
                    run_id=run_id,
                    status="succeeded",
                    node_id="node.final",
                    failure_code=None,
                    transition_seq=2,
                    evidence_seq=4,
                    started_at=as_of - timedelta(minutes=1),
                    finished_at=as_of,
                ),
            ),
        )

    async def _fake_load_route_authority(self, *, env, as_of):
        route_calls.append({"env": dict(env or {}), "as_of": as_of})
        return {"route_rows": 1}

    async def _fake_load_dispatch_resolution(self, *, env, as_of, work_bindings):
        dispatch_calls.append(
            {
                "env": dict(env or {}),
                "as_of": as_of,
                "work_bindings": tuple(work_bindings),
            }
        )
        return {"dispatch_rows": 1}

    async def _fake_load_cutover_status(self, *, env, run_id, as_of, work_bindings):
        cutover_calls.append(
            {
                "env": dict(env or {}),
                "run_id": run_id,
                "as_of": as_of,
                "work_bindings": tuple(work_bindings),
            }
        )
        return {"cutover_rows": 1}

    def _fake_operator_cockpit_run(
        *,
        run_id,
        as_of,
        route_authority,
        dispatch_resolution,
        cutover_status,
    ) -> _FakeCockpit:
        cockpit_calls.append(
            {
                "run_id": run_id,
                "as_of": as_of,
                "route_authority": route_authority,
                "dispatch_resolution": dispatch_resolution,
                "cutover_status": cutover_status,
            }
        )
        return _FakeCockpit()

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _fake_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
    )
    monkeypatch.setattr(native_operator_surface, "frontdoor_status", _fake_frontdoor_status)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_canonical_evidence",
        _fake_load_canonical_evidence,
    )
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
        native_operator_surface,
        "operator_cockpit_run",
        _fake_operator_cockpit_run,
    )
    monkeypatch.setattr(
        native_operator_surface,
        "resolve_native_instance",
        lambda env=None: _FakeInstance(),
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_persona_activation",
        _fake_load_persona_activation,
    )

    payload = native_operator_surface.query_native_operator_surface(
        run_id=run_id,
        env=env,
        as_of=as_of,
    )

    assert payload["kind"] == "native_operator_surface"
    assert payload["run_id"] == run_id
    assert payload["status"] == {
        "kind": "native_operator_status_truth",
        "authority": "surfaces.api.frontdoor.status",
        "run": {
            "run_id": run_id,
            "workflow_id": "workflow.truth.alpha",
            "request_id": "request.truth.alpha",
            "workflow_definition_id": "workflow_definition.truth.alpha.v1",
            "current_state": "running",
            "terminal_reason_code": None,
            "run_idempotency_key": "idem.truth.alpha",
            "context_bundle_id": "context.truth.alpha",
            "authority_context_digest": "digest.truth.alpha",
            "admission_decision_id": "admission.truth.alpha",
            "requested_at": (as_of - timedelta(minutes=3)).isoformat(),
            "admitted_at": (as_of - timedelta(minutes=2)).isoformat(),
            "started_at": (as_of - timedelta(minutes=1)).isoformat(),
            "finished_at": None,
            "last_event_id": "event.truth.4",
        },
        "inspection": {
            "kind": "workflow_inspection",
            "last_evidence_seq": 4,
        },
    }
    assert payload["receipts"] == {
        "kind": "native_operator_receipt_truth",
        "authority": "storage.postgres.PostgresEvidenceReader.load_evidence_timeline",
        "run_id": run_id,
        "evidence_row_count": 3,
        "receipt_count": 2,
        "latest_evidence_seq": 4,
        "latest_receipt_id": "receipt.truth.4",
        "latest_receipt_type": "workflow_completion_receipt",
        "terminal_status": "succeeded",
        "status_counts": {"running": 1, "succeeded": 1},
        "receipts": [
            {
                "receipt_id": "receipt.truth.2",
                "receipt_type": "claim_received_receipt",
                "status": "running",
                "node_id": None,
                "failure_code": None,
                "transition_seq": 1,
                "evidence_seq": 2,
                "started_at": (as_of - timedelta(minutes=3)).isoformat(),
                "finished_at": (as_of - timedelta(minutes=3)).isoformat(),
            },
            {
                "receipt_id": "receipt.truth.4",
                "receipt_type": "workflow_completion_receipt",
                "status": "succeeded",
                "node_id": "node.final",
                "failure_code": None,
                "transition_seq": 2,
                "evidence_seq": 4,
                "started_at": (as_of - timedelta(minutes=1)).isoformat(),
                "finished_at": as_of.isoformat(),
            },
        ],
    }
    assert payload["cockpit"] == {"kind": "operator_cockpit", "status_state": "ready"}
    assert payload["query"]["kind"] == "operator_query"
    assert "native_instance" not in payload["query"]
    assert "as_of" not in payload["query"]
    assert "graph" not in payload
    assert "cutover_status" not in payload

    assert query_calls == [
        {
            "env": env,
            "as_of": as_of,
            "bug_ids": None,
            "roadmap_item_ids": None,
            "cutover_gate_ids": None,
            "work_item_workflow_binding_ids": ("binding.truth.1",),
            "workflow_run_ids": [run_id],
        }
    ]
    assert status_calls == [{"run_id": run_id, "env": env}]
    assert evidence_calls == [{"env": env, "run_id": run_id}]
    assert route_calls == [{"env": env, "as_of": as_of}]
    assert dispatch_calls[0]["env"] == env
    assert dispatch_calls[0]["as_of"] == as_of
    assert len(dispatch_calls[0]["work_bindings"]) == 1
    assert dispatch_calls[0]["work_bindings"][0].workflow_run_id == run_id
    assert cutover_calls[0]["env"] == env
    assert cutover_calls[0]["run_id"] == run_id
    assert cutover_calls[0]["as_of"] == as_of
    assert len(cutover_calls[0]["work_bindings"]) == 1
    assert cutover_calls[0]["work_bindings"][0].workflow_run_id == run_id
    assert cockpit_calls == [
        {
            "run_id": run_id,
            "as_of": as_of,
            "route_authority": {"route_rows": 1},
            "dispatch_resolution": {"dispatch_rows": 1},
            "cutover_status": {"cutover_rows": 1},
        }
    ]

    def _mismatched_frontdoor_status(*, run_id: str, env=None) -> dict[str, object]:
        return {
            "native_instance": {"kind": "native_instance", "label": "drifted"},
            "run": {"run_id": run_id},
            "inspection": None,
        }

    monkeypatch.setattr(native_operator_surface, "frontdoor_status", _mismatched_frontdoor_status)

    with pytest.raises(native_operator_surface.NativeOperatorSurfaceError) as exc_info:
        native_operator_surface.query_native_operator_surface(
            run_id=run_id,
            env=env,
            as_of=as_of,
        )

    assert exc_info.value.reason_code == "native_operator_surface.status_native_instance_mismatch"


def test_native_truth_surface_cutover_keeps_activity_feedback_evidence_linked(
    monkeypatch,
) -> None:
    as_of = datetime(2026, 4, 3, 1, 0, tzinfo=timezone.utc)
    env = {"PRAXIS_RUNTIME_PROFILE": "praxis"}
    run_id = "run.truth-feedback.alpha"
    receipt_id = "receipt.truth.feedback.3"
    bug_id = "bug.truth.feedback.1"
    roadmap_item_id = "roadmap_item.truth.feedback.1"
    query_calls: list[dict[str, object]] = []

    class _FakeInstance:
        def to_contract(self) -> dict[str, object]:
            return {"kind": "native_instance", "label": "praxis"}

    class _FakeCockpit:
        def to_json(self) -> dict[str, object]:
            return {
                "kind": "operator_cockpit",
                "status_state": "ready",
                "activity_feedback": {
                    "bug_id": bug_id,
                    "roadmap_item_id": roadmap_item_id,
                    "evidence_links": [
                        {"kind": "workflow_run", "ref": run_id},
                        {"kind": "receipt", "ref": receipt_id},
                        {"kind": "retrieval_metric", "ref": "search:truthfb01"},
                        {"kind": "debate_consensus", "ref": "debate.truth.feedback"},
                    ],
                },
            }

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
        assert as_of is not None
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat(),
            "query": {
                "bug_ids": None,
                "roadmap_item_ids": None,
                "cutover_gate_ids": None,
                "work_item_workflow_binding_ids": ["binding.truth.feedback.1"],
                "workflow_run_ids": [run_id],
            },
            "counts": {
                "bugs": 1,
                "roadmap_items": 1,
                "cutover_gates": 0,
                "work_item_workflow_bindings": 1,
            },
            "bugs": [
                {
                    "bug_id": bug_id,
                    "bug_key": "BUG-TRUTH-FEEDBACK-1",
                    "title": "Observed activity drift needs explicit follow-up",
                    "status": "open",
                    "severity": "medium",
                    "priority": "p1",
                    "summary": (
                        "Observed run activity and durable observability artifacts disagree "
                        "with the current modeled expectation."
                    ),
                    "source_kind": "activity_truth_loop",
                    "discovered_in_run_id": run_id,
                    "discovered_in_receipt_id": receipt_id,
                    "owner_ref": None,
                    "decision_ref": "decision.truth.feedback.1",
                    "resolution_summary": None,
                    "opened_at": as_of.isoformat(),
                    "resolved_at": None,
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "roadmap_items": [
                {
                    "roadmap_item_id": roadmap_item_id,
                    "roadmap_key": "roadmap.truth.feedback.1",
                    "title": "Review activity truth drift",
                    "item_kind": "capability",
                    "status": "proposed",
                    "priority": "p1",
                    "parent_roadmap_item_id": None,
                    "source_bug_id": bug_id,
                    "summary": "Turn observed activity drift into an explicit reviewed change.",
                    "acceptance_criteria": {
                        "must_have": [
                            "Keep canonical definitions immutable until an explicit decision lands."
                        ]
                    },
                    "decision_ref": "decision.truth.feedback.1",
                    "target_start_at": None,
                    "target_end_at": None,
                    "completed_at": None,
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "cutover_gates": [],
            "work_item_workflow_bindings": [
                {
                    "work_item_workflow_binding_id": "binding.truth.feedback.1",
                    "binding_kind": "observed_feedback",
                    "binding_status": "active",
                    "source": {"kind": "roadmap_item", "id": roadmap_item_id},
                    "targets": {"workflow_run_id": run_id},
                    "bound_by_decision_id": "decision.truth.feedback.1",
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
            "native_instance": {"kind": "native_instance", "label": "praxis"},
        }

    async def _fake_load_run_scoped_work_bindings(self, *, env, run_id):
        return (
            WorkItemWorkflowBindingRecord(
                work_item_workflow_binding_id="binding.truth.feedback.1",
                binding_kind="observed_feedback",
                binding_status="active",
                roadmap_item_id=roadmap_item_id,
                bug_id=None,
                cutover_gate_id=None,
                workflow_class_id=None,
                schedule_definition_id=None,
                workflow_run_id=run_id,
                bound_by_decision_id="decision.truth.feedback.1",
                created_at=as_of,
                updated_at=as_of,
            ),
        )

    def _fake_frontdoor_status(*, run_id: str, env=None) -> dict[str, object]:
        return {
            "native_instance": {"kind": "native_instance", "label": "praxis"},
            "run": {
                "run_id": run_id,
                "workflow_id": "workflow.truth.feedback",
                "request_id": "request.truth.feedback",
                "workflow_definition_id": "workflow_definition.truth.feedback.v1",
                "current_state": "succeeded",
                "terminal_reason_code": None,
                "run_idempotency_key": "idem.truth.feedback",
                "context_bundle_id": "context.truth.feedback",
                "authority_context_digest": "digest.truth.feedback",
                "admission_decision_id": "admission.truth.feedback",
                "requested_at": (as_of - timedelta(minutes=3)).isoformat(),
                "admitted_at": (as_of - timedelta(minutes=2)).isoformat(),
                "started_at": (as_of - timedelta(minutes=1)).isoformat(),
                "finished_at": as_of.isoformat(),
                "last_event_id": "event.truth.feedback.2",
            },
            "inspection": {
                "kind": "workflow_inspection",
                "last_evidence_seq": 3,
            },
        }

    async def _fake_load_canonical_evidence(self, *, env, run_id):
        return (
            _FakeEvidenceRow(kind="workflow_event", record={"event_id": "event.truth.feedback.1"}),
            _FakeEvidenceRow(
                kind="receipt",
                record=_FakeReceipt(
                    receipt_id=receipt_id,
                    receipt_type="workflow_completion_receipt",
                    run_id=run_id,
                    status="succeeded",
                    node_id="node.final",
                    failure_code=None,
                    transition_seq=2,
                    evidence_seq=3,
                    started_at=as_of - timedelta(minutes=1),
                    finished_at=as_of,
                ),
            ),
        )

    async def _fake_load_route_authority(self, *, env, as_of):
        return {"route_rows": 1}

    async def _fake_load_dispatch_resolution(self, *, env, as_of, work_bindings):
        return {"dispatch_rows": 1}

    async def _fake_load_cutover_status(self, *, env, run_id, as_of, work_bindings):
        return {"cutover_rows": 1}

    def _fake_operator_cockpit_run(
        *,
        run_id,
        as_of,
        route_authority,
        dispatch_resolution,
        cutover_status,
    ) -> _FakeCockpit:
        return _FakeCockpit()

    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _fake_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_run_scoped_work_bindings",
        _fake_load_run_scoped_work_bindings,
    )
    monkeypatch.setattr(native_operator_surface, "frontdoor_status", _fake_frontdoor_status)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_canonical_evidence",
        _fake_load_canonical_evidence,
    )
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
        native_operator_surface,
        "operator_cockpit_run",
        _fake_operator_cockpit_run,
    )
    monkeypatch.setattr(
        native_operator_surface,
        "resolve_native_instance",
        lambda env=None: _FakeInstance(),
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_persona_activation",
        _fake_load_persona_activation,
    )

    payload = native_operator_surface.query_native_operator_surface(
        run_id=run_id,
        env=env,
        as_of=as_of,
    )

    assert payload["status"]["run"]["workflow_definition_id"] == "workflow_definition.truth.feedback.v1"
    assert payload["receipts"]["latest_receipt_id"] == receipt_id
    assert payload["query"]["counts"] == {
        "bugs": 1,
        "roadmap_items": 1,
        "cutover_gates": 0,
        "work_item_workflow_bindings": 1,
    }
    assert payload["query"]["bugs"] == [
        {
            "bug_id": bug_id,
            "bug_key": "BUG-TRUTH-FEEDBACK-1",
            "title": "Observed activity drift needs explicit follow-up",
            "status": "open",
            "severity": "medium",
            "priority": "p1",
            "summary": (
                "Observed run activity and durable observability artifacts disagree "
                "with the current modeled expectation."
            ),
            "source_kind": "activity_truth_loop",
            "discovered_in_run_id": run_id,
            "discovered_in_receipt_id": receipt_id,
            "owner_ref": None,
            "decision_ref": "decision.truth.feedback.1",
            "resolution_summary": None,
            "opened_at": as_of.isoformat(),
            "resolved_at": None,
            "created_at": as_of.isoformat(),
            "updated_at": as_of.isoformat(),
        }
    ]
    assert payload["query"]["roadmap_items"] == [
        {
            "roadmap_item_id": roadmap_item_id,
            "roadmap_key": "roadmap.truth.feedback.1",
            "title": "Review activity truth drift",
            "item_kind": "capability",
            "status": "proposed",
            "priority": "p1",
            "parent_roadmap_item_id": None,
            "source_bug_id": bug_id,
            "summary": "Turn observed activity drift into an explicit reviewed change.",
            "acceptance_criteria": {
                "must_have": [
                    "Keep canonical definitions immutable until an explicit decision lands."
                ]
            },
            "decision_ref": "decision.truth.feedback.1",
            "target_start_at": None,
            "target_end_at": None,
            "completed_at": None,
            "created_at": as_of.isoformat(),
            "updated_at": as_of.isoformat(),
        }
    ]
    assert payload["instruction_authority"]["roadmap_truth"] == {
        "authority": "surfaces.api.operator_read.query_operator_surface",
        "roadmap_item_ids": [roadmap_item_id],
        "items": [
            {
                "roadmap_item_id": roadmap_item_id,
                "title": "Review activity truth drift",
                "status": "proposed",
                "priority": "p1",
                "decision_ref": "decision.truth.feedback.1",
            }
        ],
    }
    assert payload["instruction_authority"]["queue_refs"] == {
        "run_id": run_id,
        "workflow_id": "workflow.truth.feedback",
        "request_id": "request.truth.feedback",
        "workflow_definition_id": "workflow_definition.truth.feedback.v1",
        "context_bundle_id": "context.truth.feedback",
        "work_item_workflow_binding_ids": ["binding.truth.feedback.1"],
        "roadmap_item_ids": [roadmap_item_id],
    }
    assert payload["cockpit"] == {
        "kind": "operator_cockpit",
        "status_state": "ready",
        "activity_feedback": {
            "bug_id": bug_id,
            "roadmap_item_id": roadmap_item_id,
            "evidence_links": [
                {"kind": "workflow_run", "ref": run_id},
                {"kind": "receipt", "ref": receipt_id},
                {"kind": "retrieval_metric", "ref": "search:truthfb01"},
                {"kind": "debate_consensus", "ref": "debate.truth.feedback"},
            ],
        },
    }
    assert query_calls == [
        {
            "env": env,
            "as_of": as_of,
            "bug_ids": None,
            "roadmap_item_ids": None,
            "cutover_gate_ids": None,
            "work_item_workflow_binding_ids": ("binding.truth.feedback.1",),
            "workflow_run_ids": [run_id],
        }
    ]
