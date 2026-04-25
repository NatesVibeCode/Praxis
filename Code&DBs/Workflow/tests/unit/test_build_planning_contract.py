from __future__ import annotations

from runtime.build_planning_contract import (
    build_candidate_resolution_manifest,
    build_reviewable_plan,
)


def _base_definition() -> dict[str, object]:
    return {
        "type": "operating_model",
        "references": [
            {
                "id": "ref-001",
                "type": "integration",
                "slug": "@gmail/search",
                "raw": "@gmail/search",
                "resolved": True,
                "resolved_to": "integration_registry:gmail/search",
            }
        ],
        "draft_flow": [
            {
                "id": "step-001",
                "title": "Review support inbox",
                "summary": "Review the support inbox.",
                "reference_slugs": ["@gmail/search"],
                "depends_on": [],
                "order": 1,
            }
        ],
        "definition_revision": "def_candidate_manifest_alpha",
    }


def _definition_with_build_graph() -> dict[str, object]:
    definition = _base_definition()
    definition["build_graph"] = {
        "graph_id": "shape:support-inbox",
        "nodes": [
            {
                "node_id": "step-001",
                "id": "step-001",
                "kind": "step",
                "title": "Review support inbox",
                "summary": "Review the support inbox.",
                "route": "llm_task",
            }
        ],
        "edges": [],
        "projection_status": {"state": "ready"},
    }
    return definition


def test_candidate_resolution_manifest_surfaces_proposals_only(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.build_planning_contract.effective_workflow_build_review_state",
        lambda conn, workflow_id, definition_revision: {
            "review_group_ref": None,
            "latest_records": [],
            "latest_by_target": {},
            "approval_records": [],
            "approved_binding_refs": [],
            "approved_import_snapshot_refs": [],
            "approved_bundle_refs": [],
            "approved_workflow_shape_ref": None,
            "proposal_requests": [],
            "widening_ops": [],
        },
    )

    manifest = build_candidate_resolution_manifest(
        definition=_base_definition(),
        workflow_id="wf_alpha",
        conn=object(),
    )

    assert manifest["execution_readiness"] == "review_required"
    assert manifest["blocking_issues"] == []
    assert manifest["review_gates"]
    slot = manifest["binding_slots"][0]
    assert slot["slot_ref"] == "binding:ref-001"
    assert slot["candidate_resolution_state"] == "candidate_set"
    assert slot["approval_state"] == "unapproved"
    assert slot["top_ranked_ref"] == "integration_registry:gmail/search"
    assert slot["approved_ref"] is None
    assert slot["candidates"][0]["candidate_approval_state"] == "proposed"
    assert manifest["workflow_shape_candidates"][0]["approval_state"] == "unapproved"


def test_candidate_resolution_manifest_projects_the_same_shape_into_storage(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _PlanningConn:
        def fetchrow(self, *_args, **_kwargs):
            return {"manifest_ref": "candidate_manifest:wf_alpha:def_candidate_manifest_alpha"}

        def execute(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(
        "runtime.build_planning_contract.effective_workflow_build_review_state",
        lambda conn, workflow_id, definition_revision: {
            "review_group_ref": None,
            "latest_records": [],
            "latest_by_target": {},
            "approval_records": [],
            "approved_binding_refs": [],
            "approved_import_snapshot_refs": [],
            "approved_bundle_refs": [],
            "approved_workflow_shape_ref": None,
            "proposal_requests": [],
            "widening_ops": [],
        },
    )
    monkeypatch.setattr("runtime.build_planning_contract._load_shape_family_defs", lambda conn: [])
    monkeypatch.setattr("runtime.build_planning_contract._load_bundle_defs", lambda conn: [])
    monkeypatch.setattr(
        "runtime.build_planning_contract.replace_workflow_build_candidate_manifest",
        lambda conn, **kwargs: captured.setdefault("projection", kwargs),
    )
    monkeypatch.setattr(
        "runtime.build_planning_contract.upsert_workflow_build_review_session",
        lambda conn, **kwargs: captured.setdefault("review_session", kwargs),
    )

    manifest = build_candidate_resolution_manifest(
        definition=_definition_with_build_graph(),
        workflow_id="wf_alpha",
        conn=_PlanningConn(),
    )

    projection = captured["projection"]
    assert projection["manifest_ref"] == manifest["manifest_ref"]
    assert projection["workflow_id"] == "wf_alpha"
    assert projection["definition_revision"] == "def_candidate_manifest_alpha"
    assert projection["manifest_revision"] == manifest["manifest_revision"]
    assert projection["intent_ref"] == manifest["intent_ref"]
    assert projection["review_group_ref"] == "workflow_build:wf_alpha:def_candidate_manifest_alpha"
    assert projection["execution_readiness"] == manifest["execution_readiness"]
    assert projection["projection_status"] == manifest["projection_status"]
    assert projection["blocking_issues"] == manifest["blocking_issues"]
    assert projection["required_confirmations"] == manifest["required_confirmations"]
    assert len(projection["slots"]) == len(manifest["binding_slots"]) + len(manifest["workflow_shape_candidates"])
    assert len(projection["candidates"]) == sum(
        len(slot["candidates"]) for slot in manifest["binding_slots"]
    ) + len(manifest["workflow_shape_candidates"])

    binding_slot = next(slot for slot in projection["slots"] if slot["slot_ref"] == "binding:ref-001")
    assert binding_slot["slot_kind"] == "binding"
    assert binding_slot["source_binding_ref"] == "binding:ref-001"
    assert binding_slot["candidate_resolution_state"] == "candidate_set"
    assert binding_slot["approval_state"] == "unapproved"
    assert binding_slot["top_ranked_ref"] == "integration_registry:gmail/search"
    assert binding_slot["approved_ref"] is None
    assert binding_slot["slot_metadata"] == {
        "source_label": "@gmail/search",
        "source_node_ids": ["step-001"],
        "blocking_issue_ids": ["issue:binding:ref-001"],
        "freshness": None,
    }

    shape_slot = next(slot for slot in projection["slots"] if slot["slot_ref"] == "workflow_shape")
    assert shape_slot["slot_kind"] == "workflow_shape"
    assert shape_slot["candidate_resolution_state"] == "candidate_set"
    assert shape_slot["approval_state"] == "unapproved"
    assert shape_slot["slot_metadata"] == {"shape_family_ref": None}
    assert shape_slot["top_ranked_ref"] == "workflow_shape:def_candidate_manifest_alpha"

    binding_candidate = next(candidate for candidate in projection["candidates"] if candidate["slot_ref"] == "binding:ref-001")
    assert binding_candidate["target_kind"] == "integration"
    assert binding_candidate["target_ref"] == "integration_registry:gmail/search"
    assert binding_candidate["candidate_approval_state"] == "proposed"
    assert binding_candidate["candidate_rationale"] == manifest["binding_slots"][0]["rationale"]

    shape_candidate = next(candidate for candidate in projection["candidates"] if candidate["slot_ref"] == "workflow_shape")
    assert shape_candidate["target_kind"] == "workflow_shape"
    assert shape_candidate["target_ref"] == "workflow_shape:def_candidate_manifest_alpha"
    assert shape_candidate["candidate_approval_state"] == "proposed"
    assert shape_candidate["candidate_rationale"] == "Current workflow shape candidate"


def test_reviewable_plan_tracks_approvals_and_proposal_requests(monkeypatch) -> None:
    decisions = [
        {
            "review_decision_id": "wbrd_binding_approve",
            "workflow_id": "wf_alpha",
            "definition_revision": "def_candidate_manifest_alpha",
            "review_group_ref": "workflow_build:wf_alpha:def_candidate_manifest_alpha",
            "target_kind": "binding",
            "target_ref": "binding:ref-001",
            "slot_ref": "binding:ref-001",
            "decision": "approve",
            "actor_type": "model",
            "actor_ref": "planner-agent",
            "authority_scope": "workflow_build/binding",
            "approval_mode": "review",
            "rationale": "Top ranked Gmail search binding is correct.",
            "source_subpath": "review_decisions",
            "supersedes_decision_ref": None,
            "candidate_ref": "integration_registry:gmail/search",
            "candidate_payload": {
                "target_ref": "integration_registry:gmail/search",
                "label": "Gmail Search",
                "kind": "integration",
            },
            "decided_at": "2026-04-15T12:00:00Z",
        },
        {
            "review_decision_id": "wbrd_shape_widen",
            "workflow_id": "wf_alpha",
            "definition_revision": "def_candidate_manifest_alpha",
            "review_group_ref": "workflow_build:wf_alpha:def_candidate_manifest_alpha",
            "target_kind": "workflow_shape",
            "target_ref": "shape:current",
            "slot_ref": "workflow_shape",
            "decision": "widen",
            "actor_type": "human",
            "actor_ref": "praxis-admin",
            "authority_scope": "workflow_build/workflow_shape",
            "approval_mode": "manual",
            "rationale": "Show an alternate escalation-only path.",
            "source_subpath": "review_decisions",
            "supersedes_decision_ref": "wbrd_shape_old",
            "candidate_ref": None,
            "candidate_payload": {
                "operation": "request_alternate_workflow_shapes",
                "count": 2,
            },
            "decided_at": "2026-04-15T12:05:00Z",
        },
        {
            "review_decision_id": "wbrd_bundle_proposal",
            "workflow_id": "wf_alpha",
            "definition_revision": "def_candidate_manifest_alpha",
            "review_group_ref": "workflow_build:wf_alpha:def_candidate_manifest_alpha",
            "target_kind": "capability_bundle",
            "target_ref": "capability_bundle:email_triage",
            "slot_ref": "capability_bundle:support_triage",
            "decision": "proposal_request",
            "actor_type": "policy",
            "actor_ref": "workflow.review.policy",
            "authority_scope": "workflow_build/capability_bundle",
            "approval_mode": "policy",
            "rationale": "Surface the outbound-reply bundle for explicit review.",
            "source_subpath": "review_decisions",
            "supersedes_decision_ref": None,
            "candidate_ref": "bundle:email-triage-reply",
            "candidate_payload": {
                "bundle_ref": "bundle:email-triage-reply",
                "family": "support_triage",
            },
            "decided_at": "2026-04-15T12:10:00Z",
        },
    ]
    monkeypatch.setattr(
        "runtime.build_planning_contract.effective_workflow_build_review_state",
        lambda conn, workflow_id, definition_revision: {
            "review_group_ref": "workflow_build:wf_alpha:def_candidate_manifest_alpha",
            "latest_records": decisions,
            "latest_by_target": {
                (str(item["target_kind"]), str(item["target_ref"])): item for item in decisions
            },
            "approval_records": decisions,
            "approved_binding_refs": ["binding:ref-001"],
            "approved_import_snapshot_refs": [],
            "approved_bundle_refs": [],
            "approved_workflow_shape_ref": None,
            "proposal_requests": [decisions[2]],
            "widening_ops": [decisions[1]],
        },
    )

    manifest = build_candidate_resolution_manifest(
        definition=_base_definition(),
        workflow_id="wf_alpha",
        conn=object(),
    )
    reviewable_plan = build_reviewable_plan(
        definition=_base_definition(),
        workflow_id="wf_alpha",
        conn=object(),
        candidate_manifest=manifest,
    )

    slot = manifest["binding_slots"][0]
    assert slot["approval_state"] == "approved"
    assert slot["approved_ref"] == "integration_registry:gmail/search"
    assert slot["review_provenance"]["actor_type"] == "model"
    assert reviewable_plan["approved_binding_refs"] == [
        {
            "slot_ref": "binding:ref-001",
            "candidate_ref": "integration_registry:gmail/search",
        }
    ]
    assert reviewable_plan["proposal_requests"][0]["target_kind"] == "capability_bundle"
    assert reviewable_plan["proposal_requests"][0]["requested_by"]["actor_type"] == "policy"
    assert reviewable_plan["proposal_requests"][0]["authority_scope"] == "workflow_build/capability_bundle"
    assert reviewable_plan["widening_ops"][0]["operation"] == {
        "operation": "request_alternate_workflow_shapes",
        "count": 2,
    }
    assert reviewable_plan["widening_ops"][0]["supersedes_decision_ref"] == "wbrd_shape_old"
    assert reviewable_plan["status"] == "needs_proposals"
