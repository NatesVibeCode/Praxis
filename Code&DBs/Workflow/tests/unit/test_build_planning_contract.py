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


def test_candidate_resolution_manifest_surfaces_proposals_only(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.build_planning_contract.list_latest_workflow_build_review_decisions",
        lambda conn, workflow_id, definition_revision: [],
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


def test_reviewable_plan_tracks_approvals_and_proposal_requests(monkeypatch) -> None:
    decisions = [
        {
            "review_decision_id": "wbrd_binding_approve",
            "workflow_id": "wf_alpha",
            "definition_revision": "def_candidate_manifest_alpha",
            "target_kind": "binding",
            "target_ref": "binding:ref-001",
            "decision": "approve",
            "actor_type": "model",
            "actor_ref": "planner-agent",
            "approval_mode": "review",
            "rationale": "Top ranked Gmail search binding is correct.",
            "source_subpath": "review_decisions",
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
            "target_kind": "workflow_shape",
            "target_ref": "shape:current",
            "decision": "widen",
            "actor_type": "human",
            "actor_ref": "nate",
            "approval_mode": "manual",
            "rationale": "Show an alternate escalation-only path.",
            "source_subpath": "review_decisions",
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
            "target_kind": "capability_bundle",
            "target_ref": "capability_bundle:email_triage",
            "decision": "proposal_request",
            "actor_type": "policy",
            "actor_ref": "workflow.review.policy",
            "approval_mode": "policy",
            "rationale": "Surface the outbound-reply bundle for explicit review.",
            "source_subpath": "review_decisions",
            "candidate_ref": "bundle:email-triage-reply",
            "candidate_payload": {
                "bundle_ref": "bundle:email-triage-reply",
                "family": "support_triage",
            },
            "decided_at": "2026-04-15T12:10:00Z",
        },
    ]
    monkeypatch.setattr(
        "runtime.build_planning_contract.list_latest_workflow_build_review_decisions",
        lambda conn, workflow_id, definition_revision: decisions,
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
    assert reviewable_plan["widening_ops"][0]["operation"] == {
        "operation": "request_alternate_workflow_shapes",
        "count": 2,
    }
    assert reviewable_plan["status"] == "needs_proposals"
