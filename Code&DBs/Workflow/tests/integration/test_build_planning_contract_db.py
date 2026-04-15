from __future__ import annotations

import uuid

from _pg_test_conn import ensure_test_database_ready, transactional_test_conn

from runtime.build_planning_contract import build_candidate_resolution_manifest
from storage.postgres.workflow_build_review_repository import (
    record_workflow_build_review_decision,
)
from storage.postgres.workflow_runtime_repository import (
    load_workflow_record,
    persist_workflow_record,
)
from surfaces.api.handlers.workflow_query import _workflow_build_payload


ensure_test_database_ready()


def _support_inbox_definition(*, definition_revision: str) -> dict[str, object]:
    return {
        "type": "operating_model",
        "source_prose": (
            "Watch the support inbox, classify incoming email by severity, "
            "and route the triage agent for review."
        ),
        "compiled_prose": (
            "Watch the support inbox, classify incoming email by severity, "
            "and route the triage agent for review."
        ),
        "narrative_blocks": [],
        "references": [],
        "capabilities": [],
        "authority": "",
        "sla": {},
        "trigger_intent": [],
        "draft_flow": [
            {
                "id": "step-001",
                "title": "Review support inbox",
                "summary": "triage-agent reviews the support inbox.",
                "source_block_ids": [],
                "depends_on": [],
                "order": 1,
            }
        ],
        "binding_ledger": [
            {
                "binding_id": "binding:ref-001",
                "source_kind": "reference",
                "source_label": "triage-agent",
                "source_span": None,
                "source_node_ids": ["step-001"],
                "state": "captured",
                "candidate_targets": [
                    {
                        "target_ref": "task_type_routing:auto/review",
                        "label": "Auto Review",
                        "kind": "agent",
                    }
                ],
                "accepted_target": None,
                "rationale": "Needs explicit approval before planning can proceed.",
                "created_at": "2026-04-09T19:00:00+00:00",
                "updated_at": "2026-04-09T19:00:00+00:00",
                "freshness": None,
            }
        ],
        "definition_revision": definition_revision,
    }


def test_real_db_candidate_manifest_and_payload_require_explicit_binding_approval() -> None:
    workflow_id = f"wf_build_contract_{uuid.uuid4().hex[:8]}"
    definition_revision = f"def_build_contract_{uuid.uuid4().hex[:8]}"
    definition = _support_inbox_definition(definition_revision=definition_revision)

    with transactional_test_conn() as conn:
        persist_workflow_record(
            conn,
            workflow_id=workflow_id,
            name="Support Intake",
            description="Compile support intake",
            definition=definition,
            compiled_spec=None,
        )

        manifest = build_candidate_resolution_manifest(
            definition=definition,
            workflow_id=workflow_id,
            conn=conn,
            compiled_spec=None,
        )
        slot = manifest["binding_slots"][0]
        assert manifest["execution_readiness"] == "review_required"
        assert slot["candidate_resolution_state"] == "candidate_set"
        assert slot["approval_state"] == "unapproved"
        assert slot["top_ranked_ref"] == "task_type_routing:auto/review"

        record_workflow_build_review_decision(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            target_kind="binding",
            target_ref="binding:ref-001",
            decision="approve",
            actor_type="human",
            actor_ref="architect",
            approval_mode="manual",
            rationale="Approved the review lane for support triage.",
            source_subpath="review_decisions",
            candidate_ref="task_type_routing:auto/review",
            candidate_payload={
                "target_ref": "task_type_routing:auto/review",
                "label": "Auto Review",
                "kind": "agent",
            },
        )

        row = load_workflow_record(conn, workflow_id=workflow_id)
        assert row is not None
        payload = _workflow_build_payload(row, conn=conn)

        manifest_slot = payload["candidate_resolution_manifest"]["binding_slots"][0]
        assert payload["candidate_resolution_manifest"]["execution_readiness"] == "ready"
        assert manifest_slot["approval_state"] == "approved"
        assert manifest_slot["approved_ref"] == "task_type_routing:auto/review"
        assert payload["reviewable_plan"]["approved_binding_refs"] == [
            {
                "slot_ref": "binding:ref-001",
                "candidate_ref": "task_type_routing:auto/review",
            }
        ]
        approval = payload["reviewable_plan"]["approval_records"][0]
        assert approval["target_kind"] == "binding"
        assert approval["approved_by"] == "architect"
        assert approval["review_actor"] == {
            "actor_type": "human",
            "actor_ref": "architect",
        }


def test_real_db_proposal_request_round_trips_without_creating_binding_approval() -> None:
    workflow_id = f"wf_build_contract_{uuid.uuid4().hex[:8]}"
    definition_revision = f"def_build_contract_{uuid.uuid4().hex[:8]}"
    definition = _support_inbox_definition(definition_revision=definition_revision)

    with transactional_test_conn() as conn:
        persist_workflow_record(
            conn,
            workflow_id=workflow_id,
            name="Support Intake",
            description="Compile support intake",
            definition=definition,
            compiled_spec=None,
        )

        record_workflow_build_review_decision(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            target_kind="binding",
            target_ref="binding:ref-001",
            decision="proposal_request",
            actor_type="model",
            actor_ref="planner-agent",
            approval_mode="review",
            rationale="Please surface the escalation route as an alternate candidate.",
            source_subpath="review_decisions",
            candidate_ref="task_type_routing:auto/escalate",
            candidate_payload={
                "target_ref": "task_type_routing:auto/escalate",
                "label": "Auto Escalate",
                "kind": "agent",
            },
        )

        row = load_workflow_record(conn, workflow_id=workflow_id)
        assert row is not None
        payload = _workflow_build_payload(row, conn=conn)

        manifest_slot = payload["candidate_resolution_manifest"]["binding_slots"][0]
        assert payload["candidate_resolution_manifest"]["execution_readiness"] == "review_required"
        assert manifest_slot["approval_state"] == "unapproved"
        assert manifest_slot["approved_ref"] is None
        assert payload["reviewable_plan"]["status"] == "needs_proposals"
        proposal_request = payload["reviewable_plan"]["proposal_requests"][0]
        assert proposal_request["target_kind"] == "binding"
        assert proposal_request["target_ref"] == "binding:ref-001"
        assert proposal_request["candidate_ref"] == "task_type_routing:auto/escalate"
        assert proposal_request["requested_by"] == {
            "actor_type": "model",
            "actor_ref": "planner-agent",
        }
