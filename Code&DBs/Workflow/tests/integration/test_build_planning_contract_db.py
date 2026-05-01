from __future__ import annotations

import uuid

from _pg_test_conn import ensure_test_database_ready, transactional_test_conn

from runtime.canonical_workflows import mutate_workflow_build
from runtime.build_planning_contract import build_candidate_resolution_manifest
from storage.postgres.workflow_build_review_repository import (
    get_latest_workflow_build_review_decision,
    record_workflow_build_review_decision,
)
from storage.postgres.workflow_runtime_repository import (
    load_workflow_record,
    persist_workflow_record,
)
from runtime.workflow_build_moment import build_workflow_build_moment


ensure_test_database_ready()


def _support_inbox_definition(
    *,
    definition_revision: str,
    include_build_graph: bool = False,
) -> dict[str, object]:
    definition: dict[str, object] = {
        "type": "operating_model",
        "source_prose": (
            "Watch the support inbox, classify incoming email by severity, "
            "and route the triage agent for review."
        ),
        "materialized_prose": (
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
    if include_build_graph:
        definition["build_graph"] = {
            "graph_id": "shape:support-inbox",
            "nodes": [
                {
                    "node_id": "step-001",
                    "id": "step-001",
                    "kind": "step",
                    "title": "Review support inbox",
                    "summary": "triage-agent reviews the support inbox.",
                    "route": "llm_task",
                }
            ],
            "edges": [],
            "projection_status": {"state": "ready"},
        }
    return definition


def _persist_support_workflow(
    conn,
    *,
    workflow_id: str,
    definition_revision: str,
    include_build_graph: bool = False,
) -> dict[str, object]:
    definition = _support_inbox_definition(
        definition_revision=definition_revision,
        include_build_graph=include_build_graph,
    )
    persist_workflow_record(
        conn,
        workflow_id=workflow_id,
        name="Support Intake",
        description="Compile support intake",
        definition=definition,
        materialized_spec=None,
    )
    return definition


def _ap_invoice_definition(
    *,
    definition_revision: str,
    include_build_graph: bool = False,
) -> dict[str, object]:
    definition: dict[str, object] = {
        "type": "operating_model",
        "source_prose": (
            "Parse inbound AP invoices, match vendors in ERP, update the payable object, "
            "and require policy approval before scheduling payment."
        ),
        "materialized_prose": (
            "Parse inbound AP invoices, match vendors in ERP, update the payable object, "
            "and require policy approval before scheduling payment."
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
                "title": "Process AP invoices",
                "summary": "Invoice agent parses invoices and prepares payable updates.",
                "source_block_ids": [],
                "depends_on": [],
                "order": 1,
            }
        ],
        "binding_ledger": [
            {
                "binding_id": "binding:invoice-001",
                "source_kind": "reference",
                "source_label": "invoice-agent",
                "source_span": None,
                "source_node_ids": ["step-001"],
                "state": "captured",
                "candidate_targets": [
                    {
                        "target_ref": "task_type_routing:auto/process_invoice",
                        "label": "Process Invoice",
                        "kind": "agent",
                    }
                ],
                "accepted_target": None,
                "rationale": "Invoice processing route needs explicit approval before planning can proceed.",
                "created_at": "2026-04-09T19:00:00+00:00",
                "updated_at": "2026-04-09T19:00:00+00:00",
                "freshness": None,
            }
        ],
        "definition_revision": definition_revision,
    }
    if include_build_graph:
        definition["build_graph"] = {
            "graph_id": "shape:ap-invoice",
            "nodes": [
                {
                    "node_id": "step-001",
                    "id": "step-001",
                    "kind": "step",
                    "title": "Process AP invoices",
                    "summary": "Invoice agent parses invoices and prepares payable updates.",
                    "route": "llm_task",
                }
            ],
            "edges": [],
            "projection_status": {"state": "ready"},
        }
    return definition


def _persist_ap_invoice_workflow(
    conn,
    *,
    workflow_id: str,
    definition_revision: str,
    include_build_graph: bool = False,
) -> dict[str, object]:
    definition = _ap_invoice_definition(
        definition_revision=definition_revision,
        include_build_graph=include_build_graph,
    )
    persist_workflow_record(
        conn,
        workflow_id=workflow_id,
        name="AP Invoice Intake",
        description="Compile AP invoice intake",
        definition=definition,
        materialized_spec=None,
    )
    return definition


def _latest_payload(conn, *, workflow_id: str) -> dict[str, object]:
    row = load_workflow_record(conn, workflow_id=workflow_id)
    assert row is not None
    return build_workflow_build_moment(row, conn=conn)


def test_real_db_candidate_manifest_projection_matches_persisted_shape() -> None:
    workflow_id = f"wf_build_contract_{uuid.uuid4().hex[:8]}"
    definition_revision = f"def_build_contract_{uuid.uuid4().hex[:8]}"

    with transactional_test_conn() as conn:
        definition = _persist_support_workflow(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
        )

        manifest = build_candidate_resolution_manifest(
            definition=definition,
            workflow_id=workflow_id,
            conn=conn,
            materialized_spec=None,
        )
        payload = _latest_payload(conn, workflow_id=workflow_id)
        projected = payload["candidate_resolution_manifest"]

        for key in (
            "manifest_version",
            "manifest_id",
            "manifest_ref",
            "manifest_revision",
            "execution_readiness",
            "review_group_ref",
            "projection_status",
            "blocking_issues",
            "required_confirmations",
            "binding_slots",
            "capability_bundle_candidates",
            "workflow_shape_candidates",
        ):
            assert projected[key] == manifest[key]


def test_real_db_candidate_manifest_and_payload_require_explicit_binding_approval() -> None:
    workflow_id = f"wf_build_contract_{uuid.uuid4().hex[:8]}"
    definition_revision = f"def_build_contract_{uuid.uuid4().hex[:8]}"

    with transactional_test_conn() as conn:
        definition = _persist_support_workflow(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
        )

        manifest = build_candidate_resolution_manifest(
            definition=definition,
            workflow_id=workflow_id,
            conn=conn,
            materialized_spec=None,
        )
        slot = manifest["binding_slots"][0]
        assert manifest["execution_readiness"] == "review_required"
        assert manifest["intent_ref"]
        assert manifest["capability_bundle_candidates"][0]["top_ranked_ref"] == "capability_bundle:email_triage"
        assert slot["candidate_resolution_state"] == "candidate_set"
        assert slot["approval_state"] == "unapproved"
        assert slot["top_ranked_ref"] == "task_type_routing:auto/review"
        intent_row = conn.fetchrow(
            "SELECT intent_ref, goal FROM workflow_build_intents WHERE workflow_id = $1 AND definition_revision = $2",
            workflow_id,
            definition_revision,
        )
        assert intent_row is not None
        manifest_row = conn.fetchrow(
            "SELECT manifest_ref, review_group_ref FROM workflow_build_candidate_manifests WHERE workflow_id = $1 AND definition_revision = $2",
            workflow_id,
            definition_revision,
        )
        assert manifest_row is not None
        session_row = conn.fetchrow(
            "SELECT review_policy_ref FROM workflow_build_review_sessions WHERE workflow_id = $1 AND definition_revision = $2",
            workflow_id,
            definition_revision,
        )
        assert session_row is not None
        assert session_row["review_policy_ref"] == "review_policy:workflow_build/default"

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

        payload = _latest_payload(conn, workflow_id=workflow_id)

        manifest_slot = payload["candidate_resolution_manifest"]["binding_slots"][0]
        assert payload["candidate_resolution_manifest"]["execution_readiness"] == "review_required"
        assert payload["intent_brief"]["goal"].startswith("Watch the support inbox")
        assert manifest_slot["approval_state"] == "approved"
        assert manifest_slot["approved_ref"] == "task_type_routing:auto/review"
        assert any(
            item["slot_ref"] == "workflow_shape"
            for item in payload["candidate_resolution_manifest"]["required_confirmations"]
        )
        assert any(
            item["slot_ref"] == "capability_bundle:support_triage"
            for item in payload["candidate_resolution_manifest"]["required_confirmations"]
        )
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

    with transactional_test_conn() as conn:
        _persist_support_workflow(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
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

        payload = _latest_payload(conn, workflow_id=workflow_id)

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


def test_real_db_import_candidates_use_the_same_review_decision_surface() -> None:
    workflow_id = f"wf_build_contract_{uuid.uuid4().hex[:8]}"
    definition_revision = f"def_build_contract_{uuid.uuid4().hex[:8]}"

    with transactional_test_conn() as conn:
        _persist_support_workflow(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
        )
        staged = mutate_workflow_build(
            conn,
            workflow_id=workflow_id,
            subpath="imports",
            body={
                "node_id": "step-001",
                "source_kind": "net",
                "source_locator": "https://example.com/support/schema",
                "requested_shape": {
                    "target_ref": "object_types:ticket",
                    "label": "Ticket",
                    "kind": "type",
                },
                "payload": {"rows": 1},
            },
        )
        snapshot_id = staged["definition"]["import_snapshots"][0]["snapshot_id"]
        approved = mutate_workflow_build(
            conn,
            workflow_id=workflow_id,
            subpath="review_decisions",
            body={
                "target_kind": "import_snapshot",
                "target_ref": snapshot_id,
                "decision": "approve",
                "candidate_ref": "object_types:ticket",
                "candidate_payload": {
                    "target_ref": "object_types:ticket",
                    "label": "Ticket",
                    "kind": "type",
                },
                "actor_type": "policy",
                "actor_ref": "workflow.import.policy",
                "approval_mode": "policy",
                "authority_scope": "workflow_build/import_snapshot",
                "rationale": "Approved imported ticket schema candidate.",
            },
        )

        snapshot = approved["definition"]["import_snapshots"][0]
        assert snapshot["approval_state"] == "admitted"
        assert snapshot["admitted_targets"][0]["target_ref"] == "object_types:ticket"
        assert approved["definition"]["review_state"]["approved_import_snapshot_refs"] == [snapshot_id]
        assert approved["reviewable_plan"]["approved_binding_refs"] == []
        assert approved["candidate_resolution_manifest"]["execution_readiness"] == "review_required"
        approval = next(
            item
            for item in approved["definition"]["review_state"]["approval_records"]
            if item["target_kind"] == "import_snapshot"
        )
        assert approval["actor_type"] == "policy"
        assert approval["authority_scope"] == "workflow_build/import_snapshot"


def test_real_db_bundle_and_workflow_shape_reviews_share_provenance_and_hardening_gate() -> None:
    workflow_id = f"wf_build_contract_{uuid.uuid4().hex[:8]}"
    definition_revision = f"def_build_contract_{uuid.uuid4().hex[:8]}"

    with transactional_test_conn() as conn:
        _persist_support_workflow(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            include_build_graph=True,
        )

        binding_only = mutate_workflow_build(
            conn,
            workflow_id=workflow_id,
            subpath="review_decisions",
            body={
                "target_kind": "binding",
                "target_ref": "binding:ref-001",
                "slot_ref": "binding:ref-001",
                "decision": "approve",
                "candidate_ref": "task_type_routing:auto/review",
                "candidate_payload": {
                    "target_ref": "task_type_routing:auto/review",
                    "label": "Auto Review",
                    "kind": "agent",
                },
                "actor_type": "human",
                "actor_ref": "architect",
                "approval_mode": "manual",
                "authority_scope": "workflow_build/binding",
                "rationale": "Approve the support review lane.",
            },
        )
        assert binding_only["materialized_spec"] is None
        assert any("Workflow shape requires explicit approval" in note for note in binding_only["planning_notes"])
        workflow_shape_ref = binding_only["candidate_resolution_manifest"]["workflow_shape_candidates"][0]["candidate_ref"]

        shape_approved = mutate_workflow_build(
            conn,
            workflow_id=workflow_id,
            subpath="review_decisions",
            body={
                "target_kind": "workflow_shape",
                "target_ref": workflow_shape_ref,
                "slot_ref": "workflow_shape",
                "decision": "approve",
                "candidate_ref": workflow_shape_ref,
                "candidate_payload": {
                    "target_ref": workflow_shape_ref,
                    "kind": "build_graph",
                },
                "actor_type": "model",
                "actor_ref": "planner-agent",
                "approval_mode": "review",
                "authority_scope": "workflow_build/workflow_shape",
                "rationale": "Approve the current support inbox flow shape.",
            },
        )
        assert shape_approved["materialized_spec"] is None
        assert any("Capability bundle approval is required" in note for note in shape_approved["planning_notes"])
        assert shape_approved["definition"]["review_state"]["approved_workflow_shape_ref"] == workflow_shape_ref
        assert shape_approved["definition"]["build_graph"]["approval_state"] == "approved"

        bundle_approved = mutate_workflow_build(
            conn,
            workflow_id=workflow_id,
            subpath="review_decisions",
            body={
                "target_kind": "capability_bundle",
                "target_ref": "capability_bundle:email_triage",
                "slot_ref": "capability_bundle:support_triage",
                "decision": "approve",
                "candidate_ref": "capability_bundle:email_triage",
                "candidate_payload": {
                    "bundle_ref": "capability_bundle:email_triage",
                    "family": "support_triage",
                },
                "actor_type": "policy",
                "actor_ref": "workflow.bundle.policy",
                "approval_mode": "policy",
                "authority_scope": "workflow_build/capability_bundle",
                "rationale": "Approve the support triage capability bundle.",
            },
        )
        assert bundle_approved["materialized_spec"] is not None
        assert bundle_approved["execution_manifest"] is not None
        assert bundle_approved["definition"]["review_state"]["approved_bundle_refs"] == [
            "capability_bundle:email_triage"
        ]
        assert bundle_approved["reviewable_plan"]["approved_bundle_refs"] == [
            "capability_bundle:email_triage"
        ]
        assert bundle_approved["execution_manifest"]["approved_bundle_refs"] == [
            "capability_bundle:email_triage"
        ]
        assert "praxis_integration" in bundle_approved["execution_manifest"]["tool_allowlist"]["mcp_tools"]
        execution_row = conn.fetchrow(
            """
            SELECT execution_manifest_ref
            FROM workflow_build_execution_manifests
            WHERE workflow_id = $1 AND definition_revision = $2
            """,
            workflow_id,
            definition_revision,
        )
        assert execution_row is not None


def test_real_db_review_decision_supersession_tracks_latest_effective_actor() -> None:
    workflow_id = f"wf_build_contract_{uuid.uuid4().hex[:8]}"
    definition_revision = f"def_build_contract_{uuid.uuid4().hex[:8]}"

    with transactional_test_conn() as conn:
        _persist_support_workflow(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
        )
        first = record_workflow_build_review_decision(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            target_kind="binding",
            target_ref="binding:ref-001",
            slot_ref="binding:ref-001",
            decision="reject",
            actor_type="model",
            actor_ref="planner-agent",
            approval_mode="review",
            authority_scope="workflow_build/binding",
            rationale="Reject the first route.",
            source_subpath="review_decisions",
            candidate_ref="task_type_routing:auto/review",
            candidate_payload={
                "target_ref": "task_type_routing:auto/review",
                "label": "Auto Review",
                "kind": "agent",
            },
        )
        second = record_workflow_build_review_decision(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            target_kind="binding",
            target_ref="binding:ref-001",
            slot_ref="binding:ref-001",
            decision="approve",
            actor_type="policy",
            actor_ref="workflow.review.policy",
            approval_mode="policy",
            authority_scope="workflow_build/binding",
            rationale="Policy overrides with explicit approval.",
            source_subpath="review_decisions",
            candidate_ref="task_type_routing:auto/review",
            candidate_payload={
                "target_ref": "task_type_routing:auto/review",
                "label": "Auto Review",
                "kind": "agent",
            },
        )

        assert second["supersedes_decision_ref"] == first["review_decision_id"]
        latest = get_latest_workflow_build_review_decision(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            target_kind="binding",
            target_ref="binding:ref-001",
        )
        assert latest is not None
        assert latest["review_decision_id"] == second["review_decision_id"]
        payload = _latest_payload(conn, workflow_id=workflow_id)
        approval = payload["reviewable_plan"]["approval_records"][0]
        assert approval["review_actor"] == {
            "actor_type": "policy",
            "actor_ref": "workflow.review.policy",
        }
        assert approval["supersedes_decision_ref"] == first["review_decision_id"]


def test_real_db_ap_invoice_path_uses_registry_bundle_candidates_and_execution_manifest() -> None:
    workflow_id = f"wf_build_contract_{uuid.uuid4().hex[:8]}"
    definition_revision = f"def_build_contract_{uuid.uuid4().hex[:8]}"

    with transactional_test_conn() as conn:
        definition = _persist_ap_invoice_workflow(
            conn,
            workflow_id=workflow_id,
            definition_revision=definition_revision,
            include_build_graph=True,
        )
        manifest = build_candidate_resolution_manifest(
            definition=definition,
            workflow_id=workflow_id,
            conn=conn,
            materialized_spec=None,
        )
        assert manifest["capability_bundle_candidates"][0]["top_ranked_ref"] == "capability_bundle:invoice_processing"
        workflow_shape_ref = manifest["workflow_shape_candidates"][0]["candidate_ref"]

        mutate_workflow_build(
            conn,
            workflow_id=workflow_id,
            subpath="review_decisions",
            body={
                "target_kind": "binding",
                "target_ref": "binding:invoice-001",
                "slot_ref": "binding:invoice-001",
                "decision": "approve",
                "candidate_ref": "task_type_routing:auto/process_invoice",
                "candidate_payload": {
                    "target_ref": "task_type_routing:auto/process_invoice",
                    "label": "Process Invoice",
                    "kind": "agent",
                },
                "actor_type": "human",
                "actor_ref": "architect",
                "approval_mode": "manual",
                "authority_scope": "workflow_build/binding",
                "rationale": "Approve invoice processing lane.",
            },
        )
        mutate_workflow_build(
            conn,
            workflow_id=workflow_id,
            subpath="review_decisions",
            body={
                "target_kind": "workflow_shape",
                "target_ref": workflow_shape_ref,
                "slot_ref": "workflow_shape",
                "decision": "approve",
                "candidate_ref": workflow_shape_ref,
                "candidate_payload": {
                    "target_ref": workflow_shape_ref,
                    "kind": "build_graph",
                },
                "actor_type": "model",
                "actor_ref": "planner-agent",
                "approval_mode": "review",
                "authority_scope": "workflow_build/workflow_shape",
                "rationale": "Approve invoice processing shape.",
            },
        )
        final_payload = mutate_workflow_build(
            conn,
            workflow_id=workflow_id,
            subpath="review_decisions",
            body={
                "target_kind": "capability_bundle",
                "target_ref": "capability_bundle:invoice_processing",
                "slot_ref": "capability_bundle:ap_invoice",
                "decision": "approve",
                "candidate_ref": "capability_bundle:invoice_processing",
                "candidate_payload": {
                    "bundle_ref": "capability_bundle:invoice_processing",
                    "family": "ap_invoice",
                },
                "actor_type": "policy",
                "actor_ref": "workflow.bundle.policy",
                "approval_mode": "policy",
                "authority_scope": "workflow_build/capability_bundle",
                "rationale": "Approve the AP invoice capability bundle.",
            },
        )

        assert final_payload["execution_manifest"] is not None
        assert final_payload["execution_manifest"]["approved_bundle_refs"] == [
            "capability_bundle:invoice_processing"
        ]
        assert "praxis_integration" in final_payload["execution_manifest"]["tool_allowlist"]["mcp_tools"]
        assert final_payload["intent_brief"]["goal"].startswith("Parse inbound AP invoices")
