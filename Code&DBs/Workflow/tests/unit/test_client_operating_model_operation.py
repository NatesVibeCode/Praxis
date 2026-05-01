from __future__ import annotations

from runtime.operations.queries.client_operating_model import (
    QueryClientOperatingModelView,
    handle_client_operating_model_view,
)


def test_client_operating_model_query_builds_operator_view() -> None:
    query = QueryClientOperatingModelView(
        view="object_truth",
        generated_at="2026-04-30T12:00:00Z",
        permission_scope={"scope_ref": "tenant.acme", "visibility": "limited", "redacted_fields": ["tax_id"]},
        evidence_refs=["object_truth.snapshot.1"],
        inputs={
            "object_ref": "object.account.1",
            "canonical_summary": {"object_ref": "object.account.1", "display_name": "Acme"},
            "fields": [{"field_name": "tax_id", "value": "12-3456789", "authority": "irs_registry"}],
        },
    )

    result = handle_client_operating_model_view(query, subsystems=None)

    assert result["ok"] is True
    assert result["operation"] == "client_operating_model_operator_view"
    assert result["view"] == "object_truth"
    assert result["state"] == "partial"
    assert result["operator_view"]["payload"]["fields"][0]["state"] == "not_authorized"
    assert result["operator_view"]["evidence_refs"] == ["object_truth.snapshot.1"]


def test_client_operating_model_query_returns_typed_error_for_missing_inputs() -> None:
    query = QueryClientOperatingModelView(
        view="next_safe_actions",
        generated_at="2026-04-30T12:00:00Z",
        inputs={"subject_ref": "workflow.1"},
    )

    result = handle_client_operating_model_view(query, subsystems=None)

    assert result["ok"] is False
    assert result["operation"] == "client_operating_model_operator_view"
    assert result["view"] == "next_safe_actions"
    assert result["error_code"] == "client_operating_model.invalid_view_inputs"
    assert "snapshot_ref" in result["error"]


def test_client_operating_model_query_builds_side_effect_free_builder_validation() -> None:
    query = QueryClientOperatingModelView(
        view="workflow_builder_validation",
        generated_at="2026-04-30T12:00:00Z",
        inputs={
            "graph": {
                "nodes": [
                    {"node_id": "start", "block_ref": "source.refresh"},
                    {"node_id": "unsafe", "block_ref": "unknown.block"},
                ],
                "edges": [{"from": "unsafe", "to": "start"}],
            },
            "approved_blocks": {
                "source.refresh": {"provides": ["fresh_snapshot"]},
            },
            "allowed_edges": [],
        },
    )

    result = handle_client_operating_model_view(query, subsystems=None)

    assert result["ok"] is True
    assert result["state"] == "blocked"
    reasons = {
        item["reason_code"]
        for item in result["operator_view"]["payload"]["validation"]["errors"]
    }
    assert "builder.block_not_approved" in reasons


def test_client_operating_model_query_builds_workflow_context_composite() -> None:
    query = QueryClientOperatingModelView(
        view="workflow_context_composite",
        generated_at="2026-04-30T12:00:00Z",
        permission_scope={"scope_ref": "workflow.renewal", "visibility": "full"},
        inputs={
            "workflow_ref": "workflow.renewal",
            "context_pack": {
                "context_ref": "workflow_context:renewal",
                "workflow_ref": "workflow.renewal",
                "context_mode": "synthetic",
                "truth_state": "synthetic",
                "confidence_score": 0.41,
                "confidence": {
                    "score": 0.41,
                    "state": "low",
                    "inputs": {
                        "promotion_evidence_count": 0,
                    },
                },
                "entities": [
                    {"entity_kind": "object", "label": "Account", "truth_state": "synthetic"},
                    {"entity_kind": "system", "label": "CRM", "truth_state": "synthetic"},
                ],
                "blockers": [
                    {
                        "severity": "hard",
                        "reason_code": "workflow_context.unknown_mutator_risk",
                    }
                ],
                "evidence_refs": [
                    {"evidence_ref": "synthetic.fixture", "evidence_tier": "synthetic"},
                    {"evidence_ref": "sop.renewal", "evidence_tier": "documented"},
                ],
                "verifier_expectations": [{"verifier_ref": "verifier.workflow_context.renewal"}],
                "synthetic_world": {
                    "world_ref": "world.renewal",
                    "seed": "seed-1",
                    "synthetic": True,
                    "records": [{"record_id": "synthetic:account:1"}],
                },
                "review_packet": {
                    "queued_decisions": [
                        {"decision_type": "accepted_risk_or_blocker_resolution"}
                    ]
                },
            },
            "builder_validation_view": {
                "operator_view": {
                    "state": "healthy",
                    "payload": {
                        "validation": {
                            "ok": True,
                            "errors": [],
                            "warnings": [],
                        },
                        "safe_action_summary": [{"action_ref": "workflow_builder.save_candidate"}],
                    },
                }
            },
        },
    )

    result = handle_client_operating_model_view(query, subsystems=None)

    assert result["ok"] is True
    assert result["view"] == "workflow_context_composite"
    assert result["state"] == "blocked"
    payload = result["operator_view"]["payload"]
    assert payload["buildability"]["ok"] is True
    assert payload["synthetic_proof"]["record_count"] == 1
    assert payload["binding_coverage"]["state"] == "missing"
    assert payload["real_evidence"]["state"] == "missing"
    assert payload["deployability"]["state"] == "blocked"
    assert payload["truth_state_classes"]["synthetic"] >= 3
    assert set(payload["truth_state_classes"]).issuperset(
        {"none", "inferred", "synthetic", "verified", "promoted", "blocked"}
    )
