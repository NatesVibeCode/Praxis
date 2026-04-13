from __future__ import annotations

from receipts import AppendOnlyWorkflowEvidenceWriter
from observability import inspect_run, replay_run


def test_inspect_marks_missing_transition_receipt_as_incomplete(claim_received_proof) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.append_workflow_event(claim_received_proof.event)

    view = inspect_run(
        run_id=claim_received_proof.route_identity.run_id,
        canonical_evidence=writer.evidence_timeline(claim_received_proof.route_identity.run_id),
    )

    assert view.completeness.is_complete is False
    assert "transition:1:receipt" in view.completeness.missing_evidence_refs
    assert view.watermark.evidence_seq == 1
    assert view.current_state is None
    assert view.terminal_reason is None


def test_replay_fails_closed_when_canonical_evidence_is_incomplete(claim_received_proof) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.append_workflow_event(claim_received_proof.event)

    view = replay_run(
        run_id=claim_received_proof.route_identity.run_id,
        canonical_evidence=writer.evidence_timeline(claim_received_proof.route_identity.run_id),
    )

    assert view.completeness.is_complete is False
    assert "transition:1:receipt" in view.completeness.missing_evidence_refs
    assert view.admitted_definition_ref is None
    assert view.terminal_reason == "runtime.replay_incomplete"
