from __future__ import annotations

from datetime import datetime, timezone

from receipts import EvidenceRow, ReceiptV1, RouteIdentity
from runtime import workflow_diagnose
from storage.postgres.validators import PostgresStorageError


def _receipt_row(
    *,
    evidence_seq: int,
    receipt_id: str,
    status: str = "succeeded",
    failure_code: str | None = None,
    outputs: dict | None = None,
) -> EvidenceRow:
    route_identity = RouteIdentity(
        workflow_id="workflow.test",
        run_id="run.test",
        request_id="request.test",
        authority_context_ref="authority.test",
        authority_context_digest="digest.test",
        claim_id="claim.test",
        attempt_no=1,
        transition_seq=evidence_seq,
    )
    receipt = ReceiptV1(
        receipt_id=receipt_id,
        receipt_type="node.completed",
        schema_version=1,
        workflow_id="workflow.test",
        run_id="run.test",
        request_id="request.test",
        route_identity=route_identity,
        transition_seq=evidence_seq,
        evidence_seq=evidence_seq,
        started_at=datetime(2026, 4, 20, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 20, 0, 0, evidence_seq, tzinfo=timezone.utc),
        executor_type="anthropic/claude",
        status=status,
        inputs={},
        outputs=outputs or {"provider_slug": "anthropic", "model_slug": "claude-sonnet"},
        artifacts=(),
        decision_refs=(),
        node_id=f"node-{evidence_seq}",
        attempt_no=1,
        failure_code=failure_code,
    )
    return EvidenceRow(
        kind="receipt",
        evidence_seq=evidence_seq,
        row_id=receipt_id,
        route_identity=route_identity,
        transition_seq=evidence_seq,
        record=receipt,
    )


def test_diagnose_run_uses_latest_receipt_from_canonical_evidence(monkeypatch) -> None:
    monkeypatch.setattr(
        workflow_diagnose,
        "_canonical_evidence_for_run",
        lambda run_id: (
            "run.test",
            (
                _receipt_row(evidence_seq=1, receipt_id="receipt.old"),
                _receipt_row(evidence_seq=2, receipt_id="receipt.new"),
            ),
        ),
    )
    monkeypatch.setattr(
        workflow_diagnose,
        "_provider_health_summary",
        lambda provider_slug: {"provider_slug": provider_slug, "healthy": True},
    )

    diagnosis = workflow_diagnose.diagnose_run("test")

    assert diagnosis["evidence_source"] == "canonical_evidence_timeline"
    assert diagnosis["selected_receipt_id"] == "receipt.new"
    assert diagnosis["receipt"]["receipt_id"] == "receipt.new"
    assert diagnosis["provider_slug"] == "anthropic"
    assert diagnosis["provider_health"] == {"provider_slug": "anthropic", "healthy": True}


def test_diagnose_run_reports_canonical_evidence_errors(monkeypatch) -> None:
    def _raise(_run_id):
        raise PostgresStorageError(
            "postgres.missing_route_identity",
            "missing route identity",
            details={"row_id": "event-1"},
        )

    monkeypatch.setattr(workflow_diagnose, "_canonical_evidence_for_run", _raise)

    diagnosis = workflow_diagnose.diagnose_run("run.test")

    assert diagnosis["receipt_found"] is False
    assert diagnosis["evidence_source"] == "canonical_evidence_timeline"
    assert diagnosis["reason_code"] == "postgres.missing_route_identity"
    assert diagnosis["canonical_evidence_error"]["details"] == {"row_id": "event-1"}
