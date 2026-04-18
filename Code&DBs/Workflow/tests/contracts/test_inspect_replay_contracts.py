from __future__ import annotations

from datetime import datetime, timezone

import pytest

from adapters.evidence import build_claim_received_proof
from receipts import AppendOnlyWorkflowEvidenceWriter
from observability import inspect_run, replay_run
from runtime import RouteIdentity


@pytest.fixture
def route_identity() -> RouteIdentity:
    return RouteIdentity(
        workflow_id="workflow-1",
        run_id="run-1",
        request_id="request-1",
        authority_context_ref="authority-context-1",
        authority_context_digest="authority-digest-1",
        claim_id="claim-1",
        lease_id=None,
        proposal_id=None,
        promotion_decision_id=None,
        attempt_no=1,
        transition_seq=1,
    )


@pytest.fixture
def occurred_at() -> datetime:
    return datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def request_payload() -> dict[str, object]:
    return {
        "node": "deterministic_task",
        "payload": {"answer": 42},
    }


@pytest.fixture
def admitted_definition_ref() -> str:
    return "workflow_definition:workflow-1:1"


@pytest.fixture
def admitted_definition_hash() -> str:
    return "sha256:deadbeef"


@pytest.fixture
def claim_received_proof(
    route_identity: RouteIdentity,
    request_payload: dict[str, object],
    admitted_definition_ref: str,
    admitted_definition_hash: str,
    occurred_at: datetime,
):
    return build_claim_received_proof(
        route_identity=route_identity,
        event_id="workflow_event:run-1:1",
        receipt_id="receipt:run-1:2",
        evidence_seq=1,
        transition_seq=1,
        request_payload=request_payload,
        admitted_definition_ref=admitted_definition_ref,
        admitted_definition_hash=admitted_definition_hash,
        occurred_at=occurred_at,
    )


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
