from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import uuid

import pytest

from adapters import build_claim_received_proof, build_transition_proof
from receipts import AppendOnlyWorkflowEvidenceWriter, EvidenceAppendError, LifecycleTransition, RunState
from runtime import RouteIdentity
from runtime.domain import RuntimeBoundaryError
from runtime.persistent_evidence import PostgresEvidenceWriter


def _fail_persistence_run(coro) -> None:
    coro.close()
    raise RuntimeError("db down")


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


def test_commit_submission_records_claim_received_pair(
    route_identity,
    request_payload,
    admitted_definition_ref,
    admitted_definition_hash,
) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()

    result = writer.commit_submission(
        route_identity=route_identity,
        admitted_definition_ref=admitted_definition_ref,
        admitted_definition_hash=admitted_definition_hash,
        request_payload=request_payload,
    )

    assert result.evidence_seq == 2

    timeline = writer.evidence_timeline(route_identity.run_id)
    assert [row.kind for row in timeline] == ["workflow_event", "receipt"]
    assert [row.evidence_seq for row in timeline] == [1, 2]
    assert timeline[0].record.event_type == "claim_received"
    assert timeline[1].record.receipt_type == "claim_received_receipt"
    assert timeline[1].record.causation_id == timeline[0].row_id
    assert timeline[0].record.request_id == route_identity.request_id


def test_commit_transition_advances_shared_evidence_order_and_causality(
    route_identity,
    request_payload,
    admitted_definition_ref,
    admitted_definition_hash,
    occurred_at,
) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.commit_submission(
        route_identity=route_identity,
        admitted_definition_ref=admitted_definition_ref,
        admitted_definition_hash=admitted_definition_hash,
        request_payload=request_payload,
    )

    transition = LifecycleTransition(
        route_identity=replace(route_identity, lease_id="lease-1", transition_seq=2),
        from_state=RunState.CLAIM_RECEIVED,
        to_state=RunState.CLAIM_VALIDATING,
        reason_code="claim.validated",
        evidence_seq=3,
        event_type="claim_validated",
        receipt_type="claim_validation_receipt",
        occurred_at=occurred_at + timedelta(minutes=1),
    )

    result = writer.commit_transition(transition=transition)

    assert result.evidence_seq == 4

    timeline = writer.evidence_timeline(route_identity.run_id)
    assert [row.evidence_seq for row in timeline] == [1, 2, 3, 4]
    assert timeline[2].record.event_type == "claim_validated"
    assert timeline[3].record.receipt_type == "claim_validation_receipt"
    assert timeline[2].record.causation_id == timeline[1].row_id
    assert timeline[3].record.causation_id == timeline[2].row_id
    assert timeline[3].record.status == "claim_validating"


def test_failed_transition_append_rolls_back_partial_rows(
    claim_received_proof,
    route_identity,
    occurred_at,
) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.append_transition_proof(claim_received_proof)

    original_append_receipt = writer._append_receipt

    def boom_after_append(state, receipt):
        original_append_receipt(state, receipt)
        raise RuntimeError("boom")

    writer._append_receipt = boom_after_append

    proof = build_transition_proof(
        route_identity=replace(route_identity, lease_id="lease-1", transition_seq=2),
        transition_seq=2,
        event_id="workflow_event:run-1:3",
        receipt_id="receipt:run-1:4",
        event_type="claim_validated",
        receipt_type="claim_validation_receipt",
        reason_code="claim.validated",
        evidence_seq=3,
        occurred_at=occurred_at + timedelta(minutes=1),
        causation_id=claim_received_proof.receipt.receipt_id,
    )

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_transition_proof(proof)

    assert exc.value.reason_code == "evidence.append_failed"
    assert [row.evidence_seq for row in writer.evidence_timeline(route_identity.run_id)] == [1, 2]


def test_postgres_commit_submission_raises_when_persistence_fails(
    route_identity,
    request_payload,
    admitted_definition_ref,
    admitted_definition_hash,
    monkeypatch,
) -> None:
    writer = PostgresEvidenceWriter()
    monkeypatch.setattr(writer, "_run", _fail_persistence_run)

    with pytest.raises(RuntimeBoundaryError, match="persistent evidence submission failed"):
        writer.commit_submission(
            route_identity=route_identity,
            admitted_definition_ref=admitted_definition_ref,
            admitted_definition_hash=admitted_definition_hash,
            request_payload=request_payload,
        )


def test_postgres_commit_transition_raises_when_persistence_fails(
    route_identity,
    occurred_at,
    monkeypatch,
) -> None:
    writer = PostgresEvidenceWriter()
    monkeypatch.setattr(writer, "_run", _fail_persistence_run)

    transition = LifecycleTransition(
        route_identity=replace(route_identity, lease_id="lease-1", transition_seq=2),
        from_state=RunState.CLAIM_RECEIVED,
        to_state=RunState.CLAIM_VALIDATING,
        reason_code="claim.validated",
        evidence_seq=3,
        event_type="claim_validated",
        receipt_type="claim_validation_receipt",
        occurred_at=occurred_at + timedelta(minutes=1),
    )

    with pytest.raises(RuntimeBoundaryError, match="persistent evidence transition failed"):
        writer.commit_transition(transition=transition)


def test_postgres_append_transition_proof_raises_when_persistence_fails(
    claim_received_proof,
    monkeypatch,
) -> None:
    writer = PostgresEvidenceWriter()
    monkeypatch.setattr(writer, "_run", _fail_persistence_run)

    with pytest.raises(RuntimeBoundaryError, match="persistent evidence proof append failed"):
        writer.append_transition_proof(claim_received_proof)


def test_postgres_append_claim_received_proof_bootstraps_run_state(
    route_identity,
    request_payload,
    admitted_definition_ref,
    admitted_definition_hash,
    occurred_at,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    unique_route_identity = replace(
        route_identity,
        run_id=f"run-bootstrap-{suffix}",
        request_id=f"request-bootstrap-{suffix}",
        claim_id=f"claim-bootstrap-{suffix}",
    )
    proof = build_claim_received_proof(
        route_identity=unique_route_identity,
        event_id=f"workflow_event:{unique_route_identity.run_id}:1",
        receipt_id=f"receipt:{unique_route_identity.run_id}:2",
        evidence_seq=1,
        transition_seq=1,
        request_payload=request_payload,
        admitted_definition_ref=admitted_definition_ref,
        admitted_definition_hash=admitted_definition_hash,
        occurred_at=occurred_at,
    )
    writer = PostgresEvidenceWriter()

    try:
        result = writer.append_transition_proof(proof)

        assert result.evidence_seq == 2
        assert writer._run(
            writer._load_current_state(run_id=unique_route_identity.run_id)
        ) == RunState.CLAIM_RECEIVED.value
        timeline = writer.evidence_timeline(unique_route_identity.run_id)
        assert [row.kind for row in timeline] == ["workflow_event", "receipt"]
    finally:
        writer.close_blocking()


def test_postgres_append_node_proof_does_not_mutate_workflow_run_state(
    route_identity,
    request_payload,
    admitted_definition_ref,
    admitted_definition_hash,
    occurred_at,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    unique_route_identity = replace(
        route_identity,
        run_id=f"run-node-{suffix}",
        request_id=f"request-node-{suffix}",
        claim_id=f"claim-node-{suffix}",
    )
    writer = PostgresEvidenceWriter()

    try:
        submission_proof = build_claim_received_proof(
            route_identity=unique_route_identity,
            event_id=f"workflow_event:{unique_route_identity.run_id}:1",
            receipt_id=f"receipt:{unique_route_identity.run_id}:2",
            evidence_seq=1,
            transition_seq=1,
            request_payload=request_payload,
            admitted_definition_ref=admitted_definition_ref,
            admitted_definition_hash=admitted_definition_hash,
            occurred_at=occurred_at,
        )
        writer.append_transition_proof(submission_proof)

        node_proof = build_transition_proof(
            route_identity=replace(unique_route_identity, transition_seq=2),
            transition_seq=2,
            event_id=f"workflow_event:{unique_route_identity.run_id}:3",
            receipt_id=f"receipt:{unique_route_identity.run_id}:4",
            event_type="node_started",
            receipt_type="node_start_receipt",
            reason_code="runtime.node_started",
            evidence_seq=3,
            occurred_at=occurred_at + timedelta(minutes=1),
            status="running",
            payload={"node_id": "node-1"},
            inputs={"node_id": "node-1"},
            outputs={"node_id": "node-1", "status": "running"},
            node_id="node-1",
            causation_id=f"receipt:{unique_route_identity.run_id}:2",
        )

        result = writer.append_transition_proof(node_proof)

        assert result.evidence_seq == 4
        assert writer._run(
            writer._load_current_state(run_id=unique_route_identity.run_id)
        ) == RunState.CLAIM_RECEIVED.value
        timeline = writer.evidence_timeline(unique_route_identity.run_id)
        assert [row.evidence_seq for row in timeline] == [1, 2, 3, 4]
        assert timeline[2].record.event_type == "node_started"
        assert timeline[3].record.receipt_type == "node_start_receipt"
    finally:
        writer.close_blocking()
