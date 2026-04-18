from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from adapters.evidence import build_claim_received_proof, build_transition_proof
from runtime import RouteIdentity
from receipts import ArtifactRef, AppendOnlyWorkflowEvidenceWriter, DecisionRef, EvidenceAppendError


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


def test_event_envelope_requires_required_fields(claim_received_proof) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()

    stored = writer.append_workflow_event(claim_received_proof.event)
    assert stored.event_type == "claim_received"

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_workflow_event(replace(claim_received_proof.event, evidence_seq=0))

    assert exc.value.reason_code == "evidence.invalid_shape"


def test_receipt_envelope_requires_required_fields(claim_received_proof, occurred_at) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    standalone_receipt = replace(
        claim_received_proof.receipt,
        receipt_id="receipt:run-1:1",
        evidence_seq=1,
        inputs={
            **claim_received_proof.receipt.inputs,
            "receipt_id": "receipt:run-1:1",
            "evidence_seq": 1,
        },
        outputs={
            **claim_received_proof.receipt.outputs,
            "receipt_id": "receipt:run-1:1",
            "evidence_seq": 1,
        },
        causation_id=None,
    )

    stored = writer.append_receipt(standalone_receipt)
    assert stored.receipt_type == "claim_received_receipt"

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_receipt(
            replace(
                standalone_receipt,
                receipt_id="receipt:run-1:2",
                evidence_seq=2,
                started_at=occurred_at,
                finished_at=occurred_at - timedelta(seconds=1),
            )
        )

    assert exc.value.reason_code == "evidence.invalid_time"


def test_reference_fields_are_typed_objects(claim_received_proof) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    typed_receipt = replace(
        claim_received_proof.receipt,
        receipt_id="receipt:run-1:1",
        evidence_seq=1,
        inputs={
            **claim_received_proof.receipt.inputs,
            "receipt_id": "receipt:run-1:1",
            "evidence_seq": 1,
        },
        outputs={
            **claim_received_proof.receipt.outputs,
            "receipt_id": "receipt:run-1:1",
            "evidence_seq": 1,
        },
        artifacts=(
            ArtifactRef(
                artifact_id="artifact-1",
                artifact_type="log",
                content_hash="sha256:artifact",
                storage_ref="storage://artifact-1",
            ),
        ),
        decision_refs=(
            DecisionRef(
                decision_type="admission",
                decision_id="decision-1",
                reason_code="policy.admit",
                source_table="admission_decisions",
            ),
        ),
        causation_id=None,
    )

    stored = writer.append_receipt(typed_receipt)
    assert isinstance(stored.artifacts[0], ArtifactRef)
    assert isinstance(stored.decision_refs[0], DecisionRef)

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_receipt(
            replace(
                typed_receipt,
                receipt_id="receipt:run-1:2",
                evidence_seq=2,
                inputs={
                    **typed_receipt.inputs,
                    "receipt_id": "receipt:run-1:2",
                    "evidence_seq": 2,
                },
                outputs={
                    **typed_receipt.outputs,
                    "receipt_id": "receipt:run-1:2",
                    "evidence_seq": 2,
                },
                decision_refs=("not-typed",),
            )
        )

    assert exc.value.reason_code == "evidence.invalid_shape"


def test_claim_received_proof_carries_explicit_causation(claim_received_proof) -> None:
    assert claim_received_proof.event.causation_id is None
    assert claim_received_proof.receipt.causation_id == claim_received_proof.event.event_id


def test_transition_proof_builder_carries_route_and_causation_lineage(
    claim_received_proof,
    route_identity,
    occurred_at,
) -> None:
    later_identity = replace(route_identity, lease_id="lease-1", transition_seq=2)

    proof = build_transition_proof(
        route_identity=later_identity,
        transition_seq=2,
        event_id="workflow_event:run-1:3",
        receipt_id="receipt:run-1:4",
        event_type="claim_validated",
        receipt_type="claim_validation_receipt",
        reason_code="claim.validated",
        evidence_seq=3,
        occurred_at=occurred_at,
        causation_id=claim_received_proof.receipt.receipt_id,
    )

    assert proof.event.causation_id == claim_received_proof.receipt.receipt_id
    assert proof.receipt.causation_id == proof.event.event_id
    assert proof.event.payload["route_identity"]["lease_id"] == "lease-1"
    assert proof.event.payload["event_id"] == "workflow_event:run-1:3"
    assert proof.receipt.inputs["transition_seq"] == 2
    assert proof.receipt.outputs["route_identity"]["lease_id"] == "lease-1"
    assert proof.receipt.outputs["receipt_id"] == "receipt:run-1:4"


def test_direct_event_append_rejects_nested_lineage_drift(claim_received_proof) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    drifted_event = replace(
        claim_received_proof.event,
        payload={
            **claim_received_proof.event.payload,
            "event_id": "workflow_event:run-1:bogus",
        },
    )

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_workflow_event(drifted_event)

    assert exc.value.reason_code == "evidence.lineage_mismatch"


def test_transition_proof_rejects_nested_receipt_lineage_drift(claim_received_proof) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    drifted_proof = replace(
        claim_received_proof,
        receipt=replace(
            claim_received_proof.receipt,
            inputs={
                **claim_received_proof.receipt.inputs,
                "event_id": "workflow_event:run-1:bogus",
            },
        ),
    )

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_transition_proof(drifted_proof)

    assert exc.value.reason_code == "evidence.lineage_mismatch"


def test_evidence_seq_orders_mixed_event_and_receipt_rows(claim_received_proof, route_identity, occurred_at) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.append_transition_proof(claim_received_proof)

    later_identity = replace(route_identity, lease_id="lease-1", transition_seq=2)
    later_event = replace(
        claim_received_proof.event,
        event_id="workflow_event:run-1:3",
        event_type="claim_validated",
        evidence_seq=3,
        occurred_at=occurred_at - timedelta(hours=1),
        reason_code="claim.validated",
        route_identity=later_identity,
        transition_seq=2,
        causation_id=claim_received_proof.receipt.receipt_id,
        payload={
            "from_state": "claim_received",
            "to_state": "claim_validating",
        },
    )
    writer.append_workflow_event(later_event)

    timeline = writer.evidence_timeline(route_identity.run_id)
    assert [row.evidence_seq for row in timeline] == [1, 2, 3]
    assert [row.kind for row in timeline] == ["workflow_event", "receipt", "workflow_event"]


def test_duplicate_evidence_seq_is_invalid(claim_received_proof, route_identity, occurred_at) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.append_transition_proof(claim_received_proof)

    duplicate_event = replace(
        claim_received_proof.event,
        event_id="workflow_event:run-1:3",
        event_type="claim_validated",
        evidence_seq=2,
        occurred_at=occurred_at + timedelta(minutes=1),
        reason_code="claim.validated",
        route_identity=replace(route_identity, lease_id="lease-1", transition_seq=2),
        transition_seq=2,
        causation_id=claim_received_proof.receipt.receipt_id,
        payload={
            "from_state": "claim_received",
            "to_state": "claim_validating",
        },
    )

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_workflow_event(duplicate_event)

    assert exc.value.reason_code == "evidence_seq.conflict"


def test_standalone_event_append_does_not_advance_authoritative_transition_seq(
    claim_received_proof,
    route_identity,
    occurred_at,
) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.append_transition_proof(claim_received_proof)

    observed_identity = replace(route_identity, lease_id="lease-1", transition_seq=2)
    observation = replace(
        claim_received_proof.event,
        event_id="workflow_event:run-1:3",
        event_type="claim_observed",
        evidence_seq=3,
        occurred_at=occurred_at + timedelta(minutes=1),
        reason_code="claim.observed",
        route_identity=observed_identity,
        transition_seq=2,
        causation_id=claim_received_proof.receipt.receipt_id,
        payload={
            "from_state": "claim_received",
            "to_state": "claim_validating",
        },
    )
    writer.append_workflow_event(observation)

    proof = build_transition_proof(
        route_identity=observed_identity,
        transition_seq=2,
        event_id="workflow_event:run-1:4",
        receipt_id="receipt:run-1:5",
        event_type="claim_validated",
        receipt_type="claim_validation_receipt",
        reason_code="claim.validated",
        evidence_seq=4,
        occurred_at=occurred_at + timedelta(minutes=2),
        causation_id=observation.event_id,
    )

    result = writer.append_transition_proof(proof)

    assert result.evidence_seq == 5
    assert [row.evidence_seq for row in writer.evidence_timeline(route_identity.run_id)] == [1, 2, 3, 4, 5]


def test_receipt_append_rejects_causation_mismatch(claim_received_proof) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.append_transition_proof(claim_received_proof)

    bad_receipt = replace(
        claim_received_proof.receipt,
        receipt_id="receipt:run-1:3",
        evidence_seq=3,
        inputs={
            **claim_received_proof.receipt.inputs,
            "receipt_id": "receipt:run-1:3",
            "evidence_seq": 3,
        },
        outputs={
            **claim_received_proof.receipt.outputs,
            "receipt_id": "receipt:run-1:3",
            "evidence_seq": 3,
        },
        causation_id="receipt:run-1:bogus",
    )

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_receipt(bad_receipt)

    assert exc.value.reason_code == "evidence.causation_mismatch"


@pytest.mark.parametrize(
    ("proof_mutator", "reason_code"),
    [
        (
            lambda proof: replace(
                proof,
                receipt=replace(proof.receipt, transition_seq=2),
            ),
            "evidence.transition_seq_mismatch",
        ),
        (
            lambda proof: replace(
                proof,
                receipt=replace(
                    proof.receipt,
                    route_identity=replace(proof.receipt.route_identity, lease_id="lease-1"),
                    inputs={
                        **proof.receipt.inputs,
                        "route_identity": {
                            **proof.receipt.inputs["route_identity"],
                            "lease_id": "lease-1",
                        },
                    },
                    outputs={
                        **proof.receipt.outputs,
                        "route_identity": {
                            **proof.receipt.outputs["route_identity"],
                            "lease_id": "lease-1",
                        },
                    },
                ),
            ),
            "evidence.route_identity_mismatch",
        ),
    ],
)
def test_transition_proof_requires_shared_route_identity_and_transition_seq(
    claim_received_proof,
    proof_mutator,
    reason_code,
) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    bad_proof = proof_mutator(claim_received_proof)

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_transition_proof(bad_proof)

    assert exc.value.reason_code == reason_code


def test_optional_route_identity_ids_are_sticky_once_written(
    claim_received_proof,
    route_identity,
    occurred_at,
) -> None:
    writer = AppendOnlyWorkflowEvidenceWriter()
    writer.append_transition_proof(claim_received_proof)

    first_later_identity = replace(route_identity, lease_id="lease-1", transition_seq=2)
    writer.append_workflow_event(
        replace(
            claim_received_proof.event,
            event_id="workflow_event:run-1:3",
            event_type="claim_validated",
            evidence_seq=3,
            occurred_at=occurred_at + timedelta(minutes=1),
            reason_code="claim.validated",
            route_identity=first_later_identity,
            transition_seq=2,
            causation_id=claim_received_proof.receipt.receipt_id,
            payload={
                "from_state": "claim_received",
                "to_state": "claim_validating",
            },
        )
    )

    with pytest.raises(EvidenceAppendError) as exc:
        writer.append_workflow_event(
            replace(
                claim_received_proof.event,
                event_id="workflow_event:run-1:4",
                event_type="claim_validated",
                evidence_seq=4,
                occurred_at=occurred_at + timedelta(minutes=2),
                reason_code="claim.validated",
                route_identity=replace(first_later_identity, lease_id="lease-2", transition_seq=3),
                transition_seq=3,
                causation_id="workflow_event:run-1:3",
                payload={
                    "from_state": "claim_validating",
                    "to_state": "claim_validating",
                },
            )
        )

    assert exc.value.reason_code == "evidence.route_identity_mismatch"
