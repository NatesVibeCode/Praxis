"""Shared helpers for canonical admission evidence writes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from adapters import build_transition_proof
from runtime.domain import RouteIdentity, RunState
from runtime.execution.evidence import (
    ADMISSION_DECISION_SOURCE_TABLE,
    _event_id,
    _receipt_id,
)
from runtime.execution.orchestrator import (
    CLAIM_REJECTED_EVENT_TYPE,
    CLAIM_VALIDATED_EVENT_TYPE,
    CLAIM_VALIDATION_RECEIPT_TYPE,
)


@dataclass(frozen=True, slots=True)
class AdmissionEvidenceRecord:
    """Canonical facts needed to seed submission and admission evidence."""

    route_identity: RouteIdentity
    request_payload: Mapping[str, Any]
    admitted_definition_ref: str
    admitted_definition_hash: str
    current_state: RunState
    reason_code: str
    decided_at: datetime
    validation_result_ref: str
    authority_context_ref: str
    admission_decision_id: str
    request_digest: str


def build_admission_transition_proof(
    admission: AdmissionEvidenceRecord,
    *,
    submission_evidence_seq: int,
):
    """Build the canonical claim_validated/claim_rejected transition proof."""

    event_type = (
        CLAIM_REJECTED_EVENT_TYPE
        if admission.current_state is RunState.CLAIM_REJECTED
        else CLAIM_VALIDATED_EVENT_TYPE
    )
    return build_transition_proof(
        route_identity=replace(admission.route_identity, transition_seq=2),
        transition_seq=2,
        event_id=_event_id(
            run_id=admission.route_identity.run_id,
            evidence_seq=submission_evidence_seq + 1,
        ),
        receipt_id=_receipt_id(
            run_id=admission.route_identity.run_id,
            evidence_seq=submission_evidence_seq + 2,
        ),
        event_type=event_type,
        receipt_type=CLAIM_VALIDATION_RECEIPT_TYPE,
        reason_code=admission.reason_code,
        evidence_seq=submission_evidence_seq + 1,
        occurred_at=admission.decided_at,
        started_at=admission.decided_at,
        finished_at=admission.decided_at,
        executor_type="runtime.intake",
        status=admission.current_state.value,
        payload={
            "from_state": RunState.CLAIM_RECEIVED.value,
            "to_state": admission.current_state.value,
            "validation_result_ref": admission.validation_result_ref,
            "authority_context_ref": admission.authority_context_ref,
            "admission_decision_id": admission.admission_decision_id,
        },
        inputs={
            "validation_result_ref": admission.validation_result_ref,
            "request_digest": admission.request_digest,
            "authority_context_ref": admission.authority_context_ref,
        },
        outputs={
            "admission_decision_id": admission.admission_decision_id,
            "to_state": admission.current_state.value,
        },
        decision_refs=(
            {
                "decision_type": "admission",
                "decision_id": admission.admission_decision_id,
                "reason_code": admission.reason_code,
                "source_table": ADMISSION_DECISION_SOURCE_TABLE,
            },
        ),
        failure_code=(
            admission.reason_code
            if admission.current_state is RunState.CLAIM_REJECTED
            else None
        ),
    )


def persist_admission_evidence(
    writer,
    *,
    admission: AdmissionEvidenceRecord,
):
    """Synchronously seed claim_received plus admission evidence."""

    submission_result = writer.commit_submission(
        route_identity=replace(admission.route_identity, transition_seq=1),
        request_payload=dict(admission.request_payload),
        admitted_definition_ref=admission.admitted_definition_ref,
        admitted_definition_hash=admission.admitted_definition_hash,
    )
    admission_result = writer.append_transition_proof(
        build_admission_transition_proof(
            admission,
            submission_evidence_seq=submission_result.evidence_seq,
        )
    )
    return submission_result, admission_result


async def persist_submission_evidence_async(
    writer,
    *,
    admission: AdmissionEvidenceRecord,
):
    """Asynchronously seed the initial claim_received evidence bundle."""

    return await writer.persist_submission_async(
        route_identity=replace(admission.route_identity, transition_seq=1),
        request_payload=dict(admission.request_payload),
        admitted_definition_ref=admission.admitted_definition_ref,
        admitted_definition_hash=admission.admitted_definition_hash,
    )


async def append_admission_transition_async(
    writer,
    *,
    admission: AdmissionEvidenceRecord,
    submission_evidence_seq: int,
):
    """Asynchronously append the canonical admission transition proof."""

    return await writer.append_transition_proof_async(
        build_admission_transition_proof(
            admission,
            submission_evidence_seq=submission_evidence_seq,
        )
    )


__all__ = [
    "AdmissionEvidenceRecord",
    "append_admission_transition_async",
    "build_admission_transition_proof",
    "persist_admission_evidence",
    "persist_submission_evidence_async",
]
