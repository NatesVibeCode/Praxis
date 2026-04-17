from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

from runtime.admission_evidence import (
    AdmissionEvidenceRecord,
    append_admission_transition_async,
    persist_admission_evidence,
    persist_submission_evidence_async,
)
from runtime.domain import RouteIdentity, RunState


def _admission_record() -> AdmissionEvidenceRecord:
    return AdmissionEvidenceRecord(
        route_identity=RouteIdentity(
            workflow_id="workflow.alpha",
            run_id="run:alpha",
            request_id="request.alpha",
            authority_context_ref="context.alpha",
            authority_context_digest="digest.alpha",
            claim_id="claim.alpha",
            lease_id=None,
            proposal_id=None,
            promotion_decision_id=None,
            attempt_no=1,
            transition_seq=0,
        ),
        request_payload={"workflow_id": "workflow.alpha"},
        admitted_definition_ref="workflow_definition.alpha:v1",
        admitted_definition_hash="sha256:alpha",
        current_state=RunState.CLAIM_ACCEPTED,
        reason_code="claim.validated",
        decided_at=datetime(2026, 4, 17, 7, 30, tzinfo=timezone.utc),
        validation_result_ref="validation.alpha",
        authority_context_ref="context.alpha",
        admission_decision_id="admission.alpha",
        request_digest="sha256:req.alpha",
    )


def test_persist_admission_evidence_advances_route_identity() -> None:
    captured: dict[str, object] = {}

    class _Writer:
        def commit_submission(self, **kwargs):
            captured["submission_route_identity"] = kwargs["route_identity"]
            return SimpleNamespace(evidence_seq=2)

        def append_transition_proof(self, proof):
            captured["admission_proof"] = proof
            return SimpleNamespace(evidence_seq=4)

    admission = _admission_record()
    persist_admission_evidence(_Writer(), admission=admission)

    assert captured["submission_route_identity"] == replace(
        admission.route_identity,
        transition_seq=1,
    )
    assert captured["admission_proof"].route_identity == replace(
        admission.route_identity,
        transition_seq=2,
    )
    assert captured["admission_proof"].transition_seq == 2


def test_async_admission_helpers_use_writer_async_contract() -> None:
    captured: dict[str, object] = {}

    class _Writer:
        async def persist_submission_async(self, **kwargs):
            captured["submission_route_identity"] = kwargs["route_identity"]
            return SimpleNamespace(evidence_seq=2)

        async def append_transition_proof_async(self, proof):
            captured["admission_proof"] = proof
            return SimpleNamespace(evidence_seq=4)

    admission = _admission_record()
    asyncio.run(persist_submission_evidence_async(_Writer(), admission=admission))
    asyncio.run(
        append_admission_transition_async(
            _Writer(),
            admission=admission,
            submission_evidence_seq=2,
        )
    )

    assert captured["submission_route_identity"] == replace(
        admission.route_identity,
        transition_seq=1,
    )
    assert captured["admission_proof"].route_identity == replace(
        admission.route_identity,
        transition_seq=2,
    )
