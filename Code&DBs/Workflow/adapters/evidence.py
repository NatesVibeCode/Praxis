"""Translation helpers for workflow_event and receipt envelopes.

Adapters own normalization only. They do not append evidence or decide truth.
The helpers here build typed workflow_event and receipt proof bundles from raw
execution data so receipts/ can persist them atomically.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from receipts.evidence import ArtifactRef, DecisionRef, ReceiptV1, RouteIdentity, TransitionProofV1, WorkflowEventV1

__all__ = [
    "build_claim_received_proof",
    "build_transition_proof",
]


def _route_identity_payload(
    route_identity: RouteIdentity,
    *,
    transition_seq: int | None = None,
) -> dict[str, Any]:
    return {
        "workflow_id": route_identity.workflow_id,
        "run_id": route_identity.run_id,
        "request_id": route_identity.request_id,
        "authority_context_ref": route_identity.authority_context_ref,
        "authority_context_digest": route_identity.authority_context_digest,
        "claim_id": route_identity.claim_id,
        "lease_id": route_identity.lease_id,
        "proposal_id": route_identity.proposal_id,
        "promotion_decision_id": route_identity.promotion_decision_id,
        "attempt_no": route_identity.attempt_no,
        "transition_seq": route_identity.transition_seq if transition_seq is None else transition_seq,
    }


def _with_lineage_defaults(
    value: Mapping[str, Any] | None,
    *,
    route_identity: RouteIdentity,
    event_id: str,
    receipt_id: str,
    evidence_seq: int,
    transition_seq: int,
    causation_id: str | None = None,
) -> dict[str, Any]:
    merged = dict(value or {})
    defaults: dict[str, Any] = {
        "route_identity": _route_identity_payload(route_identity, transition_seq=transition_seq),
        "event_id": event_id,
        "receipt_id": receipt_id,
        "evidence_seq": evidence_seq,
        "transition_seq": transition_seq,
    }
    if causation_id is not None:
        defaults["causation_id"] = causation_id
    for key, expected in defaults.items():
        if key in merged and merged[key] != expected:
            raise ValueError(f"{key} conflicts with explicit proof lineage")
        merged[key] = expected
    return merged


def build_transition_proof(
    *,
    route_identity: RouteIdentity,
    transition_seq: int,
    event_id: str,
    receipt_id: str,
    event_type: str,
    receipt_type: str,
    reason_code: str,
    evidence_seq: int,
    occurred_at: datetime,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    actor_type: str = "runtime",
    executor_type: str = "runtime",
    status: str = "succeeded",
    payload: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Any] | None = None,
    outputs: Mapping[str, Any] | None = None,
    artifacts: Sequence[ArtifactRef | Mapping[str, Any]] = (),
    decision_refs: Sequence[DecisionRef | Mapping[str, Any]] = (),
    causation_id: str | None = None,
    node_id: str | None = None,
    failure_code: str | None = None,
) -> TransitionProofV1:
    """Build a typed workflow_event and receipt proof bundle."""

    started_at = occurred_at if started_at is None else started_at
    finished_at = occurred_at if finished_at is None else finished_at
    normalized_route_identity = replace(route_identity, transition_seq=transition_seq)
    event = WorkflowEventV1(
        event_id=event_id,
        event_type=event_type,
        schema_version=1,
        workflow_id=normalized_route_identity.workflow_id,
        run_id=normalized_route_identity.run_id,
        request_id=normalized_route_identity.request_id,
        route_identity=normalized_route_identity,
        transition_seq=transition_seq,
        evidence_seq=evidence_seq,
        occurred_at=occurred_at,
        actor_type=actor_type,
        reason_code=reason_code,
        payload=_with_lineage_defaults(
            payload,
            route_identity=normalized_route_identity,
            event_id=event_id,
            receipt_id=receipt_id,
            evidence_seq=evidence_seq,
            transition_seq=transition_seq,
            causation_id=causation_id,
        ),
        causation_id=causation_id,
        node_id=node_id,
    )
    receipt = ReceiptV1(
        receipt_id=receipt_id,
        receipt_type=receipt_type,
        schema_version=1,
        workflow_id=normalized_route_identity.workflow_id,
        run_id=normalized_route_identity.run_id,
        request_id=normalized_route_identity.request_id,
        route_identity=normalized_route_identity,
        transition_seq=transition_seq,
        evidence_seq=evidence_seq + 1,
        started_at=started_at,
        finished_at=finished_at,
        executor_type=executor_type,
        status=status,
        inputs=_with_lineage_defaults(
            inputs if inputs is not None else payload,
            route_identity=normalized_route_identity,
            event_id=event_id,
            receipt_id=receipt_id,
            evidence_seq=evidence_seq,
            transition_seq=transition_seq,
            causation_id=causation_id,
        ),
        outputs=_with_lineage_defaults(
            outputs,
            route_identity=normalized_route_identity,
            event_id=event_id,
            receipt_id=receipt_id,
            evidence_seq=evidence_seq + 1,
            transition_seq=transition_seq,
            causation_id=causation_id,
        ),
        artifacts=tuple(artifacts),
        decision_refs=tuple(decision_refs),
        causation_id=event_id,
        node_id=node_id,
        attempt_no=normalized_route_identity.attempt_no,
        supersedes_receipt_id=None,
        failure_code=failure_code,
    )
    return TransitionProofV1(
        route_identity=normalized_route_identity,
        transition_seq=transition_seq,
        event=event,
        receipt=receipt,
    )


def build_claim_received_proof(
    *,
    route_identity: RouteIdentity,
    event_id: str,
    receipt_id: str,
    evidence_seq: int,
    transition_seq: int,
    request_payload: Mapping[str, Any],
    admitted_definition_ref: str,
    admitted_definition_hash: str,
    occurred_at: datetime,
    causation_id: str | None = None,
) -> TransitionProofV1:
    """Build the initial claim_received workflow_event/receipt proof bundle."""

    normalized_route_identity = replace(route_identity, transition_seq=transition_seq)
    route_payload = _route_identity_payload(
        normalized_route_identity,
        transition_seq=transition_seq,
    )
    payload = {
        "claim_envelope": dict(request_payload),
        "admitted_definition_ref": admitted_definition_ref,
        "admitted_definition_hash": admitted_definition_hash,
        "route_identity": route_payload,
    }
    inputs = dict(payload)
    outputs = {
        "event_id": event_id,
        "receipt_id": receipt_id,
        "run_id": route_identity.run_id,
        "request_id": route_identity.request_id,
        "claim_id": route_identity.claim_id,
        "route_identity": route_payload,
        "transition_seq": transition_seq,
        "evidence_seq": evidence_seq + 1,
    }
    return build_transition_proof(
        route_identity=normalized_route_identity,
        transition_seq=transition_seq,
        event_id=event_id,
        receipt_id=receipt_id,
        event_type="claim_received",
        receipt_type="claim_received_receipt",
        reason_code="claim.received",
        evidence_seq=evidence_seq,
        occurred_at=occurred_at,
        actor_type="runtime",
        executor_type="runtime.submit",
        status="claim_received",
        payload=payload,
        inputs=inputs,
        outputs=outputs,
        artifacts=(),
        decision_refs=(),
        causation_id=causation_id,
        node_id=None,
        failure_code=None,
    )
