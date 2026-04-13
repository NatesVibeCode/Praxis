"""Policy authority.

Owns admission, gate, retry, and promotion decisions. Decisions are explicit
records, not inferred outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


class PolicyBoundaryError(RuntimeError):
    """Raised when policy inputs are ambiguous or incomplete."""


class PolicyDecisionError(RuntimeError):
    """Raised when a policy decision cannot be produced safely."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


class AdmissionDecisionKind(str, Enum):
    ADMIT = "admit"
    REJECT = "reject"
    BLOCK = "block"


class GateDecisionKind(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"
    BLOCK = "block"


class PromotionDecisionKind(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class AdmissionDecisionRecord:
    """Canonical admission decision for a submitted workflow."""

    admission_decision_id: str
    workflow_id: str
    request_id: str
    decision: AdmissionDecisionKind
    reason_code: str
    decided_at: datetime
    decided_by: str
    policy_snapshot_ref: str
    validation_result_ref: str
    authority_context_ref: str


@dataclass(frozen=True, slots=True)
class GateEvaluationRecord:
    """Canonical gate evaluation result for a sealed proposal."""

    gate_evaluation_id: str
    proposal_id: str
    workflow_id: str
    run_id: str
    decision: GateDecisionKind
    reason_code: str
    decided_at: datetime
    decided_by: str
    policy_snapshot_ref: str
    validation_receipt_ref: str
    proposal_manifest_hash: str
    validated_head_ref: str
    target_kind: str
    target_ref: str


@dataclass(frozen=True, slots=True)
class PromotionDecisionRecord:
    """Canonical promotion decision row."""

    promotion_decision_id: str
    gate_evaluation_id: str
    proposal_id: str
    workflow_id: str
    run_id: str
    decision: PromotionDecisionKind
    reason_code: str
    decided_at: datetime
    decided_by: str
    policy_snapshot_ref: str
    validation_receipt_ref: str
    proposal_manifest_hash: str
    validated_head_ref: str
    target_kind: str
    target_ref: str
    current_head_ref: str | None = None
    promotion_intent_at: datetime | None = None
    finalized_at: datetime | None = None
    canonical_commit_ref: str | None = None


class PolicyEngine:
    """Decision boundary stub for admission, gate, and promotion."""

    def decide_admission(
        self,
        *,
        workflow_id: str,
        request_id: str,
        validation_result_ref: str,
        authority_context_ref: str,
        policy_snapshot_ref: str,
        decided_by: str,
        admitted: bool,
        rejection_reason_code: str | None = None,
        decided_at: datetime | None = None,
    ) -> AdmissionDecisionRecord:
        if admitted and rejection_reason_code is not None:
            raise PolicyBoundaryError(
                "admission cannot carry a rejection reason when admitted=True",
            )
        if not admitted and not rejection_reason_code:
            raise PolicyBoundaryError(
                "admission reject decisions require a rejection_reason_code",
            )

        decision = AdmissionDecisionKind.ADMIT if admitted else AdmissionDecisionKind.REJECT
        reason_code = "policy.admission_allowed" if admitted else rejection_reason_code
        assert reason_code is not None  # Narrowed by the guard above.
        return AdmissionDecisionRecord(
            admission_decision_id=f"admission:{validation_result_ref}",
            workflow_id=workflow_id,
            request_id=request_id,
            decision=decision,
            reason_code=reason_code,
            decided_at=decided_at or datetime.now(timezone.utc),
            decided_by=decided_by,
            policy_snapshot_ref=policy_snapshot_ref,
            validation_result_ref=validation_result_ref,
            authority_context_ref=authority_context_ref,
        )

    def evaluate_gate(
        self,
        *,
        proposal_id: str,
        workflow_id: str,
        run_id: str,
        validation_receipt_ref: str,
        proposal_manifest_hash: str,
        validated_head_ref: str,
        target_kind: str,
        target_ref: str,
        policy_snapshot_ref: str,
        decided_by: str,
        validation_passed: bool = True,
        proposal_receipt_present: bool = True,
        validated_manifest_hash: str | None = None,
        current_head_ref: str | None = None,
        decided_at: datetime | None = None,
    ) -> GateEvaluationRecord:
        from .gate import evaluate_gate

        return evaluate_gate(
            proposal_id=proposal_id,
            workflow_id=workflow_id,
            run_id=run_id,
            validation_receipt_ref=validation_receipt_ref,
            proposal_manifest_hash=proposal_manifest_hash,
            validated_head_ref=validated_head_ref,
            target_kind=target_kind,
            target_ref=target_ref,
            policy_snapshot_ref=policy_snapshot_ref,
            decided_by=decided_by,
            validation_passed=validation_passed,
            proposal_receipt_present=proposal_receipt_present,
            validated_manifest_hash=validated_manifest_hash,
            current_head_ref=current_head_ref,
            decided_at=decided_at,
        )

    def decide_promotion(
        self,
        *,
        gate_evaluation: GateEvaluationRecord,
        policy_snapshot_ref: str,
        decided_by: str,
        current_head_ref: str,
        proposal_manifest_hash: str | None = None,
        validation_receipt_ref: str | None = None,
        target_kind: str | None = None,
        target_ref: str | None = None,
        promotion_intent_at: datetime | None = None,
        finalized_at: datetime | None = None,
        canonical_commit_ref: str | None = None,
        decided_at: datetime | None = None,
    ) -> PromotionDecisionRecord:
        from .gate import decide_promotion

        return decide_promotion(
            gate_evaluation=gate_evaluation,
            policy_snapshot_ref=policy_snapshot_ref,
            decided_by=decided_by,
            current_head_ref=current_head_ref,
            proposal_manifest_hash=proposal_manifest_hash,
            validation_receipt_ref=validation_receipt_ref,
            target_kind=target_kind,
            target_ref=target_ref,
            promotion_intent_at=promotion_intent_at,
            finalized_at=finalized_at,
            canonical_commit_ref=canonical_commit_ref,
            decided_at=decided_at,
        )


__all__ = [
    "AdmissionDecisionKind",
    "AdmissionDecisionRecord",
    "GateDecisionKind",
    "GateEvaluationRecord",
    "PolicyBoundaryError",
    "PolicyDecisionError",
    "PolicyEngine",
    "PromotionDecisionKind",
    "PromotionDecisionRecord",
]
