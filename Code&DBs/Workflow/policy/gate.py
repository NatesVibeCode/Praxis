"""Canonical gate evaluation and promotion policy authority.

The gate owns the pre-promotion decision over a sealed proposal bundle.
Promotion may only proceed from an accepted gate evaluation whose evidence still
matches the promotion attempt.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Final

from .domain import (
    GateDecisionKind,
    GateEvaluationRecord,
    PolicyBoundaryError,
    PolicyDecisionError,
    PromotionDecisionKind,
    PromotionDecisionRecord,
)

CANONICAL_TARGET_KIND: Final[str] = "canonical_repo"
GATE_READY_REASON: Final[str] = "policy.gate_ready_for_promotion"
GATE_BLOCK_MISSING_PROPOSAL_RECEIPT: Final[str] = "policy.gate_missing_proposal_receipt"
GATE_BLOCK_MISSING_VALIDATION_RECEIPT: Final[str] = "policy.gate_missing_validation_receipt"
GATE_BLOCK_MISSING_MANIFEST_HASH: Final[str] = "policy.gate_missing_manifest_hash"
GATE_BLOCK_MISSING_VALIDATED_HEAD: Final[str] = "policy.gate_missing_validated_head_ref"
GATE_BLOCK_MISSING_TARGET: Final[str] = "policy.gate_missing_target"
GATE_REJECT_VALIDATION_FAILED: Final[str] = "policy.gate_validation_failed"
GATE_REJECT_MANIFEST_MISMATCH: Final[str] = "policy.gate_manifest_hash_mismatch"
GATE_REJECT_HEAD_MISMATCH: Final[str] = "policy.gate_validated_head_mismatch"
GATE_REJECT_TARGET_NOT_CANONICAL: Final[str] = "policy.gate_target_not_canonical"
PROMOTION_ACCEPT_REASON: Final[str] = "policy.promotion_authorized"
PROMOTION_REJECT_GATE_REJECTED: Final[str] = "policy.promotion_gate_rejected"
PROMOTION_REJECT_POLICY_SNAPSHOT_MISMATCH: Final[str] = (
    "policy.promotion_policy_snapshot_mismatch"
)
PROMOTION_REJECT_VALIDATION_RECEIPT_MISMATCH: Final[str] = (
    "policy.promotion_validation_receipt_mismatch"
)
PROMOTION_REJECT_MANIFEST_MISMATCH: Final[str] = "policy.promotion_manifest_hash_mismatch"
PROMOTION_REJECT_TARGET_MISMATCH: Final[str] = "policy.promotion_target_mismatch"
PROMOTION_REJECT_MISSING_CURRENT_HEAD: Final[str] = "policy.promotion_missing_current_head_ref"
PROMOTION_REJECT_MISSING_PROMOTION_INTENT: Final[str] = (
    "policy.promotion_missing_promotion_intent_at"
)
PROMOTION_REJECT_MISSING_FINALIZATION_EVIDENCE: Final[str] = (
    "policy.promotion_missing_finalization_evidence"
)
PROMOTION_REJECT_STALE_HEAD: Final[str] = "policy.promotion_stale_validated_head"
PROMOTION_GATE_BLOCKED: Final[str] = "policy.promotion_gate_blocked"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_text(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise PolicyBoundaryError(f"{field_name} must be a non-empty string")
    return normalized


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _stable_gate_evaluation_id(
    *,
    proposal_id: str,
    validation_receipt_ref: str | None,
) -> str:
    suffix = validation_receipt_ref or "missing-validation-receipt"
    return f"gate:{proposal_id}:{suffix}"


def _stable_promotion_decision_id(*, gate_evaluation_id: str) -> str:
    return f"promotion:{gate_evaluation_id}"


def evaluate_gate(
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
    """Evaluate whether a sealed proposal has enough evidence to proceed."""

    proposal_id = _require_text(proposal_id, field_name="proposal_id")
    workflow_id = _require_text(workflow_id, field_name="workflow_id")
    run_id = _require_text(run_id, field_name="run_id")
    policy_snapshot_ref = _require_text(
        policy_snapshot_ref,
        field_name="policy_snapshot_ref",
    )
    decided_by = _require_text(decided_by, field_name="decided_by")
    validation_receipt_ref = _optional_text(validation_receipt_ref)
    proposal_manifest_hash = _optional_text(proposal_manifest_hash)
    validated_head_ref = _optional_text(validated_head_ref)
    target_kind = _optional_text(target_kind)
    target_ref = _optional_text(target_ref)
    validated_manifest_hash = _optional_text(validated_manifest_hash)
    current_head_ref = _optional_text(current_head_ref)

    if not proposal_receipt_present:
        decision = GateDecisionKind.BLOCK
        reason_code = GATE_BLOCK_MISSING_PROPOSAL_RECEIPT
    elif validation_receipt_ref is None:
        decision = GateDecisionKind.BLOCK
        reason_code = GATE_BLOCK_MISSING_VALIDATION_RECEIPT
    elif proposal_manifest_hash is None:
        decision = GateDecisionKind.BLOCK
        reason_code = GATE_BLOCK_MISSING_MANIFEST_HASH
    elif validated_head_ref is None:
        decision = GateDecisionKind.BLOCK
        reason_code = GATE_BLOCK_MISSING_VALIDATED_HEAD
    elif target_kind is None or target_ref is None:
        decision = GateDecisionKind.BLOCK
        reason_code = GATE_BLOCK_MISSING_TARGET
    elif target_kind != CANONICAL_TARGET_KIND:
        decision = GateDecisionKind.REJECT
        reason_code = GATE_REJECT_TARGET_NOT_CANONICAL
    elif not validation_passed:
        decision = GateDecisionKind.REJECT
        reason_code = GATE_REJECT_VALIDATION_FAILED
    elif (
        validated_manifest_hash is not None
        and proposal_manifest_hash != validated_manifest_hash
    ):
        decision = GateDecisionKind.REJECT
        reason_code = GATE_REJECT_MANIFEST_MISMATCH
    elif current_head_ref is not None and current_head_ref != validated_head_ref:
        decision = GateDecisionKind.REJECT
        reason_code = GATE_REJECT_HEAD_MISMATCH
    else:
        decision = GateDecisionKind.ACCEPT
        reason_code = GATE_READY_REASON

    return GateEvaluationRecord(
        gate_evaluation_id=_stable_gate_evaluation_id(
            proposal_id=proposal_id,
            validation_receipt_ref=validation_receipt_ref,
        ),
        proposal_id=proposal_id,
        workflow_id=workflow_id,
        run_id=run_id,
        decision=decision,
        reason_code=reason_code,
        decided_at=decided_at or _now(),
        decided_by=decided_by,
        policy_snapshot_ref=policy_snapshot_ref,
        validation_receipt_ref=validation_receipt_ref or "",
        proposal_manifest_hash=proposal_manifest_hash or "",
        validated_head_ref=validated_head_ref or "",
        target_kind=target_kind or "",
        target_ref=target_ref or "",
    )


def decide_promotion(
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
    """Authorize or reject promotion from an explicit gate evaluation only."""

    policy_snapshot_ref = _require_text(
        policy_snapshot_ref,
        field_name="policy_snapshot_ref",
    )
    decided_by = _require_text(decided_by, field_name="decided_by")
    current_head_ref = _optional_text(current_head_ref)
    proposal_manifest_hash = _optional_text(proposal_manifest_hash)
    validation_receipt_ref = _optional_text(validation_receipt_ref)
    target_kind = _optional_text(target_kind)
    target_ref = _optional_text(target_ref)
    canonical_commit_ref = _optional_text(canonical_commit_ref)

    if finalized_at is not None and canonical_commit_ref is None:
        raise PolicyBoundaryError(
            "finalized_at requires canonical_commit_ref to be present",
        )
    if canonical_commit_ref is not None and finalized_at is None:
        raise PolicyBoundaryError(
            "canonical_commit_ref requires finalized_at to be present",
        )
    if (
        promotion_intent_at is not None
        and finalized_at is not None
        and finalized_at < promotion_intent_at
    ):
        raise PolicyBoundaryError(
            "finalized_at must be greater than or equal to promotion_intent_at",
        )

    if gate_evaluation.decision is GateDecisionKind.BLOCK:
        raise PolicyDecisionError(
            PROMOTION_GATE_BLOCKED,
            "promotion cannot proceed from a blocked gate evaluation",
        )

    reason_code = PROMOTION_ACCEPT_REASON
    decision = PromotionDecisionKind.ACCEPT

    if gate_evaluation.decision is GateDecisionKind.REJECT:
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_GATE_REJECTED
    elif policy_snapshot_ref != gate_evaluation.policy_snapshot_ref:
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_POLICY_SNAPSHOT_MISMATCH
    elif (
        validation_receipt_ref is not None
        and validation_receipt_ref != gate_evaluation.validation_receipt_ref
    ):
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_VALIDATION_RECEIPT_MISMATCH
    elif (
        proposal_manifest_hash is not None
        and proposal_manifest_hash != gate_evaluation.proposal_manifest_hash
    ):
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_MANIFEST_MISMATCH
    elif (
        target_kind is not None
        and target_kind != gate_evaluation.target_kind
    ) or (
        target_ref is not None
        and target_ref != gate_evaluation.target_ref
    ):
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_TARGET_MISMATCH
    elif current_head_ref is None:
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_MISSING_CURRENT_HEAD
    elif current_head_ref != gate_evaluation.validated_head_ref:
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_STALE_HEAD
    elif promotion_intent_at is None:
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_MISSING_PROMOTION_INTENT
    elif finalized_at is None or canonical_commit_ref is None:
        decision = PromotionDecisionKind.REJECT
        reason_code = PROMOTION_REJECT_MISSING_FINALIZATION_EVIDENCE

    resolved_decided_at = decided_at or _now()
    return PromotionDecisionRecord(
        promotion_decision_id=_stable_promotion_decision_id(
            gate_evaluation_id=gate_evaluation.gate_evaluation_id,
        ),
        gate_evaluation_id=gate_evaluation.gate_evaluation_id,
        proposal_id=gate_evaluation.proposal_id,
        workflow_id=gate_evaluation.workflow_id,
        run_id=gate_evaluation.run_id,
        decision=decision,
        reason_code=reason_code,
        decided_at=resolved_decided_at,
        decided_by=decided_by,
        policy_snapshot_ref=policy_snapshot_ref,
        validation_receipt_ref=gate_evaluation.validation_receipt_ref,
        proposal_manifest_hash=gate_evaluation.proposal_manifest_hash,
        validated_head_ref=gate_evaluation.validated_head_ref,
        target_kind=gate_evaluation.target_kind,
        target_ref=gate_evaluation.target_ref,
        current_head_ref=current_head_ref,
        promotion_intent_at=promotion_intent_at
        if decision is PromotionDecisionKind.ACCEPT
        else None,
        finalized_at=finalized_at if decision is PromotionDecisionKind.ACCEPT else None,
        canonical_commit_ref=canonical_commit_ref
        if decision is PromotionDecisionKind.ACCEPT
        else None,
    )


__all__ = [
    "CANONICAL_TARGET_KIND",
    "PROMOTION_GATE_BLOCKED",
    "decide_promotion",
    "evaluate_gate",
]
