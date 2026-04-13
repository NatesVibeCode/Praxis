"""Policy evaluation helpers for workflow submission review."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime
from typing import Any

from policy.domain import PolicyDecisionError, PolicyEngine
from policy.gate import CANONICAL_TARGET_KIND

_PUBLISH_REVIEW_ROLE_TASK_TYPES = frozenset({"publish", "publish_policy"})
_DEFAULT_PUBLISH_POLICY_SNAPSHOT_REF = "policy_snapshot:workflow_submission_publish_v1"
_DEFAULT_PUBLISH_TARGET_REF = "repo:canonical"


def _insert_gate_evaluation(conn, *, gate_evaluation) -> None:
    conn.execute(
        """
        INSERT INTO gate_evaluations (
            gate_evaluation_id,
            proposal_id,
            workflow_id,
            run_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_receipt_ref,
            proposal_manifest_hash,
            validated_head_ref,
            target_kind,
            target_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
        )
        ON CONFLICT (gate_evaluation_id) DO NOTHING
        """,
        gate_evaluation.gate_evaluation_id,
        gate_evaluation.proposal_id,
        gate_evaluation.workflow_id,
        gate_evaluation.run_id,
        gate_evaluation.decision.value,
        gate_evaluation.reason_code,
        gate_evaluation.decided_at,
        gate_evaluation.decided_by,
        gate_evaluation.policy_snapshot_ref,
        gate_evaluation.validation_receipt_ref,
        gate_evaluation.proposal_manifest_hash,
        gate_evaluation.validated_head_ref,
        gate_evaluation.target_kind,
        gate_evaluation.target_ref,
    )


def _insert_promotion_decision(conn, *, promotion_decision) -> None:
    conn.execute(
        """
        INSERT INTO promotion_decisions (
            promotion_decision_id,
            gate_evaluation_id,
            proposal_id,
            workflow_id,
            run_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_receipt_ref,
            proposal_manifest_hash,
            validated_head_ref,
            current_head_ref,
            promotion_intent_at,
            finalized_at,
            canonical_commit_ref,
            target_kind,
            target_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19
        )
        ON CONFLICT (gate_evaluation_id) DO NOTHING
        """,
        promotion_decision.promotion_decision_id,
        promotion_decision.gate_evaluation_id,
        promotion_decision.proposal_id,
        promotion_decision.workflow_id,
        promotion_decision.run_id,
        promotion_decision.decision.value,
        promotion_decision.reason_code,
        promotion_decision.decided_at,
        promotion_decision.decided_by,
        promotion_decision.policy_snapshot_ref,
        promotion_decision.validation_receipt_ref,
        promotion_decision.proposal_manifest_hash,
        promotion_decision.validated_head_ref,
        promotion_decision.current_head_ref,
        promotion_decision.promotion_intent_at,
        promotion_decision.finalized_at,
        promotion_decision.canonical_commit_ref,
        promotion_decision.target_kind,
        promotion_decision.target_ref,
    )


def evaluate_publish_policy(
    conn,
    *,
    submission: Mapping[str, Any],
    submission_id: str,
    run_id: str,
    workflow_id: str,
    reviewer_job_label: str,
    reviewer_role: str,
    review_decision: str,
    policy_snapshot_ref: str | None,
    target_ref: str | None,
    current_head_ref: str,
    promotion_intent_at: datetime | None,
    finalized_at: datetime | None,
    canonical_commit_ref: str | None,
    proposal_id: str,
    manifest_hash: str,
    validation_receipt_ref: str,
    has_verification_refs: bool,
) -> dict[str, Any]:
    """Evaluate gate and optionally promote. Returns policy projection dict.

    All context-derived values (head refs, IDs, hashes) are accepted as
    parameters so this module stays import-cycle-free.
    """
    effective_policy_snapshot_ref = policy_snapshot_ref or _DEFAULT_PUBLISH_POLICY_SNAPSHOT_REF
    effective_target_ref = target_ref or _DEFAULT_PUBLISH_TARGET_REF
    engine = PolicyEngine()
    gate_evaluation = engine.evaluate_gate(
        proposal_id=proposal_id,
        workflow_id=workflow_id,
        run_id=run_id,
        validation_receipt_ref=validation_receipt_ref,
        proposal_manifest_hash=manifest_hash,
        validated_head_ref=current_head_ref,
        target_kind=CANONICAL_TARGET_KIND,
        target_ref=effective_target_ref,
        policy_snapshot_ref=effective_policy_snapshot_ref,
        decided_by=f"workflow_submission.{reviewer_job_label}",
        validation_passed=has_verification_refs,
        proposal_receipt_present=True,
        validated_manifest_hash=manifest_hash,
        current_head_ref=current_head_ref,
    )
    _insert_gate_evaluation(conn, gate_evaluation=gate_evaluation)

    promotion_decision = None
    if review_decision == "approve" and all(
        value is not None
        for value in (promotion_intent_at, finalized_at, canonical_commit_ref)
    ):
        try:
            promotion_decision = engine.decide_promotion(
                gate_evaluation=gate_evaluation,
                policy_snapshot_ref=effective_policy_snapshot_ref,
                decided_by=f"workflow_submission.{reviewer_job_label}",
                current_head_ref=current_head_ref,
                proposal_manifest_hash=gate_evaluation.proposal_manifest_hash,
                validation_receipt_ref=gate_evaluation.validation_receipt_ref,
                target_kind=gate_evaluation.target_kind,
                target_ref=gate_evaluation.target_ref,
                promotion_intent_at=promotion_intent_at,
                finalized_at=finalized_at,
                canonical_commit_ref=canonical_commit_ref,
            )
        except PolicyDecisionError:
            promotion_decision = None
        if promotion_decision is not None:
            _insert_promotion_decision(conn, promotion_decision=promotion_decision)

    return {
        "gate_evaluation": asdict(gate_evaluation),
        "promotion_decision": None if promotion_decision is None else asdict(promotion_decision),
    }
