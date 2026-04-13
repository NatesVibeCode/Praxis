-- Canonical gate evaluation authority and tightened promotion decision boundary.
-- Gate evaluation is the pre-promotion authority. Promotion decisions are final
-- accept or reject records only.

CREATE TABLE IF NOT EXISTS gate_evaluations (
    gate_evaluation_id text PRIMARY KEY,
    proposal_id text NOT NULL,
    workflow_id text NOT NULL,
    run_id text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('accept', 'reject', 'block')),
    reason_code text NOT NULL,
    decided_at timestamptz NOT NULL,
    decided_by text NOT NULL,
    policy_snapshot_ref text NOT NULL,
    validation_receipt_ref text,
    proposal_manifest_hash text,
    validated_head_ref text,
    target_kind text,
    target_ref text,
    CONSTRAINT gate_evaluations_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS gate_evaluations_proposal_id_decided_at_idx
    ON gate_evaluations (proposal_id, decided_at DESC);

CREATE INDEX IF NOT EXISTS gate_evaluations_decision_decided_at_idx
    ON gate_evaluations (decision, decided_at DESC);

CREATE INDEX IF NOT EXISTS gate_evaluations_workflow_id_decided_at_idx
    ON gate_evaluations (workflow_id, decided_at DESC);

COMMENT ON TABLE gate_evaluations IS 'Canonical gate evaluation rows for sealed proposals. Owned by policy/.';
COMMENT ON COLUMN gate_evaluations.decision IS 'Pre-promotion outcome. Block stays here and must not be written as promotion truth.';
COMMENT ON COLUMN gate_evaluations.reason_code IS 'Machine-readable gate outcome code. Free-form prose is not policy evidence.';

ALTER TABLE gate_evaluations
    DROP CONSTRAINT IF EXISTS gate_evaluations_authority_fields_present;

ALTER TABLE gate_evaluations
    ADD CONSTRAINT gate_evaluations_authority_fields_present
    CHECK (
        validation_receipt_ref IS NOT NULL
        AND proposal_manifest_hash IS NOT NULL
        AND validated_head_ref IS NOT NULL
        AND target_kind IS NOT NULL
        AND target_ref IS NOT NULL
    ) NOT VALID;

CREATE UNIQUE INDEX IF NOT EXISTS gate_evaluations_authority_truth_idx
    ON gate_evaluations (
        gate_evaluation_id,
        proposal_id,
        workflow_id,
        run_id,
        validation_receipt_ref,
        proposal_manifest_hash,
        validated_head_ref,
        target_kind,
        target_ref
    );

ALTER TABLE promotion_decisions
    DROP CONSTRAINT IF EXISTS promotion_decisions_decision_check;

ALTER TABLE promotion_decisions
    DROP CONSTRAINT IF EXISTS promotion_decisions_gate_evaluation_id_fkey;

ALTER TABLE promotion_decisions
    DROP CONSTRAINT IF EXISTS promotion_decisions_gate_truth_fkey;

ALTER TABLE promotion_decisions
    DROP CONSTRAINT IF EXISTS promotion_decisions_accept_evidence_check;

ALTER TABLE promotion_decisions
    DROP CONSTRAINT IF EXISTS promotion_decisions_finalization_pair_check;

ALTER TABLE promotion_decisions
    DROP CONSTRAINT IF EXISTS promotion_decisions_reject_has_no_finalization_check;

ALTER TABLE promotion_decisions
    ADD COLUMN IF NOT EXISTS gate_evaluation_id text;

ALTER TABLE promotion_decisions
    ADD COLUMN IF NOT EXISTS current_head_ref text;

ALTER TABLE promotion_decisions
    ADD CONSTRAINT promotion_decisions_decision_check
    CHECK (decision IN ('accept', 'reject')) NOT VALID;

ALTER TABLE promotion_decisions
    DROP CONSTRAINT IF EXISTS promotion_decisions_gate_context_present_check;

ALTER TABLE promotion_decisions
    ADD CONSTRAINT promotion_decisions_gate_context_present_check
    CHECK (
        gate_evaluation_id IS NOT NULL
        AND validated_head_ref IS NOT NULL
        AND target_kind IS NOT NULL
        AND target_ref IS NOT NULL
    ) NOT VALID;

ALTER TABLE promotion_decisions
    ADD CONSTRAINT promotion_decisions_gate_truth_fkey
    FOREIGN KEY (
        gate_evaluation_id,
        proposal_id,
        workflow_id,
        run_id,
        validation_receipt_ref,
        proposal_manifest_hash,
        validated_head_ref,
        target_kind,
        target_ref
    )
    REFERENCES gate_evaluations (
        gate_evaluation_id,
        proposal_id,
        workflow_id,
        run_id,
        validation_receipt_ref,
        proposal_manifest_hash,
        validated_head_ref,
        target_kind,
        target_ref
    )
    ON DELETE RESTRICT
    NOT VALID;

ALTER TABLE promotion_decisions
    ADD CONSTRAINT promotion_decisions_finalization_pair_check
    CHECK (
        (finalized_at IS NULL AND canonical_commit_ref IS NULL)
        OR (finalized_at IS NOT NULL AND canonical_commit_ref IS NOT NULL)
    ) NOT VALID;

ALTER TABLE promotion_decisions
    ADD CONSTRAINT promotion_decisions_reject_has_no_finalization_check
    CHECK (
        decision = 'accept'
        OR (
            promotion_intent_at IS NULL
            AND finalized_at IS NULL
            AND canonical_commit_ref IS NULL
        )
    ) NOT VALID;

ALTER TABLE promotion_decisions
    ADD CONSTRAINT promotion_decisions_accept_evidence_check
    CHECK (
        decision <> 'accept'
        OR (
            current_head_ref IS NOT NULL
            AND current_head_ref = validated_head_ref
            AND promotion_intent_at IS NOT NULL
            AND finalized_at IS NOT NULL
            AND canonical_commit_ref IS NOT NULL
            AND finalized_at >= promotion_intent_at
        )
    ) NOT VALID;

CREATE UNIQUE INDEX IF NOT EXISTS promotion_decisions_gate_evaluation_id_idx
    ON promotion_decisions (gate_evaluation_id);

COMMENT ON COLUMN promotion_decisions.gate_evaluation_id IS 'Required gate authority row that this final promotion decision is derived from.';
COMMENT ON COLUMN promotion_decisions.decision IS 'Final promotion outcome only. Block is illegal here and must remain in gate_evaluations.';
COMMENT ON COLUMN promotion_decisions.current_head_ref IS 'Canonical head actually compared at promotion time. Accepted rows must prove this matched validated_head_ref.';
