-- Migration 334: add `partial_success` to workflow_runs.current_state CHECK.
--
-- Operator direction (2026-04-29, nate): the cascade-cancel pattern from
-- _recompute_workflow_run_state firing `failed` on the FIRST failed peer
-- killed independent parallel-fanout packets via run_cancelled at claim
-- time. The straight fix (option A) was to delay the `failed` flip until
-- active=0 and pending=0, but that loses the useful signal of "wave had
-- some fixes, some misses." Operator chose option B: introduce a
-- `partial_success` terminal state for runs that finish with mixed
-- outcomes (>=1 succeeded AND >=1 failed/dead_letter).
--
-- After this migration:
--   * fully-clean run → state='succeeded' (unchanged)
--   * fully-failed run → state='failed' (unchanged)
--   * run with both succeeded jobs AND failed/dead_letter jobs → state='partial_success'
--
-- The `partial_success` state IS terminal — no more state transitions after
-- it lands. Operator-facing surfaces should treat it as a successful run
-- with a non-zero `failed`/`dead_letter` count rather than as a failure.
-- Retry via _status.py uses the same path; partial_success is added to
-- the retryable set so a wave can re-attempt only its failed packets.

BEGIN;

ALTER TABLE workflow_runs
    DROP CONSTRAINT IF EXISTS workflow_runs_current_state_check;

ALTER TABLE workflow_runs
    ADD CONSTRAINT workflow_runs_current_state_check
    CHECK (
        current_state IN (
            'claim_received',
            'claim_validating',
            'claim_blocked',
            'claim_rejected',
            'claim_accepted',
            'queued',
            'running',
            'succeeded',
            'partial_success',
            'failed',
            'dead_letter',
            'lease_requested',
            'lease_blocked',
            'lease_active',
            'lease_expired',
            'proposal_submitted',
            'proposal_invalid',
            'gate_evaluating',
            'gate_blocked',
            'promotion_decision_recorded',
            'promoted',
            'promotion_rejected',
            'promotion_failed',
            'cancelled'
        )
    );

COMMIT;
