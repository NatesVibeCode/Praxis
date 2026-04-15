-- Migration 120: allow proposal_request review decisions for workflow builds
--
-- Proposal requests are review-state authority, not synthetic handler sugar. The
-- database contract must accept them anywhere the runtime records explicit build
-- review decisions.

BEGIN;

ALTER TABLE workflow_build_review_decisions
    DROP CONSTRAINT IF EXISTS workflow_build_review_decisions_decision_check;

ALTER TABLE workflow_build_review_decisions
    DROP CONSTRAINT IF EXISTS workflow_build_review_decisions_decision_check_v2;

ALTER TABLE workflow_build_review_decisions
    ADD CONSTRAINT workflow_build_review_decisions_decision_check_v2
    CHECK (
        decision IN (
            'approve',
            'reject',
            'defer',
            'widen',
            'revoke',
            'proposal_request'
        )
    );

COMMIT;
