-- Migration 114: workflow build review decisions
--
-- DB-native approval authority for build-time binding and import review decisions.

BEGIN;

CREATE TABLE IF NOT EXISTS workflow_build_review_decisions (
    review_decision_id text PRIMARY KEY,
    workflow_id text NOT NULL,
    definition_revision text NOT NULL,
    target_kind text NOT NULL,
    target_ref text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('approve', 'reject', 'defer', 'widen', 'revoke')),
    actor_type text NOT NULL CHECK (actor_type IN ('model', 'human', 'policy')),
    actor_ref text NOT NULL,
    approval_mode text NOT NULL,
    rationale text,
    source_subpath text,
    candidate_ref text,
    candidate_payload jsonb,
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workflow_build_review_decisions_workflow_id_fkey
        FOREIGN KEY (workflow_id)
        REFERENCES public.workflows (id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_workflow_build_review_decisions_workflow_revision_target
    ON workflow_build_review_decisions (
        workflow_id,
        definition_revision,
        target_kind,
        target_ref,
        decided_at DESC,
        created_at DESC
    );

CREATE INDEX IF NOT EXISTS idx_workflow_build_review_decisions_workflow_decided_at
    ON workflow_build_review_decisions (
        workflow_id,
        decided_at DESC,
        created_at DESC
    );

COMMENT ON TABLE workflow_build_review_decisions IS 'Append-only build-review authority rows for explicit binding, import, bundle, and workflow-shape decisions.';
COMMENT ON COLUMN workflow_build_review_decisions.definition_revision IS 'Definition revision the review decision applies to. Decisions do not automatically carry across revisions.';
COMMENT ON COLUMN workflow_build_review_decisions.candidate_payload IS 'Canonical candidate payload approved or rejected by the reviewer when the decision targeted a surfaced candidate.';

COMMIT;
