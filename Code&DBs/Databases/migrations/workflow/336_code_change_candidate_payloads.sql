-- Migration 336: code-change candidate payloads under sealed submissions.
--
-- Code-change candidates are not a parallel lifecycle authority. The sealed
-- submission row remains identity/review authority; this child table carries
-- only code-change-specific payload, routing, and materialization state.

BEGIN;

CREATE TABLE IF NOT EXISTS code_change_candidate_payloads (
    candidate_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id text NOT NULL UNIQUE,
    bug_id text NOT NULL,
    base_head_ref text NOT NULL,
    source_context_refs jsonb NOT NULL DEFAULT '{}'::jsonb,
    intended_files text[] NOT NULL DEFAULT '{}',
    proposal_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    patch_artifact_ref text,
    patch_sha256 text,
    verifier_ref text NOT NULL,
    verifier_inputs jsonb NOT NULL DEFAULT '{}'::jsonb,
    review_routing text NOT NULL CHECK (review_routing IN ('auto_apply', 'human_review', 'llm_review')),
    next_actor_kind text NOT NULL CHECK (next_actor_kind IN ('system', 'human', 'llm_reviewer', 'none')),
    materialization_status text NOT NULL DEFAULT 'pending' CHECK (
        materialization_status IN (
            'pending',
            'in_progress',
            'blocked_stale_head',
            'blocked_verifier_failed',
            'needs_revision',
            'materialized',
            'aborted',
            'superseded'
        )
    ),
    routing_decision_record jsonb NOT NULL DEFAULT '{}'::jsonb,
    anti_pattern_hits jsonb NOT NULL DEFAULT '[]'::jsonb,
    temp_verifier_run_id text,
    final_verifier_run_id text,
    gate_evaluation_id text,
    promotion_decision_id text,
    lease_owner text,
    lease_expires_at timestamptz,
    last_error jsonb NOT NULL DEFAULT '{}'::jsonb,
    superseded_by uuid REFERENCES code_change_candidate_payloads(candidate_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT code_change_candidate_payloads_submission_fkey
        FOREIGN KEY (submission_id)
        REFERENCES workflow_job_submissions (submission_id)
        ON DELETE CASCADE,
    CONSTRAINT code_change_candidate_payloads_bug_fkey
        FOREIGN KEY (bug_id)
        REFERENCES bugs (bug_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS code_change_candidate_payloads_bug_status_idx
    ON code_change_candidate_payloads (bug_id, materialization_status, created_at DESC);

CREATE INDEX IF NOT EXISTS code_change_candidate_payloads_next_actor_idx
    ON code_change_candidate_payloads (next_actor_kind, materialization_status, created_at DESC);

CREATE INDEX IF NOT EXISTS code_change_candidate_payloads_submission_idx
    ON code_change_candidate_payloads (submission_id);

CREATE INDEX IF NOT EXISTS code_change_candidate_payloads_patch_sha_idx
    ON code_change_candidate_payloads (patch_sha256)
    WHERE patch_sha256 IS NOT NULL;

CREATE OR REPLACE VIEW code_change_candidate_review_queue AS
WITH latest_review AS (
    SELECT DISTINCT ON (submission_id)
           submission_id,
           review_id,
           reviewer_job_label,
           reviewer_role,
           decision,
           summary,
           notes,
           evidence_refs,
           reviewed_at
      FROM workflow_job_submission_reviews
     ORDER BY submission_id, reviewed_at DESC, review_id DESC
)
SELECT
    c.candidate_id::text AS candidate_id,
    c.submission_id,
    c.bug_id,
    s.run_id,
    s.workflow_id,
    s.job_label,
    s.result_kind,
    s.summary,
    s.acceptance_status,
    c.review_routing,
    c.next_actor_kind,
    c.materialization_status,
    c.intended_files,
    c.verifier_ref,
    c.verifier_inputs,
    c.patch_artifact_ref,
    c.patch_sha256,
    c.anti_pattern_hits,
    latest_review.decision AS latest_review_decision,
    latest_review.summary AS latest_review_summary,
    latest_review.reviewed_at AS latest_reviewed_at,
    c.created_at,
    c.updated_at
FROM code_change_candidate_payloads c
JOIN workflow_job_submissions s
  ON s.submission_id = c.submission_id
LEFT JOIN latest_review
  ON latest_review.submission_id = c.submission_id
WHERE c.next_actor_kind IN ('human', 'llm_reviewer')
  AND c.materialization_status IN (
      'pending',
      'blocked_verifier_failed',
      'needs_revision'
  )
  AND COALESCE(s.acceptance_status, '') NOT IN (
      'accepted',
      'materialized',
      'rejected',
      'auto_apply_authorized'
  );

COMMENT ON TABLE code_change_candidate_payloads IS
    'Typed code-change candidate payloads under workflow_job_submissions. The submission/review spine remains lifecycle authority.';
COMMENT ON COLUMN code_change_candidate_payloads.materialization_status IS
    'Source-landing phase status only. Review rejection remains canonical in workflow_job_submission_reviews.';
COMMENT ON COLUMN code_change_candidate_payloads.routing_decision_record IS
    'Durable trace for system routing decisions, especially auto_apply where no review row exists.';
COMMENT ON VIEW code_change_candidate_review_queue IS
    'Review queue projection over sealed submissions plus code-change candidate payloads. Runtime workers must drain this view, not maintain a side table.';

COMMIT;
