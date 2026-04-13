-- Migration 080: workflow job submission authority
--
-- Sealed workflow submissions and append-only review rows.

BEGIN;

CREATE TABLE IF NOT EXISTS workflow_job_submissions (
    submission_id text PRIMARY KEY,
    run_id text NOT NULL,
    workflow_id text NOT NULL,
    job_label text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no >= 1),
    result_kind text NOT NULL,
    summary text NOT NULL,
    primary_paths jsonb NOT NULL DEFAULT '[]'::jsonb,
    tests_ran jsonb NOT NULL DEFAULT '[]'::jsonb,
    notes text,
    declared_operations jsonb NOT NULL DEFAULT '[]'::jsonb,
    changed_paths jsonb NOT NULL DEFAULT '[]'::jsonb,
    operation_set jsonb NOT NULL DEFAULT '[]'::jsonb,
    comparison_status text NOT NULL,
    comparison_report jsonb NOT NULL DEFAULT '{}'::jsonb,
    diff_artifact_ref text,
    artifact_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    verification_artifact_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    sealed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workflow_job_submissions_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS workflow_job_submissions_run_job_attempt_key
    ON workflow_job_submissions (run_id, job_label, attempt_no);

COMMENT ON TABLE workflow_job_submissions IS 'Canonical sealed workflow submission rows. These rows are the authority for worker output.';
COMMENT ON COLUMN workflow_job_submissions.declared_operations IS 'Declared write operations supplied at seal time for comparison against the measured sandbox state.';
COMMENT ON COLUMN workflow_job_submissions.operation_set IS 'Measured write operation set captured from the sandbox at seal time.';
COMMENT ON COLUMN workflow_job_submissions.comparison_report IS 'Structured comparison output between declared and measured submission state.';
COMMENT ON COLUMN workflow_job_submissions.artifact_refs IS 'Artifact refs produced by the sealed submission.';
COMMENT ON COLUMN workflow_job_submissions.verification_artifact_refs IS 'Artifact refs proving verification evidence for the sealed submission.';

CREATE TABLE IF NOT EXISTS workflow_job_submission_reviews (
    review_id text PRIMARY KEY,
    submission_id text NOT NULL,
    run_id text NOT NULL,
    workflow_id text NOT NULL,
    reviewer_job_label text NOT NULL,
    reviewer_role text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('approve', 'request_changes', 'reject')),
    summary text NOT NULL,
    notes text,
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    reviewed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workflow_job_submission_reviews_submission_id_fkey
        FOREIGN KEY (submission_id)
        REFERENCES workflow_job_submissions (submission_id)
        ON DELETE CASCADE,
    CONSTRAINT workflow_job_submission_reviews_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS workflow_job_submission_reviews_submission_reviewed_idx
    ON workflow_job_submission_reviews (submission_id, reviewed_at DESC, review_id DESC);

COMMENT ON TABLE workflow_job_submission_reviews IS 'Append-only review trail for sealed workflow submissions.';
COMMENT ON COLUMN workflow_job_submission_reviews.decision IS 'Canonical review outcome for the sealed submission.';
COMMENT ON COLUMN workflow_job_submission_reviews.evidence_refs IS 'Artifact or evidence refs attached to the review decision.';

COMMIT;
