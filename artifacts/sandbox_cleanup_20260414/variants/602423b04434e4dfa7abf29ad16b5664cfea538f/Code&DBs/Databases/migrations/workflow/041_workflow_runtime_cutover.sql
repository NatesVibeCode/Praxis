-- Migration 041: workflow runtime cutover
--
-- Align the live job scheduler and status surfaces on workflow_runs/workflow_jobs.
-- This migration:
--   1. adds missing workflow job scheduler columns used by the runtime
--   2. backfills legacy dispatch_runs into workflow_runs control-plane rows
--   3. repairs orphaned legacy runs with no jobs
--   4. repoints workflow_jobs.run_id to workflow_runs
--   5. removes dispatch_runs from the live table name so straggler reads fail fast

BEGIN;

ALTER TABLE workflow_jobs
    ADD COLUMN IF NOT EXISTS touch_keys jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS workflow_jobs_touch_keys_gin_idx
    ON workflow_jobs
    USING gin (touch_keys);

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

WITH legacy_dispatch AS (
    SELECT
        dj.run_id,
        ('workflow.legacy.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS workflow_id,
        ('legacy_request.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS request_id,
        ('workflow_definition.legacy.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS workflow_definition_id,
        ('legacy_admission.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS admission_decision_id,
        ('legacy:' || md5(COALESCE(dj.spec_snapshot::text, '') || ':' || dj.run_id)) AS definition_hash,
        COALESCE(
            (
                SELECT COUNT(*)
                FROM workflow_jobs wj
                WHERE wj.run_id = dj.run_id
            ),
            dj.total_jobs,
            0
        )::int AS actual_total_jobs,
        COALESCE(dj.created_at, now()) AS requested_at,
        COALESCE(dj.started_at, dj.created_at, now()) AS started_at,
        dj.finished_at,
        dj.spec_name,
        dj.spec_snapshot,
        dj.phase,
        dj.status,
        dj.outcome_goal,
        dj.output_dir,
        dj.terminal_reason,
        dj.parent_run_id,
        COALESCE(dj.trigger_depth, 0) AS trigger_depth,
        dj.idempotency_key
    FROM dispatch_runs dj
    WHERE NOT EXISTS (
        SELECT 1
        FROM workflow_runs wr
        WHERE wr.run_id = dj.run_id
    )
),
legacy_rows AS (
    SELECT
        run_id,
        workflow_id,
        request_id,
        workflow_definition_id,
        admission_decision_id,
        definition_hash,
        actual_total_jobs,
        requested_at,
        started_at,
        CASE
            WHEN status = 'running' AND actual_total_jobs = 0 THEN 'failed'
            WHEN status = 'running' THEN 'running'
            WHEN status = 'succeeded' THEN 'succeeded'
            WHEN status = 'failed' THEN 'failed'
            WHEN status = 'cancelled' THEN 'cancelled'
            ELSE 'failed'
        END AS mapped_state,
        CASE
            WHEN status = 'running' AND actual_total_jobs = 0 THEN COALESCE(finished_at, requested_at)
            WHEN status IN ('succeeded', 'failed', 'cancelled') THEN COALESCE(finished_at, started_at, requested_at)
            ELSE NULL
        END AS mapped_finished_at,
        CASE
            WHEN status = 'running' AND actual_total_jobs = 0 THEN 'legacy_orphaned_no_jobs'
            WHEN status = 'succeeded' THEN 'legacy_succeeded'
            WHEN status = 'failed' THEN COALESCE(NULLIF(terminal_reason, ''), 'legacy_failed')
            WHEN status = 'cancelled' THEN 'legacy_cancelled'
            ELSE NULL
        END AS terminal_reason_code,
        COALESCE(
            spec_snapshot,
            jsonb_build_object(
                'name', COALESCE(spec_name, run_id),
                'jobs', '[]'::jsonb
            )
        ) AS normalized_definition,
        jsonb_build_object(
            'name', COALESCE(spec_name, run_id),
            'workflow_id', workflow_id,
            'phase', COALESCE(phase, 'build'),
            'total_jobs', actual_total_jobs,
            'outcome_goal', COALESCE(outcome_goal, ''),
            'output_dir', COALESCE(output_dir, ''),
            'parent_run_id', parent_run_id,
            'trigger_depth', trigger_depth,
            'spec_snapshot', COALESCE(spec_snapshot, '{}'::jsonb),
            'legacy_dispatch_run', true
        ) AS request_envelope,
        COALESCE(NULLIF(idempotency_key, ''), run_id) AS run_idempotency_key
    FROM legacy_dispatch
)
INSERT INTO workflow_definitions (
    workflow_definition_id,
    workflow_id,
    schema_version,
    definition_version,
    definition_hash,
    status,
    request_envelope,
    normalized_definition,
    created_at
)
SELECT
    workflow_definition_id,
    workflow_id,
    1,
    1,
    definition_hash,
    'active',
    request_envelope,
    normalized_definition,
    requested_at
FROM legacy_rows
ON CONFLICT (workflow_definition_id) DO NOTHING;

WITH legacy_dispatch AS (
    SELECT
        dj.run_id,
        ('workflow.legacy.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS workflow_id,
        ('legacy_request.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS request_id,
        ('legacy_admission.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS admission_decision_id,
        COALESCE(dj.created_at, now()) AS decided_at
    FROM dispatch_runs dj
    WHERE NOT EXISTS (
        SELECT 1
        FROM workflow_runs wr
        WHERE wr.run_id = dj.run_id
    )
)
INSERT INTO admission_decisions (
    admission_decision_id,
    workflow_id,
    request_id,
    decision,
    reason_code,
    decided_at,
    decided_by,
    policy_snapshot_ref,
    validation_result_ref,
    authority_context_ref
)
SELECT
    admission_decision_id,
    workflow_id,
    request_id,
    'admit',
    'legacy_dispatch_backfill',
    decided_at,
    'migration.041',
    'migration.041',
    'migration.041',
    'migration.041'
FROM legacy_dispatch
ON CONFLICT (admission_decision_id) DO NOTHING;

WITH legacy_dispatch AS (
    SELECT
        dj.run_id,
        ('workflow.legacy.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS workflow_id,
        ('legacy_request.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS request_id,
        ('workflow_definition.legacy.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS workflow_definition_id,
        ('legacy_admission.' || regexp_replace(lower(dj.run_id), '[^a-z0-9._-]+', '.', 'g')) AS admission_decision_id,
        ('legacy:' || md5(COALESCE(dj.spec_snapshot::text, '') || ':' || dj.run_id)) AS definition_hash,
        COALESCE(
            (
                SELECT COUNT(*)
                FROM workflow_jobs wj
                WHERE wj.run_id = dj.run_id
            ),
            dj.total_jobs,
            0
        )::int AS actual_total_jobs,
        COALESCE(dj.created_at, now()) AS requested_at,
        COALESCE(dj.started_at, dj.created_at, now()) AS started_at,
        dj.finished_at,
        dj.spec_name,
        dj.spec_snapshot,
        dj.phase,
        dj.status,
        dj.outcome_goal,
        dj.output_dir,
        dj.terminal_reason,
        dj.parent_run_id,
        COALESCE(dj.trigger_depth, 0) AS trigger_depth,
        dj.idempotency_key
    FROM dispatch_runs dj
    WHERE NOT EXISTS (
        SELECT 1
        FROM workflow_runs wr
        WHERE wr.run_id = dj.run_id
    )
),
legacy_rows AS (
    SELECT
        run_id,
        workflow_id,
        request_id,
        workflow_definition_id,
        admission_decision_id,
        definition_hash,
        actual_total_jobs,
        requested_at,
        started_at,
        CASE
            WHEN status = 'running' AND actual_total_jobs = 0 THEN 'failed'
            WHEN status = 'running' THEN 'running'
            WHEN status = 'succeeded' THEN 'succeeded'
            WHEN status = 'failed' THEN 'failed'
            WHEN status = 'cancelled' THEN 'cancelled'
            ELSE 'failed'
        END AS mapped_state,
        CASE
            WHEN status = 'running' AND actual_total_jobs = 0 THEN COALESCE(finished_at, requested_at)
            WHEN status IN ('succeeded', 'failed', 'cancelled') THEN COALESCE(finished_at, started_at, requested_at)
            ELSE NULL
        END AS mapped_finished_at,
        CASE
            WHEN status = 'running' AND actual_total_jobs = 0 THEN 'legacy_orphaned_no_jobs'
            WHEN status = 'succeeded' THEN 'legacy_succeeded'
            WHEN status = 'failed' THEN COALESCE(NULLIF(terminal_reason, ''), 'legacy_failed')
            WHEN status = 'cancelled' THEN 'legacy_cancelled'
            ELSE NULL
        END AS terminal_reason_code,
        jsonb_build_object(
            'name', COALESCE(spec_name, run_id),
            'workflow_id', workflow_id,
            'phase', COALESCE(phase, 'build'),
            'total_jobs', actual_total_jobs,
            'outcome_goal', COALESCE(outcome_goal, ''),
            'output_dir', COALESCE(output_dir, ''),
            'parent_run_id', parent_run_id,
            'trigger_depth', trigger_depth,
            'spec_snapshot', COALESCE(spec_snapshot, '{}'::jsonb),
            'legacy_dispatch_run', true
        ) AS request_envelope,
        COALESCE(NULLIF(idempotency_key, ''), run_id) AS run_idempotency_key
    FROM legacy_dispatch
)
INSERT INTO workflow_runs (
    run_id,
    workflow_id,
    request_id,
    request_digest,
    authority_context_digest,
    workflow_definition_id,
    admitted_definition_hash,
    run_idempotency_key,
    schema_version,
    request_envelope,
    context_bundle_id,
    admission_decision_id,
    current_state,
    terminal_reason_code,
    requested_at,
    admitted_at,
    started_at,
    finished_at
)
SELECT
    run_id,
    workflow_id,
    request_id,
    substr(md5(run_id || ':request'), 1, 32),
    substr(md5(run_id || ':authority'), 1, 32),
    workflow_definition_id,
    definition_hash,
    run_idempotency_key,
    1,
    request_envelope,
    'legacy_context:' || run_id,
    admission_decision_id,
    mapped_state,
    terminal_reason_code,
    requested_at,
    requested_at,
    started_at,
    mapped_finished_at
FROM legacy_rows
ON CONFLICT (run_id) DO NOTHING;

WITH job_rollups AS (
    SELECT
        wj.run_id,
        COUNT(*) AS total_jobs,
        COUNT(*) FILTER (WHERE wj.status = 'succeeded') AS succeeded_jobs,
        COUNT(*) FILTER (WHERE wj.status = 'cancelled') AS cancelled_jobs,
        COUNT(*) FILTER (WHERE wj.status = 'dead_letter') AS dead_letter_jobs,
        COUNT(*) FILTER (WHERE wj.status IN ('failed', 'blocked')) AS failed_jobs,
        COUNT(*) FILTER (WHERE wj.status IN ('claimed', 'running')) AS active_jobs
    FROM workflow_jobs wj
    GROUP BY wj.run_id
)
UPDATE workflow_runs wr
SET current_state = CASE
        WHEN jr.dead_letter_jobs > 0 THEN 'dead_letter'
        WHEN jr.failed_jobs > 0 THEN 'failed'
        WHEN jr.cancelled_jobs = jr.total_jobs THEN 'cancelled'
        WHEN jr.succeeded_jobs = jr.total_jobs THEN 'succeeded'
        WHEN jr.active_jobs > 0 THEN 'running'
        ELSE 'queued'
    END,
    terminal_reason_code = CASE
        WHEN jr.dead_letter_jobs > 0 THEN 'workflow_job_dead_lettered'
        WHEN jr.failed_jobs > 0 THEN 'workflow_job_failed'
        WHEN jr.cancelled_jobs = jr.total_jobs THEN 'workflow_cancelled'
        WHEN jr.succeeded_jobs = jr.total_jobs THEN 'workflow_succeeded'
        ELSE NULL
    END,
    finished_at = CASE
        WHEN jr.dead_letter_jobs > 0 OR jr.failed_jobs > 0 OR jr.cancelled_jobs = jr.total_jobs OR jr.succeeded_jobs = jr.total_jobs
            THEN COALESCE(
                wr.finished_at,
                (
                    SELECT MAX(wj.finished_at)
                    FROM workflow_jobs wj
                    WHERE wj.run_id = wr.run_id
                ),
                wr.started_at,
                wr.requested_at
            )
        ELSE NULL
    END
FROM job_rollups jr
WHERE wr.run_id = jr.run_id;

UPDATE workflow_runs wr
SET current_state = 'failed',
    terminal_reason_code = 'legacy_orphaned_no_jobs',
    finished_at = COALESCE(wr.finished_at, wr.started_at, wr.requested_at)
WHERE wr.current_state IN ('queued', 'running')
  AND EXISTS (
      SELECT 1
      FROM dispatch_runs dj
      WHERE dj.run_id = wr.run_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_jobs wj
      WHERE wj.run_id = wr.run_id
  );

ALTER TABLE workflow_jobs
    DROP CONSTRAINT IF EXISTS workflow_jobs_run_id_fkey;

ALTER TABLE workflow_jobs
    ADD CONSTRAINT workflow_jobs_run_id_fkey
    FOREIGN KEY (run_id)
    REFERENCES workflow_runs(run_id)
    ON DELETE CASCADE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = 'dispatch_runs'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = 'dispatch_runs_legacy'
    ) THEN
        ALTER TABLE dispatch_runs RENAME TO dispatch_runs_legacy;
    END IF;
END
$$;

COMMIT;
