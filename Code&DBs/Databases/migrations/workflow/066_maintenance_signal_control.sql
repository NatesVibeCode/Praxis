BEGIN;

ALTER TABLE maintenance_policies
    ADD COLUMN IF NOT EXISTS last_state_fingerprint text,
    ADD COLUMN IF NOT EXISTS last_start_fingerprint text,
    ADD COLUMN IF NOT EXISTS last_started_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_dirty_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_clean_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_workflow_run_id text,
    ADD COLUMN IF NOT EXISTS last_evaluation jsonb NOT NULL DEFAULT '{}'::jsonb;

UPDATE maintenance_policies
SET config = jsonb_build_object(
        'agent_slug', 'openai/gpt-5.4-mini',
        'workflow_name', 'maintenance_daily_review',
        'job_label', 'maintenance_review',
        'workspace_ref', 'dag-project',
        'runtime_profile_ref', 'dag-project',
        'task_type', 'ops_review',
        'start_if_clean', false,
        'start_max_attempts', 2,
        'repeat_start_seconds', 259200,
        'thresholds', jsonb_build_object(
            'review', jsonb_build_object(
                'pending_total', 10,
                'failed_total', 1,
                'oldest_pending_seconds', 3600,
                'review_queue_pending_total', 5,
                'entities_needing_reembed', 25
            ),
            'repair', jsonb_build_object(
                'pending_total', 50,
                'failed_total', 1,
                'oldest_pending_seconds', 21600,
                'review_queue_pending_total', 20,
                'entities_needing_reembed', 100
            )
        )
    ),
    updated_at = now()
WHERE policy_key = 'system.maintenance_review.daily';

INSERT INTO maintenance_policies (
    policy_key,
    subject_kind,
    intent_kind,
    enabled,
    priority,
    cadence_seconds,
    max_attempts,
    config,
    created_at,
    updated_at
)
VALUES (
    'system.maintenance_repair.auto',
    'system',
    'start_maintenance_repair',
    true,
    130,
    3600,
    3,
    jsonb_build_object(
        'agent_slug', 'openai/gpt-5.4',
        'workflow_name', 'maintenance_auto_repair',
        'job_label', 'maintenance_repair',
        'workspace_ref', 'dag-project',
        'runtime_profile_ref', 'dag-project',
        'task_type', 'ops_repair',
        'start_if_clean', false,
        'start_max_attempts', 2,
        'repeat_start_seconds', 43200,
        'thresholds', jsonb_build_object(
            'review', jsonb_build_object(
                'pending_total', 10,
                'failed_total', 1,
                'oldest_pending_seconds', 3600,
                'review_queue_pending_total', 5,
                'entities_needing_reembed', 25
            ),
            'repair', jsonb_build_object(
                'pending_total', 50,
                'failed_total', 1,
                'oldest_pending_seconds', 21600,
                'review_queue_pending_total', 20,
                'entities_needing_reembed', 100
            )
        )
    ),
    now(),
    now()
)
ON CONFLICT (policy_key) DO UPDATE
SET subject_kind = EXCLUDED.subject_kind,
    intent_kind = EXCLUDED.intent_kind,
    enabled = EXCLUDED.enabled,
    priority = EXCLUDED.priority,
    cadence_seconds = EXCLUDED.cadence_seconds,
    max_attempts = EXCLUDED.max_attempts,
    config = EXCLUDED.config,
    updated_at = now();

COMMIT;
