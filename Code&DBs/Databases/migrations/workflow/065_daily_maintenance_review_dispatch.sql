BEGIN;

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
    'system.maintenance_review.daily',
    'system',
    'start_maintenance_review',
    true,
    120,
    86400,
    3,
    jsonb_build_object(
        'agent_slug', 'openai/gpt-5.4-mini',
        'workflow_name', 'maintenance_daily_review',
        'job_label', 'maintenance_review',
        'workspace_ref', 'dag-project',
        'runtime_profile_ref', 'dag-project',
        'task_type', 'ops_review',
        'start_if_clean', false,
        'start_max_attempts', 2
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
