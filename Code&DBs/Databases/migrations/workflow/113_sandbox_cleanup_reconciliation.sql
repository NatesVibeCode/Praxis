BEGIN;

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS cleanup_status text;

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS cleanup_requested_at timestamptz;

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS cleanup_attempted_at timestamptz;

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS cleanup_completed_at timestamptz;

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS cleanup_attempt_count integer NOT NULL DEFAULT 0;

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS cleanup_last_error text;

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS cleanup_outcome jsonb NOT NULL DEFAULT '{}'::jsonb;

UPDATE sandbox_sessions
SET cleanup_status = CASE
        WHEN cleanup_status IS NOT NULL THEN cleanup_status
        WHEN cleanup_completed_at IS NOT NULL THEN 'completed'
        WHEN cleanup_last_error IS NOT NULL THEN 'failed'
        ELSE NULL
    END
WHERE cleanup_status IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_sandbox_sessions_cleanup_status'
          AND conrelid = 'sandbox_sessions'::regclass
    ) THEN
        ALTER TABLE sandbox_sessions
            ADD CONSTRAINT ck_sandbox_sessions_cleanup_status
            CHECK (
                cleanup_status IS NULL
                OR cleanup_status IN (
                    'pending',
                    'in_progress',
                    'completed',
                    'failed',
                    'skipped'
                )
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS sandbox_sessions_cleanup_due_idx
    ON sandbox_sessions (cleanup_status, cleanup_attempted_at, closed_at, expires_at)
    WHERE cleanup_completed_at IS NULL;

COMMENT ON COLUMN sandbox_sessions.cleanup_status IS 'DB-backed sandbox cleanup state. Null means cleanup has never been claimed.';
COMMENT ON COLUMN sandbox_sessions.cleanup_requested_at IS 'First time DB-native reconciliation claimed this session for cleanup.';
COMMENT ON COLUMN sandbox_sessions.cleanup_attempted_at IS 'Most recent DB-native cleanup claim timestamp.';
COMMENT ON COLUMN sandbox_sessions.cleanup_completed_at IS 'When cleanup reached a terminal completed/skipped state.';
COMMENT ON COLUMN sandbox_sessions.cleanup_attempt_count IS 'How many DB-native cleanup attempts have claimed this session.';
COMMENT ON COLUMN sandbox_sessions.cleanup_last_error IS 'Last reconciliation error when cleanup_status = failed.';
COMMENT ON COLUMN sandbox_sessions.cleanup_outcome IS 'Latest structured cleanup receipt for this sandbox session.';

DO $$
BEGIN
    IF to_regclass('public.maintenance_policies') IS NULL THEN
        RETURN;
    END IF;

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
        'sandbox_session.cleanup_reconcile',
        'sandbox_session',
        'reconcile_sandbox_session_cleanup',
        true,
        70,
        600,
        5,
        '{"batch_limit":25,"claim_timeout_seconds":900}'::jsonb,
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
END
$$;

COMMIT;
