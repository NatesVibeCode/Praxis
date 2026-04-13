-- Migration 023: Durable dispatch notifications via outbox
--
-- Adds pg_notify to the receipt outbox trigger so LISTEN consumers get
-- real-time push. Also creates a durable notification table so consumers
-- that weren't listening (different session, crashed, restarted) can
-- catch up by reading undelivered rows.
--
-- This breaks session dependence: the notification exists in Postgres
-- regardless of who is listening. Any consumer reads it when ready.

-- Durable notification queue — one row per dispatch job completion
CREATE TABLE IF NOT EXISTS dispatch_notifications (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    job_label       TEXT NOT NULL,
    spec_name       TEXT NOT NULL,
    agent_slug      TEXT NOT NULL,
    status          TEXT NOT NULL,
    failure_code    TEXT DEFAULT '',
    duration_seconds FLOAT DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered       BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS dispatch_notifications_undelivered_idx
    ON dispatch_notifications (delivered, created_at)
    WHERE delivered = false;

-- Update the receipt outbox trigger to also write a notification row
-- and fire pg_notify for real-time LISTEN consumers
CREATE OR REPLACE FUNCTION workflow_outbox_capture_receipt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Original outbox insert (unchanged)
    INSERT INTO workflow_outbox (
        authority_table,
        authority_id,
        envelope_kind,
        workflow_id,
        run_id,
        request_id,
        evidence_seq,
        transition_seq,
        authority_recorded_at,
        envelope
    ) VALUES (
        'receipts',
        NEW.receipt_id,
        'receipt',
        NEW.workflow_id,
        NEW.run_id,
        NEW.request_id,
        NEW.evidence_seq,
        (NEW.inputs->>'transition_seq')::bigint,
        NEW.finished_at,
        to_jsonb(NEW)
    )
    ON CONFLICT (authority_table, authority_id) DO NOTHING;

    -- Durable notification row (only for dispatch_job receipts)
    IF NEW.receipt_type = 'dispatch_job' THEN
        INSERT INTO dispatch_notifications (
            run_id, job_label, spec_name, agent_slug, status,
            failure_code, duration_seconds
        ) VALUES (
            NEW.run_id,
            NEW.inputs->>'job_label',
            NEW.inputs->>'spec_name',
            NEW.inputs->>'agent_slug',
            NEW.status,
            COALESCE(NEW.failure_code, ''),
            EXTRACT(EPOCH FROM (NEW.finished_at - NEW.started_at))
        );

        -- Real-time push for LISTEN consumers (non-blocking, fire-and-forget)
        PERFORM pg_notify('dispatch_complete', json_build_object(
            'job_label', NEW.inputs->>'job_label',
            'spec_name', NEW.inputs->>'spec_name',
            'agent_slug', NEW.inputs->>'agent_slug',
            'status', NEW.status,
            'failure_code', COALESCE(NEW.failure_code, ''),
            'run_id', NEW.run_id
        )::text);
    END IF;

    RETURN NEW;
END;
$$;
