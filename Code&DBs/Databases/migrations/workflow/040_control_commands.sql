BEGIN;

CREATE TABLE IF NOT EXISTS control_commands (
    command_id TEXT PRIMARY KEY,
    command_type TEXT NOT NULL,
    command_status TEXT NOT NULL DEFAULT 'requested',
    requested_by_kind TEXT NOT NULL,
    requested_by_ref TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at TIMESTAMPTZ,
    approved_by TEXT,
    idempotency_key TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_ref TEXT,
    error_code TEXT,
    error_detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT control_commands_command_type_check
        CHECK (command_type IN ('dispatch.submit', 'dispatch.retry', 'dispatch.cancel', 'sync.repair')),
    CONSTRAINT control_commands_status_check
        CHECK (command_status IN ('requested', 'accepted', 'rejected', 'running', 'succeeded', 'failed')),
    CONSTRAINT control_commands_risk_level_check
        CHECK (risk_level IN ('low', 'medium', 'high')),
    CONSTRAINT control_commands_requested_before_approved_check
        CHECK (approved_at IS NULL OR requested_at <= approved_at),
    CONSTRAINT uq_control_commands_idempotency_key UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_control_commands_status_requested_at
    ON control_commands (command_status, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_control_commands_type_requested_at
    ON control_commands (command_type, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_control_commands_result_ref
    ON control_commands (result_ref)
    WHERE result_ref IS NOT NULL;

CREATE OR REPLACE FUNCTION touch_control_commands_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_control_commands_updated_at ON control_commands;
CREATE TRIGGER trg_control_commands_updated_at
    BEFORE UPDATE ON control_commands
    FOR EACH ROW EXECUTE FUNCTION touch_control_commands_updated_at();

COMMIT;
