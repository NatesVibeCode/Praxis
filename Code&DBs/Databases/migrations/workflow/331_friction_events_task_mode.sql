BEGIN;

ALTER TABLE friction_events
    ADD COLUMN IF NOT EXISTS task_mode TEXT;

CREATE INDEX IF NOT EXISTS idx_friction_events_task_mode
    ON friction_events (task_mode)
    WHERE task_mode IS NOT NULL;

COMMENT ON COLUMN friction_events.task_mode IS
    'Active task mode at the moment the friction was recorded (chat / build / release / incident / ...). Nullable: pre-existing rows and uninstrumented call sites stay NULL. Used to slice bounce-rate and pattern stats by mode so JIT-narrowing experiments are measurable.';

COMMIT;
