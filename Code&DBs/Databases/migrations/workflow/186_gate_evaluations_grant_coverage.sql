BEGIN;

ALTER TABLE gate_evaluations
    DROP CONSTRAINT IF EXISTS gate_evaluations_grant_ref_fkey;

ALTER TABLE gate_evaluations
    ADD COLUMN IF NOT EXISTS grant_ref TEXT NULL REFERENCES capability_grants (grant_id) ON DELETE RESTRICT;

ALTER TABLE gate_evaluations
    ADD COLUMN IF NOT EXISTS plan_envelope_hash TEXT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'gate_evaluations_grant_ref_fkey'
          AND conrelid = 'gate_evaluations'::regclass
    ) THEN
        ALTER TABLE gate_evaluations
            ADD CONSTRAINT gate_evaluations_grant_ref_fkey
            FOREIGN KEY (grant_ref)
            REFERENCES capability_grants (grant_id)
            ON DELETE RESTRICT
            NOT VALID;
    END IF;
END $$;

ALTER TABLE gate_evaluations
    DROP CONSTRAINT IF EXISTS gate_evaluations_plan_envelope_hash_nonblank_check;

ALTER TABLE gate_evaluations
    ADD CONSTRAINT gate_evaluations_plan_envelope_hash_nonblank_check CHECK (
        plan_envelope_hash IS NULL OR btrim(plan_envelope_hash) <> ''
    ) NOT VALID;

CREATE INDEX IF NOT EXISTS gate_evaluations_grant_ref_decided_at_idx
    ON gate_evaluations (grant_ref, decided_at DESC)
    WHERE grant_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS gate_evaluations_plan_envelope_hash_decided_at_idx
    ON gate_evaluations (plan_envelope_hash, decided_at DESC)
    WHERE plan_envelope_hash IS NOT NULL;

COMMENT ON TABLE gate_evaluations IS
    'Canonical gate evaluation rows for sealed proposals, including capability grant coverage when present.';

COMMENT ON COLUMN gate_evaluations.grant_ref IS
    'Capability grant id that covered this gate evaluation, when one was active.';

COMMENT ON COLUMN gate_evaluations.plan_envelope_hash IS
    'Canonical hash of the stamped control plan envelope evaluated by policy.';

COMMIT;
