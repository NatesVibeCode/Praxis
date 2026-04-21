BEGIN;

CREATE TABLE IF NOT EXISTS device_enrollments (
    device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_ref TEXT NOT NULL,
    credential_id TEXT NOT NULL UNIQUE,
    credential_public_key BYTEA NOT NULL,
    credential_sign_count BIGINT NOT NULL DEFAULT 0,
    device_label TEXT NOT NULL,
    aaguid TEXT,
    transports JSONB NOT NULL DEFAULT '[]'::jsonb,
    enrolled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_asserted_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revoked_by TEXT,
    revoke_reason TEXT,
    CONSTRAINT device_enrollments_principal_nonblank_check
        CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT device_enrollments_credential_id_nonblank_check
        CHECK (btrim(credential_id) <> ''),
    CONSTRAINT device_enrollments_credential_sign_count_nonnegative_check
        CHECK (credential_sign_count >= 0),
    CONSTRAINT device_enrollments_label_nonblank_check
        CHECK (btrim(device_label) <> ''),
    CONSTRAINT device_enrollments_transports_array_check
        CHECK (jsonb_typeof(transports) = 'array'),
    CONSTRAINT device_enrollments_revocation_detail_check
        CHECK (
            revoked_at IS NULL
            OR (
                revoked_by IS NOT NULL
                AND btrim(revoked_by) <> ''
                AND revoke_reason IS NOT NULL
                AND btrim(revoke_reason) <> ''
            )
        )
);

CREATE INDEX IF NOT EXISTS device_enrollments_principal_active_idx
    ON device_enrollments (principal_ref, enrolled_at DESC)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS device_enrollments_credential_id_idx
    ON device_enrollments (credential_id);

COMMENT ON TABLE device_enrollments IS
    'WebAuthn device enrollment ledger binding principals to passkey credentials for mobile control access.';

CREATE TABLE IF NOT EXISTS capability_grants (
    grant_id TEXT PRIMARY KEY,
    principal_ref TEXT NOT NULL,
    device_id UUID REFERENCES device_enrollments (device_id) ON DELETE RESTRICT,
    grant_kind TEXT NOT NULL,
    capability_scope JSONB NOT NULL,
    max_risk_level TEXT NOT NULL,
    plan_envelope_hash TEXT,
    approval_request_id UUID,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    revoked_by TEXT,
    revoke_reason TEXT,
    decision_ref TEXT,
    CONSTRAINT capability_grants_grant_id_nonblank_check
        CHECK (btrim(grant_id) <> ''),
    CONSTRAINT capability_grants_principal_nonblank_check
        CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT capability_grants_kind_valid_check
        CHECK (grant_kind IN ('device_session', 'plan', 'command', 'blast_radius')),
    CONSTRAINT capability_grants_scope_object_check
        CHECK (jsonb_typeof(capability_scope) = 'object'),
    CONSTRAINT capability_grants_max_risk_level_valid_check
        CHECK (max_risk_level IN ('low', 'medium', 'high')),
    CONSTRAINT capability_grants_plan_envelope_hash_nonblank_check
        CHECK (plan_envelope_hash IS NULL OR btrim(plan_envelope_hash) <> ''),
    CONSTRAINT capability_grants_expiry_after_issue_check
        CHECK (expires_at > issued_at),
    CONSTRAINT capability_grants_revocation_detail_check
        CHECK (
            revoked_at IS NULL
            OR (
                revoked_by IS NOT NULL
                AND btrim(revoked_by) <> ''
                AND revoke_reason IS NOT NULL
                AND btrim(revoke_reason) <> ''
            )
        )
);

ALTER TABLE capability_grants
    ADD COLUMN IF NOT EXISTS grant_id TEXT,
    ADD COLUMN IF NOT EXISTS principal_ref TEXT,
    ADD COLUMN IF NOT EXISTS device_id UUID,
    ADD COLUMN IF NOT EXISTS grant_kind TEXT,
    ADD COLUMN IF NOT EXISTS capability_scope JSONB,
    ADD COLUMN IF NOT EXISTS max_risk_level TEXT,
    ADD COLUMN IF NOT EXISTS plan_envelope_hash TEXT,
    ADD COLUMN IF NOT EXISTS approval_request_id UUID,
    ADD COLUMN IF NOT EXISTS issued_at TIMESTAMPTZ DEFAULT now(),
    ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS revoked_by TEXT,
    ADD COLUMN IF NOT EXISTS revoke_reason TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'capability_grants_grant_id_key'
          AND conrelid = 'capability_grants'::regclass
    ) THEN
        ALTER TABLE capability_grants
            ADD CONSTRAINT capability_grants_grant_id_key
            UNIQUE (grant_id);
    END IF;
END $$;

ALTER TABLE capability_grants
    DROP CONSTRAINT IF EXISTS capability_grants_grant_id_nonblank_check,
    DROP CONSTRAINT IF EXISTS capability_grants_principal_nonblank_check,
    DROP CONSTRAINT IF EXISTS capability_grants_kind_valid_check,
    DROP CONSTRAINT IF EXISTS capability_grants_scope_object_check,
    DROP CONSTRAINT IF EXISTS capability_grants_max_risk_level_valid_check,
    DROP CONSTRAINT IF EXISTS capability_grants_plan_envelope_hash_nonblank_check,
    DROP CONSTRAINT IF EXISTS capability_grants_expiry_after_issue_check,
    DROP CONSTRAINT IF EXISTS capability_grants_revocation_detail_check;

ALTER TABLE capability_grants
    ADD CONSTRAINT capability_grants_grant_id_nonblank_check
        CHECK (grant_id IS NULL OR btrim(grant_id) <> '') NOT VALID,
    ADD CONSTRAINT capability_grants_principal_nonblank_check
        CHECK (principal_ref IS NULL OR btrim(principal_ref) <> '') NOT VALID,
    ADD CONSTRAINT capability_grants_kind_valid_check
        CHECK (grant_kind IS NULL OR grant_kind IN ('device_session', 'plan', 'command', 'blast_radius')) NOT VALID,
    ADD CONSTRAINT capability_grants_scope_object_check
        CHECK (capability_scope IS NULL OR jsonb_typeof(capability_scope) = 'object') NOT VALID,
    ADD CONSTRAINT capability_grants_max_risk_level_valid_check
        CHECK (max_risk_level IS NULL OR max_risk_level IN ('low', 'medium', 'high')) NOT VALID,
    ADD CONSTRAINT capability_grants_plan_envelope_hash_nonblank_check
        CHECK (plan_envelope_hash IS NULL OR btrim(plan_envelope_hash) <> '') NOT VALID,
    ADD CONSTRAINT capability_grants_expiry_after_issue_check
        CHECK (issued_at IS NULL OR expires_at IS NULL OR expires_at > issued_at) NOT VALID,
    ADD CONSTRAINT capability_grants_revocation_detail_check
        CHECK (
            revoked_at IS NULL
            OR (
                revoked_by IS NOT NULL
                AND btrim(revoked_by) <> ''
                AND revoke_reason IS NOT NULL
                AND btrim(revoke_reason) <> ''
            )
        ) NOT VALID;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'capability_grants_device_id_fkey'
          AND conrelid = 'capability_grants'::regclass
    ) THEN
        ALTER TABLE capability_grants
            ADD CONSTRAINT capability_grants_device_id_fkey
            FOREIGN KEY (device_id)
            REFERENCES device_enrollments (device_id)
            ON DELETE RESTRICT
            NOT VALID;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS capability_grants_principal_active_idx
    ON capability_grants (principal_ref, expires_at DESC)
    WHERE principal_ref IS NOT NULL AND revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS capability_grants_plan_envelope_hash_active_idx
    ON capability_grants (plan_envelope_hash)
    WHERE plan_envelope_hash IS NOT NULL AND revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS capability_grants_device_active_idx
    ON capability_grants (device_id, expires_at DESC)
    WHERE device_id IS NOT NULL AND revoked_at IS NULL;

COMMENT ON TABLE capability_grants IS
    'Durable capability grant ledger used to cover mobile control actions without cascading grant history.';

CREATE TABLE IF NOT EXISTS approval_requests (
    request_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_status TEXT NOT NULL DEFAULT 'pending',
    principal_ref TEXT NOT NULL,
    device_id UUID REFERENCES device_enrollments (device_id) ON DELETE RESTRICT,
    requested_by_kind TEXT NOT NULL,
    requested_by_ref TEXT NOT NULL,
    command_type TEXT NOT NULL,
    control_command_id TEXT,
    plan_envelope_hash TEXT NOT NULL,
    plan_summary TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    blast_radius JSONB NOT NULL DEFAULT '{}'::jsonb,
    grant_ref TEXT REFERENCES capability_grants (grant_id) ON DELETE RESTRICT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    ratified_at TIMESTAMPTZ,
    ratified_by TEXT,
    revoked_at TIMESTAMPTZ,
    revoked_by TEXT,
    revoke_reason TEXT,
    CONSTRAINT approval_requests_status_valid_check
        CHECK (request_status IN ('pending', 'ratified', 'revoked', 'expired', 'superseded')),
    CONSTRAINT approval_requests_principal_nonblank_check
        CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT approval_requests_requested_by_kind_nonblank_check
        CHECK (btrim(requested_by_kind) <> ''),
    CONSTRAINT approval_requests_requested_by_ref_nonblank_check
        CHECK (btrim(requested_by_ref) <> ''),
    CONSTRAINT approval_requests_command_type_nonblank_check
        CHECK (btrim(command_type) <> ''),
    CONSTRAINT approval_requests_plan_hash_nonblank_check
        CHECK (btrim(plan_envelope_hash) <> ''),
    CONSTRAINT approval_requests_plan_summary_nonblank_check
        CHECK (btrim(plan_summary) <> ''),
    CONSTRAINT approval_requests_risk_level_valid_check
        CHECK (risk_level IN ('low', 'medium', 'high')),
    CONSTRAINT approval_requests_blast_radius_object_check
        CHECK (jsonb_typeof(blast_radius) = 'object'),
    CONSTRAINT approval_requests_expiry_after_request_check
        CHECK (expires_at > requested_at),
    CONSTRAINT approval_requests_status_timestamps_check
        CHECK (
            (request_status = 'ratified' AND ratified_at IS NOT NULL)
            OR (request_status = 'revoked' AND revoked_at IS NOT NULL)
            OR (request_status NOT IN ('ratified', 'revoked'))
        ),
    CONSTRAINT approval_requests_revocation_detail_check
        CHECK (
            revoked_at IS NULL
            OR (
                revoked_by IS NOT NULL
                AND btrim(revoked_by) <> ''
                AND revoke_reason IS NOT NULL
                AND btrim(revoke_reason) <> ''
            )
        )
);

CREATE INDEX IF NOT EXISTS approval_requests_pending_idx
    ON approval_requests (requested_at DESC)
    WHERE request_status = 'pending';

CREATE INDEX IF NOT EXISTS approval_requests_plan_envelope_hash_idx
    ON approval_requests (plan_envelope_hash, requested_at DESC);

CREATE INDEX IF NOT EXISTS approval_requests_principal_status_idx
    ON approval_requests (principal_ref, request_status, requested_at DESC);

CREATE INDEX IF NOT EXISTS approval_requests_grant_ref_idx
    ON approval_requests (grant_ref)
    WHERE grant_ref IS NOT NULL;

COMMENT ON TABLE approval_requests IS
    'Mobile approval lifecycle requests that can ratify or revoke capability grants for stamped plan envelopes.';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'capability_grants_approval_request_id_fkey'
          AND conrelid = 'capability_grants'::regclass
    ) THEN
        ALTER TABLE capability_grants
            ADD CONSTRAINT capability_grants_approval_request_id_fkey
            FOREIGN KEY (approval_request_id)
            REFERENCES approval_requests (request_id)
            ON DELETE RESTRICT
            NOT VALID;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS capability_grants_approval_request_id_idx
    ON capability_grants (approval_request_id)
    WHERE approval_request_id IS NOT NULL;

COMMIT;
