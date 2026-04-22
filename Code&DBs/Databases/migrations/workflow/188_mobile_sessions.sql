BEGIN;

CREATE TABLE IF NOT EXISTS mobile_bootstrap_tokens (
    token_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_ref TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    consumed_by_session_id UUID,
    revoked_at TIMESTAMPTZ,
    revoked_by TEXT,
    revoke_reason TEXT,
    CONSTRAINT mobile_bootstrap_tokens_principal_nonblank_check
        CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT mobile_bootstrap_tokens_hash_nonblank_check
        CHECK (btrim(token_hash) <> ''),
    CONSTRAINT mobile_bootstrap_tokens_expiry_after_issue_check
        CHECK (expires_at > issued_at),
    CONSTRAINT mobile_bootstrap_tokens_consumed_after_issue_check
        CHECK (consumed_at IS NULL OR consumed_at >= issued_at),
    CONSTRAINT mobile_bootstrap_tokens_revocation_detail_check
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

CREATE INDEX IF NOT EXISTS mobile_bootstrap_tokens_principal_active_idx
    ON mobile_bootstrap_tokens (principal_ref, expires_at DESC)
    WHERE consumed_at IS NULL AND revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS mobile_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_ref TEXT NOT NULL,
    device_id UUID NOT NULL REFERENCES device_enrollments (device_id) ON DELETE RESTRICT,
    session_token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_step_up_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    revoked_by TEXT,
    revoke_reason TEXT,
    budget_limit INTEGER NOT NULL DEFAULT 25,
    budget_used INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT mobile_sessions_principal_nonblank_check
        CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT mobile_sessions_token_hash_nonblank_check
        CHECK (btrim(session_token_hash) <> ''),
    CONSTRAINT mobile_sessions_expiry_after_create_check
        CHECK (expires_at > created_at),
    CONSTRAINT mobile_sessions_budget_nonnegative_check
        CHECK (budget_limit >= 0 AND budget_used >= 0),
    CONSTRAINT mobile_sessions_budget_ceiling_check
        CHECK (budget_used <= budget_limit),
    CONSTRAINT mobile_sessions_revocation_detail_check
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

CREATE INDEX IF NOT EXISTS mobile_sessions_principal_active_idx
    ON mobile_sessions (principal_ref, expires_at DESC)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS mobile_sessions_device_active_idx
    ON mobile_sessions (device_id, expires_at DESC)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS mobile_session_budget_events (
    budget_event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES mobile_sessions (session_id) ON DELETE RESTRICT,
    principal_ref TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    units INTEGER NOT NULL,
    budget_used_after INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT mobile_session_budget_events_principal_nonblank_check
        CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT mobile_session_budget_events_kind_valid_check
        CHECK (event_kind IN ('spend', 'refund', 'reset')),
    CONSTRAINT mobile_session_budget_events_units_nonnegative_check
        CHECK (units >= 0),
    CONSTRAINT mobile_session_budget_events_used_nonnegative_check
        CHECK (budget_used_after >= 0),
    CONSTRAINT mobile_session_budget_events_reason_nonblank_check
        CHECK (btrim(reason_code) <> '')
);

CREATE INDEX IF NOT EXISTS mobile_session_budget_events_session_idx
    ON mobile_session_budget_events (session_id, recorded_at DESC);

COMMENT ON TABLE mobile_bootstrap_tokens IS
    'Hashed short-lived bootstrap tokens for mobile device/session enrollment.';
COMMENT ON TABLE mobile_sessions IS
    'Mobile session ledger with step-up timestamp, revocation, and atomic spend budget.';
COMMENT ON TABLE mobile_session_budget_events IS
    'Append-only budget spend/refund/reset ledger for mobile sessions.';

COMMIT;
