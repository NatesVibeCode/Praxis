BEGIN;

CREATE TABLE IF NOT EXISTS webauthn_challenges (
    challenge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    challenge_kind TEXT NOT NULL,
    challenge_token TEXT NOT NULL UNIQUE,
    principal_ref TEXT,
    device_id UUID REFERENCES device_enrollments (device_id) ON DELETE RESTRICT,
    rp_id TEXT NOT NULL,
    user_handle TEXT,
    public_key_options JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    consumed_by_session_id UUID,
    CONSTRAINT webauthn_challenges_kind_valid_check
        CHECK (challenge_kind IN ('register', 'assert')),
    CONSTRAINT webauthn_challenges_token_nonblank_check
        CHECK (btrim(challenge_token) <> ''),
    CONSTRAINT webauthn_challenges_principal_nonblank_check
        CHECK (principal_ref IS NULL OR btrim(principal_ref) <> ''),
    CONSTRAINT webauthn_challenges_rp_id_nonblank_check
        CHECK (btrim(rp_id) <> ''),
    CONSTRAINT webauthn_challenges_user_handle_nonblank_check
        CHECK (user_handle IS NULL OR btrim(user_handle) <> ''),
    CONSTRAINT webauthn_challenges_public_key_options_object_check
        CHECK (jsonb_typeof(public_key_options) = 'object'),
    CONSTRAINT webauthn_challenges_expiry_after_create_check
        CHECK (expires_at > created_at),
    CONSTRAINT webauthn_challenges_consumed_after_create_check
        CHECK (consumed_at IS NULL OR consumed_at >= created_at)
);

CREATE INDEX IF NOT EXISTS webauthn_challenges_expiry_idx
    ON webauthn_challenges (expires_at)
    WHERE consumed_at IS NULL;

CREATE INDEX IF NOT EXISTS webauthn_challenges_kind_expiry_idx
    ON webauthn_challenges (challenge_kind, expires_at)
    WHERE consumed_at IS NULL;

CREATE INDEX IF NOT EXISTS webauthn_challenges_device_active_idx
    ON webauthn_challenges (device_id, expires_at)
    WHERE device_id IS NOT NULL AND consumed_at IS NULL;

COMMENT ON TABLE webauthn_challenges IS
    'Short-lived WebAuthn registration and assertion challenges for mobile device enrollment and step-up authentication.';

COMMIT;
