BEGIN;

ALTER TABLE provider_policies
    ADD COLUMN IF NOT EXISTS allowed_provider_refs JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE provider_policies
    ADD COLUMN IF NOT EXISTS preferred_provider_ref TEXT;

ALTER TABLE provider_policies
    DROP CONSTRAINT IF EXISTS provider_policies_allowed_provider_refs_array_check;

ALTER TABLE provider_policies
    ADD CONSTRAINT provider_policies_allowed_provider_refs_array_check
    CHECK (jsonb_typeof(allowed_provider_refs) = 'array');

COMMENT ON COLUMN provider_policies.allowed_provider_refs IS
    'Canonical provider_ref allowlist admitted by this policy; empty means legacy scalar provider_name fallback.';

COMMENT ON COLUMN provider_policies.preferred_provider_ref IS
    'Preferred provider_ref when multiple provider refs are admitted by one policy.';

COMMIT;
