ALTER TABLE context_bundles
    ADD COLUMN IF NOT EXISTS sandbox_profile_ref text;

UPDATE context_bundles
SET sandbox_profile_ref = COALESCE(
    NULLIF(btrim(bundle_payload -> 'runtime_profile' ->> 'sandbox_profile_ref'), ''),
    runtime_profile_ref
)
WHERE sandbox_profile_ref IS NULL
   OR btrim(sandbox_profile_ref) = '';

ALTER TABLE context_bundles
    ALTER COLUMN sandbox_profile_ref SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'context_bundles_sandbox_profile_ref_nonblank'
    ) THEN
        ALTER TABLE context_bundles
            ADD CONSTRAINT context_bundles_sandbox_profile_ref_nonblank
            CHECK (btrim(sandbox_profile_ref) <> '');
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS context_bundles_sandbox_profile_idx
    ON context_bundles (sandbox_profile_ref);

COMMENT ON COLUMN context_bundles.sandbox_profile_ref IS 'Canonical sandbox profile resolved for this context bundle.';
