BEGIN;

ALTER TABLE provider_cli_profiles
    ADD COLUMN IF NOT EXISTS prompt_mode TEXT NOT NULL DEFAULT 'stdin';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_cli_profiles_prompt_mode_check'
    ) THEN
        ALTER TABLE provider_cli_profiles
            ADD CONSTRAINT provider_cli_profiles_prompt_mode_check
            CHECK (prompt_mode IN ('stdin', 'argv'));
    END IF;
END $$;

UPDATE provider_cli_profiles
SET prompt_mode = COALESCE(NULLIF(prompt_mode, ''), 'stdin')
WHERE prompt_mode NOT IN ('stdin', 'argv')
   OR prompt_mode IS NULL;

COMMIT;
