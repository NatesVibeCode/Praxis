BEGIN;

ALTER TABLE compile_artifacts
    ADD COLUMN IF NOT EXISTS input_fingerprint TEXT;

UPDATE compile_artifacts
SET input_fingerprint = COALESCE(
    NULLIF(payload->'compile_provenance'->>'input_fingerprint', ''),
    content_hash
)
WHERE input_fingerprint IS NULL
   OR input_fingerprint = '';

ALTER TABLE compile_artifacts
    ALTER COLUMN input_fingerprint SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS compile_artifacts_kind_input_fingerprint_idx
    ON compile_artifacts (artifact_kind, input_fingerprint);

COMMENT ON COLUMN compile_artifacts.input_fingerprint IS
    'Exact-match compile reuse key derived from authority revisions plus file/context fingerprints.';

COMMIT;
