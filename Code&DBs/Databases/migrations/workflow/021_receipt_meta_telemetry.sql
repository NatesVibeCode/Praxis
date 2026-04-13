-- Extend receipt_meta with richer telemetry columns for dispatch observability.

ALTER TABLE receipt_meta
    ADD COLUMN IF NOT EXISTS cache_read_tokens   integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cache_creation_tokens integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS model               text DEFAULT '',
    ADD COLUMN IF NOT EXISTS num_turns            integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duration_api_ms      integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tool_use             jsonb DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_receipt_meta_model
    ON receipt_meta (model);
