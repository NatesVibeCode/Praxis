-- Migration 101: Model sync authority
--
-- Moves ANTHROPIC_DOC_MODELS and ACTIVE_MODEL_MIGRATIONS from
-- sync_provider_model_catalog.py into Postgres so the sync script
-- reads authoritative lists from the DB instead of hardcoded constants.

BEGIN;

CREATE TABLE IF NOT EXISTS model_sync_config (
    provider_slug   TEXT PRIMARY KEY,
    -- Canonical model IDs to sync (sourced from provider docs/API).
    doc_model_ids   JSONB NOT NULL DEFAULT '[]'::jsonb
                        CHECK (jsonb_typeof(doc_model_ids) = 'array'),
    -- Active migration rules: old_model_id -> new_model_id.
    -- Used to remap stale candidate refs during sync.
    migration_rules JSONB NOT NULL DEFAULT '{}'::jsonb
                        CHECK (jsonb_typeof(migration_rules) = 'object'),
    -- Free-text note for humans (source URL, observation date, etc.)
    sync_note       TEXT NOT NULL DEFAULT '',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Anthropic: sourced from https://platform.claude.com/docs/en/about-claude/models/overview
-- Observed April 8, 2026.
INSERT INTO model_sync_config (provider_slug, doc_model_ids, migration_rules, sync_note)
VALUES (
    'anthropic',
    '["claude-opus-4-7","claude-sonnet-4-6","claude-haiku-4-5-20251001"]'::jsonb,
    '{"claude-opus-4-6":"claude-opus-4-7","claude-sonnet-4-5":"claude-sonnet-4-6","claude-haiku-4-5":"claude-haiku-4-5-20251001"}'::jsonb,
    'Seeded from sync_provider_model_catalog.py — observed IDs April 8 2026'
)
ON CONFLICT (provider_slug) DO UPDATE SET
    doc_model_ids   = EXCLUDED.doc_model_ids,
    migration_rules = EXCLUDED.migration_rules,
    sync_note       = EXCLUDED.sync_note,
    updated_at      = now();

COMMIT;
