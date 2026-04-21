-- Migration 198: remove unbacked Anthropic Haiku runtime model.
--
-- Anthropic authority is CLI/subscription-only in this environment. Runtime
-- profiles may only list models that have active provider_model_candidates for
-- that rail. Haiku has no active CLI candidate, so keeping it in allowed_models
-- makes fresh-install projection fail.

BEGIN;

UPDATE registry_native_runtime_profile_authority
SET allowed_models = (
    SELECT COALESCE(jsonb_agg(model_slug ORDER BY ord), '[]'::jsonb)
    FROM jsonb_array_elements_text(allowed_models) WITH ORDINALITY AS item(model_slug, ord)
    WHERE model_slug <> 'claude-haiku-4-5-20251001'
)
WHERE allowed_models ? 'claude-haiku-4-5-20251001';

UPDATE model_sync_config
SET doc_model_ids = (
        SELECT COALESCE(jsonb_agg(model_slug ORDER BY ord), '[]'::jsonb)
        FROM jsonb_array_elements_text(doc_model_ids) WITH ORDINALITY AS item(model_slug, ord)
        WHERE model_slug <> 'claude-haiku-4-5-20251001'
    ),
    migration_rules = migration_rules - 'claude-haiku-4-5',
    sync_note = 'CLI/subscription authority only; seed models must have registry-backed CLI candidates',
    updated_at = now()
WHERE provider_slug = 'anthropic'
  AND (
      doc_model_ids ? 'claude-haiku-4-5-20251001'
      OR migration_rules ? 'claude-haiku-4-5'
  );

UPDATE provider_model_candidates
SET status = 'inactive',
    effective_to = COALESCE(effective_to, now())
WHERE provider_slug = 'anthropic'
  AND model_slug = 'claude-haiku-4-5-20251001'
  AND status = 'active';

COMMIT;
