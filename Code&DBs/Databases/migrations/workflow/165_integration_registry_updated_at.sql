-- Migration 165: add updated_at to integration_registry
-- upsert_integration() and the integrations admin API both set
-- updated_at = now() on conflict. Column was missing from the
-- original 020 migration, making every DB-native upsert fail with
-- UndefinedColumnError.

ALTER TABLE integration_registry
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
