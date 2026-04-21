-- Migration 173: Schema migration apply-tracking authority.
--
-- Before this row-level authority, "was migration N applied?" could only be
-- inferred from pg_catalog object presence. That heuristic is wrong on three
-- axes: (a) a migration that ALTERs a pre-existing object leaves no new
-- relation to detect, (b) a migration rolled back by a later migration would
-- still look "applied", and (c) there is no way to audit who applied what or
-- when.
--
-- This table records one row per migration successfully applied by
-- storage/postgres/schema.py::_bootstrap_migration. Bootstrap eagerly creates
-- the table (CREATE TABLE IF NOT EXISTS) before it becomes part of the
-- canonical manifest, so the very first apply can insert its own row.
--
-- Decision:
--   decision.2026-04-19.schema-migrations-apply-tracking-table
-- Scope:
--   authority_domain=storage.workflow_migration_authority
--
-- Contract:
--   * filename         — canonical workflow migration filename (PK)
--   * content_sha256   — hex sha256 of the migration's SQL text at apply time
--   * applied_at       — timestamp the apply transaction succeeded
--   * applied_by       — process identity (e.g. schema_bootstrap, operator)
--   * bootstrap_role   — policy bucket at apply time: canonical / bootstrap_only
--   * metadata         — free-form jsonb for future extension (e.g. applier version)
--
-- The PRIMARY KEY on (filename) means re-applies UPSERT and refresh sha256 /
-- applied_at rather than producing duplicates. Apply-tracking rows are
-- informational, not ledgered, so ON CONFLICT DO UPDATE is the right shape.

CREATE TABLE IF NOT EXISTS schema_migrations (
    filename         text        NOT NULL PRIMARY KEY,
    content_sha256   text        NOT NULL,
    applied_at       timestamptz NOT NULL DEFAULT now(),
    applied_by       text        NOT NULL,
    bootstrap_role   text        NOT NULL,
    metadata         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT schema_migrations_filename_nonblank
        CHECK (btrim(filename) <> ''),
    CONSTRAINT schema_migrations_sha256_shape
        CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT schema_migrations_bootstrap_role_check
        CHECK (bootstrap_role IN ('canonical', 'bootstrap_only'))
);

DO $$
BEGIN
    ALTER TABLE schema_migrations
        ADD CONSTRAINT schema_migrations_filename_nonblank
        CHECK (btrim(filename) <> '');
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END
$$;

DO $$
BEGIN
    ALTER TABLE schema_migrations
        ADD CONSTRAINT schema_migrations_sha256_shape
        CHECK (content_sha256 ~ '^[0-9a-f]{64}$');
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END
$$;

DO $$
BEGIN
    ALTER TABLE schema_migrations
        ADD CONSTRAINT schema_migrations_bootstrap_role_check
        CHECK (bootstrap_role IN ('canonical', 'bootstrap_only'));
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END
$$;

COMMENT ON TABLE schema_migrations IS
    'Apply-tracking for canonical workflow migrations. One row per successful apply of a file in Code&DBs/Databases/migrations/workflow/. Written by storage/postgres/schema.py::_bootstrap_migration.';
COMMENT ON COLUMN schema_migrations.filename IS
    'Canonical migration filename (e.g. 173_schema_migrations.sql). Primary key — re-applies UPSERT.';
COMMENT ON COLUMN schema_migrations.content_sha256 IS
    'Hex sha256 of the migration SQL text at apply time; drift between disk and recorded sha indicates the file was edited after apply.';
COMMENT ON COLUMN schema_migrations.applied_by IS
    'Process/operator identity that ran the apply. schema_bootstrap for the runtime bootstrapper; operator:<id> for manual applies.';
COMMENT ON COLUMN schema_migrations.bootstrap_role IS
    'Policy bucket at apply time (canonical / bootstrap_only) from workflow_migration_authority.';

CREATE INDEX IF NOT EXISTS idx_schema_migrations_applied_at
    ON schema_migrations (applied_at DESC);
CREATE INDEX IF NOT EXISTS idx_schema_migrations_bootstrap_role
    ON schema_migrations (bootstrap_role);
