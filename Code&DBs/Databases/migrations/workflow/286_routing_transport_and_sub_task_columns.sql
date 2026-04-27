-- Migration 286: task_type_routing gains transport_type + sub_task_type columns.
--
-- Operator direction (2026-04-27, nate): API permission must live in the
-- router itself, not in a parallel allowlist table that the matrix view
-- joins against. The split source-of-truth is what produced the
-- "task_type_routing_admission_audit" view's reason for existing — and the
-- column-rename foot-gun that jammed bootstrap on migration 282. One table,
-- one source of truth.
--
-- This is the first of two migrations. Step 1 is purely additive:
--
--   * ADD COLUMN sub_task_type TEXT NOT NULL DEFAULT '*'
--     Sub-task granularity inside a task_type. Most rows stay '*' (applies
--     to the whole task_type). Compile children get specific values like
--     'plan_synthesis', 'plan_fork_author', 'plan_pill_match' so a single
--     task_type can declare different (provider, model) preferences for
--     each sub-stage without bolting on yet another table.
--
--   * ADD COLUMN transport_type TEXT NOT NULL DEFAULT 'CLI'
--     CHECK (transport_type IN ('CLI', 'API'))
--     The router now declares which transport each row applies to. Existing
--     rows default to 'CLI' (the historically-default-open transport).
--     API admission is no longer derived from the
--     private_provider_api_job_allowlist table — it's derived from the
--     presence (or absence) of a routing row with transport_type='API'.
--
--   * Swap the primary key from (task_type, provider_slug, model_slug)
--     to (task_type, sub_task_type, provider_slug, model_slug,
--     transport_type) so the same (task_type, provider, model) can have
--     independent CLI and API ranks AND fine-grained sub-task overrides.
--
-- Migration 287 follows immediately: promote every allowed
-- private_provider_api_job_allowlist row into a routing row with
-- transport_type='API', then rewrite the access matrix view to read API
-- permission from routing instead of the allowlist. Migration 286 is
-- additive so consumers that JOIN on (task_type, provider, model) keep
-- working through the transition (existing rows preserve their values
-- unchanged, new columns default to '*' / 'CLI').

BEGIN;

-- Drop refresh trigger temporarily — adding columns and swapping PK each
-- emit ALTER TABLE statements that the trigger reacts to. Re-attached at
-- end of transaction.
DROP TRIGGER IF EXISTS trg_refresh_model_access_task_type_routing ON task_type_routing;

ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS sub_task_type TEXT NOT NULL DEFAULT '*';

ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS transport_type TEXT NOT NULL DEFAULT 'CLI';

-- CHECK constraints. IF NOT EXISTS guard so re-running is idempotent.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'task_type_routing_sub_task_type_nonblank'
    ) THEN
        ALTER TABLE task_type_routing
            ADD CONSTRAINT task_type_routing_sub_task_type_nonblank
            CHECK (btrim(sub_task_type) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'task_type_routing_transport_type_check'
    ) THEN
        ALTER TABLE task_type_routing
            ADD CONSTRAINT task_type_routing_transport_type_check
            CHECK (transport_type IN ('CLI', 'API'));
    END IF;
END $$;

-- Swap the primary key. The old PK was (task_type, provider_slug,
-- model_slug); the new shape needs sub_task_type + transport_type to allow
-- the same (task_type, provider, model) to have independent CLI/API rows
-- and per-sub-task overrides.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'task_type_routing_pkey'
          AND conrelid = 'task_type_routing'::regclass
    ) THEN
        ALTER TABLE task_type_routing
            DROP CONSTRAINT task_type_routing_pkey;
    END IF;

    -- Re-add. Compound PK ordering matches expected lookup pattern:
    -- (task_type, sub_task_type) for the sub-task lookup, then
    -- (provider, model) for the route id, then transport_type for the
    -- transport split.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'task_type_routing_pkey'
          AND conrelid = 'task_type_routing'::regclass
    ) THEN
        ALTER TABLE task_type_routing
            ADD CONSTRAINT task_type_routing_pkey
            PRIMARY KEY (task_type, sub_task_type, provider_slug, model_slug, transport_type);
    END IF;
END $$;

-- Re-attach the trigger that 272 installed.
CREATE TRIGGER trg_refresh_model_access_task_type_routing
    AFTER INSERT OR UPDATE OR DELETE ON task_type_routing
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

COMMENT ON COLUMN task_type_routing.sub_task_type IS
    'Sub-task granularity inside a task_type. Most rows leave this as the wildcard ''*'' (applies to the whole task_type). Compile children populate specific values (plan_synthesis, plan_fork_author, plan_pill_match) so different compile sub-stages can declare independent provider/model preferences without a second table.';

COMMENT ON COLUMN task_type_routing.transport_type IS
    'Transport this routing row applies to (CLI or API). Replaces the source-of-truth role of private_provider_api_job_allowlist for API admission: a routing row with transport_type=''API'' IS the admission, no separate allowlist needed. Migration 287 promotes existing allowlist rows into routing rows and rewrites the access matrix view to read API permission from this column.';

COMMIT;
