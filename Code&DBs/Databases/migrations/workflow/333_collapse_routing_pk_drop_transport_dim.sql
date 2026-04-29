-- Migration 333: collapse task_type_routing PK — drop transport_type from identity.
--
-- Operator direction (2026-04-29, nate): the PK installed by migration 286
-- — (task_type, sub_task_type, provider_slug, model_slug, transport_type)
-- — encourages duplicates. Every "switch model from CLI to API" leaves two
-- rows behind unless the operator manually deletes one. The projection
-- (effective_private_provider_job_catalog) doubles too, because it 1:1
-- mirrors source duplicates.
--
-- A model's transport is a property of the (provider, model) pair via
-- adapter family, not a routing dimension. The PK should be the *logical*
-- routing key: one preferred row per (task, sub_task, provider, model).
-- transport_type stays as metadata so consumers that need it (cost
-- routing, transport admission) can still read it — it's just no longer
-- part of identity.
--
-- This migration:
--   1. Dedups existing rows. For each (task_type, sub_task_type,
--      provider_slug, model_slug) group with >1 row, keep the winner and
--      delete the rest. Winner = ranked by (permitted DESC, recent net
--      successes DESC, updated_at DESC).
--   2. Drops PK installed in 286.
--   3. Re-adds PK without transport_type:
--      (task_type, sub_task_type, provider_slug, model_slug).
--
-- Standing-order references:
--   architecture-policy::routing::canonical-write-surface (roadmap)
--   BUG-572FCE93 / BUG-20085DF2 (Phase B candidate identity tuple)
--
-- Reversibility: the column transport_type stays. To restore the old PK
-- shape, re-add transport_type to the PK constraint after re-introducing
-- duplicates (which would mean operating with two rows per task+model).

BEGIN;

-- Detach the projection-refresh trigger temporarily — DELETE+ALTER each
-- emit ALTER TABLE statements that the trigger reacts to. Re-attached at
-- end of transaction.
DROP TRIGGER IF EXISTS trg_refresh_model_access_task_type_routing ON task_type_routing;

-- Step 1. Delete duplicate rows. Tied logic: permitted=true beats false;
-- net successes (recent_successes - recent_failures) beats lower; latest
-- updated_at wins as final tiebreaker.
WITH ranked AS (
    SELECT
        task_type,
        sub_task_type,
        provider_slug,
        model_slug,
        transport_type,
        ROW_NUMBER() OVER (
            PARTITION BY task_type, sub_task_type, provider_slug, model_slug
            ORDER BY
                permitted DESC,
                (COALESCE(recent_successes, 0) - COALESCE(recent_failures, 0)) DESC,
                updated_at DESC
        ) AS rn
    FROM task_type_routing
)
DELETE FROM task_type_routing AS r
 USING ranked
 WHERE ranked.rn > 1
   AND r.task_type = ranked.task_type
   AND r.sub_task_type = ranked.sub_task_type
   AND r.provider_slug = ranked.provider_slug
   AND r.model_slug = ranked.model_slug
   AND r.transport_type = ranked.transport_type;

-- Step 2. Swap the PK. New shape is the logical routing key without
-- transport_type. transport_type stays as a metadata column.
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

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'task_type_routing_pkey'
          AND conrelid = 'task_type_routing'::regclass
    ) THEN
        ALTER TABLE task_type_routing
            ADD CONSTRAINT task_type_routing_pkey
            PRIMARY KEY (task_type, sub_task_type, provider_slug, model_slug);
    END IF;
END $$;

-- Re-attach trigger from migration 272.
CREATE TRIGGER trg_refresh_model_access_task_type_routing
    AFTER INSERT OR UPDATE OR DELETE ON task_type_routing
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

COMMENT ON CONSTRAINT task_type_routing_pkey ON task_type_routing IS
    'Logical routing identity: (task_type, sub_task_type, provider_slug, model_slug). transport_type is metadata, not identity. Migration 333 collapsed transport_type out of the PK to eliminate the CLI/API duplicate-row foot-gun introduced in 286.';

COMMIT;
