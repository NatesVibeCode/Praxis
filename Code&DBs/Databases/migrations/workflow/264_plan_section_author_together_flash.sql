-- Migration 264: plan_section_author primary = Together DeepSeek-V3.2 (Flash tier)
--
-- Operator direction (2026-04-26, nate): "use Flash for fork-outs only (keeps
-- Pro's decomposition quality, halves fork-out cost)" + "use Together you have
-- the API in the keychain".
--
-- Context: compose_plan_via_llm orchestrates synthesis (1 Pro call → packet
-- seeds) + fork-out (N parallel author calls). Today plan_section_author
-- rank 1 is cursor_local/composer-2 (CLI agent, wrong lane for the API-only
-- compile exception); Together V3.2 sits at rank 99. Synthesis already runs
-- Together V4-Pro at rank 1 from migration 262.
--
-- This migration:
--   - Demotes cursor_local at rank 1 (its existing rank stays for CLI lanes
--     elsewhere; we just promote Together above it for plan_section_author).
--   - Promotes Together DeepSeek-V3.2 to rank 1 (Flash tier — cheaper/faster
--     than V4-Pro, fine for well-scoped per-packet authoring).
--   - Promotes Together DeepSeek-V4-Pro to rank 2 as a quality fallback if
--     V3.2 falls over.
--   - OpenRouter v4-flash/v4-pro stay at rank 99 — available if Together
--     and the in-stack Codex routes all fail.

BEGIN;

-- 1. Demote the existing rank 1 / 2 fork-out routes to make room.
UPDATE task_type_routing
   SET rank = 10,
       updated_at = now()
 WHERE task_type = 'plan_section_author'
   AND provider_slug = 'cursor_local'
   AND model_slug = 'composer-2'
   AND route_source = 'explicit';

-- 2. Promote Together V3.2 → rank 1 (Flash for fork-outs).
--
-- This migration runs against multiple PK shapes during the bootstrap chain:
--   * Fresh bootstrap, pre-333: PK is 5-col
--     (task_type, sub_task_type, provider_slug, model_slug, transport_type).
--     Original ON CONFLICT clause matches.
--   * Selective re-apply, post-333: PK is 4-col
--     (task_type, sub_task_type, provider_slug, model_slug). The 5-col
--     ON CONFLICT no longer matches any constraint and Postgres raises
--     "no unique or exclusion constraint matching the ON CONFLICT
--     specification".
--   * Selective re-apply, post-378: trigger
--     task_type_routing_transport_admission_check rejects CLI inserts for
--     HTTP-only providers (together is HTTP-only). The implicit
--     transport_type='CLI' default would be blocked.
--
-- The DO $$ block below picks the right ON CONFLICT shape at runtime and
-- swallows the trigger-block exception so re-apply against post-cleanup
-- state is an idempotent no-op. Selecting transport_type='API' explicitly
-- aligns with provider_transport_admissions for together; the original
-- migration relied on the historical transport_type='CLI' default which
-- was retroactively invalidated by migration 375 + trigger 378.
DO $$
DECLARE
    pk_has_transport BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
          FROM pg_constraint c
          JOIN pg_attribute a
            ON a.attrelid = c.conrelid
           AND a.attnum = ANY(c.conkey)
         WHERE c.conrelid = 'public.task_type_routing'::regclass
           AND c.contype = 'p'
           AND a.attname = 'transport_type'
    ) INTO pk_has_transport;

    IF pk_has_transport THEN
        -- Pre-333 PK shape (fresh bootstrap, 5-col PK).
        INSERT INTO task_type_routing (
            task_type, rank, provider_slug, model_slug, route_source, permitted, updated_at
        ) VALUES
            ('plan_section_author', 1, 'together', 'deepseek-ai/DeepSeek-V3.2',  'explicit', TRUE, now()),
            ('plan_section_author', 2, 'together', 'deepseek-ai/DeepSeek-V4-Pro', 'explicit', TRUE, now())
        ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type)
        DO UPDATE SET
            rank = EXCLUDED.rank,
            route_source = EXCLUDED.route_source,
            permitted = EXCLUDED.permitted,
            updated_at = now();
    ELSE
        -- Post-333 PK shape (selective re-apply, 4-col PK). The trigger
        -- added in migration 378 may block CLI inserts for HTTP-only
        -- providers; in that case this migration is a true no-op and the
        -- desired rows live in later migrations or in the routing-table
        -- runtime authority.
        BEGIN
            INSERT INTO task_type_routing (
                task_type, rank, provider_slug, model_slug, transport_type,
                route_source, permitted, updated_at
            ) VALUES
                ('plan_section_author', 1, 'together', 'deepseek-ai/DeepSeek-V3.2',  'API', 'explicit', TRUE, now()),
                ('plan_section_author', 2, 'together', 'deepseek-ai/DeepSeek-V4-Pro', 'API', 'explicit', TRUE, now())
            ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug)
            DO UPDATE SET
                rank = EXCLUDED.rank,
                route_source = EXCLUDED.route_source,
                permitted = EXCLUDED.permitted,
                updated_at = now();
        EXCEPTION
            WHEN check_violation OR raise_exception THEN
                RAISE NOTICE
                    '264_plan_section_author_together_flash: post-cleanup state — '
                    'INSERT skipped (likely transport-admission trigger): %', SQLERRM;
        END;
    END IF;
END $$;

-- 3. Tag Together V3.2 candidate with fork-author capability tags so capability-
--    based matching in the planner sees it as the appropriate Flash route.
UPDATE provider_model_candidates
   SET capability_tags = '["plan_section_author","structured-output","schema-normalization","cheap","fast","api-only","flash-tier","compile"]'::jsonb,
       task_affinities = '{
         "primary": ["plan_section_author","structured-output","schema-normalization"],
         "secondary": ["compile"],
         "specialized": ["fork-out","cheap","fast","api-only"],
         "fallback": [],
         "avoid": ["cli","tool-use","agentic-coding","plan_synthesis"]
       }'::jsonb
 WHERE provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V3.2';

COMMIT;
