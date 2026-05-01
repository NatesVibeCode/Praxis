-- Migration 364: remove retired Together DeepSeek V3.2 from native runtime authority.
--
-- The native runtime profile sync is intentionally strict: every
-- allowed_models entry must have an active provider_model_candidates row inside
-- provider_names. V3.2 is no longer an admitted Together serverless candidate
-- for this operator instance; keeping it in allowed_models makes startup fail
-- the authority projection even though V4-Pro is the only permitted API lane.

BEGIN;

UPDATE registry_native_runtime_profile_authority AS profile
   SET allowed_models = (
           SELECT COALESCE(jsonb_agg(model_slug ORDER BY first_seen), '[]'::jsonb)
           FROM (
               SELECT model_slug, MIN(ord) AS first_seen
               FROM jsonb_array_elements_text(
                   COALESCE(profile.allowed_models, '[]'::jsonb)
                   || '["deepseek-ai/DeepSeek-V4-Pro"]'::jsonb
               ) WITH ORDINALITY AS item(model_slug, ord)
               WHERE model_slug <> 'deepseek-ai/DeepSeek-V3.2'
               GROUP BY model_slug
           ) AS normalized
       ),
       recorded_at = now()
 WHERE profile.allowed_models ? 'deepseek-ai/DeepSeek-V3.2'
    OR NOT (profile.allowed_models ? 'deepseek-ai/DeepSeek-V4-Pro');

UPDATE provider_model_candidates
   SET status = 'retired',
       effective_to = COALESCE(effective_to, now()),
       decision_ref = 'decision.2026-04-26.private-api-compile-only-deepseek-v4-pro',
       capability_tags = '["api-only","retired","not-available"]'::jsonb,
       task_affinities = '{
         "primary": [],
         "secondary": [],
         "specialized": [],
         "fallback": [],
         "avoid": ["compile","compile_author","plan_section_author","plan_synthesis"]
       }'::jsonb
 WHERE provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V3.2';

DELETE FROM runtime_profile_admitted_routes
 WHERE provider_slug = 'together'
   AND model_slug = 'deepseek-ai/DeepSeek-V3.2';

-- Re-apply note:
--   Together is HTTP/API-only. Historical rows may still carry the old
--   task_type_routing transport_type='CLI' default; migration 378 rejects
--   updates to those rows. Prune impossible legacy CLI rows first, then retire
--   only the API routes that can be validly represented.
DO $$
DECLARE
    has_transport_type BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'task_type_routing'
           AND column_name = 'transport_type'
    ) INTO has_transport_type;

    IF has_transport_type THEN
        DELETE FROM task_type_routing AS route
         WHERE route.provider_slug = 'together'
           AND route.transport_type = 'CLI'
           AND NOT EXISTS (
               SELECT 1
                 FROM provider_transport_admissions AS admission
                WHERE admission.provider_slug = route.provider_slug
                  AND admission.transport_kind = 'cli'
                  AND admission.status = 'active'
           );

        UPDATE task_type_routing
           SET permitted = FALSE,
               rank = 99,
               rationale = 'Retired by migration 364: Together DeepSeek V3.2 is not an active serverless candidate; native API compile authority is V4-Pro only.',
               updated_at = now()
         WHERE provider_slug = 'together'
           AND model_slug = 'deepseek-ai/DeepSeek-V3.2'
           AND transport_type = 'API';
    ELSE
        UPDATE task_type_routing
           SET permitted = FALSE,
               rank = 99,
               rationale = 'Retired by migration 364: Together DeepSeek V3.2 is not an active serverless candidate; native API compile authority is V4-Pro only.',
               updated_at = now()
         WHERE provider_slug = 'together'
           AND model_slug = 'deepseek-ai/DeepSeek-V3.2';
    END IF;
END $$;

INSERT INTO private_provider_api_job_allowlist (
    runtime_profile_ref,
    job_type,
    adapter_type,
    provider_slug,
    model_slug,
    allowed,
    reason_code,
    decision_ref
)
SELECT profile.runtime_profile_ref,
       job_type,
       'llm_task',
       'together',
       'deepseek-ai/DeepSeek-V3.2',
       FALSE,
       'private_api_compile_only.deepseek_v4_pro',
       'decision.2026-04-26.private-api-compile-only-deepseek-v4-pro'
  FROM registry_native_runtime_profile_authority AS profile
 CROSS JOIN (VALUES ('compile'), ('compile_author'), ('plan_section_author'), ('plan_synthesis')) AS jobs(job_type)
ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
DO UPDATE SET
    allowed = EXCLUDED.allowed,
    reason_code = EXCLUDED.reason_code,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

SELECT refresh_private_provider_job_catalog(runtime_profile_ref)
  FROM registry_native_runtime_profile_authority;

SELECT refresh_private_provider_control_plane_snapshot(runtime_profile_ref)
  FROM registry_native_runtime_profile_authority;

COMMIT;
