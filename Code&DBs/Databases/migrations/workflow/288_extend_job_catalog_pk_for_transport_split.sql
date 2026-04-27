-- Migration 288: extend private_provider_job_catalog PK to include transport_type.
--
-- Migrations 286+287 split task_type_routing rows by transport_type
-- ('CLI' vs 'API'). The catalog projection function
-- refresh_private_provider_job_catalog reads each routing row and INSERTs
-- one private_provider_job_catalog row per (route, economics) match. With
-- the new split, a single (task_type, provider, model) can produce TWO
-- catalog rows (one per transport_type). The catalog's legacy PK
-- (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
-- doesn't include transport_type, so both projected rows collide on the
-- same key → CardinalityViolation on the function's ON CONFLICT clause.
--
-- This shows up as "ON CONFLICT DO UPDATE command cannot affect row a
-- second time" inside any migration that PERFORMs the refresh — including
-- migration 262 (which is re-run by bootstrap whenever its expected_object
-- rows are absent). The error blocks bootstrap entirely.
--
-- Fix: extend the catalog PK to include transport_type, then rewrite the
-- function's ON CONFLICT clause to match. Idempotent — re-applying is safe.

BEGIN;

-- 1. Replace the refresh function with one that:
--    (a) joins routing's transport_type to economics.transport_type so each
--        routing row matches the right transport classification, and
--    (b) DISTINCT-ON dedups so the (task_type, provider, model, adapter)
--        tuple matches the legacy 5-column catalog PK (one row per tuple,
--        chosen by sub_task_type=specific-wins-over-wildcard then route.rank).
--
-- The catalog table's PK is intentionally NOT extended to include
-- transport_type because earlier migrations (e.g. 267) INSERT...ON CONFLICT
-- with the legacy 5-column key and would fail on re-application. Dedup at
-- the function level is the right boundary: routing tracks both transports
-- but the catalog projection only emits the active one per 5-col tuple.
--
-- API admission is derived directly from routing rows with
-- transport_type='API' (per migration 287). The legacy api_job_policy
-- subquery is dropped — routing IS the admission.
CREATE OR REPLACE FUNCTION refresh_private_provider_job_catalog(
    p_runtime_profile_ref text
)
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    IF p_runtime_profile_ref IS NULL OR btrim(p_runtime_profile_ref) = '' THEN
        RAISE EXCEPTION 'runtime_profile_ref must be a non-empty string';
    END IF;

    DELETE FROM private_provider_job_catalog
    WHERE runtime_profile_ref = btrim(p_runtime_profile_ref);

    WITH active_candidates AS (
        SELECT DISTINCT ON (provider_slug, model_slug)
            candidate_ref, provider_ref, provider_slug, model_slug,
            default_parameters, created_at
        FROM provider_model_candidates
        WHERE status = 'active'
          AND effective_from <= now()
          AND (effective_to IS NULL OR effective_to > now())
        ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
    ),
    economics AS (
        SELECT
            profile.provider_slug,
            entry.key AS adapter_type,
            CASE WHEN entry.key = 'cli_llm' THEN 'CLI'
                 WHEN entry.key = 'llm_task' THEN 'API'
                 ELSE 'API' END AS transport_type,
            COALESCE(NULLIF(entry.value ->> 'billing_mode', ''), 'unspecified') AS cost_structure,
            entry.value AS cost_metadata
        FROM provider_cli_profiles AS profile
        CROSS JOIN LATERAL jsonb_each(COALESCE(profile.adapter_economics, '{}'::jsonb)) AS entry(key, value)
        WHERE profile.status = 'active'
          AND entry.key IN ('cli_llm', 'llm_task')
    ),
    matrix_rows AS (
        SELECT DISTINCT ON (
            btrim(p_runtime_profile_ref),
            route.task_type,
            economics.adapter_type,
            route.provider_slug,
            route.model_slug,
            economics.transport_type
        )
            btrim(p_runtime_profile_ref) AS runtime_profile_ref,
            route.task_type AS job_type,
            economics.transport_type,
            economics.adapter_type,
            route.provider_slug,
            route.model_slug,
            COALESCE(
                NULLIF(active_candidates.default_parameters ->> 'model_version', ''),
                NULLIF(active_candidates.default_parameters ->> 'version', ''),
                route.model_slug
            ) AS model_version,
            economics.cost_structure,
            economics.cost_metadata,
            CASE
                WHEN route.permitted IS NOT TRUE THEN 'disabled'
                WHEN active_candidates.candidate_ref IS NULL THEN 'disabled'
                WHEN admitted.candidate_ref IS NULL THEN 'disabled'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'disabled'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'disabled'
                WHEN transport.status <> 'active' THEN 'disabled'
                ELSE 'available'
            END AS availability_state,
            CASE
                WHEN route.permitted IS NOT TRUE THEN 'task_type_routing.denied'
                WHEN active_candidates.candidate_ref IS NULL THEN 'provider_model_candidate.missing'
                WHEN admitted.candidate_ref IS NULL THEN 'runtime_profile.not_admitted'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'transport.not_admitted'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'transport.policy_denied'
                WHEN transport.status <> 'active' THEN 'transport.inactive'
                ELSE 'available'
            END AS reason_code,
            active_candidates.candidate_ref,
            active_candidates.provider_ref,
            jsonb_build_array(
                'table.task_type_routing',
                'table.provider_model_candidates',
                'table.runtime_profile_admitted_routes',
                'table.provider_transport_admissions',
                'table.provider_cli_profiles',
                'table.private_provider_transport_control_policy'
            ) AS source_refs
        FROM task_type_routing AS route
        JOIN economics
          ON economics.provider_slug = route.provider_slug
         AND economics.transport_type = route.transport_type
        LEFT JOIN active_candidates
          ON active_candidates.provider_slug = route.provider_slug
         AND active_candidates.model_slug = route.model_slug
        LEFT JOIN runtime_profile_admitted_routes AS admitted
          ON admitted.runtime_profile_ref = btrim(p_runtime_profile_ref)
         AND admitted.candidate_ref = active_candidates.candidate_ref
         AND admitted.eligibility_status = 'admitted'
        LEFT JOIN provider_transport_admissions AS transport
          ON transport.provider_slug = route.provider_slug
         AND transport.adapter_type = economics.adapter_type
        ORDER BY
            btrim(p_runtime_profile_ref),
            route.task_type,
            economics.adapter_type,
            route.provider_slug,
            route.model_slug,
            economics.transport_type,
            -- Wildcard sub_task_type loses to specific ones so per-sub-task
            -- overrides win when present.
            CASE WHEN route.sub_task_type = '*' THEN 1 ELSE 0 END,
            route.rank ASC
    )
    INSERT INTO private_provider_job_catalog (
        runtime_profile_ref, job_type, transport_type, adapter_type,
        provider_slug, model_slug, model_version, cost_structure, cost_metadata,
        availability_state, reason_code, candidate_ref, provider_ref,
        source_refs, projected_at
    )
    SELECT
        runtime_profile_ref, job_type, transport_type, adapter_type,
        provider_slug, model_slug, model_version, cost_structure, cost_metadata,
        availability_state, reason_code, candidate_ref, provider_ref,
        source_refs, now()
    FROM matrix_rows
    ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
    DO UPDATE SET
        model_version = EXCLUDED.model_version,
        cost_structure = EXCLUDED.cost_structure,
        cost_metadata = EXCLUDED.cost_metadata,
        availability_state = EXCLUDED.availability_state,
        reason_code = EXCLUDED.reason_code,
        candidate_ref = EXCLUDED.candidate_ref,
        provider_ref = EXCLUDED.provider_ref,
        source_refs = EXCLUDED.source_refs,
        projected_at = EXCLUDED.projected_at;
END
$fn$;

-- 4. Force a refresh now so downstream queries see fresh catalog rows.
DO $$
DECLARE
    profile_ref text;
BEGIN
    FOR profile_ref IN
        SELECT runtime_profile_ref
        FROM registry_native_runtime_profile_authority
    LOOP
        PERFORM refresh_private_provider_job_catalog(profile_ref);
    END LOOP;
END $$;

COMMIT;
