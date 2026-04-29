-- Migration 329: allow task-scoped route eligibility to materialize into the
-- private provider job catalog.
--
-- Why:
-- runtime_profile_admitted_routes is candidate-global, but task_type_route_eligibility
-- already expresses job-type-scoped exceptions like "allow Sonnet for build/review/compile
-- without admitting it everywhere". The mechanical gate (`effective_private_provider_job_catalog`)
-- must honor that narrower authority or the runtime lies: soft route logic says a lane is
-- allowed while workflow admission still blocks it as unavailable.

BEGIN;

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
            candidate_ref,
            provider_ref,
            provider_slug,
            model_slug,
            default_parameters,
            created_at
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
            CASE
                WHEN entry.key = 'cli_llm' THEN 'CLI'
                WHEN entry.key = 'llm_task' THEN 'API'
                ELSE 'API'
            END AS transport_type,
            COALESCE(NULLIF(entry.value ->> 'billing_mode', ''), 'unspecified') AS cost_structure,
            entry.value AS cost_metadata
        FROM provider_cli_profiles AS profile
        CROSS JOIN LATERAL jsonb_each(
            COALESCE(profile.adapter_economics, '{}'::jsonb)
        ) AS entry(key, value)
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
                WHEN route_window.eligibility_status = 'rejected' THEN 'disabled'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'disabled'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'disabled'
                WHEN transport.status <> 'active' THEN 'disabled'
                WHEN admitted.candidate_ref IS NOT NULL THEN 'available'
                WHEN route_window.eligibility_status = 'eligible' THEN 'available'
                ELSE 'disabled'
            END AS availability_state,
            CASE
                WHEN route.permitted IS NOT TRUE THEN 'task_type_routing.denied'
                WHEN active_candidates.candidate_ref IS NULL THEN 'provider_model_candidate.missing'
                WHEN route_window.eligibility_status = 'rejected'
                    THEN COALESCE(NULLIF(route_window.reason_code, ''), 'task_route_eligibility.rejected')
                WHEN transport.provider_transport_admission_id IS NULL THEN 'transport.not_admitted'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'transport.policy_denied'
                WHEN transport.status <> 'active' THEN 'transport.inactive'
                WHEN admitted.candidate_ref IS NOT NULL THEN 'available'
                WHEN route_window.eligibility_status = 'eligible'
                    THEN COALESCE(NULLIF(route_window.reason_code, ''), 'task_route_eligibility.eligible')
                ELSE 'runtime_profile.not_admitted'
            END AS reason_code,
            active_candidates.candidate_ref,
            active_candidates.provider_ref,
            (
                jsonb_build_array(
                    'table.task_type_routing',
                    'table.provider_model_candidates',
                    'table.runtime_profile_admitted_routes',
                    'table.provider_transport_admissions',
                    'table.provider_cli_profiles',
                    'table.private_provider_transport_control_policy'
                )
                || CASE
                    WHEN route_window.task_route_eligibility_id IS NULL THEN '[]'::jsonb
                    ELSE jsonb_build_array('table.task_type_route_eligibility')
                END
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
        LEFT JOIN LATERAL (
            SELECT
                eligibility.task_route_eligibility_id,
                eligibility.eligibility_status,
                eligibility.reason_code
            FROM task_type_route_eligibility AS eligibility
            WHERE eligibility.provider_slug = route.provider_slug
              AND (eligibility.task_type = route.task_type OR eligibility.task_type IS NULL)
              AND (eligibility.model_slug = route.model_slug OR eligibility.model_slug IS NULL)
              AND eligibility.effective_from <= now()
              AND (eligibility.effective_to IS NULL OR eligibility.effective_to > now())
            ORDER BY
                CASE WHEN eligibility.task_type = route.task_type THEN 1 ELSE 0 END DESC,
                CASE WHEN eligibility.model_slug = route.model_slug THEN 1 ELSE 0 END DESC,
                eligibility.effective_from DESC,
                eligibility.decision_ref DESC,
                eligibility.task_route_eligibility_id DESC
            LIMIT 1
        ) AS route_window ON TRUE
        ORDER BY
            btrim(p_runtime_profile_ref),
            route.task_type,
            economics.adapter_type,
            route.provider_slug,
            route.model_slug,
            economics.transport_type,
            CASE WHEN route.sub_task_type = '*' THEN 1 ELSE 0 END,
            route.rank ASC
    )
    INSERT INTO private_provider_job_catalog (
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
        model_version,
        cost_structure,
        cost_metadata,
        availability_state,
        reason_code,
        candidate_ref,
        provider_ref,
        source_refs,
        projected_at
    )
    SELECT
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
        model_version,
        cost_structure,
        cost_metadata,
        availability_state,
        reason_code,
        candidate_ref,
        provider_ref,
        source_refs,
        now()
    FROM matrix_rows
    ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
    DO UPDATE SET
        transport_type = EXCLUDED.transport_type,
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

COMMENT ON VIEW task_type_routing_admission_audit IS
'Surfaces task_type_routing rows that the production resolver and workflow admission gate will actually keep. A routing row only materializes when the private provider job catalog admits the tuple after candidate existence, transport policy, runtime-profile admission, and any active task_type_route_eligibility override are applied.';

DO $$
DECLARE
    profile_ref text;
BEGIN
    FOR profile_ref IN
        SELECT runtime_profile_ref
        FROM registry_native_runtime_profile_authority
    LOOP
        PERFORM refresh_private_provider_job_catalog(profile_ref);
        PERFORM refresh_private_provider_control_plane_snapshot(profile_ref);
    END LOOP;
END $$;

COMMIT;
