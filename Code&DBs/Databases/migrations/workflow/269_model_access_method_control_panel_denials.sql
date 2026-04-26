-- Migration 269: Model access method control-panel denials.
--
-- A model access method may be turned off for one task type or more broadly.
-- The effective catalog must remove it before routing and rollover see it,
-- and upstream denials must tell agents not to re-enable it just to complete
-- the current task.

BEGIN;

CREATE TABLE IF NOT EXISTS private_provider_model_access_denials (
    runtime_profile_ref TEXT NOT NULL CHECK (btrim(runtime_profile_ref) <> ''),
    job_type TEXT NOT NULL DEFAULT '*' CHECK (btrim(job_type) <> ''),
    transport_type TEXT NOT NULL DEFAULT '*' CHECK (transport_type IN ('*', 'CLI', 'API')),
    adapter_type TEXT NOT NULL DEFAULT '*' CHECK (btrim(adapter_type) <> ''),
    provider_slug TEXT NOT NULL DEFAULT '*' CHECK (btrim(provider_slug) <> ''),
    model_slug TEXT NOT NULL DEFAULT '*' CHECK (btrim(model_slug) <> ''),
    denied BOOLEAN NOT NULL DEFAULT TRUE,
    reason_code TEXT NOT NULL DEFAULT 'control_panel.model_access_method_turned_off'
        CHECK (btrim(reason_code) <> ''),
    operator_message TEXT NOT NULL DEFAULT 'this Model Access method has been turned off on purpose at the control panel either for this specific task type, or more broadly, consult the control panel and do not turn it on without confirming with the user even if you think that will help you complete your task.'
        CHECK (btrim(operator_message) <> ''),
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (runtime_profile_ref, job_type, transport_type, adapter_type, provider_slug, model_slug)
);

CREATE INDEX IF NOT EXISTS private_provider_model_access_denials_lookup_idx
    ON private_provider_model_access_denials (
        runtime_profile_ref,
        denied,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
        job_type
    );

UPDATE private_provider_transport_control_policy
   SET reason_code = 'control_panel.transport_turned_off',
       operator_message = 'this Model Access method has been turned off on purpose at the control panel either for this specific task type, or more broadly, consult the control panel and do not turn it on without confirming with the user even if you think that will help you complete your task.',
       updated_at = now()
 WHERE transport_type IN ('CLI', 'API');

CREATE OR REPLACE FUNCTION refresh_private_provider_job_catalog(
    p_runtime_profile_ref TEXT
) RETURNS VOID AS $$
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
        CROSS JOIN LATERAL jsonb_each(COALESCE(profile.adapter_economics, '{}'::jsonb)) AS entry(key, value)
        WHERE profile.status = 'active'
          AND entry.key IN ('cli_llm', 'llm_task')
    ),
    api_job_policy AS (
        SELECT *
        FROM private_provider_api_job_allowlist
        WHERE runtime_profile_ref = btrim(p_runtime_profile_ref)
    ),
    transport_control_policy AS (
        SELECT *
        FROM private_provider_transport_control_policy
        WHERE runtime_profile_ref = btrim(p_runtime_profile_ref)
    ),
    matrix_rows AS (
        SELECT
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
                WHEN access_denial.denied IS TRUE THEN 'disabled'
                WHEN economics.transport_type = 'API'
                 AND COALESCE(api_policy.allowed, false) IS NOT TRUE THEN 'disabled'
                WHEN admitted.candidate_ref IS NULL THEN 'disabled'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'disabled'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'disabled'
                WHEN transport.status <> 'active' THEN 'disabled'
                ELSE 'available'
            END AS availability_state,
            CASE
                WHEN route.permitted IS NOT TRUE THEN 'task_type_routing.denied'
                WHEN active_candidates.candidate_ref IS NULL THEN 'provider_model_candidate.missing'
                WHEN access_denial.denied IS TRUE
                THEN COALESCE(
                    access_denial.reason_code,
                    'control_panel.model_access_method_turned_off'
                )
                WHEN economics.transport_type = 'API'
                 AND COALESCE(api_policy.allowed, false) IS NOT TRUE
                THEN COALESCE(control_policy.reason_code, 'control_panel.transport_turned_off')
                WHEN admitted.candidate_ref IS NULL THEN 'runtime_profile_route.not_admitted'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'provider_transport.missing'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'provider_transport.policy_denied'
                WHEN transport.status <> 'active' THEN 'provider_transport.disabled'
                ELSE 'catalog.available'
            END AS reason_code,
            active_candidates.candidate_ref,
            active_candidates.provider_ref,
            jsonb_build_array(
                'table.task_type_routing',
                'table.provider_model_candidates',
                'table.runtime_profile_admitted_routes',
                'table.provider_transport_admissions',
                'table.provider_cli_profiles',
                'table.private_provider_api_job_allowlist',
                'table.private_provider_transport_control_policy',
                'table.private_provider_model_access_denials'
            ) AS source_refs
        FROM task_type_routing AS route
        JOIN economics
          ON economics.provider_slug = route.provider_slug
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
        LEFT JOIN api_job_policy AS api_policy
          ON api_policy.job_type = route.task_type
         AND api_policy.adapter_type = economics.adapter_type
         AND api_policy.provider_slug = route.provider_slug
         AND api_policy.model_slug = route.model_slug
        LEFT JOIN transport_control_policy AS control_policy
          ON control_policy.transport_type = economics.transport_type
        LEFT JOIN LATERAL (
            SELECT denial.*
            FROM private_provider_model_access_denials AS denial
            WHERE denial.runtime_profile_ref = btrim(p_runtime_profile_ref)
              AND denial.denied IS TRUE
              AND denial.job_type IN ('*', route.task_type)
              AND denial.transport_type IN ('*', economics.transport_type)
              AND denial.adapter_type IN ('*', economics.adapter_type)
              AND denial.provider_slug IN ('*', route.provider_slug)
              AND denial.model_slug IN ('*', route.model_slug)
            ORDER BY
                CASE WHEN denial.job_type = route.task_type THEN 1 ELSE 0 END DESC,
                CASE WHEN denial.model_slug = route.model_slug THEN 1 ELSE 0 END DESC,
                CASE WHEN denial.provider_slug = route.provider_slug THEN 1 ELSE 0 END DESC,
                CASE WHEN denial.adapter_type = economics.adapter_type THEN 1 ELSE 0 END DESC,
                CASE WHEN denial.transport_type = economics.transport_type THEN 1 ELSE 0 END DESC,
                denial.updated_at DESC
            LIMIT 1
        ) AS access_denial ON TRUE
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

    INSERT INTO authority_projection_state (
        projection_ref,
        last_refreshed_at,
        freshness_status,
        error_code,
        error_detail,
        updated_at
    ) VALUES (
        'projection.private_provider_job_catalog',
        now(),
        'fresh',
        NULL,
        NULL,
        now()
    )
    ON CONFLICT (projection_ref) DO UPDATE
    SET last_refreshed_at = EXCLUDED.last_refreshed_at,
        freshness_status = EXCLUDED.freshness_status,
        error_code = NULL,
        error_detail = NULL,
        updated_at = now();

    PERFORM refresh_private_provider_control_plane_snapshot(btrim(p_runtime_profile_ref));
END;
$$ LANGUAGE plpgsql;

DROP VIEW IF EXISTS provider_transport_gate_denials;

CREATE VIEW provider_transport_gate_denials AS
SELECT
    catalog.runtime_profile_ref,
    catalog.job_type,
    catalog.transport_type,
    catalog.adapter_type,
    catalog.provider_slug,
    catalog.model_slug,
    catalog.reason_code,
    catalog.source_refs,
    COALESCE(denial.job_type, '*') AS control_scope_job_type,
    COALESCE(denial.provider_slug, '*') AS control_scope_provider_slug,
    COALESCE(denial.model_slug, '*') AS control_scope_model_slug,
    policy.default_posture,
    COALESCE(denial.operator_message, policy.operator_message) AS operator_message,
    COALESCE(denial.decision_ref, policy.decision_ref) AS decision_ref,
    catalog.projected_at,
    catalog.projection_ref
FROM private_provider_job_catalog AS catalog
LEFT JOIN private_provider_transport_control_policy AS policy
  ON policy.runtime_profile_ref = catalog.runtime_profile_ref
 AND policy.transport_type = catalog.transport_type
LEFT JOIN LATERAL (
    SELECT access_denial.*
    FROM private_provider_model_access_denials AS access_denial
    WHERE access_denial.runtime_profile_ref = catalog.runtime_profile_ref
      AND access_denial.denied IS TRUE
      AND access_denial.job_type IN ('*', catalog.job_type)
      AND access_denial.transport_type IN ('*', catalog.transport_type)
      AND access_denial.adapter_type IN ('*', catalog.adapter_type)
      AND access_denial.provider_slug IN ('*', catalog.provider_slug)
      AND access_denial.model_slug IN ('*', catalog.model_slug)
    ORDER BY
        CASE WHEN access_denial.job_type = catalog.job_type THEN 1 ELSE 0 END DESC,
        CASE WHEN access_denial.model_slug = catalog.model_slug THEN 1 ELSE 0 END DESC,
        CASE WHEN access_denial.provider_slug = catalog.provider_slug THEN 1 ELSE 0 END DESC,
        CASE WHEN access_denial.adapter_type = catalog.adapter_type THEN 1 ELSE 0 END DESC,
        CASE WHEN access_denial.transport_type = catalog.transport_type THEN 1 ELSE 0 END DESC,
        access_denial.updated_at DESC
    LIMIT 1
) AS denial ON TRUE
WHERE catalog.availability_state = 'disabled';

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
