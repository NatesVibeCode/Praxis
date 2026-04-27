-- Migration 267: Provider transport control-panel denial messaging.
--
-- Operator direction (2026-04-26, nate): when a model access method has been
-- turned off at the control panel, upstream surfaces must say so plainly:
-- "this Model Access method has been turned off on purpose at the control
-- panel either for this specific task type, or more broadly, consult the
-- control panel and do not turn it on without confirming with the user even if
-- you think that will help you complete your task."

BEGIN;

CREATE TABLE IF NOT EXISTS private_provider_transport_control_policy (
    runtime_profile_ref TEXT NOT NULL CHECK (btrim(runtime_profile_ref) <> ''),
    transport_type TEXT NOT NULL CHECK (transport_type IN ('CLI', 'API')),
    default_posture TEXT NOT NULL CHECK (
        default_posture IN ('allow_unless_disabled', 'deny_unless_allowlisted')
    ),
    reason_code TEXT NOT NULL DEFAULT 'control_panel.transport_turned_off'
        CHECK (btrim(reason_code) <> ''),
    operator_message TEXT NOT NULL CHECK (btrim(operator_message) <> ''),
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (runtime_profile_ref, transport_type)
);

INSERT INTO private_provider_transport_control_policy (
    runtime_profile_ref,
    transport_type,
    default_posture,
    reason_code,
    operator_message,
    decision_ref,
    updated_at
)
SELECT
    profile.runtime_profile_ref,
    policy.transport_type,
    policy.default_posture,
    'control_panel.transport_turned_off',
    'this Model Access method has been turned off on purpose at the control panel either for this specific task type, or more broadly, consult the control panel and do not turn it on without confirming with the user even if you think that will help you complete your task.',
    'decision.2026-04-26.private-api-compile-only-deepseek-v4-pro',
    now()
FROM registry_native_runtime_profile_authority AS profile
CROSS JOIN (
    VALUES
        ('API'::text, 'deny_unless_allowlisted'::text),
        ('CLI'::text, 'allow_unless_disabled'::text)
) AS policy(transport_type, default_posture)
ON CONFLICT (runtime_profile_ref, transport_type) DO UPDATE SET
    default_posture = EXCLUDED.default_posture,
    reason_code = EXCLUDED.reason_code,
    operator_message = EXCLUDED.operator_message,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO runtime_profile_admitted_routes (
    runtime_profile_ref,
    model_profile_id,
    provider_policy_id,
    candidate_ref,
    provider_ref,
    provider_slug,
    model_slug,
    eligibility_status,
    reason_code,
    source_window_refs,
    projected_at,
    projection_ref
)
SELECT
    profile.runtime_profile_ref,
    'model_profile.together.compile.deepseek-v4-pro',
    'provider_policy.together.llm_task',
    'candidate.together.deepseek-v4-pro',
    'provider.together',
    'together',
    'deepseek-ai/DeepSeek-V4-Pro',
    'admitted',
    'private_api_compile_only.deepseek_v4_pro',
    '["migration.267_provider_transport_control_panel_policy"]'::jsonb,
    now(),
    'projection.runtime_profile_admitted_routes'
FROM registry_native_runtime_profile_authority AS profile
ON CONFLICT (runtime_profile_ref, candidate_ref) DO UPDATE SET
    model_profile_id = EXCLUDED.model_profile_id,
    provider_policy_id = EXCLUDED.provider_policy_id,
    provider_ref = EXCLUDED.provider_ref,
    provider_slug = EXCLUDED.provider_slug,
    model_slug = EXCLUDED.model_slug,
    eligibility_status = EXCLUDED.eligibility_status,
    reason_code = EXCLUDED.reason_code,
    source_window_refs = EXCLUDED.source_window_refs,
    projected_at = EXCLUDED.projected_at,
    projection_ref = EXCLUDED.projection_ref;

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
                'table.private_provider_transport_control_policy'
            ) AS source_refs
        FROM task_type_routing AS route
        JOIN economics
          ON economics.provider_slug = route.provider_slug
         AND economics.transport_type = route.transport_type
         AND route.sub_task_type = '*'
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

-- DROP first because Postgres CREATE OR REPLACE VIEW cannot drop or retype
-- columns relative to the existing view (raises "cannot drop columns from
-- view" on bootstrap). The view is read-only — dropping it has no data side
-- effect and the next statement re-creates it idempotently.
DROP VIEW IF EXISTS provider_transport_gate_denials;
CREATE OR REPLACE VIEW provider_transport_gate_denials AS
SELECT
    catalog.runtime_profile_ref,
    catalog.job_type,
    catalog.transport_type,
    catalog.adapter_type,
    catalog.provider_slug,
    catalog.model_slug,
    catalog.reason_code,
    catalog.source_refs,
    policy.default_posture,
    policy.operator_message,
    policy.decision_ref,
    catalog.projected_at,
    catalog.projection_ref
FROM private_provider_job_catalog AS catalog
LEFT JOIN private_provider_transport_control_policy AS policy
  ON policy.runtime_profile_ref = catalog.runtime_profile_ref
 AND policy.transport_type = catalog.transport_type
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
