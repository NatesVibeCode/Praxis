-- Migration 287: Routing IS the API admission authority.
--
-- Step 2 of the routing-as-source-of-truth collapse (286 added the columns;
-- 287 promotes data and rewrites the matrix view).
--
-- Three actions in one transaction:
--
--   (1) Promote every allowed private_provider_api_job_allowlist row into
--       a task_type_routing row with transport_type='API'. The allowlist's
--       per-runtime-profile granularity collapses (unique by job_type +
--       provider + model — runtime-profile differences are still handled
--       via runtime_profile_admitted_routes downstream). After this step
--       there is exactly one place that says "API is allowed for this
--       (task, provider, model)": a routing row.
--
--   (2) DROP and recreate private_model_access_control_matrix. The
--       api_allowlist join is replaced with a task_type_routing join
--       filtered to transport_type='API'. The view's output schema is
--       identical (column names, control_state, control_reason_code,
--       control_decision_ref, source_refs, projection_ref) — every
--       downstream query (compiler_llm.resolve_matrix_gated_routes,
--       operator_support.handle_query_model_access_control_matrix,
--       provider_control_plane_repository) keeps working unchanged.
--
--   (3) DROP task_type_routing_admission_audit. It existed to surface the
--       gap between routing and allowlist. With one source of truth, the
--       gap is structurally impossible — every routing row IS its own
--       admission. The view's only purpose is gone.
--
-- The allowlist table itself is intentionally NOT dropped here. It still
-- exists, still has rows, but no consumer reads it — its triggers fire
-- harmlessly on writes. Operator can ALTER TABLE ... RENAME or DROP
-- TABLE in a follow-up once the dust settles. This is the reversible
-- shape: data is preserved, downstream consumers are migrated, the
-- redundant table sits idle.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- (1) Promote allowlist rows into routing as transport_type='API'.
-- ──────────────────────────────────────────────────────────────────────────

-- Drop the refresh trigger temporarily — the bulk INSERT below would
-- otherwise refresh the projection per-row before the matrix view is
-- rewritten. Re-attached at end of transaction; final refresh happens
-- after the view rewrite lands.
DROP TRIGGER IF EXISTS trg_refresh_model_access_task_type_routing ON task_type_routing;

INSERT INTO task_type_routing (
    task_type,
    sub_task_type,
    model_slug,
    provider_slug,
    transport_type,
    permitted,
    rank,
    benchmark_score,
    benchmark_name,
    cost_per_m_tokens,
    max_concurrent,
    rationale,
    updated_at
)
SELECT DISTINCT
    A.job_type        AS task_type,
    '*'               AS sub_task_type,
    A.model_slug      AS model_slug,
    A.provider_slug   AS provider_slug,
    'API'             AS transport_type,
    TRUE              AS permitted,
    1                 AS rank,
    0                 AS benchmark_score,
    ''                AS benchmark_name,
    0                 AS cost_per_m_tokens,
    5                 AS max_concurrent,
    'Promoted from private_provider_api_job_allowlist via migration 287; allowlist is no longer the API admission source-of-truth.' AS rationale,
    now()             AS updated_at
FROM private_provider_api_job_allowlist AS A
WHERE A.allowed = TRUE
ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO NOTHING;

-- ──────────────────────────────────────────────────────────────────────────
-- (2) Rewrite the access matrix view: routing replaces allowlist.
-- ──────────────────────────────────────────────────────────────────────────

DROP VIEW IF EXISTS provider_transport_gate_denials CASCADE;
DROP VIEW IF EXISTS private_model_access_control_matrix CASCADE;

CREATE VIEW private_model_access_control_matrix AS
WITH runtime_profiles AS (
    SELECT runtime_profile_ref
    FROM registry_native_runtime_profile_authority
),
task_types AS (
    SELECT task_type
    FROM task_type_profiles
    WHERE COALESCE(status, 'active') = 'active'
    UNION
    SELECT DISTINCT task_type
    FROM task_type_routing
),
active_candidates AS (
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
access_methods AS (
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
matrix AS (
    SELECT
        runtime_profiles.runtime_profile_ref,
        task_types.task_type AS job_type,
        access_methods.transport_type,
        access_methods.adapter_type,
        active_candidates.provider_slug,
        active_candidates.model_slug,
        COALESCE(
            NULLIF(active_candidates.default_parameters ->> 'model_version', ''),
            NULLIF(active_candidates.default_parameters ->> 'version', ''),
            active_candidates.model_slug
        ) AS model_version,
        access_methods.cost_structure,
        access_methods.cost_metadata,
        active_candidates.candidate_ref,
        active_candidates.provider_ref
    FROM runtime_profiles
    CROSS JOIN task_types
    JOIN active_candidates ON TRUE
    JOIN access_methods
      ON access_methods.provider_slug = active_candidates.provider_slug
),
resolved AS (
    SELECT
        matrix.*,
        COALESCE(transport_policy.default_posture, 'allow_unless_disabled') AS transport_default_posture,
        COALESCE(
            transport_policy.reason_code,
            'control_panel.transport_policy_default'
        ) AS transport_reason_code,
        COALESCE(
            transport_policy.operator_message,
            'this Model Access method has been turned off on purpose at the control panel either for this specific task type, or more broadly, consult the control panel and do not turn it on without confirming with the user even if you think that will help you complete your task.'
        ) AS transport_operator_message,
        COALESCE(
            transport_policy.decision_ref,
            'decision.model_access_control.default_transport_policy'
        ) AS transport_decision_ref,
        -- API admission: presence of a routing row with transport_type='API'
        -- and permitted=TRUE for the (task, provider, model) tuple.
        -- Replaces the previous private_provider_api_job_allowlist join
        -- as part of the routing-as-source-of-truth collapse (migration 287).
        api_routing_admission.allowed AS api_allowed,
        api_routing_admission.reason_code AS api_reason_code,
        api_routing_admission.decision_ref AS api_decision_ref,
        access_denial.denied AS access_denied,
        access_denial.reason_code AS access_denial_reason_code,
        access_denial.operator_message AS access_denial_operator_message,
        access_denial.decision_ref AS access_denial_decision_ref,
        access_denial.control_scope AS access_denial_scope
    FROM matrix
    LEFT JOIN private_provider_transport_control_policy AS transport_policy
      ON transport_policy.runtime_profile_ref = matrix.runtime_profile_ref
     AND transport_policy.transport_type = matrix.transport_type
    LEFT JOIN LATERAL (
        SELECT
            TRUE AS allowed,
            'task_type_routing.api_route_present' AS reason_code,
            'decision.task_type_routing.api_admission' AS decision_ref
        FROM task_type_routing AS api_route
        WHERE api_route.task_type = matrix.job_type
          AND api_route.provider_slug = matrix.provider_slug
          AND api_route.model_slug = matrix.model_slug
          AND api_route.transport_type = 'API'
          AND api_route.permitted IS TRUE
        LIMIT 1
    ) AS api_routing_admission ON matrix.transport_type = 'API'
    LEFT JOIN LATERAL (
        SELECT
            denial.*,
            concat_ws(
                '/',
                CASE WHEN denial.job_type = '*' THEN 'any_task' ELSE 'task' END,
                CASE WHEN denial.transport_type = '*' THEN 'any_transport' ELSE 'transport' END,
                CASE WHEN denial.adapter_type = '*' THEN 'any_adapter' ELSE 'adapter' END,
                CASE WHEN denial.provider_slug = '*' THEN 'any_provider' ELSE 'provider' END,
                CASE WHEN denial.model_slug = '*' THEN 'any_model' ELSE 'model' END
            ) AS control_scope
        FROM private_provider_model_access_denials AS denial
        WHERE denial.runtime_profile_ref = matrix.runtime_profile_ref
          AND denial.denied IS TRUE
          AND denial.job_type IN ('*', matrix.job_type)
          AND denial.transport_type IN ('*', matrix.transport_type)
          AND denial.adapter_type IN ('*', matrix.adapter_type)
          AND denial.provider_slug IN ('*', matrix.provider_slug)
          AND denial.model_slug IN ('*', matrix.model_slug)
        ORDER BY
            CASE WHEN denial.job_type = matrix.job_type THEN 16 ELSE 0 END
          + CASE WHEN denial.transport_type = matrix.transport_type THEN 8 ELSE 0 END
          + CASE WHEN denial.adapter_type = matrix.adapter_type THEN 4 ELSE 0 END
          + CASE WHEN denial.provider_slug = matrix.provider_slug THEN 2 ELSE 0 END
          + CASE WHEN denial.model_slug = matrix.model_slug THEN 1 ELSE 0 END DESC,
            denial.updated_at DESC
        LIMIT 1
    ) AS access_denial ON TRUE
)
SELECT
    runtime_profile_ref,
    job_type,
    transport_type,
    adapter_type,
    concat(transport_type, ':', adapter_type) AS access_method,
    provider_slug,
    model_slug,
    model_version,
    cost_structure,
    cost_metadata,
    CASE
        WHEN access_denied IS TRUE THEN false
        WHEN transport_default_posture = 'deny_unless_allowlisted'
        THEN COALESCE(api_allowed, false)
        ELSE true
    END AS control_enabled,
    CASE
        WHEN access_denied IS TRUE THEN 'off'
        WHEN transport_default_posture = 'deny_unless_allowlisted'
         AND COALESCE(api_allowed, false) IS NOT TRUE
        THEN 'off'
        ELSE 'on'
    END AS control_state,
    CASE
        WHEN access_denied IS TRUE
        THEN COALESCE(access_denial_scope, 'explicit_denial')
        WHEN transport_default_posture = 'deny_unless_allowlisted'
         AND COALESCE(api_allowed, false) IS TRUE
        THEN 'task_type_routing/api_route_present'
        WHEN transport_default_posture = 'deny_unless_allowlisted'
        THEN 'transport_default_deny'
        ELSE 'transport_default_allow'
    END AS control_scope,
    CASE
        WHEN access_denied IS TRUE THEN true
        WHEN transport_default_posture = 'deny_unless_allowlisted'
         AND COALESCE(api_allowed, false) IS TRUE
        THEN true
        ELSE false
    END AS control_is_explicit,
    CASE
        WHEN access_denied IS TRUE
        THEN COALESCE(access_denial_reason_code, 'control_panel.model_access_method_turned_off')
        WHEN transport_default_posture = 'deny_unless_allowlisted'
         AND COALESCE(api_allowed, false) IS TRUE
        THEN COALESCE(api_reason_code, 'task_type_routing.api_route_present')
        WHEN transport_default_posture = 'deny_unless_allowlisted'
        THEN COALESCE(transport_reason_code, 'control_panel.transport_turned_off')
        ELSE 'control_panel.transport_default_allowed'
    END AS control_reason_code,
    CASE
        WHEN access_denied IS TRUE
        THEN COALESCE(access_denial_operator_message, transport_operator_message)
        WHEN transport_default_posture = 'deny_unless_allowlisted'
         AND COALESCE(api_allowed, false) IS NOT TRUE
        THEN transport_operator_message
        ELSE 'this Model Access method is currently enabled by the control panel.'
    END AS control_operator_message,
    CASE
        WHEN access_denied IS TRUE
        THEN COALESCE(access_denial_decision_ref, transport_decision_ref)
        WHEN transport_default_posture = 'deny_unless_allowlisted'
         AND COALESCE(api_allowed, false) IS TRUE
        THEN COALESCE(api_decision_ref, transport_decision_ref)
        ELSE transport_decision_ref
    END AS control_decision_ref,
    candidate_ref,
    provider_ref,
    jsonb_build_array(
        'table.task_type_profiles',
        'table.task_type_routing',
        'table.provider_model_candidates',
        'table.provider_cli_profiles',
        'table.private_provider_transport_control_policy',
        'table.private_provider_model_access_denials'
    ) AS source_refs,
    now() AS projected_at,
    'projection.private_model_access_control_matrix'::text AS projection_ref
FROM resolved;

COMMENT ON VIEW private_model_access_control_matrix IS
    'CQRS control-panel switchboard: every active task type crossed with every active provider/model access method has a non-null effective ON/OFF state and reason trail. As of migration 287, API admission is derived from task_type_routing rows with transport_type=''API'' instead of the legacy private_provider_api_job_allowlist table.';

-- Recreate the gate-denials view that depended on the matrix view.
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
    control.control_scope AS control_scope_job_type,
    control.provider_slug AS control_scope_provider_slug,
    control.model_slug AS control_scope_model_slug,
    control.control_state AS default_posture,
    control.control_operator_message AS operator_message,
    control.control_decision_ref AS decision_ref,
    catalog.projected_at,
    catalog.projection_ref
FROM private_provider_job_catalog AS catalog
LEFT JOIN private_model_access_control_matrix AS control
  ON control.runtime_profile_ref = catalog.runtime_profile_ref
 AND control.job_type = catalog.job_type
 AND control.adapter_type = catalog.adapter_type
 AND control.provider_slug = catalog.provider_slug
 AND control.model_slug = catalog.model_slug
WHERE catalog.availability_state = 'disabled';

-- ──────────────────────────────────────────────────────────────────────────
-- (3) Drop the audit view — its reason for existing (gap between routing
--     and allowlist) is gone with one source of truth.
-- ──────────────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS task_type_routing_admission_audit CASCADE;

-- ──────────────────────────────────────────────────────────────────────────
-- Re-attach the routing trigger and refresh the projection per profile so
-- private_provider_job_catalog reflects the routing-derived admissions.
-- The effective view (effective_private_provider_job_catalog) is computed
-- live from the matrix view and doesn't need this refresh — but the
-- projection table is read directly by other consumers, so keeping it in
-- sync matters.
-- ──────────────────────────────────────────────────────────────────────────
CREATE TRIGGER trg_refresh_model_access_task_type_routing
    AFTER INSERT OR UPDATE OR DELETE ON task_type_routing
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

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

-- ──────────────────────────────────────────────────────────────────────────
-- Sentinel row so bootstrap can detect that 287 has actually applied.
-- The matrix view's name existed before 287 (migration 272 created it), so
-- a name-only existence check would skip 287 and leave the OLD api_allowlist-
-- derived definition in place. This row only gets inserted when 287 runs;
-- the manifest declares it as an expected_object, so bootstrap will keep
-- attempting 287 until the row appears.
INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES (
    'projection.private_model_access_control_matrix.routing_derived_v287',
    'Routing-derived API admission marker (migration 287)',
    'projection',
    'Sentinel row signaling that the access matrix view derives API admission from task_type_routing.transport_type=API rows instead of the legacy private_provider_api_job_allowlist. Used by bootstrap to detect that migration 287 has actually applied; the view''s name existed before 287, so a name-only check would skip the migration and leave the legacy definition in place.',
    jsonb_build_object('source', 'migration.287_routing_replaces_api_allowlist_as_admission_authority'),
    jsonb_build_object('marker_for', 'migration.287')
)
ON CONFLICT (object_kind) DO UPDATE SET
    summary = EXCLUDED.summary,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;

-- Verification (run manually after apply):
--   SELECT task_type, sub_task_type, provider_slug, model_slug, transport_type, permitted, rank
--     FROM task_type_routing
--    WHERE transport_type = 'API'
--    ORDER BY task_type, provider_slug;
--
--   SELECT job_type, transport_type, provider_slug, model_slug, control_state, control_reason_code
--     FROM private_model_access_control_matrix
--    WHERE runtime_profile_ref = 'praxis'
--      AND transport_type = 'API'
--      AND control_state = 'on'
--    ORDER BY job_type, provider_slug
--    LIMIT 20;
