-- Migration 272: Model access control matrix.
--
-- Exposes the private instance control panel as a never-blank switchboard:
-- every active task type crossed with every active provider/model access
-- method gets an effective ON/OFF answer and a reason trail.

BEGIN;

CREATE OR REPLACE VIEW private_model_access_control_matrix AS
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
        api_allowlist.allowed AS api_allowed,
        api_allowlist.reason_code AS api_reason_code,
        api_allowlist.decision_ref AS api_decision_ref,
        access_denial.denied AS access_denied,
        access_denial.reason_code AS access_denial_reason_code,
        access_denial.operator_message AS access_denial_operator_message,
        access_denial.decision_ref AS access_denial_decision_ref,
        access_denial.control_scope AS access_denial_scope
    FROM matrix
    LEFT JOIN private_provider_transport_control_policy AS transport_policy
      ON transport_policy.runtime_profile_ref = matrix.runtime_profile_ref
     AND transport_policy.transport_type = matrix.transport_type
    LEFT JOIN private_provider_api_job_allowlist AS api_allowlist
      ON api_allowlist.runtime_profile_ref = matrix.runtime_profile_ref
     AND api_allowlist.job_type = matrix.job_type
     AND api_allowlist.adapter_type = matrix.adapter_type
     AND api_allowlist.provider_slug = matrix.provider_slug
     AND api_allowlist.model_slug = matrix.model_slug
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
        THEN 'task/provider/model/access_method_allowlist'
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
        THEN COALESCE(api_reason_code, 'private_api_job_policy.allowed')
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
        'table.private_provider_api_job_allowlist',
        'table.private_provider_model_access_denials'
    ) AS source_refs,
    now() AS projected_at,
    'projection.private_model_access_control_matrix'::text AS projection_ref
FROM resolved;

COMMENT ON VIEW private_model_access_control_matrix IS
    'CQRS control-panel switchboard: every active task type crossed with every active provider/model access method has a non-null effective ON/OFF state and reason trail.';

CREATE OR REPLACE FUNCTION refresh_private_provider_job_catalog(
    p_runtime_profile_ref TEXT
) RETURNS VOID AS $$
BEGIN
    IF p_runtime_profile_ref IS NULL OR btrim(p_runtime_profile_ref) = '' THEN
        RAISE EXCEPTION 'runtime_profile_ref must be a non-empty string';
    END IF;

    DELETE FROM private_provider_job_catalog
    WHERE runtime_profile_ref = btrim(p_runtime_profile_ref);

    WITH matrix_rows AS (
        SELECT
            control.runtime_profile_ref,
            control.job_type,
            control.transport_type,
            control.adapter_type,
            control.provider_slug,
            control.model_slug,
            control.model_version,
            control.cost_structure,
            control.cost_metadata,
            CASE
                WHEN control.control_enabled IS NOT TRUE THEN 'disabled'
                WHEN admitted.candidate_ref IS NULL THEN 'disabled'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'disabled'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'disabled'
                WHEN transport.status <> 'active' THEN 'disabled'
                ELSE 'available'
            END AS availability_state,
            CASE
                WHEN control.control_enabled IS NOT TRUE
                THEN control.control_reason_code
                WHEN admitted.candidate_ref IS NULL THEN 'runtime_profile_route.not_admitted'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'provider_transport.missing'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'provider_transport.policy_denied'
                WHEN transport.status <> 'active' THEN 'provider_transport.disabled'
                ELSE 'catalog.available'
            END AS reason_code,
            control.candidate_ref,
            control.provider_ref,
            (
                control.source_refs
                || jsonb_build_array(
                    'projection.private_model_access_control_matrix',
                    'table.runtime_profile_admitted_routes',
                    'table.provider_transport_admissions'
                )
            ) AS source_refs
        FROM private_model_access_control_matrix AS control
        LEFT JOIN runtime_profile_admitted_routes AS admitted
          ON admitted.runtime_profile_ref = control.runtime_profile_ref
         AND admitted.candidate_ref = control.candidate_ref
         AND admitted.eligibility_status = 'admitted'
        LEFT JOIN provider_transport_admissions AS transport
          ON transport.provider_slug = control.provider_slug
         AND transport.adapter_type = control.adapter_type
        WHERE control.runtime_profile_ref = btrim(p_runtime_profile_ref)
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

CREATE OR REPLACE FUNCTION refresh_private_model_access_projection_profiles()
RETURNS TRIGGER AS $$
DECLARE
    profile_ref text;
BEGIN
    FOR profile_ref IN
        SELECT runtime_profile_ref
        FROM registry_native_runtime_profile_authority
    LOOP
        PERFORM refresh_private_provider_job_catalog(profile_ref);
    END LOOP;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_refresh_model_access_task_type_profiles ON task_type_profiles;
CREATE TRIGGER trg_refresh_model_access_task_type_profiles
    AFTER INSERT OR UPDATE OR DELETE ON task_type_profiles
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

DROP TRIGGER IF EXISTS trg_refresh_model_access_task_type_routing ON task_type_routing;
CREATE TRIGGER trg_refresh_model_access_task_type_routing
    AFTER INSERT OR UPDATE OR DELETE ON task_type_routing
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

DROP TRIGGER IF EXISTS trg_refresh_model_access_provider_model_candidates ON provider_model_candidates;
CREATE TRIGGER trg_refresh_model_access_provider_model_candidates
    AFTER INSERT OR UPDATE OR DELETE ON provider_model_candidates
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

DROP TRIGGER IF EXISTS trg_refresh_model_access_provider_cli_profiles ON provider_cli_profiles;
CREATE TRIGGER trg_refresh_model_access_provider_cli_profiles
    AFTER INSERT OR UPDATE OR DELETE ON provider_cli_profiles
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

DROP TRIGGER IF EXISTS trg_refresh_model_access_api_allowlist ON private_provider_api_job_allowlist;
CREATE TRIGGER trg_refresh_model_access_api_allowlist
    AFTER INSERT OR UPDATE OR DELETE ON private_provider_api_job_allowlist
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

DROP TRIGGER IF EXISTS trg_refresh_model_access_transport_policy ON private_provider_transport_control_policy;
CREATE TRIGGER trg_refresh_model_access_transport_policy
    AFTER INSERT OR UPDATE OR DELETE ON private_provider_transport_control_policy
    FOR EACH STATEMENT EXECUTE FUNCTION refresh_private_model_access_projection_profiles();

DROP TRIGGER IF EXISTS trg_refresh_model_access_denials ON private_provider_model_access_denials;
CREATE TRIGGER trg_refresh_model_access_denials
    AFTER INSERT OR UPDATE OR DELETE ON private_provider_model_access_denials
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

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'table:private_model_access_control_matrix',
    'Private model access control matrix',
    'projection',
    'Never-blank CQRS switchboard for model access by task type, CLI/API transport, adapter, provider, and model slug.',
    jsonb_build_object(
        'source', 'migration.272_model_access_control_matrix',
        'view', 'private_model_access_control_matrix'
    ),
    jsonb_build_object(
        'projection_ref', 'projection.private_model_access_control_matrix',
        'source_ref', 'provider/task/access control policy tables',
        'operation_name', 'operator.model_access_control_matrix'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_entries (
    object_kind,
    field_path,
    source,
    field_kind,
    label,
    description,
    required,
    default_value,
    valid_values,
    examples,
    deprecation_notes,
    display_order,
    origin_ref,
    metadata
) VALUES
('table:private_model_access_control_matrix', 'job_type', 'operator', 'text', 'Task type', 'Task type receiving this model access control decision.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 10, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'transport_type', 'operator', 'enum', 'Transport type', 'Connection method family for this access method.', true, NULL, '["CLI","API"]'::jsonb, '[]'::jsonb, '', 20, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'adapter_type', 'operator', 'enum', 'Adapter type', 'Runtime adapter/access method implementation.', true, NULL, '["cli_llm","llm_task"]'::jsonb, '[]'::jsonb, '', 30, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'access_method', 'operator', 'text', 'Access method', 'Human-readable transport and adapter pair, for example CLI:cli_llm or API:llm_task.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 40, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'provider_slug', 'operator', 'text', 'Provider slug', 'Provider slug governed by this control decision.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 50, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'model_slug', 'operator', 'text', 'Model slug', 'Model slug governed by this control decision.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 60, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'control_enabled', 'operator', 'boolean', 'Control enabled', 'True means the control panel currently allows this model access method for this task type.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 70, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'control_state', 'operator', 'enum', 'Control state', 'Never-blank ON/OFF state derived from the control panel.', true, NULL, '["on","off"]'::jsonb, '[]'::jsonb, '', 80, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'control_scope', 'operator', 'text', 'Control scope', 'Whether the state came from exact task/provider/model policy, wildcard denial, transport default deny, or transport default allow.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 90, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'control_is_explicit', 'operator', 'boolean', 'Explicit control', 'True when an exact allowlist row or explicit denial row set this state; false when inherited from transport default posture.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 100, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'control_reason_code', 'operator', 'text', 'Control reason code', 'Machine-readable reason explaining why the control state is on or off.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 110, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'control_decision_ref', 'operator', 'text', 'Control decision ref', 'Durable decision or policy reference responsible for this effective state.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 120, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb),
('table:private_model_access_control_matrix', 'control_operator_message', 'operator', 'text', 'Control operator message', 'Operator-facing instruction shown to agents when this access method is disabled.', true, NULL, '[]'::jsonb, '[]'::jsonb, '', 130, '{"source":"migration.272_model_access_control_matrix"}'::jsonb, '{}'::jsonb)
ON CONFLICT (object_kind, field_path, source) DO UPDATE SET
    field_kind = EXCLUDED.field_kind,
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    required = EXCLUDED.required,
    default_value = EXCLUDED.default_value,
    valid_values = EXCLUDED.valid_values,
    examples = EXCLUDED.examples,
    deprecation_notes = EXCLUDED.deprecation_notes,
    display_order = EXCLUDED.display_order,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
) VALUES (
    'projection.public.private_model_access_control_matrix',
    'projection',
    'private_model_access_control_matrix',
    'public',
    'authority.provider_onboarding',
    'table:private_model_access_control_matrix',
    'active',
    'read_model',
    'praxis.engine',
    'decision.model_access_control_matrix.20260426',
    jsonb_build_object(
        'projection_ref', 'projection.private_model_access_control_matrix',
        'source_ref', 'provider/task/access control policy tables'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'operation.operator.model_access_control_matrix',
    'Model access control matrix query',
    'command',
    'CQRS query that exposes never-blank ON/OFF model access control by task type, provider, model slug, and connection method.',
    jsonb_build_object(
        'source', 'migration.272_model_access_control_matrix',
        'operation_name', 'operator.model_access_control_matrix',
        'operation_kind', 'query'
    ),
    jsonb_build_object(
        'operation_kind', 'query',
        'authority_domain_ref', 'authority.provider_onboarding',
        'projection_ref', 'projection.private_model_access_control_matrix',
        'handler_ref', 'runtime.operations.queries.operator_support.handle_query_model_access_control_matrix'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
) VALUES (
    'operation.operator.model_access_control_matrix',
    'command',
    'operator.model_access_control_matrix',
    NULL,
    'authority.provider_onboarding',
    'operation.operator.model_access_control_matrix',
    'active',
    'read_model',
    'praxis.engine',
    'decision.model_access_control_matrix.20260426',
    jsonb_build_object(
        'handler_ref', 'runtime.operations.queries.operator_support.handle_query_model_access_control_matrix',
        'source_kind', 'operation_query'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO operation_catalog_registry (
    operation_ref,
    operation_name,
    source_kind,
    operation_kind,
    http_method,
    http_path,
    input_model_ref,
    handler_ref,
    authority_ref,
    authority_domain_ref,
    projection_ref,
    posture,
    idempotency_policy,
    enabled,
    binding_revision,
    decision_ref,
    input_schema_ref,
    output_schema_ref,
    storage_target_ref,
    receipt_required,
    event_required,
    projection_freshness_policy_ref
) VALUES (
    'operator-model-access-control-matrix',
    'operator.model_access_control_matrix',
    'operation_query',
    'query',
    'GET',
    '/api/operator/model-access-control-matrix',
    'runtime.operations.queries.operator_support.QueryModelAccessControlMatrix',
    'runtime.operations.queries.operator_support.handle_query_model_access_control_matrix',
    'authority.provider_onboarding',
    'authority.provider_onboarding',
    'projection.private_model_access_control_matrix',
    'observe',
    'read_only',
    TRUE,
    'binding.operation_catalog_registry.model_access_control_matrix.20260426',
    'decision.model_access_control_matrix.20260426',
    'runtime.operations.queries.operator_support.QueryModelAccessControlMatrix',
    'operation.output.default',
    'praxis.primary_postgres',
    TRUE,
    FALSE,
    'projection_freshness.default'
)
ON CONFLICT (operation_ref) DO UPDATE SET
    operation_name = EXCLUDED.operation_name,
    source_kind = EXCLUDED.source_kind,
    operation_kind = EXCLUDED.operation_kind,
    http_method = EXCLUDED.http_method,
    http_path = EXCLUDED.http_path,
    input_model_ref = EXCLUDED.input_model_ref,
    handler_ref = EXCLUDED.handler_ref,
    authority_ref = EXCLUDED.authority_ref,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    projection_ref = EXCLUDED.projection_ref,
    posture = EXCLUDED.posture,
    idempotency_policy = EXCLUDED.idempotency_policy,
    enabled = EXCLUDED.enabled,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    input_schema_ref = EXCLUDED.input_schema_ref,
    output_schema_ref = EXCLUDED.output_schema_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    receipt_required = EXCLUDED.receipt_required,
    event_required = EXCLUDED.event_required,
    projection_freshness_policy_ref = EXCLUDED.projection_freshness_policy_ref,
    updated_at = now();

COMMIT;
