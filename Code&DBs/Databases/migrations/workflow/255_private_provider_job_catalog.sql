-- Migration 255: private provider job catalog.
--
-- This is the operator-facing catalog for "what can this instance actually
-- use for this job?" Disabled lanes stay in the backing projection for audit,
-- but consumers read effective_private_provider_job_catalog so disabled
-- capability mechanically disappears from runtime/tool surfaces.

BEGIN;

CREATE TABLE IF NOT EXISTS private_provider_job_catalog (
    runtime_profile_ref TEXT NOT NULL CHECK (btrim(runtime_profile_ref) <> ''),
    job_type TEXT NOT NULL CHECK (btrim(job_type) <> ''),
    transport_type TEXT NOT NULL CHECK (transport_type IN ('CLI', 'API')),
    adapter_type TEXT NOT NULL CHECK (adapter_type IN ('cli_llm', 'llm_task')),
    provider_slug TEXT NOT NULL CHECK (btrim(provider_slug) <> ''),
    model_slug TEXT NOT NULL CHECK (btrim(model_slug) <> ''),
    model_version TEXT NOT NULL DEFAULT '',
    cost_structure TEXT NOT NULL CHECK (btrim(cost_structure) <> ''),
    cost_metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(cost_metadata) = 'object'),
    availability_state TEXT NOT NULL CHECK (availability_state IN ('available', 'disabled')),
    reason_code TEXT NOT NULL CHECK (btrim(reason_code) <> ''),
    candidate_ref TEXT,
    provider_ref TEXT,
    source_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(source_refs) = 'array'),
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    projection_ref TEXT NOT NULL DEFAULT 'projection.private_provider_job_catalog',
    PRIMARY KEY (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
);

CREATE INDEX IF NOT EXISTS private_provider_job_catalog_effective_idx
    ON private_provider_job_catalog (runtime_profile_ref, job_type, transport_type, provider_slug, model_slug)
    WHERE availability_state = 'available';

CREATE INDEX IF NOT EXISTS private_provider_job_catalog_model_idx
    ON private_provider_job_catalog (provider_slug, model_slug, transport_type, availability_state);

CREATE OR REPLACE VIEW effective_private_provider_job_catalog AS
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
    reason_code,
    candidate_ref,
    provider_ref,
    source_refs,
    projected_at,
    projection_ref
FROM private_provider_job_catalog
WHERE availability_state = 'available';

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
                WHEN admitted.candidate_ref IS NULL THEN 'disabled'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'disabled'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'disabled'
                WHEN transport.status <> 'active' THEN 'disabled'
                ELSE 'available'
            END AS availability_state,
            CASE
                WHEN route.permitted IS NOT TRUE THEN 'task_type_routing.denied'
                WHEN active_candidates.candidate_ref IS NULL THEN 'provider_model_candidate.missing'
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
                'table.provider_cli_profiles'
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
        LEFT JOIN provider_transport_admissions AS transport
          ON transport.provider_slug = route.provider_slug
         AND transport.adapter_type = economics.adapter_type
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
END;
$$ LANGUAGE plpgsql;

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
) VALUES
(
    'table.public.private_provider_job_catalog',
    'table',
    'private_provider_job_catalog',
    'public',
    'authority.provider_onboarding',
    'private_provider_job_catalog',
    'active',
    'projection',
    'praxis.engine',
    'decision.private_provider_job_catalog.20260426',
    jsonb_build_object(
        'projection_ref', 'projection.private_provider_job_catalog',
        'effective_view_ref', 'view.public.effective_private_provider_job_catalog',
        'instance_scope', 'private'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    object_kind = EXCLUDED.object_kind,
    object_name = EXCLUDED.object_name,
    schema_name = EXCLUDED.schema_name,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_projection_registry (
    projection_ref,
    authority_domain_ref,
    source_event_stream_ref,
    reducer_ref,
    storage_target_ref,
    freshness_policy_ref,
    enabled,
    decision_ref
) VALUES (
    'projection.private_provider_job_catalog',
    'authority.provider_onboarding',
    'stream.provider_onboarding',
    'function.refresh_private_provider_job_catalog',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.private_provider_job_catalog.20260426'
)
ON CONFLICT (projection_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    source_event_stream_ref = EXCLUDED.source_event_stream_ref,
    reducer_ref = EXCLUDED.reducer_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    freshness_policy_ref = EXCLUDED.freshness_policy_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'private_provider_job_catalog',
    'Private provider job catalog',
    'projection',
    'Private-instance read model matrixing job type, CLI/API transport, provider, model, model version, and cost structure. Consumers read the effective view so disabled capability disappears mechanically.',
    jsonb_build_object(
        'source', 'migration.255_private_provider_job_catalog',
        'projection_ref', 'projection.private_provider_job_catalog'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.provider_onboarding',
        'read_model_object_ref', 'table.public.private_provider_job_catalog',
        'effective_read_model_ref', 'view.public.effective_private_provider_job_catalog'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMENT ON TABLE private_provider_job_catalog IS
    'Private-instance provider/model matrix by job type, transport, cost structure, and version. Disabled rows are audit-only; runtime consumers use effective_private_provider_job_catalog.';

COMMENT ON VIEW effective_private_provider_job_catalog IS
    'Mechanical capability surface for private provider/model routing. Disabled catalog rows are absent from this view.';

COMMIT;
