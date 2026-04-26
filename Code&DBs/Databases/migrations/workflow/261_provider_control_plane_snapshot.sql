-- Migration 261: Durable circuit-breaker projection and provider control-plane snapshot.
--
-- Provider capability control becomes one DB-backed read model instead of
-- query-time assembly across unrelated authorities. Breaker state is durable,
-- projected, and mechanically reflected in the effective provider catalog.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.circuit_breakers',
    'praxis.engine',
    'stream.provider_execution',
    'projection.circuit_breakers',
    'praxis.primary_postgres',
    TRUE,
    'decision.provider_control_plane_snapshot.20260426'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    current_projection_ref = EXCLUDED.current_projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS provider_circuit_breaker_state (
    provider_slug TEXT PRIMARY KEY CHECK (btrim(provider_slug) <> ''),
    runtime_state TEXT NOT NULL CHECK (runtime_state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    failure_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_threshold INTEGER NOT NULL DEFAULT 0,
    recovery_timeout_s DOUBLE PRECISION NOT NULL DEFAULT 0,
    half_open_max_calls INTEGER NOT NULL DEFAULT 1,
    last_failure_at TIMESTAMPTZ,
    opened_at TIMESTAMPTZ,
    half_open_after TIMESTAMPTZ,
    half_open_calls INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    projection_ref TEXT NOT NULL DEFAULT 'projection.circuit_breakers'
);

CREATE INDEX IF NOT EXISTS provider_circuit_breaker_state_runtime_idx
    ON provider_circuit_breaker_state (runtime_state, updated_at DESC);

CREATE OR REPLACE VIEW effective_provider_circuit_breaker_state AS
WITH latest_manual_override AS (
    SELECT DISTINCT ON (decision_scope_ref)
        lower(btrim(decision_scope_ref)) AS provider_slug,
        decision_kind,
        rationale,
        operator_decision_id,
        decision_key,
        decision_status,
        decided_by,
        decision_source,
        effective_from,
        effective_to,
        updated_at
    FROM operator_decisions
    WHERE decision_kind IN (
            'circuit_breaker_force_open',
            'circuit_breaker_force_closed'
      )
      AND decision_scope_kind = 'provider'
      AND decision_scope_ref IS NOT NULL
      AND effective_from <= now()
      AND (effective_to IS NULL OR effective_to > now())
    ORDER BY decision_scope_ref, effective_from DESC, decided_at DESC, created_at DESC, operator_decision_id DESC
),
all_provider_slugs AS (
    SELECT provider_slug FROM provider_circuit_breaker_state
    UNION
    SELECT provider_slug FROM latest_manual_override
)
SELECT
    slugs.provider_slug,
    COALESCE(state.runtime_state, 'CLOSED') AS runtime_state,
    COALESCE(
        CASE latest.decision_kind
            WHEN 'circuit_breaker_force_open' THEN 'OPEN'
            WHEN 'circuit_breaker_force_closed' THEN 'CLOSED'
            ELSE NULL
        END,
        state.runtime_state,
        'CLOSED'
    ) AS effective_state,
    CASE latest.decision_kind
        WHEN 'circuit_breaker_force_open' THEN 'OPEN'
        WHEN 'circuit_breaker_force_closed' THEN 'CLOSED'
        ELSE NULL
    END AS manual_override_state,
    NULLIF(latest.rationale, '') AS manual_override_reason,
    COALESCE(state.failure_count, 0) AS failure_count,
    COALESCE(state.success_count, 0) AS success_count,
    COALESCE(state.failure_threshold, 0) AS failure_threshold,
    COALESCE(state.recovery_timeout_s, 0) AS recovery_timeout_s,
    COALESCE(state.half_open_max_calls, 1) AS half_open_max_calls,
    state.last_failure_at,
    state.opened_at,
    state.half_open_after,
    COALESCE(state.half_open_calls, 0) AS half_open_calls,
    latest.operator_decision_id,
    latest.decision_key,
    latest.decision_status,
    latest.decided_by,
    latest.decision_source,
    latest.effective_from,
    latest.effective_to,
    COALESCE(state.updated_at, latest.updated_at, now()) AS updated_at,
    COALESCE(state.projected_at, latest.updated_at, now()) AS projected_at,
    COALESCE(state.projection_ref, 'projection.circuit_breakers') AS projection_ref
FROM all_provider_slugs AS slugs
LEFT JOIN provider_circuit_breaker_state AS state
  ON state.provider_slug = slugs.provider_slug
LEFT JOIN latest_manual_override AS latest
  ON latest.provider_slug = slugs.provider_slug;

CREATE TABLE IF NOT EXISTS private_provider_control_plane_snapshot (
    runtime_profile_ref TEXT NOT NULL CHECK (btrim(runtime_profile_ref) <> ''),
    job_type TEXT NOT NULL CHECK (btrim(job_type) <> ''),
    transport_type TEXT NOT NULL CHECK (transport_type IN ('CLI', 'API')),
    adapter_type TEXT NOT NULL CHECK (adapter_type IN ('cli_llm', 'llm_task')),
    provider_slug TEXT NOT NULL CHECK (btrim(provider_slug) <> ''),
    model_slug TEXT NOT NULL CHECK (btrim(model_slug) <> ''),
    model_version TEXT NOT NULL DEFAULT '',
    cost_structure TEXT NOT NULL CHECK (btrim(cost_structure) <> ''),
    cost_metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(cost_metadata) = 'object'),
    credential_availability_state TEXT NOT NULL DEFAULT 'unknown'
        CHECK (credential_availability_state IN ('available', 'missing', 'not_required', 'unknown')),
    credential_sources JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(credential_sources) = 'array'),
    credential_observations JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(credential_observations) = 'array'),
    capability_state TEXT NOT NULL CHECK (capability_state IN ('runnable', 'removed')),
    is_runnable BOOLEAN NOT NULL,
    breaker_state TEXT NOT NULL CHECK (breaker_state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    manual_override_state TEXT,
    primary_removal_reason_code TEXT,
    removal_reasons JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(removal_reasons) = 'array'),
    candidate_ref TEXT,
    provider_ref TEXT,
    source_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(source_refs) = 'array'),
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    projection_ref TEXT NOT NULL DEFAULT 'projection.private_provider_control_plane_snapshot',
    PRIMARY KEY (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
);

CREATE INDEX IF NOT EXISTS private_provider_control_plane_snapshot_effective_idx
    ON private_provider_control_plane_snapshot (runtime_profile_ref, job_type, transport_type, provider_slug, model_slug)
    WHERE is_runnable;

CREATE INDEX IF NOT EXISTS private_provider_control_plane_snapshot_provider_idx
    ON private_provider_control_plane_snapshot (provider_slug, model_slug, breaker_state, capability_state);

CREATE TABLE IF NOT EXISTS private_provider_api_job_allowlist (
    runtime_profile_ref TEXT NOT NULL CHECK (btrim(runtime_profile_ref) <> ''),
    job_type TEXT NOT NULL CHECK (btrim(job_type) <> ''),
    adapter_type TEXT NOT NULL DEFAULT 'llm_task' CHECK (adapter_type = 'llm_task'),
    provider_slug TEXT NOT NULL CHECK (btrim(provider_slug) <> ''),
    model_slug TEXT NOT NULL CHECK (btrim(model_slug) <> ''),
    allowed BOOLEAN NOT NULL DEFAULT TRUE,
    reason_code TEXT NOT NULL DEFAULT 'private_api_job_policy.allowed' CHECK (btrim(reason_code) <> ''),
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
);

CREATE INDEX IF NOT EXISTS private_provider_api_job_allowlist_runtime_job_idx
    ON private_provider_api_job_allowlist (runtime_profile_ref, job_type, allowed);

CREATE OR REPLACE VIEW effective_private_provider_control_plane AS
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
    credential_availability_state,
    credential_sources,
    credential_observations,
    capability_state,
    is_runnable,
    breaker_state,
    manual_override_state,
    primary_removal_reason_code,
    removal_reasons,
    candidate_ref,
    provider_ref,
    source_refs,
    projected_at,
    projection_ref
FROM private_provider_control_plane_snapshot
WHERE is_runnable;

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
    COALESCE(primary_removal_reason_code, 'catalog.available') AS reason_code,
    candidate_ref,
    provider_ref,
    source_refs,
    projected_at,
    projection_ref
FROM effective_private_provider_control_plane;

CREATE OR REPLACE FUNCTION refresh_private_provider_control_plane_snapshot(
    p_runtime_profile_ref TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    normalized_runtime_profile_ref TEXT;
BEGIN
    normalized_runtime_profile_ref := NULLIF(btrim(p_runtime_profile_ref), '');

    DELETE FROM private_provider_control_plane_snapshot
    WHERE normalized_runtime_profile_ref IS NULL
       OR runtime_profile_ref = normalized_runtime_profile_ref;

    WITH catalog_rows AS (
        SELECT *
        FROM private_provider_job_catalog
        WHERE normalized_runtime_profile_ref IS NULL
           OR runtime_profile_ref = normalized_runtime_profile_ref
    ),
    breaker_rows AS (
        SELECT *
        FROM effective_provider_circuit_breaker_state
    ),
    latest_credential_observations AS (
        SELECT DISTINCT ON (lower(btrim(details ->> 'provider_slug')), subject_id)
            lower(btrim(details ->> 'provider_slug')) AS provider_slug,
            subject_id,
            status,
            summary,
            details,
            captured_at
        FROM heartbeat_probe_snapshots
        WHERE probe_kind = 'credential_expiry'
          AND details ? 'provider_slug'
          AND NULLIF(btrim(details ->> 'provider_slug'), '') IS NOT NULL
        ORDER BY lower(btrim(details ->> 'provider_slug')), subject_id, captured_at DESC
    ),
    credential_rows AS (
        SELECT
            provider_slug,
            bool_or(status = 'ok') AS has_available_credential,
            bool_or(status IN ('failed', 'degraded')) AS has_missing_credential,
            jsonb_agg(
                jsonb_build_object(
                    'credential_ref', subject_id,
                    'status', status,
                    'summary', summary,
                    'source_kind', details ->> 'source_kind',
                    'captured_at', captured_at
                )
                ORDER BY captured_at DESC, subject_id
            ) AS credential_observations
        FROM latest_credential_observations
        GROUP BY provider_slug
    ),
    projected_base AS (
        SELECT
            catalog.runtime_profile_ref,
            catalog.job_type,
            catalog.transport_type,
            catalog.adapter_type,
            catalog.provider_slug,
            catalog.model_slug,
            catalog.model_version,
            catalog.cost_structure,
            catalog.cost_metadata,
            CASE
                WHEN COALESCE(transport.credential_sources, '[]'::jsonb) ? 'ambient_cli_session'
                THEN 'available'
                WHEN jsonb_array_length(COALESCE(transport.credential_sources, '[]'::jsonb)) = 0
                THEN 'unknown'
                WHEN COALESCE(credentials.has_available_credential, false)
                THEN 'available'
                WHEN COALESCE(credentials.has_missing_credential, false)
                THEN 'missing'
                ELSE 'unknown'
            END AS credential_availability_state,
            COALESCE(transport.credential_sources, '[]'::jsonb) AS credential_sources,
            COALESCE(credentials.credential_observations, '[]'::jsonb) AS credential_observations,
            catalog.availability_state,
            catalog.reason_code,
            COALESCE(breaker.effective_state, 'CLOSED') AS effective_breaker_state,
            NULLIF(breaker.manual_override_state, '') AS manual_override_state,
            breaker.manual_override_reason,
            catalog.candidate_ref,
            catalog.provider_ref,
            catalog.source_refs
        FROM catalog_rows AS catalog
        LEFT JOIN breaker_rows AS breaker
          ON breaker.provider_slug = catalog.provider_slug
        LEFT JOIN provider_transport_admissions AS transport
          ON transport.provider_slug = catalog.provider_slug
         AND transport.adapter_type = catalog.adapter_type
        LEFT JOIN credential_rows AS credentials
          ON credentials.provider_slug = catalog.provider_slug
    ),
    projected_rows AS (
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
            credential_availability_state,
            credential_sources,
            credential_observations,
            CASE
                WHEN availability_state = 'available'
                 AND effective_breaker_state <> 'OPEN'
                 AND credential_availability_state <> 'missing'
                THEN 'runnable'
                ELSE 'removed'
            END AS capability_state,
            (
                availability_state = 'available'
                AND effective_breaker_state <> 'OPEN'
                AND credential_availability_state <> 'missing'
            ) AS is_runnable,
            effective_breaker_state AS breaker_state,
            manual_override_state,
            CASE
                WHEN availability_state <> 'available' THEN reason_code
                WHEN effective_breaker_state = 'OPEN' THEN 'circuit_breaker.open'
                WHEN credential_availability_state = 'missing' THEN 'credential.missing'
                ELSE NULL
            END AS primary_removal_reason_code,
            (
                CASE
                    WHEN availability_state <> 'available'
                    THEN jsonb_build_array(
                        jsonb_build_object(
                            'reason_code', reason_code,
                            'source_ref', 'projection.private_provider_job_catalog',
                            'details', jsonb_build_object(
                                'availability_state', availability_state,
                                'adapter_type', adapter_type,
                                'transport_type', transport_type
                            )
                        )
                    )
                    ELSE '[]'::jsonb
                END
                ||
                CASE
                    WHEN effective_breaker_state = 'OPEN'
                    THEN jsonb_build_array(
                        jsonb_build_object(
                            'reason_code', 'circuit_breaker.open',
                            'source_ref', 'projection.circuit_breakers',
                            'details', jsonb_build_object(
                                'breaker_state', effective_breaker_state,
                                'manual_override_state', manual_override_state,
                                'manual_override_reason', manual_override_reason
                            )
                        )
                    )
                    ELSE '[]'::jsonb
                END
                ||
                CASE
                    WHEN credential_availability_state = 'missing'
                    THEN jsonb_build_array(
                        jsonb_build_object(
                            'reason_code', 'credential.missing',
                            'source_ref', 'projection.credential_availability',
                            'details', jsonb_build_object(
                                'credential_sources', credential_sources,
                                'credential_observations', credential_observations
                            )
                        )
                    )
                    ELSE '[]'::jsonb
                END
            ) AS removal_reasons,
            candidate_ref,
            provider_ref,
            (
                source_refs
                ||
                CASE
                    WHEN effective_breaker_state = 'OPEN'
                    THEN jsonb_build_array(
                        'table.provider_circuit_breaker_state',
                        'table.operator_decisions'
                    )
                    ELSE '[]'::jsonb
                END
                ||
                CASE
                    WHEN credential_availability_state <> 'unknown'
                    THEN jsonb_build_array(
                        'table.provider_transport_admissions',
                        'table.heartbeat_probe_snapshots'
                    )
                    ELSE '[]'::jsonb
                END
            ) AS source_refs
        FROM projected_base
    )
    INSERT INTO private_provider_control_plane_snapshot (
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
        model_version,
        cost_structure,
        cost_metadata,
        credential_availability_state,
        credential_sources,
        credential_observations,
        capability_state,
        is_runnable,
        breaker_state,
        manual_override_state,
        primary_removal_reason_code,
        removal_reasons,
        candidate_ref,
        provider_ref,
        source_refs,
        projected_at,
        projection_ref
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
        credential_availability_state,
        credential_sources,
        credential_observations,
        capability_state,
        is_runnable,
        breaker_state,
        manual_override_state,
        primary_removal_reason_code,
        removal_reasons,
        candidate_ref,
        provider_ref,
        source_refs,
        now(),
        'projection.private_provider_control_plane_snapshot'
    FROM projected_rows
    ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
    DO UPDATE SET
        transport_type = EXCLUDED.transport_type,
        model_version = EXCLUDED.model_version,
        cost_structure = EXCLUDED.cost_structure,
        cost_metadata = EXCLUDED.cost_metadata,
        credential_availability_state = EXCLUDED.credential_availability_state,
        credential_sources = EXCLUDED.credential_sources,
        credential_observations = EXCLUDED.credential_observations,
        capability_state = EXCLUDED.capability_state,
        is_runnable = EXCLUDED.is_runnable,
        breaker_state = EXCLUDED.breaker_state,
        manual_override_state = EXCLUDED.manual_override_state,
        primary_removal_reason_code = EXCLUDED.primary_removal_reason_code,
        removal_reasons = EXCLUDED.removal_reasons,
        candidate_ref = EXCLUDED.candidate_ref,
        provider_ref = EXCLUDED.provider_ref,
        source_refs = EXCLUDED.source_refs,
        projected_at = EXCLUDED.projected_at,
        projection_ref = EXCLUDED.projection_ref;

    INSERT INTO authority_projection_state (
        projection_ref,
        last_refreshed_at,
        freshness_status,
        error_code,
        error_detail,
        updated_at
    ) VALUES (
        'projection.private_provider_control_plane_snapshot',
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
END;
$$ LANGUAGE plpgsql;

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
                WHEN economics.transport_type = 'API'
                 AND COALESCE(api_policy.allowed, false) IS NOT TRUE THEN 'disabled'
                ELSE 'available'
            END AS availability_state,
            CASE
                WHEN route.permitted IS NOT TRUE THEN 'task_type_routing.denied'
                WHEN active_candidates.candidate_ref IS NULL THEN 'provider_model_candidate.missing'
                WHEN admitted.candidate_ref IS NULL THEN 'runtime_profile_route.not_admitted'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'provider_transport.missing'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'provider_transport.policy_denied'
                WHEN transport.status <> 'active' THEN 'provider_transport.disabled'
                WHEN economics.transport_type = 'API'
                 AND COALESCE(api_policy.allowed, false) IS NOT TRUE
                THEN COALESCE(api_policy.reason_code, 'private_api_job_policy.not_allowed')
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
                'table.private_provider_api_job_allowlist'
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
    'table.public.provider_circuit_breaker_state',
    'table',
    'provider_circuit_breaker_state',
    'public',
    'authority.circuit_breakers',
    'provider_circuit_breaker_state',
    'active',
    'projection',
    'praxis.engine',
    'decision.provider_control_plane_snapshot.20260426',
    jsonb_build_object(
        'projection_ref', 'projection.circuit_breakers',
        'effective_view_ref', 'view.public.effective_provider_circuit_breaker_state'
    )
),
(
    'table.public.private_provider_control_plane_snapshot',
    'table',
    'private_provider_control_plane_snapshot',
    'public',
    'authority.provider_onboarding',
    'private_provider_control_plane_snapshot',
    'active',
    'projection',
    'praxis.engine',
    'decision.provider_control_plane_snapshot.20260426',
    jsonb_build_object(
        'projection_ref', 'projection.private_provider_control_plane_snapshot',
        'effective_view_ref', 'view.public.effective_private_provider_control_plane',
        'compatibility_view_ref', 'view.public.effective_private_provider_job_catalog',
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
) VALUES
(
    'projection.circuit_breakers',
    'authority.circuit_breakers',
    'stream.provider_execution',
    'runtime.circuit_breaker.persist_provider_circuit_breaker_state',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.provider_control_plane_snapshot.20260426'
),
(
    'projection.private_provider_control_plane_snapshot',
    'authority.provider_onboarding',
    'stream.provider_onboarding',
    'function.refresh_private_provider_control_plane_snapshot',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.provider_control_plane_snapshot.20260426'
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

INSERT INTO authority_projection_contracts (
    projection_contract_ref,
    projection_ref,
    authority_domain_ref,
    source_ref_kind,
    source_ref,
    read_model_object_ref,
    freshness_policy_ref,
    last_event_required,
    last_receipt_required,
    failure_visibility_required,
    replay_supported,
    enabled,
    decision_ref,
    metadata
) VALUES
(
    'projection_contract.circuit_breakers',
    'projection.circuit_breakers',
    'authority.circuit_breakers',
    'table',
    'provider_circuit_breaker_state,operator_decisions',
    'table.public.provider_circuit_breaker_state',
    'projection_freshness.default',
    FALSE,
    FALSE,
    TRUE,
    TRUE,
    TRUE,
    'decision.provider_control_plane_snapshot.20260426',
    jsonb_build_object(
        'consumer', 'operator.circuit_states',
        'effective_view_ref', 'view.public.effective_provider_circuit_breaker_state'
    )
),
(
    'projection_contract.private_provider_control_plane_snapshot',
    'projection.private_provider_control_plane_snapshot',
    'authority.provider_onboarding',
    'table',
    'private_provider_job_catalog,provider_transport_admissions,heartbeat_probe_snapshots,provider_circuit_breaker_state,operator_decisions',
    'table.public.private_provider_control_plane_snapshot',
    'projection_freshness.default',
    FALSE,
    FALSE,
    TRUE,
    TRUE,
    TRUE,
    'decision.provider_control_plane_snapshot.20260426',
    jsonb_build_object(
        'consumer', 'operator.provider_control_plane',
        'effective_view_ref', 'view.public.effective_private_provider_control_plane',
        'compatibility_view_ref', 'view.public.effective_private_provider_job_catalog'
    )
)
ON CONFLICT (projection_ref) DO UPDATE SET
    projection_contract_ref = EXCLUDED.projection_contract_ref,
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    source_ref_kind = EXCLUDED.source_ref_kind,
    source_ref = EXCLUDED.source_ref,
    read_model_object_ref = EXCLUDED.read_model_object_ref,
    freshness_policy_ref = EXCLUDED.freshness_policy_ref,
    last_event_required = EXCLUDED.last_event_required,
    last_receipt_required = EXCLUDED.last_receipt_required,
    failure_visibility_required = EXCLUDED.failure_visibility_required,
    replay_supported = EXCLUDED.replay_supported,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_projection_state (
    projection_ref,
    last_refreshed_at,
    freshness_status
) VALUES
    ('projection.circuit_breakers', NULL, 'unknown'),
    ('projection.private_provider_job_catalog', NULL, 'unknown'),
    ('projection.private_provider_control_plane_snapshot', NULL, 'unknown')
ON CONFLICT (projection_ref) DO NOTHING;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
(
    'provider_circuit_breaker_state',
    'Provider circuit breaker state',
    'projection',
    'Durable provider circuit-breaker runtime state with effective manual override projection.',
    jsonb_build_object(
        'source', 'migration.261_provider_control_plane_snapshot',
        'projection_ref', 'projection.circuit_breakers'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.circuit_breakers',
        'read_model_object_ref', 'table.public.provider_circuit_breaker_state',
        'effective_read_model_ref', 'view.public.effective_provider_circuit_breaker_state'
    )
),
(
    'private_provider_control_plane_snapshot',
    'Private provider control plane snapshot',
    'projection',
    'Private-instance CQRS read model with transport, provider, model, model version, cost structure, credential availability, breaker state, structured removal reasons, and runnable capability state.',
    jsonb_build_object(
        'source', 'migration.261_provider_control_plane_snapshot',
        'projection_ref', 'projection.private_provider_control_plane_snapshot'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.provider_onboarding',
        'read_model_object_ref', 'table.public.private_provider_control_plane_snapshot',
        'effective_read_model_ref', 'view.public.effective_private_provider_control_plane',
        'compatibility_view_ref', 'view.public.effective_private_provider_job_catalog'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

UPDATE data_dictionary_objects
SET
    summary = 'CQRS read model that returns one projected provider control-plane snapshot with capability state, breaker state, structured removal reasons, freshness, and lever metadata.',
    metadata = jsonb_build_object(
        'operation_kind', 'query',
        'authority_domain_ref', 'authority.provider_onboarding',
        'projection_ref', 'projection.private_provider_control_plane_snapshot',
        'handler_ref', 'runtime.operations.queries.circuits.handle_query_provider_control_plane'
    ),
    updated_at = now()
WHERE object_kind = 'operation.operator.provider_control_plane';

UPDATE authority_object_registry
SET
    metadata = jsonb_build_object(
        'handler_ref', 'runtime.operations.queries.circuits.handle_query_provider_control_plane',
        'source_kind', 'operation_query',
        'projection_ref', 'projection.private_provider_control_plane_snapshot'
    ),
    updated_at = now()
WHERE object_ref = 'operation.operator.provider_control_plane';

UPDATE operation_catalog_registry
SET
    projection_ref = 'projection.private_provider_control_plane_snapshot',
    binding_revision = 'binding.operation_catalog_registry.provider_control_plane.20260426b',
    decision_ref = 'decision.provider_control_plane_snapshot.20260426',
    updated_at = now()
WHERE operation_ref = 'operator-provider-control-plane';

COMMENT ON TABLE provider_circuit_breaker_state IS
    'Durable provider circuit-breaker runtime state. Effective operator-visible state is exposed through effective_provider_circuit_breaker_state.';

COMMENT ON TABLE private_provider_control_plane_snapshot IS
    'Private-instance provider control-plane snapshot with credential availability, breaker state, structured removal reasons, and runnable capability projection.';

COMMENT ON VIEW effective_private_provider_control_plane IS
    'Runnable provider control-plane rows only. This is the effective private control-plane capability surface.';

COMMIT;
