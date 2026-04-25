-- Migration 243: runtime-profile admitted route projection.
--
-- Workflow validation should not reconstruct runtime route authority by
-- walking profile, policy, binding, candidate, and eligibility tables. Native
-- runtime sync owns that reducer. Validation reads this projection only.

BEGIN;

CREATE TABLE IF NOT EXISTS runtime_profile_admitted_routes (
    runtime_profile_ref TEXT NOT NULL,
    model_profile_id TEXT NOT NULL,
    provider_policy_id TEXT NOT NULL,
    candidate_ref TEXT NOT NULL REFERENCES provider_model_candidates (candidate_ref) ON DELETE CASCADE,
    provider_ref TEXT NOT NULL CHECK (btrim(provider_ref) <> ''),
    provider_slug TEXT NOT NULL CHECK (btrim(provider_slug) <> ''),
    model_slug TEXT NOT NULL CHECK (btrim(model_slug) <> ''),
    eligibility_status TEXT NOT NULL CHECK (btrim(eligibility_status) <> ''),
    reason_code TEXT NOT NULL CHECK (btrim(reason_code) <> ''),
    source_window_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(source_window_refs) = 'array'),
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    projection_ref TEXT NOT NULL DEFAULT 'projection.runtime_profile_admitted_routes',
    PRIMARY KEY (runtime_profile_ref, candidate_ref)
);

CREATE INDEX IF NOT EXISTS runtime_profile_admitted_routes_slug_idx
    ON runtime_profile_admitted_routes (runtime_profile_ref, provider_slug, model_slug);

CREATE INDEX IF NOT EXISTS runtime_profile_admitted_routes_projection_idx
    ON runtime_profile_admitted_routes (projection_ref, projected_at DESC);

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.provider_onboarding',
    'praxis.engine',
    'stream.provider_onboarding',
    'projection.runtime_profile_admitted_routes',
    'praxis.primary_postgres',
    TRUE,
    'decision.provider_onboarding.runtime_profile_admitted_routes_projection.20260425'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    current_projection_ref = EXCLUDED.current_projection_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
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
    'table.public.runtime_profile_admitted_routes',
    'table',
    'runtime_profile_admitted_routes',
    'public',
    'authority.provider_onboarding',
    'runtime_profile_admitted_routes',
    'active',
    'projection',
    'praxis.engine',
    'decision.provider_onboarding.runtime_profile_admitted_routes_projection.20260425',
    jsonb_build_object(
        'projection_ref', 'projection.runtime_profile_admitted_routes',
        'reducer_ref', 'registry.native_runtime_profile_sync.sync_native_runtime_profile_authority'
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
    'projection.runtime_profile_admitted_routes',
    'authority.provider_onboarding',
    'stream.provider_onboarding',
    'registry.native_runtime_profile_sync.sync_native_runtime_profile_authority',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.provider_onboarding.runtime_profile_admitted_routes_projection.20260425'
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
) VALUES (
    'projection_contract.runtime_profile_admitted_routes',
    'projection.runtime_profile_admitted_routes',
    'authority.provider_onboarding',
    'table',
    'registry_native_runtime_profile_authority,model_profile_candidate_bindings,route_eligibility_states',
    'table.public.runtime_profile_admitted_routes',
    'projection_freshness.default',
    FALSE,
    FALSE,
    TRUE,
    TRUE,
    TRUE,
    'decision.provider_onboarding.runtime_profile_admitted_routes_projection.20260425',
    jsonb_build_object(
        'consumer', 'runtime.workflow_validation',
        'invariant', 'provider/model routes must be admitted by runtime profile before workflow submission'
    )
)
ON CONFLICT (projection_ref) DO UPDATE SET
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
) VALUES (
    'projection.runtime_profile_admitted_routes',
    NULL,
    'unknown'
)
ON CONFLICT (projection_ref) DO NOTHING;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'runtime_profile_admitted_routes',
    'Runtime profile admitted routes projection',
    'projection',
    'CQRS read model consumed by workflow validation to confirm selected provider/model routes are admitted by the runtime profile.',
    jsonb_build_object(
        'source', 'migration.243_runtime_profile_admitted_routes_projection',
        'projection_ref', 'projection.runtime_profile_admitted_routes'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.provider_onboarding',
        'read_model_object_ref', 'table.public.runtime_profile_admitted_routes'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMENT ON TABLE runtime_profile_admitted_routes IS
    'CQRS projection of provider/model routes admitted for each runtime profile. Validation reads this projection instead of reconstructing routing authority.';

COMMIT;
