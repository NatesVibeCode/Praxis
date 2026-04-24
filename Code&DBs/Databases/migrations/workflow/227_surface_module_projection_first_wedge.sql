-- Migration 227: First Surface workspace typed-projection wedge.
--
-- Proves the CQRS read-model pattern for Surface workspace composition by
-- registering ONE module projection through the existing authority substrate
-- (authority_projection_registry + authority_projection_contracts, migrations
-- 200/204). No parallel module_projection_registry table — that would violate
-- architecture-policy::platform-architecture::conceptual-events-register-
-- through-operation-catalog-registry.
--
-- Anchored by:
--   architecture-policy::surface-catalog::surface-composition-cqrs-direction
--   architecture-policy::platform-architecture::legal-is-computable-not-permitted
--   roadmap_item.praxis.public.beta.ramp.llm.first.infrastructure.wedge.surface.composition.projections.first.wedge
--
-- Scope is read-side only. The legality compiler that narrows module palettes
-- by consuming pills + gates depends on Phase 1.4 DataPill/PillBinding and the
-- typed_gap emission wiring (event contract registered in migration 226,
-- emitters queued). This migration does NOT build legal_modules(state).

BEGIN;

-- 1. Register the projection itself.
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
    'projection.surface_module.platform_overview_pass_rate',
    'authority.surface_catalog',
    'stream.surface_catalog',
    'runtime.surface_projections.pass_rate_reducer',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction'
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

-- 2. Seed projection state so authority_event_projection_contract_report
--    does not report 'missing_projection_state'.
INSERT INTO authority_projection_state (projection_ref, freshness_status, last_refreshed_at)
VALUES ('projection.surface_module.platform_overview_pass_rate', 'fresh', now())
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status = EXCLUDED.freshness_status,
    last_refreshed_at = EXCLUDED.last_refreshed_at,
    updated_at = now();

-- 3. Register the read-model object so authority_projection_contracts
--    read_model_object_ref resolves to a real authority_object_registry row.
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
    'projection.surface_module.platform_overview_pass_rate',
    'projection',
    'projection.surface_module.platform_overview_pass_rate',
    NULL,
    'authority.surface_catalog',
    'projection.surface_module.platform_overview_pass_rate',
    'active',
    'projection',
    'praxis.engine',
    'decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction',
    jsonb_build_object(
        'migration', '227_surface_module_projection_first_wedge.sql',
        'output_shape', jsonb_build_object(
            'value', 'number',
            'format', 'percent'
        ),
        'reducer_entry', 'runtime.surface_projections.pass_rate_reducer',
        'note', 'First surface-module projection; pass_rate computed from workflow_runs via engineering_observability.build_platform_observability.'
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

-- 4. Bind the projection to its source through authority_projection_contracts
--    (the CQRS read-side contract surface, migration 204).
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
    'projection_contract.surface_module.platform_overview_pass_rate',
    'projection.surface_module.platform_overview_pass_rate',
    'authority.surface_catalog',
    'table',
    'table.public.workflow_runs',
    'projection.surface_module.platform_overview_pass_rate',
    'projection_freshness.default',
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    'decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction',
    jsonb_build_object(
        'migration', '227_surface_module_projection_first_wedge.sql',
        'reducer_entry', 'runtime.surface_projections.pass_rate_reducer',
        'output_shape', jsonb_build_object(
            'value', 'number',
            'format', 'percent'
        ),
        'consumer_note', 'Returned via /api/projections/<projection_ref>; client envelope includes last_event_id, last_receipt_id, last_refreshed_at, freshness_status from authority_projection_state.'
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

-- 5. Surface the projection in the data dictionary for LLM discovery.
INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'projection.surface_module.platform_overview_pass_rate',
    'Surface module projection: platform overview pass rate',
    'projection',
    'Typed read-model for the Surface workspace pass-rate metric module. Returns the 24h pass_rate with freshness envelope; replaces the raw /api/platform-overview.pass_rate path for the one migrated preset.',
    jsonb_build_object(
        'source', 'migration.227_surface_module_projection_first_wedge',
        'projection_ref', 'projection.surface_module.platform_overview_pass_rate',
        'projection_contract_ref', 'projection_contract.surface_module.platform_overview_pass_rate'
    ),
    jsonb_build_object(
        'authority_domain_ref', 'authority.surface_catalog',
        'output_shape', jsonb_build_object('value', 'number', 'format', 'percent'),
        'consumer_surface', '/api/projections/projection.surface_module.platform_overview_pass_rate',
        'reducer_ref', 'runtime.surface_projections.pass_rate_reducer'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
