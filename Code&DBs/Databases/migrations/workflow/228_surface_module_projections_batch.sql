-- Migration 228: Second + third Surface workspace module projections.
--
-- Batch continuation of migration 227. Adds two additional metric-module
-- projections under authority.surface_catalog so we can retire the rest of
-- the platform-overview endpoint-string presets. Pattern identical to 227;
-- one row per projection in each of the four authority surfaces:
--
--   authority_projection_registry   -- the projection
--   authority_projection_state      -- freshness bootstrap
--   authority_object_registry       -- read-model object
--   authority_projection_contracts  -- source binding + failure visibility
--   data_dictionary_objects         -- LLM-discoverable entry
--
-- Anchored by:
--   architecture-policy::surface-catalog::surface-composition-cqrs-direction
--   architecture-policy::platform-architecture::legal-is-computable-not-permitted

BEGIN;

-- 1. authority_projection_registry rows ----------------------------------
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
        'projection.surface_module.platform_overview_open_bugs',
        'authority.surface_catalog',
        'stream.surface_catalog',
        'runtime.surface_projections.open_bugs_reducer',
        'praxis.primary_postgres',
        'projection_freshness.default',
        TRUE,
        'decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction'
    ),
    (
        'projection.surface_module.platform_overview_total_runs',
        'authority.surface_catalog',
        'stream.surface_catalog',
        'runtime.surface_projections.total_runs_reducer',
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

-- 2. authority_projection_state ------------------------------------------
INSERT INTO authority_projection_state (projection_ref, freshness_status, last_refreshed_at)
VALUES
    ('projection.surface_module.platform_overview_open_bugs', 'fresh', now()),
    ('projection.surface_module.platform_overview_total_runs', 'fresh', now())
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status = EXCLUDED.freshness_status,
    last_refreshed_at = EXCLUDED.last_refreshed_at,
    updated_at = now();

-- 3. authority_object_registry -------------------------------------------
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
        'projection.surface_module.platform_overview_open_bugs',
        'projection',
        'projection.surface_module.platform_overview_open_bugs',
        NULL,
        'authority.surface_catalog',
        'projection.surface_module.platform_overview_open_bugs',
        'active',
        'projection',
        'praxis.engine',
        'decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction',
        jsonb_build_object(
            'migration', '228_surface_module_projections_batch.sql',
            'output_shape', jsonb_build_object('value', 'number', 'format', 'number'),
            'reducer_entry', 'runtime.surface_projections.open_bugs_reducer',
            'note', 'Count of currently-open bugs from /api/platform-overview.open_bugs.'
        )
    ),
    (
        'projection.surface_module.platform_overview_total_runs',
        'projection',
        'projection.surface_module.platform_overview_total_runs',
        NULL,
        'authority.surface_catalog',
        'projection.surface_module.platform_overview_total_runs',
        'active',
        'projection',
        'praxis.engine',
        'decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction',
        jsonb_build_object(
            'migration', '228_surface_module_projections_batch.sql',
            'output_shape', jsonb_build_object('value', 'number', 'format', 'number'),
            'reducer_entry', 'runtime.surface_projections.total_runs_reducer',
            'note', 'Total workflow_runs count from /api/platform-overview.total_workflow_runs.'
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

-- 4. authority_projection_contracts --------------------------------------
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
        'projection_contract.surface_module.platform_overview_open_bugs',
        'projection.surface_module.platform_overview_open_bugs',
        'authority.surface_catalog',
        'table',
        'table.public.bugs',
        'projection.surface_module.platform_overview_open_bugs',
        'projection_freshness.default',
        TRUE,
        TRUE,
        TRUE,
        TRUE,
        TRUE,
        'decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction',
        jsonb_build_object(
            'migration', '228_surface_module_projections_batch.sql',
            'reducer_entry', 'runtime.surface_projections.open_bugs_reducer',
            'output_shape', jsonb_build_object('value', 'number', 'format', 'number'),
            'consumer_note', '/api/projections/<projection_ref>; envelope includes freshness + source_refs.'
        )
    ),
    (
        'projection_contract.surface_module.platform_overview_total_runs',
        'projection.surface_module.platform_overview_total_runs',
        'authority.surface_catalog',
        'table',
        'table.public.workflow_runs',
        'projection.surface_module.platform_overview_total_runs',
        'projection_freshness.default',
        TRUE,
        TRUE,
        TRUE,
        TRUE,
        TRUE,
        'decision.architecture_policy.surface_catalog.surface_composition_cqrs_direction',
        jsonb_build_object(
            'migration', '228_surface_module_projections_batch.sql',
            'reducer_entry', 'runtime.surface_projections.total_runs_reducer',
            'output_shape', jsonb_build_object('value', 'number', 'format', 'number'),
            'consumer_note', '/api/projections/<projection_ref>; envelope includes freshness + source_refs.'
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

-- 5. data_dictionary_objects ---------------------------------------------
INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'projection.surface_module.platform_overview_open_bugs',
        'Surface module projection: open bugs count',
        'projection',
        'Typed read-model for the Surface workspace open-bugs metric module. Returns the current count of unresolved bugs.',
        jsonb_build_object(
            'source', 'migration.228_surface_module_projections_batch',
            'projection_ref', 'projection.surface_module.platform_overview_open_bugs',
            'projection_contract_ref', 'projection_contract.surface_module.platform_overview_open_bugs'
        ),
        jsonb_build_object(
            'authority_domain_ref', 'authority.surface_catalog',
            'output_shape', jsonb_build_object('value', 'number', 'format', 'number'),
            'consumer_surface', '/api/projections/projection.surface_module.platform_overview_open_bugs',
            'reducer_ref', 'runtime.surface_projections.open_bugs_reducer'
        )
    ),
    (
        'projection.surface_module.platform_overview_total_runs',
        'Surface module projection: total workflow runs',
        'projection',
        'Typed read-model for the Surface workspace total-runs metric module. Returns the total workflow_runs count.',
        jsonb_build_object(
            'source', 'migration.228_surface_module_projections_batch',
            'projection_ref', 'projection.surface_module.platform_overview_total_runs',
            'projection_contract_ref', 'projection_contract.surface_module.platform_overview_total_runs'
        ),
        jsonb_build_object(
            'authority_domain_ref', 'authority.surface_catalog',
            'output_shape', jsonb_build_object('value', 'number', 'format', 'number'),
            'consumer_surface', '/api/projections/projection.surface_module.platform_overview_total_runs',
            'reducer_ref', 'runtime.surface_projections.total_runs_reducer'
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
