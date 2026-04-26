-- Migration 259: Register ui_shell_state.live projection.
--
-- Folds shell-navigation events keyed by session_aggregate_ref (per-browser-tab
-- aggregate) into the live ShellState that the React app subscribes to. Reducer
-- in runtime/surface_projections.py consumes:
--   - session.bootstrapped       (initialize aggregate, apply deep-link route)
--   - surface.opened             (apply shell_state_diff)
--   - tab.closed                 (remove dynamic tab, fall back)
--   - history.popped             (cause-of-change record; no state mutation)
--   - draft.guard.consulted      (analytic only; no state mutation)
--
-- API exposes the projection at GET /api/projections/ui_shell_state.live?session=<uuid>
-- (resolved by the existing projections handler with no code change).
--
-- Anchored to decision.shell_navigation_cqrs.20260426.

BEGIN;

-- 1. authority_projection_registry --------------------------------------------
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
    'ui_shell_state.live',
    'authority.surface_catalog',
    'stream.shell_navigation',
    'runtime.surface_projections.reduce_ui_shell_state',
    'praxis.primary_postgres',
    'projection_freshness.default',
    TRUE,
    'decision.shell_navigation_cqrs.20260426'
)
ON CONFLICT (projection_ref) DO UPDATE SET
    authority_domain_ref     = EXCLUDED.authority_domain_ref,
    source_event_stream_ref  = EXCLUDED.source_event_stream_ref,
    reducer_ref              = EXCLUDED.reducer_ref,
    storage_target_ref       = EXCLUDED.storage_target_ref,
    freshness_policy_ref     = EXCLUDED.freshness_policy_ref,
    enabled                  = EXCLUDED.enabled,
    decision_ref             = EXCLUDED.decision_ref,
    updated_at               = now();

-- 2. authority_projection_state -----------------------------------------------
INSERT INTO authority_projection_state (
    projection_ref,
    freshness_status,
    last_refreshed_at
) VALUES (
    'ui_shell_state.live',
    'fresh',
    now()
)
ON CONFLICT (projection_ref) DO UPDATE SET
    freshness_status   = EXCLUDED.freshness_status,
    last_refreshed_at  = EXCLUDED.last_refreshed_at,
    updated_at         = now();

-- 3. authority_object_registry — read-model object ---------------------------
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
    'projection.ui_shell_state.live',
    'projection',
    'ui_shell_state.live',
    NULL,
    'authority.surface_catalog',
    'projection.ui_shell_state.live',
    'active',
    'projection',
    'praxis.engine',
    'decision.shell_navigation_cqrs.20260426',
    jsonb_build_object(
        'migration', '259_register_ui_shell_state_projection.sql',
        'reducer_entry', 'runtime.surface_projections.reduce_ui_shell_state',
        'aggregate_key', 'session_aggregate_ref',
        'output_shape', jsonb_build_object(
            'activeRouteId', 'string',
            'activeTabId', 'string',
            'dynamicTabs', 'array',
            'buildWorkflowId', 'string|null',
            'buildIntent', 'string|null',
            'builderSeed', 'unknown|null',
            'buildView', 'enum',
            'moonRunId', 'string|null',
            'dashboardDetail', 'enum|null'
        ),
        'note', 'Per-browser-tab live shell state. Subscribers: GET /api/projections/ui_shell_state.live, GET /api/shell/state/stream (SSE).'
    )
)
ON CONFLICT (object_ref) DO UPDATE SET
    object_kind                  = EXCLUDED.object_kind,
    object_name                  = EXCLUDED.object_name,
    authority_domain_ref         = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind  = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status             = EXCLUDED.lifecycle_status,
    write_model_kind             = EXCLUDED.write_model_kind,
    owner_ref                    = EXCLUDED.owner_ref,
    source_decision_ref          = EXCLUDED.source_decision_ref,
    metadata                     = EXCLUDED.metadata,
    updated_at                   = now();

-- 4. authority_projection_contracts -------------------------------------------
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
    'projection_contract.ui_shell_state.live',
    'ui_shell_state.live',
    'authority.surface_catalog',
    'event_stream',
    'stream.shell_navigation',
    'projection.ui_shell_state.live',
    'projection_freshness.default',
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    TRUE,
    'decision.shell_navigation_cqrs.20260426',
    jsonb_build_object(
        'migration', '259_register_ui_shell_state_projection.sql',
        'reducer_entry', 'runtime.surface_projections.reduce_ui_shell_state',
        'aggregate_key', 'session_aggregate_ref',
        'consumer_note', '/api/projections/ui_shell_state.live?session=<uuid> for read; /api/shell/state/stream?session=<uuid> for SSE updates.',
        'event_types', jsonb_build_array(
            'session.bootstrapped',
            'surface.opened',
            'tab.closed',
            'history.popped',
            'draft.guard.consulted'
        )
    )
)
ON CONFLICT (projection_ref) DO UPDATE SET
    projection_contract_ref      = EXCLUDED.projection_contract_ref,
    authority_domain_ref         = EXCLUDED.authority_domain_ref,
    source_ref_kind              = EXCLUDED.source_ref_kind,
    source_ref                   = EXCLUDED.source_ref,
    read_model_object_ref        = EXCLUDED.read_model_object_ref,
    freshness_policy_ref         = EXCLUDED.freshness_policy_ref,
    last_event_required          = EXCLUDED.last_event_required,
    last_receipt_required        = EXCLUDED.last_receipt_required,
    failure_visibility_required  = EXCLUDED.failure_visibility_required,
    replay_supported             = EXCLUDED.replay_supported,
    enabled                      = EXCLUDED.enabled,
    decision_ref                 = EXCLUDED.decision_ref,
    metadata                     = EXCLUDED.metadata,
    updated_at                   = now();

-- 5. data_dictionary_objects --------------------------------------------------
INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'projection.ui_shell_state.live',
    'Projection: ui_shell_state.live',
    'projection',
    'Live per-browser-tab React shell state, derived by folding shell.surface.opened / shell.tab.closed / shell.session.bootstrapped / shell.history.popped events keyed by session_aggregate_ref. Read via GET /api/projections/ui_shell_state.live?session=<uuid>; subscribe via SSE at /api/shell/state/stream?session=<uuid>.',
    jsonb_build_object('source', 'migration.259_register_ui_shell_state_projection', 'projection_ref', 'ui_shell_state.live'),
    jsonb_build_object(
        'authority_domain_ref', 'authority.surface_catalog',
        'reducer_entry', 'runtime.surface_projections.reduce_ui_shell_state',
        'aggregate_key', 'session_aggregate_ref'
    )
)
ON CONFLICT (object_kind) DO UPDATE SET
    label      = EXCLUDED.label,
    category   = EXCLUDED.category,
    summary    = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata   = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
