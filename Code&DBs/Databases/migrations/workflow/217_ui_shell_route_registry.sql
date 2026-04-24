-- Migration 217: DB authority for app shell routes.
--
-- UI action authority is only half useful if the route/state transitions still
-- require reading shell/state.ts. This registry makes the route-to-surface map
-- queryable for agents before they touch the React shell.

BEGIN;

CREATE TABLE IF NOT EXISTS ui_shell_route_registry (
    route_id TEXT PRIMARY KEY CHECK (btrim(route_id) <> ''),
    path_template TEXT NOT NULL CHECK (btrim(path_template) <> ''),
    surface_name TEXT NOT NULL CHECK (btrim(surface_name) <> ''),
    state_effect TEXT NOT NULL CHECK (btrim(state_effect) <> ''),
    notes TEXT NOT NULL DEFAULT '',
    source_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(source_refs) = 'array'),
    status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready', 'legacy', 'deprecated')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    display_order INTEGER NOT NULL DEFAULT 0,
    binding_revision TEXT NOT NULL CHECK (btrim(binding_revision) <> ''),
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ui_shell_route_registry_unique_path
        UNIQUE (path_template)
);

CREATE INDEX IF NOT EXISTS ui_shell_route_registry_surface_order_idx
    ON ui_shell_route_registry (surface_name, enabled, display_order, route_id);

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'ui_shell_route_registry',
    'UI shell route registry',
    'table',
    'DB-backed registry of app shell routes, target surfaces, and shell state effects.',
    '{"migration":"217_ui_shell_route_registry.sql"}'::jsonb,
    '{"authority_domain_ref":"authority.surface_catalog"}'::jsonb
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
    'table.public.ui_shell_route_registry',
    'table',
    'ui_shell_route_registry',
    'public',
    'authority.surface_catalog',
    'ui_shell_route_registry',
    'active',
    'registry',
    'praxis.engine',
    'decision.ui_shell_route_registry.app_routes.20260424',
    '{"migration":"217_ui_shell_route_registry.sql"}'::jsonb
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

INSERT INTO ui_shell_route_registry (
    route_id,
    path_template,
    surface_name,
    state_effect,
    notes,
    source_refs,
    status,
    enabled,
    display_order,
    binding_revision,
    decision_ref
) VALUES
    (
        'route.app.dashboard',
        '/app',
        'dashboard',
        'activeTabId=dashboard',
        'Default shell route.',
        '["Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        10,
        'binding.ui_shell_route_registry.app_routes.20260424',
        'decision.ui_shell_route_registry.app_routes.20260424'
    ),
    (
        'route.app.workflow',
        '/app/workflow',
        'build',
        'activeTabId=build, buildView=moon',
        'Clean URL for Moon workflow build/edit.',
        '["Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        20,
        'binding.ui_shell_route_registry.app_routes.20260424',
        'decision.ui_shell_route_registry.app_routes.20260424'
    ),
    (
        'route.app.build.legacy',
        '/app/build',
        'build',
        'activeTabId=build, buildView=moon',
        'Legacy alias accepted for older bookmarks.',
        '["Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'legacy',
        TRUE,
        30,
        'binding.ui_shell_route_registry.app_routes.20260424',
        'decision.ui_shell_route_registry.app_routes.20260424'
    ),
    (
        'route.app.run',
        '/app/run/{run_id}',
        'build',
        'activeTabId=build, moonRunId={run_id}',
        'Moon-owned run detail route.',
        '["Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        40,
        'binding.ui_shell_route_registry.app_routes.20260424',
        'decision.ui_shell_route_registry.app_routes.20260424'
    ),
    (
        'route.app.manifests',
        '/app/manifests',
        'manifests',
        'activeTabId=manifests',
        'Manifest catalog route.',
        '["Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        50,
        'binding.ui_shell_route_registry.app_routes.20260424',
        'decision.ui_shell_route_registry.app_routes.20260424'
    ),
    (
        'route.app.atlas',
        '/app/atlas',
        'atlas',
        'activeTabId=atlas',
        'Secondary graph-map route.',
        '["Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        60,
        'binding.ui_shell_route_registry.app_routes.20260424',
        'decision.ui_shell_route_registry.app_routes.20260424'
    )
ON CONFLICT (route_id) DO UPDATE SET
    path_template = EXCLUDED.path_template,
    surface_name = EXCLUDED.surface_name,
    state_effect = EXCLUDED.state_effect,
    notes = EXCLUDED.notes,
    source_refs = EXCLUDED.source_refs,
    status = EXCLUDED.status,
    enabled = EXCLUDED.enabled,
    display_order = EXCLUDED.display_order,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
