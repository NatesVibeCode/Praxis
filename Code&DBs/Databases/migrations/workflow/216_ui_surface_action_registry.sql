-- Migration 216: DB authority for product UI actions.
--
-- Moon primitives already live in surface_catalog_registry. Dashboard and app
-- shell actions were still source/audit-derived, which forced future agents to
-- infer UX contracts from React. This registry gives product UI actions a
-- durable owner without polluting the Moon primitive catalog.

BEGIN;

CREATE TABLE IF NOT EXISTS ui_surface_action_registry (
    action_id TEXT PRIMARY KEY CHECK (btrim(action_id) <> ''),
    surface_name TEXT NOT NULL CHECK (btrim(surface_name) <> ''),
    label TEXT NOT NULL CHECK (btrim(label) <> ''),
    action_kind TEXT NOT NULL CHECK (
        action_kind IN (
            'navigation',
            'assistant',
            'mutation',
            'destructive_mutation',
            'builder',
            'builder_mutation',
            'authority_context',
            'release'
        )
    ),
    effect TEXT NOT NULL CHECK (btrim(effect) <> ''),
    target_surface_name TEXT CHECK (target_surface_name IS NULL OR btrim(target_surface_name) <> ''),
    http_method TEXT CHECK (http_method IS NULL OR http_method IN ('GET', 'POST', 'PUT', 'PATCH', 'DELETE')),
    endpoint_template TEXT CHECK (endpoint_template IS NULL OR btrim(endpoint_template) <> ''),
    state_effect TEXT CHECK (state_effect IS NULL OR btrim(state_effect) <> ''),
    confidence TEXT NOT NULL CHECK (
        confidence IN ('db_backed_catalog', 'source_traced', 'runtime_verified')
    ),
    source_refs JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(source_refs) = 'array'),
    status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready', 'deprecated', 'hidden')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    display_order INTEGER NOT NULL DEFAULT 0,
    binding_revision TEXT NOT NULL CHECK (btrim(binding_revision) <> ''),
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ui_surface_action_registry_surface_order_idx
    ON ui_surface_action_registry (surface_name, enabled, display_order, action_id);

CREATE INDEX IF NOT EXISTS ui_surface_action_registry_target_idx
    ON ui_surface_action_registry (target_surface_name)
    WHERE target_surface_name IS NOT NULL;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'ui_surface_action_registry',
    'UI surface action registry',
    'table',
    'DB-backed registry of product UI actions, their effects, source anchors, route targets, and authority confidence.',
    '{"migration":"216_ui_surface_action_registry.sql"}'::jsonb,
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
    'table.public.ui_surface_action_registry',
    'table',
    'ui_surface_action_registry',
    'public',
    'authority.surface_catalog',
    'ui_surface_action_registry',
    'active',
    'registry',
    'praxis.engine',
    'decision.ui_surface_action_registry.dashboard_shell.20260424',
    '{"migration":"216_ui_surface_action_registry.sql"}'::jsonb
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

INSERT INTO ui_surface_action_registry (
    action_id,
    surface_name,
    label,
    action_kind,
    effect,
    target_surface_name,
    http_method,
    endpoint_template,
    state_effect,
    confidence,
    source_refs,
    status,
    enabled,
    display_order,
    binding_revision,
    decision_ref
) VALUES
    (
        'dashboard.new-operating-model',
        'dashboard',
        '+ New Operating Model',
        'navigation',
        'Opens Moon builder in operating-model flow.',
        'build',
        NULL,
        NULL,
        'activeTabId=build, buildIntent=operating-model',
        'source_traced',
        '["docs/moon-ui-actions-audit.md","Code&DBs/Workflow/surfaces/app/src/App.tsx","Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"]'::jsonb,
        'ready',
        TRUE,
        10,
        'binding.ui_surface_action_registry.dashboard_shell.20260424',
        'decision.ui_surface_action_registry.dashboard_shell.20260424'
    ),
    (
        'dashboard.workflow-builder',
        'dashboard',
        '+ Workflow Builder',
        'navigation',
        'Opens Moon Build for a new workflow.',
        'build',
        NULL,
        NULL,
        'activeTabId=build, buildWorkflowId=null, buildView=moon',
        'source_traced',
        '["docs/moon-ui-actions-audit.md","Code&DBs/Workflow/surfaces/app/src/App.tsx","Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"]'::jsonb,
        'ready',
        TRUE,
        20,
        'binding.ui_surface_action_registry.dashboard_shell.20260424',
        'decision.ui_surface_action_registry.dashboard_shell.20260424'
    ),
    (
        'dashboard.open-chat',
        'dashboard',
        'Ask anything...',
        'assistant',
        'Opens the chat panel.',
        'chat',
        NULL,
        NULL,
        'chatOpen=true',
        'source_traced',
        '["docs/moon-ui-actions-audit.md","Code&DBs/Workflow/surfaces/app/src/App.tsx","Code&DBs/Workflow/surfaces/app/src/dashboard/ChatPanel.tsx"]'::jsonb,
        'ready',
        TRUE,
        30,
        'binding.ui_surface_action_registry.dashboard_shell.20260424',
        'decision.ui_surface_action_registry.dashboard_shell.20260424'
    ),
    (
        'dashboard.upload-kb',
        'dashboard',
        '+ Add to Knowledge Base',
        'mutation',
        'Opens file picker and uploads the selected file.',
        NULL,
        'POST',
        '/api/files',
        NULL,
        'source_traced',
        '["docs/moon-ui-actions-audit.md","Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"]'::jsonb,
        'ready',
        TRUE,
        40,
        'binding.ui_surface_action_registry.dashboard_shell.20260424',
        'decision.ui_surface_action_registry.dashboard_shell.20260424'
    ),
    (
        'dashboard.run-workflow',
        'dashboard',
        'Run Now',
        'mutation',
        'Triggers the selected workflow.',
        'build',
        'POST',
        '/api/trigger/{workflow_id}',
        'opens latest run detail when available',
        'source_traced',
        '["docs/moon-ui-actions-audit.md","Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"]'::jsonb,
        'ready',
        TRUE,
        50,
        'binding.ui_surface_action_registry.dashboard_shell.20260424',
        'decision.ui_surface_action_registry.dashboard_shell.20260424'
    ),
    (
        'dashboard.delete-workflow',
        'dashboard',
        'Delete',
        'destructive_mutation',
        'Deletes the selected workflow after confirmation.',
        NULL,
        'DELETE',
        '/api/workflows/delete/{workflow_id}',
        'removes workflow from dashboard list',
        'source_traced',
        '["docs/moon-ui-actions-audit.md","Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx"]'::jsonb,
        'ready',
        TRUE,
        60,
        'binding.ui_surface_action_registry.dashboard_shell.20260424',
        'decision.ui_surface_action_registry.dashboard_shell.20260424'
    )
ON CONFLICT (action_id) DO UPDATE SET
    surface_name = EXCLUDED.surface_name,
    label = EXCLUDED.label,
    action_kind = EXCLUDED.action_kind,
    effect = EXCLUDED.effect,
    target_surface_name = EXCLUDED.target_surface_name,
    http_method = EXCLUDED.http_method,
    endpoint_template = EXCLUDED.endpoint_template,
    state_effect = EXCLUDED.state_effect,
    confidence = EXCLUDED.confidence,
    source_refs = EXCLUDED.source_refs,
    status = EXCLUDED.status,
    enabled = EXCLUDED.enabled,
    display_order = EXCLUDED.display_order,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
