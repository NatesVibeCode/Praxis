-- Migration 244: DB authority for UI surface file anchors.
--
-- The LLM-facing UI experience graph needs file anchors, but those anchors
-- should not be authored as Python literals in the read model. This registry
-- keeps renderer/runtime source anchors queryable beside the existing UI
-- route, action, and feature-flow registries.

BEGIN;

CREATE TABLE IF NOT EXISTS ui_surface_file_anchor_registry (
    anchor_id TEXT PRIMARY KEY CHECK (btrim(anchor_id) <> ''),
    surface_name TEXT NOT NULL CHECK (btrim(surface_name) <> ''),
    source_file TEXT NOT NULL CHECK (btrim(source_file) <> ''),
    anchor_kind TEXT NOT NULL CHECK (
        anchor_kind IN ('renderer', 'style', 'state', 'runtime', 'shared')
    ),
    label TEXT NOT NULL DEFAULT '' CHECK (label IS NOT NULL),
    notes TEXT NOT NULL DEFAULT '' CHECK (notes IS NOT NULL),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    display_order INTEGER NOT NULL DEFAULT 0,
    binding_revision TEXT NOT NULL CHECK (btrim(binding_revision) <> ''),
    decision_ref TEXT NOT NULL CHECK (btrim(decision_ref) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ui_surface_file_anchor_registry_surface_file_key
        UNIQUE (surface_name, source_file)
);

CREATE INDEX IF NOT EXISTS ui_surface_file_anchor_registry_surface_order_idx
    ON ui_surface_file_anchor_registry (surface_name, enabled, display_order, anchor_id);

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES (
    'ui_surface_file_anchor_registry',
    'UI surface file anchor registry',
    'table',
    'DB-backed registry of UI surface source-file anchors used by the LLM-facing experience graph.',
    '{"migration":"244_ui_surface_file_anchor_registry.sql"}'::jsonb,
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
    'table.public.ui_surface_file_anchor_registry',
    'table',
    'ui_surface_file_anchor_registry',
    'public',
    'authority.surface_catalog',
    'ui_surface_file_anchor_registry',
    'active',
    'registry',
    'praxis.engine',
    'architecture-policy::ui-experience-graph::registry-owned-file-anchors',
    '{"migration":"244_ui_surface_file_anchor_registry.sql"}'::jsonb
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

INSERT INTO ui_surface_file_anchor_registry (
    anchor_id,
    surface_name,
    source_file,
    anchor_kind,
    label,
    notes,
    enabled,
    display_order,
    binding_revision,
    decision_ref
) VALUES
    (
        'dashboard.renderer.dashboard',
        'dashboard',
        'Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx',
        'renderer',
        'Dashboard renderer',
        'Overview surface and dashboard action wiring.',
        TRUE,
        10,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'dashboard.style.dashboard',
        'dashboard',
        'Code&DBs/Workflow/surfaces/app/src/dashboard/dashboard.css',
        'style',
        'Dashboard styles',
        'Overview layout and visual state styling.',
        TRUE,
        20,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'dashboard.renderer.chat-panel',
        'dashboard',
        'Code&DBs/Workflow/surfaces/app/src/dashboard/ChatPanel.tsx',
        'renderer',
        'Dashboard chat panel',
        'Embedded assistant entry point on the dashboard.',
        TRUE,
        30,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'build.renderer.moon-build-page',
        'build',
        'Code&DBs/Workflow/surfaces/app/src/moon/MoonBuildPage.tsx',
        'renderer',
        'Moon build page',
        'Primary workflow design, inspection, and release surface.',
        TRUE,
        10,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'build.renderer.action-dock',
        'build',
        'Code&DBs/Workflow/surfaces/app/src/moon/MoonActionDock.tsx',
        'renderer',
        'Moon action dock',
        'Workflow composition and action controls.',
        TRUE,
        20,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'build.renderer.node-detail',
        'build',
        'Code&DBs/Workflow/surfaces/app/src/moon/MoonNodeDetail.tsx',
        'renderer',
        'Moon node detail',
        'Node contract and authority-context inspector.',
        TRUE,
        30,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'build.renderer.release-tray',
        'build',
        'Code&DBs/Workflow/surfaces/app/src/moon/MoonReleaseTray.tsx',
        'renderer',
        'Moon release tray',
        'Workflow release and run handoff controls.',
        TRUE,
        40,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'build.shared.presenter',
        'build',
        'Code&DBs/Workflow/surfaces/app/src/moon/moonBuildPresenter.ts',
        'shared',
        'Moon presenter',
        'Presenter logic for build-surface UI state.',
        TRUE,
        50,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'build.shared.graph-definition',
        'build',
        'Code&DBs/Workflow/surfaces/app/src/shared/buildGraphDefinition.ts',
        'shared',
        'Build graph definition',
        'Shared graph shape used by the Moon canvas.',
        TRUE,
        60,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'run-detail.renderer.run-detail',
        'run-detail',
        'Code&DBs/Workflow/surfaces/app/src/dashboard/RunDetailView.tsx',
        'renderer',
        'Run detail view',
        'Execution observer for one workflow run.',
        TRUE,
        10,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'run-detail.shared.run-graph',
        'run-detail',
        'Code&DBs/Workflow/surfaces/app/src/shared/RunGraphView.tsx',
        'shared',
        'Run graph view',
        'Shared run graph renderer.',
        TRUE,
        20,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'run-detail.shared.live-snapshot',
        'run-detail',
        'Code&DBs/Workflow/surfaces/app/src/dashboard/useLiveRunSnapshot.ts',
        'shared',
        'Live run snapshot hook',
        'Run snapshot refresh and state ownership.',
        TRUE,
        30,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'chat.renderer.chat-panel',
        'chat',
        'Code&DBs/Workflow/surfaces/app/src/dashboard/ChatPanel.tsx',
        'renderer',
        'Chat panel',
        'Operator conversation and compile entry surface.',
        TRUE,
        10,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'chat.shared.use-chat',
        'chat',
        'Code&DBs/Workflow/surfaces/app/src/workspace/useChat.ts',
        'shared',
        'Chat runtime hook',
        'Workspace chat state and transport logic.',
        TRUE,
        20,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'chat.renderer.tool-result',
        'chat',
        'Code&DBs/Workflow/surfaces/app/src/workspace/ToolResultRenderer.tsx',
        'renderer',
        'Tool result renderer',
        'Tool-result rendering inside the chat flow.',
        TRUE,
        30,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'manifests.renderer.catalog',
        'manifests',
        'Code&DBs/Workflow/surfaces/app/src/praxis/ManifestCatalogPage.tsx',
        'renderer',
        'Manifest catalog page',
        'Control-plane manifest discovery surface.',
        TRUE,
        10,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'manifests.renderer.editor',
        'manifests',
        'Code&DBs/Workflow/surfaces/app/src/grid/ManifestEditorPage.tsx',
        'renderer',
        'Manifest editor page',
        'Manifest contract editing surface.',
        TRUE,
        20,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'manifests.renderer.bundle',
        'manifests',
        'Code&DBs/Workflow/surfaces/app/src/praxis/ManifestBundleView.tsx',
        'renderer',
        'Manifest bundle view',
        'Manifest bundle inspection surface.',
        TRUE,
        30,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'atlas.renderer.page',
        'atlas',
        'Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.tsx',
        'renderer',
        'Atlas page',
        'Secondary system map renderer.',
        TRUE,
        10,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    ),
    (
        'atlas.runtime.graph',
        'atlas',
        'Code&DBs/Workflow/runtime/atlas_graph.py',
        'runtime',
        'Atlas graph read model',
        'DB read model used by the Atlas graph surface.',
        TRUE,
        20,
        'binding.ui_surface_file_anchor_registry.shell.20260424',
        'architecture-policy::ui-experience-graph::registry-owned-file-anchors'
    )
ON CONFLICT (anchor_id) DO UPDATE SET
    surface_name = EXCLUDED.surface_name,
    source_file = EXCLUDED.source_file,
    anchor_kind = EXCLUDED.anchor_kind,
    label = EXCLUDED.label,
    notes = EXCLUDED.notes,
    enabled = EXCLUDED.enabled,
    display_order = EXCLUDED.display_order,
    binding_revision = EXCLUDED.binding_revision,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
