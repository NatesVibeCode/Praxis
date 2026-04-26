-- Migration 256: Expand ui_shell_route_registry to be the authoritative source
-- for app shell routing — component binding, label/context templates,
-- keyboard shortcut, draft-guard flag, dynamic-vs-static, reverse-URL canonical
-- priority. Closes the KNOWN_WEAKNESSES entry at
-- runtime/ui_experience_graph.py:118-126 — "shell navigation state still needs
-- a generated React adapter from ui_shell_route_registry."
--
-- Existing 6 rows from migration 217 are backfilled with metadata reverse-engineered
-- from surfaces/app/src/shell/surfaceRegistry.tsx and App.tsx (so this migration
-- alone does not change runtime behavior — the React rewrite that consumes the
-- expanded columns lands separately). Migration 257 adds dynamic-kind rows and
-- canonical flags in the same wedge.
--
-- Anchored to decision.shell_navigation_cqrs.20260426.

BEGIN;

ALTER TABLE ui_shell_route_registry
    ADD COLUMN IF NOT EXISTS component_ref TEXT,
    ADD COLUMN IF NOT EXISTS tab_kind_label TEXT,
    ADD COLUMN IF NOT EXISTS tab_label_template TEXT,
    ADD COLUMN IF NOT EXISTS context_label TEXT,
    ADD COLUMN IF NOT EXISTS context_detail_template TEXT,
    ADD COLUMN IF NOT EXISTS nav_description_template TEXT,
    ADD COLUMN IF NOT EXISTS nav_keywords JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS event_bus_kind TEXT,
    ADD COLUMN IF NOT EXISTS keyboard_shortcut TEXT,
    ADD COLUMN IF NOT EXISTS draft_guard_required BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_dynamic BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_canonical_for_surface BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS tab_strip_position INTEGER;

ALTER TABLE ui_shell_route_registry
    ADD CONSTRAINT ui_shell_route_registry_nav_keywords_array
    CHECK (jsonb_typeof(nav_keywords) = 'array');

-- Partial unique index: at most one canonical row per surface_name when ready.
CREATE UNIQUE INDEX IF NOT EXISTS ui_shell_route_registry_one_canonical_per_surface
    ON ui_shell_route_registry (surface_name)
    WHERE is_canonical_for_surface = TRUE AND status = 'ready';

-- Backfill the 6 existing rows from 217 with values derived from
-- surfaces/app/src/shell/surfaceRegistry.tsx STATIC_SURFACES + App.tsx
-- lazy-import map. Canonical flags are set in migration 242.

UPDATE ui_shell_route_registry SET
    component_ref            = 'dashboard/Dashboard.Dashboard',
    tab_kind_label           = 'Suite',
    tab_label_template       = 'Overview',
    context_label            = 'Control plane',
    context_detail_template  = '{{config.tagline}}',
    nav_description_template = 'Return to the operating overview.',
    nav_keywords             = '["overview","dashboard","home"]'::jsonb,
    event_bus_kind           = NULL,
    keyboard_shortcut        = NULL,
    draft_guard_required     = FALSE,
    is_dynamic               = FALSE,
    tab_strip_position       = 10,
    updated_at               = now()
WHERE route_id = 'route.app.dashboard';

UPDATE ui_shell_route_registry SET
    component_ref            = 'moon/MoonBuildPage.MoonBuildPage',
    tab_kind_label           = 'Build',
    tab_label_template       = '{{moonRunId ? "Run view" : buildWorkflowId ? "Workflow workspace" : "New workflow"}}',
    context_label            = 'App builder',
    context_detail_template  = 'Shape the workflow graph, inspect detail, and release from one workspace.',
    nav_description_template = 'Jump back into Moon Build.',
    nav_keywords             = '["build","workflow","moon"]'::jsonb,
    event_bus_kind           = 'build',
    keyboard_shortcut        = 'ctrl+n',
    draft_guard_required     = TRUE,
    is_dynamic               = FALSE,
    tab_strip_position       = 20,
    updated_at               = now()
WHERE route_id = 'route.app.workflow';

UPDATE ui_shell_route_registry SET
    component_ref            = 'moon/MoonBuildPage.MoonBuildPage',
    tab_kind_label           = 'Build',
    tab_label_template       = '{{moonRunId ? "Run view" : buildWorkflowId ? "Workflow workspace" : "New workflow"}}',
    context_label            = 'App builder',
    context_detail_template  = 'Shape the workflow graph, inspect detail, and release from one workspace.',
    nav_description_template = 'Jump back into Moon Build.',
    nav_keywords             = '["build","workflow","moon"]'::jsonb,
    event_bus_kind           = 'build',
    keyboard_shortcut        = NULL,
    draft_guard_required     = TRUE,
    is_dynamic               = FALSE,
    tab_strip_position       = NULL,
    updated_at               = now()
WHERE route_id = 'route.app.build.legacy';

UPDATE ui_shell_route_registry SET
    component_ref            = 'moon/MoonBuildPage.MoonBuildPage',
    tab_kind_label           = 'Run',
    tab_label_template       = 'Run view',
    context_label            = 'Run observer',
    context_detail_template  = 'Trace the execution graph, inspect receipts, and jump to the source workflow.',
    nav_description_template = 'Return to the active run view.',
    nav_keywords             = '["run","trace","observer","execution"]'::jsonb,
    event_bus_kind           = 'run-detail',
    keyboard_shortcut        = NULL,
    draft_guard_required     = FALSE,
    is_dynamic               = FALSE,
    tab_strip_position       = NULL,
    updated_at               = now()
WHERE route_id = 'route.app.run';

UPDATE ui_shell_route_registry SET
    component_ref            = 'praxis/ManifestCatalogPage.ManifestCatalogPage',
    tab_kind_label           = 'Catalog',
    tab_label_template       = 'Manifests',
    context_label            = 'Manifest catalog',
    context_detail_template  = 'Discover control-plane manifests before opening them by exact id.',
    nav_description_template = 'Open the manifest catalog.',
    nav_keywords             = '["manifest","manifests","catalog","search","list","discover","control-plane","plan","approval"]'::jsonb,
    event_bus_kind           = NULL,
    keyboard_shortcut        = NULL,
    draft_guard_required     = FALSE,
    is_dynamic               = FALSE,
    tab_strip_position       = 40,
    updated_at               = now()
WHERE route_id = 'route.app.manifests';

UPDATE ui_shell_route_registry SET
    component_ref            = 'atlas/AtlasPage.AtlasPage',
    tab_kind_label           = 'Accent',
    tab_label_template       = 'Graph Diagram',
    context_label            = 'Knowledge graph',
    context_detail_template  = 'Graph view of memory entities and authority-linked edges across the system.',
    nav_description_template = 'Open the knowledge-graph diagram.',
    nav_keywords             = '["accent","atlas","graph","diagram","knowledge","memory","entities","map","overview"]'::jsonb,
    event_bus_kind           = NULL,
    keyboard_shortcut        = NULL,
    draft_guard_required     = FALSE,
    is_dynamic               = FALSE,
    tab_strip_position       = 30,
    updated_at               = now()
WHERE route_id = 'route.app.atlas';

-- Refresh data_dictionary_objects summary to reflect expanded role.
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
    'Authoritative DB-backed registry of app shell routes with component bindings, label/context templates, keyboard shortcuts, draft-guard flags, and reverse-URL canonical priority. Drives parseShellLocationState/buildShellUrl replacement in surfaces/app/src/shell/routeRegistry.ts.',
    '{"migration":"256_ui_shell_route_registry_expand_columns.sql"}'::jsonb,
    '{"authority_domain_ref":"authority.surface_catalog","decision_ref":"decision.shell_navigation_cqrs.20260426"}'::jsonb
)
ON CONFLICT (object_kind) DO UPDATE SET
    label      = EXCLUDED.label,
    category   = EXCLUDED.category,
    summary    = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata   = EXCLUDED.metadata,
    updated_at = now();

-- Update authority_object_registry metadata to anchor the new decision.
UPDATE authority_object_registry
   SET source_decision_ref = 'decision.shell_navigation_cqrs.20260426',
       metadata = metadata || jsonb_build_object(
           'expanded_in', '256_ui_shell_route_registry_expand_columns.sql'
       ),
       updated_at = now()
 WHERE object_ref = 'table.public.ui_shell_route_registry';

COMMIT;
