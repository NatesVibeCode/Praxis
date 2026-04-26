-- Migration 257: Add dynamic-kind rows to ui_shell_route_registry and flip
-- canonical-for-surface flags on the static rows.
--
-- Today the React shell has dynamic tab kinds (compose / manifest /
-- manifest-editor / run-detail) and a Costs detail drill-in that have no
-- registry rows — they live entirely in App.tsx + state.ts. This migration
-- registers them so the shell can render every navigable surface from one
-- DB-backed source.
--
-- Canonical priority: when state→URL reverse builds for surface_name='build',
-- two rows could match (route.app.workflow vs route.app.run). Reverse-build
-- picks among canonical rows by checking which slot is bound (moonRunId →
-- run, no moonRunId → workflow). The legacy /app/build alias is never
-- canonical.
--
-- Anchored to decision.shell_navigation_cqrs.20260426.

BEGIN;

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
    decision_ref,
    component_ref,
    tab_kind_label,
    tab_label_template,
    context_label,
    context_detail_template,
    nav_description_template,
    nav_keywords,
    event_bus_kind,
    keyboard_shortcut,
    draft_guard_required,
    is_dynamic,
    is_canonical_for_surface,
    tab_strip_position
) VALUES
    (
        'route.app.compose',
        '/app/compose?intent={intent}&pill={pill_refs[]}',
        'compose',
        'activeTabId={dynamic_tab_id}, dynamicTabs+={kind=compose,intent,pillRefs}',
        'Compose-from-intent surface tab. URL slots are intent (required) and pill (repeatable).',
        '["Code&DBs/Workflow/surfaces/app/src/praxis/SurfaceComposeView.tsx","Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        70,
        'binding.ui_shell_route_registry.dynamic_kinds.20260426',
        'decision.shell_navigation_cqrs.20260426',
        'praxis/SurfaceComposeView.SurfaceComposeView',
        'Compose',
        '{{intent ? ("Compose " + intent + (pillRefs.length > 0 ? (" · " + pillRefs.length + " pill" + (pillRefs.length === 1 ? "" : "s")) : "")) : "Compose surface"}}',
        'Compose surface',
        '{{intent ? ("Compiled from " + intent + (pillRefs.length > 0 ? (" + " + pillRefs.length + " pill" + (pillRefs.length === 1 ? "" : "s")) : "") + " via legal_templates projection.") : "Compile an experience template from intent + pills through legal_templates."}}',
        'Open the composed surface.',
        '["compose","intent","pill","experience","template"]'::jsonb,
        NULL,
        NULL,
        FALSE,
        TRUE,
        TRUE,
        NULL
    ),
    (
        'route.app.manifest',
        '/app/manifests?manifest={manifest_id}&tab={manifest_tab_id}',
        'manifest',
        'activeTabId={dynamic_tab_id}, dynamicTabs+={kind=manifest,manifestId,manifestTabId}',
        'Manifest bundle view tab. Opens a specific manifest by id, optionally on a named sub-tab.',
        '["Code&DBs/Workflow/surfaces/app/src/praxis/ManifestBundleView.tsx","Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        80,
        'binding.ui_shell_route_registry.dynamic_kinds.20260426',
        'decision.shell_navigation_cqrs.20260426',
        'praxis/ManifestBundleView.ManifestBundleView',
        'Surface',
        '{{manifest_tab_id && manifest_tab_id !== "main" ? (manifest_id + " · " + manifest_tab_id) : manifest_id}}',
        'Surface tab',
        'Review live manifest output alongside the builder and run detail tabs.',
        'Open the surface tab.',
        '["surface","manifest","tab"]'::jsonb,
        'manifest',
        NULL,
        FALSE,
        TRUE,
        TRUE,
        NULL
    ),
    (
        'route.app.manifest_editor',
        '/app/manifests?manifest=editor&target={manifest_id}',
        'manifest-editor',
        'activeTabId={dynamic_tab_id}, dynamicTabs+={kind=manifest-editor,manifestId}',
        'Manifest editor tab. Edits the surface contract for a specific manifest.',
        '["Code&DBs/Workflow/surfaces/app/src/grid/ManifestEditorPage.tsx","Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        90,
        'binding.ui_shell_route_registry.dynamic_kinds.20260426',
        'decision.shell_navigation_cqrs.20260426',
        'grid/ManifestEditorPage.ManifestEditorPage',
        'Editor',
        '{{"Edit " + manifest_id}}',
        'Manifest editor',
        'Edit the surface contract directly and reopen the live tab when you are ready.',
        'Open the manifest editor tab.',
        '["editor","manifest","edit","contract"]'::jsonb,
        'manifest-editor',
        NULL,
        FALSE,
        TRUE,
        TRUE,
        NULL
    ),
    (
        'route.app.run_detail_legacy',
        '/app/run/{run_id}/legacy',
        'run-detail',
        'activeTabId={dynamic_tab_id}, dynamicTabs+={kind=run-detail,runId}',
        'Legacy run-detail dynamic tab (Moon now owns run rendering via /app/run/{run_id}). Kept for any flows still creating dynamic run tabs.',
        '["Code&DBs/Workflow/surfaces/app/src/dashboard/RunDetailView.tsx","Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'legacy',
        TRUE,
        100,
        'binding.ui_shell_route_registry.dynamic_kinds.20260426',
        'decision.shell_navigation_cqrs.20260426',
        'dashboard/RunDetailView.RunDetailView',
        'Run',
        '{{"Run " + run_id}}',
        'Run detail',
        'Trace execution, inspect jobs, and jump back into the builder without losing context.',
        'Open the run detail tab.',
        '["run","detail","trace","execution"]'::jsonb,
        'run-detail',
        NULL,
        FALSE,
        TRUE,
        FALSE,
        NULL
    ),
    (
        'route.app.dashboard_costs',
        '/app?detail=costs',
        'dashboard',
        'activeTabId=dashboard, dashboardDetail=costs',
        'Costs drill-in on the Overview surface. Not a top-level tab — auxiliary view rendered inside Dashboard.',
        '["Code&DBs/Workflow/surfaces/app/src/dashboard/CostsPanel.tsx","Code&DBs/Workflow/surfaces/app/src/shell/state.ts"]'::jsonb,
        'ready',
        TRUE,
        15,
        'binding.ui_shell_route_registry.dynamic_kinds.20260426',
        'decision.shell_navigation_cqrs.20260426',
        'dashboard/CostsPanel.CostsPanel',
        'Suite',
        'Costs',
        'Cost summary',
        'Token spend and cost trends across runs.',
        'Open the cost summary drill-in.',
        '["costs","spend","tokens","money"]'::jsonb,
        NULL,
        NULL,
        FALSE,
        FALSE,
        FALSE,
        NULL
    )
ON CONFLICT (route_id) DO UPDATE SET
    path_template            = EXCLUDED.path_template,
    surface_name             = EXCLUDED.surface_name,
    state_effect             = EXCLUDED.state_effect,
    notes                    = EXCLUDED.notes,
    source_refs              = EXCLUDED.source_refs,
    status                   = EXCLUDED.status,
    enabled                  = EXCLUDED.enabled,
    display_order            = EXCLUDED.display_order,
    binding_revision         = EXCLUDED.binding_revision,
    decision_ref             = EXCLUDED.decision_ref,
    component_ref            = EXCLUDED.component_ref,
    tab_kind_label           = EXCLUDED.tab_kind_label,
    tab_label_template       = EXCLUDED.tab_label_template,
    context_label            = EXCLUDED.context_label,
    context_detail_template  = EXCLUDED.context_detail_template,
    nav_description_template = EXCLUDED.nav_description_template,
    nav_keywords             = EXCLUDED.nav_keywords,
    event_bus_kind           = EXCLUDED.event_bus_kind,
    keyboard_shortcut        = EXCLUDED.keyboard_shortcut,
    draft_guard_required     = EXCLUDED.draft_guard_required,
    is_dynamic               = EXCLUDED.is_dynamic,
    is_canonical_for_surface = EXCLUDED.is_canonical_for_surface,
    tab_strip_position       = EXCLUDED.tab_strip_position,
    updated_at               = now();

-- Flip canonical flags on the existing static rows. Order: workflow before
-- run because the partial unique index is satisfied as long as no two rows
-- collide on (surface_name) where canonical AND status='ready'. Since
-- workflow.surface_name='build' and run.surface_name='build' both want
-- canonical=TRUE, we need to choose ONE per surface_name as the strict
-- canonical and let the reverse-build matcher pick between them by slot.
--
-- Decision: route.app.workflow is canonical for surface_name='build' (the
-- "no moonRunId" path is the default). route.app.run gets is_canonical=FALSE
-- but the reverse-build matcher prefers it whenever moonRunId is set in the
-- shell state diff. The flag means "default reverse target"; the matcher
-- itself encodes the slot-priority rule.

UPDATE ui_shell_route_registry SET
    is_canonical_for_surface = TRUE,
    updated_at = now()
WHERE route_id IN (
    'route.app.dashboard',
    'route.app.workflow',
    'route.app.manifests',
    'route.app.atlas'
);

UPDATE ui_shell_route_registry SET
    is_canonical_for_surface = FALSE,
    updated_at = now()
WHERE route_id IN (
    'route.app.build.legacy',
    'route.app.run'
);

COMMIT;
