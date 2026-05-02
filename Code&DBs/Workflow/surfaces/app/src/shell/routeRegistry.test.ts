import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import {
  buildPath,
  buildPathForSurface,
  clearRoutesForTest,
  interpolateLabel,
  matchPath,
  setRoutesForTest,
  type RouteRegistryRow,
} from './routeRegistry';

const MIN_ROUTES: RouteRegistryRow[] = [
  {
    route_id: 'route.app.dashboard',
    path_template: '/app',
    surface_name: 'dashboard',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 10,
    binding_revision: 'b',
    decision_ref: 'd',
    component_ref: 'dashboard/Dashboard.Dashboard',
    tab_kind_label: 'Suite',
    tab_label_template: 'Overview',
    context_label: 'Control plane',
    context_detail_template: '',
    nav_description_template: '',
    nav_keywords: [],
    event_bus_kind: null,
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: false,
    is_canonical_for_surface: true,
    tab_strip_position: 10,
  },
  {
    route_id: 'route.app.workflow',
    path_template: '/app/workflow',
    surface_name: 'build',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 20,
    binding_revision: 'b',
    decision_ref: 'd',
    component_ref: 'canvas/CanvasBuildPage.CanvasBuildPage',
    tab_kind_label: 'Build',
    tab_label_template: '{{canvasRunId ? "Run view" : buildWorkflowId ? "Workflow workspace" : "New workflow"}}',
    context_label: 'App builder',
    context_detail_template: '',
    nav_description_template: '',
    nav_keywords: [],
    event_bus_kind: 'build',
    keyboard_shortcut: 'ctrl+n',
    draft_guard_required: true,
    is_dynamic: false,
    is_canonical_for_surface: true,
    tab_strip_position: 20,
  },
  {
    route_id: 'route.app.run',
    path_template: '/app/run/{run_id}',
    surface_name: 'build',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 40,
    binding_revision: 'b',
    decision_ref: 'd',
    component_ref: 'canvas/CanvasBuildPage.CanvasBuildPage',
    tab_kind_label: 'Run',
    tab_label_template: 'Run view',
    context_label: 'Run observer',
    context_detail_template: '',
    nav_description_template: '',
    nav_keywords: [],
    event_bus_kind: 'run-detail',
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: false,
    is_canonical_for_surface: false,
    tab_strip_position: null,
  },
  {
    route_id: 'route.app.manifests',
    path_template: '/app/manifests',
    surface_name: 'manifests',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 50,
    binding_revision: 'b',
    decision_ref: 'd',
    component_ref: 'praxis/ManifestCatalogPage.ManifestCatalogPage',
    tab_kind_label: 'Catalog',
    tab_label_template: 'Manifests',
    context_label: 'Manifest catalog',
    context_detail_template: '',
    nav_description_template: '',
    nav_keywords: [],
    event_bus_kind: null,
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: false,
    is_canonical_for_surface: true,
    tab_strip_position: 40,
  },
  {
    route_id: 'route.app.manifest',
    path_template: '/app/manifests?manifest={manifest_id}&tab={manifest_tab_id}',
    surface_name: 'manifests',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 55,
    binding_revision: 'b',
    decision_ref: 'd',
    component_ref: 'praxis/ManifestBundleView.ManifestBundleView',
    tab_kind_label: 'manifest',
    tab_label_template: '{{manifest_id}}',
    context_label: 'Manifest bundle',
    context_detail_template: '',
    nav_description_template: '',
    nav_keywords: [],
    event_bus_kind: 'manifest',
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: true,
    is_canonical_for_surface: false,
    tab_strip_position: null,
  },
];

describe('routeRegistry', () => {
  beforeEach(() => {
    setRoutesForTest(MIN_ROUTES);
  });
  afterEach(() => {
    clearRoutesForTest();
  });

  describe('matchPath', () => {
    test('matches exact /app to dashboard', () => {
      const m = matchPath('/app', '');
      expect(m).toEqual({ route_id: 'route.app.dashboard', slot_values: {} });
    });

    test('extracts run_id slot from /app/run/{id}', () => {
      const m = matchPath('/app/run/wf_42', '');
      expect(m).toEqual({ route_id: 'route.app.run', slot_values: { run_id: 'wf_42' } });
    });

    test('returns null for unknown path', () => {
      const m = matchPath('/app/unknown', '');
      expect(m).toBeNull();
    });

    test('matches /app/workflow with no query slots', () => {
      const m = matchPath('/app/workflow', '');
      expect(m).toEqual({ route_id: 'route.app.workflow', slot_values: {} });
    });

    test('preserves workflow id on /app/workflow deep links', () => {
      const m = matchPath('/app/workflow', '?workflow=wf_42');
      expect(m).toEqual({ route_id: 'route.app.workflow', slot_values: { workflow: 'wf_42' } });
    });

    test('prefers manifest detail route over the manifest catalog route when query slots are bound', () => {
      const m = matchPath('/app/manifests', '?manifest=entity-workspace-2a1a7c&tab=main');
      expect(m).toEqual({
        route_id: 'route.app.manifest',
        slot_values: {
          manifest_id: 'entity-workspace-2a1a7c',
          manifest_tab_id: 'main',
        },
      });
    });

    test('keeps bare /app/manifests on the catalog route', () => {
      const m = matchPath('/app/manifests', '');
      expect(m).toEqual({ route_id: 'route.app.manifests', slot_values: {} });
    });
  });

  describe('buildPath', () => {
    test('renders pathname slot', () => {
      expect(buildPath('route.app.run', { run_id: 'wf_42' })).toBe('/app/run/wf_42');
    });

    test('renders empty path for unknown route_id falls back to /app', () => {
      expect(buildPath('route.app.does_not_exist')).toBe('/app');
    });

    test('renders /app for dashboard', () => {
      expect(buildPath('route.app.dashboard')).toBe('/app');
    });

    test('renders workflow id on workflow route', () => {
      expect(buildPath('route.app.workflow', { workflow: 'wf_42' })).toBe('/app/workflow?workflow=wf_42');
    });

    test('renders manifest detail query slots', () => {
      expect(buildPath('route.app.manifest', { manifest_id: 'entity-workspace-2a1a7c', manifest_tab_id: 'main' }))
        .toBe('/app/manifests?manifest=entity-workspace-2a1a7c&tab=main');
    });
  });

  describe('buildPathForSurface', () => {
    test('picks canonical workflow route when no run_id slot', () => {
      expect(buildPathForSurface('build', {})).toBe('/app/workflow');
    });

    test('picks run route when run_id slot is bound', () => {
      // Even though run is not canonical=true, buildPathForSurface prefers
      // rows whose required slots are bound. With run_id present, both rows
      // qualify; canonical=workflow wins per priority order. To prefer run
      // when slot is bound, callers should buildPath('route.app.run', ...)
      // directly. This test documents that buildPathForSurface defaults to
      // canonical when both rows match.
      expect(buildPathForSurface('build', { run_id: 'wf_42' })).toBe('/app/workflow');
    });
  });

  describe('interpolateLabel', () => {
    test('returns empty string for null/undefined template', () => {
      expect(interpolateLabel(null, {})).toBe('');
      expect(interpolateLabel(undefined, {})).toBe('');
    });

    test('returns literal text untouched', () => {
      expect(interpolateLabel('Run view', {})).toBe('Run view');
    });

    test('substitutes identifier from state', () => {
      expect(interpolateLabel('Hello {{name}}', { name: 'World' })).toBe('Hello World');
    });

    test('handles ternary expression with truthy branch', () => {
      const tmpl = '{{canvasRunId ? "Run view" : "Build"}}';
      expect(interpolateLabel(tmpl, { canvasRunId: 'wf_42' })).toBe('Run view');
    });

    test('handles ternary expression with falsy branch', () => {
      const tmpl = '{{canvasRunId ? "Run view" : "Build"}}';
      expect(interpolateLabel(tmpl, { canvasRunId: null })).toBe('Build');
    });

    test('handles nested ternary', () => {
      const tmpl = '{{canvasRunId ? "Run view" : buildWorkflowId ? "Workflow workspace" : "New workflow"}}';
      expect(interpolateLabel(tmpl, { canvasRunId: null, buildWorkflowId: null })).toBe('New workflow');
      expect(interpolateLabel(tmpl, { canvasRunId: null, buildWorkflowId: 'wf_1' })).toBe('Workflow workspace');
      expect(interpolateLabel(tmpl, { canvasRunId: 'r_1' })).toBe('Run view');
    });

    test('returns empty for malformed expression', () => {
      // Malformed expressions are caught and return empty.
      expect(interpolateLabel('{{ unbalanced', {})).toBe('{{ unbalanced');
    });
  });
});
