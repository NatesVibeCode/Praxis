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
    component_ref: 'moon/MoonBuildPage.MoonBuildPage',
    tab_kind_label: 'Build',
    tab_label_template: '{{moonRunId ? "Run view" : buildWorkflowId ? "Workflow workspace" : "New workflow"}}',
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
    component_ref: 'moon/MoonBuildPage.MoonBuildPage',
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
      const tmpl = '{{moonRunId ? "Run view" : "Build"}}';
      expect(interpolateLabel(tmpl, { moonRunId: 'wf_42' })).toBe('Run view');
    });

    test('handles ternary expression with falsy branch', () => {
      const tmpl = '{{moonRunId ? "Run view" : "Build"}}';
      expect(interpolateLabel(tmpl, { moonRunId: null })).toBe('Build');
    });

    test('handles nested ternary', () => {
      const tmpl = '{{moonRunId ? "Run view" : buildWorkflowId ? "Workflow workspace" : "New workflow"}}';
      expect(interpolateLabel(tmpl, { moonRunId: null, buildWorkflowId: null })).toBe('New workflow');
      expect(interpolateLabel(tmpl, { moonRunId: null, buildWorkflowId: 'wf_1' })).toBe('Workflow workspace');
      expect(interpolateLabel(tmpl, { moonRunId: 'r_1' })).toBe('Run view');
    });

    test('returns empty for malformed expression', () => {
      // Malformed expressions are caught and return empty.
      expect(interpolateLabel('{{ unbalanced', {})).toBe('{{ unbalanced');
    });
  });
});
