import { describe, expect, test } from 'vitest';
import { routeSlotState } from './useShellState';
import { manifestTabShellId, type DynamicTab } from './state';
import type { RouteRegistryRow } from './routeRegistry';

const ROUTES: RouteRegistryRow[] = [
  {
    route_id: 'route.app.manifest',
    path_template: '/app/manifests?manifest={manifest_id}&tab={manifest_tab_id}',
    surface_name: 'manifests',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 50,
    binding_revision: 'test',
    decision_ref: 'test',
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

describe('routeSlotState', () => {
  test('materializes manifest deep links into a dynamic active tab', () => {
    const patch = routeSlotState(
      'route.app.manifest',
      { manifest_id: 'entity-workspace-2a1a7c', manifest_tab_id: 'main' },
      ROUTES,
    );

    expect(patch.activeRouteId).toBe('route.app.manifest');
    expect(patch.activeTabId).toBe(manifestTabShellId('entity-workspace-2a1a7c', 'main'));
    expect(patch.dynamicTabs).toEqual([
      {
        id: manifestTabShellId('entity-workspace-2a1a7c', 'main'),
        kind: 'manifest',
        label: 'Entity Workspace',
        closable: true,
        manifestId: 'entity-workspace-2a1a7c',
        manifestTabId: 'main',
      },
    ]);
  });

  test('keeps existing dynamic tabs when adding the route-backed tab', () => {
    const existing: DynamicTab = {
      id: manifestTabShellId('blank-workspace-2df889', 'main'),
      kind: 'manifest',
      label: 'Compose',
      closable: true,
      manifestId: 'blank-workspace-2df889',
      manifestTabId: 'main',
    };

    const patch = routeSlotState(
      'route.app.manifest',
      { manifest_id: 'entity-workspace-2a1a7c', manifest_tab_id: 'main' },
      ROUTES,
      [existing],
    );

    expect(patch.dynamicTabs).toEqual([
      existing,
      {
        id: manifestTabShellId('entity-workspace-2a1a7c', 'main'),
        kind: 'manifest',
        label: 'Entity Workspace',
        closable: true,
        manifestId: 'entity-workspace-2a1a7c',
        manifestTabId: 'main',
      },
    ]);
  });
});
