import {
  closeDynamicTab,
  composeShellId,
  createDefaultShellState,
  manifestEditorShellId,
  manifestTabShellId,
  runDetailShellId,
  upsertDynamicTab,
} from './state';

describe('shell state helpers', () => {
  test('default state anchors at the dashboard route', () => {
    const state = createDefaultShellState();
    expect(state.activeRouteId).toBe('route.app.dashboard');
    expect(state.activeTabId).toBe('dashboard');
    expect(state.dynamicTabs).toEqual([]);
    expect(state.buildView).toBe('canvas');
    expect(state.canvasRunId).toBeNull();
    expect(state.dashboardDetail).toBeNull();
  });

  test('shell id helpers produce unique deterministic ids', () => {
    expect(manifestTabShellId('manifest_123', 'details')).toBe('manifest:manifest_123:details');
    expect(manifestTabShellId('manifest_123')).toBe('manifest:manifest_123:main');
    expect(manifestEditorShellId('manifest_123')).toBe('manifest-editor:manifest_123');
    expect(runDetailShellId('run_123')).toBe('run-detail:run_123');
    expect(composeShellId('intent.foo', ['pillA', 'pillB'])).toBe('compose:intent.foo:pillA|pillB');
    expect(composeShellId('intent.foo')).toBe('compose:intent.foo:no-pills');
  });

  test('closeDynamicTab falls back to dashboard when no dynamic tabs remain', () => {
    const tab = {
      id: manifestTabShellId('manifest_123', 'main'),
      kind: 'manifest' as const,
      label: 'manifest_123',
      closable: true as const,
      manifestId: 'manifest_123',
      manifestTabId: 'main',
    };
    const result = closeDynamicTab([tab], tab.id, tab.id);
    expect(result.dynamicTabs).toEqual([]);
    expect(result.activeTabId).toBe('dashboard');
  });

  test('closeDynamicTab leaves activeTabId untouched when closing a non-active tab', () => {
    const tabA = {
      id: manifestTabShellId('a', 'main'),
      kind: 'manifest' as const,
      label: 'a',
      closable: true as const,
      manifestId: 'a',
      manifestTabId: 'main',
    };
    const tabB = {
      id: manifestTabShellId('b', 'main'),
      kind: 'manifest' as const,
      label: 'b',
      closable: true as const,
      manifestId: 'b',
      manifestTabId: 'main',
    };
    const result = closeDynamicTab([tabA, tabB], tabA.id, tabB.id);
    expect(result.dynamicTabs).toEqual([tabA]);
    expect(result.activeTabId).toBe(tabA.id);
  });

  test('upsertDynamicTab replaces an existing tab with the same id', () => {
    const original = {
      id: manifestTabShellId('manifest_123', 'main'),
      kind: 'manifest' as const,
      label: 'manifest_123',
      closable: true as const,
      manifestId: 'manifest_123',
      manifestTabId: 'main',
    };
    const updated = {
      ...original,
      label: 'manifest_123 · details',
      manifestTabId: 'details',
    };
    expect(upsertDynamicTab([original], updated)).toEqual([updated]);
  });

  test('upsertDynamicTab appends new tabs', () => {
    const tabA = {
      id: manifestTabShellId('a', 'main'),
      kind: 'manifest' as const,
      label: 'a',
      closable: true as const,
      manifestId: 'a',
      manifestTabId: 'main',
    };
    const tabB = {
      id: manifestTabShellId('b', 'main'),
      kind: 'manifest' as const,
      label: 'b',
      closable: true as const,
      manifestId: 'b',
      manifestTabId: 'main',
    };
    expect(upsertDynamicTab([tabA], tabB)).toEqual([tabA, tabB]);
  });
});
