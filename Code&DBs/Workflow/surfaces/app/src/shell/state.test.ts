import {
  buildShellUrl,
  closeDynamicTab,
  createDefaultShellState,
  manifestTabShellId,
  parseShellLocationState,
  runDetailShellId,
  upsertDynamicTab,
} from './state';

describe('shell state helpers', () => {
  test('parses manifest query params into a dynamic tab', () => {
    const payload = parseShellLocationState('?manifest=manifest_123&tab=details');

    expect(payload.shellState.activeTabId).toBe(manifestTabShellId('manifest_123', 'details'));
    expect(payload.shellState.dynamicTabs[0]).toMatchObject({
      kind: 'manifest',
      manifestId: 'manifest_123',
      manifestTabId: 'details',
    });
  });

  test('builds run detail urls from the active dynamic tab', () => {
    const state = {
      ...createDefaultShellState(),
      activeTabId: runDetailShellId('run_123'),
      dynamicTabs: [
        {
          id: runDetailShellId('run_123'),
          kind: 'run-detail' as const,
          label: 'Run run_123',
          closable: true as const,
          runId: 'run_123',
        },
      ],
    };

    expect(buildShellUrl(state, false)).toBe('/app/run/run_123');
  });

  test('parses build query params into the build tab', () => {
    const payload = parseShellLocationState('?page=build&workflow=wf_123&intent=research');

    expect(payload.shellState.activeTabId).toBe('build');
    expect(payload.shellState.buildWorkflowId).toBe('wf_123');
    expect(payload.shellState.buildIntent).toBe('research');
    expect(payload.shellState.buildView).toBe('moon');
  });

  test('parses costs urls into the costs tab', () => {
    const payload = parseShellLocationState('?page=costs', '/app/costs');

    expect(payload.shellState.activeTabId).toBe('costs');
    expect(payload.chatOpen).toBe(false);
  });

  test('parses manifests urls into the manifest catalog tab', () => {
    const payload = parseShellLocationState('?page=manifests', '/app/manifests');

    expect(payload.shellState.activeTabId).toBe('manifests');
    expect(payload.chatOpen).toBe(false);
  });

  test('maps legacy edit-model urls into the build tab', () => {
    const payload = parseShellLocationState('?page=edit-model&workflow=wf_legacy&surface=details');

    expect(payload.shellState.activeTabId).toBe('build');
    expect(payload.shellState.buildWorkflowId).toBe('wf_legacy');
    expect(payload.shellState.buildView).toBe('moon');
  });

  test('builds build urls with workflow and intent', () => {
    const state = {
      ...createDefaultShellState(),
      activeTabId: 'build' as const,
      buildWorkflowId: 'wf_moon',
      buildIntent: 'research',
      buildView: 'moon' as const,
    };

    expect(buildShellUrl(state, false)).toBe('/app/build?workflow=wf_moon&intent=research');
  });

  test('builds costs urls from the active static tab', () => {
    const state = {
      ...createDefaultShellState(),
      activeTabId: 'costs' as const,
    };

    expect(buildShellUrl(state, false)).toBe('/app/costs');
  });

  test('builds manifest catalog urls from the active static tab', () => {
    const state = {
      ...createDefaultShellState(),
      activeTabId: 'manifests' as const,
    };

    expect(buildShellUrl(state, false)).toBe('/app/manifests');
  });

  test('closing the active dynamic tab falls back to dashboard when none remain', () => {
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
});
