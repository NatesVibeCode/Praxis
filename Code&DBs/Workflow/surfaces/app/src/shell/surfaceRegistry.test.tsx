import { vi } from 'vitest';

import { buildShellNavigationItems, buildShellTabs, resolveActiveShellSurface } from './surfaceRegistry';
import { createDefaultShellState, runDetailShellId } from './state';

describe('surfaceRegistry', () => {
  test('drives tabs, navigate items, and active context from the same surface definitions', () => {
    const state = {
      ...createDefaultShellState(),
      activeTabId: runDetailShellId('run_123'),
      buildWorkflowId: 'wf_alpha',
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
    const activeDynamicTab = state.dynamicTabs[0];

    const tabs = buildShellTabs(state);
    expect(tabs).toEqual([
      { id: 'dashboard', label: 'Overview', kind: 'Suite', closable: false },
      { id: 'build', label: 'Workflow workspace', kind: 'Build', closable: false },
      { id: 'costs', label: 'Cost Summary', kind: 'Finance', closable: false },
      { id: 'atlas', label: 'Graph Diagram', kind: 'Accent', closable: false },
      { id: 'manifests', label: 'Manifests', kind: 'Catalog', closable: false },
      { id: 'run-detail:run_123', label: 'Run run_123', kind: 'Run', closable: true },
    ]);

    const activateTab = vi.fn();
    const setChatOpen = vi.fn();
    const navigateItems = buildShellNavigationItems({
      state,
      chatOpen: false,
      activateTab,
      setChatOpen,
    });

    expect(navigateItems.find((item) => item.id === 'navigate:build')).toMatchObject({
      label: 'Workflow workspace',
      description: 'Jump back into Moon Build.',
      selected: false,
    });
    expect(navigateItems.find((item) => item.id === 'navigate:costs')).toMatchObject({
      label: 'Cost Summary',
      description: 'Inspect token spend and recent costed runs.',
      selected: false,
    });
    expect(navigateItems.find((item) => item.id === 'navigate:manifests')).toMatchObject({
      label: 'Manifests',
      description: 'Open the manifest catalog.',
      selected: false,
    });
    expect(navigateItems.find((item) => item.id === 'navigate:atlas')).toMatchObject({
      label: 'Graph Diagram',
      description: 'Open the knowledge-graph diagram.',
      selected: false,
    });
    expect(navigateItems.find((item) => item.id === 'tab:run-detail:run_123')).toMatchObject({
      label: 'Run run_123',
      description: 'Open the run detail tab.',
      selected: true,
    });

    const activeSurface = resolveActiveShellSurface(state, activeDynamicTab);
    expect(activeSurface).toMatchObject({
      category: 'dynamic',
      kind: 'run-detail',
      context: {
        label: 'Run detail',
        detail: 'Trace execution, inspect jobs, and jump back into the builder without losing context.',
      },
    });

    expect(resolveActiveShellSurface({ ...state, activeTabId: 'manifests' }, null)).toMatchObject({
      category: 'static',
      id: 'manifests',
      context: {
        label: 'Manifest catalog',
        detail: 'Discover control-plane manifests before opening them by exact id.',
      },
    });
  });

  test('labels /app/run/:id as a run observer instead of a new workflow', () => {
    const state = {
      ...createDefaultShellState(),
      activeTabId: 'build' as const,
      moonRunId: 'workflow.chain.demo.001',
    };

    expect(buildShellTabs(state).find((tab) => tab.id === 'build')).toMatchObject({
      kind: 'Run',
      label: 'Run view',
    });

    expect(resolveActiveShellSurface(state, null)).toMatchObject({
      category: 'static',
      id: 'build',
      context: {
        label: 'Run observer',
        detail: 'Trace the execution graph, inspect receipts, and jump to the source workflow.',
      },
    });

    const navigateItems = buildShellNavigationItems({
      state,
      chatOpen: false,
      activateTab: vi.fn(),
      setChatOpen: vi.fn(),
    });
    expect(navigateItems.find((item) => item.id === 'navigate:build')).toMatchObject({
      label: 'Run view',
      description: 'Return to the active run view.',
      selected: true,
    });
  });
});
