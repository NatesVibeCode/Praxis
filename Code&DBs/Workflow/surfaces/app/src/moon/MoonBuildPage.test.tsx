import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { MoonBuildPage } from './MoonBuildPage';
import type { BuildPayload } from '../shared/types';

const moonBuildPageMocks = vi.hoisted(() => ({
  getCatalog: vi.fn(),
  loadCatalog: vi.fn(),
  mutate: vi.fn(),
  registerUndoExecutor: vi.fn(() => () => undefined),
  reload: vi.fn(),
  runUiAction: vi.fn(async ({ apply }: { apply: () => void | Promise<void> }) => {
    await apply();
    return { id: 'ui-action-1', undoable: false };
  }),
  setPayload: vi.fn(),
  showToast: vi.fn(),
  undoUiAction: vi.fn(),
}));

vi.mock('../shared/hooks/useBuildPayload', () => ({
  useBuildPayload: () => ({
    payload: null,
    loading: false,
    error: null,
    mutate: moonBuildPageMocks.mutate,
    reload: moonBuildPageMocks.reload,
    setPayload: moonBuildPageMocks.setPayload,
  }),
}));

vi.mock('../dashboard/useLiveRunSnapshot', () => ({
  useLiveRunSnapshot: () => ({
    run: null,
    loading: false,
    error: null,
    streamStatus: 'idle',
    refresh: vi.fn(),
  }),
}));

vi.mock('./catalog', () => ({
  getCatalog: moonBuildPageMocks.getCatalog,
  loadCatalog: moonBuildPageMocks.loadCatalog,
}));

vi.mock('../menu', () => ({
  MenuPanel: ({
    open,
    title,
    sections,
  }: {
    open: boolean;
    title?: string;
    sections: Array<{ id: string; items: Array<{ id: string; label: string; disabled?: boolean; onSelect: () => void }> }>;
  }) => {
    if (!open) return null;
    return (
      <div aria-label={title || 'menu'}>
        {sections.flatMap((section) =>
          section.items.map((item) => (
            <button key={item.id} type="button" disabled={item.disabled} onClick={item.onSelect}>
              {item.label}
            </button>
          )),
        )}
      </div>
    );
  },
}));

vi.mock('../primitives/Toast', () => ({
  Toast: () => null,
  useToast: () => ({ show: moonBuildPageMocks.showToast }),
}));

vi.mock('../control/UiActionFeed', () => ({
  UiActionFeed: () => null,
}));

vi.mock('../control/uiActionLedger', () => ({
  registerUiActionUndoExecutor: moonBuildPageMocks.registerUndoExecutor,
  runUiAction: moonBuildPageMocks.runUiAction,
  undoUiAction: moonBuildPageMocks.undoUiAction,
}));

vi.mock('./MoonActionDock', () => ({
  MoonActionDock: () => null,
}));

vi.mock('./MoonNodeDetail', () => ({
  MoonNodeDetail: () => null,
}));

vi.mock('./MoonReleaseTray', () => ({
  MoonReleaseTray: () => null,
}));

vi.mock('./MoonRunPanel', () => ({
  MoonRunPanel: () => null,
}));

vi.mock('./MoonDragGhost', () => ({
  MoonDragGhost: () => null,
}));

vi.mock('./MoonEdges', () => ({
  MoonEdges: () => null,
  getEdgeGeometry: () => null,
}));

vi.mock('./MoonPopout', () => ({
  MoonPopout: () => null,
}));

vi.mock('./useMoonDrag', () => ({
  useMoonDrag: () => ({
    drag: {
      active: false,
      payload: null,
      ghostX: 0,
      ghostY: 0,
      hoveredTarget: null,
    },
    startDrag: vi.fn(),
    cancelDrag: vi.fn(),
  }),
}));

describe('MoonBuildPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    const triggerCatalog = [
      {
        id: 'trigger-manual',
        label: 'Manual',
        icon: 'trigger',
        family: 'trigger',
        status: 'ready',
        dropKind: 'node',
        actionValue: 'trigger',
        description: 'User-initiated run',
      },
    ];
    moonBuildPageMocks.getCatalog.mockReturnValue(triggerCatalog);
    moonBuildPageMocks.loadCatalog.mockResolvedValue(triggerCatalog);
  });

  test('seeds a two-step draft graph when a trigger is selected from the picker', async () => {
    render(
      <MoonBuildPage
        workflowId={null}
        initialMode="trigger-picker"
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: 'Manual' }));

    await waitFor(() => {
      expect(moonBuildPageMocks.setPayload).toHaveBeenCalledTimes(1);
    });

    const nextPayload = moonBuildPageMocks.setPayload.mock.calls[0][0] as BuildPayload;

    expect(nextPayload.build_graph?.nodes).toHaveLength(2);
    expect(nextPayload.build_graph?.nodes?.[0]).toMatchObject({
      node_id: 'node-1',
      title: 'Manual',
      route: 'trigger',
      status: 'ready',
      trigger: {
        event_type: 'manual',
        filter: {},
      },
    });
    expect(nextPayload.build_graph?.nodes?.[1]).toMatchObject({
      node_id: 'node-2',
      title: 'Next step',
      route: '',
    });
    expect(nextPayload.build_graph?.edges).toEqual([
      {
        edge_id: 'edge-1-2',
        kind: 'sequence',
        from_node_id: 'node-1',
        to_node_id: 'node-2',
      },
    ]);
  });
});
