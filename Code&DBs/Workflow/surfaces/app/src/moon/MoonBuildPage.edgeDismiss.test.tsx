import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { MoonBuildPage } from './MoonBuildPage';
import type { BuildPayload } from '../shared/types';

const moonBuildPageDismissMocks = vi.hoisted(() => ({
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

const payload: BuildPayload = {
  definition: {},
  build_graph: {
    nodes: [
      {
        node_id: 'node-1',
        kind: 'step',
        title: 'Webhook',
        route: 'trigger/webhook',
        status: 'ready',
      },
      {
        node_id: 'node-2',
        kind: 'step',
        title: 'Next step',
        route: '',
      },
    ],
    edges: [
      {
        edge_id: 'edge-1-2',
        kind: 'sequence',
        from_node_id: 'node-1',
        to_node_id: 'node-2',
      },
    ],
  },
};

vi.mock('../shared/hooks/useBuildPayload', () => ({
  useBuildPayload: () => ({
    payload,
    loading: false,
    error: null,
    mutate: moonBuildPageDismissMocks.mutate,
    reload: moonBuildPageDismissMocks.reload,
    setPayload: moonBuildPageDismissMocks.setPayload,
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
  getCatalog: moonBuildPageDismissMocks.getCatalog,
  loadCatalog: moonBuildPageDismissMocks.loadCatalog,
}));

vi.mock('../menu', () => ({
  MenuPanel: () => null,
}));

vi.mock('../primitives/Toast', () => ({
  Toast: () => null,
  useToast: () => ({ show: moonBuildPageDismissMocks.showToast }),
}));

vi.mock('../control/UiActionFeed', () => ({
  UiActionFeed: () => null,
}));

vi.mock('../control/uiActionLedger', () => ({
  registerUiActionUndoExecutor: moonBuildPageDismissMocks.registerUndoExecutor,
  runUiAction: moonBuildPageDismissMocks.runUiAction,
  undoUiAction: moonBuildPageDismissMocks.undoUiAction,
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
  getEdgeGeometry: () => ({
    centerX: 180,
    centerY: 90,
    endX: 260,
    endY: 90,
    path: 'M0 0',
    startX: 100,
    startY: 90,
  }),
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

describe('MoonBuildPage edge dismiss', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    moonBuildPageDismissMocks.getCatalog.mockReturnValue([]);
    moonBuildPageDismissMocks.loadCatalog.mockImplementation(() => new Promise(() => undefined));
  });

  test('clicking away dismisses the selected gate card', () => {
    render(<MoonBuildPage workflowId="wf-123" />);

    fireEvent.click(screen.getByRole('button', { name: 'Select gate between Webhook and Next step' }));

    expect(screen.getByRole('button', { name: 'Edit gate' })).toBeInTheDocument();

    fireEvent.mouseDown(document.body);

    expect(screen.queryByRole('button', { name: 'Edit gate' })).not.toBeInTheDocument();
  });
});
