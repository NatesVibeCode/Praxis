import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { MoonBuildPage } from './MoonBuildPage';
import type { BuildPayload } from '../shared/types';

const moonBuildPageDockMocks = vi.hoisted(() => ({
  getCatalog: vi.fn(),
  loadCatalog: vi.fn(),
  mutate: vi.fn(),
  payload: null as BuildPayload | null,
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
    payload: moonBuildPageDockMocks.payload,
    loading: false,
    error: null,
    mutate: moonBuildPageDockMocks.mutate,
    reload: moonBuildPageDockMocks.reload,
    setPayload: moonBuildPageDockMocks.setPayload,
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
  getCatalog: moonBuildPageDockMocks.getCatalog,
  loadCatalog: moonBuildPageDockMocks.loadCatalog,
}));

vi.mock('../menu', () => ({
  MenuPanel: () => null,
}));

vi.mock('../primitives/Toast', () => ({
  Toast: () => null,
  useToast: () => ({ show: moonBuildPageDockMocks.showToast }),
}));

vi.mock('../control/UiActionFeed', () => ({
  UiActionFeed: () => null,
}));

vi.mock('../control/uiActionLedger', () => ({
  registerUiActionUndoExecutor: moonBuildPageDockMocks.registerUndoExecutor,
  runUiAction: moonBuildPageDockMocks.runUiAction,
  undoUiAction: moonBuildPageDockMocks.undoUiAction,
}));

vi.mock('./MoonActionDock', () => ({
  MoonActionDock: () => null,
}));

vi.mock('./MoonNodeDetail', () => ({
  MoonNodeDetail: () => <div data-testid="node-detail">Node detail</div>,
}));

vi.mock('./MoonReleaseTray', () => ({
  MoonReleaseTray: () => <div data-testid="release-tray">Release tray</div>,
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
  MoonPopout: () => <div data-testid="node-popout">Node popout</div>,
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

describe('MoonBuildPage dock authority', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    moonBuildPageDockMocks.payload = payload;
    moonBuildPageDockMocks.getCatalog.mockReturnValue([]);
    moonBuildPageDockMocks.loadCatalog.mockImplementation(() => new Promise(() => undefined));
  });

  test('single click opens the detail dock and does not open the node popout', () => {
    render(<MoonBuildPage workflowId="wf-123" />);

    fireEvent.click(screen.getByText('Webhook'));

    expect(screen.getByTestId('node-detail')).toBeInTheDocument();
    expect(screen.queryByTestId('node-popout')).not.toBeInTheDocument();

    fireEvent.click(screen.getByText('Webhook'));

    expect(screen.queryByTestId('node-popout')).not.toBeInTheDocument();
  });

  test('double clicking a node opens the node popout', () => {
    render(<MoonBuildPage workflowId="wf-123" />);

    fireEvent.doubleClick(screen.getByText('Webhook'));

    expect(screen.getByTestId('node-popout')).toBeInTheDocument();
  });

  test('opening release closes the node popout and keeps it closed while release is open', () => {
    render(<MoonBuildPage workflowId="wf-123" />);

    fireEvent.click(screen.getByText('Webhook'));
    fireEvent.doubleClick(screen.getByText('Webhook'));

    expect(screen.getByTestId('node-popout')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Open Release dock' }));

    expect(screen.getByTestId('release-tray')).toBeInTheDocument();
    expect(screen.queryByTestId('node-popout')).not.toBeInTheDocument();

    fireEvent.click(screen.getByText('Webhook'));

    expect(screen.queryByTestId('node-popout')).not.toBeInTheDocument();
  });
});
