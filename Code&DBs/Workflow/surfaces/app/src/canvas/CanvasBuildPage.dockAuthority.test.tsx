import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { CanvasBuildPage } from './CanvasBuildPage';
import type { BuildPayload } from '../shared/types';

const canvasBuildPageDockMocks = vi.hoisted(() => ({
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
    payload: canvasBuildPageDockMocks.payload,
    loading: false,
    error: null,
    mutate: canvasBuildPageDockMocks.mutate,
    reload: canvasBuildPageDockMocks.reload,
    setPayload: canvasBuildPageDockMocks.setPayload,
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
  getCatalog: canvasBuildPageDockMocks.getCatalog,
  loadCatalog: canvasBuildPageDockMocks.loadCatalog,
}));

vi.mock('../menu', () => ({
  MenuPanel: () => null,
}));

vi.mock('../primitives/Toast', () => ({
  Toast: () => null,
  useToast: () => ({ show: canvasBuildPageDockMocks.showToast }),
}));

vi.mock('../control/UiActionFeed', () => ({
  UiActionFeed: () => null,
}));

vi.mock('../control/uiActionLedger', () => ({
  registerUiActionUndoExecutor: canvasBuildPageDockMocks.registerUndoExecutor,
  runUiAction: canvasBuildPageDockMocks.runUiAction,
  undoUiAction: canvasBuildPageDockMocks.undoUiAction,
}));

vi.mock('./CanvasActionDock', () => ({
  CanvasActionDock: () => null,
}));

vi.mock('./CanvasNodeDetail', () => ({
  CanvasNodeDetail: () => <div data-testid="node-detail">Node detail</div>,
}));

vi.mock('./CanvasReleaseTray', () => ({
  CanvasReleaseTray: () => <div data-testid="release-tray">Release tray</div>,
}));

vi.mock('./CanvasRunPanel', () => ({
  CanvasRunPanel: () => null,
}));

vi.mock('./CanvasDragGhost', () => ({
  CanvasDragGhost: () => null,
}));

vi.mock('./CanvasEdges', () => ({
  CanvasEdges: () => null,
  getEdgeGeometry: () => null,
}));

vi.mock('./CanvasPopout', () => ({
  CanvasPopout: () => <div data-testid="node-popout">Node popout</div>,
}));

vi.mock('./useCanvasDrag', () => ({
  useCanvasDrag: () => ({
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

describe('CanvasBuildPage dock authority', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    canvasBuildPageDockMocks.payload = payload;
    canvasBuildPageDockMocks.getCatalog.mockReturnValue([]);
    canvasBuildPageDockMocks.loadCatalog.mockImplementation(() => new Promise(() => undefined));
  });

  test('single click opens the detail dock and does not open the node popout', () => {
    render(<CanvasBuildPage workflowId="wf-123" />);

    fireEvent.click(screen.getByText('Webhook'));

    expect(screen.getByTestId('node-detail')).toBeInTheDocument();
    expect(screen.queryByTestId('node-popout')).not.toBeInTheDocument();

    fireEvent.click(screen.getByText('Webhook'));

    expect(screen.queryByTestId('node-popout')).not.toBeInTheDocument();
  });

  test('double clicking a node opens the node popout', () => {
    render(<CanvasBuildPage workflowId="wf-123" />);

    fireEvent.doubleClick(screen.getByText('Webhook'));

    expect(screen.getByTestId('node-popout')).toBeInTheDocument();
  });

  test('opening release closes the node popout and keeps it closed while release is open', () => {
    render(<CanvasBuildPage workflowId="wf-123" />);

    fireEvent.click(screen.getByText('Webhook'));
    fireEvent.doubleClick(screen.getByText('Webhook'));

    expect(screen.getByTestId('node-popout')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Open release checklist' }));

    expect(screen.getByTestId('release-tray')).toBeInTheDocument();
    expect(screen.queryByTestId('node-popout')).not.toBeInTheDocument();

    fireEvent.click(screen.getByText('Webhook'));

    expect(screen.queryByTestId('node-popout')).not.toBeInTheDocument();
  });

  test('shows review as the primary decision and keeps release blocked in the top dock', () => {
    canvasBuildPageDockMocks.payload = {
      ...payload,
      binding_ledger: [
        {
          binding_id: 'binding-routing',
          source_label: 'Routing authority',
          state: 'suggested',
          candidate_targets: [{ target_ref: 'tool:canvas_mutate_field', label: 'Canvas field editor' }],
        },
      ],
    };

    const { container } = render(<CanvasBuildPage workflowId="wf-123" />);

    expect(screen.getByRole('button', { name: 'Open review decisions' })).toHaveTextContent('Review 1 decision');
    expect(screen.getByRole('button', { name: 'Open release checklist' })).toHaveTextContent('Release blocked');
    expect(container.querySelector('.canvas-halfcanvas--bottom')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Open release checklist' }));

    expect(screen.getByText('Review readiness (1)')).toBeInTheDocument();
    expect(screen.queryByTestId('release-tray')).not.toBeInTheDocument();
  });
});
