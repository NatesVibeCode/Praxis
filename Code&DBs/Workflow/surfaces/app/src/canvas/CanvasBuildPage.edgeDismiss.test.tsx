import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { CanvasBuildPage } from './CanvasBuildPage';
import type { BuildPayload } from '../shared/types';
import { withBuildEdgeRelease } from '../shared/edgeRelease';

const canvasBuildPageDismissMocks = vi.hoisted(() => ({
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

const basePayload: BuildPayload = {
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

let currentPayload: BuildPayload = basePayload;

vi.mock('../shared/hooks/useBuildPayload', () => ({
  useBuildPayload: () => ({
    payload: currentPayload,
    loading: false,
    error: null,
    mutate: canvasBuildPageDismissMocks.mutate,
    reload: canvasBuildPageDismissMocks.reload,
    setPayload: canvasBuildPageDismissMocks.setPayload,
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
  getCatalog: canvasBuildPageDismissMocks.getCatalog,
  loadCatalog: canvasBuildPageDismissMocks.loadCatalog,
}));

vi.mock('../menu', () => ({
  MenuPanel: () => null,
}));

vi.mock('../primitives/Toast', () => ({
  Toast: () => null,
  useToast: () => ({ show: canvasBuildPageDismissMocks.showToast }),
}));

vi.mock('../control/UiActionFeed', () => ({
  UiActionFeed: () => null,
}));

vi.mock('../control/uiActionLedger', () => ({
  registerUiActionUndoExecutor: canvasBuildPageDismissMocks.registerUndoExecutor,
  runUiAction: canvasBuildPageDismissMocks.runUiAction,
  undoUiAction: canvasBuildPageDismissMocks.undoUiAction,
}));

vi.mock('./CanvasActionDock', () => ({
  CanvasActionDock: () => null,
}));

vi.mock('./CanvasNodeDetail', () => ({
  CanvasNodeDetail: () => <div data-testid="node-detail">Node detail</div>,
}));

vi.mock('./CanvasReleaseTray', () => ({
  CanvasReleaseTray: () => null,
}));

vi.mock('./CanvasRunPanel', () => ({
  CanvasRunPanel: () => null,
}));

vi.mock('./CanvasDragGhost', () => ({
  CanvasDragGhost: () => null,
}));

vi.mock('./CanvasEdges', async () => {
  const actual = await vi.importActual<typeof import('./CanvasEdges')>('./CanvasEdges');
  return {
    ...actual,
    CanvasEdges: () => null,
    getEdgeGeometry: () => ({
      centerX: 180,
      centerY: 90,
      endX: 260,
      endY: 90,
      path: 'M0 0',
      startX: 100,
      startY: 90,
    }),
  };
});

vi.mock('./CanvasPopout', () => ({
  CanvasPopout: () => null,
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

describe('CanvasBuildPage edge dismiss', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    currentPayload = basePayload;
    canvasBuildPageDismissMocks.getCatalog.mockReturnValue([]);
    canvasBuildPageDismissMocks.loadCatalog.mockImplementation(() => new Promise(() => undefined));
  });

  test('clicking away dismisses the selected gate card', () => {
    render(<CanvasBuildPage workflowId="wf-123" />);

    fireEvent.click(screen.getByRole('button', { name: 'Add gate between Webhook and Next step' }));

    expect(screen.getByRole('button', { name: 'Add gate' })).toBeInTheDocument();

    fireEvent.mouseDown(document.body);

    expect(screen.queryByRole('button', { name: 'Add gate' })).not.toBeInTheDocument();
  });

  test('clicking inside the detail dock keeps the selected gate card open', () => {
    render(<CanvasBuildPage workflowId="wf-123" />);

    fireEvent.click(screen.getByRole('button', { name: 'Add gate between Webhook and Next step' }));
    fireEvent.click(screen.getByRole('button', { name: 'Add gate' }));

    expect(screen.getByTestId('node-detail')).toBeInTheDocument();

    fireEvent.mouseDown(screen.getByTestId('node-detail'));

    expect(screen.getByRole('button', { name: 'Add gate' })).toBeInTheDocument();
  });

  test('clicking the desktop detail toggle keeps the selected gate card open', () => {
    render(<CanvasBuildPage workflowId="wf-123" />);

    fireEvent.click(screen.getByRole('button', { name: 'Add gate between Webhook and Next step' }));

    const detailToggle = screen.getByRole('button', { name: 'Open Inspector dock' });
    fireEvent.mouseDown(detailToggle);
    fireEvent.click(detailToggle);

    expect(screen.getByRole('button', { name: 'Add gate' })).toBeInTheDocument();
  });

  test('configured gate code is visible before hover or selection', () => {
    currentPayload = {
      ...basePayload,
      build_graph: {
        ...basePayload.build_graph!,
        edges: [
          withBuildEdgeRelease(basePayload.build_graph!.edges[0], {
            family: 'conditional',
            edge_type: 'conditional',
            branch_reason: 'then',
            label: 'Then',
            release_condition: { field: 'ready', op: 'equals', value: true },
          }),
        ],
      },
    };

    render(<CanvasBuildPage workflowId="wf-123" />);

    const gateButton = screen.getByRole('button', { name: 'Select THEN gate between Webhook and Next step' });
    expect(gateButton).toHaveTextContent('THEN');
    expect(screen.queryByText('Then path')).not.toBeInTheDocument();
  });
});
