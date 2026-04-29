import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { MoonBuildPage } from './MoonBuildPage';
import type { BuildPayload } from '../shared/types';

const moonBuildPageMocks = vi.hoisted(() => ({
  getCatalog: vi.fn(),
  loadCatalog: vi.fn(),
  compileDefinition: vi.fn(),
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
    payload: moonBuildPageMocks.payload,
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

vi.mock('../shared/buildController', () => ({
  compileDefinition: moonBuildPageMocks.compileDefinition,
}));

vi.mock('../menu', () => ({
  MenuPanel: ({
    open,
    title,
    sections,
    width,
  }: {
    open: boolean;
    title?: string;
    sections: Array<{ id: string; items: Array<{ id: string; label: string; disabled?: boolean; onSelect: () => void }> }>;
    width?: number;
  }) => {
    if (!open) return null;
    return (
      <div aria-label={title || 'menu'} data-width={width}>
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
    moonBuildPageMocks.payload = null;
    moonBuildPageMocks.compileDefinition.mockResolvedValue({
      definition: {},
      build_graph: {
        nodes: [],
        edges: [],
      },
      workflow: { id: 'wf-created' },
    });
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

  test('explains the compose contract in compose mode', async () => {
    render(
      <MoonBuildPage
        workflowId={null}
        initialMode="compose"
      />,
    );

    expect(await screen.findByText(/describe the workflow/i)).toBeInTheDocument();
    expect(screen.getByText(/authority it should trust/i)).toBeInTheDocument();
    expect(screen.getByPlaceholderText(/scrape gmail/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /compose workflow/i })).toBeDisabled();
  });

  test('applies the yielding detail-dock class when the detail panel is opened on a populated graph', async () => {
    moonBuildPageMocks.loadCatalog.mockImplementation(() => new Promise(() => undefined));
    moonBuildPageMocks.payload = {
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

    render(<MoonBuildPage workflowId="wf-123" />);

    await act(async () => {});

    fireEvent.click(screen.getByRole('button', { name: 'Open Inspector dock' }));

    await waitFor(() => {
      expect(screen.getByTestId('moon-middle')).toHaveClass('moon-middle--context-open');
    });
  });

  test('uses the larger trigger picker width contract', async () => {
    render(
      <MoonBuildPage
        workflowId={null}
        initialMode="trigger-picker"
      />,
    );

    expect(await screen.findByLabelText('Choose a trigger')).toHaveAttribute('data-width', '400');
  });

  test('surfaces compile errors inside the compose panel instead of leaving the UI silent', async () => {
    moonBuildPageMocks.compileDefinition.mockRejectedValueOnce(new Error('Request timed out after 45s'));

    render(
      <MoonBuildPage
        workflowId={null}
        initialMode="compose"
      />,
    );

    fireEvent.change(screen.getByRole('textbox', { name: /workflow intent/i }), {
      target: { value: 'Build a workflow from this prompt' },
    });
    fireEvent.click(screen.getByRole('button', { name: /compose workflow/i }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Request timed out after 45s');
  });

  test('deletes selected nodes on Delete/Backspace and removes incident edges', async () => {
    moonBuildPageMocks.payload = {
      definition: {},
      build_graph: {
        nodes: [
          {
            node_id: 'node-1',
            kind: 'step',
            title: 'Start',
            route: 'trigger/manual',
            status: 'ready',
          },
          {
            node_id: 'node-2',
            kind: 'step',
            title: 'Next',
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

    render(<MoonBuildPage workflowId={null} />);

    fireEvent.click(screen.getByText('Start'));
    fireEvent.keyDown(window, { key: 'Delete' });

    await waitFor(() => {
      expect(moonBuildPageMocks.setPayload).toHaveBeenCalled();
    });

    const calls = moonBuildPageMocks.setPayload.mock.calls;
    const nextPayload = calls[calls.length - 1]?.[0] as BuildPayload;
    expect(nextPayload.build_graph?.nodes).toHaveLength(1);
    expect(nextPayload.build_graph?.nodes?.[0]).toMatchObject({ node_id: 'node-2' });
    expect(nextPayload.build_graph?.edges).toHaveLength(0);
  });

  test('supports pointer-centered wheel zoom and drag-to-pan on empty canvas', async () => {
    moonBuildPageMocks.payload = {
      definition: {},
      build_graph: {
        nodes: [
          {
            node_id: 'node-1',
            kind: 'step',
            title: 'Start',
            route: 'trigger/manual',
            status: 'ready',
          },
          {
            node_id: 'node-2',
            kind: 'step',
            title: 'Next',
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

    render(<MoonBuildPage workflowId={null} />);

    const graph = screen.getByLabelText('Workflow build graph');
    const baseline = graph.getAttribute('style') || '';

    fireEvent(graph, new MouseEvent('pointerdown', { bubbles: true, clientX: 0, clientY: 0, button: 0 }));
    fireEvent(graph, new MouseEvent('pointermove', { bubbles: true, clientX: 25, clientY: 17, button: 0 }));
    fireEvent(graph, new MouseEvent('pointerup', { bubbles: true, clientX: 25, clientY: 17, button: 0 }));

    await waitFor(() => {
      expect(graph).not.toHaveAttribute('style', baseline);
      expect(graph.getAttribute('style') || '').toContain('translate(25px, 17px)');
    });

    fireEvent.wheel(graph, { deltaY: -120, clientX: 0, clientY: 0 });
    await waitFor(() => {
      expect(graph.getAttribute('style')).toMatch(/scale\([1-9]/);
    });
  });
});
