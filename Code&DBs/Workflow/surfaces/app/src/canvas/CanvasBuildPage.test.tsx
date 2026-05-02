import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { CanvasBuildPage } from './CanvasBuildPage';
import type { BuildPayload } from '../shared/types';
import { withBuildEdgeRelease } from '../shared/edgeRelease';

const canvasBuildPageMocks = vi.hoisted(() => ({
  getCatalog: vi.fn(),
  loadCatalog: vi.fn(),
  materializePlan: vi.fn(),
  previewCompile: vi.fn(),
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
    payload: canvasBuildPageMocks.payload,
    loading: false,
    error: null,
    mutate: canvasBuildPageMocks.mutate,
    reload: canvasBuildPageMocks.reload,
    setPayload: canvasBuildPageMocks.setPayload,
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
  getCatalog: canvasBuildPageMocks.getCatalog,
  loadCatalog: canvasBuildPageMocks.loadCatalog,
}));

vi.mock('../shared/buildController', () => ({
  materializePlan: canvasBuildPageMocks.materializePlan,
  previewCompile: canvasBuildPageMocks.previewCompile,
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
  useToast: () => ({ show: canvasBuildPageMocks.showToast }),
}));

vi.mock('../control/UiActionFeed', () => ({
  UiActionFeed: () => null,
}));

vi.mock('../control/uiActionLedger', () => ({
  registerUiActionUndoExecutor: canvasBuildPageMocks.registerUndoExecutor,
  runUiAction: canvasBuildPageMocks.runUiAction,
  undoUiAction: canvasBuildPageMocks.undoUiAction,
}));

vi.mock('./CanvasActionDock', () => ({
  CanvasActionDock: () => null,
}));

vi.mock('./CanvasNodeDetail', () => ({
  CanvasNodeDetail: ({
    workflowSummary,
    operatingModelCompositeStatus,
  }: {
    workflowSummary?: any | null;
    operatingModelCompositeStatus?: any | null;
  }) => workflowSummary ? (
    <div aria-label="Workflow inspector summary">
      <span>{workflowSummary.title}</span>
      <span>{workflowSummary.stepCount} steps · {workflowSummary.linkCount} links</span>
      <span>{workflowSummary.reviewCount} decisions</span>
      <span>{workflowSummary.toolLane}</span>
      <span>{workflowSummary.branches}</span>
      <span>{workflowSummary.dataPills.join(' ')}</span>
      <span>{workflowSummary.disconnected} disconnected</span>
      {workflowSummary.contextAuthority ? (
        <span>
          {workflowSummary.contextAuthority.contextRef}
          {' '}
          {workflowSummary.contextAuthority.objectLabels.join(' ')}
        </span>
      ) : null}
      {operatingModelCompositeStatus ? (
        <span>
          {operatingModelCompositeStatus.deployabilityState}
          {' '}
          {operatingModelCompositeStatus.syntheticProofState}
        </span>
      ) : null}
    </div>
  ) : null,
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

vi.mock('./CanvasEdges', () => ({
  CanvasEdges: () => null,
  getEdgeGeometry: () => null,
}));

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

describe('CanvasBuildPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    canvasBuildPageMocks.payload = null;
    canvasBuildPageMocks.previewCompile.mockResolvedValue({
      kind: 'compile_preview',
      cqrs_role: 'query',
      scope_packet: { suggested_steps: [] },
    });
    canvasBuildPageMocks.materializePlan.mockResolvedValue({
      definition: {},
      build_graph: {
        nodes: [{ node_id: 'node-1', title: 'Probe node' }],
        edges: [],
      },
      workflow: { id: 'wf-created' },
      operation_receipt: { receipt_id: 'receipt-1', correlation_id: 'corr-1' },
      graph_summary: { node_count: 1, edge_count: 0 },
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
    canvasBuildPageMocks.getCatalog.mockReturnValue(triggerCatalog);
    canvasBuildPageMocks.loadCatalog.mockResolvedValue(triggerCatalog);
  });

  test('seeds a two-step draft graph when a trigger is selected from the picker', async () => {
    render(
      <CanvasBuildPage
        workflowId={null}
        initialMode="trigger-picker"
      />,
    );

    fireEvent.click(await screen.findByRole('button', { name: 'Manual' }));

    await waitFor(() => {
      expect(canvasBuildPageMocks.setPayload).toHaveBeenCalledTimes(1);
    });

    const nextPayload = canvasBuildPageMocks.setPayload.mock.calls[0][0] as BuildPayload;

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
      <CanvasBuildPage
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
    canvasBuildPageMocks.loadCatalog.mockImplementation(() => new Promise(() => undefined));
    canvasBuildPageMocks.payload = {
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

    render(<CanvasBuildPage workflowId="wf-123" />);

    await act(async () => {});

    fireEvent.click(screen.getByRole('button', { name: 'Open Inspector dock' }));

    await waitFor(() => {
      expect(screen.getByTestId('canvas-middle')).toHaveClass('canvas-middle--action-open');
    });
  });

  test('renders an overview-style human review strip for populated workflows', async () => {
    canvasBuildPageMocks.payload = {
      workflow: { id: 'wf-review', name: 'Integration builder' },
      definition: {},
      operation_receipt: { receipt_id: 'receipt-review-123456789' },
      compile_preview: {
        kind: 'compile_preview',
        scope_packet: {
          matches: [{ object_kind: 'domain', label: 'app domain' }],
        },
      },
      build_graph: {
        nodes: [
          {
            node_id: 'node-1',
            kind: 'step',
            title: 'Plan search',
            route: 'research/search',
            status: 'ready',
            required_inputs: ['app_domain'],
            agent_tool_plan: { tool_name: 'canvas_mutate_field', repeats: 4 },
          },
          {
            node_id: 'node-2',
            kind: 'step',
            title: 'Evaluate APIs',
            route: 'analysis/evaluate',
            outputs: ['integration_score'],
          },
          {
            node_id: 'note-1',
            kind: 'state',
            title: 'Loose note',
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

    render(<CanvasBuildPage workflowId="wf-review" />);

    fireEvent.click(screen.getByRole('button', { name: 'Open Inspector dock' }));

    const reviewSummary = screen.getByLabelText('Workflow inspector summary');
    expect(reviewSummary).toHaveTextContent('Integration builder');
    expect(reviewSummary).toHaveTextContent('2 steps · 1 links');
    expect(reviewSummary).toHaveTextContent('canvas_mutate_field x4');
    expect(reviewSummary).toHaveTextContent('app domain');
    expect(reviewSummary).toHaveTextContent('1 disconnected');
  });

  test('hydrates Workflow Context authority through the operate gateway', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async (url, init) => {
      if (String(url) === '/api/operate') {
        const body = JSON.parse(String((init as RequestInit | undefined)?.body || '{}'));
        if (body.operation === 'workflow_context_read') {
          return new Response(JSON.stringify({
            ok: true,
            result: {
              context_packs: [
                {
                  context_ref: 'workflow_context:live:abc123',
                  context_mode: 'synthetic',
                  truth_state: 'synthetic',
                  entities: [{ entity_kind: 'object', label: 'Account', io_mode: 'synthetic' }],
                },
              ],
            },
          }), { status: 200 });
        }
        if (
          body.operation === 'client_operating_model_operator_view'
          && body.input?.view === 'workflow_context_composite'
        ) {
          return new Response(JSON.stringify({
            ok: true,
            result: {
              operator_view: {
                state: 'partial',
                view_id: 'workflow_context_composite.test',
                payload: {
                  deployability: {
                    state: 'simulation_ready',
                    can_build: true,
                    can_simulate: true,
                    can_promote: false,
                  },
                  buildability: { state: 'missing' },
                  synthetic_proof: { state: 'ready' },
                  binding_coverage: { state: 'missing' },
                  real_evidence: { state: 'missing' },
                  confidence: { state: 'low', score: 0.41 },
                  blockers: { hard_count: 0, soft_count: 0, review_decision_count: 0 },
                  truth_state_classes: { synthetic: 2 },
                },
              },
            },
            operation_receipt: { receipt_id: 'receipt.composite.test' },
          }), { status: 200 });
        }
        return new Response(JSON.stringify({ ok: true, result: { payload: {} } }), { status: 200 });
      }
      return new Response(JSON.stringify({ metadata: {} }), { status: 200 });
    });
    canvasBuildPageMocks.payload = {
      workflow: { id: 'workflow.context.live', name: 'Context-backed workflow' },
      definition: {},
      build_graph: {
        nodes: [{ node_id: 'node-1', kind: 'step', title: 'Score account' }],
        edges: [],
      },
    };

    try {
      render(<CanvasBuildPage workflowId="workflow.context.live" />);

      fireEvent.click(screen.getByRole('button', { name: 'Open Inspector dock' }));

      await waitFor(() => {
        const workflowContextCall = fetchMock.mock.calls.find(([, init]) => {
          const body = JSON.parse(String((init as RequestInit | undefined)?.body || '{}'));
          return body.operation === 'workflow_context_read';
        });
        expect(workflowContextCall).toBeTruthy();
      });
      await waitFor(() => {
        const reviewSummary = screen.getByLabelText('Workflow inspector summary');
        expect(reviewSummary).toHaveTextContent('workflow_context:live:abc123');
        expect(reviewSummary).toHaveTextContent('Account');
        expect(reviewSummary).toHaveTextContent('simulation_ready ready');
      });
      await waitFor(() => {
        const compositeCall = fetchMock.mock.calls.find(([, init]) => {
          const body = JSON.parse(String((init as RequestInit | undefined)?.body || '{}'));
          return body.operation === 'client_operating_model_operator_view'
            && body.input?.view === 'workflow_context_composite';
        });
        expect(compositeCall).toBeTruthy();
      });
    } finally {
      fetchMock.mockRestore();
    }
  });

  test('summarizes branch points without flattening them into the review route', async () => {
    canvasBuildPageMocks.payload = {
      workflow: { id: 'wf-branch', name: 'Branching integration workflow' },
      definition: {},
      build_graph: {
        nodes: [
          { node_id: 'start', kind: 'step', title: 'Normalize app', route: 'research/search' },
          { node_id: 'decide', kind: 'step', title: 'Evaluate fit', route: 'analysis/evaluate' },
          { node_id: 'build', kind: 'step', title: 'Build integration', route: 'build' },
          { node_id: 'manual', kind: 'step', title: 'Manual review', route: 'review' },
          { node_id: 'finish', kind: 'step', title: 'Package result', route: 'summary' },
        ],
        edges: [
          { edge_id: 'edge-start-decide', kind: 'sequence', from_node_id: 'start', to_node_id: 'decide' },
          withBuildEdgeRelease(
            { edge_id: 'edge-decide-build', kind: 'sequence', from_node_id: 'decide', to_node_id: 'build' },
            { family: 'conditional', edge_type: 'conditional', branch_reason: 'then', label: 'Then' },
          ),
          withBuildEdgeRelease(
            { edge_id: 'edge-decide-manual', kind: 'sequence', from_node_id: 'decide', to_node_id: 'manual' },
            { family: 'conditional', edge_type: 'conditional', branch_reason: 'else', label: 'Else' },
          ),
          { edge_id: 'edge-build-finish', kind: 'sequence', from_node_id: 'build', to_node_id: 'finish' },
          { edge_id: 'edge-manual-finish', kind: 'sequence', from_node_id: 'manual', to_node_id: 'finish' },
        ],
      },
    };

    render(<CanvasBuildPage workflowId="wf-branch" />);

    fireEvent.click(screen.getByRole('button', { name: 'Open Inspector dock' }));

    const reviewSummary = screen.getByLabelText('Workflow inspector summary');
    expect(reviewSummary).toHaveTextContent('1 split: Evaluate fit');
    expect(reviewSummary).toHaveTextContent('Then / Else');
    expect(reviewSummary).toHaveTextContent('5 steps · 5 links');
    expect(reviewSummary).not.toHaveTextContent('Manual review→5Package result');
    expect(screen.queryByLabelText('Branch review board')).not.toBeInTheDocument();
  });

  test('uses the larger trigger picker width contract', async () => {
    render(
      <CanvasBuildPage
        workflowId={null}
        initialMode="trigger-picker"
      />,
    );

    expect(await screen.findByLabelText('Choose a trigger')).toHaveAttribute('data-width', '400');
  });

  test('surfaces compile errors inside the compose panel instead of leaving the UI silent', async () => {
    canvasBuildPageMocks.materializePlan.mockRejectedValueOnce(new Error('Request timed out after 45s'));

    render(
      <CanvasBuildPage
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

  test('uses agent CQRS materialize and waits for a multi-step graph before ready handoff', async () => {
    const onMaterializeHandoff = vi.fn();
    canvasBuildPageMocks.materializePlan.mockResolvedValueOnce({
      definition: { workflow_id: 'wf-created' },
      build_graph: {
        nodes: [
          { node_id: 'node-1', kind: 'step', title: 'Plan search' },
          { node_id: 'node-2', kind: 'step', title: 'Build integration' },
        ],
        edges: [{ edge_id: 'edge-1', kind: 'sequence', from_node_id: 'node-1', to_node_id: 'node-2' }],
      },
      workflow: { id: 'wf-created' },
      operation_receipt: { receipt_id: 'receipt-1', correlation_id: 'corr-1' },
      graph_summary: { node_count: 2, edge_count: 1 },
    });

    render(
      <CanvasBuildPage
        workflowId={null}
        initialMode="compose"
        onMaterializeHandoff={onMaterializeHandoff}
      />,
    );

    fireEvent.change(screen.getByRole('textbox', { name: /workflow intent/i }), {
      target: { value: 'Plan search, retrieve, evaluate, then build an integration' },
    });
    fireEvent.click(screen.getByRole('button', { name: /compose workflow/i }));

    await waitFor(() => {
      expect(canvasBuildPageMocks.materializePlan).toHaveBeenCalledWith(
        expect.stringContaining('Plan search'),
        { workflowId: null, llmTimeoutSeconds: 35 },
      );
    });

    await waitFor(() => {
      expect(onMaterializeHandoff).toHaveBeenCalledTimes(1);
    });
  });

  test('deletes selected nodes on Delete/Backspace and removes incident edges', async () => {
    canvasBuildPageMocks.payload = {
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

    render(<CanvasBuildPage workflowId={null} />);

    fireEvent.click(screen.getByRole('button', { name: /select workflow step start/i }));
    fireEvent.keyDown(window, { key: 'Delete' });

    await waitFor(() => {
      expect(canvasBuildPageMocks.setPayload).toHaveBeenCalled();
    });

    const calls = canvasBuildPageMocks.setPayload.mock.calls;
    const nextPayload = calls[calls.length - 1]?.[0] as BuildPayload;
    expect(nextPayload.build_graph?.nodes).toHaveLength(1);
    expect(nextPayload.build_graph?.nodes?.[0]).toMatchObject({ node_id: 'node-2' });
    expect(nextPayload.build_graph?.edges).toHaveLength(0);
  });

  test('supports pointer-centered wheel zoom and drag-to-pan on empty canvas', async () => {
    canvasBuildPageMocks.payload = {
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

    render(<CanvasBuildPage workflowId={null} />);

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
