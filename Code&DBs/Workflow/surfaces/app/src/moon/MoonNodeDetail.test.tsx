import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { afterEach, beforeEach, vi } from 'vitest';

import { MoonNodeDetail } from './MoonNodeDetail';
import type { OrbitEdge, OrbitNode } from './moonBuildPresenter';
import type { BuildPayload } from '../shared/types';

vi.mock('../shared/hooks/useObjectTypes', () => ({
  useObjectTypes: () => ({
    objectTypes: [],
    loading: false,
  }),
}));

describe('MoonNodeDetail', () => {
  beforeEach(() => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(() => new Promise<Response>(() => {}));
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('renders gate panel with condition mode and remove button for configured conditional gates', () => {
    const onApplyGate = vi.fn();

    const selectedEdge: OrbitEdge = {
      id: 'edge-trigger-next',
      from: 'node-trigger',
      to: 'node-next',
      kind: 'sequence',
      isOnDominantPath: true,
      gateState: 'configured',
      gateLabel: 'Then',
      gateFamily: 'conditional',
      branchReason: 'then',
      siblingCount: 2,
      siblingIndex: 0,
      inLineage: true,
    };

    const buildGraph: NonNullable<BuildPayload['build_graph']> = {
      nodes: [
        { node_id: 'node-trigger', kind: 'step', title: 'Webhook' },
        { node_id: 'node-next', kind: 'step', title: 'Next step' },
        { node_id: 'node-else', kind: 'step', title: 'Else path' },
      ],
      edges: [
        {
          edge_id: 'edge-trigger-next',
          kind: 'sequence',
          from_node_id: 'node-trigger',
          to_node_id: 'node-next',
          release: {
            family: 'conditional',
            edge_type: 'conditional',
            state: 'configured',
            label: 'Then',
            branch_reason: 'then',
            release_condition: {
              field: 'should_continue',
              op: 'equals',
              value: true,
            },
            config: {
              condition: {
                field: 'should_continue',
                op: 'equals',
                value: true,
              },
            },
          },
        },
        {
          edge_id: 'edge-trigger-else',
          kind: 'sequence',
          from_node_id: 'node-trigger',
          to_node_id: 'node-else',
          release: {
            family: 'conditional',
            edge_type: 'conditional',
            state: 'configured',
            label: 'Else',
            branch_reason: 'else',
            release_condition: {
              op: 'not',
              conditions: [{ field: 'should_continue', op: 'equals', value: true }],
            },
            config: {
              condition: {
                field: 'should_continue',
                op: 'equals',
                value: true,
              },
            },
          },
        },
      ],
    };

    render(
      <MoonNodeDetail
        node={null}
        content={null}
        workflowId={null}
        onMutate={vi.fn()}
        onClose={vi.fn()}
        selectedEdge={selectedEdge}
        edgeFromLabel="Webhook"
        edgeToLabel="Next step"
        onApplyGate={onApplyGate}
        gateItems={[
          {
            id: 'ctrl-branch',
            label: 'Branch',
            icon: 'gate',
            family: 'control',
            status: 'ready',
            dropKind: 'edge',
            gateFamily: 'conditional',
          },
          {
            id: 'ctrl-on-failure',
            label: 'On Failure',
            icon: 'gate',
            family: 'control',
            status: 'ready',
            dropKind: 'edge',
            gateFamily: 'after_failure',
          },
        ]}
        buildGraph={buildGraph}
        onUpdateBuildGraph={vi.fn()}
      />,
    );

    // Gate panel header is shown
    expect(screen.getByText('Gate')).toBeInTheDocument();
    // Conditional branch editor is shown with its route display and condition-mode select
    expect(screen.getByLabelText('Branch routes')).toBeInTheDocument();
    expect(screen.getByLabelText('Condition mode')).toBeInTheDocument();
    // Old tab-style composer/JSON mode buttons are gone — replaced by the select
    expect(screen.queryByRole('button', { name: 'Composer' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'JSON' })).not.toBeInTheDocument();
    // Remove gate button is present since the gate is configured
    expect(screen.getByRole('button', { name: 'Remove gate' })).toBeInTheDocument();
    // Gate-type selection now happens via drag-and-drop from the catalog (MoonBuildPage),
    // so onApplyGate is not triggered from within the detail panel.
    expect(onApplyGate).not.toHaveBeenCalled();
  });

  test('Remove gate clears conditional branch pair and commits a normal release', async () => {
    const onCommitGraphAction = vi.fn().mockResolvedValue(undefined);

    const selectedEdge: OrbitEdge = {
      id: 'edge-trigger-next',
      from: 'node-trigger',
      to: 'node-next',
      kind: 'sequence',
      isOnDominantPath: true,
      gateState: 'configured',
      gateLabel: 'Then',
      gateFamily: 'conditional',
      branchReason: 'then',
      siblingCount: 2,
      siblingIndex: 0,
      inLineage: true,
    };

    const buildGraph: NonNullable<BuildPayload['build_graph']> = {
      nodes: [
        { node_id: 'node-trigger', kind: 'step', title: 'Webhook' },
        { node_id: 'node-next', kind: 'step', title: 'Next step' },
        { node_id: 'node-else', kind: 'step', title: 'Else path' },
      ],
      edges: [
        {
          edge_id: 'edge-trigger-next',
          kind: 'sequence',
          from_node_id: 'node-trigger',
          to_node_id: 'node-next',
          release: {
            family: 'conditional',
            edge_type: 'conditional',
            state: 'configured',
            label: 'Then',
            branch_reason: 'then',
            release_condition: { field: 'should_continue', op: 'equals', value: true },
            config: { condition: { field: 'should_continue', op: 'equals', value: true } },
          },
        },
        {
          edge_id: 'edge-trigger-else',
          kind: 'sequence',
          from_node_id: 'node-trigger',
          to_node_id: 'node-else',
          release: {
            family: 'conditional',
            edge_type: 'conditional',
            state: 'configured',
            label: 'Else',
            branch_reason: 'else',
            release_condition: {
              op: 'not',
              conditions: [{ field: 'should_continue', op: 'equals', value: true }],
            },
            config: { condition: { field: 'should_continue', op: 'equals', value: true } },
          },
        },
      ],
    };

    render(
      <MoonNodeDetail
        node={null}
        content={null}
        workflowId={null}
        onMutate={vi.fn()}
        onClose={vi.fn()}
        selectedEdge={selectedEdge}
        edgeFromLabel="Webhook"
        edgeToLabel="Next step"
        onApplyGate={vi.fn()}
        gateItems={[]}
        buildGraph={buildGraph}
        onCommitGraphAction={onCommitGraphAction}
      />,
    );

    // Two-click confirm: first click primes the button, second fires the action.
    const removeButton = screen.getByRole('button', { name: 'Remove gate' });
    await act(async () => {
      fireEvent.click(removeButton);
    });
    const confirmButton = await screen.findByRole('button', { name: 'Click again to remove' });
    await act(async () => {
      fireEvent.click(confirmButton);
    });

    await waitFor(() => expect(onCommitGraphAction).toHaveBeenCalledTimes(1));

    const [committedGraph, meta] = onCommitGraphAction.mock.calls[0];
    expect(meta.label).toBe('Remove gate');
    const byId: Record<string, any> = Object.fromEntries(
      (committedGraph.edges || []).map((edge: any) => [edge.edge_id, edge]),
    );
    expect(byId['edge-trigger-next'].release.family).toBe('after_success');
    // Paired conditional sibling is cleared in the same commit.
    expect(byId['edge-trigger-else'].release.family).toBe('after_success');
  });

  test('Remove gate is not offered when the edge is already ungated', () => {
    const selectedEdge: OrbitEdge = {
      id: 'edge-plain',
      from: 'node-a',
      to: 'node-b',
      kind: 'sequence',
      isOnDominantPath: true,
      gateState: 'empty',
      siblingCount: 1,
      siblingIndex: 0,
      inLineage: true,
    };

    render(
      <MoonNodeDetail
        node={null}
        content={null}
        workflowId={null}
        onMutate={vi.fn()}
        onClose={vi.fn()}
        selectedEdge={selectedEdge}
        edgeFromLabel="A"
        edgeToLabel="B"
        onApplyGate={vi.fn()}
        gateItems={[]}
        buildGraph={{ nodes: [], edges: [] }}
        onCommitGraphAction={vi.fn()}
      />,
    );

    expect(screen.queryByRole('button', { name: 'Remove gate' })).not.toBeInTheDocument();
  });

  test('renders run completion gate contracts for selected run nodes', () => {
    const node: OrbitNode = {
      id: 'enter_data',
      kind: 'step',
      title: 'Enter data',
      summary: 'submit artifact_bundle via praxis_submit_artifact_bundle',
      glyphType: 'tool',
      ringState: 'run-succeeded',
      isOnDominantPath: true,
      issueCount: 0,
      dominantPathIndex: 0,
      x: 0,
      y: 0,
      rank: 0,
      multiplicity: null,
      taskType: 'data_entry',
      outcomeGoal: 'CRM record is populated.',
      prompt: 'Enter the applicant data in the CRM tool.',
      completionContract: {
        result_kind: 'artifact_bundle',
        submit_tool_names: ['praxis_submit_artifact_bundle'],
        submission_required: true,
        verification_required: false,
      },
      outgoingEdgeCount: 0,
      inLineage: true,
    };

    render(
      <MoonNodeDetail
        node={node}
        content={null}
        workflowId="wf_1"
        onMutate={vi.fn()}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByLabelText('Run completion gate')).toBeInTheDocument();
    expect(screen.getByText('Submission required')).toBeInTheDocument();
    expect(screen.getByText('data_entry')).toBeInTheDocument();
    expect(screen.getByText('artifact_bundle')).toBeInTheDocument();
    expect(screen.getByText('praxis_submit_artifact_bundle')).toBeInTheDocument();
    expect(screen.getByText('CRM record is populated.')).toBeInTheDocument();
    expect(screen.getByText('Enter the applicant data in the CRM tool.')).toBeInTheDocument();
  });

  test('renders block contract string lists as data fields with values from the build graph', () => {
    const node: OrbitNode = {
      id: 'n-step',
      kind: 'step',
      title: 'Research',
      summary: 'Run research',
      glyphType: 'research',
      ringState: 'decided-grounded',
      isOnDominantPath: true,
      issueCount: 0,
      dominantPathIndex: 0,
      x: 0,
      y: 0,
      rank: 0,
      multiplicity: null,
      outgoingEdgeCount: 0,
      inLineage: true,
    };

    const buildGraph: NonNullable<BuildPayload['build_graph']> = {
      nodes: [
        {
          node_id: 'n-step',
          kind: 'step',
          title: 'Research',
          route: 'auto/research',
          required_inputs: ['customer_id'],
          outputs: ['research_findings'],
          persistence_targets: ['store.research_notes'],
        },
      ],
      edges: [],
    };

    render(
      <MoonNodeDetail
        node={node}
        content={null}
        workflowId="wf_1"
        onMutate={vi.fn()}
        onClose={vi.fn()}
        buildGraph={buildGraph}
        onUpdateBuildGraph={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: /show advanced contract fields/i }));

    expect(screen.getByLabelText('Required inputs')).toBeInTheDocument();
    expect(screen.getByLabelText('Outputs')).toBeInTheDocument();
    expect(screen.getByLabelText('Persistence targets')).toBeInTheDocument();
    expect(screen.getByLabelText('Required inputs field: customer_id')).toBeInTheDocument();
    expect(screen.getByLabelText('Outputs field: research_findings')).toBeInTheDocument();
    expect(screen.getByLabelText('Persistence targets field: store.research_notes')).toBeInTheDocument();
  });

  test('webhook trigger steps still show contract data fields under block properties', async () => {
    const node: OrbitNode = {
      id: 'n-hook',
      kind: 'step',
      title: 'Webhook',
      summary: 'Inbound webhook',
      glyphType: 'tool',
      ringState: 'decided-grounded',
      isOnDominantPath: true,
      issueCount: 0,
      dominantPathIndex: 0,
      x: 0,
      y: 0,
      rank: 0,
      route: 'trigger/webhook',
      multiplicity: null,
      outgoingEdgeCount: 0,
      inLineage: true,
    };

    const buildGraph: NonNullable<BuildPayload['build_graph']> = {
      nodes: [
        {
          node_id: 'n-hook',
          kind: 'step',
          title: 'Webhook',
          route: 'trigger/webhook',
          trigger: {
            event_type: 'db.webhook_events.insert',
            filter: {},
          },
          required_inputs: ['payload'],
          outputs: ['verified_event'],
          persistence_targets: [],
        },
      ],
      edges: [],
    };

    render(
      <MoonNodeDetail
        node={node}
        content={null}
        workflowId="wf_1"
        onMutate={vi.fn()}
        onClose={vi.fn()}
        buildGraph={buildGraph}
        onUpdateBuildGraph={vi.fn()}
      />,
    );

    expect(screen.getByText('Trigger config')).toBeInTheDocument();
    await waitFor(() => expect(globalThis.fetch).toHaveBeenCalledWith('/api/moon/pickers/payload-fields'));
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: /show advanced contract fields/i }));
    });
    expect(screen.getByLabelText('Required inputs')).toBeInTheDocument();
    expect(screen.getByLabelText('Outputs')).toBeInTheDocument();
    expect(screen.getByLabelText('Persistence targets')).toBeInTheDocument();
    expect(screen.getByLabelText('Required inputs field: payload')).toBeInTheDocument();
    expect(screen.getByLabelText('Outputs field: verified_event')).toBeInTheDocument();
  });

  test('keeps long route and contract values in the DOM for readable wrapping surfaces', () => {
    const longRoute = 'integration/very-long-authority-route/that-should-wrap-cleanly-inside-the-detail-surface';
    const longField = 'customer.primary_account.executive.owner_email_address';
    const node: OrbitNode = {
      id: 'n-long',
      kind: 'step',
      title: 'Long contract step',
      summary: 'Long summary',
      glyphType: 'tool',
      ringState: 'decided-grounded',
      isOnDominantPath: true,
      issueCount: 0,
      dominantPathIndex: 0,
      x: 0,
      y: 0,
      rank: 0,
      route: longRoute,
      multiplicity: null,
      outgoingEdgeCount: 0,
      inLineage: true,
    };

    const buildGraph: NonNullable<BuildPayload['build_graph']> = {
      nodes: [
        {
          node_id: 'n-long',
          kind: 'step',
          title: 'Long contract step',
          route: longRoute,
          required_inputs: [longField],
          outputs: ['research.summary'],
          persistence_targets: [],
        },
      ],
      edges: [],
    };

    render(
      <MoonNodeDetail
        node={node}
        content={null}
        workflowId="wf_1"
        onMutate={vi.fn()}
        onClose={vi.fn()}
        buildGraph={buildGraph}
        onUpdateBuildGraph={vi.fn()}
      />,
    );

    expect(screen.getAllByText(`Route: ${longRoute}`)).toHaveLength(2);

    fireEvent.click(screen.getByRole('button', { name: /show advanced contract fields/i }));

    expect(screen.getByDisplayValue(longField)).toBeInTheDocument();
  });
});
