import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

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
  test('uses dropdown controls for conditional gates and still applies gate changes', () => {
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

    expect(screen.getByLabelText('Gate type')).toBeInTheDocument();
    expect(screen.getByLabelText('Condition mode')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Composer' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'JSON' })).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText('Gate type'), { target: { value: 'after_failure' } });

    expect(onApplyGate).toHaveBeenCalledWith('edge-trigger-next', 'after_failure');
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

  test('webhook trigger steps still show contract data fields under block properties', () => {
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
    fireEvent.click(screen.getByRole('button', { name: /show advanced contract fields/i }));
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
