import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { MoonNodeDetail } from './MoonNodeDetail';
import type { OrbitEdge } from './moonBuildPresenter';
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
});
