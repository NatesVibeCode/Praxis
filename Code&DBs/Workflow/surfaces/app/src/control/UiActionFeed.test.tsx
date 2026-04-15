import { act, render, screen } from '@testing-library/react';
import React from 'react';

import { world } from '../world';
import { UiActionFeed } from './UiActionFeed';
import { UI_ACTION_LOG_PATH } from './uiActionLedger';

describe('UiActionFeed', () => {
  beforeEach(() => {
    world.hydrate({ state: {}, version: 0 });
  });

  test('stays stable when the feed transitions from empty to populated', async () => {
    render(
      <UiActionFeed
        surface="moon"
        scope="moon:wf_test"
        variant="compact"
      />,
    );

    expect(screen.queryByLabelText('Recent control actions')).toBeNull();

    await act(async () => {
      world.set(UI_ACTION_LOG_PATH, [
        {
          id: 'action-1',
          surface: 'moon',
          undoScope: 'moon:wf_test',
          category: 'graph',
          label: 'Append node',
          authority: 'build.build_graph',
          reason: 'Add a new step after Next step.',
          outcome: 'Next step was appended to the chain.',
          target: { kind: 'node', label: 'Next step', id: 'node-next' },
          changeSummary: ['Appended step', 'After Next step'],
          status: 'applied',
          undoable: true,
          recovery: 'undo_ready',
          occurredAt: Date.now(),
          undoDescriptor: {
            kind: 'moon.payload.restore',
            scope: 'moon:wf_test',
            payload: { build_graph: { nodes: [], edges: [] } },
          },
        },
      ]);
    });

    expect(screen.getByLabelText('Recent control actions')).toBeInTheDocument();
    expect(screen.getByText('Append node')).toBeInTheDocument();
  });
});
