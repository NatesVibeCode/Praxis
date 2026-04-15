import {
  UI_ACTION_LOG_PATH,
  registerUiActionUndoExecutor,
  runUiAction,
  undoUiAction,
} from './uiActionLedger';
import { world } from '../world';

describe('uiActionLedger', () => {
  beforeEach(() => {
    world.hydrate({ state: {}, version: 0 });
  });

  test('keeps undo ownership isolated to each control lane', async () => {
    const gridEntry = await runUiAction({
      surface: 'grid',
      undoScope: 'grid.layout',
      label: 'Move module',
      authority: 'ui.layout.quadrants',
      reason: 'Move the module to a new quadrant.',
      outcome: 'The module moved.',
      apply: () => {
        world.propose('ui.layout.quadrants', { q1: { module: 'chart' } });
      },
      undoDescriptor: {
        kind: 'world.propose',
        path: 'ui.layout.quadrants',
        value: {},
      },
    });

    const moonEntry = await runUiAction({
      surface: 'moon',
      undoScope: 'moon:wf_123',
      label: 'Attach reference',
      authority: 'build.authority_attachments',
      reason: 'Attach escalation policy evidence.',
      outcome: 'Escalation policy is now available.',
      apply: () => undefined,
      undoDescriptor: {
        kind: 'moon.payload.restore',
        scope: 'moon:wf_123',
        payload: { workflow: { id: 'wf_123', name: 'Support Intake' } },
      },
    });

    const entries = world.get(UI_ACTION_LOG_PATH) as Array<{
      id: string;
      undoScope: string;
      recovery: string;
      undoable: boolean;
    }>;

    expect(entries).toHaveLength(2);
    expect(entries.find((entry) => entry.id === gridEntry.id)).toMatchObject({
      undoScope: 'grid.layout',
      recovery: 'undo_ready',
      undoable: true,
    });
    expect(entries.find((entry) => entry.id === moonEntry.id)).toMatchObject({
      undoScope: 'moon:wf_123',
      recovery: 'undo_ready',
      undoable: true,
    });
  });

  test('executes registered undo executors for serialized descriptors', async () => {
    const restorePayload = jest.fn();
    const unregister = registerUiActionUndoExecutor('moon.payload.restore', (descriptor) => {
      restorePayload(descriptor.payload);
      return true;
    });

    const entry = await runUiAction({
      surface: 'moon',
      undoScope: 'moon:draft',
      label: 'Restore draft graph',
      authority: 'build.build_graph',
      reason: 'Undo the latest draft graph edit.',
      outcome: 'The draft graph is back to the previous payload.',
      apply: () => undefined,
      undoDescriptor: {
        kind: 'moon.payload.restore',
        scope: 'moon:draft',
        payload: { build_graph: { nodes: [{ node_id: 'node-1' }], edges: [] } },
      },
    });

    const result = await undoUiAction(entry.id);

    unregister();

    expect(result.ok).toBe(true);
    expect(restorePayload).toHaveBeenCalledWith({
      build_graph: { nodes: [{ node_id: 'node-1' }], edges: [] },
    });
  });
});
