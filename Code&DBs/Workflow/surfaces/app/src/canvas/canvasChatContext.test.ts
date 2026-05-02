import { afterEach, describe, expect, it } from 'vitest';
import {
  clearCanvasChatContext,
  canvasChatSelectionContext,
  setCanvasChatContext,
} from './canvasChatContext';

describe('canvasChatContext', () => {
  afterEach(() => {
    clearCanvasChatContext();
  });

  it('forwards the visible UI snapshot as a read-only witness', () => {
    setCanvasChatContext({
      workflow_id: 'wf_visible',
      workflow_name: 'Visible workflow',
      selected_node_id: 'node_search',
      selected_edge_id: null,
      view_mode: 'build',
      visible_ui_snapshot: {
        kind: 'canvas_visible_snapshot',
        read_only: true,
        durability: 'visible_ui_snapshot_not_write_authority',
        node_count: 3,
        edge_count: 2,
        nodes: [{ node_id: 'node_search', title: 'Search app docs' }],
      },
    });

    const [entry] = canvasChatSelectionContext();

    expect(entry.workflow_id).toBe('wf_visible');
    expect(entry.visible_ui_snapshot).toMatchObject({
      read_only: true,
      durability: 'visible_ui_snapshot_not_write_authority',
      node_count: 3,
      edge_count: 2,
    });
  });
});
