import { describe, expect, it } from 'vitest';
import { edgePresentation, getEdgeGeometry } from './CanvasEdges';
import { CANVAS_LAYOUT } from './canvasLayout';
import { COLUMN_SPACING, RANK_SPACING } from './canvasBuildPresenter';
import type { GraphLayout, OrbitEdge } from './canvasBuildPresenter';

function layout(nodes: Array<[string, number, number]>): GraphLayout {
  return {
    nodes: new Map(nodes.map(([id, x, y], index) => [
      id,
      {
        id,
        x,
        y,
        rank: index,
        column: 0,
        width: CANVAS_LAYOUT.nodeWidth,
        height: CANVAS_LAYOUT.nodeHeight,
        shape: 'task',
      },
    ])),
    layers: [],
    width: 0,
    height: 0,
  };
}

function edge(overrides: Partial<OrbitEdge>): OrbitEdge {
  return {
    id: 'edge-1',
    from: 'a',
    to: 'b',
    kind: 'sequence',
    isOnDominantPath: false,
    gateState: 'configured',
    siblingCount: 1,
    siblingIndex: 0,
    inLineage: true,
    ...overrides,
  };
}

describe('Canvas edge geometry', () => {
  it('connects vertical workflow cards from bottom port to top port', () => {
    const geometry = getEdgeGeometry(
      { from: 'a', to: 'b' },
      layout([
        ['a', 0, 0],
        ['b', 0, RANK_SPACING],
      ]),
    );

    expect(geometry).toMatchObject({
      startX: CANVAS_LAYOUT.canvasPad,
      startY: CANVAS_LAYOUT.canvasPad + CANVAS_LAYOUT.nodeHeight / 2,
      endX: CANVAS_LAYOUT.canvasPad,
      endY: CANVAS_LAYOUT.canvasPad + RANK_SPACING - CANVAS_LAYOUT.nodeHeight / 2,
      path: `M${CANVAS_LAYOUT.canvasPad} ${CANVAS_LAYOUT.canvasPad + CANVAS_LAYOUT.nodeHeight / 2}L${CANVAS_LAYOUT.canvasPad} ${CANVAS_LAYOUT.canvasPad + RANK_SPACING - CANVAS_LAYOUT.nodeHeight / 2}`,
    });
  });

  it('routes branch lines through card ports without bottom loop curves', () => {
    const geometry = getEdgeGeometry(
      { from: 'a', to: 'b' },
      layout([
        ['a', 0, 0],
        ['b', COLUMN_SPACING, RANK_SPACING],
      ]),
    );

    expect(geometry?.path).toBe('M120 159L120 191L392 217L392 249');
    expect(geometry?.path).not.toContain('C');
  });

  it('uses side ports for same-rank relationships', () => {
    const geometry = getEdgeGeometry(
      { from: 'a', to: 'b' },
      layout([
        ['a', 0, 0],
        ['b', COLUMN_SPACING, 0],
      ]),
    );

    expect(geometry).toMatchObject({
      startX: CANVAS_LAYOUT.canvasPad + CANVAS_LAYOUT.nodeWidth / 2,
      startY: CANVAS_LAYOUT.canvasPad,
      endX: CANVAS_LAYOUT.canvasPad + COLUMN_SPACING - CANVAS_LAYOUT.nodeWidth / 2,
      endY: CANVAS_LAYOUT.canvasPad,
    });
    expect(geometry?.path).not.toContain('C');
  });
});

describe('Canvas edge and gate presentation', () => {
  it('derives fixed canvas labels from edge release data', () => {
    expect(edgePresentation(edge({ gateFamily: undefined })).shortLabel).toBe('OK');
    expect(edgePresentation(edge({ gateFamily: 'conditional', branchReason: 'then' })).shortLabel).toBe('THEN');
    expect(edgePresentation(edge({ gateFamily: 'conditional', branchReason: 'else' })).shortLabel).toBe('ELSE');
    expect(edgePresentation(edge({ gateFamily: 'conditional' })).shortLabel).toBe('COND');
    expect(edgePresentation(edge({ gateFamily: 'after_failure' })).shortLabel).toBe('FAIL');
    expect(edgePresentation(edge({ gateFamily: 'after_any' })).shortLabel).toBe('ANY');
  });

  it('uses line pattern and glyph for family instead of failure color', () => {
    const conditional = edgePresentation(edge({ gateFamily: 'conditional', branchReason: 'then' }));
    const failure = edgePresentation(edge({ gateFamily: 'after_failure', gateState: 'configured' }));
    const always = edgePresentation(edge({ gateFamily: 'after_any' }));

    expect(conditional.strokeDasharray).toBe('6 6');
    expect(conditional.glyph).toBe('decompose');
    expect(failure.strokeDasharray).toBeUndefined();
    expect(failure.glyph).toBe('warning');
    expect(failure.color).not.toContain('--canvas-state-error');
    expect(always.strokeDasharray).toBe('2 5');
    expect(always.glyph).toBe('loop');
  });

  it('uses canonical color only for gate state', () => {
    expect(edgePresentation(edge({ gateFamily: 'after_failure', gateState: 'blocked' })).color).toContain('--canvas-state-error');
    expect(edgePresentation(edge({ gateFamily: 'conditional', gateState: 'proposed' })).color).toContain('--canvas-state-warning');
    expect(edgePresentation(edge({ gateFamily: 'after_any', gateState: 'passed' })).color).toContain('--canvas-state-success');
  });
});
