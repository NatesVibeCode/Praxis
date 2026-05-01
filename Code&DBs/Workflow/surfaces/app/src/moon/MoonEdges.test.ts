import { describe, expect, it } from 'vitest';
import { edgePresentation, getEdgeGeometry } from './MoonEdges';
import { MOON_LAYOUT } from './moonLayout';
import { COLUMN_SPACING, RANK_SPACING } from './moonBuildPresenter';
import type { GraphLayout, OrbitEdge } from './moonBuildPresenter';

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
        width: MOON_LAYOUT.nodeWidth,
        height: MOON_LAYOUT.nodeHeight,
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

describe('Moon edge geometry', () => {
  it('connects vertical workflow cards from bottom port to top port', () => {
    const geometry = getEdgeGeometry(
      { from: 'a', to: 'b' },
      layout([
        ['a', 0, 0],
        ['b', 0, RANK_SPACING],
      ]),
    );

    expect(geometry).toMatchObject({
      startX: MOON_LAYOUT.canvasPad,
      startY: MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeHeight / 2,
      endX: MOON_LAYOUT.canvasPad,
      endY: MOON_LAYOUT.canvasPad + RANK_SPACING - MOON_LAYOUT.nodeHeight / 2,
      path: `M${MOON_LAYOUT.canvasPad} ${MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeHeight / 2}L${MOON_LAYOUT.canvasPad} ${MOON_LAYOUT.canvasPad + RANK_SPACING - MOON_LAYOUT.nodeHeight / 2}`,
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
      startX: MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeWidth / 2,
      startY: MOON_LAYOUT.canvasPad,
      endX: MOON_LAYOUT.canvasPad + COLUMN_SPACING - MOON_LAYOUT.nodeWidth / 2,
      endY: MOON_LAYOUT.canvasPad,
    });
    expect(geometry?.path).not.toContain('C');
  });
});

describe('Moon edge and gate presentation', () => {
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
    expect(failure.color).not.toContain('--moon-state-error');
    expect(always.strokeDasharray).toBe('2 5');
    expect(always.glyph).toBe('loop');
  });

  it('uses canonical color only for gate state', () => {
    expect(edgePresentation(edge({ gateFamily: 'after_failure', gateState: 'blocked' })).color).toContain('--moon-state-error');
    expect(edgePresentation(edge({ gateFamily: 'conditional', gateState: 'proposed' })).color).toContain('--moon-state-warning');
    expect(edgePresentation(edge({ gateFamily: 'after_any', gateState: 'passed' })).color).toContain('--moon-state-success');
  });
});
