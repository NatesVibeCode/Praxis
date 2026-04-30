import { describe, expect, it } from 'vitest';
import { getEdgeGeometry } from './MoonEdges';
import { MOON_LAYOUT } from './moonLayout';
import type { GraphLayout } from './moonBuildPresenter';

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

describe('Moon edge geometry', () => {
  it('connects vertical workflow cards from bottom port to top port', () => {
    const geometry = getEdgeGeometry(
      { from: 'a', to: 'b' },
      layout([
        ['a', 0, 0],
        ['b', 0, 228],
      ]),
    );

    expect(geometry).toMatchObject({
      startX: MOON_LAYOUT.canvasPad,
      startY: MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeHeight / 2,
      endX: MOON_LAYOUT.canvasPad,
      endY: MOON_LAYOUT.canvasPad + 228 - MOON_LAYOUT.nodeHeight / 2,
      path: `M${MOON_LAYOUT.canvasPad} ${MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeHeight / 2}L${MOON_LAYOUT.canvasPad} ${MOON_LAYOUT.canvasPad + 228 - MOON_LAYOUT.nodeHeight / 2}`,
    });
  });

  it('routes branch lines through card ports without bottom loop curves', () => {
    const geometry = getEdgeGeometry(
      { from: 'a', to: 'b' },
      layout([
        ['a', 0, 0],
        ['b', 368, 228],
      ]),
    );

    expect(geometry?.path).toBe('M120 168L120 200L488 268L488 300');
    expect(geometry?.path).not.toContain('C');
  });

  it('uses side ports for same-rank relationships', () => {
    const geometry = getEdgeGeometry(
      { from: 'a', to: 'b' },
      layout([
        ['a', 0, 0],
        ['b', 368, 0],
      ]),
    );

    expect(geometry).toMatchObject({
      startX: MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeWidth / 2,
      startY: MOON_LAYOUT.canvasPad,
      endX: MOON_LAYOUT.canvasPad + 368 - MOON_LAYOUT.nodeWidth / 2,
      endY: MOON_LAYOUT.canvasPad,
    });
    expect(geometry?.path).not.toContain('C');
  });
});
