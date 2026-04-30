import {
  getMoonAppendPosition,
  getMoonCanvasDimensions,
  getMoonNodeCanvasPosition,
  MOON_LAYOUT,
} from './moonLayout';

describe('moonLayout', () => {
  test('uses the larger Moon workspace geometry contract', () => {
    expect(MOON_LAYOUT).toMatchObject({
      graphAppendRadius: 24,
      graphAppendSize: 48,
      minGraphHeight: 260,
      nodeHeight: 96,
      nodeRadius: 36,
      nodeSize: 72,
      nodeSpacing: 144,
      nodeWidth: 244,
      projectedNodeSize: 52,
      triggerMenuWidth: 400,
    });
  });

  test('derives node and append positions from the shared layout authority', () => {
    expect(getMoonNodeCanvasPosition({ x: 0, y: 0 })).toEqual({
      left: MOON_LAYOUT.canvasPad - MOON_LAYOUT.nodeWidth / 2,
      top: MOON_LAYOUT.canvasPad - MOON_LAYOUT.nodeHeight / 2,
    });

    expect(getMoonAppendPosition({ width: 360, height: 520 })).toEqual({
      left: 360 / 2 + MOON_LAYOUT.canvasPad - MOON_LAYOUT.graphAppendRadius,
      top: 520 + MOON_LAYOUT.canvasPad - MOON_LAYOUT.graphAppendRadius,
    });
  });

  test('expands graph canvas dimensions using shared padding', () => {
    expect(getMoonCanvasDimensions({ width: 420, height: 180 })).toEqual({
      width: 420 + MOON_LAYOUT.canvasPad * 2,
      height: 180 + MOON_LAYOUT.canvasPad * 2,
    });
  });
});
