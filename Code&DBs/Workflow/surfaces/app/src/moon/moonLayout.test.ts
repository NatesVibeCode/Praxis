import {
  getMoonAppendPosition,
  getMoonCanvasDimensions,
  getMoonNodeCanvasPosition,
  MOON_LAYOUT,
} from './moonLayout';

describe('moonLayout', () => {
  test('derives node and append positions from the shared layout authority', () => {
    expect(getMoonNodeCanvasPosition({ x: 0, y: 0 })).toEqual({
      left: MOON_LAYOUT.canvasPad - MOON_LAYOUT.nodeRadius,
      top: MOON_LAYOUT.canvasPad - MOON_LAYOUT.nodeRadius,
    });

    expect(getMoonAppendPosition(360)).toEqual({
      left: 360 + MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeRadius,
      top: MOON_LAYOUT.canvasPad - MOON_LAYOUT.graphAppendRadius,
    });
  });

  test('expands graph canvas dimensions using shared padding', () => {
    expect(getMoonCanvasDimensions({ width: 420, height: 180 })).toEqual({
      width: 420 + MOON_LAYOUT.canvasPad * 2,
      height: 180 + MOON_LAYOUT.canvasPad * 2,
    });
  });
});
