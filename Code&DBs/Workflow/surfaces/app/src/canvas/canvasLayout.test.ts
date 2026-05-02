import {
  getCanvasAppendPosition,
  getCanvasCanvasDimensions,
  getCanvasNodeCanvasPosition,
  CANVAS_LAYOUT,
} from './canvasLayout';

describe('canvasLayout', () => {
  test('uses the compact Canvas workspace geometry contract', () => {
    expect(CANVAS_LAYOUT).toMatchObject({
      graphAppendRadius: 18,
      graphAppendSize: 36,
      minGraphHeight: 260,
      nodeHeight: 78,
      nodeRadius: 36,
      nodeSize: 72,
      nodeSpacing: 120,
      nodeWidth: 188,
      projectedNodeSize: 52,
      triggerMenuWidth: 400,
    });
  });

  test('derives node and append positions from the shared layout authority', () => {
    expect(getCanvasNodeCanvasPosition({ x: 0, y: 0 })).toEqual({
      left: CANVAS_LAYOUT.canvasPad - CANVAS_LAYOUT.nodeWidth / 2,
      top: CANVAS_LAYOUT.canvasPad - CANVAS_LAYOUT.nodeHeight / 2,
    });

    expect(getCanvasAppendPosition({ width: 360, height: 520 })).toEqual({
      left: 360 / 2 + CANVAS_LAYOUT.canvasPad - CANVAS_LAYOUT.graphAppendRadius,
      top: 520 + CANVAS_LAYOUT.canvasPad - CANVAS_LAYOUT.graphAppendRadius,
    });
  });

  test('expands graph canvas dimensions using shared padding', () => {
    expect(getCanvasCanvasDimensions({ width: 420, height: 180 })).toEqual({
      width: 420 + CANVAS_LAYOUT.canvasPad * 2,
      height: 180 + CANVAS_LAYOUT.canvasPad * 2,
    });
  });
});
