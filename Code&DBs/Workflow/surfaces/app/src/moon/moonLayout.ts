import type React from 'react';

export const MOON_LAYOUT = {
  canvasPad: 120,
  graphAppendRadius: 20,
  graphAppendSize: 40,
  minGraphHeight: 200,
  nodeRadius: 30,
  nodeSpacing: 120,
  nodeSize: 60,
  projectedNodeSize: 44,
  triggerMenuWidth: 320,
} as const;

export const MOON_LAYOUT_CSS_VARS = {
  '--moon-layout-canvas-pad': `${MOON_LAYOUT.canvasPad}px`,
  '--moon-layout-graph-append-size': `${MOON_LAYOUT.graphAppendSize}px`,
  '--moon-layout-node-size': `${MOON_LAYOUT.nodeSize}px`,
  '--moon-layout-node-spacing': `${MOON_LAYOUT.nodeSpacing}px`,
  '--moon-layout-projected-node-size': `${MOON_LAYOUT.projectedNodeSize}px`,
} as React.CSSProperties;

export function getMoonCanvasDimensions(layout: { width: number; height: number }): { width: number; height: number } {
  return {
    width: layout.width + MOON_LAYOUT.canvasPad * 2,
    height: layout.height + MOON_LAYOUT.canvasPad * 2,
  };
}

export function getMoonNodeCanvasPosition(node: { x: number; y: number }): { left: number; top: number } {
  return {
    left: node.x + MOON_LAYOUT.canvasPad - MOON_LAYOUT.nodeRadius,
    top: node.y + MOON_LAYOUT.canvasPad - MOON_LAYOUT.nodeRadius,
  };
}

export function getMoonNodeAnchorRect(containerRect: DOMRect, node: { x: number; y: number }): DOMRect {
  const position = getMoonNodeCanvasPosition(node);
  return new DOMRect(
    containerRect.left + position.left,
    containerRect.top + position.top,
    MOON_LAYOUT.nodeSize,
    MOON_LAYOUT.nodeSize,
  );
}

export function getMoonAppendPosition(layoutWidth: number): { left: number; top: number } {
  return {
    left: layoutWidth + MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeRadius,
    top: MOON_LAYOUT.canvasPad - MOON_LAYOUT.graphAppendRadius,
  };
}
