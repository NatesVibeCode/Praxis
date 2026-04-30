import type React from 'react';

export const MOON_LAYOUT = {
  canvasPad: 120,
  graphAppendRadius: 24,
  graphAppendSize: 48,
  minGraphHeight: 260,
  nodeHeight: 96,
  nodeRadius: 36,
  nodeSpacing: 144,
  nodeSize: 72,
  nodeWidth: 244,
  projectedNodeSize: 52,
  triggerMenuWidth: 400,
} as const;

export const MOON_LAYOUT_CSS_VARS = {
  '--moon-layout-canvas-pad': `${MOON_LAYOUT.canvasPad}px`,
  '--moon-layout-graph-append-size': `${MOON_LAYOUT.graphAppendSize}px`,
  '--moon-layout-node-height': `${MOON_LAYOUT.nodeHeight}px`,
  '--moon-layout-node-size': `${MOON_LAYOUT.nodeSize}px`,
  '--moon-layout-node-spacing': `${MOON_LAYOUT.nodeSpacing}px`,
  '--moon-layout-node-width': `${MOON_LAYOUT.nodeWidth}px`,
  '--moon-layout-projected-node-size': `${MOON_LAYOUT.projectedNodeSize}px`,
} as React.CSSProperties;

export function getMoonCanvasDimensions(layout: { width: number; height: number }): { width: number; height: number } {
  return {
    width: layout.width + MOON_LAYOUT.canvasPad * 2,
    height: layout.height + MOON_LAYOUT.canvasPad * 2,
  };
}

export function getMoonNodeCanvasPosition(node: { x: number; y: number; width?: number; height?: number }): { left: number; top: number } {
  const width = typeof node.width === 'number' ? node.width : MOON_LAYOUT.nodeWidth;
  const height = typeof node.height === 'number' ? node.height : MOON_LAYOUT.nodeHeight;
  return {
    left: node.x + MOON_LAYOUT.canvasPad - width / 2,
    top: node.y + MOON_LAYOUT.canvasPad - height / 2,
  };
}

export function getMoonNodeAnchorRect(containerRect: DOMRect, node: { x: number; y: number; width?: number; height?: number }): DOMRect {
  const position = getMoonNodeCanvasPosition(node);
  const width = typeof node.width === 'number' ? node.width : MOON_LAYOUT.nodeWidth;
  const height = typeof node.height === 'number' ? node.height : MOON_LAYOUT.nodeHeight;
  return new DOMRect(
    containerRect.left + position.left,
    containerRect.top + position.top,
    width,
    height,
  );
}

export function getMoonAppendPosition(layout: { width: number; height: number } | number): { left: number; top: number } {
  if (typeof layout === 'number') {
    return {
      left: layout + MOON_LAYOUT.canvasPad + MOON_LAYOUT.nodeRadius,
      top: MOON_LAYOUT.canvasPad - MOON_LAYOUT.graphAppendRadius,
    };
  }
  return {
    left: layout.width / 2 + MOON_LAYOUT.canvasPad - MOON_LAYOUT.graphAppendRadius,
    top: layout.height + MOON_LAYOUT.canvasPad - MOON_LAYOUT.graphAppendRadius,
  };
}
