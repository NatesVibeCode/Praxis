import type React from 'react';

export const CANVAS_LAYOUT = {
  canvasPad: 120,
  graphAppendRadius: 18,
  graphAppendSize: 36,
  minGraphHeight: 260,
  nodeHeight: 78,
  nodeRadius: 36,
  nodeSpacing: 120,
  nodeSize: 72,
  nodeWidth: 188,
  projectedNodeSize: 52,
  triggerMenuWidth: 400,
} as const;

export const CANVAS_LAYOUT_CSS_VARS = {
  '--canvas-layout-canvas-pad': `${CANVAS_LAYOUT.canvasPad}px`,
  '--canvas-layout-graph-append-size': `${CANVAS_LAYOUT.graphAppendSize}px`,
  '--canvas-layout-node-height': `${CANVAS_LAYOUT.nodeHeight}px`,
  '--canvas-layout-node-size': `${CANVAS_LAYOUT.nodeSize}px`,
  '--canvas-layout-node-spacing': `${CANVAS_LAYOUT.nodeSpacing}px`,
  '--canvas-layout-node-width': `${CANVAS_LAYOUT.nodeWidth}px`,
  '--canvas-layout-projected-node-size': `${CANVAS_LAYOUT.projectedNodeSize}px`,
} as React.CSSProperties;

export function getCanvasCanvasDimensions(layout: { width: number; height: number }): { width: number; height: number } {
  return {
    width: layout.width + CANVAS_LAYOUT.canvasPad * 2,
    height: layout.height + CANVAS_LAYOUT.canvasPad * 2,
  };
}

export function getCanvasNodeCanvasPosition(node: { x: number; y: number; width?: number; height?: number }): { left: number; top: number } {
  const width = typeof node.width === 'number' ? node.width : CANVAS_LAYOUT.nodeWidth;
  const height = typeof node.height === 'number' ? node.height : CANVAS_LAYOUT.nodeHeight;
  return {
    left: node.x + CANVAS_LAYOUT.canvasPad - width / 2,
    top: node.y + CANVAS_LAYOUT.canvasPad - height / 2,
  };
}

export function getCanvasNodeAnchorRect(containerRect: DOMRect, node: { x: number; y: number; width?: number; height?: number }): DOMRect {
  const position = getCanvasNodeCanvasPosition(node);
  const width = typeof node.width === 'number' ? node.width : CANVAS_LAYOUT.nodeWidth;
  const height = typeof node.height === 'number' ? node.height : CANVAS_LAYOUT.nodeHeight;
  return new DOMRect(
    containerRect.left + position.left,
    containerRect.top + position.top,
    width,
    height,
  );
}

export function getCanvasAppendPosition(layout: { width: number; height: number } | number): { left: number; top: number } {
  if (typeof layout === 'number') {
    return {
      left: layout + CANVAS_LAYOUT.canvasPad + CANVAS_LAYOUT.nodeRadius,
      top: CANVAS_LAYOUT.canvasPad - CANVAS_LAYOUT.graphAppendRadius,
    };
  }
  return {
    left: layout.width / 2 + CANVAS_LAYOUT.canvasPad - CANVAS_LAYOUT.graphAppendRadius,
    top: layout.height + CANVAS_LAYOUT.canvasPad - CANVAS_LAYOUT.graphAppendRadius,
  };
}
