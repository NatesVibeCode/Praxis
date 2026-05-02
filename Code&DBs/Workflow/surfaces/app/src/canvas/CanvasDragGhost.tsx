import React from 'react';
import type { DragState } from './useCanvasDrag';
import { CanvasGlyph } from './CanvasGlyph';

interface Props {
  drag: DragState;
}

export function CanvasDragGhost({ drag }: Props) {
  if (!drag.active || !drag.payload) return null;

  return (
    <div
      className="canvas-drag-ghost"
      style={{
        left: drag.ghostX,
        top: drag.ghostY,
      }}
    >
      <span className="canvas-drag-ghost__label">{drag.payload.label}</span>
      {drag.hoveredTarget && (
        <span className="canvas-drag-ghost__hint">
          {drag.hoveredTarget.zone === 'append' ? '+ Add' : 'Drop here'}
        </span>
      )}
    </div>
  );
}
