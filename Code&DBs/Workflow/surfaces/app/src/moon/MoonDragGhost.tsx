import React from 'react';
import type { DragState } from './useMoonDrag';
import { MoonGlyph } from './MoonGlyph';

interface Props {
  drag: DragState;
}

export function MoonDragGhost({ drag }: Props) {
  if (!drag.active || !drag.payload) return null;

  return (
    <div
      className="moon-drag-ghost"
      style={{
        left: drag.ghostX,
        top: drag.ghostY,
      }}
    >
      <span className="moon-drag-ghost__label">{drag.payload.label}</span>
      {drag.hoveredTarget && (
        <span className="moon-drag-ghost__hint">
          {drag.hoveredTarget.zone === 'append' ? '+ Add' : 'Drop here'}
        </span>
      )}
    </div>
  );
}
