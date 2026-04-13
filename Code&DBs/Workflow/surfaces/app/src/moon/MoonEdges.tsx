import React from 'react';
import type { OrbitEdge, DagLayout } from './moonBuildPresenter';

const R = 30; // node radius = half of 60px node size

interface MoonEdgesProps {
  edges: OrbitEdge[];
  layout: DagLayout;
  selectedEdgeId: string | null;
  onEdgeClick?: (id: string) => void;
}

export function MoonEdges({ edges, layout, selectedEdgeId, onEdgeClick }: MoonEdgesProps) {
  const PAD = 120; // canvas padding
  return (
    <svg
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        width: layout.width + PAD * 2,
        height: layout.height + PAD * 2,
        pointerEvents: 'none',
      }}
    >
      {edges.map((edge) => {
        const from = layout.nodes.get(edge.from);
        const to = layout.nodes.get(edge.to);
        if (!from || !to) return null;

        const x1 = from.x + PAD + R;
        const y1 = from.y + PAD;
        const x2 = to.x + PAD - R;
        const y2 = to.y + PAD;
        const dx = x2 - x1;
        const d = `M${x1} ${y1}C${x1 + dx * 0.4} ${y1},${x2 - dx * 0.4} ${y2},${x2} ${y2}`;

        const isSelected = edge.id === selectedEdgeId;
        const color = isSelected || edge.isOnDominantPath
          ? 'var(--moon-accent, #6CB6FF)'
          : 'var(--moon-muted, #484f58)';
        const width = isSelected ? 2.5 : edge.isOnDominantPath ? 2 : 1.5;

        return (
          <path
            key={edge.id}
            d={d}
            stroke={color}
            strokeWidth={width}
            fill="none"
            style={{ pointerEvents: 'stroke', cursor: 'pointer' }}
            onClick={(e) => {
              e.stopPropagation();
              onEdgeClick?.(edge.id);
            }}
          />
        );
      })}
    </svg>
  );
}
