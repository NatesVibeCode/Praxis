import React from 'react';
import type { OrbitEdge, GraphLayout } from './moonBuildPresenter';
import { MOON_LAYOUT } from './moonLayout';

const NODE_RADIUS = MOON_LAYOUT.nodeRadius;
export const MOON_GRAPH_CANVAS_PAD = MOON_LAYOUT.canvasPad;

export interface MoonEdgeGeometry {
  centerX: number;
  centerY: number;
  endX: number;
  endY: number;
  path: string;
  startX: number;
  startY: number;
}

export function getEdgeGeometry(
  edge: Pick<OrbitEdge, 'from' | 'to'>,
  layout: GraphLayout,
  pad = MOON_GRAPH_CANVAS_PAD,
): MoonEdgeGeometry | null {
  const from = layout.nodes.get(edge.from);
  const to = layout.nodes.get(edge.to);
  if (!from || !to) return null;

  const startX = from.x + pad + NODE_RADIUS;
  const startY = from.y + pad;
  const endX = to.x + pad - NODE_RADIUS;
  const endY = to.y + pad;
  const dx = endX - startX;

  return {
    centerX: (startX + endX) / 2,
    centerY: (startY + endY) / 2,
    endX,
    endY,
    path: `M${startX} ${startY}C${startX + dx * 0.4} ${startY},${endX - dx * 0.4} ${endY},${endX} ${endY}`,
    startX,
    startY,
  };
}

interface MoonEdgesProps {
  edges: OrbitEdge[];
  layout: GraphLayout;
  selectedEdgeId: string | null;
  onEdgeClick?: (id: string) => void;
}

export function MoonEdges({ edges, layout, selectedEdgeId, onEdgeClick }: MoonEdgesProps) {
  return (
    <svg
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        width: layout.width + MOON_GRAPH_CANVAS_PAD * 2,
        height: layout.height + MOON_GRAPH_CANVAS_PAD * 2,
        pointerEvents: 'none',
      }}
    >
      <defs>
        <filter id="moon-edge-glow" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="2" result="blur" />
          <feComposite in="SourceGraphic" in2="blur" operator="over" />
        </filter>
      </defs>
      {edges.map((edge) => {
        const geometry = getEdgeGeometry(edge, layout);
        if (!geometry) return null;

        const isSelected = edge.id === selectedEdgeId;
        const isConditional = edge.gateFamily === 'conditional';
        const isFlowing = isSelected || edge.isOnDominantPath;
        const color = isConditional
          ? 'var(--moon-fg, #ffffff)'
          : isFlowing
            ? 'var(--moon-fg, #ffffff)'
            : 'var(--moon-fg-muted, rgba(232, 232, 232, 0.55))';
        const width = isSelected ? 2.5 : edge.isOnDominantPath ? 2 : 1.75;

        return (
          <g key={edge.id} filter={isFlowing ? 'url(#moon-edge-glow)' : undefined}>
            <path
              d={geometry.path}
              stroke="transparent"
              strokeWidth={18}
              fill="none"
              data-drop-edge={edge.id}
              style={{ pointerEvents: 'stroke', cursor: 'pointer' }}
              onClick={(e) => {
                e.stopPropagation();
                onEdgeClick?.(edge.id);
              }}
            />
            <path
              d={geometry.path}
              stroke={color}
              strokeWidth={width}
              fill="none"
              strokeDasharray={isConditional ? '7 5' : undefined}
              style={{ pointerEvents: 'none', transition: 'stroke 300ms ease, stroke-width 300ms ease' }}
            />
            {isFlowing && (
              <path
                d={geometry.path}
                stroke="var(--moon-fg, #ffffff)"
                strokeWidth={width * 1.6}
                fill="none"
                strokeDasharray="4 24"
                style={{
                  pointerEvents: 'none',
                  opacity: 0.35,
                  animation: 'moon-edge-flow 2.5s infinite linear'
                }}
              />
            )}
          </g>
        );
      })}
    </svg>
  );
}
