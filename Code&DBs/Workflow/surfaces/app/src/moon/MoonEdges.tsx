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

/**
 * Line treatment derived from gate family. One source of truth so the canvas
 * and any preview surfaces (e.g. dock explanations) read consistent shapes.
 *
 * - conditional: dashed, white. Reads as "maybe this path." Distinct from
 *   success because it carries routing intent, not a failure outcome.
 * - after_failure: solid, muted coral tint. The only place we allow color
 *   to leak into the canvas — failure earns attention.
 * - after_any: dotted. Reads as "runs regardless" — intentionally the
 *   weakest signal because it adds no guard.
 * - after_success / untyped: solid, inherits flowing-vs-muted color.
 */
export interface EdgeStyle {
  strokeDasharray?: string;
  /** CSS color string or var() reference. */
  color: string;
  /** Stroke width before sibling-fan thinning. */
  baseWidth: number;
}

export function edgeStyleFromFamily(edge: OrbitEdge, isFlowing: boolean): EdgeStyle {
  const baseColor = isFlowing
    ? 'var(--moon-fg, #ffffff)'
    : 'var(--moon-fg-muted, rgba(232, 232, 232, 0.55))';
  switch (edge.gateFamily) {
    case 'conditional':
      return { color: 'var(--moon-fg, #ffffff)', strokeDasharray: '7 5', baseWidth: 1.75 };
    case 'after_failure':
      return {
        color: 'var(--moon-danger, #ff8a6a)',
        strokeDasharray: undefined,
        baseWidth: 1.9,
      };
    case 'after_any':
      return { color: baseColor, strokeDasharray: '2 4', baseWidth: 1.5 };
    case 'after_success':
      return { color: 'var(--moon-fg, #ffffff)', strokeDasharray: undefined, baseWidth: 1.9 };
    default:
      return { color: baseColor, strokeDasharray: undefined, baseWidth: 1.75 };
  }
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
        const isFlowing = isSelected || edge.isOnDominantPath;
        const style = edgeStyleFromFamily(edge, isFlowing);
        // When 3+ siblings share a source, thin each blade so the bundle
        // reads as a single fan gesture rather than three competing lines.
        const fanThin = edge.siblingCount >= 3 ? 0.8 : 1;
        const baseWidth = isSelected ? style.baseWidth + 0.75 : style.baseWidth;
        const width = Math.max(1, baseWidth * fanThin);

        // Focus-lineage dim: when a selection has reduced this edge out of
        // the lineage set, drop opacity so attention collapses to the chain
        // the user is inspecting. edge.inLineage is true by default when no
        // selection is active, so rest-state rendering is unchanged.
        const groupOpacity = edge.inLineage ? 1 : 0.22;
        return (
          <g
            key={edge.id}
            filter={isFlowing ? 'url(#moon-edge-glow)' : undefined}
            style={{ opacity: groupOpacity, transition: 'opacity 240ms ease' }}
          >
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
              stroke={style.color}
              strokeWidth={width}
              fill="none"
              strokeDasharray={style.strokeDasharray}
              style={{ pointerEvents: 'none', transition: 'stroke 300ms ease, stroke-width 300ms ease' }}
            />
            {isFlowing && (
              <path
                d={geometry.path}
                stroke={style.color}
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
