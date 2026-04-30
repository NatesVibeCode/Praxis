import React from 'react';
import type { OrbitEdge, GraphLayout } from './moonBuildPresenter';
import { MOON_LAYOUT } from './moonLayout';

const EDGE_STEM = 32;
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

  const rawDx = to.x - from.x;
  const rawDy = to.y - from.y;
  const fromHalfWidth = (from.width ?? MOON_LAYOUT.nodeWidth) / 2;
  const fromHalfHeight = (from.height ?? MOON_LAYOUT.nodeHeight) / 2;
  const toHalfWidth = (to.width ?? MOON_LAYOUT.nodeWidth) / 2;
  const toHalfHeight = (to.height ?? MOON_LAYOUT.nodeHeight) / 2;
  const verticalFlow = Math.abs(rawDy) >= Math.max(fromHalfHeight, toHalfHeight) / 2;
  const xSign = rawDx >= 0 ? 1 : -1;
  const ySign = rawDy >= 0 ? 1 : -1;

  const startX = verticalFlow ? from.x + pad : from.x + pad + xSign * fromHalfWidth;
  const startY = verticalFlow ? from.y + pad + ySign * fromHalfHeight : from.y + pad;
  const endX = verticalFlow ? to.x + pad : to.x + pad - xSign * toHalfWidth;
  const endY = verticalFlow ? to.y + pad - ySign * toHalfHeight : to.y + pad;

  let path: string;
  if (verticalFlow) {
    if (Math.abs(startX - endX) < 1) {
      path = `M${startX} ${startY}L${endX} ${endY}`;
    } else {
      const startLeadY = startY + ySign * EDGE_STEM;
      const endLeadY = endY - ySign * EDGE_STEM;
      path = `M${startX} ${startY}L${startX} ${startLeadY}L${endX} ${endLeadY}L${endX} ${endY}`;
    }
  } else {
    const midX = (startX + endX) / 2;
    path = `M${startX} ${startY}L${midX} ${startY}L${midX} ${endY}L${endX} ${endY}`;
  }

  return {
    centerX: (startX + endX) / 2,
    centerY: (startY + endY) / 2,
    endX,
    endY,
    path,
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
      className="moon-graph-edges"
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        width: layout.width + MOON_GRAPH_CANVAS_PAD * 2,
        height: layout.height + MOON_GRAPH_CANVAS_PAD * 2,
        overflow: 'visible',
        pointerEvents: 'none',
        zIndex: 4,
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
              stroke={isFlowing ? 'rgba(246, 241, 232, 0.52)' : 'rgba(246, 241, 232, 0.36)'}
              strokeWidth={Math.max(9, width * 3.6)}
              strokeLinecap="round"
              strokeLinejoin="round"
              fill="none"
              style={{ pointerEvents: 'none' }}
            />
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
              stroke={isFlowing ? 'rgba(255, 255, 255, 0.92)' : style.color}
              strokeWidth={Math.max(width, 3.25)}
              strokeLinecap="round"
              strokeLinejoin="round"
              fill="none"
              strokeDasharray={style.strokeDasharray}
              style={{ pointerEvents: 'none', transition: 'stroke 300ms ease, stroke-width 300ms ease' }}
            />
            <circle
              cx={geometry.startX}
              cy={geometry.startY}
              r={4.4}
              fill={isFlowing ? 'rgba(255, 255, 255, 0.96)' : 'rgba(246, 241, 232, 0.76)'}
              stroke="rgba(8, 8, 8, 0.82)"
              strokeWidth={1.8}
              style={{ pointerEvents: 'none' }}
            />
            <circle
              cx={geometry.endX}
              cy={geometry.endY}
              r={4.4}
              fill={isFlowing ? 'rgba(255, 255, 255, 0.96)' : 'rgba(246, 241, 232, 0.76)'}
              stroke="rgba(8, 8, 8, 0.82)"
              strokeWidth={1.8}
              style={{ pointerEvents: 'none' }}
            />
            {isFlowing && (
              <path
                d={geometry.path}
                stroke={style.color}
                strokeWidth={width * 1.6}
                strokeLinecap="round"
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
