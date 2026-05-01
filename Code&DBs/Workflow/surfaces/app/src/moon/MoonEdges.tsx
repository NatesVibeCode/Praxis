import React from 'react';
import type { OrbitEdge, GraphLayout, GateState, GlyphType } from './moonBuildPresenter';
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

export type MoonGateTone = 'empty' | 'inert' | 'pending' | 'passed' | 'blocked';

export interface EdgePresentation {
  /** Short code shown on the canvas chip. */
  shortLabel: 'OK' | 'THEN' | 'ELSE' | 'COND' | 'FAIL' | 'ANY';
  /** Human label for inspector/card copy. */
  routeLabel: string;
  /** State label, separate from route/family. */
  stateLabel: string;
  glyph: GlyphType;
  tone: MoonGateTone;
  strokeDasharray?: string;
  /** CSS color string or var() reference. */
  color: string;
  /** Stroke width before sibling-fan thinning. */
  baseWidth: number;
}

function gateTone(gateState: GateState): MoonGateTone {
  switch (gateState) {
    case 'blocked': return 'blocked';
    case 'passed': return 'passed';
    case 'proposed': return 'pending';
    case 'configured': return 'inert';
    case 'empty':
    default:
      return 'empty';
  }
}

function gateStateLabel(gateState: GateState): string {
  switch (gateState) {
    case 'blocked': return 'Blocked';
    case 'passed': return 'Passed';
    case 'proposed': return 'Needs decision';
    case 'configured': return 'Configured';
    case 'empty':
    default:
      return 'Ungated';
  }
}

function stateColor(tone: MoonGateTone, baseColor: string): string {
  switch (tone) {
    case 'blocked': return 'var(--moon-state-error, #f85149)';
    case 'passed': return 'var(--moon-state-success, #3fb950)';
    case 'pending': return 'var(--moon-state-warning, #d29922)';
    case 'inert':
    case 'empty':
    default:
      return baseColor;
  }
}

/**
 * One canvas presentation contract for edge lines and gate pods.
 * Family controls glyph/label/dash. Color controls state only.
 */
export function edgePresentation(
  edge: Pick<OrbitEdge, 'gateFamily' | 'branchReason' | 'gateState' | 'gateLabel'>,
  isFlowing = false,
): EdgePresentation {
  const baseColor = isFlowing
    ? 'var(--moon-fg, #ffffff)'
    : 'var(--moon-fg-muted, rgba(232, 232, 232, 0.55))';
  const tone = gateTone(edge.gateState);
  const color = stateColor(tone, baseColor);
  const branchReason = (edge.branchReason || '').trim().toLowerCase();

  switch (edge.gateFamily) {
    case 'conditional':
      if (branchReason === 'then') {
        return { shortLabel: 'THEN', routeLabel: edge.gateLabel || 'Then path', stateLabel: gateStateLabel(edge.gateState), glyph: 'decompose', tone, color, strokeDasharray: '6 6', baseWidth: 1.15 };
      }
      if (branchReason === 'else') {
        return { shortLabel: 'ELSE', routeLabel: edge.gateLabel || 'Else path', stateLabel: gateStateLabel(edge.gateState), glyph: 'decompose', tone, color, strokeDasharray: '6 6', baseWidth: 1.15 };
      }
      return { shortLabel: 'COND', routeLabel: edge.gateLabel || 'Conditional path', stateLabel: gateStateLabel(edge.gateState), glyph: 'decompose', tone, color, strokeDasharray: '6 6', baseWidth: 1.15 };
    case 'after_failure':
      return { shortLabel: 'FAIL', routeLabel: edge.gateLabel || 'Failure path', stateLabel: gateStateLabel(edge.gateState), glyph: 'warning', tone, color, strokeDasharray: undefined, baseWidth: 1.25 };
    case 'after_any':
      return { shortLabel: 'ANY', routeLabel: edge.gateLabel || 'Always path', stateLabel: gateStateLabel(edge.gateState), glyph: 'loop', tone, color, strokeDasharray: '2 5', baseWidth: 1.05 };
    case 'after_success':
      return { shortLabel: 'OK', routeLabel: edge.gateLabel || 'Success path', stateLabel: gateStateLabel(edge.gateState), glyph: 'arrow', tone, color, strokeDasharray: undefined, baseWidth: 1.2 };
    default:
      return { shortLabel: 'OK', routeLabel: edge.gateLabel || 'Standard path', stateLabel: gateStateLabel(edge.gateState), glyph: 'arrow', tone, color, strokeDasharray: undefined, baseWidth: 1.15 };
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
        const style = edgePresentation(edge, isFlowing);
        // When 3+ siblings share a source, thin each blade so the bundle
        // reads as a single fan gesture rather than three competing lines.
        const fanThin = edge.siblingCount >= 3 ? 0.8 : 1;
        const baseWidth = isSelected ? style.baseWidth + 0.55 : style.baseWidth;
        const width = Math.max(1, baseWidth * fanThin);

        // Focus-lineage dim: when a selection has reduced this edge out of
        // the lineage set, drop opacity so attention collapses to the chain
        // the user is inspecting. edge.inLineage is true by default when no
        // selection is active, so rest-state rendering is unchanged.
        const groupOpacity = edge.inLineage ? 1 : 0.22;
        return (
          <g
            key={edge.id}
            filter={isSelected ? 'url(#moon-edge-glow)' : undefined}
            style={{ opacity: groupOpacity, transition: 'opacity 240ms ease' }}
          >
            <path
              d={geometry.path}
              stroke={isSelected ? 'rgba(246, 241, 232, 0.24)' : 'rgba(246, 241, 232, 0.1)'}
              strokeWidth={isSelected ? Math.max(4, width * 2) : Math.max(3, width * 1.8)}
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
              stroke={isSelected ? 'rgba(255, 255, 255, 0.78)' : style.color}
              strokeWidth={isSelected ? Math.max(width, 1.9) : Math.max(width, 1.2)}
              strokeLinecap="round"
              strokeLinejoin="round"
              fill="none"
              strokeDasharray={style.strokeDasharray}
              style={{ pointerEvents: 'none', transition: 'stroke 300ms ease, stroke-width 300ms ease' }}
            />
            <circle
              cx={geometry.startX}
              cy={geometry.startY}
              r={isSelected ? 3.2 : 2.4}
              fill={isSelected ? 'rgba(255, 255, 255, 0.76)' : 'rgba(246, 241, 232, 0.42)'}
              stroke="rgba(8, 8, 8, 0.82)"
              strokeWidth={1.2}
              style={{ pointerEvents: 'none' }}
            />
            <circle
              cx={geometry.endX}
              cy={geometry.endY}
              r={isSelected ? 3.2 : 2.4}
              fill={isSelected ? 'rgba(255, 255, 255, 0.76)' : 'rgba(246, 241, 232, 0.42)'}
              stroke="rgba(8, 8, 8, 0.82)"
              strokeWidth={1.2}
              style={{ pointerEvents: 'none' }}
            />
            {isSelected && (
              <path
                d={geometry.path}
                stroke={style.color}
                strokeWidth={Math.max(width * 1.2, 1.2)}
                strokeLinecap="round"
                fill="none"
                strokeDasharray="3 28"
                style={{
                  pointerEvents: 'none',
                  opacity: 0.24,
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
