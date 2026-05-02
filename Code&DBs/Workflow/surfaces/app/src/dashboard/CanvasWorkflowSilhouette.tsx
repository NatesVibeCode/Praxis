import React from 'react';

/**
 * Tiny per-workflow silhouette used on the dashboard list + any other
 * surface that needs a "workflow face" without loading the full build
 * graph. Emits a compact SVG in the Canvas visual vocabulary:
 *
 *   [trigger chevron?] [ body dots × N ] [status ring]
 *
 * The hollow-circle language matches .canvas-graph-node and the
 * CanvasStatusRing component so the dashboard row and the canvas read as
 * one product. The rightmost dot carries last-run state — a failed run
 * paints the tail coral (the ONLY hue Canvas allows).
 *
 * If server-side silhouette data becomes available (e.g. a graph-shape
 * hash with loop/fanout flags) this component can widen without
 * consumers changing. For now it runs on metadata that the dashboard
 * already has.
 */

export type SilhouetteStatus = 'ok' | 'active' | 'failed' | 'idle' | 'pending' | null | undefined;

interface Props {
  /** Number of body nodes to draw. Clamped [1, 5]. */
  nodeCount?: number;
  /** Whether this workflow has an enabled trigger (chevron on the left). */
  hasTrigger?: boolean;
  /** Whether the trigger is a cron schedule (short tick marks above). */
  isCron?: boolean;
  /** Last-run status — paints the tail ring. Accepts any string;
   *  unknown values are normalized to 'idle' via {@link mapStatus}. */
  lastRunStatus?: SilhouetteStatus | string;
  /** Total width in px. Default 84. */
  width?: number;
  /** Height in px. Default 22. */
  height?: number;
  /** ARIA label for screen readers. */
  label?: string;
}

function mapStatus(raw: SilhouetteStatus | string | undefined): 'ok' | 'active' | 'failed' | 'idle' | 'pending' {
  if (!raw) return 'pending';
  switch (raw) {
    case 'succeeded':
    case 'ok':
      return 'ok';
    case 'running':
    case 'claimed':
    case 'active':
      return 'active';
    case 'failed':
    case 'dead_letter':
    case 'blocked':
    case 'parent_failed':
      return 'failed';
    case 'cancelled':
    case 'ready':
    case 'idle':
      return 'idle';
    default:
      return 'pending';
  }
}

function statusVar(state: 'ok' | 'active' | 'failed' | 'idle' | 'pending'): string {
  switch (state) {
    case 'ok':
      return 'var(--canvas-status-ok, #ffffff)';
    case 'active':
      return 'var(--canvas-status-active, #ffffff)';
    case 'failed':
      return 'var(--canvas-status-failed, #ff8a6a)';
    case 'idle':
      return 'var(--canvas-status-idle, #8b949e)';
    case 'pending':
    default:
      return 'var(--canvas-status-pending, #484f58)';
  }
}

export function CanvasWorkflowSilhouette({
  nodeCount = 1,
  hasTrigger = false,
  isCron = false,
  lastRunStatus,
  width = 84,
  height = 22,
  label,
}: Props) {
  const n = Math.max(1, Math.min(5, Math.round(nodeCount)));
  const tailState = mapStatus(lastRunStatus);
  const tailStroke = statusVar(tailState);
  const neutralStroke = 'var(--canvas-status-pending, #484f58)';

  const cy = height / 2;
  const leftPad = hasTrigger ? 12 : 6;
  const rightPad = 8;
  const trackWidth = width - leftPad - rightPad;
  const step = n > 1 ? trackWidth / (n - 1) : 0;
  const nodeR = 3.25;

  const nodes: JSX.Element[] = [];
  for (let i = 0; i < n; i += 1) {
    const cx = leftPad + (n === 1 ? trackWidth / 2 : step * i);
    const isTail = i === n - 1;
    const stroke = isTail ? tailStroke : neutralStroke;
    nodes.push(
      <circle
        key={`node-${i}`}
        cx={cx}
        cy={cy}
        r={nodeR}
        fill={isTail && tailState === 'ok' ? stroke : 'none'}
        stroke={stroke}
        strokeWidth={1.25}
        opacity={isTail ? 1 : 0.55}
      />,
    );
    if (i > 0) {
      const prevCx = leftPad + step * (i - 1);
      nodes.push(
        <line
          key={`edge-${i}`}
          x1={prevCx + nodeR}
          y1={cy}
          x2={cx - nodeR}
          y2={cy}
          stroke={neutralStroke}
          strokeWidth={1}
          opacity={0.45}
        />,
      );
    }
  }

  const ariaLabel = label
    ?? `Workflow silhouette — ${n} step${n === 1 ? '' : 's'}, ${tailState}`;

  return (
    <svg
      className="canvas-workflow-silhouette"
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={ariaLabel}
    >
      {hasTrigger && (
        <g opacity={0.7}>
          {/* chevron pointing into the first node: the trigger arrow */}
          <path
            d={`M2 ${cy - 4} L8 ${cy} L2 ${cy + 4}`}
            fill="none"
            stroke={neutralStroke}
            strokeWidth={1.25}
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          {isCron && (
            <>
              {/* three tick marks above the chevron — "recurring" signal */}
              <line x1={1} y1={2} x2={1} y2={5} stroke={neutralStroke} strokeWidth={1} />
              <line x1={4} y1={2} x2={4} y2={5} stroke={neutralStroke} strokeWidth={1} />
              <line x1={7} y1={2} x2={7} y2={5} stroke={neutralStroke} strokeWidth={1} />
            </>
          )}
        </g>
      )}
      {nodes}
    </svg>
  );
}
