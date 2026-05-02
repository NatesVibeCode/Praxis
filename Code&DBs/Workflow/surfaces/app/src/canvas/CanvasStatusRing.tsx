import React from 'react';
import { statusState, statusStrokeVar, type CanvasStatusState, type RawStatus } from './canvasStatus';

interface Props {
  /** Raw engine status (succeeded, running, failed, etc.) OR a resolved state. */
  status: RawStatus | CanvasStatusState;
  /** Ring diameter in px. Default 14. */
  size?: number;
  /** Stroke weight in px. Default size/7. */
  weight?: number;
  /** When true, adds an outer halo ring (used for selected/active run-tiles). */
  halo?: boolean;
  /** Optional aria-label override. Default: the status label. */
  label?: string;
}

/**
 * Small hollow ring rendering the Canvas node silhouette at a compact scale.
 * Inherits the same vocabulary used by graph nodes:
 *
 *   pending → dim ring, static
 *   idle    → same ring, brighter
 *   active  → outline + rotating conic-gradient sweep (pure CSS)
 *   ok      → outline fills to solid white
 *   failed  → coral outline with a 45° gap on the right (broken-arc notch)
 *
 * No hue shift between pending/idle/active/ok. Only failed earns coral.
 * Animations are CSS-driven via the canvas-status-ring-* keyframes in
 * canvas/style/components.css.
 */
export function CanvasStatusRing({
  status,
  size = 14,
  weight,
  halo = false,
  label,
}: Props) {
  const state: CanvasStatusState = (['pending', 'idle', 'active', 'ok', 'failed'] as CanvasStatusState[])
    .includes(status as CanvasStatusState)
    ? (status as CanvasStatusState)
    : statusState(status as RawStatus);
  const stroke = statusStrokeVar(state);
  const w = weight ?? Math.max(1.25, size / 7);
  const r = (size - w) / 2;
  const c = size / 2;

  // Broken-arc path for `failed` — outline minus a ~45° wedge on the upper right.
  // Reads as "something cracked here" without needing a second icon.
  const failedPath = () => {
    const startAngle = -30; // degrees from 12 o'clock
    const endAngle = -75;
    const toXY = (deg: number) => {
      const rad = ((deg - 90) * Math.PI) / 180;
      return { x: c + r * Math.cos(rad), y: c + r * Math.sin(rad) };
    };
    const a = toXY(startAngle);
    const b = toXY(endAngle);
    // Sweep the long way around the circle (348° arc) from `a` to `b`.
    return `M${a.x.toFixed(2)} ${a.y.toFixed(2)} A${r} ${r} 0 1 0 ${b.x.toFixed(2)} ${b.y.toFixed(2)}`;
  };

  return (
    <span
      className={`canvas-status-ring canvas-status-ring--${state}${halo ? ' canvas-status-ring--halo' : ''}`}
      role="img"
      aria-label={label ?? state}
      style={{
        width: size,
        height: size,
        // CSS reads these to size the conic sweep / fill ring.
        ['--canvas-ring-size' as any]: `${size}px`,
        ['--canvas-ring-weight' as any]: `${w}px`,
        ['--canvas-ring-stroke' as any]: stroke,
      }}
    >
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} aria-hidden="true">
        {state === 'failed' ? (
          <path
            d={failedPath()}
            fill="none"
            stroke={stroke}
            strokeWidth={w}
            strokeLinecap="round"
          />
        ) : (
          <circle
            cx={c}
            cy={c}
            r={r}
            fill={state === 'ok' ? stroke : 'none'}
            stroke={stroke}
            strokeWidth={w}
            opacity={state === 'pending' ? 0.45 : state === 'idle' ? 0.7 : 1}
          />
        )}
      </svg>
    </span>
  );
}
