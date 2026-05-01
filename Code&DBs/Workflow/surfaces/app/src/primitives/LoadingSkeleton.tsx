import React from 'react';

interface LoadingSkeletonProps {
  lines?: number;
  height?: number;
  width?: string;
  widths?: string[];
}

/**
 * LoadingSkeleton — same shape as before for content-shape hints, plus
 * a prx-spinner sibling so consumers that just need a "working" indicator
 * can swap in the firmware-glyph rotation.
 *
 * Public API unchanged. Shipped class `ws-skeleton` preserved for any
 * stylesheet still scoped to it.
 */
export function LoadingSkeleton({ lines = 3, height = 16, width, widths }: LoadingSkeletonProps) {
  return (
    <div className="ws-skeleton" data-testid="prx-loading-skeleton" aria-hidden="true">
      {Array.from({ length: lines }, (_, i) => (
        <div
          key={i}
          className="ws-skeleton__line"
          style={{
            height,
            width: widths?.[i] ?? width ?? '100%',
            animationDelay: `${i * 0.12}s`,
          }}
        />
      ))}
    </div>
  );
}
