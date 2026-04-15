import React from 'react';

interface LoadingSkeletonProps {
  lines?: number;
  height?: number;
  width?: string;
  widths?: string[];
}

export function LoadingSkeleton({ lines = 3, height = 16, width, widths }: LoadingSkeletonProps) {
  return (
    <div className="ws-skeleton" aria-hidden="true">
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
