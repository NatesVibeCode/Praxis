import React from 'react';

interface LoadingSkeletonProps {
  lines?: number;
  height?: number;
  width?: string;
  widths?: string[];
}

export function LoadingSkeleton({ lines = 3, height = 16, width, widths }: LoadingSkeletonProps) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {Array.from({ length: lines }, (_, i) => (
        <div
          key={i}
          style={{
            height,
            width: widths?.[i] ?? width ?? '100%',
            background: 'var(--border, #30363d)',
            borderRadius: 4,
            opacity: 0.5,
            animation: 'skeleton-pulse 1.5s ease-in-out infinite',
            animationDelay: `${i * 0.1}s`,
          }}
        />
      ))}
      <style>{`
        @keyframes skeleton-pulse {
          0%, 100% { opacity: 0.3; }
          50% { opacity: 0.6; }
        }
      `}</style>
    </div>
  );
}
