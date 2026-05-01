import React from 'react';

interface MetricCardProps {
  label?: string;
  value?: string | number | null;
  color?: string;
}

/**
 * MetricCard — renders a single prx-roi-style stat tile.
 * Public API unchanged. For multi-stat layouts, prefer StatsRow
 * (compressed) or compose MetricCards inside a grid.
 */
export function MetricCard({ label, value, color }: MetricCardProps) {
  return (
    <div className="prx-roi" data-testid="prx-metric-card" style={{ gridTemplateColumns: '1fr', minWidth: 140 }}>
      <div className="stat" style={{ borderRight: 'none' }}>
        {label && <div className="label">{label}</div>}
        <div className="v" style={color ? { color } : undefined}>
          {value ?? '—'}
        </div>
      </div>
    </div>
  );
}
