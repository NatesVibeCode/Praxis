import React from 'react';

interface Stat {
  label: string;
  value?: string | number | null;
  color?: string;
}

interface StatsRowProps {
  stats: Stat[];
}

export function StatsRow({ stats }: StatsRowProps) {
  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: `repeat(${Math.min(stats.length || 1, 6)}, 1fr)`,
      gap: 'var(--space-md, 12px)',
    }}>
      {stats.map((stat, i) => (
        <div key={i} style={{
          background: 'var(--bg-card, #161b22)',
          border: '1px solid var(--border, #30363d)',
          borderRadius: 'var(--radius, 6px)',
          padding: 'var(--space-lg, 16px)',
        }}>
          <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: 12, marginBottom: 4 }}>
            {stat.label}
          </div>
          <div style={{
            fontSize: 24,
            fontWeight: 700,
            color: stat.color ?? 'var(--text, #e6edf3)',
            lineHeight: 1,
          }}>
            {stat.value ?? '—'}
          </div>
        </div>
      ))}
    </div>
  );
}
