import React from 'react';

interface MetricCardProps {
  label?: string;
  value?: string | number | null;
  color?: string;
}

export function MetricCard({ label, value, color }: MetricCardProps) {
  return (
    <div style={{
      background: 'var(--bg-card, #161b22)',
      border: '1px solid var(--border, #30363d)',
      borderRadius: 'var(--radius, 6px)',
      padding: 'var(--space-lg, 16px)',
      display: 'flex',
      flexDirection: 'column',
      gap: 6,
      minWidth: 140,
      flex: '1 1 0',
    }}>
      {label && (
        <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: 12 }}>{label}</div>
      )}
      <div style={{
        fontSize: 28,
        fontWeight: 700,
        color: color ?? 'var(--text, #e6edf3)',
        lineHeight: 1,
      }}>
        {value ?? '—'}
      </div>
    </div>
  );
}
