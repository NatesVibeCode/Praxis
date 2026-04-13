import React from 'react';

interface StatusGridProps {
  title?: string;
  data: unknown[];
  columns?: number;
}

function statusColor(status: string): string {
  if (status === 'active' || status === 'healthy' || status === 'succeeded') return '#3fb950';
  if (status === 'running' || status === 'loading') return '#58a6ff';
  if (status === 'failed' || status === 'error' || status === 'dead') return '#f85149';
  if (status === 'warning' || status === 'degraded') return '#d29922';
  return '#8b949e';
}

export function StatusGrid({ title, data, columns = 3 }: StatusGridProps) {
  return (
    <div style={{
      background: 'var(--bg-card, #161b22)',
      border: '1px solid var(--border, #30363d)',
      borderRadius: 'var(--radius, 6px)',
      overflow: 'hidden',
    }}>
      {title && (
        <div style={{
          padding: 'var(--space-md, 12px) var(--space-lg, 16px)',
          borderBottom: '1px solid var(--border, #30363d)',
          fontWeight: 600,
          fontSize: 14,
        }}>
          {title}
        </div>
      )}
      <div style={{
        display: 'grid',
        gridTemplateColumns: `repeat(${columns}, 1fr)`,
        gap: 1,
        background: 'var(--border, #30363d)',
      }}>
        {data.map((item, i) => {
          const row = item as Record<string, unknown>;
          const name = String(row.name ?? row.label ?? row.id ?? `Item ${i + 1}`);
          const status = String(row.status ?? 'unknown');
          const detail = row.detail ?? row.description ?? row.agent ?? null;
          return (
            <div key={i} style={{
              background: 'var(--bg-card, #161b22)',
              padding: 'var(--space-md, 12px)',
              display: 'flex',
              alignItems: 'center',
              gap: 8,
            }}>
              <div style={{
                width: 8, height: 8, borderRadius: '50%',
                background: statusColor(status), flexShrink: 0,
              }} />
              <div style={{ minWidth: 0 }}>
                <div style={{ fontSize: 13, fontWeight: 500, color: 'var(--text, #e6edf3)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                  {name}
                </div>
                {detail && (
                  <div style={{ fontSize: 11, color: 'var(--text-muted, #8b949e)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {String(detail)}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
      {data.length === 0 && (
        <div style={{ padding: 'var(--space-lg, 16px)', color: 'var(--text-muted, #8b949e)', fontSize: 13 }}>
          No items
        </div>
      )}
    </div>
  );
}
