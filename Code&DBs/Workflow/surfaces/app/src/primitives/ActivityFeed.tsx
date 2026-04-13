import React from 'react';

interface ActivityFeedProps {
  title?: string;
  data: unknown[];
}

function formatRelative(ts: string | null | undefined): string {
  if (!ts) return '';
  const d = new Date(ts);
  if (isNaN(d.getTime())) return '';
  const diffMs = Date.now() - d.getTime();
  const mins = Math.floor(diffMs / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function statusDot(status: string): string {
  if (status === 'succeeded') return '#3fb950';
  if (status === 'running' || status === 'claimed') return '#58a6ff';
  if (status === 'failed' || status === 'dead_letter') return '#f85149';
  return '#8b949e';
}

export function ActivityFeed({ title, data }: ActivityFeedProps) {
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
      {data.length === 0 ? (
        <div style={{ padding: 'var(--space-lg, 16px)', color: 'var(--text-muted, #8b949e)', fontSize: 13 }}>
          No activity yet
        </div>
      ) : (
        <div>
          {data.map((item, i) => {
            const row = item as Record<string, unknown>;
            const label = String(row.label ?? row.name ?? row.spec_name ?? row.run_id ?? `Event ${i + 1}`);
            const status = String(row.status ?? '');
            const ts = (row.created_at ?? row.started_at ?? row.updated_at) as string | undefined;
            const agent = row.agent ?? row.agent_slug ?? null;
            return (
              <div key={i} style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                padding: '10px var(--space-lg, 16px)',
                borderBottom: i < data.length - 1 ? '1px solid var(--border, #30363d)' : 'none',
              }}>
                <div style={{
                  width: 8, height: 8, borderRadius: '50%',
                  background: statusDot(status), flexShrink: 0,
                }} />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{
                    fontSize: 13, color: 'var(--text, #e6edf3)',
                    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
                  }}>
                    {label}
                  </div>
                  {agent && (
                    <div style={{ fontSize: 11, color: 'var(--text-muted, #8b949e)' }}>
                      {String(agent)}
                    </div>
                  )}
                </div>
                {ts && (
                  <div style={{ fontSize: 11, color: 'var(--text-muted, #8b949e)', flexShrink: 0 }}>
                    {formatRelative(ts)}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
