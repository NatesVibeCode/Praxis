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

function statusTone(status: string): 'ok' | 'warn' | 'err' | 'dim' | undefined {
  if (status === 'succeeded') return 'ok';
  if (status === 'running' || status === 'claimed') return 'warn';
  if (status === 'failed' || status === 'dead_letter') return 'err';
  if (!status) return undefined;
  return 'dim';
}

/**
 * ActivityFeed — renders the prx-runlog CSS structure.
 * Public API unchanged. Status colors come from the prx-runlog .stat
 * data-tone glyph + color rules instead of an inline dot.
 */
export function ActivityFeed({ title, data }: ActivityFeedProps) {
  return (
    <div className="prx-friction" data-testid="prx-activity-feed" style={{ overflow: 'hidden' }}>
      {title && (
        <div className="hd" style={{ paddingTop: 12, paddingBottom: 12 }}>
          <span>{title}</span>
        </div>
      )}
      {data.length === 0 ? (
        <div style={{ padding: '14px 16px', color: 'var(--text-muted, #8b949e)', fontSize: 13 }}>
          No activity yet
        </div>
      ) : (
        <div className="prx-runlog" data-testid="prx-activity-feed-rows">
          {data.map((item, i) => {
            const row = item as Record<string, unknown>;
            const label = String(row.label ?? row.name ?? row.spec_name ?? row.run_id ?? `Event ${i + 1}`);
            const status = String(row.status ?? '');
            const ts = (row.created_at ?? row.started_at ?? row.updated_at) as string | undefined;
            const agent = (row.agent ?? row.agent_slug ?? null) as string | null;
            const tone = statusTone(status);
            return (
              <div className="row" key={i} style={{ gridTemplateColumns: '88px 120px 1fr 90px' }}>
                <span className="ts">{ts ? formatRelative(ts) : ''}</span>
                <span className="actor">{agent ?? ''}</span>
                <span className="what">{label}</span>
                <span className="stat" data-tone={tone ?? 'dim'}>
                  {status || '—'}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
