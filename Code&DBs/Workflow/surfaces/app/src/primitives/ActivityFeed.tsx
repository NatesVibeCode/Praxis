import type { StatusTone } from '../primitives-prx/types';

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

function statusTone(status: string): StatusTone {
  if (status === 'succeeded') return 'ok';
  if (status === 'running' || status === 'claimed') return 'warn';
  if (status === 'failed' || status === 'dead_letter') return 'err';
  return 'dim';
}

export function ActivityFeed({ title, data }: ActivityFeedProps) {
  return (
    <div className="prx-friction" data-testid="prx-activity-feed">
      {title && <div className="hd"><span>{title}</span></div>}
      {data.length === 0 ? (
        <div className="prx-friction__empty">No activity yet</div>
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
              <div className="row" key={i}>
                <span className="ts">{ts ? formatRelative(ts) : ''}</span>
                <span className="actor">{agent ?? ''}</span>
                <span className="what">{label}</span>
                <span className="stat" data-tone={tone}>{status || '—'}</span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
