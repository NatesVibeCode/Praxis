import type { LedTone, StatusTone } from '../primitives-prx/types';

interface StatusGridProps {
  title?: string;
  data: unknown[];
  columns?: number;
}

function toLedTone(status: string): LedTone {
  if (status === 'active' || status === 'healthy' || status === 'succeeded') return 'ok';
  if (status === 'failed' || status === 'error' || status === 'dead') return 'err';
  if (status === 'running' || status === 'loading' || status === 'warning' || status === 'degraded') return 'live';
  return 'idle';
}

export function StatusGrid({ title, data, columns = 3 }: StatusGridProps) {
  return (
    <div className="prx-status-grid" data-testid="prx-status-grid">
      {title && <div className="prx-status-grid__head">{title}</div>}
      <div
        className="prx-status-grid__body"
        style={{ gridTemplateColumns: `repeat(${columns}, 1fr)` }}
      >
        {data.map((item, i) => {
          const row = item as Record<string, unknown>;
          const name = String(row.name ?? row.label ?? row.id ?? `Item ${i + 1}`);
          const status = String(row.status ?? 'unknown');
          const detail = row.detail ?? row.description ?? row.agent ?? null;
          const tone = toLedTone(status);
          return (
            <div key={i} className="prx-status-grid__cell">
              <span className="prx-led" data-tone={tone} />
              <div className="prx-status-grid__cell-text">
                <span className="prx-status-grid__cell-name">{name}</span>
                {detail && (
                  <span className="prx-status-grid__cell-detail">{String(detail)}</span>
                )}
              </div>
            </div>
          );
        })}
      </div>
      {data.length === 0 && (
        <div className="prx-status-grid__empty">No items</div>
      )}
    </div>
  );
}
