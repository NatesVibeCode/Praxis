import React, { useMemo, useState } from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { publishSelection } from '../../hooks/useWorldSelection';

interface Bug {
  id?: string;
  bug_id?: string;
  title: string;
  severity: string;
  status: string;
  category?: string;
  description?: string;
  replay_ready?: boolean;
  replay_reason_code?: string | null;
  resume_context?: Record<string, unknown> | null;
  updated_at?: string | null;
  filed_at?: string | null;
}

interface BugResponse {
  bugs?: Bug[];
  count?: number;
  returned_count?: number;
}

interface BugCardConfig {
  endpoint?: string;
  title?: string;
  publishSelection?: string;
}

type TicketFilter = 'all' | 'critical' | 'replayable';

function bugId(bug: Bug): string {
  return bug.bug_id ?? bug.id ?? '';
}

function endpointWithOpenTicketDefaults(endpoint: unknown): string {
  const raw = typeof endpoint === 'string' && endpoint.trim()
    ? endpoint.trim()
    : 'bugs?open_only=1&include_replay_state=1&limit=20';
  const [path, query = ''] = raw.split('?');
  const params = new URLSearchParams(query);
  if (path === 'bugs') {
    if (!params.has('open_only')) params.set('open_only', '1');
    if (!params.has('include_replay_state')) params.set('include_replay_state', '1');
    if (!params.has('limit')) params.set('limit', '20');
  }
  const rendered = params.toString();
  return rendered ? `${path}?${rendered}` : path;
}

function normalizeBugs(raw: unknown): { bugs: Bug[]; count: number | null } {
  if (Array.isArray(raw)) {
    return { bugs: raw as Bug[], count: raw.length };
  }
  const payload = raw as BugResponse | null;
  const bugs = Array.isArray(payload?.bugs) ? payload.bugs : [];
  return {
    bugs,
    count: typeof payload?.count === 'number' ? payload.count : null,
  };
}

function nextStepText(value: unknown): string | null {
  if (!value || typeof value !== 'object') return null;
  const context = value as Record<string, unknown>;
  const nextSteps = context.next_steps;
  if (Array.isArray(nextSteps) && nextSteps.length > 0) {
    const first = nextSteps.find((item) => typeof item === 'string' && item.trim());
    return typeof first === 'string' ? first.trim() : null;
  }
  const candidate = context.candidate_fix ?? context.hypothesis ?? context.why_it_matters;
  return typeof candidate === 'string' && candidate.trim() ? candidate.trim() : null;
}

function statusTone(status: string): string {
  const normalized = status.toUpperCase();
  if (normalized === 'OPEN') return 'var(--accent, #58a6ff)';
  if (normalized === 'IN_PROGRESS') return '#f0883e';
  return 'var(--text-muted, #8b949e)';
}

const severityColors: Record<string, string> = {
  P0: 'var(--danger, #f85149)',
  P1: '#f0883e',
  P2: '#d29922',
  P3: 'var(--text-muted, #8b949e)',
};

const severityBackgrounds: Record<string, string> = {
  P0: 'rgba(248, 81, 73, 0.15)',
  P1: 'rgba(240, 136, 62, 0.15)',
  P2: 'rgba(210, 153, 34, 0.12)',
  P3: 'rgba(139, 148, 158, 0.12)',
};

function BugCardModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as BugCardConfig;
  const endpoint = useMemo(() => endpointWithOpenTicketDefaults(cfg.endpoint), [cfg.endpoint]);
  const { data: raw, loading, error, refetch } = useModuleData<unknown>(endpoint, {
    refreshInterval: 60000,
  });
  const [filter, setFilter] = useState<TicketFilter>('all');
  const { bugs, count } = useMemo(() => normalizeBugs(raw), [raw]);
  const visibleBugs = useMemo(() => {
    if (filter === 'critical') return bugs.filter((bug) => bug.severity === 'P0' || bug.severity === 'P1');
    if (filter === 'replayable') return bugs.filter((bug) => bug.replay_ready);
    return bugs;
  }, [bugs, filter]);

  const handleSelect = (bug: Bug) => {
    if (cfg.publishSelection) publishSelection(cfg.publishSelection, bug as unknown as Record<string, unknown>);
    window.dispatchEvent(new CustomEvent('module-selection', {
      detail: { type: 'ticket', data: bug },
    }));
  };

  const title = typeof cfg.title === 'string' && cfg.title.trim() ? cfg.title.trim() : 'Open Tickets';

  return (
    <section style={{
      display: 'flex',
      flexDirection: 'column',
      gap: 'var(--space-sm, 8px)',
      padding: 'var(--space-md, 16px)',
      width: '100%',
      height: '100%',
      boxSizing: 'border-box',
      backgroundColor: 'var(--bg-card, #161b22)',
      borderRadius: 'var(--radius, 8px)',
      border: '1px solid var(--border, #30363d)',
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12 }}>
        <div>
          <div style={{ color: 'var(--text, #c9d1d9)', fontSize: 15, fontWeight: 700 }}>
            {title}
          </div>
          <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: 12, marginTop: 2 }}>
            {count === null ? `${bugs.length} loaded` : `${count} open, ${bugs.length} loaded`}
          </div>
        </div>
        <button
          type="button"
          onClick={refetch}
          title="Refresh tickets"
          aria-label="Refresh tickets"
          style={{
            flexShrink: 0,
            width: 32,
            height: 32,
            borderRadius: 6,
            border: '1px solid var(--border, #30363d)',
            background: 'rgba(255,255,255,0.04)',
            color: 'var(--text, #c9d1d9)',
            cursor: 'pointer',
          }}
        >
          R
        </button>
      </div>

      <div role="tablist" aria-label="Ticket filters" style={{ display: 'flex', gap: 6 }}>
        {[
          ['all', 'All'],
          ['critical', 'P0/P1'],
          ['replayable', 'Replay'],
        ].map(([value, label]) => (
          <button
            key={value}
            type="button"
            role="tab"
            aria-selected={filter === value}
            onClick={() => setFilter(value as TicketFilter)}
            style={{
              minHeight: 28,
              padding: '0 10px',
              borderRadius: 6,
              border: filter === value
                ? '1px solid rgba(88,166,255,0.55)'
                : '1px solid var(--border, #30363d)',
              background: filter === value ? 'rgba(88,166,255,0.13)' : 'rgba(255,255,255,0.03)',
              color: filter === value ? 'var(--text, #c9d1d9)' : 'var(--text-muted, #8b949e)',
              fontSize: 12,
              fontWeight: 700,
              cursor: 'pointer',
            }}
          >
            {label}
          </button>
        ))}
      </div>

      {loading && bugs.length === 0 && (
        <LoadingSkeleton lines={4} height={18} widths={['96%', '100%', '88%', '76%']} />
      )}

      {error && (
        <div style={{ color: 'var(--danger, #f85149)', fontSize: 13 }}>{error}</div>
      )}

      <div style={{ flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 8 }}>
        {!loading && visibleBugs.length === 0 && (
          <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: 13 }}>
            No matching open tickets
          </div>
        )}
        {visibleBugs.map((bug) => {
          const id = bugId(bug);
          const nextStep = nextStepText(bug.resume_context);
          return (
            <button
              key={id || bug.title}
              type="button"
              onClick={() => handleSelect(bug)}
              style={{
                display: 'grid',
                gridTemplateColumns: 'auto 1fr auto',
                gap: 8,
                alignItems: 'start',
                width: '100%',
                padding: '10px',
                textAlign: 'left',
                cursor: 'pointer',
                border: '1px solid var(--border, #30363d)',
                borderRadius: 8,
                background: 'rgba(255,255,255,0.025)',
                color: 'var(--text, #c9d1d9)',
              }}
            >
              <span style={{
                backgroundColor: severityBackgrounds[bug.severity] ?? severityBackgrounds.P2,
                color: severityColors[bug.severity] ?? severityColors.P2,
                padding: '2px 7px',
                borderRadius: 6,
                fontSize: 11,
                fontWeight: 800,
                whiteSpace: 'nowrap',
              }}>
                {bug.severity}
              </span>
              <span style={{ minWidth: 0 }}>
                <span style={{ display: 'block', fontSize: 13, fontWeight: 650, lineHeight: 1.3 }}>
                  {bug.title}
                </span>
                <span style={{
                  display: 'block',
                  color: 'var(--text-muted, #8b949e)',
                  fontSize: 11,
                  marginTop: 4,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}>
                  {id || 'untracked'}{bug.category ? ` - ${bug.category}` : ''}{nextStep ? ` - ${nextStep}` : ''}
                </span>
              </span>
              <span style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'flex-end',
                gap: 4,
                minWidth: 72,
              }}>
                <span style={{
                  color: statusTone(bug.status),
                  fontSize: 10,
                  fontWeight: 800,
                  textTransform: 'uppercase',
                  whiteSpace: 'nowrap',
                }}>
                  {bug.status}
                </span>
                <span style={{
                  color: bug.replay_ready ? 'var(--success, #3fb950)' : 'var(--text-muted, #8b949e)',
                  fontSize: 10,
                  fontWeight: 700,
                  whiteSpace: 'nowrap',
                }}>
                  {bug.replay_ready ? 'Replay ready' : 'No replay'}
                </span>
              </span>
            </button>
          );
        })}
      </div>
    </section>
  );
}

export default BugCardModule;
