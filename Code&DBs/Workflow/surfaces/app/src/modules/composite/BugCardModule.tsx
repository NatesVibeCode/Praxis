import React, { useState } from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';

interface Bug {
  id?: string;
  bug_id?: string;
  title: string;
  severity: string;
  status: string;
  description?: string;
}

function bugId(b: Bug): string { return b.bug_id ?? b.id ?? ''; }

const severityColors: Record<string, string> = {
  P0: 'var(--danger, #f85149)',
  P1: '#d29922',
  P2: '#d29922',
};

const severityBg: Record<string, string> = {
  P0: 'rgba(248, 81, 73, 0.15)',
  P1: 'rgba(210, 153, 34, 0.15)',
  P2: 'rgba(210, 153, 34, 0.1)',
};

function BugCardModule({ config }: QuadrantProps) {
  const { data: raw, loading, error } = useModuleData<any>('bugs', { refreshInterval: 60000 });
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const data: Bug[] | null = raw ? (Array.isArray(raw) ? raw : (raw.bugs ?? [])) : null;

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', gap: 'var(--space-sm, 8px)',
      padding: 'var(--space-md, 16px)', width: '100%', height: '100%',
      boxSizing: 'border-box', backgroundColor: 'var(--bg-card, #161b22)',
      borderRadius: 'var(--radius, 8px)',
    }}>
      <div style={{ color: 'var(--text, #c9d1d9)', fontSize: '15px', fontWeight: 600 }}>
        Bugs
      </div>

      {loading && !data && (
        <LoadingSkeleton lines={4} height={18} widths={['96%', '100%', '88%', '76%']} />
      )}

      {error && (
        <div style={{ color: 'var(--danger, #f85149)', fontSize: '13px' }}>{error}</div>
      )}

      <div style={{ flex: 1, overflowY: 'auto' }}>
        {data && data.length === 0 && (
          <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: '13px' }}>No bugs</div>
        )}
        {data?.map(bug => (
          <div
            key={bugId(bug)}
            onClick={() => setExpandedId(expandedId === bugId(bug) ? null : bugId(bug))}
            style={{
              padding: '8px 10px', cursor: 'pointer',
              borderBottom: '1px solid var(--border, #30363d)',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{
                backgroundColor: severityBg[bug.severity] ?? severityBg.P2,
                color: severityColors[bug.severity] ?? severityColors.P2,
                padding: '1px 6px', borderRadius: '4px', fontSize: '11px',
                fontWeight: 700, whiteSpace: 'nowrap',
              }}>
                {bug.severity}
              </span>
              <span style={{
                color: 'var(--text, #c9d1d9)', fontSize: '13px', fontWeight: 500,
                flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              }}>
                {bug.title}
              </span>
              <span style={{
                color: 'var(--text-muted, #8b949e)', fontSize: '11px', whiteSpace: 'nowrap',
              }}>
                {bug.status}
              </span>
            </div>

            {expandedId === bugId(bug) && bug.description && (
              <div style={{
                color: 'var(--text-muted, #8b949e)', fontSize: '12px',
                marginTop: '6px', lineHeight: '1.4',
              }}>
                {bug.description}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default BugCardModule;
