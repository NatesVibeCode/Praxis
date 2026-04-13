import React from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';

interface WorkflowStatusData {
  total: number;
  passed: number;
  failed: number;
  pending: number;
  last_activity?: string;
}

function WorkflowStatusModule({ config }: QuadrantProps) {
  const { data, loading, error, refetch } = useModuleData<WorkflowStatusData>('workflow-status', {
    refreshInterval: 30000,
  });

  const passRate = data && data.total > 0
    ? Math.round((data.passed / data.total) * 100)
    : 0;

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', gap: 'var(--space-sm, 8px)',
      padding: 'var(--space-md, 16px)', width: '100%', height: '100%',
      boxSizing: 'border-box', backgroundColor: 'var(--bg-card, #161b22)',
      borderRadius: 'var(--radius, 8px)',
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div style={{ color: 'var(--text, #c9d1d9)', fontSize: '15px', fontWeight: 600 }}>
          Workflow Status
        </div>
        <button
          onClick={refetch}
          style={{
            backgroundColor: 'var(--bg, #0d1117)', color: 'var(--accent, #58a6ff)',
            border: '1px solid var(--border, #30363d)', borderRadius: 'var(--radius, 8px)',
            padding: '4px 12px', fontSize: '12px', cursor: 'pointer',
          }}
        >
          Refresh
        </button>
      </div>

      {loading && !data && (
        <LoadingSkeleton lines={4} height={18} widths={['100%', '84%', '92%', '68%']} />
      )}

      {error && (
        <div style={{ color: 'var(--danger, #f85149)', fontSize: '13px' }}>{error}</div>
      )}

      {data && (
        <>
          {/* Pass rate bar */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px' }}>
              <span style={{ color: 'var(--text-muted, #8b949e)' }}>Pass rate</span>
              <span style={{ color: 'var(--text, #c9d1d9)', fontWeight: 600 }}>{passRate}%</span>
            </div>
            <div style={{
              width: '100%', height: '6px', backgroundColor: 'var(--bg, #0d1117)',
              borderRadius: '3px', overflow: 'hidden',
            }}>
              <div style={{
                width: `${passRate}%`, height: '100%',
                backgroundColor: passRate >= 80 ? 'var(--success, #3fb950)'
                  : passRate >= 50 ? 'var(--warning, #d29922)'
                  : 'var(--danger, #f85149)',
                borderRadius: '3px', transition: 'width 0.3s ease',
              }} />
            </div>
          </div>

          {/* Workflow counts */}
          <div style={{ display: 'flex', gap: 'var(--space-md, 16px)', flexWrap: 'wrap' }}>
            {[
              { label: 'Total', value: data.total, color: 'var(--text, #c9d1d9)' },
              { label: 'Passed', value: data.passed, color: 'var(--success, #3fb950)' },
              { label: 'Failed', value: data.failed, color: 'var(--danger, #f85149)' },
              { label: 'Pending', value: data.pending, color: 'var(--warning, #d29922)' },
            ].map(({ label, value, color }) => (
              <div key={label} style={{ textAlign: 'center', minWidth: '48px' }}>
                <div style={{ fontSize: '18px', fontWeight: 700, color }}>{value}</div>
                <div style={{ fontSize: '11px', color: 'var(--text-muted, #8b949e)' }}>{label}</div>
              </div>
            ))}
          </div>

          {/* Last activity */}
          {data.last_activity && (
            <div style={{ fontSize: '11px', color: 'var(--text-muted, #8b949e)', marginTop: 'auto' }}>
              Last activity: {new Date(data.last_activity).toLocaleString()}
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default WorkflowStatusModule;
