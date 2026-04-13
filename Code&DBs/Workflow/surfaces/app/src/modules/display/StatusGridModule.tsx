import React from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { StatusGrid } from '../../primitives/StatusGrid';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { getPath } from '../../utils/format';

function StatusGridModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as {
    endpoint?: string; title?: string; columns?: number; path?: string;
  };

  const endpoint = cfg.endpoint ?? 'platform-overview';
  const path = cfg.path ?? (cfg.endpoint ? undefined : 'active_models');

  const { data, loading } = useModuleData<unknown>(endpoint, {});

  const items = data && path
    ? getPath(data, path)
    : data;

  if (loading) {
    return (
      <div style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border)',
        borderRadius: 'var(--radius)',
        overflow: 'hidden',
      }}>
        {cfg.title && (
          <div style={{
            padding: 'var(--space-md) var(--space-lg)',
            borderBottom: '1px solid var(--border)',
            fontWeight: 600,
            fontSize: 14,
          }}>
            {cfg.title}
          </div>
        )}
        <div style={{ padding: 'var(--space-lg)' }}>
          <LoadingSkeleton lines={4} height={24} widths={['100%', '92%', '96%', '82%']} />
        </div>
      </div>
    );
  }

  return (
    <StatusGrid
      title={cfg.title}
      data={Array.isArray(items) ? items : []}
      columns={cfg.columns}
    />
  );
}

export default StatusGridModule;
