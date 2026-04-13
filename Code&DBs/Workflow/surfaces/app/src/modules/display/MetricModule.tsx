import React from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { MetricCard } from '../../primitives/MetricCard';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { formatValue, getPath } from '../../utils/format';

function MetricModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as {
    endpoint?: string; path?: string; label?: string;
    format?: string; color?: string; value?: string | number;
  };

  const { data, loading } = useModuleData<unknown>(cfg.endpoint ?? '', {
    enabled: !!cfg.endpoint,
  });

  if (loading) {
    return (
      <div style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border)',
        borderRadius: 'var(--radius)',
        padding: 'var(--space-lg)',
        display: 'flex',
        flexDirection: 'column',
        gap: 'var(--space-sm)',
        minWidth: 140,
        flex: '1 1 0',
      }}>
        <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>{cfg.label ?? 'Metric'}</div>
        <LoadingSkeleton lines={1} height={32} width="58%" />
      </div>
    );
  }

  let raw: unknown = cfg.value;
  if (cfg.endpoint && data) {
    if (cfg.path) {
      raw = getPath(data, cfg.path);
    } else if (typeof data === 'object' && data !== null && 'count' in (data as Record<string, unknown>)) {
      raw = (data as Record<string, unknown>).count;
    }
  }
  const display = formatValue(raw, cfg.format);

  return <MetricCard label={cfg.label} value={display} />;
}

export default MetricModule;
