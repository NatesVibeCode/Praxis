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
    source?: { projection_ref?: string };
  };

  const projectionRef = cfg.source?.projection_ref;
  const spec = projectionRef
    ? { source: { projection_ref: projectionRef } }
    : (cfg.endpoint ?? '');
  const hasSource = Boolean(projectionRef) || Boolean(cfg.endpoint);
  const { data, loading } = useModuleData<unknown>(spec, {
    enabled: hasSource,
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
  let formatHint: string | undefined = cfg.format;
  if (projectionRef && data && typeof data === 'object' && data !== null) {
    const output = data as Record<string, unknown>;
    raw = output.value;
    if (!formatHint && typeof output.format === 'string') {
      formatHint = output.format;
    }
  } else if (cfg.endpoint && data) {
    if (cfg.path) {
      raw = getPath(data, cfg.path);
    } else if (typeof data === 'object' && data !== null && 'count' in (data as Record<string, unknown>)) {
      raw = (data as Record<string, unknown>).count;
    }
  }
  const display = formatValue(raw, formatHint);

  return <MetricCard label={cfg.label} value={display} />;
}

export default MetricModule;
