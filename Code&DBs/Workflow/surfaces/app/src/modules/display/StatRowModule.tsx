import React from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { StatsRow } from '../../primitives/StatsRow';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { formatValue, getPath } from '../../utils/format';

function StatRowModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as {
    endpoint?: string;
    stats?: { path: string; label: string; format?: string; color?: string }[];
  };

  const { data, loading } = useModuleData<unknown>(cfg.endpoint ?? '', {
    enabled: !!cfg.endpoint,
  });

  if (loading) {
    const loadingStats = (cfg.stats ?? [{ label: 'Loading' }]).map((stat, index) => (
      <div key={`${stat.label}-${index}`} style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border)',
        borderRadius: 'var(--radius)',
        padding: 'var(--space-lg)',
      }}>
        <div style={{ color: 'var(--text-muted)', fontSize: 12, marginBottom: 4 }}>{stat.label}</div>
        <LoadingSkeleton lines={1} height={30} width="64%" />
      </div>
    ));

    return (
      <div style={{
        display: 'grid',
        gridTemplateColumns: `repeat(${Math.min((cfg.stats ?? [{ label: 'Loading' }]).length, 6)}, 1fr)`,
        gap: 'var(--space-md)',
      }}>
        {loadingStats}
      </div>
    );
  }

  const stats = (cfg.stats ?? []).map(s => ({
    label: s.label,
    value: formatValue(data && s.path ? getPath(data, s.path) : undefined, s.format),
    color: s.color,
  }));

  return <StatsRow stats={stats} />;
}

export default StatRowModule;
