import { useMemo } from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { useWorldSelection } from '../../hooks/useWorldSelection';
import { ChartView } from '../../primitives/ChartView';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { getPath } from '../../utils/format';

function ChartModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as {
    endpoint?: string; path?: string;
    type?: 'bar' | 'line' | 'pie';
    xKey?: string; yKey?: string; title?: string;
    subscribeSelection?: string;
    selectionKey?: string;
  };

  const selectedObject = useWorldSelection<Record<string, unknown>>(cfg.subscribeSelection ?? '');

  const { data, loading } = useModuleData<unknown>(cfg.endpoint ?? '', {
    enabled: !!cfg.endpoint,
  });

  // Extract raw items from response
  const rawItems = useMemo(() => {
    if (!data) return [];
    if (cfg.path) {
      const resolved = getPath(data, cfg.path);
      return Array.isArray(resolved) ? resolved : [];
    }
    if (Array.isArray(data)) return data;
    const d = data as Record<string, unknown>;
    if (Array.isArray(d.objects)) return d.objects as Record<string, unknown>[];
    if (Array.isArray(d.bugs)) return d.bugs as Record<string, unknown>[];
    if (Array.isArray(d.results)) return d.results as Record<string, unknown>[];
    return [];
  }, [data, cfg.path]);

  // Client-side aggregation: group by a field and count
  const groupBy = (cfg as Record<string, unknown>).groupBy as string | undefined;
  const xKey = cfg.xKey ?? 'name';
  const yKey = cfg.yKey ?? 'value';

  const items = useMemo(() => {
    if (groupBy && rawItems.length > 0) {
      const counts: Record<string, number> = {};
      for (const item of rawItems) {
        const val = String(
          (item as Record<string, unknown>)[groupBy]
          ?? (item as any).properties?.[groupBy]
          ?? 'unknown'
        );
        counts[val] = (counts[val] ?? 0) + 1;
      }
      return Object.entries(counts).map(([name, value]) => ({ name, value }));
    }
    return rawItems;
  }, [rawItems, groupBy]);

  // Determine which item to highlight based on selection
  const highlightName = useMemo(() => {
    if (!cfg.subscribeSelection || !selectedObject) return null;
    // Use selectionKey config to pick which field from the selected object to match against xKey
    const key = cfg.selectionKey ?? 'name';
    return (selectedObject as Record<string, unknown>)[key] as string | null
      ?? (selectedObject as Record<string, unknown>).label as string | null
      ?? null;
  }, [cfg.subscribeSelection, cfg.selectionKey, selectedObject]);

  // Build data with highlight opacity for non-pie charts
  const styledData = useMemo(() => {
    if (!highlightName) return items;
    return items.map(item => ({
      ...item,
      __highlighted: String(item[xKey]) === highlightName,
    }));
  }, [items, highlightName, xKey]);

  // Build fill array for pie/bar charts
  const cellColors = useMemo(() => {
    if (!highlightName) return undefined;
    return styledData.map(item =>
      (item as Record<string, unknown>).__highlighted ? 1.0 : 0.25
    );
  }, [styledData, highlightName]);

  return (
    <div>
      {cfg.title && (
        <div style={{
          color: 'var(--text)', fontWeight: 600, fontSize: 14,
          marginBottom: 'var(--space-sm)',
        }}>
          {cfg.title}
        </div>
      )}
      {highlightName && (
        <div style={{
          fontSize: 12, color: 'var(--accent)', marginBottom: 'var(--space-sm)',
        }}>
          Highlighting: {highlightName}
        </div>
      )}
      {loading ? (
        <div style={{
          background: 'var(--bg-card)', border: '1px solid var(--border)',
          borderRadius: 'var(--radius)', padding: 'var(--space-lg)',
        }}>
          <LoadingSkeleton lines={4} height={18} widths={['92%', '100%', '84%', '68%']} />
        </div>
      ) : (
        <ChartView
          chartType={cfg.type ?? 'bar'}
          data={styledData}
          xKey={xKey}
          yKey={yKey}
          cellOpacities={cellColors}
        />
      )}
    </div>
  );
}

export default ChartModule;
