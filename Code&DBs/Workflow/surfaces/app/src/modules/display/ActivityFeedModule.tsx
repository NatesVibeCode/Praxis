import React, { useMemo } from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { useWorldSelection, publishSelection } from '../../hooks/useWorldSelection';
import { ActivityFeed } from '../../primitives/ActivityFeed';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { getPath } from '../../utils/format';

function ActivityFeedModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as {
    endpoint?: string; title?: string; refreshInterval?: number; path?: string;
    subscribeSelection?: string;
    selectionKey?: string;
  };

  const selectedObject = useWorldSelection<Record<string, unknown>>(cfg.subscribeSelection ?? '');

  const endpoint = cfg.endpoint ?? 'platform-overview';
  const path = cfg.path ?? (cfg.endpoint ? undefined : 'recent_workflows');

  const { data, loading } = useModuleData<unknown>(endpoint, {
    refreshInterval: cfg.refreshInterval,
  });

  const items = data && path
    ? getPath(data, path)
    : data;

  const allItems = loading ? [] : (Array.isArray(items) ? items : []);

  // Filter items when a selection is active
  const filteredItems = useMemo(() => {
    if (!cfg.subscribeSelection || !selectedObject) return allItems;
    const key = cfg.selectionKey ?? 'name';
    const selName = (selectedObject as Record<string, unknown>)[key] as string
      ?? (selectedObject as Record<string, unknown>).label as string
      ?? null;
    if (!selName) return allItems;
    const lower = selName.toLowerCase();
    return allItems.filter((item: Record<string, unknown>) => {
      // Match against label, agent, or any string field containing the selected name
      const label = String(item.label ?? '').toLowerCase();
      const agent = String(item.agent ?? '').toLowerCase();
      const entity = String(item.entity ?? '').toLowerCase();
      return label.includes(lower) || agent.includes(lower) || entity.includes(lower);
    });
  }, [allItems, cfg.subscribeSelection, cfg.selectionKey, selectedObject]);

  const selectionName = useMemo(() => {
    if (!cfg.subscribeSelection || !selectedObject) return null;
    const key = cfg.selectionKey ?? 'name';
    return (selectedObject as Record<string, unknown>)[key] as string
      ?? (selectedObject as Record<string, unknown>).label as string
      ?? null;
  }, [cfg.subscribeSelection, cfg.selectionKey, selectedObject]);

  const handleClear = () => {
    if (cfg.subscribeSelection) {
      publishSelection(cfg.subscribeSelection, null);
    }
  };

  const title = selectionName
    ? undefined  // We render our own header when filtered
    : cfg.title;

  if (loading) {
    const loadingTitle = cfg.title ?? 'Recent Activity';

    if (selectionName) {
      return (
        <div>
          <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: 'var(--space-sm) var(--space-lg)',
            background: 'var(--bg-card)', border: '1px solid var(--border)',
            borderRadius: 'var(--radius) var(--radius) 0 0',
            fontSize: 13,
          }}>
            <span>
              <span style={{ color: 'var(--text-muted)' }}>Showing activity for: </span>
              <span style={{ color: 'var(--accent)', fontWeight: 600 }}>{selectionName}</span>
            </span>
            <button
              onClick={handleClear}
              style={{
                background: 'none', border: '1px solid var(--border)',
                borderRadius: 'var(--radius)', color: 'var(--text-muted)',
                cursor: 'pointer', fontSize: 11, padding: '2px 8px',
              }}
            >
              Clear
            </button>
          </div>
          <div style={{
            background: 'var(--bg-card)',
            border: '1px solid var(--border)',
            borderTop: 'none',
            borderRadius: '0 0 var(--radius) var(--radius)',
            padding: 'var(--space-lg)',
          }}>
            <LoadingSkeleton lines={5} height={14} widths={['100%', '82%', '94%', '76%', '88%']} />
          </div>
        </div>
      );
    }

    return (
      <div style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border)',
        borderRadius: 'var(--radius)',
        overflow: 'hidden',
      }}>
        <div style={{
          padding: 'var(--space-md) var(--space-lg)',
          borderBottom: '1px solid var(--border)',
          fontWeight: 600,
          fontSize: 14,
        }}>
          {loadingTitle}
        </div>
        <div style={{ padding: 'var(--space-lg)' }}>
          <LoadingSkeleton lines={5} height={14} widths={['100%', '82%', '94%', '76%', '88%']} />
        </div>
      </div>
    );
  }

  if (selectionName) {
    return (
      <div>
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: 'var(--space-sm) var(--space-lg)',
          background: 'var(--bg-card)', border: '1px solid var(--border)',
          borderRadius: 'var(--radius) var(--radius) 0 0',
          fontSize: 13,
        }}>
          <span>
            <span style={{ color: 'var(--text-muted)' }}>Showing activity for: </span>
            <span style={{ color: 'var(--accent)', fontWeight: 600 }}>{selectionName}</span>
          </span>
          <button
            onClick={handleClear}
            style={{
              background: 'none', border: '1px solid var(--border)',
              borderRadius: 'var(--radius)', color: 'var(--text-muted)',
              cursor: 'pointer', fontSize: 11, padding: '2px 8px',
            }}
          >
            Clear
          </button>
        </div>
        <div style={{ borderTop: 'none' }}>
          <ActivityFeed
            title={cfg.title ?? 'Recent Activity'}
            data={filteredItems}
          />
        </div>
      </div>
    );
  }

  return (
    <ActivityFeed
      title={title}
      data={filteredItems}
    />
  );
}

export default ActivityFeedModule;
