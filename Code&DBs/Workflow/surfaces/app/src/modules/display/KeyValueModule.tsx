import React from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { useWorldSelection } from '../../hooks/useWorldSelection';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { getPath } from '../../utils/format';

function capitalizeLabel(key: string): string {
  return key
    .split(/[_\s]+/)
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

interface KVItem { label: string; value: string | number }

function KeyValueModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as {
    items?: KVItem[];
    endpoint?: string;
    path?: string;
    objectType?: string;
    objectId?: string;
    subscribeSelection?: string;
    displayProperties?: string[];
  };

  const selectedObject = useWorldSelection<Record<string, unknown>>(cfg.subscribeSelection ?? '');

  const { data, loading } = useModuleData<unknown>(cfg.endpoint ?? '', {
    enabled: !!cfg.endpoint && !cfg.objectType,
  });

  const { data: objData, loading: objLoading } = useModuleData<{ properties?: Record<string, unknown> }>(
    `objects/${cfg.objectId}`,
    { enabled: !!cfg.objectType && !!cfg.objectId },
  );

  let items: KVItem[] = [];

  if (cfg.subscribeSelection) {
    if (selectedObject) {
      const props = (selectedObject as Record<string, unknown>).properties ?? selectedObject;
      const entries = props && typeof props === 'object' ? props as Record<string, unknown> : {};

      if (cfg.displayProperties) {
        items = cfg.displayProperties.map(k => ({
          label: capitalizeLabel(k),
          value: entries[k] == null ? '\u2014' : String(entries[k]),
        }));
      } else {
        items = Object.entries(entries).map(([k, v]) => ({
          label: capitalizeLabel(k),
          value: v == null ? '\u2014' : String(v),
        }));
      }
    }
  } else if (cfg.items) {
    items = cfg.items;
  } else if (cfg.objectType && cfg.objectId && objData) {
    const props = objData.properties ?? {};
    items = Object.entries(props).map(([k, v]) => ({
      label: k.charAt(0).toUpperCase() + k.slice(1),
      value: v == null ? '\u2014' : String(v),
    }));
  } else if (cfg.endpoint && data) {
    const resolved = cfg.path ? getPath(data, cfg.path) : data;
    if (resolved && typeof resolved === 'object' && !Array.isArray(resolved)) {
      items = Object.entries(resolved as Record<string, unknown>).map(([k, v]) => ({
        label: k,
        value: v == null ? '\u2014' : String(v),
      }));
    } else if (Array.isArray(resolved)) {
      items = resolved as KVItem[];
    }
  }

  if (loading || objLoading) {
    return (
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', padding: 'var(--space-lg)',
      }}>
        <LoadingSkeleton lines={5} height={14} widths={['64%', '100%', '86%', '94%', '72%']} />
      </div>
    );
  }

  if (cfg.subscribeSelection && !selectedObject) {
    return (
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', padding: 'var(--space-lg)',
        color: 'var(--text-muted)', textAlign: 'center',
      }}>
        Select an item to view details
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', padding: 'var(--space-lg)',
        color: 'var(--text-muted)', textAlign: 'center',
      }}>
        No data
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
      {items.map((item, i) => (
        <div key={i} style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: 'var(--space-sm) var(--space-lg)',
          borderBottom: i < items.length - 1 ? '1px solid var(--border)' : undefined,
          fontSize: 13,
        }}>
          <span style={{ color: 'var(--text-muted)' }}>{item.label}</span>
          <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 500 }}>{item.value}</span>
        </div>
      ))}
    </div>
  );
}

export default KeyValueModule;
