import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { useSlice } from '../../hooks/useSlice';
import { DataTable } from '../../primitives/DataTable';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { publishSelection } from '../../hooks/useWorldSelection';
import { getPath } from '../../utils/format';
import { world } from '../../world';
import { sourceBindingFor, sourceSelectionPath, type SourceBoundConfig } from '../sourceBindings';

interface PropertyDef {
  name: string;
  property_type?: string;
  type?: string;
  [key: string]: unknown;
}

interface ObjectRecord {
  object_id: string;
  status?: string;
  properties: Record<string, unknown>;
  [key: string]: unknown;
}

interface ColumnConfig {
  key: string;
  label?: string;
  sortable?: boolean;
}

interface DataTableConfig {
  objectType?: string;
  endpoint?: string;
  path?: string;
  columns?: ColumnConfig[];
  refreshInterval?: number;
  publishSelection?: string;
  searchQuery?: string;
  sourceSelection?: string;
  sourceBindings?: Record<string, SourceBoundConfig>;
  disabledMessage?: string;
  emptyMessage?: string;
  emptyDetail?: string;
}

function activeSourceLabel(value: unknown): string | null {
  if (!value || typeof value !== 'object') return null;
  const record = value as Record<string, unknown>;
  const label = typeof record.label === 'string' ? record.label.trim() : '';
  if (label) return label;
  const id = typeof record.id === 'string' ? record.id.trim() : '';
  return id || null;
}

function DataTableModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as DataTableConfig;
  const [propDefs, setPropDefs] = useState<PropertyDef[]>([]);
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);
  const activeSource = useSlice(world, sourceSelectionPath(cfg.sourceSelection));
  const sourceBinding = sourceBindingFor(cfg.sourceBindings, activeSource);
  const effectiveCfg = useMemo(
    () => ({ ...cfg, ...(sourceBinding ?? {}) }) as DataTableConfig,
    [cfg, sourceBinding],
  );
  const disabledMessage = typeof effectiveCfg.disabledMessage === 'string'
    ? effectiveCfg.disabledMessage.trim()
    : '';
  const emptyMessage = typeof effectiveCfg.emptyMessage === 'string' && effectiveCfg.emptyMessage.trim()
    ? effectiveCfg.emptyMessage.trim()
    : 'No records returned';
  const emptyDetail = typeof effectiveCfg.emptyDetail === 'string'
    ? effectiveCfg.emptyDetail.trim()
    : '';
  const searchQueryPath =
    effectiveCfg.searchQuery === undefined
      ? 'shared.search_query'
      : effectiveCfg.searchQuery
        ? `shared.${effectiveCfg.searchQuery}`
        : null;
  const searchQueryRaw = useSlice(world, searchQueryPath);

  const handleRowClick = useCallback((row: Record<string, unknown>, index: number) => {
    setSelectedIndex(index);
    // Publish to world state for inter-module communication
    if (cfg.publishSelection) {
      publishSelection(cfg.publishSelection, row);
    }
    // Fire detail panel event
    window.dispatchEvent(new CustomEvent('module-selection', {
      detail: { type: cfg.objectType ?? 'record', data: row },
    }));
  }, [cfg.publishSelection, cfg.objectType]);

  // Fetch field definitions for objectType
  useEffect(() => {
    if (!effectiveCfg.objectType) {
      setPropDefs([]);
      return;
    }
    fetch(`/api/object-types/${effectiveCfg.objectType}`)
      .then(r => r.json())
      .then(data => {
        const typePayload = (data as { type?: { fields?: PropertyDef[] } } | null)?.type;
        const directPayload = (data as { fields?: PropertyDef[] })?.fields;
        setPropDefs(typePayload?.fields ?? directPayload ?? []);
      })
      .catch(() => setPropDefs([]));
  }, [effectiveCfg.objectType]);

  // Determine fetch endpoint — prefer explicit endpoint over objectType-derived
  const fetchEndpoint = disabledMessage
    ? ''
    : effectiveCfg.endpoint
    ? effectiveCfg.endpoint
    : effectiveCfg.objectType
      ? `objects?type=${effectiveCfg.objectType}`
      : '';

  const { data: rawData, loading, error } = useModuleData<unknown>(
    fetchEndpoint,
    { enabled: !!fetchEndpoint, refreshInterval: effectiveCfg.refreshInterval ?? 30000 },
  );

  useEffect(() => {
    setSelectedIndex(null);
    if (cfg.publishSelection) {
      publishSelection(cfg.publishSelection, null);
    }
  }, [fetchEndpoint, cfg.publishSelection]);

  // Build rows — handle objects, bugs, and generic array responses (must be before columns)
  const rows = useMemo(() => {
    if (!rawData) return [];
    const d = rawData as Record<string, unknown>;

    // Try common response shapes: {objects: [...]}, {bugs: [...]}, or direct array
    let items: unknown[] = [];
    if (effectiveCfg.path) {
      const resolved = getPath(d, effectiveCfg.path);
      items = Array.isArray(resolved) ? resolved : [];
    } else if (Array.isArray(d)) {
      items = d;
    } else if (Array.isArray(d.objects)) {
      items = d.objects;
    } else if (Array.isArray(d.bugs)) {
      items = d.bugs;
    } else if (Array.isArray(d.results)) {
      items = d.results;
    } else if (Array.isArray(d.items)) {
      items = d.items;
    }

    return items.map((item: any) => {
      if (item.properties && typeof item.properties === 'object') {
        // Object record — flatten properties
        return { ...item.properties, object_id: item.object_id, status: item.status ?? '' };
      }
      // Bug or generic record — use as-is
      return item;
    });
  }, [rawData, effectiveCfg.path]);

  const filteredRows = useMemo(() => {
    const query = typeof searchQueryRaw === 'string' ? searchQueryRaw.trim().toLowerCase() : '';
    if (!query) return rows;

    return rows.filter((row) =>
      Object.values(row).some((value) =>
        String(value ?? '').toLowerCase().includes(query)
      )
    );
  }, [rows, searchQueryRaw]);

  const emptyState = useMemo(() => {
    const sourceLabel = activeSourceLabel(activeSource);
    const query = typeof searchQueryRaw === 'string' ? searchQueryRaw.trim() : '';
    const facts = [
      sourceLabel ? `Source: ${sourceLabel}` : null,
      effectiveCfg.objectType ? `Object type: ${effectiveCfg.objectType}` : null,
      query ? `Filter: "${query}"` : null,
      'Rows: 0',
    ].filter((fact): fact is string => Boolean(fact));

    return (
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: 8,
        minHeight: 120,
        color: 'var(--text-muted, #8b949e)',
      }}>
        <div style={{ color: 'var(--text, #e6edf3)', fontSize: 14, fontWeight: 650 }}>
          {emptyMessage}
        </div>
        {emptyDetail && (
          <div style={{ maxWidth: 520, fontSize: 12, lineHeight: 1.5 }}>
            {emptyDetail}
          </div>
        )}
        <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 6 }}>
          {facts.map((fact) => (
            <span
              key={fact}
              style={{
                border: '1px solid var(--border, #30363d)',
                borderRadius: 999,
                color: 'var(--text-muted, #8b949e)',
                fontSize: 11,
                fontWeight: 650,
                padding: '3px 8px',
                whiteSpace: 'nowrap',
              }}
            >
              {fact}
            </span>
          ))}
        </div>
      </div>
    );
  }, [activeSource, effectiveCfg.objectType, emptyDetail, emptyMessage, searchQueryRaw]);

  // Derive columns — explicit config → property defs → auto-detect from first row
  const columns = useMemo(() => {
    if (effectiveCfg.columns && effectiveCfg.columns.length > 0) return effectiveCfg.columns;
    if (effectiveCfg.objectType && propDefs.length > 0) {
      return [
        { key: 'object_id', label: 'ID' },
        ...propDefs.map(p => ({ key: p.name, label: p.name.charAt(0).toUpperCase() + p.name.slice(1) })),
        { key: 'status', label: 'Status' },
      ];
    }
    if (filteredRows.length > 0) {
      const first = filteredRows[0] as Record<string, unknown>;
      const skip = new Set(['search_vector', '__highlighted']);
      return Object.keys(first)
        .filter(k => !skip.has(k) && !k.startsWith('_'))
        .slice(0, 8)
        .map(k => ({
          key: k,
          label: k.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        }));
    }
    return [];
  }, [effectiveCfg.columns, effectiveCfg.objectType, propDefs, filteredRows]);

  if (loading) {
    return (
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', padding: 'var(--space-lg)',
        width: '100%', height: '100%', boxSizing: 'border-box',
      }}>
        <LoadingSkeleton lines={6} height={18} widths={['100%', '96%', '88%', '92%', '80%', '72%']} />
      </div>
    );
  }

  if (disabledMessage) {
    return (
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', padding: 'var(--space-lg)',
        color: 'var(--text-muted)', width: '100%', height: '100%', boxSizing: 'border-box',
        display: 'flex', alignItems: 'center', justifyContent: 'center', textAlign: 'center',
      }}>
        {disabledMessage}
      </div>
    );
  }

  if (error) {
    return (
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', padding: 'var(--space-lg)',
        color: 'var(--danger, #f85149)', width: '100%', height: '100%', boxSizing: 'border-box',
      }}>
        Error: {error}
      </div>
    );
  }

  return (
    <div style={{ width: '100%', height: '100%', overflow: 'auto', boxSizing: 'border-box' }}>
      <DataTable
        columns={columns}
        data={filteredRows as Record<string, unknown>[]}
        onRowClick={handleRowClick}
        selectedIndex={selectedIndex}
        emptyState={emptyState}
      />
    </div>
  );
}

export default DataTableModule;
