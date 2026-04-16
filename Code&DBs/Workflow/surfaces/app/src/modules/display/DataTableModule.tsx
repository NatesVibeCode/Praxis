import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { QuadrantProps } from '../types';
import { useModuleData } from '../../hooks/useModuleData';
import { useSlice } from '../../hooks/useSlice';
import { DataTable } from '../../primitives/DataTable';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { publishSelection } from '../../hooks/useWorldSelection';
import { getPath } from '../../utils/format';
import { world } from '../../world';

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
}

function DataTableModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as DataTableConfig;
  const [propDefs, setPropDefs] = useState<PropertyDef[]>([]);
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);
  const searchQueryPath =
    cfg.searchQuery === undefined
      ? 'shared.search_query'
      : cfg.searchQuery
        ? `shared.${cfg.searchQuery}`
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

  // Fetch property definitions for objectType
  useEffect(() => {
    if (!cfg.objectType) return;
    fetch(`/api/object-types/${cfg.objectType}`)
      .then(r => r.json())
      .then(data => {
        const typePayload = (data as { type?: { property_definitions?: PropertyDef[] } } | null)?.type;
        const directPayload = (data as { property_definitions?: PropertyDef[] })?.property_definitions;
        setPropDefs(typePayload?.property_definitions ?? directPayload ?? []);
      })
      .catch(() => setPropDefs([]));
  }, [cfg.objectType]);

  // Determine fetch endpoint — prefer explicit endpoint over objectType-derived
  const fetchEndpoint = cfg.endpoint
    ? cfg.endpoint
    : cfg.objectType
      ? `objects?type=${cfg.objectType}`
      : '';

  const { data: rawData, loading, error } = useModuleData<unknown>(
    fetchEndpoint,
    { enabled: !!fetchEndpoint, refreshInterval: cfg.refreshInterval ?? 30000 },
  );

  // Build rows — handle objects, bugs, and generic array responses (must be before columns)
  const rows = useMemo(() => {
    if (!rawData) return [];
    const d = rawData as Record<string, unknown>;

    // Try common response shapes: {objects: [...]}, {bugs: [...]}, or direct array
    let items: unknown[] = [];
    if (cfg.path) {
      const resolved = getPath(d, cfg.path);
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
  }, [rawData, cfg.path]);

  const filteredRows = useMemo(() => {
    const query = typeof searchQueryRaw === 'string' ? searchQueryRaw.trim().toLowerCase() : '';
    if (!query) return rows;

    return rows.filter((row) =>
      Object.values(row).some((value) =>
        String(value ?? '').toLowerCase().includes(query)
      )
    );
  }, [rows, searchQueryRaw]);

  // Derive columns — explicit config → property defs → auto-detect from first row
  const columns = useMemo(() => {
    if (cfg.columns && cfg.columns.length > 0) return cfg.columns;
    if (cfg.objectType && propDefs.length > 0) {
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
  }, [cfg.columns, cfg.objectType, propDefs, filteredRows]);

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
      />
    </div>
  );
}

export default DataTableModule;
