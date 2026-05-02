import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ManifestTree, PrxTable, SectionStrip, TableFilterInput, TokenChip } from '../primitives';
import { statusCapProps } from '../primitives-prx';
import type { AtlasElement, AtlasPayload } from './AtlasPage';

// Ledger lens for the Atlas graph. Same payload as the cytoscape canvas
// in AtlasPage.tsx — different lens. Closes BUG-37D18B71 (the missing
// graph-table toggle and data-graph hierarchy-aware node metadata).
//
// React owns sort/filter/selection state directly; primitives supply the
// reusable visual pieces while this lens keeps the table interaction contract.

interface AtlasTableViewProps {
  payload: AtlasPayload | null;
  onSelect: (id: string) => void;
  selectedId: string | null;
  filter: string;
  onFilterChange: (value: string) => void;
  areaFilter: string | null;
  onAreaFilterChange: (slug: string | null) => void;
}

type SortKey = 'label' | 'area' | 'node_kind' | 'semantic_role' | 'state' | 'activity' | 'updated';
type SortDir = 1 | -1;

interface RowState {
  id: string;
  label: string;
  area: string;
  nodeKind: string;
  semanticRole: string;
  state: 'ok' | 'warn' | 'err' | 'dim';
  stateLabel: string;
  activity: number;
  updatedAt: string;
  relationSource: string;
}

function deriveStateTone(node: AtlasElement): { tone: 'ok' | 'warn' | 'err' | 'dim'; label: string } {
  const data = node.data;
  // Order matters: error > active > authority-verified > inert.
  if ((data.signal_risk ?? 0) > 0) return { tone: 'err', label: 'risk' };
  if ((data.signal_activity ?? 0) >= 0.6) return { tone: 'warn', label: 'live' };
  if ((data.signal_authority ?? 0) >= 0.5) return { tone: 'ok', label: 'authority' };
  if ((data.signal_stale ?? 0) > 0) return { tone: 'dim', label: 'stale' };
  return { tone: 'dim', label: '—' };
}

function shortISO(value: string | null | undefined): string {
  if (!value) return '—';
  // best-effort short rendering; payload values are ISO strings
  const dot = value.indexOf('.');
  return dot > 0 ? value.slice(0, dot).replace('T', ' ') : value.replace('T', ' ');
}

function buildRows(payload: AtlasPayload | null): RowState[] {
  if (!payload?.nodes) return [];
  return payload.nodes
    .filter((n) => n.data.node_kind !== 'class_label')
    .map((node) => {
      const { tone, label: stateLabel } = deriveStateTone(node);
      return {
        id: node.data.id,
        label: node.data.label || node.data.id,
        area: node.data.area || '—',
        nodeKind: node.data.node_kind || '—',
        semanticRole: node.data.semantic_role || '—',
        state: tone,
        stateLabel,
        activity: node.data.activity_score ?? 0,
        updatedAt: shortISO(node.data.updated_at),
        relationSource: (node.data.relation_source || '').replace(/^evidence:/, ''),
      };
    });
}

function sortRows(rows: RowState[], key: SortKey | null, dir: SortDir): RowState[] {
  if (!key) return rows;
  const accessor: Record<SortKey, (r: RowState) => string | number> = {
    label: (r) => r.label.toLowerCase(),
    area: (r) => r.area,
    node_kind: (r) => r.nodeKind,
    semantic_role: (r) => r.semanticRole,
    state: (r) => r.stateLabel,
    activity: (r) => r.activity,
    updated: (r) => r.updatedAt,
  };
  const get = accessor[key];
  return [...rows].sort((a, b) => {
    const av = get(a);
    const bv = get(b);
    if (av < bv) return -dir;
    if (av > bv) return dir;
    return 0;
  });
}

export function AtlasTableView({
  payload,
  onSelect,
  selectedId,
  filter,
  onFilterChange,
  areaFilter,
  onAreaFilterChange,
}: AtlasTableViewProps) {
  const [sortKey, setSortKey] = useState<SortKey | null>('activity');
  const [sortDir, setSortDir] = useState<SortDir>(-1);
  const tableRef = useRef<HTMLDivElement | null>(null);

  const allRows = useMemo(() => buildRows(payload), [payload]);

  // Area hierarchy for the sidebar tree.
  const areaCounts = useMemo(() => {
    const map = new Map<string, number>();
    allRows.forEach((r) => map.set(r.area, (map.get(r.area) || 0) + 1));
    return Array.from(map.entries()).sort((a, b) => b[1] - a[1]);
  }, [allRows]);

  // Apply filters then sort.
  const visibleRows = useMemo(() => {
    let view = allRows;
    if (areaFilter) view = view.filter((r) => r.area === areaFilter);
    if (filter) {
      const f = filter.toLowerCase();
      view = view.filter((r) =>
        (r.label + ' ' + r.area + ' ' + r.nodeKind + ' ' + r.semanticRole + ' ' + r.id)
          .toLowerCase()
          .includes(f),
      );
    }
    return sortRows(view, sortKey, sortDir);
  }, [allRows, filter, areaFilter, sortKey, sortDir]);

  const handleSort = useCallback(
    (key: SortKey) => {
      if (sortKey === key) {
        setSortDir((d) => (d === 1 ? -1 : 1));
      } else {
        setSortKey(key);
        setSortDir(1);
      }
    },
    [sortKey],
  );

  // Keyboard nav: j/k = down/up, Enter = open drawer (caller decides),
  // Escape = clear selection.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      const idx = visibleRows.findIndex((r) => r.id === selectedId);
      if (e.key === 'j' || e.key === 'ArrowDown') {
        const next = visibleRows[Math.min(idx + 1, visibleRows.length - 1)] ?? visibleRows[0];
        if (next) {
          onSelect(next.id);
          e.preventDefault();
        }
      } else if (e.key === 'k' || e.key === 'ArrowUp') {
        const prev = visibleRows[Math.max(idx - 1, 0)] ?? visibleRows[0];
        if (prev) {
          onSelect(prev.id);
          e.preventDefault();
        }
      } else if (e.key === 'Escape' && selectedId) {
        onSelect('');
        e.preventDefault();
      }
    }
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [visibleRows, selectedId, onSelect]);

  // Scroll selected row into view when selection changes via keyboard.
  useEffect(() => {
    if (!selectedId || !tableRef.current) return;
    const tr = tableRef.current.querySelector<HTMLElement>(`tr[data-id="${CSS.escape(selectedId)}"]`);
    tr?.scrollIntoView({ block: 'nearest' });
  }, [selectedId]);

  const sortDirection = (key: SortKey): 'asc' | 'desc' | null => {
    if (sortKey !== key) return null;
    return sortDir > 0 ? 'asc' : 'desc';
  };

  return (
    <div className="atlas-table-shell">
      <aside className="atlas-table-sidebar">
        <SectionStrip
          number={4}
          label="manifest tree"
          style={{ borderTop: 'none', paddingTop: 0, marginBottom: 12 }}
        />
        <ManifestTree
          role="navigation"
          ariaLabel="Filter by area"
          rows={[
            {
              id: 'all',
              glyph: '└─',
              label: 'all areas',
              meta: allRows.length,
              tone: areaFilter === null ? 'default' : 'muted',
              onClick: () => onAreaFilterChange(null),
              style: { cursor: 'pointer' },
            },
            ...areaCounts.map(([area, count]) => ({
              id: area,
              glyph: '├─',
              label: area,
              meta: count,
              tone: areaFilter === area ? 'default' as const : 'muted' as const,
              onClick: () => onAreaFilterChange(areaFilter === area ? null : area),
              style: { cursor: 'pointer' },
            })),
          ]}
        />
      </aside>

      <main className="atlas-table-main">
        <PrxTable<RowState>
          bodyRef={tableRef}
          rows={visibleRows}
          rowKey={(row) => row.id}
          selectedRowKey={selectedId}
          onRowClick={(row) => onSelect(row.id)}
          getRowProps={(row) => ({ 'data-id': row.id })}
          emptyState="no nodes match · clear filters"
          toolbar={
            <TableFilterInput
              placeholder="filter…"
              value={filter}
              onChange={(e) => onFilterChange(e.target.value)}
            />
          }
          filters={
            <>
              {areaFilter && (
                <TokenChip
                  tone="write"
                  onClick={() => onAreaFilterChange(null)}
                  style={{ cursor: 'pointer' }}
                  title="clear area filter"
                >
                  area={areaFilter}
                </TokenChip>
              )}
              {filter && (
                <TokenChip
                  tone="read"
                  onClick={() => onFilterChange('')}
                  style={{ cursor: 'pointer' }}
                >
                  q={filter}
                </TokenChip>
              )}
            </>
          }
          meta={`${visibleRows.length} of ${allRows.length}`}
          columns={[
            {
              key: 'label',
              label: 'name',
              sortDirection: sortDirection('label'),
              onSort: () => handleSort('label'),
            },
            {
              key: 'area',
              label: 'area',
              sortDirection: sortDirection('area'),
              onSort: () => handleSort('area'),
              render: (row) => <TokenChip>{row.area}</TokenChip>,
            },
            {
              key: 'nodeKind',
              label: 'kind',
              sortDirection: sortDirection('node_kind'),
              onSort: () => handleSort('node_kind'),
              render: (row) => <TokenChip>{row.nodeKind}</TokenChip>,
            },
            {
              key: 'semanticRole',
              label: 'role',
              sortDirection: sortDirection('semantic_role'),
              onSort: () => handleSort('semantic_role'),
              cellStyle: { color: 'var(--text-muted)' },
            },
            {
              key: 'stateLabel',
              label: 'state',
              sortDirection: sortDirection('state'),
              onSort: () => handleSort('state'),
              render: (row) => <span {...statusCapProps(row.state)}>{row.stateLabel}</span>,
            },
            {
              key: 'activity',
              label: 'activity',
              sortDirection: sortDirection('activity'),
              onSort: () => handleSort('activity'),
              render: (row) => `${(row.activity * 100).toFixed(0)}%`,
              cellStyle: { color: 'var(--text-muted)' },
            },
            {
              key: 'updatedAt',
              label: 'updated',
              sortDirection: sortDirection('updated'),
              onSort: () => handleSort('updated'),
              cellStyle: { color: 'var(--text-muted)' },
            },
          ]}
        />
      </main>
    </div>
  );
}
