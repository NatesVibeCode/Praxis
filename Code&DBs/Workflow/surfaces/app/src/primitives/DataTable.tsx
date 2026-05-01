import React, { useState } from 'react';

interface ColumnConfig {
  key: string;
  label?: string;
  sortable?: boolean;
}

interface DataTableProps {
  columns: ColumnConfig[];
  data: Record<string, unknown>[];
  onRowClick?: (row: Record<string, unknown>, index: number) => void;
  selectedIndex?: number | null;
  emptyState?: React.ReactNode;
}

/**
 * DataTable — renders the prx-table CSS structure.
 * Public API unchanged from the inline-styles version; only the rendered
 * markup changes. Sort, click, selection behavior preserved.
 */
export function DataTable({ columns, data, onRowClick, selectedIndex, emptyState }: DataTableProps) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  };

  const sorted = sortKey
    ? [...data].sort((a, b) => {
        const av = String(a[sortKey] ?? '');
        const bv = String(b[sortKey] ?? '');
        return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av);
      })
    : data;

  if (columns.length === 0) {
    return (
      <div className="prx-table" data-testid="prx-data-table-empty">
        <div style={{ padding: '16px', color: 'var(--text-muted, #8b949e)', fontSize: 13 }}>
          No columns configured
        </div>
      </div>
    );
  }

  return (
    <div className="prx-table" data-testid="prx-data-table">
      <div className="body">
        <table style={{ width: '100%', tableLayout: 'fixed' }}>
          <thead>
            <tr>
              {columns.map((col) => {
                const isSorted = sortKey === col.key;
                const cls = isSorted ? `sort-${sortDir}` : '';
                return (
                  <th
                    key={col.key}
                    className={cls}
                    data-key={col.key}
                    onClick={col.sortable !== false ? () => handleSort(col.key) : undefined}
                    style={{ cursor: col.sortable !== false ? 'pointer' : 'default' }}
                  >
                    {col.label ?? col.key}
                    <span className="arrow" />
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {sorted.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length}
                  className="empty"
                  style={{ textAlign: 'center', padding: '16px 12px' }}
                >
                  {emptyState ?? 'No data'}
                </td>
              </tr>
            ) : (
              sorted.map((row, i) => (
                <tr
                  key={i}
                  className={selectedIndex === i ? 'selected' : ''}
                  data-row-index={i}
                  onClick={() => onRowClick?.(row, i)}
                  style={{ cursor: onRowClick ? 'pointer' : 'default' }}
                >
                  {columns.map((col) => (
                    <td
                      key={col.key}
                      style={{
                        padding: '9px 14px',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {String(row[col.key] ?? '')}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
