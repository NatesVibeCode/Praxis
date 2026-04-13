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
}

export function DataTable({ columns, data, onRowClick, selectedIndex }: DataTableProps) {
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
      <div style={{ padding: 'var(--space-lg, 16px)', color: 'var(--text-muted, #8b949e)', fontSize: 13 }}>
        No columns configured
      </div>
    );
  }

  return (
    <table style={{
      width: '100%', borderCollapse: 'collapse', fontSize: 13,
      color: 'var(--text, #e6edf3)',
    }}>
      <thead>
        <tr>
          {columns.map((col) => (
            <th
              key={col.key}
              onClick={col.sortable !== false ? () => handleSort(col.key) : undefined}
              style={{
                padding: '8px 12px',
                textAlign: 'left',
                fontWeight: 600,
                fontSize: 12,
                color: 'var(--text-muted, #8b949e)',
                borderBottom: '1px solid var(--border, #30363d)',
                cursor: col.sortable !== false ? 'pointer' : 'default',
                userSelect: 'none',
                whiteSpace: 'nowrap',
              }}
            >
              {col.label ?? col.key}
              {sortKey === col.key && (
                <span style={{ marginLeft: 4 }}>{sortDir === 'asc' ? '↑' : '↓'}</span>
              )}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {sorted.length === 0 ? (
          <tr>
            <td
              colSpan={columns.length}
              style={{ padding: '16px 12px', color: 'var(--text-muted, #8b949e)', textAlign: 'center' }}
            >
              No data
            </td>
          </tr>
        ) : (
          sorted.map((row, i) => (
            <tr
              key={i}
              onClick={() => onRowClick?.(row, i)}
              style={{
                background: selectedIndex === i
                  ? 'var(--bg-selected, rgba(88,166,255,0.1))'
                  : 'transparent',
                cursor: onRowClick ? 'pointer' : 'default',
                borderBottom: '1px solid var(--border, #21262d)',
              }}
              onMouseEnter={(e) => {
                if (selectedIndex !== i) {
                  (e.currentTarget as HTMLTableRowElement).style.background = 'var(--bg-hover, rgba(255,255,255,0.04))';
                }
              }}
              onMouseLeave={(e) => {
                if (selectedIndex !== i) {
                  (e.currentTarget as HTMLTableRowElement).style.background = 'transparent';
                }
              }}
            >
              {columns.map((col) => (
                <td
                  key={col.key}
                  style={{
                    padding: '8px 12px',
                    maxWidth: 240,
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
  );
}
