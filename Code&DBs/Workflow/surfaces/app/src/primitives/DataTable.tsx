import { useCallback, useMemo, useState } from 'react';
import { PrxTable, type TableColumn } from './StructuralPrimitives';

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

export function DataTable({ columns, data, onRowClick, selectedIndex, emptyState }: DataTableProps) {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');

  const sorted = useMemo(() => {
    if (!sortKey) return data;
    return [...data].sort((a, b) => {
      const av = String(a[sortKey] ?? '');
      const bv = String(b[sortKey] ?? '');
      return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av);
    });
  }, [data, sortKey, sortDir]);

  const handleSort = useCallback((key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  }, [sortKey]);

  const typedColumns: TableColumn<Record<string, unknown>>[] = useMemo(
    () => columns.map((col) => ({
      key: col.key,
      label: col.label ?? col.key,
      sortable: col.sortable,
      sortDirection: sortKey === col.key ? sortDir : undefined,
      onSort: () => handleSort(col.key),
    })),
    [columns, sortKey, sortDir, handleSort],
  );

  const selectedKey = selectedIndex !== null && selectedIndex !== undefined
    ? selectedIndex
    : null;

  return (
    <PrxTable
      columns={typedColumns}
      rows={sorted}
      rowKey={(_row, index) => index}
      selectedRowKey={selectedKey}
      onRowClick={onRowClick}
      emptyState={emptyState ?? 'No data'}
    />
  );
}
