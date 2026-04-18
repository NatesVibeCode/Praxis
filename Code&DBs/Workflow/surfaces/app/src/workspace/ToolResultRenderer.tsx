import React from 'react';

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

interface TableColumn {
  key: string;
  label: string;
}

/**
 * Rows are open records. When `id` is present it is used as the stable React
 * key and as the membership key in `selectedItems`. When absent, a composite
 * of column values is derived (see `deriveRowKey`). Callers that populate
 * `selectedItems` must use the same derivation — see `deriveRowKey` below.
 */
export interface TableRow {
  id?: string | number;
  [key: string]: unknown;
}

interface TableData {
  columns: TableColumn[];
  rows: TableRow[];
}

interface CardItem {
  /**
   * `id` is used as the stable React key when present; `name` is the
   * secondary fallback. If neither exists, a composite of all values is used
   * (see `deriveCardKey`). True duplicate objects append the index only as a
   * last-resort disambiguator.
   */
  id?: string | number;
  name?: string;
  [key: string]: unknown;
}

interface CardsData {
  items: CardItem[];
}

type JobStatus = 'succeeded' | 'failed' | 'running' | string;

interface StatusJob {
  /**
   * `id` is used as the stable React key when present.
   * Falls back to `label`, which is assumed to be unique within a single
   * run's job list. If labels repeat, React will warn but remain functional.
   */
  id?: string | number;
  label: string;
  status: JobStatus;
  duration?: string;
}

interface StatusData {
  status: JobStatus;
  spec_name?: string;
  run_id?: string;
  total_jobs?: number;
  completed_jobs?: number;
  jobs?: StatusJob[];
}

interface TextData {
  content: string;
}

interface ErrorData {
  message?: string;
}

// ---------------------------------------------------------------------------
// Discriminated union — one member per branch handled by ToolResultRenderer
// ---------------------------------------------------------------------------

export type ToolResultType =
  | { type: 'table';  data: TableData;  selectable?: boolean; summary?: string }
  | { type: 'cards';  data: CardsData;  selectable?: boolean; summary?: string }
  | { type: 'status'; data: StatusData; selectable?: boolean; summary?: string }
  | { type: 'text';   data: TextData;   selectable?: boolean; summary?: string }
  | { type: 'error';  data: ErrorData;  selectable?: boolean; summary?: string };

// ---------------------------------------------------------------------------
// Public prop shape — structurally identical to the original.
// `onSelectItems` narrows `any[]` → `TableRow[]` (the only concrete item type
// emitted by this component). Callers already handling `any[]` remain valid.
// ---------------------------------------------------------------------------

export interface ToolResultRendererProps {
  result: ToolResultType;
  onSelectItems?: (items: TableRow[]) => void;
  selectedItems?: Set<string>;
}

// ---------------------------------------------------------------------------
// Stable key helpers (exported so callers can build `selectedItems` correctly)
// ---------------------------------------------------------------------------

/**
 * Derive the string key used both as the React key and as the `selectedItems`
 * membership key for a table row.
 *
 * Key precedence:
 *  1. `row.id`  — guaranteed unique when the server provides it
 *  2. composite of all column values joined with NULL byte — deterministic,
 *     not globally unique when two rows are truly identical across all columns
 *
 * NOTE: callers who build `selectedItems` externally must apply the same
 * derivation. The previous implementation used `JSON.stringify(row)`, which
 * has been replaced here to satisfy the stable-key requirement.
 */
export function deriveRowKey(row: TableRow, columns: TableColumn[]): string {
  if (row.id != null) return String(row.id);
  return columns.map((c) => String(row[c.key] ?? '')).join('\x00');
}

// ---------------------------------------------------------------------------
// Status color / icon helpers
// ---------------------------------------------------------------------------

function resolveStatusColor(status: JobStatus): string {
  switch (status) {
    case 'succeeded': return 'var(--success)';
    case 'failed':    return 'var(--danger)';
    case 'running':   return 'var(--accent)';
    default:          return 'var(--text-muted)';
  }
}

function resolveStatusIcon(status: JobStatus): string {
  switch (status) {
    case 'succeeded': return 'ok';
    case 'failed':    return 'x';
    default:          return '-';
  }
}

// ---------------------------------------------------------------------------
// Branch components
// ---------------------------------------------------------------------------

function ToolResultError({ data }: { data: ErrorData }) {
  // Fallback: render generic message when `message` is absent or not a string
  const message =
    typeof data.message === 'string' && data.message.length > 0
      ? data.message
      : 'Tool error';

  return (
    <div className="ws-tool-error">
      <span className="ws-tool-error__icon">x</span>
      <span>{message}</span>
    </div>
  );
}

function ToolResultText({ data }: { data: TextData }) {
  // Empty state: content is empty string — render the wrapper with no text
  const content = typeof data.content === 'string' ? data.content : '';
  return <div className="ws-tool-text">{content}</div>;
}

function ToolResultStatus({ data }: { data: StatusData }) {
  // Malformed fallback: status must be a string
  if (typeof data.status !== 'string') {
    return (
      <div className="ws-tool-text" style={{ color: 'var(--text-muted)' }}>
        Malformed status result.
      </div>
    );
  }

  const color = resolveStatusColor(data.status);
  // Empty state: jobs absent or empty — omit the jobs section entirely
  const jobs = Array.isArray(data.jobs) ? data.jobs : [];

  return (
    <div className="ws-tool-status">
      <div className="ws-tool-status__header">
        <span className="ws-tool-status__dot" style={{ background: color }} />
        <span className="ws-tool-status__name">
          {data.spec_name ?? data.run_id ?? '—'}
        </span>
        <span className="ws-tool-status__badge" style={{ color }}>
          {data.status}
        </span>
      </div>

      {data.total_jobs != null && (
        <div className="ws-tool-status__progress">
          {data.completed_jobs ?? 0} / {data.total_jobs} jobs
        </div>
      )}

      {/* Empty state: jobs.length === 0 → section is not rendered */}
      {jobs.length > 0 && (
        <div className="ws-tool-status__jobs">
          {jobs.map((j) => {
            // Stable key: prefer `id`, fall back to `label`.
            // `label` is assumed unique within a run's job list.
            const key = j.id != null ? String(j.id) : j.label;
            return (
              <div key={key} className="ws-tool-status__job">
                <span style={{ color: resolveStatusColor(j.status) }}>
                  {resolveStatusIcon(j.status)}
                </span>
                <span>{j.label}</span>
                {j.duration != null && (
                  <span className="ws-tool-status__dur">{j.duration}</span>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

interface ToolResultTableProps {
  data: TableData;
  selectable?: boolean;
  onSelectItems?: (items: TableRow[]) => void;
  selectedItems?: Set<string>;
}

function ToolResultTable({
  data,
  selectable,
  onSelectItems,
  selectedItems,
}: ToolResultTableProps) {
  const { columns, rows } = data;

  // Malformed fallback: columns must be a non-empty array
  if (!Array.isArray(columns) || columns.length === 0) {
    return (
      <div className="ws-tool-text" style={{ color: 'var(--text-muted)' }}>
        Malformed table: missing column definitions.
      </div>
    );
  }

  // Empty state: 0 rows
  if (!Array.isArray(rows) || rows.length === 0) {
    return (
      <div className="ws-tool-text" style={{ color: 'var(--text-muted)' }}>
        No results.
      </div>
    );
  }

  const handleSelectAll = (checked: boolean) => {
    onSelectItems?.(checked ? rows : []);
  };

  return (
    <div className="ws-tool-table-wrap">
      <table className="ws-tool-table">
        <thead>
          <tr>
            {selectable && (
              <th className="ws-tool-table__check">
                <input
                  type="checkbox"
                  onChange={(e) => handleSelectAll(e.target.checked)}
                />
              </th>
            )}
            {columns.map((col) => (
              <th key={col.key}>{col.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const key = deriveRowKey(row, columns);
            const isSelected = selectedItems?.has(key) ?? false;
            return (
              <tr
                key={key}
                className={isSelected ? 'ws-tool-table__row--selected' : ''}
              >
                {selectable && (
                  <td className="ws-tool-table__check">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      onChange={() => onSelectItems?.([row])}
                    />
                  </td>
                )}
                {columns.map((col) => (
                  <td key={col.key}>{String(row[col.key] ?? '')}</td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
      <div className="ws-tool-table__footer">
        {rows.length} result{rows.length !== 1 ? 's' : ''}
      </div>
    </div>
  );
}

function deriveCardKey(item: CardItem, index: number): string {
  if (item.id != null) return String(item.id);
  if (typeof item.name === 'string' && item.name.length > 0) return item.name;
  // Fallback: composite of all own values.
  // The index suffix only disambiguates truly identical objects (rare in practice).
  return Object.values(item).map(String).join('\x00') + `\x00${index}`;
}

function ToolResultCards({ data }: { data: CardsData }) {
  // Malformed fallback: treat non-array items as empty
  const items = Array.isArray(data.items) ? data.items : [];

  // Empty state: 0 items
  if (items.length === 0) {
    return (
      <div className="ws-tool-text" style={{ color: 'var(--text-muted)' }}>
        No items.
      </div>
    );
  }

  return (
    <div className="ws-tool-cards">
      {items.map((item, index) => (
        <div key={deriveCardKey(item, index)} className="ws-tool-card">
          {Object.entries(item).map(([k, v]) => (
            <div key={k} className="ws-tool-card__field">
              <span className="ws-tool-card__key">{k}</span>
              <span className="ws-tool-card__value">{String(v)}</span>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Public export — prop shape unchanged from original
// ---------------------------------------------------------------------------

export function ToolResultRenderer({
  result,
  onSelectItems,
  selectedItems,
}: ToolResultRendererProps) {
  switch (result.type) {
    case 'error':
      return <ToolResultError data={result.data} />;

    case 'text':
      return <ToolResultText data={result.data} />;

    case 'status':
      return <ToolResultStatus data={result.data} />;

    case 'table':
      return (
        <ToolResultTable
          data={result.data}
          selectable={result.selectable}
          onSelectItems={onSelectItems}
          selectedItems={selectedItems}
        />
      );

    case 'cards':
      return <ToolResultCards data={result.data} />;

    default: {
      // Exhaustiveness guard: TypeScript will error here if a new `type` is
      // added to `ToolResultType` without a corresponding `case`.
      const _exhaustive: never = result;
      void _exhaustive;
      return (
        <div className="ws-tool-text" style={{ color: 'var(--text-muted)' }}>
          Unsupported result type.
        </div>
      );
    }
  }
}
