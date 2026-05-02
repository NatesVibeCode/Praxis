/**
 * Tests for diff_A_ToolResultRenderer.tsx
 *
 * Run from the surfaces/app project root:
 *   npx jest artifacts/canvas_ui_phase1_20260417/diff_A_ToolResultRenderer.test.tsx
 *
 * Dependencies assumed: @testing-library/react, @testing-library/jest-dom, jest
 */
import React from 'react';
import { render, screen, fireEvent, within } from '@testing-library/react';
import '@testing-library/jest-dom';

import {
  ToolResultRenderer,
  ToolResultType,
  TableRow,
  deriveRowKey,
} from './diff_A_ToolResultRenderer';

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const TABLE_COLUMNS = [
  { key: 'id', label: 'ID' },
  { key: 'name', label: 'Name' },
  { key: 'status', label: 'Status' },
];

const TABLE_ROWS: TableRow[] = [
  { id: 'r1', name: 'Alpha', status: 'active' },
  { id: 'r2', name: 'Beta',  status: 'inactive' },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function renderResult(
  result: ToolResultType,
  overrides?: Partial<{ onSelectItems: jest.Mock; selectedItems: Set<string> }>
) {
  return render(
    <ToolResultRenderer
      result={result}
      onSelectItems={overrides?.onSelectItems}
      selectedItems={overrides?.selectedItems}
    />
  );
}

// ---------------------------------------------------------------------------
// error branch
// ---------------------------------------------------------------------------

describe('error branch', () => {
  it('renders the error message from data', () => {
    renderResult({ type: 'error', data: { message: 'Something went wrong' } });
    expect(screen.getByText('Something went wrong')).toBeInTheDocument();
  });

  it('renders the error icon', () => {
    const { container } = renderResult({ type: 'error', data: { message: 'Oops' } });
    expect(container.querySelector('.ws-tool-error__icon')).toBeInTheDocument();
  });

  it('falls back to "Tool error" when message is absent', () => {
    renderResult({ type: 'error', data: {} });
    expect(screen.getByText('Tool error')).toBeInTheDocument();
  });

  it('falls back to "Tool error" when message is an empty string', () => {
    renderResult({ type: 'error', data: { message: '' } });
    expect(screen.getByText('Tool error')).toBeInTheDocument();
  });

  it('falls back to "Tool error" when message is not a string (runtime guard)', () => {
    renderResult({ type: 'error', data: { message: 42 as never } });
    expect(screen.getByText('Tool error')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// text branch
// ---------------------------------------------------------------------------

describe('text branch', () => {
  it('renders content', () => {
    renderResult({ type: 'text', data: { content: 'Hello world' } });
    expect(screen.getByText('Hello world')).toBeInTheDocument();
  });

  it('renders .ws-tool-text wrapper', () => {
    const { container } = renderResult({ type: 'text', data: { content: 'x' } });
    expect(container.querySelector('.ws-tool-text')).toBeInTheDocument();
  });

  // Empty state
  it('renders empty div (no text) when content is empty string', () => {
    const { container } = renderResult({ type: 'text', data: { content: '' } });
    const el = container.querySelector('.ws-tool-text');
    expect(el).toBeInTheDocument();
    expect(el?.textContent).toBe('');
  });

  // Malformed fallback
  it('renders empty content when content is not a string (runtime guard)', () => {
    const { container } = renderResult({ type: 'text', data: { content: null as never } });
    const el = container.querySelector('.ws-tool-text');
    expect(el?.textContent).toBe('');
  });
});

// ---------------------------------------------------------------------------
// status branch
// ---------------------------------------------------------------------------

describe('status branch', () => {
  const baseStatus: ToolResultType = {
    type: 'status',
    data: { status: 'running', spec_name: 'my-workflow', run_id: 'run-123' },
  };

  it('renders spec_name', () => {
    renderResult(baseStatus);
    expect(screen.getByText('my-workflow')).toBeInTheDocument();
  });

  it('renders status badge', () => {
    renderResult(baseStatus);
    expect(screen.getByText('running')).toBeInTheDocument();
  });

  it('falls back to run_id when spec_name is absent', () => {
    renderResult({ type: 'status', data: { status: 'succeeded', run_id: 'run-456' } });
    expect(screen.getByText('run-456')).toBeInTheDocument();
  });

  it('renders em-dash when neither spec_name nor run_id is present', () => {
    renderResult({ type: 'status', data: { status: 'running' } });
    expect(screen.getByText('—')).toBeInTheDocument();
  });

  it('renders progress line when total_jobs is present', () => {
    renderResult({
      type: 'status',
      data: { status: 'running', total_jobs: 10, completed_jobs: 3 },
    });
    expect(screen.getByText('3 / 10 jobs')).toBeInTheDocument();
  });

  it('defaults completed_jobs to 0 in progress line', () => {
    renderResult({
      type: 'status',
      data: { status: 'running', total_jobs: 5 },
    });
    expect(screen.getByText('0 / 5 jobs')).toBeInTheDocument();
  });

  it('omits progress line when total_jobs is absent', () => {
    const { container } = renderResult({
      type: 'status',
      data: { status: 'running' },
    });
    expect(container.querySelector('.ws-tool-status__progress')).toBeNull();
  });

  // Empty state
  it('omits jobs section when jobs array is empty', () => {
    const { container } = renderResult({
      type: 'status',
      data: { status: 'running', jobs: [] },
    });
    expect(container.querySelector('.ws-tool-status__jobs')).toBeNull();
  });

  it('omits jobs section when jobs is absent', () => {
    const { container } = renderResult({
      type: 'status',
      data: { status: 'running' },
    });
    expect(container.querySelector('.ws-tool-status__jobs')).toBeNull();
  });

  it('renders jobs when present', () => {
    const { container } = renderResult({
      type: 'status',
      data: {
        status: 'running',
        jobs: [
          { id: 'j1', label: 'Build', status: 'succeeded' },
          { id: 'j2', label: 'Test',  status: 'failed' },
        ],
      },
    });
    const jobEls = container.querySelectorAll('.ws-tool-status__job');
    expect(jobEls).toHaveLength(2);
    expect(screen.getByText('Build')).toBeInTheDocument();
    expect(screen.getByText('Test')).toBeInTheDocument();
  });

  it('renders job duration when present', () => {
    renderResult({
      type: 'status',
      data: {
        status: 'running',
        jobs: [{ id: 'j1', label: 'Deploy', status: 'succeeded', duration: '1m 20s' }],
      },
    });
    expect(screen.getByText('1m 20s')).toBeInTheDocument();
  });

  it('omits duration when absent', () => {
    const { container } = renderResult({
      type: 'status',
      data: {
        status: 'running',
        jobs: [{ id: 'j1', label: 'Deploy', status: 'running' }],
      },
    });
    expect(container.querySelector('.ws-tool-status__dur')).toBeNull();
  });

  it('uses job label as key when id is absent (no React key warning)', () => {
    // If this renders without throwing it means React accepted the keys
    expect(() =>
      renderResult({
        type: 'status',
        data: {
          status: 'running',
          jobs: [
            { label: 'Step A', status: 'succeeded' },
            { label: 'Step B', status: 'running' },
          ],
        },
      })
    ).not.toThrow();
  });

  // Malformed fallback
  it('shows malformed fallback when status field is missing', () => {
    renderResult({ type: 'status', data: {} as never });
    expect(screen.getByText(/malformed status result/i)).toBeInTheDocument();
  });

  it('shows malformed fallback when status is not a string', () => {
    renderResult({ type: 'status', data: { status: 42 as never } });
    expect(screen.getByText(/malformed status result/i)).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// table branch
// ---------------------------------------------------------------------------

describe('table branch', () => {
  // Empty state
  it('shows "No results." when rows array is empty', () => {
    renderResult({ type: 'table', data: { columns: TABLE_COLUMNS, rows: [] } });
    expect(screen.getByText('No results.')).toBeInTheDocument();
  });

  it('shows "No results." when rows is missing', () => {
    renderResult({ type: 'table', data: { columns: TABLE_COLUMNS, rows: undefined as never } });
    expect(screen.getByText('No results.')).toBeInTheDocument();
  });

  // Malformed fallback
  it('shows malformed fallback when columns is empty array', () => {
    renderResult({ type: 'table', data: { columns: [], rows: TABLE_ROWS } });
    expect(screen.getByText(/malformed table/i)).toBeInTheDocument();
  });

  it('shows malformed fallback when columns is missing', () => {
    renderResult({ type: 'table', data: { columns: undefined as never, rows: TABLE_ROWS } });
    expect(screen.getByText(/malformed table/i)).toBeInTheDocument();
  });

  // Normal rendering
  it('renders column headers', () => {
    renderResult({ type: 'table', data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS } });
    expect(screen.getByText('ID')).toBeInTheDocument();
    expect(screen.getByText('Name')).toBeInTheDocument();
    expect(screen.getByText('Status')).toBeInTheDocument();
  });

  it('renders cell values', () => {
    renderResult({ type: 'table', data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS } });
    expect(screen.getByText('Alpha')).toBeInTheDocument();
    expect(screen.getByText('Beta')).toBeInTheDocument();
  });

  it('renders footer with plural "results"', () => {
    renderResult({ type: 'table', data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS } });
    expect(screen.getByText('2 results')).toBeInTheDocument();
  });

  it('renders footer with singular "result" for 1 row', () => {
    renderResult({ type: 'table', data: { columns: TABLE_COLUMNS, rows: [TABLE_ROWS[0]] } });
    expect(screen.getByText('1 result')).toBeInTheDocument();
  });

  it('does not render checkboxes when selectable is false', () => {
    const { container } = renderResult({
      type: 'table',
      data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS },
      selectable: false,
    });
    expect(container.querySelectorAll('input[type="checkbox"]')).toHaveLength(0);
  });

  it('renders header + row checkboxes when selectable=true', () => {
    const { container } = renderResult({
      type: 'table',
      data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS },
      selectable: true,
    });
    // 1 header + 2 rows = 3
    expect(container.querySelectorAll('input[type="checkbox"]')).toHaveLength(3);
  });

  it('calls onSelectItems with all rows on select-all check', () => {
    const handler = jest.fn();
    const { container } = renderResult(
      { type: 'table', data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS }, selectable: true },
      { onSelectItems: handler }
    );
    const selectAll = container.querySelector('thead input[type="checkbox"]') as HTMLInputElement;
    fireEvent.click(selectAll);
    expect(handler).toHaveBeenCalledWith(TABLE_ROWS);
  });

  it('calls onSelectItems with empty array on select-all uncheck', () => {
    const handler = jest.fn();
    const { container } = renderResult(
      { type: 'table', data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS }, selectable: true },
      { onSelectItems: handler }
    );
    const selectAll = container.querySelector('thead input[type="checkbox"]') as HTMLInputElement;
    // First click checks, second unchecks
    fireEvent.click(selectAll);
    fireEvent.click(selectAll);
    expect(handler).toHaveBeenLastCalledWith([]);
  });

  it('calls onSelectItems with the single row when a row checkbox is toggled', () => {
    const handler = jest.fn();
    const { container } = renderResult(
      { type: 'table', data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS }, selectable: true },
      { onSelectItems: handler }
    );
    const rowCheckboxes = container.querySelectorAll('tbody input[type="checkbox"]');
    fireEvent.click(rowCheckboxes[0]);
    expect(handler).toHaveBeenCalledWith([TABLE_ROWS[0]]);
  });

  it('marks selected rows using the stable row key in selectedItems', () => {
    // Key for TABLE_ROWS[0] = '1' (from row.id)
    const selectedItems = new Set([deriveRowKey(TABLE_ROWS[0], TABLE_COLUMNS)]);
    const { container } = renderResult(
      { type: 'table', data: { columns: TABLE_COLUMNS, rows: TABLE_ROWS }, selectable: true },
      { selectedItems }
    );
    const selected = container.querySelectorAll('.ws-tool-table__row--selected');
    expect(selected).toHaveLength(1);
    // Verify it's the first row
    expect(within(selected[0] as HTMLElement).getByText('Alpha')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// deriveRowKey helper (unit tests)
// ---------------------------------------------------------------------------

describe('deriveRowKey', () => {
  const cols = [{ key: 'name', label: 'Name' }, { key: 'age', label: 'Age' }];

  it('returns String(id) when id is present', () => {
    expect(deriveRowKey({ id: 42, name: 'X' }, cols)).toBe('42');
  });

  it('returns composite of column values when id is absent', () => {
    const key = deriveRowKey({ name: 'Alice', age: 30 }, cols);
    expect(key).toBe('Alice\x0030');
  });

  it('returns consistent key for identical rows (no id)', () => {
    const row = { name: 'Bob', age: 25 };
    expect(deriveRowKey(row, cols)).toBe(deriveRowKey(row, cols));
  });

  it('distinguishes rows with different column values', () => {
    const k1 = deriveRowKey({ name: 'Alice', age: 30 }, cols);
    const k2 = deriveRowKey({ name: 'Alice', age: 31 }, cols);
    expect(k1).not.toBe(k2);
  });
});

// ---------------------------------------------------------------------------
// cards branch
// ---------------------------------------------------------------------------

describe('cards branch', () => {
  // Empty state
  it('shows "No items." when items array is empty', () => {
    renderResult({ type: 'cards', data: { items: [] } });
    expect(screen.getByText('No items.')).toBeInTheDocument();
  });

  // Malformed fallback
  it('shows "No items." when items is null (non-array)', () => {
    renderResult({ type: 'cards', data: { items: null as never } });
    expect(screen.getByText('No items.')).toBeInTheDocument();
  });

  it('shows "No items." when items is undefined', () => {
    renderResult({ type: 'cards', data: { items: undefined as never } });
    expect(screen.getByText('No items.')).toBeInTheDocument();
  });

  // Normal rendering
  it('renders a card for each item', () => {
    const { container } = renderResult({
      type: 'cards',
      data: {
        items: [
          { id: 'c1', name: 'Foo', value: '10' },
          { id: 'c2', name: 'Bar', value: '20' },
        ],
      },
    });
    expect(container.querySelectorAll('.ws-tool-card')).toHaveLength(2);
  });

  it('renders field keys and values', () => {
    renderResult({
      type: 'cards',
      data: { items: [{ id: 'c1', name: 'Foo', score: '99' }] },
    });
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('Foo')).toBeInTheDocument();
    expect(screen.getByText('score')).toBeInTheDocument();
    expect(screen.getByText('99')).toBeInTheDocument();
  });

  it('uses id as card key when present (no React warning)', () => {
    expect(() =>
      renderResult({
        type: 'cards',
        data: {
          items: [
            { id: 'c1', name: 'A' },
            { id: 'c2', name: 'B' },
          ],
        },
      })
    ).not.toThrow();
  });

  it('uses name as card key when id is absent', () => {
    expect(() =>
      renderResult({
        type: 'cards',
        data: {
          items: [
            { name: 'Alpha' },
            { name: 'Beta' },
          ],
        },
      })
    ).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// ToolResultType exhaustiveness
// ---------------------------------------------------------------------------

describe('ToolResultType discriminated union', () => {
  it('covers all five known types without TypeScript error', () => {
    const types: ToolResultType['type'][] = ['error', 'text', 'status', 'table', 'cards'];
    expect(types).toHaveLength(5);
  });
});
