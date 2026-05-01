import { fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { AtlasTableView } from './AtlasTableView';
import type { AtlasPayload } from './AtlasPage';

function makePayload(): AtlasPayload {
  return {
    ok: true,
    nodes: [
      {
        data: {
          id: 'operator_decisions',
          label: 'operator_decisions',
          area: 'authority',
          node_kind: 'object',
          semantic_role: 'authority',
          activity_score: 0.92,
          updated_at: '2026-04-30T12:00:00Z',
          signal_authority: 0.7,
        },
      },
      {
        data: {
          id: 'memory_entities',
          label: 'memory_entities',
          area: 'memory',
          node_kind: 'object',
          semantic_role: 'data',
          activity_score: 0.62,
          updated_at: '2026-04-30T11:00:00Z',
          signal_activity: 0.7,
        },
      },
      {
        data: {
          id: 'broken_op',
          label: 'broken_op',
          area: 'authority',
          node_kind: 'object',
          semantic_role: 'risk',
          activity_score: 0.18,
          updated_at: '2026-04-29T10:00:00Z',
          signal_risk: 0.9,
        },
      },
    ],
    edges: [],
    areas: [],
    metadata: {
      node_count: 3,
      edge_count: 0,
      aggregate_edge_count: 0,
      source_authority: 'Praxis.db',
      generated_at: '2026-04-30T12:00:00Z',
    } as AtlasPayload['metadata'],
    warnings: [],
  };
}

interface Harness {
  payload: AtlasPayload | null;
  selectedId: string | null;
  filter: string;
  areaFilter: string | null;
  onSelect: ReturnType<typeof vi.fn>;
  onFilterChange: ReturnType<typeof vi.fn>;
  onAreaFilterChange: ReturnType<typeof vi.fn>;
}

function harness(overrides: Partial<Harness> = {}): Harness {
  return {
    payload: makePayload(),
    selectedId: null,
    filter: '',
    areaFilter: null,
    onSelect: vi.fn(),
    onFilterChange: vi.fn(),
    onAreaFilterChange: vi.fn(),
    ...overrides,
  };
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('AtlasTableView', () => {
  it('renders one row per non-class-label node and shows column headers', () => {
    const h = harness();
    render(<AtlasTableView {...h} />);

    expect(screen.getByPlaceholderText('filter…')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('area')).toBeInTheDocument();
    expect(screen.getByText('kind')).toBeInTheDocument();
    expect(screen.getByText('role')).toBeInTheDocument();
    expect(screen.getByText('state')).toBeInTheDocument();
    expect(screen.getByText('activity')).toBeInTheDocument();

    const rows = screen.getAllByRole('row');
    // 1 header + 3 data rows
    expect(rows).toHaveLength(4);

    expect(screen.getByText('operator_decisions')).toBeInTheDocument();
    expect(screen.getByText('memory_entities')).toBeInTheDocument();
    expect(screen.getByText('broken_op')).toBeInTheDocument();
  });

  it('derives state tone from signal flags (risk → err, activity → warn, authority → ok)', () => {
    const { container } = render(<AtlasTableView {...harness()} />);

    const stateCells = Array.from(container.querySelectorAll<HTMLElement>('tbody .stat-cap'));
    const byLabel = (label: string) => stateCells.find((el) => el.textContent === label);

    expect(byLabel('authority')?.getAttribute('data-tone')).toBe('ok');
    expect(byLabel('live')?.getAttribute('data-tone')).toBe('warn');
    expect(byLabel('risk')?.getAttribute('data-tone')).toBe('err');
  });

  it('narrows rows when filter changes (filter is controlled — caller maintains state)', () => {
    const h = harness({ filter: 'memory' });
    render(<AtlasTableView {...h} />);

    expect(screen.queryByText('operator_decisions')).not.toBeInTheDocument();
    expect(screen.queryByText('broken_op')).not.toBeInTheDocument();
    expect(screen.getByText('memory_entities')).toBeInTheDocument();
  });

  it('narrows rows when area filter is applied via the sidebar', () => {
    const h = harness({ areaFilter: 'memory' });
    render(<AtlasTableView {...h} />);

    expect(screen.queryByText('operator_decisions')).not.toBeInTheDocument();
    expect(screen.queryByText('broken_op')).not.toBeInTheDocument();
    expect(screen.getByText('memory_entities')).toBeInTheDocument();
  });

  it('fires onSelect with the node id when a row is clicked', () => {
    const h = harness();
    render(<AtlasTableView {...h} />);

    fireEvent.click(screen.getByText('memory_entities').closest('tr')!);
    expect(h.onSelect).toHaveBeenCalledWith('memory_entities');
  });

  it('toggles area filter via the sidebar tree', () => {
    const h = harness();
    render(<AtlasTableView {...h} />);

    const sidebar = screen.getByRole('navigation', { name: /filter by area/i });
    const memoryRow = within(sidebar).getByText('memory').closest('.row')!;
    fireEvent.click(memoryRow);
    expect(h.onAreaFilterChange).toHaveBeenCalledWith('memory');
  });

  it('shows an empty state when filter excludes everything', () => {
    const h = harness({ filter: 'no-match-anywhere' });
    render(<AtlasTableView {...h} />);

    expect(screen.getByText(/no nodes match/i)).toBeInTheDocument();
  });

  it('renders nothing meaningful when payload is null', () => {
    const h = harness({ payload: null });
    render(<AtlasTableView {...h} />);

    // Headers do not render when there are zero rows AFTER applying filters.
    // With an empty payload, the empty-state takes over the body slot.
    expect(screen.getByText(/no nodes match/i)).toBeInTheDocument();
  });

  it('marks the selected row with the .selected class', () => {
    const h = harness({ selectedId: 'memory_entities' });
    render(<AtlasTableView {...h} />);

    const row = screen.getByText('memory_entities').closest('tr')!;
    expect(row.className).toContain('selected');
  });
});
