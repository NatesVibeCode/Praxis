/**
 * Contract test for the React primitive library.
 *
 * Every component in `src/primitives/` must render the `prx-*` CSS
 * structure. This test fails if someone re-introduces inline-style
 * soup in place of the prx-* class structure.
 */
import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { ActivityFeed } from '../ActivityFeed';
import { DataTable } from '../DataTable';
import { LoadingSkeleton } from '../LoadingSkeleton';
import { MetricCard } from '../MetricCard';
import { StatusGrid } from '../StatusGrid';
import { StatsRow } from '../StatsRow';

describe('primitives/ contract — must render prx-* classes', () => {
  test('DataTable delegates to PrxTable', () => {
    render(<DataTable columns={[{ key: 'a', label: 'A' }]} data={[{ a: 'x' }]} />);
    const wrapper = screen.getByTestId('prx-table');
    expect(wrapper).toHaveClass('prx-table');
    expect(wrapper.querySelector('.body')).toBeInTheDocument();
  });

  test('StatsRow delegates to StatusRail', () => {
    render(<StatsRow stats={[{ label: 'queue', value: 14 }]} />);
    const rail = screen.getByTestId('prx-status-rail');
    expect(rail).toHaveClass('prx-status-rail');
    expect(rail.querySelector('.item .label')).toBeInTheDocument();
    expect(rail.querySelector('.item .v')).toBeInTheDocument();
  });

  test('MetricCard renders prx-roi', () => {
    render(<MetricCard label="dispatches" value={1427} />);
    const card = screen.getByTestId('prx-metric-card');
    expect(card).toHaveClass('prx-roi');
    expect(card.querySelector('.stat .label')).toBeInTheDocument();
    expect(card.querySelector('.stat .v')).toBeInTheDocument();
  });

  test('ActivityFeed renders prx-friction wrapping prx-runlog', () => {
    render(
      <ActivityFeed
        title="recent"
        data={[{ label: 'workflow.run.alpha', status: 'succeeded', agent: 'auto/draft' }]}
      />,
    );
    const wrap = screen.getByTestId('prx-activity-feed');
    expect(wrap).toHaveClass('prx-friction');
    const rows = screen.getByTestId('prx-activity-feed-rows');
    expect(rows).toHaveClass('prx-runlog');
    expect(rows.querySelector('.row .stat')).toBeInTheDocument();
  });

  test('StatusGrid renders prx-status-grid with prx-led indicators', () => {
    render(
      <StatusGrid
        title="Services"
        data={[{ name: 'api', status: 'active' }, { name: 'worker', status: 'failed' }]}
      />,
    );
    const grid = screen.getByTestId('prx-status-grid');
    expect(grid).toHaveClass('prx-status-grid');
    expect(grid.querySelector('.prx-led')).toBeInTheDocument();
    expect(grid.querySelector('.prx-led[data-tone="ok"]')).toBeInTheDocument();
    expect(grid.querySelector('.prx-led[data-tone="err"]')).toBeInTheDocument();
  });

  test('LoadingSkeleton preserves ws-skeleton (documented exception)', () => {
    render(<LoadingSkeleton lines={2} />);
    const skel = screen.getByTestId('prx-loading-skeleton');
    expect(skel).toHaveClass('ws-skeleton');
  });
});
