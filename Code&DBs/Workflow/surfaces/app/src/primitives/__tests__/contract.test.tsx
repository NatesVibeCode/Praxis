/**
 * Contract test for the React primitive library.
 *
 * Per architecture-policy::design-system-single-react-primitive-library
 * (CODEOWNERS @nate · docs/primitive-adoption.md), every component in
 * `src/primitives/` must render the `prx-*` CSS structure. This test
 * fails on PR if someone re-introduces inline-style soup in place of
 * the prx-* class structure.
 *
 * If a primitive is intentionally moved off prx-* (e.g., domain-specific
 * Toast), document the exception here AND add a comment in
 * docs/primitive-adoption.md.
 */
import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { ActivityFeed } from '../ActivityFeed';
import { DataTable } from '../DataTable';
import { LoadingSkeleton } from '../LoadingSkeleton';
import { MetricCard } from '../MetricCard';
import { StatsRow } from '../StatsRow';

describe('primitives/ contract — must render prx-* classes', () => {
  test('DataTable renders prx-table', () => {
    render(<DataTable columns={[{ key: 'a', label: 'A' }]} data={[{ a: 'x' }]} />);
    const wrapper = screen.getByTestId('prx-data-table');
    expect(wrapper).toHaveClass('prx-table');
    expect(wrapper.querySelector('.body')).toBeInTheDocument();
  });

  test('StatsRow renders prx-status-rail', () => {
    render(<StatsRow stats={[{ label: 'queue', value: 14 }]} />);
    const rail = screen.getByTestId('prx-stats-row');
    expect(rail).toHaveClass('prx-status-rail');
    // each stat carries label + value spans
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

  test('LoadingSkeleton preserves ws-skeleton (documented exception)', () => {
    render(<LoadingSkeleton lines={2} />);
    const skel = screen.getByTestId('prx-loading-skeleton');
    // Documented exception: keeps ws-skeleton class (own dedicated styles)
    expect(skel).toHaveClass('ws-skeleton');
  });
});
