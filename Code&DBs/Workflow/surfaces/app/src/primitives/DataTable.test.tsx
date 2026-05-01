import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { DataTable } from './DataTable';

describe('DataTable', () => {
  test('renders the prx-table structure with fixed-layout content containment', () => {
    render(
      <DataTable
        columns={[
          { key: 'workflow', label: 'Workflow' },
          { key: 'run_id', label: 'Run' },
          { key: 'status', label: 'Status' },
        ]}
        data={[
          {
            workflow: 'workflow.run.very-long-workflow-name-that-should-not-expand-the-table',
            run_id: 'run:workflow.run.very-long-run-id-that-should-not-expand-the-table',
            status: 'failed',
          },
        ]}
      />,
    );

    // Class-based assertion (post-refactor structure)
    const wrapper = screen.getByTestId('prx-data-table');
    expect(wrapper).toHaveClass('prx-table');

    // Layout assertion remains — table-layout fixed both in CSS class and inline
    const table = wrapper.querySelector('table');
    expect(table).toBeInTheDocument();
    expect(table).toHaveStyle({ tableLayout: 'fixed', width: '100%' });

    // Long content survives without expanding the table
    expect(
      screen.getByText('workflow.run.very-long-workflow-name-that-should-not-expand-the-table'),
    ).toBeInTheDocument();
  });
});
