import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { DataTable } from './DataTable';

describe('DataTable', () => {
  test('renders the prx-table structure via PrxTable delegation', () => {
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

    const wrapper = screen.getByTestId('prx-table');
    expect(wrapper).toHaveClass('prx-table');

    const table = wrapper.querySelector('table');
    expect(table).toBeInTheDocument();

    expect(
      screen.getByText('workflow.run.very-long-workflow-name-that-should-not-expand-the-table'),
    ).toBeInTheDocument();
  });
});
