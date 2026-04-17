import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { DataTable } from './DataTable';

describe('DataTable', () => {
  test('keeps wide content contained within a fixed-layout wrapper', () => {
    const { container } = render(
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

    const table = container.querySelector('table');
    expect(table).toBeInTheDocument();
    expect(table).toHaveStyle({ tableLayout: 'fixed', width: '100%' });
    expect(screen.getByText('workflow.run.very-long-workflow-name-that-should-not-expand-the-table')).toBeInTheDocument();
  });
});
