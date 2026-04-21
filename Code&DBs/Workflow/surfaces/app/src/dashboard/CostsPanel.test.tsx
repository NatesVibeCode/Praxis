import '@testing-library/jest-dom';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { CostsPanel } from './CostsPanel';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

describe('CostsPanel', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  test('renders the cost ledger from the live API and opens runs from the table', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/costs') {
        return jsonResponse({
          total_cost_usd: 27.25,
          total_input_tokens: 12000,
          total_output_tokens: 8000,
          cost_by_agent: {
            'openai/gpt-5.4': 18.5,
            'anthropic/claude-sonnet-4-6': 8.75,
          },
          record_count: 4,
        });
      }
      if (url === '/api/runs/recent?limit=12') {
        return jsonResponse([
          {
            run_id: 'run_1',
            spec_name: 'Support Intake',
            status: 'running',
            total_jobs: 4,
            completed_jobs: 2,
            total_cost: 3.5,
            created_at: '2026-04-14T11:50:00+00:00',
            finished_at: null,
          },
        ]);
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const onViewRun = vi.fn();

    render(<CostsPanel onBack={() => undefined} onViewRun={onViewRun} />);

    await screen.findByText('Cost Summary');
    expect(await screen.findByText('$27.25')).toBeInTheDocument();
    expect(screen.getByText('12,000')).toBeInTheDocument();
    expect(screen.getByText('8,000')).toBeInTheDocument();
    expect(screen.getByText('Ledger Records')).toBeInTheDocument();
    expect(screen.getByText('openai/gpt-5.4 leads spend')).toBeInTheDocument();
    expect(screen.getByText('Support Intake')).toBeInTheDocument();

    fireEvent.click(screen.getByText('Support Intake'));
    expect(onViewRun).toHaveBeenCalledWith('run_1');

    await waitFor(() => {
      const urls = fetchMock.mock.calls.map(([url]) => String(url));
      expect(urls).toContain('/api/costs');
      expect(urls).toContain('/api/runs/recent?limit=12');
    });
  });
});
