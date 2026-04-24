import '@testing-library/jest-dom';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { afterEach, describe, expect, test, vi } from 'vitest';

import IntentBoxModule from './IntentBoxModule';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status >= 200 && status < 300 ? 'OK' : 'Error',
    json: async () => body,
  } as Response;
}

describe('IntentBoxModule', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('does not show Generate when intent analysis fell back', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/intent/analyze?q=bespoke%20dashboard') {
        return jsonResponse({
          templates: [],
          can_generate: true,
          analysis: {
            source: 'fallback',
            matches: {
              ui_components: [],
              calculations: [],
              workflows: [],
              total_count: 0,
            },
          },
        });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <IntentBoxModule
        quadrantId="intent-1"
        span={{ cols: 2, rows: 1 }}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText('What would you like to build?'), {
      target: { value: 'bespoke dashboard' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Go' }));

    await waitFor(() => {
      expect(screen.getByText(/generation is paused until analysis recovers/i)).toBeInTheDocument();
    });

    expect(screen.queryByRole('button', { name: 'Generate App' })).not.toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith('/api/intent/analyze?q=bespoke%20dashboard');
  });
});
