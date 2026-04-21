import '@testing-library/jest-dom';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import SearchPanelModule from './SearchPanelModule';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

describe('SearchPanelModule', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  test('defaults to the global search authority', async () => {
    vi.useFakeTimers();

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/search?q=alpha') {
        return jsonResponse({
          results: [
            { name: 'Alpha Result', description: 'Registry entry', kind: 'registry' },
          ],
        });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <SearchPanelModule
        quadrantId="search-1"
        span={{ cols: 2, rows: 1 }}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText('Search...'), {
      target: { value: 'alpha' },
    });

    await act(async () => {
      vi.advanceTimersByTime(300);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith('/api/search?q=alpha');
    });
  });
});
