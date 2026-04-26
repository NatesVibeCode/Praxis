import '@testing-library/jest-dom';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { ButtonRowModule } from './ButtonRowModule';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status >= 200 && status < 300 ? 'OK' : 'Error',
    json: async () => body,
  } as Response;
}

describe('ButtonRowModule', () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('dispatches configured operations only through the unified operate gateway', async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ ok: true }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <ButtonRowModule
        quadrantId="actions-1"
        span={{ cols: 1, rows: 1 }}
        config={{
          actions: [
            {
              label: 'Approve',
              endpoint: '/api/bugs/resolve',
              body: { status: 'FIXED' },
              operation: 'surface.invoice.approve',
              input: { invoice_id: 'invoice-123' },
            },
          ],
        }}
      />,
    );

    fireEvent.click(screen.getByText('Approve'));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    const [requestUrl, requestInit] = fetchMock.mock.calls[0] as [
      string,
      RequestInit,
    ];
    expect(requestUrl).toBe('/api/operate');
    expect(requestInit.method).toBe('POST');
    expect(JSON.parse(String(requestInit.body))).toEqual({
      operation: 'surface.invoice.approve',
      input: { invoice_id: 'invoice-123' },
    });
  });
});
