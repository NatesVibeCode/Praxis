import { act, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { useModuleData } from './useModuleData';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status >= 200 && status < 300 ? 'OK' : 'Error',
    json: async () => body,
  } as Response;
}

function deferredResponse() {
  let resolve!: (value: Response) => void;
  const promise = new Promise<Response>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function Probe({ endpoint }: { endpoint: string }) {
  const state = useModuleData<{ value: string }>(endpoint);
  return (
    <div data-testid="state">
      {JSON.stringify({
        loading: state.loading,
        error: state.error,
        value: state.data?.value ?? null,
      })}
    </div>
  );
}

describe('useModuleData', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('keeps an aborted older request from clearing the active request state', async () => {
    const first = deferredResponse();
    const second = deferredResponse();
    const requests: Array<{ input: string; signal?: AbortSignal }> = [];
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      requests.push({ input: String(input), signal: init?.signal ?? undefined });
      return requests.length === 1 ? first.promise : second.promise;
    });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { rerender } = render(<Probe endpoint="alpha" />);

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    rerender(<Probe endpoint="beta" />);

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });

    expect(requests[0].signal?.aborted).toBe(true);

    await act(async () => {
      first.resolve(jsonResponse({ value: 'stale' }));
      await first.promise;
    });

    expect(screen.getByTestId('state')).toHaveTextContent('"loading":true');
    expect(screen.getByTestId('state')).toHaveTextContent('"value":null');

    await act(async () => {
      second.resolve(jsonResponse({ value: 'current' }));
      await second.promise;
    });

    await waitFor(() => {
      expect(screen.getByTestId('state')).toHaveTextContent('"loading":false');
      expect(screen.getByTestId('state')).toHaveTextContent('"value":"current"');
    });
  });
});
