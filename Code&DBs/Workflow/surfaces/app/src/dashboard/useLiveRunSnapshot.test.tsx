import { act, render, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';

import { loadRunSnapshot, useLiveRunSnapshot } from './useLiveRunSnapshot';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

class MockEventSource {
  static instances: MockEventSource[] = [];

  readonly url: string;
  closed = false;
  onopen: any = null;
  onerror: any = null;
  onmessage: any = null;
  private listeners = new Map<string, Set<(ev: MessageEvent<string>) => unknown>>();

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (ev: MessageEvent<string>) => unknown) {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (ev: MessageEvent<string>) => unknown) {
    this.listeners.get(type)?.delete(listener);
  }

  close() {
    this.closed = true;
  }

  open() {
    this.onopen?.(new Event('open'));
  }

  error() {
    this.onerror?.(new Event('error'));
  }

  emit(type: string, payload: unknown = {}) {
    const event = new MessageEvent('message', {
      data: JSON.stringify(payload),
    });
    if (type === 'message') {
      this.onmessage?.(event);
      return;
    }
    this.listeners.get(type)?.forEach((listener) => listener(event));
  }
}

function Probe({ runId }: { runId: string | null }) {
  const { run, loading, error, streamStatus } = useLiveRunSnapshot(runId);

  return (
    <div data-testid="state">
      {JSON.stringify({
        loading,
        error,
        streamStatus,
        runId: run?.run_id ?? null,
        status: run?.status ?? null,
        totalJobs: run?.total_jobs ?? null,
      })}
    </div>
  );
}

describe('useLiveRunSnapshot', () => {
  const originalEventSource = window.EventSource;

  beforeEach(() => {
    MockEventSource.instances = [];
    Object.defineProperty(window, 'EventSource', {
      configurable: true,
      writable: true,
      value: MockEventSource,
    });
    jest.restoreAllMocks();
  });

  afterEach(() => {
    Object.defineProperty(window, 'EventSource', {
      configurable: true,
      writable: true,
      value: originalEventSource,
    });
  });

  it('loads the authoritative run detail before falling back to recent runs', async () => {
    const detail = {
      run_id: 'run-1',
      spec_name: 'Spec One',
      status: 'running',
      total_jobs: 3,
      completed_jobs: 1,
      total_cost: 3.5,
      total_duration_ms: 1200,
      created_at: '2026-04-11T11:30:00+00:00',
      finished_at: null,
      jobs: [],
    };

    const fetchMock = jest.fn().mockResolvedValue(jsonResponse(detail));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const snapshot = await loadRunSnapshot('run-1');

    expect(snapshot).toEqual(detail);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith('/api/runs/run-1');
  });

  it('falls back to recent runs only when the detail route is missing', async () => {
    const recent = {
      run_id: 'run-2',
      spec_name: 'Spec Two',
      status: 'queued',
      total_jobs: 4,
      completed_jobs: 0,
      total_cost: 0,
      created_at: '2026-04-11T12:00:00+00:00',
      finished_at: null,
    };

    const fetchMock = jest
      .fn()
      .mockResolvedValueOnce(jsonResponse({ error: 'missing' }, 404))
      .mockResolvedValueOnce(jsonResponse([recent]));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const snapshot = await loadRunSnapshot('run-2');

    expect(snapshot).toEqual({
      ...recent,
      total_duration_ms: 0,
      jobs: [],
    });
    expect(fetchMock).toHaveBeenNthCalledWith(1, '/api/runs/run-2');
    expect(fetchMock).toHaveBeenNthCalledWith(2, '/api/runs/recent?limit=100');
  });

  it('refreshes the run snapshot when the live stream emits progress', async () => {
    const initial = {
      run_id: 'run-3',
      spec_name: 'Spec Three',
      status: 'running',
      total_jobs: 2,
      completed_jobs: 1,
      total_cost: 1.25,
      total_duration_ms: 600,
      created_at: '2026-04-11T12:30:00+00:00',
      finished_at: null,
      jobs: [],
    };
    const updated = {
      ...initial,
      status: 'succeeded' as const,
      completed_jobs: 2,
      finished_at: '2026-04-11T12:31:00+00:00',
    };

    const fetchMock = jest
      .fn()
      .mockResolvedValueOnce(jsonResponse(initial))
      .mockResolvedValueOnce(jsonResponse(updated));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(<Probe runId="run-3" />);

    const state = await screen.findByTestId('state');
    await waitFor(() => {
      expect(state).toHaveTextContent('"status":"running"');
    });

    expect(MockEventSource.instances).toHaveLength(1);
    expect(MockEventSource.instances[0].url).toBe('/api/workflow-runs/run-3/stream');

    act(() => {
      MockEventSource.instances[0].open();
      MockEventSource.instances[0].emit('progress', { completed: 2, total: 2 });
    });

    await waitFor(() => {
      expect(state).toHaveTextContent('"status":"succeeded"');
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock).toHaveBeenNthCalledWith(1, '/api/runs/run-3');
    expect(fetchMock).toHaveBeenNthCalledWith(2, '/api/runs/run-3');
  });
});
