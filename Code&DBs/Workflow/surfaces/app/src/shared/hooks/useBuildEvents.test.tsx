import { act, render, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { useBuildEvents } from './useBuildEvents';

class MockEventSource {
  static instances: MockEventSource[] = [];

  readonly url: string;
  closed = false;
  onopen: ((event: Event) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  private listeners = new Map<string, Set<(event: MessageEvent<string>) => void>>();

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: MessageEvent<string>) => void) {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  close() {
    this.closed = true;
  }

  error() {
    this.onerror?.(new Event('error'));
  }

  emit(type: string, payload: unknown) {
    const event = new MessageEvent<string>('message', { data: JSON.stringify(payload) });
    if (type === 'message') {
      this.onmessage?.(event);
      return;
    }
    this.listeners.get(type)?.forEach((listener) => listener(event));
  }
}

function Probe({ workflowId }: { workflowId: string | null }) {
  const state = useBuildEvents(workflowId);
  return (
    <div data-testid="state">
      {JSON.stringify({
        connected: state.connected,
        error: state.error,
        eventCount: state.events.length,
        latestEventType: state.latestEvent?.type ?? null,
      })}
    </div>
  );
}

describe('useBuildEvents', () => {
  const originalEventSource = window.EventSource;

  beforeEach(() => {
    MockEventSource.instances = [];
    Object.defineProperty(window, 'EventSource', {
      configurable: true,
      writable: true,
      value: MockEventSource,
    });
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    Object.defineProperty(window, 'EventSource', {
      configurable: true,
      writable: true,
      value: originalEventSource,
    });
  });

  it('does not reconnect after unmount', () => {
    const { unmount } = render(<Probe workflowId="wf-1" />);
    expect(MockEventSource.instances).toHaveLength(1);

    act(() => {
      MockEventSource.instances[0].error();
    });

    unmount();

    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(MockEventSource.instances).toHaveLength(1);
    expect(MockEventSource.instances[0].closed).toBe(true);
  });

  it('does not reconnect a previous workflow after the id changes', async () => {
    const { rerender } = render(<Probe workflowId="wf-1" />);
    expect(MockEventSource.instances).toHaveLength(1);

    act(() => {
      MockEventSource.instances[0].error();
    });

    rerender(<Probe workflowId="wf-2" />);

    await waitFor(() => {
      expect(MockEventSource.instances).toHaveLength(2);
    });

    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(MockEventSource.instances).toHaveLength(2);
    expect(MockEventSource.instances[0].closed).toBe(true);
    expect(MockEventSource.instances[1].url).toBe('/api/workflows/wf-2/build/stream');
  });
});
