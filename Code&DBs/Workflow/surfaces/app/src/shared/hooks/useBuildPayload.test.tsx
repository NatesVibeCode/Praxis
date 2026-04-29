import { act, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import type { BuildPayload } from '../types';
import { useBuildPayload } from './useBuildPayload';

const mocks = vi.hoisted(() => ({
  loadWorkflowBuild: vi.fn(),
  postBuildMutation: vi.fn(),
  latestEvent: null as unknown,
}));

vi.mock('../buildController', () => ({
  loadWorkflowBuild: mocks.loadWorkflowBuild,
  postBuildMutation: mocks.postBuildMutation,
}));

vi.mock('./useBuildEvents', () => ({
  useBuildEvents: () => ({
    events: mocks.latestEvent ? [mocks.latestEvent] : [],
    latestEvent: mocks.latestEvent,
    connected: false,
    error: null,
  }),
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function payload(id: string): BuildPayload {
  return { workflow_id: id } as unknown as BuildPayload;
}

let latestHook: ReturnType<typeof useBuildPayload> | null = null;

function Probe({ workflowId }: { workflowId: string | null }) {
  latestHook = useBuildPayload(workflowId);
  const current = latestHook.payload as unknown as { workflow_id?: string } | null;
  return (
    <div data-testid="state">
      {JSON.stringify({
        payload: current?.workflow_id ?? null,
        loading: latestHook.loading,
        error: latestHook.error,
      })}
    </div>
  );
}

describe('useBuildPayload', () => {
  beforeEach(() => {
    latestHook = null;
    mocks.latestEvent = null;
    mocks.loadWorkflowBuild.mockReset();
    mocks.postBuildMutation.mockReset();
  });

  it('does not let an older load overwrite a newer mutation result', async () => {
    const load = deferred<BuildPayload>();
    let loadSignal: AbortSignal | undefined;
    mocks.loadWorkflowBuild.mockImplementation((_workflowId: string, options?: { signal?: AbortSignal }) => {
      loadSignal = options?.signal;
      return load.promise;
    });
    mocks.postBuildMutation.mockResolvedValue(payload('wf-new'));

    render(<Probe workflowId="wf-1" />);

    await waitFor(() => {
      expect(mocks.loadWorkflowBuild).toHaveBeenCalledWith('wf-1', expect.objectContaining({ signal: expect.any(AbortSignal) }));
    });

    await act(async () => {
      await latestHook?.mutate('save', {});
    });

    expect(loadSignal?.aborted).toBe(true);
    await waitFor(() => {
      expect(screen.getByTestId('state')).toHaveTextContent('"payload":"wf-new"');
    });

    await act(async () => {
      load.resolve(payload('wf-old'));
      await load.promise;
    });

    expect(screen.getByTestId('state')).toHaveTextContent('"payload":"wf-new"');
  });

  it('reloads service-bus events against the current workflow id', async () => {
    mocks.loadWorkflowBuild.mockResolvedValue(payload('wf-loaded'));

    const { rerender } = render(<Probe workflowId="wf-old" />);
    await waitFor(() => {
      expect(mocks.loadWorkflowBuild).toHaveBeenCalledWith('wf-old', expect.any(Object));
    });

    rerender(<Probe workflowId="wf-current" />);
    await waitFor(() => {
      expect(mocks.loadWorkflowBuild).toHaveBeenCalledWith('wf-current', expect.any(Object));
    });

    mocks.latestEvent = { type: 'build.updated' };
    rerender(<Probe workflowId="wf-current" />);

    await waitFor(() => {
      const lastCall = mocks.loadWorkflowBuild.mock.calls[mocks.loadWorkflowBuild.mock.calls.length - 1];
      expect(lastCall[0]).toBe('wf-current');
    });
  });
});
