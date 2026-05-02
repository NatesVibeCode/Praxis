import { describe, test, expect } from 'vitest';
import { canvasBuildReducer, initialCanvasBuildState } from './canvasBuildReducer';

describe('canvasBuildReducer — run view transitions', () => {
  test('ENTER_RUN_VIEW from URL source sets viewMode run + activeRunId + clears build transient state', () => {
    const base = {
      ...initialCanvasBuildState,
      selectedNodeId: 'step-1',
      dragItemId: 'foo',
      previewTarget: 'bar',
      emptyMode: 'compose' as const,
      popoutOpen: true,
    };
    const next = canvasBuildReducer(base, { type: 'ENTER_RUN_VIEW', runId: 'workflow_abc', source: 'url' });
    expect(next.viewMode).toBe('run');
    expect(next.activeRunId).toBe('workflow_abc');
    expect(next.runViewSource).toBe('url');
    expect(next.runViewOpen).toBe(false);
    // build-mode transient state cleared
    expect(next.selectedNodeId).toBeNull();
    expect(next.dragItemId).toBeNull();
    expect(next.previewTarget).toBeNull();
    expect(next.emptyMode).toBeNull();
    expect(next.popoutOpen).toBe(false);
  });

  test('ENTER_RUN_VIEW from dispatch source records the correct origin', () => {
    const next = canvasBuildReducer(initialCanvasBuildState, { type: 'ENTER_RUN_VIEW', runId: 'run_1', source: 'dispatch' });
    expect(next.runViewSource).toBe('dispatch');
  });

  test('EXIT_RUN_VIEW returns viewMode to build and clears run state', () => {
    const entered = canvasBuildReducer(initialCanvasBuildState, { type: 'ENTER_RUN_VIEW', runId: 'r1', source: 'url' });
    const selected = canvasBuildReducer(entered, { type: 'SELECT_RUN_JOB', jobId: 'step_a' });
    expect(selected.selectedRunJobId).toBe('step_a');

    const exited = canvasBuildReducer(selected, { type: 'EXIT_RUN_VIEW' });
    expect(exited.viewMode).toBe('build');
    expect(exited.activeRunId).toBeNull();
    expect(exited.runViewSource).toBeNull();
    expect(exited.runViewOpen).toBe(false);
    expect(exited.selectedRunJobId).toBeNull();
    expect(exited.emptyMode).toBe('choice'); // back to default
  });

  test('SELECT_RUN_JOB updates selectedRunJobId without touching viewMode', () => {
    const entered = canvasBuildReducer(initialCanvasBuildState, { type: 'ENTER_RUN_VIEW', runId: 'r1', source: 'url' });
    const a = canvasBuildReducer(entered, { type: 'SELECT_RUN_JOB', jobId: 'job_a' });
    expect(a.selectedRunJobId).toBe('job_a');
    expect(a.viewMode).toBe('run');
    const cleared = canvasBuildReducer(a, { type: 'SELECT_RUN_JOB', jobId: null });
    expect(cleared.selectedRunJobId).toBeNull();
    expect(cleared.viewMode).toBe('run');
  });

  test('DISPATCH_SUCCESS is a thin wrapper that enters run view with dispatch source', () => {
    const next = canvasBuildReducer(initialCanvasBuildState, { type: 'DISPATCH_SUCCESS', runId: 'r1' });
    expect(next.viewMode).toBe('run');
    expect(next.activeRunId).toBe('r1');
    expect(next.runViewSource).toBe('dispatch');
  });

  test('CLOSE_RUN exits run view the same as EXIT_RUN_VIEW', () => {
    const entered = canvasBuildReducer(initialCanvasBuildState, { type: 'ENTER_RUN_VIEW', runId: 'r1', source: 'url' });
    const closed = canvasBuildReducer(entered, { type: 'CLOSE_RUN' });
    expect(closed.viewMode).toBe('build');
    expect(closed.activeRunId).toBeNull();
    expect(closed.runViewOpen).toBe(false);
  });
});
