import { describe, test, expect } from 'vitest';
import { presentRun, jobStatusToRingState } from './moonRunPresenter';
import type { RunDetail } from '../dashboard/useLiveRunSnapshot';

function makeRun(overrides: Partial<RunDetail> = {}): RunDetail {
  return {
    run_id: 'workflow_abc',
    spec_name: 'Test Workflow',
    status: 'running',
    total_jobs: 0,
    completed_jobs: 0,
    total_cost: 0,
    created_at: null,
    finished_at: null,
    total_duration_ms: 0,
    jobs: [],
    graph: { nodes: [], edges: [] },
    health: null,
    ...overrides,
  };
}

describe('jobStatusToRingState', () => {
  test('maps each workflow job status to the correct ring state', () => {
    expect(jobStatusToRingState('succeeded')).toBe('run-succeeded');
    expect(jobStatusToRingState('failed')).toBe('run-failed');
    expect(jobStatusToRingState('dead_letter')).toBe('run-failed');
    expect(jobStatusToRingState('blocked')).toBe('run-failed');
    expect(jobStatusToRingState('cancelled')).toBe('run-failed');
    expect(jobStatusToRingState('parent_failed')).toBe('run-failed');
    expect(jobStatusToRingState('running')).toBe('run-active');
    expect(jobStatusToRingState('claimed')).toBe('run-active');
    expect(jobStatusToRingState('pending')).toBe('run-pending');
    expect(jobStatusToRingState('ready')).toBe('run-pending');
    expect(jobStatusToRingState(undefined)).toBe('run-pending');
    expect(jobStatusToRingState('')).toBe('run-pending');
  });
});

describe('presentRun', () => {
  test('returns empty view model when run is null', () => {
    const vm = presentRun(null, null);
    expect(vm.nodes).toHaveLength(0);
    expect(vm.edges).toHaveLength(0);
    expect(vm.totalNodes).toBe(0);
  });

  test('returns empty view model when run has no graph', () => {
    const vm = presentRun(makeRun({ graph: null }), null);
    expect(vm.nodes).toHaveLength(0);
  });

  test('maps each RunGraphNode status to a ringState', () => {
    const run = makeRun({
      status: 'running',
      graph: {
        nodes: [
          { id: 'a', label: 'a', type: 'cli_llm', adapter: 'anthropic', position: 0, status: 'succeeded' },
          { id: 'b', label: 'b', type: 'cli_llm', adapter: 'anthropic', position: 1, status: 'running' },
          { id: 'c', label: 'c', type: 'cli_llm', adapter: 'openai',    position: 2, status: 'pending' },
          { id: 'd', label: 'd', type: 'cli_llm', adapter: 'openai',    position: 3, status: 'failed' },
          { id: 'e', label: 'e', type: 'cli_llm', adapter: 'openai',    position: 4, status: 'blocked' },
        ],
        edges: [
          { id: 'e1', from: 'a', to: 'b', type: 'sequence' },
          { id: 'e2', from: 'b', to: 'c', type: 'sequence' },
          { id: 'e3', from: 'c', to: 'd', type: 'sequence' },
          { id: 'e4', from: 'd', to: 'e', type: 'sequence' },
        ],
      },
    });
    const vm = presentRun(run, null);
    expect(vm.nodes).toHaveLength(5);
    const byId = new Map(vm.nodes.map((n) => [n.id, n]));
    expect(byId.get('a')?.ringState).toBe('run-succeeded');
    expect(byId.get('b')?.ringState).toBe('run-active');
    expect(byId.get('c')?.ringState).toBe('run-pending');
    expect(byId.get('d')?.ringState).toBe('run-failed');
    expect(byId.get('e')?.ringState).toBe('run-failed');
  });

  test('assigns layout positions along ranks and columns', () => {
    const run = makeRun({
      graph: {
        nodes: [
          { id: 'a', label: 'a', type: '', adapter: '', position: 0, status: 'succeeded' },
          { id: 'b', label: 'b', type: '', adapter: '', position: 1, status: 'running' },
          { id: 'c', label: 'c', type: '', adapter: '', position: 2, status: 'pending' },
        ],
        edges: [
          { id: 'e1', from: 'a', to: 'b', type: 'sequence' },
          { id: 'e2', from: 'b', to: 'c', type: 'sequence' },
        ],
      },
    });
    const vm = presentRun(run, null);
    const ranks = vm.nodes.map((n) => n.rank);
    expect(ranks).toEqual([0, 1, 2]);
    // Each successive rank has a higher x.
    const xs = vm.nodes.map((n) => n.x);
    expect(xs[1]).toBeGreaterThan(xs[0]);
    expect(xs[2]).toBeGreaterThan(xs[1]);
  });

  test('identifies critical path through the longest dependency chain', () => {
    const run = makeRun({
      graph: {
        nodes: [
          { id: 'a', label: 'a', type: '', adapter: '', position: 0, status: 'succeeded' },
          { id: 'b', label: 'b', type: '', adapter: '', position: 1, status: 'succeeded' },
          { id: 'c', label: 'c', type: '', adapter: '', position: 1, status: 'succeeded' },
          { id: 'd', label: 'd', type: '', adapter: '', position: 2, status: 'succeeded' },
        ],
        edges: [
          { id: 'e1', from: 'a', to: 'b', type: 'sequence' },
          { id: 'e2', from: 'a', to: 'c', type: 'sequence' },
          { id: 'e3', from: 'b', to: 'd', type: 'sequence' },
          // c has no successor — critical path is a → b → d
        ],
      },
    });
    const vm = presentRun(run, null);
    expect(vm.dominantPath).toEqual(['a', 'b', 'd']);
    expect(vm.nodes.find((n) => n.id === 'a')?.isOnDominantPath).toBe(true);
    expect(vm.nodes.find((n) => n.id === 'c')?.isOnDominantPath).toBe(false);
    expect(vm.nodes.find((n) => n.id === 'd')?.isOnDominantPath).toBe(true);
  });

  test('populates selectedNode when selectedJobId matches a node id', () => {
    const run = makeRun({
      graph: {
        nodes: [
          { id: 'analyze', label: 'analyze', type: '', adapter: '', position: 0, status: 'succeeded' },
          { id: 'propose',  label: 'propose',  type: '', adapter: '', position: 1, status: 'running' },
        ],
        edges: [{ id: 'e', from: 'analyze', to: 'propose', type: 'sequence' }],
      },
    });
    const vm = presentRun(run, 'propose');
    expect(vm.selectedNode?.id).toBe('propose');
    expect(vm.selectedNode?.ringState).toBe('run-active');
  });

  test('activeNode prefers running over pending', () => {
    const run = makeRun({
      graph: {
        nodes: [
          { id: 'a', label: 'a', type: '', adapter: '', position: 0, status: 'succeeded' },
          { id: 'b', label: 'b', type: '', adapter: '', position: 1, status: 'pending' },
          { id: 'c', label: 'c', type: '', adapter: '', position: 2, status: 'running' },
        ],
        edges: [
          { id: 'e1', from: 'a', to: 'b', type: 'sequence' },
          { id: 'e2', from: 'b', to: 'c', type: 'sequence' },
        ],
      },
    });
    const vm = presentRun(run, null);
    expect(vm.activeNode?.id).toBe('c'); // running wins over pending
  });

  test('counts resolved and blocked correctly for partial runs', () => {
    const run = makeRun({
      graph: {
        nodes: [
          { id: 'a', label: 'a', type: '', adapter: '', position: 0, status: 'succeeded' },
          { id: 'b', label: 'b', type: '', adapter: '', position: 1, status: 'succeeded' },
          { id: 'c', label: 'c', type: '', adapter: '', position: 2, status: 'failed' },
          { id: 'd', label: 'd', type: '', adapter: '', position: 3, status: 'blocked' },
        ],
        edges: [],
      },
    });
    const vm = presentRun(run, null);
    expect(vm.totalNodes).toBe(4);
    expect(vm.resolvedNodes).toBe(2);
    expect(vm.blockedNodes).toBe(2);
  });

  test('release.readiness reflects overall run status', () => {
    const withGraph = (status: RunDetail['status']): RunDetail => makeRun({
      status,
      graph: {
        nodes: [{ id: 'a', label: 'a', type: '', adapter: '', position: 0, status }],
        edges: [],
      },
    });
    expect(presentRun(withGraph('succeeded'), null).release.readiness).toBe('ready');
    expect(presentRun(withGraph('failed'), null).release.readiness).toBe('blocked');
    expect(presentRun(withGraph('running'), null).release.readiness).toBe('draft');
  });

  test('OrbitEdges carry from/to + isOnDominantPath correctly', () => {
    const run = makeRun({
      graph: {
        nodes: [
          { id: 'a', label: 'a', type: '', adapter: '', position: 0, status: 'succeeded' },
          { id: 'b', label: 'b', type: '', adapter: '', position: 1, status: 'succeeded' },
          { id: 'c', label: 'c', type: '', adapter: '', position: 1, status: 'pending' },
        ],
        edges: [
          { id: 'e1', from: 'a', to: 'b', type: 'sequence' },
          { id: 'e2', from: 'a', to: 'c', type: 'sequence' },
        ],
      },
    });
    const vm = presentRun(run, null);
    expect(vm.edges).toHaveLength(2);
    const e1 = vm.edges.find((e) => e.id === 'e1');
    const e2 = vm.edges.find((e) => e.id === 'e2');
    expect(e1?.from).toBe('a');
    expect(e1?.to).toBe('b');
    // Critical path is a→b (longer chain available for b than c).
    expect(e1?.isOnDominantPath).toBe(true);
    expect(e2?.isOnDominantPath).toBe(false);
  });
});
