import { describe, test, expect } from 'vitest';
import { computeLineage, presentBuild } from './canvasBuildPresenter';
import type { BuildPayload, BuildNode, BuildEdge } from '../shared/types';
import { withBuildEdgeRelease } from '../shared/edgeRelease';

function makeNode(id: string, overrides: Partial<BuildNode> = {}): BuildNode {
  return { node_id: id, kind: 'step', title: id, route: '', status: '', summary: '', ...overrides };
}

function makeEdge(from: string, to: string): BuildEdge {
  return { edge_id: `e-${from}-${to}`, kind: 'sequence', from_node_id: from, to_node_id: to };
}

function makePayload(nodes: BuildNode[], edges: BuildEdge[]): BuildPayload {
  return {
    definition: {},
    build_graph: { nodes, edges },
    build_state: 'draft',
  };
}

describe('computeLineage', () => {
  const edges = [
    makeEdge('a', 'b'),
    makeEdge('b', 'c'),
    makeEdge('b', 'd'),
    makeEdge('d', 'e'),
  ];
  const nodeIds = ['a', 'b', 'c', 'd', 'e'];

  test('returns null when no node is selected', () => {
    expect(computeLineage(null, edges, nodeIds)).toBeNull();
  });

  test('returns null when selected id is not in the graph', () => {
    expect(computeLineage('missing', edges, nodeIds)).toBeNull();
  });

  test('walks both ancestors and descendants for a mid-branch node', () => {
    // Selecting 'd' should pull in 'a', 'b' (ancestors) and 'e' (descendant),
    // plus 'd' itself. The sibling 'c' must stay out so the UI dims it.
    const lineage = computeLineage('d', edges, nodeIds);
    expect(lineage).not.toBeNull();
    expect([...lineage!].sort()).toEqual(['a', 'b', 'd', 'e']);
    expect(lineage!.has('c')).toBe(false);
  });

  test('includes only the node itself when it is isolated', () => {
    const lineage = computeLineage('lone', [], ['lone']);
    expect([...lineage!]).toEqual(['lone']);
  });

  test('walks the full chain when selecting the root', () => {
    const lineage = computeLineage('a', edges, nodeIds);
    expect([...lineage!].sort()).toEqual(['a', 'b', 'c', 'd', 'e']);
  });
});

describe('presentBuild focus-lineage', () => {
  const nodes = [makeNode('a'), makeNode('b'), makeNode('c'), makeNode('d')];
  const edges = [makeEdge('a', 'b'), makeEdge('b', 'c'), makeEdge('b', 'd')];
  const payload = makePayload(nodes, edges);

  test('no selection → focusActive is false and every node/edge is inLineage', () => {
    const vm = presentBuild(payload, null, null);
    expect(vm.focusActive).toBe(false);
    for (const n of vm.nodes) expect(n.inLineage).toBe(true);
    for (const e of vm.edges) expect(e.inLineage).toBe(true);
  });

  test('selecting a leaf narrows inLineage to the ancestor chain only', () => {
    const vm = presentBuild(payload, 'c', null);
    expect(vm.focusActive).toBe(true);
    const lineageNodes = vm.nodes.filter(n => n.inLineage).map(n => n.id).sort();
    expect(lineageNodes).toEqual(['a', 'b', 'c']);
    // 'd' is a sibling branch — must dim.
    expect(vm.nodes.find(n => n.id === 'd')!.inLineage).toBe(false);
    // The a→b edge is in lineage (both endpoints are), b→c is in lineage,
    // b→d is NOT (d is outside the lineage).
    const edgeById = new Map(vm.edges.map(e => [e.id, e]));
    expect(edgeById.get('e-a-b')!.inLineage).toBe(true);
    expect(edgeById.get('e-b-c')!.inLineage).toBe(true);
    expect(edgeById.get('e-b-d')!.inLineage).toBe(false);
  });

  test('selecting the branch point keeps every downstream branch in lineage', () => {
    const vm = presentBuild(payload, 'b', null);
    expect(vm.focusActive).toBe(true);
    for (const n of vm.nodes) expect(n.inLineage).toBe(true);
    for (const e of vm.edges) expect(e.inLineage).toBe(true);
  });

  test('selecting a missing node falls back to rest state', () => {
    const vm = presentBuild(payload, 'not-in-graph', null);
    expect(vm.focusActive).toBe(false);
    for (const n of vm.nodes) expect(n.inLineage).toBe(true);
  });

  test('keeps artifact state nodes out of the human review map', () => {
    const artifactPayload = makePayload(
      [
        makeNode('step-1', { title: 'Plan discovery' }),
        makeNode('step-2', { title: 'Search and retrieve' }),
        makeNode('artifact-1', { kind: 'state', title: 'evidence_pack.json' }),
      ],
      [
        makeEdge('step-1', 'step-2'),
        makeEdge('step-1', 'artifact-1'),
      ],
    );

    const vm = presentBuild(artifactPayload, null, null);

    expect(vm.nodes.map(node => node.id)).toEqual(['step-1', 'step-2']);
    expect(vm.edges.map(edge => `${edge.from}->${edge.to}`)).toEqual(['step-1->step-2']);
    expect(vm.totalNodes).toBe(2);
  });

  test('keeps gate review nodes out of the main workflow map', () => {
    const gatePayload = makePayload(
      [
        makeNode('step-1', { title: 'Plan discovery' }),
        makeNode('gate-1', { kind: 'gate', title: 'Resolve input text' }),
        makeNode('step-2', { title: 'Search and retrieve' }),
      ],
      [
        makeEdge('step-1', 'gate-1'),
        makeEdge('gate-1', 'step-2'),
      ],
    );

    const vm = presentBuild(gatePayload, null, null);

    expect(vm.nodes.map(node => node.id)).toEqual(['step-1', 'step-2']);
    expect(vm.edges.map(edge => `${edge.from}->${edge.to}`)).toEqual(['step-1->step-2']);
    expect(vm.totalNodes).toBe(2);
  });

  test('keeps a branched graph as branches instead of flattening every node into the spine', () => {
    const branchPayload = makePayload(
      [
        makeNode('start', { title: 'Normalize app' }),
        makeNode('decide', { title: 'Evaluate fit' }),
        makeNode('build', { title: 'Build integration' }),
        makeNode('manual', { title: 'Manual review' }),
        makeNode('finish', { title: 'Package result' }),
      ],
      [
        makeEdge('start', 'decide'),
        withBuildEdgeRelease(makeEdge('decide', 'build'), {
          family: 'conditional',
          edge_type: 'conditional',
          branch_reason: 'then',
          label: 'Then',
        }),
        withBuildEdgeRelease(makeEdge('decide', 'manual'), {
          family: 'conditional',
          edge_type: 'conditional',
          branch_reason: 'else',
          label: 'Else',
        }),
        makeEdge('build', 'finish'),
        makeEdge('manual', 'finish'),
      ],
    );

    const vm = presentBuild(branchPayload, null, null);

    expect(vm.dominantPath).toEqual(['start', 'decide', 'build', 'finish']);
    expect(vm.nodes.find(node => node.id === 'start')!.y).toBeLessThan(vm.nodes.find(node => node.id === 'decide')!.y);
    expect(vm.nodes.find(node => node.id === 'decide')!.y).toBeLessThan(vm.nodes.find(node => node.id === 'finish')!.y);
    expect(vm.nodes.find(node => node.id === 'build')!.x).not.toBe(vm.nodes.find(node => node.id === 'manual')!.x);
    expect(vm.nodes.find(node => node.id === 'manual')?.dominantPathIndex).toBe(-1);
    expect(vm.edges.find(edge => edge.from === 'decide' && edge.to === 'build')?.isOnDominantPath).toBe(true);
    expect(vm.edges.find(edge => edge.from === 'decide' && edge.to === 'manual')?.isOnDominantPath).toBe(false);
    expect(vm.branchBoard).toHaveLength(1);
    expect(vm.branchBoard[0].sourceTitle).toBe('Evaluate fit');
    expect(vm.branchBoard[0].lanes.map(lane => lane.label)).toEqual(['Then', 'Else']);
    expect(vm.branchBoard[0].lanes[0].nodeIds).toEqual(['build']);
    expect(vm.branchBoard[0].lanes[0].terminal.label).toBe('Rejoins at Package result');
    expect(vm.branchBoard[0].lanes[1].nodeIds).toEqual(['manual']);
  });

  test('builds a review board for nested multi-step branch paths', () => {
    const branchPayload = makePayload(
      [
        makeNode('start', { title: 'Normalize app' }),
        makeNode('decide', { title: 'Evaluate fit' }),
        makeNode('auto', { title: 'Prepare build lane' }),
        makeNode('manual', { title: 'Manual approval' }),
        makeNode('route', { title: 'Route integration path' }),
        makeNode('docs', { title: 'Retrieve docs' }),
        makeNode('api', { title: 'Inspect API' }),
        makeNode('finish', { title: 'Package result' }),
      ],
      [
        makeEdge('start', 'decide'),
        withBuildEdgeRelease(makeEdge('decide', 'auto'), {
          family: 'conditional',
          edge_type: 'conditional',
          branch_reason: 'then',
          label: 'Then',
        }),
        withBuildEdgeRelease(makeEdge('decide', 'manual'), {
          family: 'conditional',
          edge_type: 'conditional',
          branch_reason: 'else',
          label: 'Else',
        }),
        makeEdge('auto', 'route'),
        withBuildEdgeRelease(makeEdge('route', 'docs'), {
          family: 'conditional',
          edge_type: 'conditional',
          label: 'Docs',
        }),
        withBuildEdgeRelease(makeEdge('route', 'api'), {
          family: 'conditional',
          edge_type: 'conditional',
          label: 'API',
        }),
        makeEdge('docs', 'finish'),
        makeEdge('api', 'finish'),
        makeEdge('manual', 'finish'),
      ],
    );

    const vm = presentBuild(branchPayload, null, null);

    expect(vm.branchBoard).toHaveLength(2);
    expect(vm.branchBoard.map(split => split.sourceTitle)).toEqual(['Evaluate fit', 'Route integration path']);
    expect(vm.branchBoard[0].lanes[0].nodeIds).toEqual(['auto', 'route']);
    expect(vm.branchBoard[0].lanes[0].terminal.label).toBe('Next split at Route integration path');
    expect(vm.branchBoard[1].lanes.map(lane => lane.label)).toEqual(['API', 'Docs']);
    expect(vm.branchBoard[1].lanes[0].terminal.label).toBe('Rejoins at Package result');
  });
});
