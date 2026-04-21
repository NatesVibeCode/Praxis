import { describe, test, expect } from 'vitest';
import { computeLineage, presentBuild } from './moonBuildPresenter';
import type { BuildPayload, BuildNode, BuildEdge } from '../shared/types';

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
});
