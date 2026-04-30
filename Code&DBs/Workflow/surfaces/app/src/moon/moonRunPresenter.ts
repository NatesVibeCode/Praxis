// Pure presenter: RunDetail -> MoonBuildViewModel.
// Mirrors the shape of moonBuildPresenter so MoonBuildPage can render a
// workflow run through the same OrbitNode/OrbitEdge rendering primitives.
// No React, no fetch, no DOM.

import type {
  RunDetail,
  RunGraph,
  RunGraphNode,
  RunGraphEdge,
  RunJob,
} from '../dashboard/useLiveRunSnapshot';
import type {
  MoonBuildViewModel,
  OrbitNode,
  OrbitEdge,
  RingState,
  GlyphType,
  GraphLayout,
  LayoutNode,
} from './moonBuildPresenter';
import { RANK_SPACING, COLUMN_SPACING, NODE_SHAPE_DIMENSIONS, glyphFromLabel, computeLineage } from './moonBuildPresenter';

const COMPONENT_ROW_GAP = 40;
const COMPONENT_COLUMN_GAP = 120;

// --- Status mapping ---

/**
 * Map a workflow job/run-graph node status to a Moon ring state.
 * Mirrors runStatusToRingState in moonBuildPresenter.ts, but exported here
 * so run-view presentation is self-contained. Kept in sync manually; the
 * unit tests guarantee both codepaths agree for every job status value.
 */
export function jobStatusToRingState(status: string | undefined): RingState {
  const normalized = (status || '').toLowerCase();
  if (normalized === 'succeeded') return 'run-succeeded';
  if (
    normalized === 'failed'
    || normalized === 'dead_letter'
    || normalized === 'blocked'
    || normalized === 'cancelled'
    || normalized === 'parent_failed'
  ) return 'run-failed';
  if (normalized === 'running' || normalized === 'claimed') return 'run-active';
  // pending / ready / queued / empty: keep pending
  return 'run-pending';
}

/**
 * Type-based glyph inference. Labels carry the real semantic signal in run
 * graphs (job_type is usually "dispatch"); parse tokens out of the label
 * plus any adapter/type hints and match against the bounded vocabulary in
 * moonBuildPresenter's TYPE_TOKEN_TO_GLYPH. Same type → same icon.
 */
function inferGlyph(node: RunGraphNode, job: RunJob | undefined): GlyphType {
  const label = (node.label || node.id || '').toLowerCase();
  const jobType = (node.type || job?.job_type || '').toLowerCase();
  const adapter = (node.adapter || job?.agent_slug || '').toLowerCase();
  return glyphFromLabel(`${label} ${jobType} ${adapter}`) ?? 'step';
}

// --- Layout (topological, same spacing constants as build presenter) ---

function compareRunNodeIds(
  a: string,
  b: string,
  nodeById: Map<string, RunGraphNode>,
): number {
  const aPosition = nodeById.get(a)?.position ?? Number.MAX_SAFE_INTEGER;
  const bPosition = nodeById.get(b)?.position ?? Number.MAX_SAFE_INTEGER;
  if (aPosition !== bPosition) return aPosition - bPosition;
  return a.localeCompare(b);
}

function layoutLayeredRunNodes(
  graph: RunGraph,
  nodeIds: string[] = graph.nodes.map((node) => node.id),
): GraphLayout {
  const empty: GraphLayout = { nodes: new Map(), layers: [], width: 0, height: 0 };
  if (!nodeIds.length) return empty;

  const nodeSet = new Set(nodeIds);
  const nodeById = new Map(graph.nodes.map((node) => [node.id, node]));
  const adj = new Map<string, string[]>();
  const radj = new Map<string, string[]>();
  const inDeg = new Map<string, number>();
  for (const id of nodeIds) {
    adj.set(id, []);
    radj.set(id, []);
    inDeg.set(id, 0);
  }
  for (const e of graph.edges) {
    if (!nodeSet.has(e.from) || !nodeSet.has(e.to)) continue;
    adj.get(e.from)!.push(e.to);
    radj.get(e.to)!.push(e.from);
    inDeg.set(e.to, (inDeg.get(e.to) || 0) + 1);
  }
  for (const next of adj.values()) next.sort((a, b) => compareRunNodeIds(a, b, nodeById));

  // Assign rank = longest path from any root.
  const rank = new Map<string, number>();
  const queue = [...inDeg.entries()]
    .filter(([, d]) => d === 0)
    .map(([id]) => id)
    .sort((a, b) => compareRunNodeIds(a, b, nodeById));
  for (const id of queue) rank.set(id, 0);
  const topo: string[] = [];
  const pending = new Map(inDeg);
  const work = [...queue];
  while (work.length) {
    const id = work.shift()!;
    topo.push(id);
    for (const next of adj.get(id) || []) {
      rank.set(next, Math.max(rank.get(next) || 0, (rank.get(id) || 0) + 1));
      const d = (pending.get(next) || 1) - 1;
      pending.set(next, d);
      if (d === 0) work.push(next);
    }
    work.sort((a, b) => compareRunNodeIds(a, b, nodeById));
  }
  for (const id of [...nodeIds].sort((a, b) => compareRunNodeIds(a, b, nodeById))) {
    if (topo.includes(id)) continue;
    rank.set(id, rank.get(id) || 0);
    topo.push(id);
  }

  // Group nodes by rank, order each layer by parent centroid.
  const layerMap = new Map<number, string[]>();
  for (const id of topo) {
    const r = rank.get(id) || 0;
    if (!layerMap.has(r)) layerMap.set(r, []);
    layerMap.get(r)!.push(id);
  }
  const sortedRanks = [...layerMap.keys()].sort((a, b) => a - b);
  const positions = new Map<string, LayoutNode>();
  for (const r of sortedRanks) {
    const ids = layerMap.get(r)!;
    if (r > 0) {
      ids.sort((a, b) => {
        const pa = radj.get(a) || [];
        const pb = radj.get(b) || [];
        const ax = pa.length ? pa.reduce((s, p) => s + (positions.get(p)?.x || 0), 0) / pa.length : 0;
        const bx = pb.length ? pb.reduce((s, p) => s + (positions.get(p)?.x || 0), 0) / pb.length : 0;
        if (ax !== bx) return ax - bx;
        return a.localeCompare(b);
      });
    }
    const ox = -(ids.length - 1) * COLUMN_SPACING / 2;
    for (let c = 0; c < ids.length; c++) {
      positions.set(ids[c], {
        id: ids[c],
        rank: r,
        column: c,
        height: NODE_SHAPE_DIMENSIONS.task.height,
        shape: 'task',
        width: NODE_SHAPE_DIMENSIONS.task.width,
        x: r * RANK_SPACING,
        y: ox + c * COLUMN_SPACING,
      });
    }
  }

  const rawYs = [...positions.values()].map(p => p.y);
  const minY = rawYs.length ? Math.min(...rawYs) : 0;
  if (minY !== 0) {
    for (const lnode of positions.values()) lnode.y -= minY;
  }

  const layers = sortedRanks.map(r => ({ rank: r, nodeIds: layerMap.get(r)! }));
  const rights = [...positions.values()].map(p => p.x + p.width / 2);
  const bottoms = [...positions.values()].map(p => p.y + p.height / 2);
  return {
    nodes: positions,
    layers,
    width: rights.length ? Math.max(...rights) : 0,
    height: bottoms.length ? Math.max(...bottoms) : 0,
  };
}

function weakComponents(graph: RunGraph): string[][] {
  const nodeById = new Map(graph.nodes.map((node) => [node.id, node]));
  const neighbors = new Map<string, string[]>();
  for (const node of graph.nodes) neighbors.set(node.id, []);
  for (const edge of graph.edges) {
    if (!neighbors.has(edge.from) || !neighbors.has(edge.to)) continue;
    neighbors.get(edge.from)!.push(edge.to);
    neighbors.get(edge.to)!.push(edge.from);
  }

  const seen = new Set<string>();
  const components: string[][] = [];
  const sortedNodeIds = graph.nodes
    .map((node) => node.id)
    .sort((a, b) => compareRunNodeIds(a, b, nodeById));

  for (const id of sortedNodeIds) {
    if (seen.has(id)) continue;
    const component: string[] = [];
    const work = [id];
    seen.add(id);
    while (work.length) {
      const current = work.shift()!;
      component.push(current);
      for (const next of [...(neighbors.get(current) || [])].sort((a, b) => compareRunNodeIds(a, b, nodeById))) {
        if (seen.has(next)) continue;
        seen.add(next);
        work.push(next);
      }
    }
    components.push(component.sort((a, b) => compareRunNodeIds(a, b, nodeById)));
  }

  return components;
}

function countRoots(graph: RunGraph): number {
  const inDeg = new Map(graph.nodes.map((node) => [node.id, 0]));
  for (const edge of graph.edges) {
    if (!inDeg.has(edge.from) || !inDeg.has(edge.to)) continue;
    inDeg.set(edge.to, (inDeg.get(edge.to) || 0) + 1);
  }
  return [...inDeg.values()].filter((degree) => degree === 0).length;
}

function layoutRootHeavyRunGraph(graph: RunGraph, components: string[][]): GraphLayout {
  const componentLayouts = components.map((nodeIds) => layoutLayeredRunNodes(graph, nodeIds));
  const columnCount = Math.max(1, Math.ceil(Math.sqrt(components.length)));
  const cellWidth = Math.max(...componentLayouts.map((layout) => layout.width), NODE_SHAPE_DIMENSIONS.task.width)
    + COMPONENT_COLUMN_GAP;
  const cellHeight = Math.max(...componentLayouts.map((layout) => layout.height), NODE_SHAPE_DIMENSIONS.task.height)
    + COMPONENT_ROW_GAP;
  const positions = new Map<string, LayoutNode>();

  componentLayouts.forEach((layout, index) => {
    const column = index % columnCount;
    const row = Math.floor(index / columnCount);
    const offsetX = column * cellWidth;
    const offsetY = row * cellHeight;
    for (const [nodeId, node] of layout.nodes) {
      positions.set(nodeId, {
        ...node,
        rank: column,
        column: row,
        x: node.x + offsetX,
        y: node.y + offsetY,
      });
    }
  });

  const rights = [...positions.values()].map(p => p.x + p.width / 2);
  const bottoms = [...positions.values()].map(p => p.y + p.height / 2);
  return {
    nodes: positions,
    layers: components.map((nodeIds, index) => ({ rank: index, nodeIds })),
    width: rights.length ? Math.max(...rights) : 0,
    height: bottoms.length ? Math.max(...bottoms) : 0,
  };
}

function layoutFromRunGraph(graph: RunGraph): GraphLayout {
  const empty: GraphLayout = { nodes: new Map(), layers: [], width: 0, height: 0 };
  if (!graph.nodes.length) return empty;

  const components = weakComponents(graph);
  const rootCount = countRoots(graph);
  const shouldCompact =
    components.length >= 4
    && rootCount >= 4
    && rootCount / graph.nodes.length >= 0.5;

  if (shouldCompact) return layoutRootHeavyRunGraph(graph, components);
  return layoutLayeredRunNodes(graph);
}

/**
 * Longest-running critical path through the DAG — the "dominant path" for
 * run view. Ties broken by node id for determinism.
 */
function computeCriticalPath(graph: RunGraph): string[] {
  if (!graph.nodes.length) return [];

  const adj = new Map<string, string[]>();
  const inDeg = new Map<string, number>();
  for (const n of graph.nodes) {
    adj.set(n.id, []);
    inDeg.set(n.id, 0);
  }
  for (const e of graph.edges) {
    if (!adj.has(e.from) || !adj.has(e.to)) continue;
    adj.get(e.from)!.push(e.to);
    inDeg.set(e.to, (inDeg.get(e.to) || 0) + 1);
  }

  // Topological order.
  const topo: string[] = [];
  const pending = new Map(inDeg);
  const queue = [...pending.entries()].filter(([, d]) => d === 0).map(([id]) => id);
  while (queue.length) {
    const id = queue.shift()!;
    topo.push(id);
    for (const next of adj.get(id) || []) {
      const d = (pending.get(next) || 1) - 1;
      pending.set(next, d);
      if (d === 0) queue.push(next);
    }
  }

  // Longest-path DP from sources. Tie-break by id to stay deterministic.
  const distance = new Map<string, number>();
  const predecessor = new Map<string, string | null>();
  for (const id of topo) {
    distance.set(id, 0);
    predecessor.set(id, null);
  }
  for (const u of topo) {
    for (const v of adj.get(u) || []) {
      const candidate = (distance.get(u) || 0) + 1;
      const current = distance.get(v) || 0;
      if (candidate > current || (candidate === current && (predecessor.get(v) || '') > u)) {
        distance.set(v, candidate);
        predecessor.set(v, u);
      }
    }
  }

  // Find the node with maximum distance; walk back.
  let tail: string | null = null;
  let best = -1;
  for (const [id, d] of distance) {
    if (d > best || (d === best && (tail === null || id < tail))) {
      best = d;
      tail = id;
    }
  }
  if (tail === null) return [];
  const path: string[] = [];
  let cursor: string | null = tail;
  while (cursor !== null) {
    path.push(cursor);
    cursor = predecessor.get(cursor) ?? null;
  }
  return path.reverse();
}

// --- Main presenter ---

export function presentRun(
  run: RunDetail | null,
  selectedJobId: string | null,
): MoonBuildViewModel {
  const emptyLayout: GraphLayout = { nodes: new Map(), layers: [], width: 0, height: 0 };
  const empty: MoonBuildViewModel = {
    nodes: [],
    edges: [],
    dominantPath: [],
    branchBoard: [],
    layout: emptyLayout,
    release: { readiness: 'draft', blockers: [], projectedJobs: [], checklist: [] },
    dockContent: null,
    selectedNode: null,
    activeNode: null,
    firstUnresolvedId: null,
    totalNodes: 0,
    resolvedNodes: 0,
    blockedNodes: 0,
    focusActive: false,
  };
  if (!run || !run.graph || !run.graph.nodes.length) return empty;

  const graph = run.graph;
  const layout = layoutFromRunGraph(graph);
  const criticalPath = computeCriticalPath(graph);
  const pathSet = new Set(criticalPath);
  const pathIndexMap = new Map(criticalPath.map((id, i) => [id, i]));
  const lineageEdges = graph.edges.map((e) => ({ from_node_id: e.from, to_node_id: e.to }));
  const lineage = computeLineage(selectedJobId, lineageEdges, graph.nodes.map((n) => n.id));
  const focusActive = lineage !== null;
  const isInLineage = (id: string) => !focusActive || lineage!.has(id);

  // Index jobs by label so we can attach duration/cost when rendering.
  const jobsByLabel = new Map<string, RunJob>();
  for (const job of run.jobs || []) {
    jobsByLabel.set(job.label, job);
  }

  // Outgoing-edge tally for the run-view nodes so sibling-aware fan styling
  // and the branch-pod affordance have the same metadata in live and build
  // views.
  const outgoingByNode = new Map<string, number>();
  for (const e of graph.edges) {
    outgoingByNode.set(e.from, (outgoingByNode.get(e.from) || 0) + 1);
  }

  const nodes: OrbitNode[] = graph.nodes.map((n) => {
    const pos = layout.nodes.get(n.id) || {
      x: 0,
      y: 0,
      rank: 0,
      width: NODE_SHAPE_DIMENSIONS.task.width,
      height: NODE_SHAPE_DIMENSIONS.task.height,
      shape: 'task' as const,
    };
    const ringState = jobStatusToRingState(n.status);
    const job = jobsByLabel.get(n.label) || jobsByLabel.get(n.id);
    const glyph = inferGlyph(n, job);
    const summary = summarizeNode(n, job);
    return {
      id: n.id,
      kind: 'step' as const,
      title: n.label || n.id,
      summary,
      shape: pos.shape,
      width: pos.width,
      height: pos.height,
      glyphType: glyph,
      ringState,
      isOnDominantPath: pathSet.has(n.id),
      issueCount: 0,
      route: n.adapter || undefined,
      dominantPathIndex: pathIndexMap.get(n.id) ?? -1,
      x: pos.x,
      y: pos.y,
      rank: pos.rank,
      multiplicity: null,
      taskType: n.task_type,
      description: n.description,
      outcomeGoal: n.outcome_goal,
      prompt: n.prompt,
      completionContract: n.completion_contract ?? null,
      outgoingEdgeCount: outgoingByNode.get(n.id) || 0,
      inLineage: isInLineage(n.id),
    };
  });

  // Sibling fan metadata — matches the build presenter so MoonEdges can
  // thin strokes identically for live runs with branching.
  const siblingsBySource = new Map<string, string[]>();
  for (const e of graph.edges) {
    const bucket = siblingsBySource.get(e.from) || [];
    bucket.push(e.id);
    siblingsBySource.set(e.from, bucket);
  }
  const siblingMetaById = new Map<string, { index: number; count: number }>();
  for (const bucket of siblingsBySource.values()) {
    for (let i = 0; i < bucket.length; i++) {
      siblingMetaById.set(bucket[i], { index: i, count: bucket.length });
    }
  }

  const edges: OrbitEdge[] = graph.edges.map((e) => {
    const sm = siblingMetaById.get(e.id) || { index: 0, count: 1 };
    const gateFamily = runEdgeGateFamily(e);
    const gateConfig = runEdgeGateConfig(e);
    return {
      id: e.id,
      from: e.from,
      to: e.to,
      kind: e.type || 'sequence',
      isOnDominantPath: pathSet.has(e.from) && pathSet.has(e.to),
      gateState: gateFamily && gateFamily !== 'after_success' ? 'configured' as const : 'empty' as const,
      gateLabel: runEdgeGateLabel(e, gateFamily),
      gateFamily: gateFamily && gateFamily !== 'after_success' ? gateFamily : undefined,
      branchReason: typeof e.condition?.branch === 'string' ? e.condition.branch : undefined,
      gateConfig,
      siblingCount: sm.count,
      siblingIndex: sm.index,
      inLineage: isInLineage(e.from) && isInLineage(e.to),
    };
  });

  const selectedNode = selectedJobId
    ? nodes.find((n) => n.id === selectedJobId) ?? null
    : null;

  // First non-terminal node — the "active" focus for centering / highlight.
  const activeNode =
    nodes.find((n) => n.ringState === 'run-active') ||
    nodes.find((n) => n.ringState === 'run-pending') ||
    null;

  const totalNodes = nodes.length;
  const resolvedNodes = nodes.filter((n) => n.ringState === 'run-succeeded').length;
  const blockedNodes = nodes.filter((n) => n.ringState === 'run-failed').length;

  return {
    nodes,
    edges,
    dominantPath: criticalPath,
    branchBoard: [],
    layout,
    release: runReleaseStatus(run),
    dockContent: null,
    selectedNode,
    activeNode,
    firstUnresolvedId: activeNode?.id ?? null,
    totalNodes,
    resolvedNodes,
    blockedNodes,
    focusActive,
  };
}

function summarizeCompletionContract(node: RunGraphNode): string | null {
  const contract = node.completion_contract;
  if (!contract) return null;
  const resultKind = typeof contract.result_kind === 'string' && contract.result_kind.trim()
    ? contract.result_kind.trim()
    : 'result';
  const tools = Array.isArray(contract.submit_tool_names)
    ? contract.submit_tool_names.filter((tool): tool is string => typeof tool === 'string' && tool.trim().length > 0)
    : [];
  if (contract.submission_required) {
    return tools.length
      ? `submit ${resultKind} via ${tools.join(', ')}`
      : `submit ${resultKind}`;
  }
  if (contract.verification_required) return `verify ${resultKind}`;
  return resultKind;
}

function summarizeNode(node: RunGraphNode, job: RunJob | undefined): string {
  const parts: string[] = [];
  if (node.agent || job?.agent_slug) parts.push(node.agent || job!.agent_slug!);
  const contractSummary = summarizeCompletionContract(node);
  if (contractSummary) parts.push(contractSummary);
  if (node.duration_ms || job?.duration_ms) {
    const ms = node.duration_ms ?? job!.duration_ms!;
    parts.push(`${(ms / 1000).toFixed(1)}s`);
  }
  if (typeof node.cost_usd === 'number' && node.cost_usd > 0) {
    parts.push(`$${node.cost_usd.toFixed(2)}`);
  }
  return parts.join(' · ');
}

function runEdgeGateFamily(edge: RunGraphEdge): string | undefined {
  if (edge.condition) return 'conditional';
  const type = (edge.type || '').trim();
  if (type === 'conditional' || type === 'after_failure' || type === 'after_any' || type === 'after_success') {
    return type;
  }
  return undefined;
}

function runEdgeGateLabel(edge: RunGraphEdge, family: string | undefined): string | undefined {
  switch (family) {
    case 'conditional':
      return typeof edge.condition?.branch === 'string' && edge.condition.branch.trim()
        ? edge.condition.branch.trim()
        : 'Condition';
    case 'after_failure':
      return 'On failure';
    case 'after_any':
      return 'On any';
    case 'after_success':
      return 'On success';
    default:
      return undefined;
  }
}

function runEdgeGateConfig(edge: RunGraphEdge): Record<string, unknown> | undefined {
  const config: Record<string, unknown> = {};
  if (edge.condition) config.condition = edge.condition;
  if (edge.data_mapping && Object.keys(edge.data_mapping).length > 0) {
    config.data_mapping = edge.data_mapping;
  }
  return Object.keys(config).length > 0 ? config : undefined;
}

function runReleaseStatus(run: RunDetail): MoonBuildViewModel['release'] {
  // Abuse ReleaseStatus as a compact run-status surface for the action dock.
  // readiness: 'ready' = succeeded, 'blocked' = failed, 'draft' = running.
  const readiness: 'ready' | 'blocked' | 'draft' =
    run.status === 'succeeded' ? 'ready' :
    run.status === 'failed' || run.status === 'cancelled' ? 'blocked' :
    'draft';
  return {
    readiness,
    blockers: [],
    projectedJobs: [],
    checklist: [],
  };
}
