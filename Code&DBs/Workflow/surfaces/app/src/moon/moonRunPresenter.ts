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
import { RANK_SPACING, COLUMN_SPACING, glyphFromLabel } from './moonBuildPresenter';

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

function layoutFromRunGraph(graph: RunGraph): GraphLayout {
  const empty: GraphLayout = { nodes: new Map(), layers: [], width: 0, height: 0 };
  if (!graph.nodes.length) return empty;

  const adj = new Map<string, string[]>();
  const radj = new Map<string, string[]>();
  const inDeg = new Map<string, number>();
  for (const n of graph.nodes) {
    adj.set(n.id, []);
    radj.set(n.id, []);
    inDeg.set(n.id, 0);
  }
  for (const e of graph.edges) {
    if (!adj.has(e.from) || !adj.has(e.to)) continue;
    adj.get(e.from)!.push(e.to);
    radj.get(e.to)!.push(e.from);
    inDeg.set(e.to, (inDeg.get(e.to) || 0) + 1);
  }

  // Assign rank = longest path from any root.
  const rank = new Map<string, number>();
  const queue = [...inDeg.entries()].filter(([, d]) => d === 0).map(([id]) => id);
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
      positions.set(ids[c], { id: ids[c], rank: r, column: c, x: r * RANK_SPACING, y: ox + c * COLUMN_SPACING });
    }
  }

  const layers = sortedRanks.map(r => ({ rank: r, nodeIds: layerMap.get(r)! }));
  const xs = [...positions.values()].map(p => p.x);
  const ys = [...positions.values()].map(p => p.y);
  return {
    nodes: positions,
    layers,
    width: xs.length ? Math.max(...xs) + RANK_SPACING : 0,
    height: ys.length ? Math.max(...ys) - Math.min(...ys) + COLUMN_SPACING : 0,
  };
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
    layout: emptyLayout,
    release: { readiness: 'draft', blockers: [], projectedJobs: [], checklist: [] },
    dockContent: null,
    selectedNode: null,
    activeNode: null,
    firstUnresolvedId: null,
    totalNodes: 0,
    resolvedNodes: 0,
    blockedNodes: 0,
  };
  if (!run || !run.graph || !run.graph.nodes.length) return empty;

  const graph = run.graph;
  const layout = layoutFromRunGraph(graph);
  const criticalPath = computeCriticalPath(graph);
  const pathSet = new Set(criticalPath);
  const pathIndexMap = new Map(criticalPath.map((id, i) => [id, i]));

  // Index jobs by label so we can attach duration/cost when rendering.
  const jobsByLabel = new Map<string, RunJob>();
  for (const job of run.jobs || []) {
    jobsByLabel.set(job.label, job);
  }

  const nodes: OrbitNode[] = graph.nodes.map((n) => {
    const pos = layout.nodes.get(n.id) || { x: 0, y: 0, rank: 0 };
    const ringState = jobStatusToRingState(n.status);
    const job = jobsByLabel.get(n.label) || jobsByLabel.get(n.id);
    const glyph = inferGlyph(n, job);
    const summary = summarizeNode(n, job);
    return {
      id: n.id,
      kind: 'step' as const,
      title: n.label || n.id,
      summary,
      glyphType: glyph,
      ringState,
      isOnDominantPath: pathSet.has(n.id),
      issueCount: 0,
      route: n.adapter || undefined,
      dominantPathIndex: pathIndexMap.get(n.id) ?? -1,
      x: pos.x,
      y: pos.y,
      rank: pos.rank,
    };
  });

  const edges: OrbitEdge[] = graph.edges.map((e) => ({
    id: e.id,
    from: e.from,
    to: e.to,
    kind: e.type || 'sequence',
    isOnDominantPath: pathSet.has(e.from) && pathSet.has(e.to),
    gateState: 'empty' as const,
  }));

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
    layout,
    release: runReleaseStatus(run),
    dockContent: null,
    selectedNode,
    activeNode,
    firstUnresolvedId: activeNode?.id ?? null,
    totalNodes,
    resolvedNodes,
    blockedNodes,
  };
}

function summarizeNode(node: RunGraphNode, job: RunJob | undefined): string {
  const parts: string[] = [];
  if (node.agent || job?.agent_slug) parts.push(node.agent || job!.agent_slug!);
  if (node.duration_ms || job?.duration_ms) {
    const ms = node.duration_ms ?? job!.duration_ms!;
    parts.push(`${(ms / 1000).toFixed(1)}s`);
  }
  if (typeof node.cost_usd === 'number' && node.cost_usd > 0) {
    parts.push(`$${node.cost_usd.toFixed(2)}`);
  }
  return parts.join(' · ');
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
