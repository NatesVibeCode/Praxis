// Pure presenter: BuildPayload -> MoonBuildViewModel
// No React, no fetch, no DOM. Just data transformation.

import type {
  BuildPayload, BuildNode, BuildEdge, BuildIssue,
  AuthorityAttachment, BindingLedgerEntry, ImportSnapshot,
} from '../shared/types';
import { branchLabel, normalizeBuildEdgeRelease } from '../shared/edgeRelease';

// --- View model types ---

export type GlyphType =
  | 'step' | 'gate' | 'state' | 'trigger' | 'human' | 'binding'
  | 'research' | 'classify' | 'draft' | 'notify' | 'review' | 'tool' | 'blocked'
  | 'analyze' | 'decompose' | 'diff' | 'chat' | 'spec' | 'build' | 'test'
  | 'deploy' | 'data' | 'metric' | 'render' | 'adversarial' | 'validate'
  | 'idea' | 'summary' | 'webhook' | 'schedule';

/**
 * Type vocabulary: a *token* (word-boundary match) found in a label maps
 * to exactly one glyph. Jobs sharing a type token share a glyph; jobs with
 * no recognised token fall back to 'step'.
 *
 * Keep this list small and deliberate. Don't add a type unless the glyph
 * genuinely means something about the job's role, not just a word that
 * happens to appear in a label.
 */
const TYPE_TOKEN_TO_GLYPH: Record<string, GlyphType> = {
  analyze: 'analyze', analyse: 'analyze', inspect: 'analyze', audit: 'analyze',
  decompose: 'decompose', fanout: 'decompose', loop: 'decompose', split: 'decompose',
  propose: 'idea', suggest: 'idea', brainstorm: 'idea',
  adversarial: 'adversarial', redteam: 'adversarial', attack: 'adversarial', debate: 'adversarial',
  diff: 'diff', compare: 'diff', contrast: 'diff',
  review: 'review', approve: 'review',
  validate: 'validate', validation: 'validate', verify: 'validate',
  test: 'test', assert: 'test',
  build: 'build', compile: 'build',
  deploy: 'deploy', release: 'deploy', ship: 'deploy', cutover: 'deploy',
  chat: 'chat', message: 'chat',
  spec: 'spec', schema: 'spec', blueprint: 'spec',
  render: 'render', markdown: 'render',
  data: 'data', dataset: 'data', ingest: 'data', fetch: 'data',
  metric: 'metric', measure: 'metric', score: 'metric',
  research: 'research',
  classify: 'classify', triage: 'classify',
  draft: 'draft', write: 'draft',
  notify: 'notify', alert: 'notify', email: 'notify',
  trigger: 'trigger',
  human: 'human',
  tool: 'tool', api: 'tool',
  webhook: 'webhook', hook: 'webhook',
  schedule: 'schedule', cron: 'schedule', clock: 'schedule', timer: 'schedule',
  summary: 'summary', report: 'summary', record: 'summary', register: 'summary',
};

/**
 * Tokenize a label by non-word separators and return the first known type
 * glyph, or null if no token matches the vocabulary.
 */
export function glyphFromLabel(label: string): GlyphType | null {
  const tokens = label.toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
  for (const t of tokens) {
    const g = TYPE_TOKEN_TO_GLYPH[t];
    if (g) return g;
  }
  return null;
}

// 6-state circle model
export type RingState =
  | 'unresolved'          // hollow white ring, no icon
  | 'active-unresolved'   // hollow white ring + blue glow
  | 'decided-incomplete'  // blue fill + icon + badge
  | 'decided-grounded'    // blue fill + icon, fully resolved
  | 'blocked'             // amber/red rim accent
  | 'projected'           // smaller dashed ring
  | 'run-pending'         // grey ring — queued
  | 'run-active'          // blue ring + pulse animation
  | 'run-succeeded'       // green ring
  | 'run-failed';         // red ring

export type GateState = 'empty' | 'proposed' | 'configured' | 'blocked' | 'passed';

// --- Graph layout ---

export const RANK_SPACING = 200;   // horizontal: distance between steps
export const COLUMN_SPACING = 168; // vertical: distance between parallel branches — needs to fit 2-line labels between sibling rings

export interface LayoutNode {
  id: string;
  rank: number;
  column: number;
  x: number;
  y: number;
}

export interface GraphLayout {
  nodes: Map<string, LayoutNode>;
  layers: { rank: number; nodeIds: string[] }[];
  width: number;
  height: number;
}

/**
 * Multiplicity captures "this single spec node runs more than once" in two
 * visually distinct shapes:
 *   - loop: N runs in sequence (time axis). Node silhouette = diagonal stack.
 *   - parallel: N runs concurrently (space axis). Node silhouette = vertical
 *     stack.
 * Count is the iteration/worker count when it can be derived from
 * integration_args, or null when the spec leaves it dynamic.
 */
export interface NodeMultiplicity {
  kind: 'loop' | 'parallel';
  count: number | null;
}

export interface NodeCompletionContract {
  result_kind?: string;
  submit_tool_names?: string[];
  submission_required?: boolean;
  verification_required?: boolean;
  [key: string]: unknown;
}

export interface OrbitNode {
  id: string;
  kind: 'step' | 'gate' | 'state';
  title: string;
  summary: string;
  glyphType: GlyphType;
  ringState: RingState;
  isOnDominantPath: boolean;
  issueCount: number;
  route?: string;
  dominantPathIndex: number;
  x: number;
  y: number;
  rank: number;
  multiplicity: NodeMultiplicity | null;
  taskType?: string;
  description?: string;
  outcomeGoal?: string;
  prompt?: string;
  completionContract?: NodeCompletionContract | null;
  /** Number of outgoing edges that leave this node in the graph. */
  outgoingEdgeCount: number;
  /**
   * True when a node is selected AND this node lies on its lineage
   * (ancestors + self + descendants). When no node is selected, every
   * OrbitNode carries inLineage=true so rendering stays at the rest state.
   * Drives the focus-lineage presenter: non-lineage nodes dim to ghost.
   */
  inLineage: boolean;
}

export interface OrbitEdge {
  id: string;
  from: string;
  to: string;
  kind: string;
  isOnDominantPath: boolean;
  gateState: GateState;
  gateLabel?: string;
  gateFamily?: string;
  branchReason?: string;
  gateConfig?: Record<string, unknown>;
  /**
   * Total number of outgoing edges that share this edge's source node.
   * When >= 2 the canvas treats these edges as a single fan bundle and can
   * thin the strokes / render a fan-pivot glyph.
   */
  siblingCount: number;
  /**
   * Zero-based index of this edge among its source's outgoing edges, sorted
   * by destination y-position. Used so styling (e.g. mid-fan thinning) can
   * tell the outermost and innermost blades apart.
   */
  siblingIndex: number;
  /**
   * True when a node is selected AND both endpoints are on the selection's
   * lineage. When no node is selected, every edge carries inLineage=true.
   * Drives stroke opacity and gate-label reveal for the focus-lineage
   * presenter.
   */
  inLineage: boolean;
}

export interface DockContent {
  contextAttachments: AuthorityAttachment[];
  connectBindings: BindingLedgerEntry[];
  imports: ImportSnapshot[];
}

export interface ReadinessItem {
  label: string;
  passed: boolean;
  nodeId?: string;
  dock?: 'action' | 'context';
  message: string;
}

export interface ReleaseStatus {
  readiness: 'ready' | 'blocked' | 'draft';
  blockers: { message: string; severity: string; nodeIds: string[] }[];
  projectedJobs: { label: string; agent: string }[];
  checklist: ReadinessItem[];
}

export interface MoonBuildViewModel {
  nodes: OrbitNode[];
  edges: OrbitEdge[];
  dominantPath: string[];
  layout: GraphLayout;
  release: ReleaseStatus;
  dockContent: DockContent | null;
  selectedNode: OrbitNode | null;
  activeNode: OrbitNode | null;
  firstUnresolvedId: string | null;
  totalNodes: number;
  resolvedNodes: number;
  blockedNodes: number;
  /**
   * True when a user-selected node is active and has produced a non-empty
   * lineage. Lets renderers tell "nothing selected → full rest state" from
   * "selection active → dim anything outside inLineage". Mirrors the
   * selectedNodeId input: stays false when the caller passes null.
   */
  focusActive: boolean;
}

function branchSideScore(edge: BuildEdge): number {
  const release = normalizeBuildEdgeRelease(edge);
  const side = typeof release.config?.branch_side === 'string'
    ? release.config.branch_side.trim().toLowerCase()
    : '';
  if (side === 'above') return -1;
  if (side === 'below') return 1;
  if (release.branch_reason === 'then') return -1;
  if (release.branch_reason === 'else') return 1;
  return 0;
}

// --- Helpers ---

const ROUTE_TO_GLYPH: Record<string, GlyphType> = {
  'trigger': 'trigger',
  'trigger/webhook': 'webhook',
  'trigger/schedule': 'schedule',
};

/**
 * Route strings that map to each multiplicity kind. Loop runs N iterations in
 * sequence; fanout runs N workers in parallel. Both are represented as node
 * routes in the workflow spec — the visual silhouette derives from this
 * mapping, not from spec shape.
 */
const LOOP_ROUTES = new Set(['workflow.loop']);
const PARALLEL_ROUTES = new Set(['workflow.fanout']);

const LOOP_COUNT_KEYS = ['iterations', 'count', 'item_count', 'items'] as const;
const PARALLEL_COUNT_KEYS = ['count', 'parallelism', 'worker_count', 'workers', 'fanout'] as const;

function coerceCount(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
    return Math.floor(value);
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    if (Number.isFinite(parsed) && parsed > 0) return Math.floor(parsed);
  }
  if (Array.isArray(value)) return value.length > 0 ? value.length : null;
  return null;
}

function readCount(args: Record<string, unknown> | undefined, keys: readonly string[]): number | null {
  if (!args) return null;
  for (const key of keys) {
    const count = coerceCount(args[key]);
    if (count !== null) return count;
  }
  return null;
}

function nodeToMultiplicity(node: BuildNode): NodeMultiplicity | null {
  const route = (node.route || '').trim();
  if (!route) return null;
  const args = (node.integration_args as Record<string, unknown> | undefined) || undefined;
  if (LOOP_ROUTES.has(route)) {
    return { kind: 'loop', count: readCount(args, LOOP_COUNT_KEYS) };
  }
  if (PARALLEL_ROUTES.has(route)) {
    return { kind: 'parallel', count: readCount(args, PARALLEL_COUNT_KEYS) };
  }
  return null;
}

function nodeToGlyph(node: BuildNode): GlyphType {
  if (node.kind === 'gate') return 'gate';
  if (node.kind === 'state') return 'state';
  const status = (node.status || '').toLowerCase();
  if (status === 'blocked' || status === 'error') return 'blocked';
  const route = (node.route || '').toLowerCase();
  if (ROUTE_TO_GLYPH[route]) return ROUTE_TO_GLYPH[route];
  const label = ((node as { label?: string; title?: string }).label || (node as { title?: string }).title || '').toLowerCase();
  return glyphFromLabel(`${route} ${label}`) ?? 'step';
}

function isNodeDecided(node: BuildNode): boolean {
  const route = (node.route || '').trim();
  return route.length > 0;
}

function nodeNeedsBadge(node: BuildNode, payload: BuildPayload): boolean {
  if (!isNodeDecided(node)) return false;
  const nodeIssueIds = new Set(node.issue_ids || []);
  if (nodeIssueIds.size > 0) return true;
  if ((payload.build_issues || []).some(issue => issue.node_id === node.node_id || (issue.issue_id && nodeIssueIds.has(issue.issue_id)))) {
    return true;
  }
  // Badge if missing attachments or has unresolved bindings
  const attachments = (payload.authority_attachments || []).filter(a => a.node_id === node.node_id);
  if (attachments.length === 0) return true;
  const bindingIds = new Set(node.binding_ids || []);
  const bindings = (payload.binding_ledger || []).filter(b => bindingIds.has(b.binding_id));
  if (bindings.some(b => b.state !== 'accepted')) return true;
  return false;
}

function nodeHasBlockers(node: BuildNode, payload: BuildPayload): boolean {
  const nodeIssueIds = new Set(node.issue_ids || []);
  if ((payload.build_blockers || []).some(issue => issue.node_id === node.node_id || (issue.issue_id && nodeIssueIds.has(issue.issue_id)))) {
    return true;
  }
  return false;
}

function nodeToRingState(node: BuildNode, payload: BuildPayload, activeId: string | null): RingState {
  const status = (node.status || '').toLowerCase();
  const decided = isNodeDecided(node);
  const blocked = status === 'blocked' || status === 'error' || nodeHasBlockers(node, payload);
  if (blocked) return 'blocked';
  if (!decided && (status === 'draft' || status === 'pending')) return 'projected';
  if (!decided) {
    return node.node_id === activeId ? 'active-unresolved' : 'unresolved';
  }
  return nodeNeedsBadge(node, payload) ? 'decided-incomplete' : 'decided-grounded';
}

function runStatusToRingState(status: string | undefined): RingState | null {
  const jobStatus = (status || '').toLowerCase();
  if (jobStatus === 'succeeded') return 'run-succeeded';
  if (
    jobStatus === 'failed'
    || jobStatus === 'dead_letter'
    || jobStatus === 'blocked'
    || jobStatus === 'cancelled'
    || jobStatus === 'parent_failed'
  ) return 'run-failed';
  if (jobStatus === 'running' || jobStatus === 'claimed') return 'run-active';
  if (jobStatus === 'pending' || jobStatus === 'ready') return 'run-pending';
  return null;
}

function extractDominantPath(payload: BuildPayload): string[] {
  // Prefer compiled spec job ordering -> node mapping
  const spec = payload.compiled_spec_projection?.compiled_spec;
  if (spec?.jobs?.length) {
    return spec.jobs
      .map(j => j.source_node_id || j.source_step_id || '')
      .filter(Boolean);
  }
  // Fallback: topological order from edges
  const nodes = payload.build_graph?.nodes || [];
  const edges = payload.build_graph?.edges || [];
  if (!nodes.length) return [];

  const inDegree = new Map<string, number>();
  const adj = new Map<string, string[]>();
  for (const n of nodes) {
    inDegree.set(n.node_id, 0);
    adj.set(n.node_id, []);
  }
  for (const e of edges) {
    adj.get(e.from_node_id)?.push(e.to_node_id);
    inDegree.set(e.to_node_id, (inDegree.get(e.to_node_id) || 0) + 1);
  }
  const queue = [...inDegree.entries()].filter(([, d]) => d === 0).map(([id]) => id);
  const order: string[] = [];
  while (queue.length) {
    const id = queue.shift()!;
    order.push(id);
    for (const next of adj.get(id) || []) {
      const d = (inDegree.get(next) || 1) - 1;
      inDegree.set(next, d);
      if (d === 0) queue.push(next);
    }
  }
  return order;
}

export function extractLayout(payload: BuildPayload): GraphLayout {
  const nodes = payload.build_graph?.nodes || [];
  const edges = payload.build_graph?.edges || [];
  const empty: GraphLayout = { nodes: new Map(), layers: [], width: 0, height: 0 };
  if (!nodes.length) return empty;

  const adj = new Map<string, string[]>();
  const radj = new Map<string, string[]>();
  const inDeg = new Map<string, number>();
  for (const n of nodes) {
    adj.set(n.node_id, []);
    radj.set(n.node_id, []);
    inDeg.set(n.node_id, 0);
  }
  for (const e of edges) {
    adj.get(e.from_node_id)?.push(e.to_node_id);
    radj.get(e.to_node_id)?.push(e.from_node_id);
    inDeg.set(e.to_node_id, (inDeg.get(e.to_node_id) || 0) + 1);
  }
  const branchBiasByNode = new Map<string, number>();
  for (const edge of edges) {
    const score = branchSideScore(edge);
    if (score !== 0) branchBiasByNode.set(edge.to_node_id, score);
  }

  // Assign rank = longest path from any root
  const rank = new Map<string, number>();
  const queue = [...inDeg.entries()].filter(([, d]) => d === 0).map(([id]) => id);
  for (const id of queue) rank.set(id, 0);
  const topo: string[] = [];
  while (queue.length) {
    const id = queue.shift()!;
    topo.push(id);
    for (const next of adj.get(id) || []) {
      rank.set(next, Math.max(rank.get(next) || 0, (rank.get(id) || 0) + 1));
      const d = (inDeg.get(next) || 1) - 1;
      inDeg.set(next, d);
      if (d === 0) queue.push(next);
    }
  }

  // Group by rank, order within each layer by parent centroid
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
        const aBias = branchBiasByNode.get(a) || 0;
        const bBias = branchBiasByNode.get(b) || 0;
        if (aBias !== bBias) return aBias - bBias;
        return a.localeCompare(b);
      });
    }
    const ox = -(ids.length - 1) * COLUMN_SPACING / 2;
    for (let c = 0; c < ids.length; c++) {
      positions.set(ids[c], { id: ids[c], rank: r, column: c, x: r * RANK_SPACING, y: ox + c * COLUMN_SPACING });
    }
  }

  // Per-layer positions are centered around y=0 for visual balance, which
  // leaves top-column nodes at negative y. The canvas container starts at
  // y=0, so negative-y nodes would overflow its top edge and clip. Shift
  // every position down by minY so the topmost node lands at y=0 while
  // relative spacing stays intact.
  const rawYs = [...positions.values()].map(p => p.y);
  const minY = rawYs.length ? Math.min(...rawYs) : 0;
  if (minY !== 0) {
    for (const lnode of positions.values()) lnode.y -= minY;
  }

  const layers = sortedRanks.map(r => ({ rank: r, nodeIds: layerMap.get(r)! }));
  const xs = [...positions.values()].map(p => p.x);
  const ys = [...positions.values()].map(p => p.y);
  return {
    nodes: positions,
    layers,
    width: xs.length ? Math.max(...xs) + RANK_SPACING : 0,
    height: ys.length ? Math.max(...ys) + COLUMN_SPACING : 0,
  };
}

/**
 * Selection-driven lineage: the set of nodes that are ancestors of the
 * selected node, the selected node itself, and its descendants. Drives the
 * focus-lineage presenter by letting the canvas dim everything outside.
 *
 * Returns null when selectedId is null or not present in the graph — callers
 * should treat null as "no focus, render all nodes/edges at rest state".
 */
export function computeLineage(
  selectedId: string | null,
  edges: readonly Pick<BuildEdge, 'from_node_id' | 'to_node_id'>[],
  nodeIds: readonly string[],
): Set<string> | null {
  if (!selectedId) return null;
  const nodeIdSet = new Set(nodeIds);
  if (!nodeIdSet.has(selectedId)) return null;

  const forwardAdj = new Map<string, string[]>();
  const backwardAdj = new Map<string, string[]>();
  for (const id of nodeIds) {
    forwardAdj.set(id, []);
    backwardAdj.set(id, []);
  }
  for (const e of edges) {
    forwardAdj.get(e.from_node_id)?.push(e.to_node_id);
    backwardAdj.get(e.to_node_id)?.push(e.from_node_id);
  }

  const lineage = new Set<string>([selectedId]);
  const walk = (adj: Map<string, string[]>) => {
    const stack = [selectedId];
    while (stack.length) {
      const id = stack.pop()!;
      for (const next of adj.get(id) || []) {
        if (!lineage.has(next)) {
          lineage.add(next);
          stack.push(next);
        }
      }
    }
  };
  walk(forwardAdj);
  walk(backwardAdj);
  return lineage;
}

// --- Main presenter ---

export interface RunJobStatus {
  label: string;
  status: string;
}

export function presentBuild(
  payload: BuildPayload | null,
  selectedNodeId: string | null,
  activeNodeId: string | null,
  runJobs?: RunJobStatus[],
): MoonBuildViewModel {
  const emptyLayout: GraphLayout = { nodes: new Map(), layers: [], width: 0, height: 0 };
  const empty: MoonBuildViewModel = {
    nodes: [], edges: [], dominantPath: [], layout: emptyLayout,
    release: { readiness: 'draft', blockers: [], projectedJobs: [], checklist: [] },
    dockContent: null, selectedNode: null, activeNode: null, firstUnresolvedId: null,
    totalNodes: 0, resolvedNodes: 0, blockedNodes: 0, focusActive: false,
  };
  if (!payload) return empty;

  const rawNodes = payload.build_graph?.nodes || [];
  const rawEdges = payload.build_graph?.edges || [];
  const issues = payload.build_issues || [];
  const dominantPath = extractDominantPath(payload);
  const layout = extractLayout(payload);
  const pathSet = new Set(dominantPath);
  const pathIndexMap = new Map(dominantPath.map((id, i) => [id, i]));
  const lineage = computeLineage(selectedNodeId, rawEdges, rawNodes.map(n => n.node_id));
  const focusActive = lineage !== null;
  const isInLineage = (id: string) => !focusActive || lineage!.has(id);

  // Build run status overlay: match job labels to node titles
  const runStatusByTitle = new Map<string, string>();
  if (runJobs?.length) {
    for (const j of runJobs) {
      runStatusByTitle.set(j.label.toLowerCase(), j.status);
    }
  }

  // Position dominant path nodes along spine, others as satellites
  const pathNodes = dominantPath.map(id => rawNodes.find(n => n.node_id === id)).filter(Boolean) as BuildNode[];
  const otherNodes = rawNodes.filter(n => !pathSet.has(n.node_id));
  const allOrdered = [...pathNodes, ...otherNodes];

  const issuesByNode = new Map<string, number>();
  for (const issue of issues) {
    if (issue.node_id) issuesByNode.set(issue.node_id, (issuesByNode.get(issue.node_id) || 0) + 1);
  }

  // Tally outgoing edges per source so a node knows how many branches it has.
  const outgoingByNode = new Map<string, number>();
  for (const e of rawEdges) {
    outgoingByNode.set(e.from_node_id, (outgoingByNode.get(e.from_node_id) || 0) + 1);
  }

  const nodes: OrbitNode[] = allOrdered.map((n) => {
    let ring = nodeToRingState(n, payload, activeNodeId);

    // Override with run status when a run is active
    ring = runStatusToRingState(runStatusByTitle.get((n.title || n.node_id).toLowerCase())) || ring;

    return {
      id: n.node_id,
      kind: n.kind,
      title: (n.title || n.node_id).replace(/\.\s*Step$/i, '').trim(),
      summary: n.summary || '',
      glyphType: nodeToGlyph(n),
      ringState: ring,
      isOnDominantPath: pathSet.has(n.node_id),
      issueCount: issuesByNode.get(n.node_id) || 0,
      route: n.route,
      dominantPathIndex: pathIndexMap.get(n.node_id) ?? -1,
      x: layout.nodes.get(n.node_id)?.x ?? 0,
      y: layout.nodes.get(n.node_id)?.y ?? 0,
      rank: layout.nodes.get(n.node_id)?.rank ?? 0,
      multiplicity: nodeToMultiplicity(n),
      outgoingEdgeCount: outgoingByNode.get(n.node_id) || 0,
      inLineage: isInLineage(n.node_id),
    };
  });

  // Precompute sibling counts + indices so the edge list can report
  // fan-position metadata to the renderer without a second pass downstream.
  const siblingsBySource = new Map<string, string[]>();
  for (const e of rawEdges) {
    const bucket = siblingsBySource.get(e.from_node_id) || [];
    bucket.push(e.edge_id);
    siblingsBySource.set(e.from_node_id, bucket);
  }
  // Sort each bucket by the destination y so sibling order matches the visual
  // top-to-bottom ordering of the fanned edges.
  for (const bucket of siblingsBySource.values()) {
    bucket.sort((a, b) => {
      const ea = rawEdges.find(e => e.edge_id === a);
      const eb = rawEdges.find(e => e.edge_id === b);
      const ya = ea ? (layout.nodes.get(ea.to_node_id)?.y ?? 0) : 0;
      const yb = eb ? (layout.nodes.get(eb.to_node_id)?.y ?? 0) : 0;
      return ya - yb;
    });
  }
  const siblingIndexById = new Map<string, { index: number; count: number }>();
  for (const bucket of siblingsBySource.values()) {
    for (let i = 0; i < bucket.length; i++) {
      siblingIndexById.set(bucket[i], { index: i, count: bucket.length });
    }
  }

  // Gate state on edges
  const edges: OrbitEdge[] = rawEdges.map(e => {
    const release = normalizeBuildEdgeRelease(e);
    let gateState: GateState = 'empty';
    if (release.family !== 'after_success') {
      const gs = (release.state || '').toLowerCase();
      if (gs === 'passed') gateState = 'passed';
      else if (gs === 'blocked') gateState = 'blocked';
      else if (gs === 'configured' || release.family) gateState = 'configured';
      else gateState = 'proposed';
    }
    const siblingMeta = siblingIndexById.get(e.edge_id) || { index: 0, count: 1 };
    return {
      id: e.edge_id,
      from: e.from_node_id,
      to: e.to_node_id,
      kind: e.kind,
      isOnDominantPath: pathSet.has(e.from_node_id) && pathSet.has(e.to_node_id),
      gateState,
      gateLabel: release.label || branchLabel(release.branch_reason),
      gateFamily: release.family !== 'after_success' ? release.family : undefined,
      branchReason: release.branch_reason,
      gateConfig: release.config ? { ...release.config as Record<string, unknown> } : undefined,
      siblingCount: siblingMeta.count,
      siblingIndex: siblingMeta.index,
      inLineage: isInLineage(e.from_node_id) && isInLineage(e.to_node_id),
    };
  });

  // First unresolved on dominant path
  const firstUnresolvedId = nodes.find(
    n => n.isOnDominantPath && (n.ringState === 'unresolved' || n.ringState === 'active-unresolved')
  )?.id ?? null;

  // Dock content for selected node
  let dockContent: DockContent | null = null;
  const selectedNode = nodes.find(n => n.id === selectedNodeId) || null;
  const activeNode = nodes.find(n => n.id === activeNodeId) || null;
  if (selectedNodeId) {
    const attachments = (payload.authority_attachments || []).filter(a => a.node_id === selectedNodeId);
    const rawNode = rawNodes.find(n => n.node_id === selectedNodeId);
    const bindingIds = new Set(rawNode?.binding_ids || []);
    const bindings = (payload.binding_ledger || []).filter(b => bindingIds.has(b.binding_id));
    const imports = (payload.import_snapshots || []).filter(s => s.node_id === selectedNodeId);

    dockContent = { contextAttachments: attachments, connectBindings: bindings, imports };
  }

  // Release
  const projectionState = typeof payload.projection_status?.state === 'string'
    ? payload.projection_status.state.toLowerCase()
    : '';
  const buildState = (projectionState || payload.build_state || 'draft').toLowerCase();
  const readiness: ReleaseStatus['readiness'] = buildState === 'ready' ? 'ready' : buildState === 'blocked' ? 'blocked' : 'draft';
  const blockerSource = [
    ...(payload.build_blockers || []),
    ...issues.filter(issue => (issue.severity || '').toLowerCase() === 'blocking'),
  ];
  const blockerSeen = new Set<string>();
  const blockers = blockerSource.filter((b) => {
    const key = b.issue_id || `${b.node_id || ''}:${b.kind || ''}:${b.label || b.summary || ''}`;
    if (blockerSeen.has(key)) return false;
    blockerSeen.add(key);
    return true;
  }).map(b => ({
    message: b.summary || b.label || 'Unknown blocker',
    severity: b.severity || 'blocking',
    nodeIds: b.node_id ? [b.node_id] : [],
  }));
  const projectedJobs = (payload.compiled_spec_projection?.compiled_spec?.jobs || []).map(j => ({
    label: j.label || 'unnamed',
    agent: j.agent || 'auto/build',
  }));

  const blockedNodes = nodes.filter(n => n.ringState === 'blocked').length;
  const resolvedNodes = nodes.filter(n =>
    n.ringState === 'decided-grounded' || n.ringState === 'decided-incomplete'
  ).length;

  // Readiness checklist
  const checklist: ReadinessItem[] = [];
  const untypedNodes = nodes.filter(n => n.isOnDominantPath && (n.ringState === 'unresolved' || n.ringState === 'active-unresolved'));
  checklist.push({
    label: 'All nodes typed',
    passed: untypedNodes.length === 0,
    nodeId: untypedNodes[0]?.id,
    message: untypedNodes.length === 0 ? 'All nodes have types' : `${untypedNodes.length} node${untypedNodes.length > 1 ? 's' : ''} need types`,
  });
  const incompleteNodes = nodes.filter(n => n.isOnDominantPath && n.ringState === 'decided-incomplete');
  checklist.push({
    label: 'Context attached',
    passed: incompleteNodes.length === 0,
    nodeId: incompleteNodes[0]?.id,
    dock: 'context',
    message: incompleteNodes.length === 0 ? 'All nodes have context' : `${incompleteNodes.length} node${incompleteNodes.length > 1 ? 's' : ''} need context`,
  });
  checklist.push({
    label: 'No blockers',
    passed: blockers.length === 0,
    nodeId: blockers[0]?.nodeIds?.[0],
    message: blockers.length === 0 ? 'No blockers' : `${blockers.length} blocker${blockers.length > 1 ? 's' : ''}`,
  });
  checklist.push({
    label: 'Jobs projected',
    passed: projectedJobs.length > 0,
    message: projectedJobs.length > 0 ? `${projectedJobs.length} jobs ready` : 'No jobs — compile first',
  });

  return {
    nodes, edges, dominantPath, layout, release: { readiness, blockers, projectedJobs, checklist },
    dockContent, selectedNode, activeNode, firstUnresolvedId,
    totalNodes: nodes.length, resolvedNodes, blockedNodes, focusActive,
  };
}
