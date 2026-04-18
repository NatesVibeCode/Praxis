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
  | 'idea' | 'summary';

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
  tool: 'tool', api: 'tool', webhook: 'tool',
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

export const RANK_SPACING = 160;   // horizontal: distance between steps
export const COLUMN_SPACING = 120; // vertical: distance between parallel branches

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

function nodeToGlyph(node: BuildNode): GlyphType {
  if (node.kind === 'gate') return 'gate';
  if (node.kind === 'state') return 'state';
  const status = (node.status || '').toLowerCase();
  if (status === 'blocked' || status === 'error') return 'blocked';
  const route = (node.route || '').toLowerCase();
  const label = ((node as { label?: string; title?: string }).label || (node as { title?: string }).title || '').toLowerCase();
  return glyphFromLabel(`${route} ${label}`) ?? 'step';
}

function isNodeDecided(node: BuildNode): boolean {
  const route = (node.route || '').trim();
  return route.length > 0;
}

function nodeNeedsBadge(node: BuildNode, payload: BuildPayload): boolean {
  if (!isNodeDecided(node)) return false;
  // Badge if missing attachments or has unresolved bindings
  const attachments = (payload.authority_attachments || []).filter(a => a.node_id === node.node_id);
  if (attachments.length === 0) return true;
  const bindingIds = new Set(node.binding_ids || []);
  const bindings = (payload.binding_ledger || []).filter(b => bindingIds.has(b.binding_id));
  if (bindings.some(b => b.state !== 'accepted')) return true;
  return false;
}

function nodeHasIssuesOrBlockers(node: BuildNode, payload: BuildPayload): boolean {
  const nodeIssueIds = new Set(node.issue_ids || []);
  if (nodeIssueIds.size > 0) return true;
  if ((payload.build_issues || []).some(issue => issue.node_id === node.node_id || (issue.issue_id && nodeIssueIds.has(issue.issue_id)))) {
    return true;
  }
  if ((payload.build_blockers || []).some(issue => issue.node_id === node.node_id || (issue.issue_id && nodeIssueIds.has(issue.issue_id)))) {
    return true;
  }
  return false;
}

function nodeToRingState(node: BuildNode, payload: BuildPayload, activeId: string | null): RingState {
  const status = (node.status || '').toLowerCase();
  const decided = isNodeDecided(node);
  const blocked = status === 'blocked' || status === 'error' || nodeHasIssuesOrBlockers(node, payload);
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
  if (jobStatus === 'failed' || jobStatus === 'dead_letter') return 'run-failed';
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
    totalNodes: 0, resolvedNodes: 0, blockedNodes: 0,
  };
  if (!payload) return empty;

  const rawNodes = payload.build_graph?.nodes || [];
  const rawEdges = payload.build_graph?.edges || [];
  const issues = payload.build_issues || [];
  const dominantPath = extractDominantPath(payload);
  const layout = extractLayout(payload);
  const pathSet = new Set(dominantPath);
  const pathIndexMap = new Map(dominantPath.map((id, i) => [id, i]));

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
    };
  });

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
  const buildState = (payload.build_state || 'draft').toLowerCase();
  const readiness: ReleaseStatus['readiness'] = buildState === 'ready' ? 'ready' : buildState === 'blocked' ? 'blocked' : 'draft';
  const blockers = (payload.build_blockers || []).map(b => ({
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
    totalNodes: nodes.length, resolvedNodes, blockedNodes,
  };
}
