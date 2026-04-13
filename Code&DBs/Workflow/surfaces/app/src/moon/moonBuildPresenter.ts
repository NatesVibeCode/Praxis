// Pure presenter: BuildPayload -> MoonBuildViewModel
// No React, no fetch, no DOM. Just data transformation.

import type {
  BuildPayload, BuildNode, BuildEdge, BuildIssue,
  AuthorityAttachment, BindingLedgerEntry, ImportSnapshot,
} from '../shared/types';

// --- View model types ---

export type GlyphType = 'step' | 'gate' | 'state' | 'trigger' | 'human' | 'binding' | 'research' | 'classify' | 'draft' | 'notify' | 'review' | 'tool' | 'blocked';

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

// --- DAG layout ---

export const RANK_SPACING = 160;   // horizontal: distance between steps
export const COLUMN_SPACING = 120; // vertical: distance between parallel branches

export interface LayoutNode {
  id: string;
  rank: number;
  column: number;
  x: number;
  y: number;
}

export interface DagLayout {
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
  needsBadge: boolean;
  dominantPathIndex: number;
  x: number;
  y: number;
  rank: number;
}

export interface OrbitEdge {
  id: string;
  from: string;
  to: string;
  isOnDominantPath: boolean;
  gateState: GateState;
  gateLabel?: string;
  gateFamily?: string;
}

export interface DockContent {
  actionOptions: { label: string; value: string }[];
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
  layout: DagLayout;
  release: ReleaseStatus;
  dockContent: DockContent | null;
  selectedNode: OrbitNode | null;
  activeNode: OrbitNode | null;
  firstUnresolvedId: string | null;
  totalNodes: number;
  resolvedNodes: number;
  blockedNodes: number;
}

// --- Helpers ---

function nodeToGlyph(node: BuildNode): GlyphType {
  if (node.kind === 'gate') return 'gate';
  if (node.kind === 'state') return 'state';
  const status = (node.status || '').toLowerCase();
  if (status === 'blocked' || status === 'error') return 'blocked';
  const route = (node.route || '').toLowerCase();
  if (route.includes('trigger')) return 'trigger';
  if (route.includes('research')) return 'research';
  if (route.includes('classify') || route.includes('score') || route.includes('triage')) return 'classify';
  if (route.includes('draft') || route.includes('write') || route.includes('generate')) return 'draft';
  if (route.includes('notify') || route.includes('alert') || route.includes('send')) return 'notify';
  if (route.includes('review') || route.includes('approve')) return 'review';
  if (route.includes('human')) return 'human';
  if (route.includes('api') || route.includes('tool') || route.includes('webhook')) return 'tool';
  if (!route) return 'step'; // no route assigned = empty ring, no icon
  return 'step';
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

function nodeToRingState(node: BuildNode, activeId: string | null): RingState {
  const status = (node.status || '').toLowerCase();
  const decided = isNodeDecided(node);
  // Only show blocked ring for nodes that have a route but are explicitly blocked/error
  if ((status === 'blocked' || status === 'error') && decided) return 'blocked';
  if (status === 'draft' || status === 'pending') {
    if (!decided) return 'projected';
  }
  if (!decided) {
    return node.node_id === activeId ? 'active-unresolved' : 'unresolved';
  }
  // Decided — grounded vs incomplete is handled via needsBadge separately
  return 'decided-grounded';
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

export function extractLayout(payload: BuildPayload): DagLayout {
  const nodes = payload.build_graph?.nodes || [];
  const edges = payload.build_graph?.edges || [];
  const empty: DagLayout = { nodes: new Map(), layers: [], width: 0, height: 0 };
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
        return ax - bx;
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
  const emptyLayout: DagLayout = { nodes: new Map(), layers: [], width: 0, height: 0 };
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
    const badge = nodeNeedsBadge(n, payload);
    let ring = nodeToRingState(n, activeNodeId);
    if (badge && ring === 'decided-grounded') ring = 'decided-incomplete' as RingState;

    // Override with run status when a run is active
    const jobStatus = runStatusByTitle.get((n.title || n.node_id).toLowerCase());
    if (jobStatus) {
      if (jobStatus === 'succeeded') ring = 'run-succeeded';
      else if (jobStatus === 'failed' || jobStatus === 'dead_letter') ring = 'run-failed';
      else if (jobStatus === 'running' || jobStatus === 'claimed') ring = 'run-active';
      else if (jobStatus === 'pending' || jobStatus === 'ready') ring = 'run-pending';
    }

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
      needsBadge: badge,
      dominantPathIndex: pathIndexMap.get(n.node_id) ?? -1,
      x: layout.nodes.get(n.node_id)?.x ?? 0,
      y: layout.nodes.get(n.node_id)?.y ?? 0,
      rank: layout.nodes.get(n.node_id)?.rank ?? 0,
    };
  });

  // Gate state on edges
  const edges: OrbitEdge[] = rawEdges.map(e => {
    const raw = e as any;
    let gateState: GateState = 'empty';
    if (raw.gate) {
      const gs = (raw.gate.state || '').toLowerCase();
      if (gs === 'passed') gateState = 'passed';
      else if (gs === 'blocked') gateState = 'blocked';
      else if (gs === 'configured' || raw.gate.family) gateState = 'configured';
      else gateState = 'proposed';
    }
    return {
      id: e.edge_id,
      from: e.from_node_id,
      to: e.to_node_id,
      isOnDominantPath: pathSet.has(e.from_node_id) && pathSet.has(e.to_node_id),
      gateState,
      gateLabel: raw.gate?.label,
      gateFamily: raw.gate?.family,
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

    const actionOptions = [
      { label: 'Research', value: 'auto/research' },
      { label: 'Build', value: 'auto/build' },
      { label: 'Review', value: 'auto/review' },
      { label: 'Gate', value: 'gate' },
      { label: 'Trigger', value: 'trigger' },
      { label: 'Notify', value: 'notify' },
    ];

    dockContent = { actionOptions, contextAttachments: attachments, connectBindings: bindings, imports };
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
  const incompleteNodes = nodes.filter(n => n.isOnDominantPath && n.needsBadge);
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
