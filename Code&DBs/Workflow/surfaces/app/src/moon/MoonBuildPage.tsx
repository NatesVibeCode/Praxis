import React, { useReducer, useMemo, useCallback, useEffect, useRef, useState } from 'react';
import { useBuildPayload } from '../shared/hooks/useBuildPayload';
import { compileDefinition } from '../shared/buildController';
import { presentBuild } from './moonBuildPresenter';
import type { OrbitNode, OrbitEdge, GateState, RunJobStatus } from './moonBuildPresenter';
import { useLiveRunSnapshot } from '../dashboard/useLiveRunSnapshot';
import { moonBuildReducer, initialMoonBuildState } from './moonBuildReducer';
import { MoonGlyph } from './MoonGlyph';
import { MoonPopout } from './MoonPopout';
import { MoonNodeDetail } from './MoonNodeDetail';
import { MoonActionDock } from './MoonActionDock';
import { MoonReleaseTray } from './MoonReleaseTray';
import { MoonRunPanel } from './MoonRunPanel';
import { MoonDragGhost } from './MoonDragGhost';
import { MoonEdges } from './MoonEdges';
import { useMoonDrag } from './useMoonDrag';
import { loadCatalog, getCatalog } from './catalog';
import type { CatalogItem } from './catalog';
import type { BuildNode, BuildEdge, BuildPayload } from '../shared/types';
import './moon-build.css';

const EXAMPLE_PROMPTS = [
  'Research competitor pricing, classify by tier, draft a comparison report, notify the team on Slack',
  'Every morning, pull open tickets from Linear, summarize blockers, post to #standup',
  'When a new lead arrives, enrich from public data, score fit, route to the right account exec',
];

interface Props {
  workflowId: string | null;
  onBack?: () => void;
  onWorkflowCreated?: (id: string) => void;
  onViewRun?: (runId: string) => void;
  /** Initial empty mode: 'choice' (default), 'compose' (prose entry), 'trigger-picker' */
  initialMode?: 'choice' | 'compose' | 'trigger-picker';
}

// Half-moon dock invitation
function HalfMoon({ position, label, onClick }: {
  position: 'top' | 'left' | 'right' | 'bottom'; label: string; onClick: () => void;
}) {
  return (
    <button
      className={`moon-halfmoon moon-halfmoon--${position}`}
      onClick={onClick}
      aria-label={`Open ${label} dock`}
    >
      <span className="moon-halfmoon__text">{label}</span>
    </button>
  );
}

// Gate chip between nodes — traffic-light icons
function gateIcon(state: GateState): string {
  switch (state) {
    case 'empty': return '×';
    case 'proposed': return '≡';
    case 'configured': return '→';
    case 'passed': return '→';
    case 'blocked': return '!';
    default: return '×';
  }
}

function GateChip({ state, label, onClick }: {
  state: GateState; label?: string; onClick?: () => void;
}) {
  return (
    <div className={`moon-gate-chip moon-gate-chip--${state}`} onClick={onClick}>
      <span className="moon-gate-chip__icon">{gateIcon(state)}</span>
      {label && <span>{label}</span>}
    </div>
  );
}

// Ring class from state
function ringClass(node: OrbitNode, isSelected: boolean): string {
  const classes = [`moon-chain__ring`, `moon-chain__ring--${node.ringState}`];
  if (isSelected) classes.push('moon-chain__ring--selected');
  if (node.route && node.ringState === 'blocked') classes.push('moon-chain__ring--has-route');
  return classes.join(' ');
}

// Should this node show an icon?
function showIcon(node: OrbitNode): boolean {
  return node.ringState === 'decided-grounded' ||
    node.ringState === 'decided-incomplete' ||
    node.ringState === 'run-active' ||
    node.ringState === 'run-succeeded' ||
    node.ringState === 'run-failed' ||
    (node.ringState === 'blocked' && !!node.route);
}

const TRIGGER_MANUAL_ROUTE = 'trigger';
const TRIGGER_SCHEDULE_ROUTE = 'trigger/schedule';
const TRIGGER_WEBHOOK_ROUTE = 'trigger/webhook';
const WEBHOOK_TRIGGER_EVENT = 'db.webhook_events.insert';
const DEFAULT_BRANCH_CONDITION = {
  field: 'should_continue',
  op: 'equals',
  value: true,
} as const;

function isTriggerRoute(route?: string): boolean {
  return route === TRIGGER_MANUAL_ROUTE || route === TRIGGER_SCHEDULE_ROUTE || route === TRIGGER_WEBHOOK_ROUTE;
}

function normalizeTriggerFilter(filter: unknown): Record<string, unknown> {
  return filter && typeof filter === 'object' && !Array.isArray(filter)
    ? { ...filter as Record<string, unknown> }
    : {};
}

function buildTriggerConfig(route?: string, existing?: BuildNode['trigger']): BuildNode['trigger'] | undefined {
  if (!isTriggerRoute(route)) return undefined;
  const filter = normalizeTriggerFilter(existing?.filter);
  const sourceRef = typeof existing?.source_ref === 'string' ? existing.source_ref : undefined;
  if (route === TRIGGER_SCHEDULE_ROUTE) {
    return {
      event_type: 'schedule',
      cron_expression: (typeof existing?.cron_expression === 'string' && existing.cron_expression.trim()) || '@daily',
      filter,
    };
  }
  if (route === TRIGGER_WEBHOOK_ROUTE) {
    return {
      event_type: WEBHOOK_TRIGGER_EVENT,
      source_ref: sourceRef,
      filter,
    };
  }
  return {
    event_type: 'manual',
    source_ref: sourceRef,
    filter,
  };
}

function cloneBranchCondition(condition: unknown): Record<string, unknown> {
  if (condition && typeof condition === 'object' && !Array.isArray(condition)) {
    return JSON.parse(JSON.stringify(condition)) as Record<string, unknown>;
  }
  return { ...DEFAULT_BRANCH_CONDITION };
}

function branchLabel(reason: string | null | undefined): string {
  const normalized = (reason || '').trim();
  if (!normalized) return 'Branch';
  if (normalized === 'then') return 'Then';
  if (normalized === 'else') return 'Else';
  return normalized
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function nextGraphNodeId(nodes: BuildNode[], prefix: string): string {
  const existingIds = new Set(nodes.map(node => node.node_id));
  for (let index = 1; index < 10_000; index += 1) {
    const candidate = `${prefix}-${String(index).padStart(3, '0')}`;
    if (!existingIds.has(candidate)) return candidate;
  }
  return `${prefix}-${Date.now()}`;
}

function nextGraphEdgeId(edges: BuildEdge[], fromNodeId: string, toNodeId: string): string {
  const existingIds = new Set(edges.map(edge => edge.edge_id));
  const base = `edge-${fromNodeId}-${toNodeId}`;
  if (!existingIds.has(base)) return base;
  for (let index = 1; index < 10_000; index += 1) {
    const candidate = `${base}-${index}`;
    if (!existingIds.has(candidate)) return candidate;
  }
  return `${base}-${Date.now()}`;
}

function graphHasBranches(graph: NonNullable<BuildPayload['build_graph']>): boolean {
  const inboundCounts = new Map<string, number>();
  const outboundCounts = new Map<string, number>();
  for (const edge of graph.edges || []) {
    outboundCounts.set(edge.from_node_id, (outboundCounts.get(edge.from_node_id) || 0) + 1);
    inboundCounts.set(edge.to_node_id, (inboundCounts.get(edge.to_node_id) || 0) + 1);
  }
  return [...outboundCounts.values(), ...inboundCounts.values()].some(count => count > 1);
}

export function MoonBuildPage({ workflowId, onBack, onWorkflowCreated, onViewRun, initialMode }: Props) {
  const { payload, loading, error, mutate, reload, setPayload } = useBuildPayload(workflowId);
  const [state, dispatch] = useReducer(moonBuildReducer, {
    ...initialMoonBuildState,
    emptyMode: initialMode ?? initialMoonBuildState.emptyMode,
  });
  const centerRef = useRef<HTMLDivElement>(null);
  const [catalog, setCatalog] = useState<CatalogItem[]>(getCatalog());
  const [mutationError, setMutationError] = useState<string | null>(null);

  // Load live catalog from backend on mount
  useEffect(() => { loadCatalog().then(setCatalog); }, []);

  useEffect(() => {
    if (!mutationError) return;
    const t = setTimeout(() => setMutationError(null), 5000);
    return () => clearTimeout(t);
  }, [mutationError]);

  // Live run snapshot — active when a dispatch has produced a run
  const { run: activeRun } = useLiveRunSnapshot(state.activeRunId);
  const runJobs: RunJobStatus[] | undefined = useMemo(() => {
    if (!activeRun?.jobs?.length) return undefined;
    return activeRun.jobs.map(j => ({ label: j.label, status: j.status }));
  }, [activeRun]);

  const viewModel = useMemo(
    () => presentBuild(payload, state.selectedNodeId, state.activeNodeId, runJobs),
    [payload, state.selectedNodeId, state.activeNodeId, runJobs],
  );

  const runMutation = useCallback(async (subpath: string, body: Record<string, unknown>) => {
    try {
      await mutate(subpath, body);
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
      throw err;
    }
  }, [mutate]);

  const updateBuildGraph = useCallback(async (graph: NonNullable<BuildPayload['build_graph']>) => {
    if (!payload) return;
    setPayload({ ...payload, build_graph: graph });
    if (workflowId) {
      await runMutation('build_graph', {
        nodes: graph.nodes || [],
        edges: graph.edges || [],
      });
    }
  }, [payload, runMutation, setPayload, workflowId]);

  // Auto-advance active node:
  // - after compile (advanceQueued=true)
  // - after node action changes firstUnresolvedId
  // - initial load when no active node set
  useEffect(() => {
    if (state.advanceQueued) {
      dispatch({ type: 'ADVANCE_ACTIVE', nextUnresolvedId: viewModel.firstUnresolvedId });
      return;
    }
    // If active node is now decided (no longer unresolved), advance
    if (state.activeNodeId && viewModel.firstUnresolvedId &&
        state.activeNodeId !== viewModel.firstUnresolvedId) {
      const activeNode = viewModel.nodes.find(n => n.id === state.activeNodeId);
      if (activeNode && activeNode.ringState !== 'active-unresolved' && activeNode.ringState !== 'unresolved') {
        dispatch({ type: 'ADVANCE_ACTIVE', nextUnresolvedId: viewModel.firstUnresolvedId });
      }
    }
    // Initial: no active node yet but nodes exist
    if (!state.activeNodeId && viewModel.firstUnresolvedId) {
      dispatch({ type: 'ADVANCE_ACTIVE', nextUnresolvedId: viewModel.firstUnresolvedId });
    }
  }, [state.advanceQueued, state.activeNodeId, viewModel.firstUnresolvedId, viewModel.nodes]);

  // Apply action to a node — local mutation for UI-built chains, API for compiled chains
  const handleNodeAction = useCallback(async (nodeId: string, actionValue: string) => {
    if (!payload?.build_graph) return;
    try {
      const graph = payload.build_graph;
      const nodes: BuildNode[] = [...(graph.nodes || [])];
      const edges: BuildEdge[] = [...(graph.edges || [])];
      const idx = nodes.findIndex(n => n.node_id === nodeId);
      if (idx < 0) return;

      // Find the catalog item for the title
      const catalogItem = catalog.find(c => c.actionValue === actionValue);
      const title = catalogItem?.label || actionValue;

      // Update the node locally
      nodes[idx] = {
        ...nodes[idx],
        route: actionValue,
        status: 'ready',
        title,
        trigger: buildTriggerConfig(actionValue, nodes[idx].trigger),
      };

      // If no unresolved nodes remain after this, add a new empty one
      const hasUnresolved = nodes.some((n, i) => i !== idx && !(n.route || '').trim());
      if (!hasUnresolved) {
        const newId = `node-${nodes.length + 1}`;
        nodes.push({ node_id: newId, kind: 'step', title: 'Next step', route: '', status: '', summary: '' });
        edges.push({ edge_id: `edge-${nodeId}-${newId}`, kind: 'sequence', from_node_id: nodeId, to_node_id: newId });
      }

      await updateBuildGraph({ ...graph, nodes, edges });

      // Close popout and advance to next unresolved node
      dispatch({ type: 'CLOSE_POPOUT' });
      dispatch({ type: 'ADVANCE_ACTIVE', nextUnresolvedId: null });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [payload, updateBuildGraph, catalog]);

  const handleApplyGate = useCallback(async (edgeId: string, gateFamily: string) => {
    if (!payload?.build_graph) return;

    try {
      const gateItem = catalog.find(c => c.gateFamily === gateFamily);
      const gateLabel = gateItem?.label || gateFamily;

      const graph = payload.build_graph;
      const nodes = [...(graph.nodes || [])];
      const edges = [...(graph.edges || [])];
      const idx = edges.findIndex((e: any) => e.edge_id === edgeId);
      if (idx >= 0) {
        const edge = edges[idx];
        if (gateFamily === 'conditional') {
          const condition = cloneBranchCondition(edge.gate?.config?.condition);
          const sourceNode = nodes.find(node => node.node_id === edge.from_node_id);
          const targetNode = nodes.find(node => node.node_id === edge.to_node_id);
          edges[idx] = {
            ...edge,
            kind: 'conditional',
            branch_reason: 'then',
            gate: {
              ...(edge.gate || {}),
              state: 'configured',
              label: branchLabel('then'),
              family: gateFamily,
              config: {
                ...(edge.gate?.config || {}),
                condition,
              },
            },
          };

          const hasSiblingBranch = edges.some(other =>
            other.edge_id !== edge.edge_id &&
            other.from_node_id === edge.from_node_id &&
            other.gate?.family === 'conditional',
          );

          if (!hasSiblingBranch) {
            const elseNodeId = nextGraphNodeId(nodes, 'branch');
            const elseNode: BuildNode = {
              node_id: elseNodeId,
              kind: 'step',
              title: 'Else path',
              summary: `Runs when ${sourceNode?.title || 'the upstream step'} does not satisfy the branch condition.`,
              route: '',
              status: '',
            };
            nodes.push(elseNode);
            edges.push({
              edge_id: nextGraphEdgeId(edges, edge.from_node_id, elseNodeId),
              kind: 'conditional',
              from_node_id: edge.from_node_id,
              to_node_id: elseNodeId,
              branch_reason: 'else',
              gate: {
                state: 'configured',
                label: branchLabel('else'),
                family: gateFamily,
                config: {
                  condition,
                },
              },
            });

            if (targetNode && !targetNode.summary) {
              const targetIndex = nodes.findIndex(node => node.node_id === targetNode.node_id);
              if (targetIndex >= 0) {
                nodes[targetIndex] = {
                  ...nodes[targetIndex],
                  summary: `Runs when ${sourceNode?.title || 'the upstream step'} satisfies the branch condition.`,
                };
              }
            }
          }

          await updateBuildGraph({ ...graph, nodes, edges });
        } else {
          edges[idx] = {
            ...edge,
            gate: { state: 'configured', label: gateLabel, family: gateFamily },
          };
          await updateBuildGraph({ ...graph, edges } as any);
        }
      }
      dispatch({ type: 'CLOSE_POPOUT' });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [payload, updateBuildGraph, catalog]);

  const handleCompile = useCallback(async () => {
    if (!state.compileProse.trim()) return;
    dispatch({ type: 'COMPILE_START' });
    try {
      const prefix = state.selectedTrigger
        ? `Starting with a ${state.selectedTrigger.label} trigger: `
        : '';
      const result = await compileDefinition(prefix + state.compileProse.trim());
      // Patch first node with trigger route if trigger was selected
      if (state.selectedTrigger) {
        const graph = (result as any)?.build_graph;
        if (graph?.nodes?.length) {
          graph.nodes[0] = {
            ...graph.nodes[0],
            route: state.selectedTrigger.actionValue,
            status: 'ready',
            trigger: buildTriggerConfig(state.selectedTrigger.actionValue, graph.nodes[0].trigger),
          };
        }
      }
      const wfId = (result as any)?.definition?.workflow_id || (result as any)?.workflow?.id;
      const asPayload = {
        ...result,
        workflow: wfId ? { id: wfId, name: (result as any)?.definition?.compiled_prose?.slice(0, 60) || '' } : null,
      };
      setPayload(asPayload);
      if (wfId && onWorkflowCreated) onWorkflowCreated(wfId);
      dispatch({ type: 'COMPILE_SUCCESS' });
    } catch (e: any) {
      dispatch({ type: 'COMPILE_ERROR', error: e.message || 'Compilation failed' });
    }
  }, [state.compileProse, state.selectedTrigger, onWorkflowCreated, setPayload]);

  const handleTriggerSelect = useCallback((item: CatalogItem) => {
    dispatch({ type: 'SELECT_TRIGGER', trigger: {
      id: item.id, label: item.label, icon: item.icon, actionValue: item.actionValue!,
    }});
    // Build chain locally — no compiler. User builds step by step.
    setPayload({
      build_graph: {
        nodes: [
          { node_id: 'node-1', kind: 'step', title: item.label, route: item.actionValue, trigger: buildTriggerConfig(item.actionValue), status: 'ready', summary: `${item.label} trigger` },
          { node_id: 'node-2', kind: 'step', title: 'Next step', route: '', status: '', summary: '' },
        ],
        edges: [
          { edge_id: 'edge-1-2', from_node_id: 'node-1', to_node_id: 'node-2' },
        ],
      },
      build_state: 'draft',
      definition: {},
      build_issues: [],
      build_blockers: [],
      authority_attachments: [],
      binding_ledger: [],
      import_snapshots: [],
    } as any);
    dispatch({ type: 'COMPILE_SUCCESS' });
  }, [setPayload]);

  const openDock = useCallback((dock: 'action' | 'context' | 'connect') => {
    const mapped: 'action' | 'context' = dock === 'connect' ? 'context' : dock;
    if (state.openDock === mapped) dispatch({ type: 'CLOSE_DOCK' });
    else dispatch({ type: 'OPEN_DOCK', dock: mapped });
  }, [state.openDock]);

  const applyCatalogToNode = useCallback((catalogId: string, nodeId: string) => {
    const item = catalog.find(c => c.id === catalogId);
    if (item?.actionValue) void handleNodeAction(nodeId, item.actionValue);
  }, [handleNodeAction, catalog]);

  const applyCatalogToEdge = useCallback(async (catalogId: string, edgeId: string) => {
    try {
      const item = catalog.find(c => c.id === catalogId);
      if (!item?.gateFamily) return;
      await handleApplyGate(edgeId, item.gateFamily);
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [catalog, handleApplyGate]);

  // Click fallback: if a catalog item is staged, clicking a node applies it
  const handleNodeClick = useCallback((nodeId: string, isSelected: boolean) => {
    if (state.pendingCatalogId) {
      const catalogId = state.pendingCatalogId;
      dispatch({ type: 'CLEAR_CATALOG' });
      dispatch({ type: 'SELECT_NODE', nodeId });
      applyCatalogToNode(catalogId, nodeId);
      return;
    }
    if (isSelected && nodeId !== state.activeNodeId) {
      dispatch({ type: 'SELECT_NODE', nodeId: null });
    } else {
      dispatch({ type: 'SELECT_NODE', nodeId });
    }
  }, [state.pendingCatalogId, state.activeNodeId, applyCatalogToNode]);

  const appendNode = useCallback(async (label?: string) => {
    if (!payload?.build_graph) return;
    try {
      const graph = payload.build_graph;
      const stepNodes = (graph.nodes || []).filter(n => n.kind === 'step' || !n.kind);
      const lastStep = stepNodes[stepNodes.length - 1] ?? null;
      const newId = `node-${stepNodes.length + 1}`;
      const newNode: BuildNode = {
        node_id: newId,
        kind: 'step',
        title: label || 'Next step',
        route: '',
        status: '',
        summary: '',
      };
      const newEdges: BuildEdge[] = [...(graph.edges || [])];
      if (lastStep) {
        newEdges.push({ edge_id: `edge-${lastStep.node_id}-${newId}`, kind: 'sequence', from_node_id: lastStep.node_id, to_node_id: newId });
      }
      const newNodes: BuildNode[] = [...(graph.nodes || []), newNode];
      await updateBuildGraph({ ...graph, nodes: newNodes, edges: newEdges });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [payload, updateBuildGraph]);

  const reorderNode = useCallback(async (sourceNodeId: string, targetNodeId: string) => {
    if (sourceNodeId === targetNodeId || !payload?.build_graph) return;
    try {
      const graph = payload.build_graph;
      if (graphHasBranches(graph)) {
        setMutationError('Reordering is disabled once the graph branches. Move the branch by rewiring edges instead.');
        return;
      }
      const nodes = [...(graph.nodes || [])];
      const fromIdx = nodes.findIndex(n => n.node_id === sourceNodeId);
      const toIdx = nodes.findIndex(n => n.node_id === targetNodeId);
      if (fromIdx < 0 || toIdx < 0) return;

      const [moved] = nodes.splice(fromIdx, 1);
      nodes.splice(toIdx, 0, moved);
      const edges = nodes.slice(1).map((n, i) => ({
        edge_id: `edge-${nodes[i].node_id}-${n.node_id}`,
        kind: 'sequence' as const,
        from_node_id: nodes[i].node_id,
        to_node_id: n.node_id,
      }));
      await updateBuildGraph({ ...graph, nodes, edges });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [payload, updateBuildGraph]);

  const drag = useMoonDrag((dragPayload, target) => {
    dispatch({ type: 'DRAG_END' });

    if (dragPayload.kind === 'catalog') {
      if (target.zone === 'node') {
        dispatch({ type: 'SELECT_NODE', nodeId: target.id });
        void applyCatalogToNode(dragPayload.id, target.id);
        return;
      }
      if (target.zone === 'append') {
        const item = catalog.find(c => c.id === dragPayload.id);
        void appendNode(item?.label);
        return;
      }
      if (target.zone === 'edge') {
        dispatch({ type: 'SELECT_EDGE', edgeId: target.id });
        void applyCatalogToEdge(dragPayload.id, target.id);
      }
      return;
    }

    if (dragPayload.kind === 'node' && target.zone === 'node') {
      dispatch({ type: 'SELECT_NODE', nodeId: target.id });
      void reorderNode(dragPayload.id, target.id);
    }
  });

  const startCatalogDrag = useCallback((event: React.PointerEvent, item: CatalogItem) => {
    dispatch({ type: 'DRAG_START', itemId: item.id, dropKind: item.dropKind });
    drag.startDrag(event, { kind: 'catalog', id: item.id, label: item.label });
  }, [drag]);

  const startNodeDrag = useCallback((event: React.PointerEvent, node: OrbitNode) => {
    dispatch({ type: 'DRAG_START', itemId: node.id, dropKind: 'node' });
    drag.startDrag(event, { kind: 'node', id: node.id, label: node.title });
  }, [drag]);

  // --- Center-follow offset ---
  const spineNodes = viewModel.nodes.filter(n => n.isOnDominantPath);
  const activeIdx = spineNodes.findIndex(n => n.id === state.activeNodeId);
  const nodeSpacing = 120; // matches CSS token
  const nodeSize = 60;

  // Track container width so offset recalculates when docks open/close
  const [centerWidth, setCenterWidth] = useState(0);
  useEffect(() => {
    const el = centerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      for (const entry of entries) setCenterWidth(entry.contentRect.width);
    });
    ro.observe(el);
    setCenterWidth(el.clientWidth);
    return () => ro.disconnect();
  }, []);

  // Translate chain so active node sits at visual center of container
  // Use 50% CSS fallback when JS measurement isn't ready yet
  const hasMeasured = centerWidth > 0;
  const translateX = useMemo(() => {
    // Center on active node, or first node if no active node
    const idx = activeIdx >= 0 ? activeIdx : 0;
    const activePos = idx * nodeSpacing + nodeSize / 2;
    if (!hasMeasured) return -activePos;
    const containerCenter = centerWidth / 2;
    return containerCenter - activePos;
  }, [activeIdx, nodeSpacing, nodeSize, centerWidth, hasMeasured]);

  const hasNodes = viewModel.nodes.length > 0;
  const actionOpen = state.openDock === 'action';
  const contextOpen = state.openDock === 'context';
  const releaseOpen = state.releaseOpen;
  const compiling = state.compilePhase === 'compiling';
  const previewTargetId = drag.drag.hoveredTarget?.id ?? null;

  // Build edge lookup for gate rendering
  const edgeMap = useMemo(() => {
    const m = new Map<string, OrbitEdge>();
    for (const e of viewModel.edges) {
      // key: "from->to" for dominant path edges
      m.set(`${e.from}->${e.to}`, e);
    }
    return m;
  }, [viewModel.edges]);

  // Selected edge for gate config in Detail dock
  const selectedEdge = state.selectedEdgeId
    ? viewModel.edges.find(e => e.id === state.selectedEdgeId) || null
    : null;
  const edgeFromNode = selectedEdge ? viewModel.nodes.find(n => n.id === selectedEdge.from) : null;
  const edgeToNode = selectedEdge ? viewModel.nodes.find(n => n.id === selectedEdge.to) : null;

  return (
    <div className="moon-page">
      {mutationError && (
        <div className="moon-error-toast" role="alert" aria-live="polite">
          {mutationError}
          <button type="button" onClick={() => setMutationError(null)} aria-label="Dismiss error">
            &times;
          </button>
        </div>
      )}
      <div className="moon-frame-top" />

      <div className="moon-body">
        {/* Middle row */}
        <div className="moon-middle">
          {/* Left dock: Action */}
          <div className={`moon-dock-side${actionOpen ? ' moon-dock-side--open moon-dock-left--open' : ' moon-dock-side--closed'}`}>
            {actionOpen && (
              <MoonActionDock
                workflowId={workflowId}
                payload={payload}
                onReload={reload}
                onClose={() => dispatch({ type: 'CLOSE_DOCK' })}
                onStartCatalogDrag={startCatalogDrag}
              />
            )}
          </div>

          {/* Center */}
          <div className="moon-center" ref={centerRef}>
            {!actionOpen && <HalfMoon position="left" label="Action" onClick={() => openDock('action')} />}
            {!contextOpen && <HalfMoon position="right" label="Detail" onClick={() => openDock('context')} />}
            {!releaseOpen && <HalfMoon position="bottom" label="Release" onClick={() => dispatch({ type: 'TOGGLE_RELEASE' })} />}

            {!hasNodes ? (
              <div className="moon-start">
                {/* Nucleus circle — clickable to pick or change trigger */}
                <div
                  className={`moon-nucleus${state.emptyMode === 'choice' ? ' moon-nucleus--interactive' : ''}`}
                  onClick={state.emptyMode === 'choice' ? () => dispatch({ type: 'EMPTY_PICK_TRIGGER' }) : undefined}
                >
                  <div className={`moon-nucleus__ring${state.selectedTrigger ? ' moon-nucleus__ring--decided' : ''}`}>
                    {state.emptyMode === 'trigger-picker' && !state.selectedTrigger && (
                      <MoonGlyph type="trigger" size={22} color="#6CB6FF" />
                    )}
                    {state.selectedTrigger && (
                      <MoonGlyph type={state.selectedTrigger.icon as any} size={22} color="#fff" />
                    )}
                  </div>
                  {state.emptyMode === 'choice' && (
                    <span className="moon-nucleus__label">Choose a trigger</span>
                  )}
                  {state.selectedTrigger && (
                    <span className="moon-nucleus__label">{state.selectedTrigger.label}</span>
                  )}
                </div>

                {/* Trigger picker — anchored below circle */}
                {state.emptyMode === 'trigger-picker' && (
                  <div className="moon-trigger-picker">
                    <div className="moon-trigger-picker__line" />
                    <div className="moon-trigger-picker__card">
                      {catalog.filter(c => c.family === 'trigger').map(item => (
                        <button
                          key={item.id}
                          className={`moon-trigger-picker__item${item.status === 'coming_soon' ? ' moon-trigger-picker__item--muted' : ''}`}
                          onClick={() => item.status === 'ready' && handleTriggerSelect(item)}
                          disabled={item.status !== 'ready'}
                        >
                          <MoonGlyph type={item.icon} size={16} />
                          <span>{item.label}</span>
                          {item.status === 'coming_soon' && <span className="moon-trigger-picker__badge">Soon</span>}
                        </button>
                      ))}
                      <button className="moon-trigger-picker__back" onClick={() => dispatch({ type: 'EMPTY_RESET' })}>
                        Back
                      </button>
                    </div>
                  </div>
                )}

                {/* Text invite (choice mode only, no trigger selected) */}
                {state.emptyMode === 'choice' && !state.selectedTrigger && (
                  <button
                    className="moon-compose__text-invite"
                    onClick={() => dispatch({ type: 'EMPTY_PICK_COMPOSE' })}
                  >
                    Or describe in words
                  </button>
                )}

                {/* Compose flow — only from "Or describe in words" */}
                {state.emptyMode === 'compose' && (
                  <div className="moon-compose">
                    <div className="moon-compose__hint">Describe what your workflow should do</div>
                    <div className="moon-compose__examples">
                      {EXAMPLE_PROMPTS.map((prompt) => (
                        <button
                          key={prompt}
                          className="moon-compose__chip"
                          onClick={() => dispatch({ type: 'SET_PROSE', prose: prompt })}
                        >
                          {prompt}
                        </button>
                      ))}
                    </div>
                    <textarea
                      value={state.compileProse}
                      onChange={(e) => dispatch({ type: 'SET_PROSE', prose: e.target.value })}
                      placeholder="Research competitor pricing, classify by tier, draft a report..."
                      rows={3}
                      disabled={compiling}
                    />
                    {state.compileError && <div className="moon-compose__error">{state.compileError}</div>}
                    <div className="moon-compose__actions">
                      <button className="moon-compose__btn" onClick={handleCompile} disabled={compiling || !state.compileProse.trim()}>
                        {compiling ? 'Building...' : 'Build'}
                      </button>
                      <button className="moon-compose__btn moon-compose__btn--secondary"
                        onClick={() => dispatch({ type: 'EMPTY_RESET' })}>Back</button>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div
                className="moon-graph"
                style={{
                  position: 'relative',
                  width: viewModel.layout.width + 240,
                  height: viewModel.layout.height + 240,
                  margin: '0 auto',
                  minHeight: 200,
                }}
              >
                <MoonEdges
                  edges={viewModel.edges}
                  layout={viewModel.layout}
                  selectedEdgeId={state.selectedEdgeId}
                  onEdgeClick={(id) => {
                    dispatch({ type: 'SELECT_EDGE', edgeId: id });
                    dispatch({ type: 'OPEN_DOCK', dock: 'context' });
                  }}
                />
                <div style={{ position: 'absolute', top: 0, left: 0 }}>
                  {viewModel.nodes.map((node) => {
                    const isSelected = node.id === state.selectedNodeId;
                    return (
                      <div
                        key={node.id}
                        className={`moon-graph-node ${ringClass(node, isSelected)}${isSelected ? ' moon-graph-node--selected' : ''}${previewTargetId === node.id ? ' moon-graph-node--drag-over' : ''}`}
                        style={{ left: node.x + 120 - 30, top: node.y + 120 - 30 }}
                        data-drop-node={node.id}
                        onClick={() => handleNodeClick(node.id, isSelected)}
                        onPointerDown={e => startNodeDrag(e, node)}
                      >
                        {showIcon(node) ? (
                          <MoonGlyph type={node.glyphType} size={22} color="#fff" />
                        ) : (
                          <span className="moon-chain__step-index">{node.dominantPathIndex >= 0 ? node.dominantPathIndex + 1 : ''}</span>
                        )}
                        {node.needsBadge && <div className="moon-chain__badge" />}
                        <span className="moon-graph-node__label">{node.title}</span>
                      </div>
                    );
                  })}
                  {/* Append socket */}
                  <div
                    className={`moon-graph-append${previewTargetId === '__append__' ? ' moon-graph-append--drag-over' : ''}`}
                    style={{ left: viewModel.layout.width + 120 + 30, top: 120 - 20 }}
                    data-drop-append="true"
                    onClick={() => appendNode()}
                  >
                    <span style={{ color: 'var(--moon-muted, #484f58)', fontSize: 18 }}>+</span>
                  </div>
                </div>
                <MoonDragGhost drag={drag.drag} />
                {/* Popout — rendered at container level so it's not clipped */}
                {state.selectedNodeId && state.popoutOpen && viewModel.selectedNode && (() => {
                  const sel = viewModel.nodes.find(n => n.id === state.selectedNodeId);
                  if (!sel) return null;
                  return (
                    <div style={{ position: 'absolute', left: sel.x + 120, top: sel.y + 120 - 80, zIndex: 10 }}>
                      <MoonPopout
                        node={viewModel.selectedNode}
                        content={viewModel.dockContent}
                        onClose={() => dispatch({ type: 'CLOSE_POPOUT' })}
                        onSelect={handleNodeAction}
                        catalog={catalog}
                        onStartCatalogDrag={startCatalogDrag}
                      />
                    </div>
                  );
                })()}
              </div>
            )}

          </div>

          {/* Right dock: Detail */}
          <div className={`moon-dock-side${contextOpen ? ' moon-dock-side--open moon-dock-right--open' : ' moon-dock-side--closed'}`}>
            {contextOpen && (
                <MoonNodeDetail
                  node={viewModel.selectedNode}
                  content={viewModel.dockContent}
                  workflowId={workflowId}
                  onMutate={runMutation}
                  buildGraph={payload?.build_graph}
                  onUpdateBuildGraph={updateBuildGraph}
                  onClose={() => dispatch({ type: 'CLOSE_DOCK' })}
                  selectedEdge={selectedEdge}
                  edgeFromLabel={edgeFromNode?.title}
                edgeToLabel={edgeToNode?.title}
                onApplyGate={handleApplyGate}
                gateItems={catalog.filter(c => c.family === 'control' && c.status === 'ready')}
              />
            )}
          </div>
        </div>

        {/* Bottom dock: Release or Run */}
        <div className={`moon-dock-bottom${(releaseOpen || state.runViewOpen) ? ' moon-dock-bottom--open' : ' moon-dock-bottom--closed'}`}>
          {state.runViewOpen && state.activeRunId ? (
            <MoonRunPanel
              runId={state.activeRunId}
              workflowId={workflowId}
              onClose={() => dispatch({ type: 'CLOSE_RUN' })}
              onSwitchRun={(newRunId) => dispatch({ type: 'DISPATCH_SUCCESS', runId: newRunId })}
            />
          ) : releaseOpen ? (
            <MoonReleaseTray
              release={viewModel.release}
              payload={payload}
              workflowId={workflowId}
              onClose={() => dispatch({ type: 'TOGGLE_RELEASE' })}
              onSelectNode={(nodeId) => dispatch({ type: 'SELECT_NODE', nodeId })}
              onOpenDock={(dock) => dispatch({ type: 'OPEN_DOCK', dock: dock as 'action' | 'context' })}
              onViewRun={onViewRun}
              onDispatchSuccess={(runId) => dispatch({ type: 'DISPATCH_SUCCESS', runId })}
            />
          ) : null}
        </div>
      </div>

      <div className="moon-frame-bottom" />
    </div>
  );
}
