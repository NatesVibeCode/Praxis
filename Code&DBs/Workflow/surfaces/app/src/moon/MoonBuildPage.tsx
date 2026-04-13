import React, { useReducer, useMemo, useCallback, useEffect, useRef, useState } from 'react';
import { useBuildPayload } from '../shared/hooks/useBuildPayload';
import { compileDefinition, refineDefinition } from '../shared/buildController';
import { presentBuild } from './moonBuildPresenter';
import type { OrbitNode, OrbitEdge, GateState, RunJobStatus } from './moonBuildPresenter';
import { useLiveRunSnapshot } from '../dashboard/useLiveRunSnapshot';
import { moonBuildReducer, initialMoonBuildState } from './moonBuildReducer';
import type { DragDropKind } from './moonBuildReducer';
import { MoonGlyph } from './MoonGlyph';
import { MoonPopout } from './MoonPopout';
import { MoonNodeDetail } from './MoonNodeDetail';
import { MoonActionDock } from './MoonActionDock';
import { MoonReleaseTray } from './MoonReleaseTray';
import { MoonRunPanel } from './MoonRunPanel';
import { MoonDragGhost } from './MoonDragGhost';
import { MoonEdges } from './MoonEdges';
import { useMoonDrag } from './useMoonDrag';
import type { DragPayload, DropTarget } from './useMoonDrag';
import { CATALOG, loadCatalog, getCatalog, catalogByFamily } from './catalog';
import type { CatalogItem } from './catalog';
import type { BuildNode, BuildEdge } from '../shared/types';
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

export function MoonBuildPage({ workflowId, onBack, onWorkflowCreated, onViewRun, initialMode }: Props) {
  const { payload, loading, error, mutate, reload, setPayload } = useBuildPayload(workflowId);
  const [state, dispatch] = useReducer(moonBuildReducer, {
    ...initialMoonBuildState,
    emptyMode: initialMode ?? initialMoonBuildState.emptyMode,
  });
  const centerRef = useRef<HTMLDivElement>(null);
  const [catalog, setCatalog] = useState<CatalogItem[]>(getCatalog());

  // Load live catalog from backend on mount
  useEffect(() => { loadCatalog().then(setCatalog); }, []);

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
    const graph = payload.build_graph;
    const nodes: BuildNode[] = [...(graph.nodes || [])];
    const edges: BuildEdge[] = [...(graph.edges || [])];
    const idx = nodes.findIndex(n => n.node_id === nodeId);
    if (idx < 0) return;

    // Find the catalog item for the title
    const catalogItem = catalog.find(c => c.actionValue === actionValue);
    const title = catalogItem?.label || actionValue;

    // Update the node locally
    nodes[idx] = { ...nodes[idx], route: actionValue, status: 'ready', title };

    // If no unresolved nodes remain after this, add a new empty one
    const hasUnresolved = nodes.some((n, i) => i !== idx && !(n.route || '').trim());
    if (!hasUnresolved) {
      const newId = `node-${nodes.length + 1}`;
      nodes.push({ node_id: newId, kind: 'step', title: 'Next step', route: '', status: '', summary: '' });
      edges.push({ edge_id: `edge-${nodeId}-${newId}`, kind: 'sequence', from_node_id: nodeId, to_node_id: newId });
    }

    setPayload({ ...payload, build_graph: { ...graph, nodes, edges } });

    // Persist to DB whenever there's an existing workflow
    if (workflowId) {
      void mutate('build_graph', { nodes, edges });
    }

    // Close popout and advance to next unresolved node
    dispatch({ type: 'CLOSE_POPOUT' });
    dispatch({ type: 'ADVANCE_ACTIVE', nextUnresolvedId: null });
  }, [payload, setPayload, mutate, workflowId, catalog]);

  const handleApplyGate = useCallback((edgeId: string, gateFamily: string) => {
    if (!payload?.build_graph) return;

    const gateItem = catalog.find(c => c.gateFamily === gateFamily);
    const gateLabel = gateItem?.label || gateFamily;

    const graph = payload.build_graph;
    const edges = [...(graph.edges || [])];
    const idx = edges.findIndex((e: any) => e.edge_id === edgeId);
    if (idx >= 0) {
      edges[idx] = {
        ...edges[idx],
        gate: { state: 'configured', label: gateLabel, family: gateFamily },
      };
      setPayload({ ...payload, build_graph: { ...graph, edges } } as any);
      if (workflowId) {
        void mutate('build_graph', { nodes: graph.nodes || [], edges });
      }
    }
    dispatch({ type: 'CLOSE_POPOUT' });
  }, [payload, setPayload, mutate, workflowId, catalog]);

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
          graph.nodes[0].route = state.selectedTrigger.actionValue;
          graph.nodes[0].status = 'ready';
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
          { node_id: 'node-1', kind: 'step', title: item.label, route: item.actionValue, status: 'ready', summary: `${item.label} trigger` },
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

  // --- Drag handlers ---
  const handleDragStart = useCallback((itemId: string, dropKind: DragDropKind) => {
    dispatch({ type: 'DRAG_START', itemId, dropKind });
  }, []);

  const handleNodeDragOver = useCallback((e: React.DragEvent, nodeId: string) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'link';
    dispatch({ type: 'DRAG_PREVIEW', targetId: nodeId });
  }, []);

  const handleEdgeDragOver = useCallback((e: React.DragEvent, edgeId: string) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'link';
    dispatch({ type: 'DRAG_PREVIEW', targetId: edgeId });
  }, []);

  const handleAppendDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'link';
    dispatch({ type: 'DRAG_PREVIEW', targetId: '__append__' });
  }, []);

  const handleDragLeave = useCallback(() => {
    dispatch({ type: 'DRAG_PREVIEW', targetId: null });
  }, []);

  const applyCatalogToNode = useCallback((catalogId: string, nodeId: string) => {
    const item = catalog.find(c => c.id === catalogId);
    if (item?.actionValue) handleNodeAction(nodeId, item.actionValue);
  }, [handleNodeAction, catalog]);

  const applyCatalogToEdge = useCallback((catalogId: string, edgeId: string) => {
    const item = catalog.find(c => c.id === catalogId);
    if (!item?.gateFamily || !payload?.definition) return;
    const instruction = `Add a "${item.label}" gate (family: ${item.gateFamily}) on edge "${edgeId}"`;
    refineDefinition(instruction, payload.definition).then(() => reload()).catch(() => {});
  }, [payload, reload]);

  const handleNodeDrop = useCallback((e: React.DragEvent, nodeId: string) => {
    e.preventDefault();
    const catalogId = e.dataTransfer.getData('moon/catalog-id');
    const sourceNodeId = e.dataTransfer.getData('moon/node-id');
    dispatch({ type: 'DRAG_END' });
    dispatch({ type: 'SELECT_NODE', nodeId });

    // Catalog item dropped on node
    if (catalogId) {
      applyCatalogToNode(catalogId, nodeId);
      return;
    }

    // Node reorder: move source node to target position
    if (sourceNodeId && sourceNodeId !== nodeId && payload?.build_graph) {
      const graph = payload.build_graph;
      const nodes = [...(graph.nodes || [])];
      const fromIdx = nodes.findIndex(n => n.node_id === sourceNodeId);
      const toIdx = nodes.findIndex(n => n.node_id === nodeId);
      if (fromIdx >= 0 && toIdx >= 0) {
        const [moved] = nodes.splice(fromIdx, 1);
        nodes.splice(toIdx, 0, moved);
        // Rebuild edges as sequential chain
        const edges = nodes.slice(1).map((n, i) => ({
          edge_id: `edge-${nodes[i].node_id}-${n.node_id}`,
          kind: 'sequence' as const,
          from_node_id: nodes[i].node_id,
          to_node_id: n.node_id,
        }));
        setPayload({ ...payload, build_graph: { ...graph, nodes, edges } });
        if (workflowId) {
          void mutate('build_graph', { nodes, edges });
        }
      }
      return;
    }

    // DB object dropped on node — auto-attach as context
    const objectTypeId = e.dataTransfer.getData('moon/object-type-id');
    if (objectTypeId && payload?.build_graph) {
      const objectLabel = e.dataTransfer.getData('moon/object-type-label') || objectTypeId;
      mutate('attachments', {
        node_id: nodeId,
        authority_kind: 'object_type',
        authority_ref: objectTypeId,
        role: 'input',
        label: objectLabel,
        promote_to_state: false,
      }).catch(() => {});
      dispatch({ type: 'OPEN_DOCK', dock: 'context' });
      return;
    }

    // Dock hint (attachment drag)
    const dockHint = e.dataTransfer.getData('moon/dock');
    if (dockHint === 'context' || dockHint === 'connect') {
      dispatch({ type: 'OPEN_DOCK', dock: 'context' });
    }
  }, [applyCatalogToNode, payload, setPayload, mutate]);

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

  const handleEdgeDrop = useCallback((e: React.DragEvent, edgeId: string) => {
    e.preventDefault();
    const catalogId = e.dataTransfer.getData('moon/catalog-id');
    dispatch({ type: 'DRAG_END' });
    if (catalogId) {
      applyCatalogToEdge(catalogId, edgeId);
      return;
    }
    dispatch({ type: 'SELECT_EDGE', edgeId });
  }, [applyCatalogToEdge]);

  const appendNode = useCallback((label?: string) => {
    if (!payload?.build_graph) return;
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
    setPayload({ ...payload, build_graph: { ...graph, nodes: newNodes, edges: newEdges } });
    if (workflowId) {
      void mutate('build_graph', { nodes: newNodes, edges: newEdges });
    }
  }, [payload, setPayload, mutate, workflowId]);

  const handleAppendDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    dispatch({ type: 'DRAG_END' });
    const catalogId = e.dataTransfer.getData('moon/catalog-id');
    const item = catalogId ? catalog.find(c => c.id === catalogId) : null;
    appendNode(item?.label);
  }, [appendNode]);

  const handleDragEnd = useCallback(() => {
    dispatch({ type: 'DRAG_END' });
  }, []);

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
  const isDragging = state.dragItemId !== null;
  const pendingItem = state.pendingCatalogId ? catalog.find(c => c.id === state.pendingCatalogId) : null;

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
                className="moon-dag"
                style={{
                  position: 'relative',
                  width: viewModel.layout.width + 240,
                  height: viewModel.layout.height + 240,
                  margin: '0 auto',
                  minHeight: 200,
                }}
                onDragEnd={handleDragEnd}
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
                        className={`moon-dag-node ${ringClass(node, isSelected)}${isSelected ? ' moon-dag-node--selected' : ''}${state.previewTarget === node.id ? ' moon-dag-node--drag-over' : ''}`}
                        style={{ left: node.x + 120 - 30, top: node.y + 120 - 30 }}
                        onClick={() => handleNodeClick(node.id, isSelected)}
                        draggable
                        onDragStart={e => {
                          e.dataTransfer.setData('moon/node-id', node.id);
                          e.dataTransfer.effectAllowed = 'move';
                          dispatch({ type: 'DRAG_START', itemId: node.id, dropKind: 'node' });
                        }}
                        onDragOver={e => handleNodeDragOver(e, node.id)}
                        onDragLeave={handleDragLeave}
                        onDrop={e => handleNodeDrop(e, node.id)}
                      >
                        {showIcon(node) ? (
                          <MoonGlyph type={node.glyphType} size={22} color="#fff" />
                        ) : (
                          <span className="moon-chain__step-index">{node.dominantPathIndex >= 0 ? node.dominantPathIndex + 1 : ''}</span>
                        )}
                        {node.needsBadge && <div className="moon-chain__badge" />}
                        <span className="moon-dag-node__label">{node.title}</span>
                      </div>
                    );
                  })}
                  {/* Append socket */}
                  <div
                    className={`moon-dag-append${state.previewTarget === '__append__' ? ' moon-dag-append--drag-over' : ''}`}
                    style={{ left: viewModel.layout.width + 120 + 30, top: 120 - 20 }}
                    onClick={() => appendNode()}
                    onDragOver={handleAppendDragOver}
                    onDragLeave={handleDragLeave}
                    onDrop={handleAppendDrop}
                  >
                    <span style={{ color: 'var(--moon-muted, #484f58)', fontSize: 18 }}>+</span>
                  </div>
                </div>
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
                onMutate={mutate}
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
