import React, { useReducer, useMemo, useCallback, useEffect, useRef, useState } from 'react';
import { useBuildPayload } from '../shared/hooks/useBuildPayload';
import { compileDefinition } from '../shared/buildController';
import { presentBuild } from './moonBuildPresenter';
import type { OrbitNode, OrbitEdge, RunJobStatus } from './moonBuildPresenter';
import { presentRun } from './moonRunPresenter';
import { useLiveRunSnapshot } from '../dashboard/useLiveRunSnapshot';
import { moonBuildReducer, initialMoonBuildState } from './moonBuildReducer';
import { MoonGlyph } from './MoonGlyph';
import { MoonPopout } from './MoonPopout';
import { MoonNodeDetail, type AuthorityActionMeta } from './MoonNodeDetail';
import { MoonActionDock } from './MoonActionDock';
import { MoonReleaseTray } from './MoonReleaseTray';
import { MoonRunPanel } from './MoonRunPanel';
import { MoonRunOverlay } from './MoonRunOverlay';
import { MoonDragGhost } from './MoonDragGhost';
import { MoonEdges, getEdgeGeometry } from './MoonEdges';
import { useMoonDrag } from './useMoonDrag';
import { loadCatalog, getCatalog } from './catalog';
import type { CatalogItem } from './catalog';
import type { BuildNode, BuildEdge, BuildPayload } from '../shared/types';
import {
  baseConditionFromRelease,
  branchLabel,
  normalizeBuildEdgeRelease,
  withBuildEdgeRelease,
} from '../shared/edgeRelease';
import { MenuPanel, type MenuSection } from '../menu';
import { getCatalogSurfacePolicy, getCatalogTruth } from './actionTruth';
import { scaffoldMoonPrimitiveNode } from './moonPrimitives';
import {
  getMoonAppendPosition,
  getMoonCanvasDimensions,
  getMoonNodeAnchorRect,
  getMoonNodeCanvasPosition,
  MOON_LAYOUT,
  MOON_LAYOUT_CSS_VARS,
} from './moonLayout';
import { Toast, useToast } from '../primitives/Toast';
import { UiActionFeed } from '../control/UiActionFeed';
import {
  registerUiActionUndoExecutor,
  runUiAction,
  undoUiAction,
  type UiActionTarget,
  type UiActionUndoDescriptor,
} from '../control/uiActionLedger';
import './moon-build.css';

const EXAMPLE_PROMPTS = [
  'Research competitor pricing, classify by tier, draft a comparison report, notify the team on Slack',
  'Every morning, pull open tickets from Linear, summarize blockers, post to #standup',
  'When a new lead arrives, enrich from public data, score fit, route to the right account exec',
];

interface Props {
  workflowId: string | null;
  /**
   * When set, Moon renders a run-view over its canvas for this run_id.
   * The URL `/app/run/:id` routes here via shell state.
   */
  runId?: string | null;
  onBack?: () => void;
  onWorkflowCreated?: (id: string) => void;
  onViewRun?: (runId: string) => void;
  onDraftStateChange?: (draft: { dirty: boolean; message?: string | null }) => void;
  /** Initial empty mode: 'choice' (default), 'compose' (prose entry), 'trigger-picker' */
  initialMode?: 'choice' | 'compose' | 'trigger-picker';
}

type MoonGlowProfile = 'soft' | 'strict';

function readMoonGlowProfile(): MoonGlowProfile {
  if (typeof window === 'undefined') return 'soft';
  const storage = window.localStorage;
  if (!storage || typeof storage.getItem !== 'function') return 'soft';
  try {
    return storage.getItem('moon-glow-profile') === 'strict' ? 'strict' : 'soft';
  } catch {
    return 'soft';
  }
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

function DockToggleButton({
  active,
  label,
  onClick,
}: {
  active: boolean;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={`moon-center__dock-btn${active ? ' moon-center__dock-btn--active' : ''}`}
      data-keep-edge-menu-open="true"
      aria-label={`Open ${label} dock`}
      aria-pressed={active}
      onClick={onClick}
    >
      <span className="moon-center__dock-btn-label">{label}</span>
    </button>
  );
}

type GatePodTone = 'empty' | 'core' | 'later' | 'legacy';

function gatePodTone(edge: OrbitEdge, item?: CatalogItem | null): GatePodTone {
  if (!edge.gateFamily) return 'empty';
  const policy = item ? getCatalogSurfacePolicy(item) : null;
  if (policy?.tier === 'primary') return 'core';
  if (policy?.tier === 'advanced') return 'later';
  return 'legacy';
}

function gatePodLabel(edge: OrbitEdge, item?: CatalogItem | null): string {
  if (!edge.gateFamily) return 'Add gate';
  return edge.gateLabel || item?.label || 'Gate';
}

/**
 * Glyph that reads the gate family at a glance. Keeps the canvas legible when
 * multiple branches leave a node — eye picks out which branch is the failure
 * path versus the condition versus the always path without reading labels.
 */
function gatePodGlyphType(edge: OrbitEdge): import('./moonBuildPresenter').GlyphType {
  switch (edge.gateFamily) {
    case 'conditional': return 'decompose';
    case 'after_failure': return 'blocked';
    case 'after_any': return 'summary';
    case 'after_success': return 'validate';
    default: return 'gate';
  }
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
type BranchSide = 'above' | 'below';
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

function oppositeBranchSide(side: BranchSide): BranchSide {
  return side === 'above' ? 'below' : 'above';
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

function nodeDisplayName(node: Pick<BuildNode, 'node_id' | 'title'> | null | undefined): string {
  const title = typeof node?.title === 'string' ? node.title.trim() : '';
  return title || node?.node_id || 'step';
}

function nodeTarget(node: Pick<BuildNode, 'node_id' | 'title'> | null | undefined): UiActionTarget | null {
  if (!node?.node_id) return null;
  return {
    kind: 'node',
    label: nodeDisplayName(node),
    id: node.node_id,
  };
}

function edgeDisplayName(
  edge: Pick<BuildEdge, 'edge_id' | 'from_node_id' | 'to_node_id'> | null | undefined,
  graph: NonNullable<BuildPayload['build_graph']> | null | undefined,
): string {
  if (!edge) return 'edge';
  const nodes = graph?.nodes || [];
  const fromNode = nodes.find((node) => node.node_id === edge.from_node_id);
  const toNode = nodes.find((node) => node.node_id === edge.to_node_id);
  return `${nodeDisplayName(fromNode)} -> ${nodeDisplayName(toNode)}`;
}

function edgeTarget(
  edge: Pick<BuildEdge, 'edge_id' | 'from_node_id' | 'to_node_id'> | null | undefined,
  graph: NonNullable<BuildPayload['build_graph']> | null | undefined,
): UiActionTarget | null {
  if (!edge?.edge_id) return null;
  return {
    kind: 'edge',
    label: edgeDisplayName(edge, graph),
    id: edge.edge_id,
  };
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

function resolvePersistedWorkflowId(workflowId: string | null, payload: BuildPayload | null): string | null {
  if (workflowId) return workflowId;
  if (typeof payload?.workflow?.id === 'string' && payload.workflow.id.trim()) return payload.workflow.id;
  const definitionWorkflowId = (payload?.definition as Record<string, unknown> | undefined)?.workflow_id;
  return typeof definitionWorkflowId === 'string' && definitionWorkflowId.trim() ? definitionWorkflowId : null;
}

function hasLocalDraftPayload(payload: BuildPayload | null): boolean {
  if (!payload) return false;
  if ((payload.build_graph?.nodes?.length || 0) > 0) return true;
  if ((payload.build_graph?.edges?.length || 0) > 0) return true;
  if (payload.definition && Object.keys(payload.definition).length > 0) return true;
  if (payload.compiled_spec && Object.keys(payload.compiled_spec).length > 0) return true;
  return false;
}

function shouldKeepEdgeMenusOpen(target: EventTarget | null): boolean {
  if (!(target instanceof Element)) return false;
  return Boolean(
    target.closest('.moon-graph-gate')
    || target.closest('.moon-dock-overlay')
    || target.closest('.moon-dock-side')
    || target.closest('[data-keep-edge-menu-open="true"]')
    || target.closest('.menu-panel'),
  );
}

export function MoonBuildPage({ workflowId, runId, onBack, onWorkflowCreated, onViewRun, onDraftStateChange, initialMode }: Props) {
  const { payload, loading, error, mutate, reload, setPayload } = useBuildPayload(workflowId);
  const [state, dispatch] = useReducer(moonBuildReducer, {
    ...initialMoonBuildState,
    emptyMode: initialMode ?? initialMoonBuildState.emptyMode,
  });
  const centerRef = useRef<HTMLDivElement>(null);
  const triggerAnchorRef = useRef<HTMLDivElement>(null);
  const pinnedSelectionRef = useRef<string | null>(null);
  const [catalog, setCatalog] = useState<CatalogItem[]>(getCatalog());
  const [moonGlowProfile, setMoonGlowProfile] = useState<MoonGlowProfile>(readMoonGlowProfile);
  const [mutationError, setMutationError] = useState<string | null>(null);
  /**
   * Which node has its branch-family picker open. A node-scoped picker is the
   * single affordance for adding a new outgoing edge from an existing step
   * (success / failure / any / conditional). It co-exists with the edge-pod
   * picker that configures an *existing* edge's gate.
   */
  const [branchPickerNodeId, setBranchPickerNodeId] = useState<string | null>(null);
  const { show } = useToast();
  const persistedWorkflowId = useMemo(
    () => resolvePersistedWorkflowId(workflowId, payload),
    [payload, workflowId],
  );
  const draftGuardState = useMemo(() => {
    const dirty = !persistedWorkflowId
      && (
        Boolean(state.selectedTrigger)
        || Boolean(state.compileProse.trim())
        || hasLocalDraftPayload(payload)
      );
    return {
      dirty,
      message: dirty
        ? 'This draft workflow only exists locally. Save it from Action or Release before leaving, or leave anyway and discard the draft.'
        : null,
    };
  }, [payload, persistedWorkflowId, state.compileProse, state.selectedTrigger]);

  // Load live catalog from backend on mount.
  useEffect(() => {
    let cancelled = false;
    loadCatalog().then((items) => {
      if (!cancelled) setCatalog(items);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  // Self-heal the current tab after a transient catalog miss instead of
  // leaving the trigger picker stranded on an empty cache.
  useEffect(() => {
    if (catalog.length > 0) return undefined;
    let cancelled = false;
    const timer = window.setTimeout(() => {
      loadCatalog().then((items) => {
        if (!cancelled && items.length > 0) setCatalog(items);
      });
    }, 2500);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [catalog.length]);

  useEffect(() => {
    if (state.emptyMode !== 'trigger-picker' || catalog.length > 0) return undefined;
    let cancelled = false;
    loadCatalog().then((items) => {
      if (!cancelled && items.length > 0) setCatalog(items);
    });
    return () => {
      cancelled = true;
    };
  }, [catalog.length, state.emptyMode]);

  useEffect(() => {
    const syncProfile = () => setMoonGlowProfile(readMoonGlowProfile());
    const onStorage = (event: StorageEvent) => {
      if (event.key === null || event.key === 'moon-glow-profile') {
        syncProfile();
      }
    };
    syncProfile();
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  }, []);

  useEffect(() => {
    onDraftStateChange?.(draftGuardState);
  }, [draftGuardState, onDraftStateChange]);

  useEffect(() => () => {
    onDraftStateChange?.({ dirty: false, message: null });
  }, [onDraftStateChange]);

  useEffect(() => {
    if (!mutationError) return;
    const t = setTimeout(() => setMutationError(null), 5000);
    return () => clearTimeout(t);
  }, [mutationError]);

  useEffect(() => {
    if (!error) return;
    setMutationError((current) => current ?? error);
  }, [error]);

  // Sync external runId prop (from URL /app/run/:id) with reducer state.
  // Enter run view when the URL brings in a new run_id; exit when it clears.
  useEffect(() => {
    if (runId && runId !== state.activeRunId) {
      dispatch({ type: 'ENTER_RUN_VIEW', runId, source: 'url' });
    } else if (!runId && state.viewMode === 'run' && state.runViewSource === 'url') {
      dispatch({ type: 'EXIT_RUN_VIEW' });
    }
    // Intentionally depend only on the prop — internal DISPATCH_SUCCESS
    // paths manage their own state transitions.
  }, [runId]);

  // Live run snapshot — active when a dispatch has produced a run OR when
  // the URL route is showing an existing run via /app/run/:id.
  const { run: activeRun, loading: activeRunLoading, error: activeRunError } = useLiveRunSnapshot(state.activeRunId);
  const runJobs: RunJobStatus[] | undefined = useMemo(() => {
    if (!activeRun?.jobs?.length) return undefined;
    return activeRun.jobs.map(j => ({ label: j.label, status: j.status }));
  }, [activeRun]);

  const viewModel = useMemo(
    () => {
      if (state.viewMode === 'run') {
        // Run mode: render the run's DAG with status-tinted rings.
        return presentRun(activeRun, state.selectedRunJobId);
      }
      return presentBuild(payload, state.selectedNodeId, state.activeNodeId, runJobs);
    },
    [
      state.viewMode,
      activeRun,
      state.selectedRunJobId,
      payload,
      state.selectedNodeId,
      state.activeNodeId,
      runJobs,
    ],
  );

  const contractSuggestionExtras = useMemo(
    () =>
      payload
        ? {
            compiledSpec:
              payload.compiled_spec_projection?.compiled_spec ?? payload.compiled_spec ?? null,
            buildIssues: payload.build_issues ?? null,
          }
        : null,
    [payload],
  );

  const runMutation = useCallback(async (subpath: string, body: Record<string, unknown>) => {
    try {
      return await mutate(subpath, body);
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
      throw err;
    }
  }, [mutate]);
  const moonUndoScope = workflowId ? `moon:${workflowId}` : 'moon:draft';

  useEffect(() => registerUiActionUndoExecutor('moon.payload.restore', (descriptor) => {
    if (descriptor.scope !== moonUndoScope) return false;
    setPayload(descriptor.payload as BuildPayload | null);
    return true;
  }), [moonUndoScope, setPayload]);

  useEffect(() => registerUiActionUndoExecutor('workflow.buildMutation', async (descriptor) => {
    if (!workflowId || descriptor.workflowId !== workflowId) return false;
    await runMutation(descriptor.subpath, descriptor.body);
    return true;
  }), [runMutation, workflowId]);

  const buildMutationUndoDescriptor = useCallback((nextPayload: BuildPayload | null): UiActionUndoDescriptor | null => {
    if (!Array.isArray(nextPayload?.undo_receipt?.steps) || !nextPayload?.undo_receipt?.workflow_id) {
      return null;
    }
    return {
      kind: 'sequence',
      steps: nextPayload.undo_receipt.steps.map((step) => ({
        kind: 'workflow.buildMutation' as const,
        workflowId: nextPayload.undo_receipt!.workflow_id,
        subpath: step.subpath,
        body: step.body,
      })),
    } satisfies UiActionUndoDescriptor;
  }, []);

  const buildDraftGraphUndoDescriptor = useCallback((previousPayload: BuildPayload | null): UiActionUndoDescriptor => ({
    kind: 'moon.payload.restore',
    scope: moonUndoScope,
    payload: previousPayload,
  }), [moonUndoScope]);

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

  const applyGraphPayload = useCallback(async (nextPayload: BuildPayload | null) => {
    setPayload(nextPayload);
    if (!workflowId) return;
    const nextGraph = nextPayload?.build_graph;
    await runMutation('build_graph', {
      nodes: nextGraph?.nodes || [],
      edges: nextGraph?.edges || [],
    });
  }, [runMutation, setPayload, workflowId]);

  const handleUndoAction = useCallback((entryId: string, label: string) => {
    void (async () => {
      const result = await undoUiAction(entryId);
      if (!result.ok) {
        show(result.error || 'Undo failed.', 'error');
        return;
      }
      show(`Undid ${label}.`, 'success');
    })();
  }, [show]);

  const commitMoonGraphAction = useCallback(async (details: {
    label: string;
    reason: string;
    outcome: string;
    nextPayload: BuildPayload | null;
    afterApply?: () => void;
    afterUndo?: () => void;
    authority?: string;
    target?: UiActionTarget | null;
    changeSummary?: string[];
  }) => {
    const previousPayload = payload ? structuredClone(payload) as BuildPayload : null;
    const nextPayload = details.nextPayload ? structuredClone(details.nextPayload) as BuildPayload : null;
    let undoDescriptor: UiActionUndoDescriptor | null = null;
    const entry = await runUiAction({
      surface: 'moon',
      undoScope: moonUndoScope,
      category: 'graph',
      label: details.label,
      authority: details.authority || 'build.build_graph',
      reason: details.reason,
      outcome: details.outcome,
      target: details.target ?? null,
      changeSummary: details.changeSummary,
      apply: async () => {
        if (workflowId) {
          const persistedPayload = await runMutation('build_graph', {
            nodes: nextPayload?.build_graph?.nodes || [],
            edges: nextPayload?.build_graph?.edges || [],
          });
          undoDescriptor = buildMutationUndoDescriptor(persistedPayload ?? null);
        } else {
          await applyGraphPayload(nextPayload);
          undoDescriptor = buildDraftGraphUndoDescriptor(previousPayload);
        }
        details.afterApply?.();
      },
      buildUndoDescriptor: () => undoDescriptor,
      onUndone: details.afterUndo,
    });
    show(`${details.label}: ${details.outcome}`, 'info', {
      actionLabel: 'Undo',
      durationMs: 5000,
      onAction: () => handleUndoAction(entry.id, details.label),
    });
    return entry;
  }, [applyGraphPayload, buildDraftGraphUndoDescriptor, buildMutationUndoDescriptor, handleUndoAction, moonUndoScope, payload, runMutation, show, workflowId]);

  const commitMoonAuthorityAction = useCallback(async (details: {
    subpath: string;
    body: Record<string, unknown>;
    label: string;
    reason: string;
    outcome: string;
    authority?: string;
    afterApply?: () => void;
    target?: UiActionTarget | null;
    changeSummary?: string[];
  }) => {
    let undoDescriptor: UiActionUndoDescriptor | null = null;
    const entry = await runUiAction({
      surface: 'moon',
      undoScope: moonUndoScope,
      category: 'authority',
      label: details.label,
      authority: details.authority || `build.${details.subpath}`,
      reason: details.reason,
      outcome: details.outcome,
      target: details.target ?? null,
      changeSummary: details.changeSummary,
      apply: async () => {
        const nextPayload = await runMutation(details.subpath, details.body);
        undoDescriptor = buildMutationUndoDescriptor(nextPayload ?? null);
        details.afterApply?.();
      },
      buildUndoDescriptor: () => undoDescriptor,
    });
    if (entry.undoable) {
      show(`${details.label}: ${details.outcome}`, 'info', {
        actionLabel: 'Undo',
        durationMs: 5000,
        onAction: () => handleUndoAction(entry.id, details.label),
      });
      return;
    }
    show(`${details.label}: ${details.outcome}`, 'success');
  }, [buildMutationUndoDescriptor, handleUndoAction, moonUndoScope, runMutation, show]);

  // Auto-advance active node:
  // - after compile (advanceQueued=true)
  // - after node action changes firstUnresolvedId
  // - initial load when no active node set
  //
  // Skipped entirely in run view — run-mode selection is user-driven via
  // SELECT_RUN_JOB; the build-mode "next unresolved step" concept doesn't apply.
  useEffect(() => {
    if (state.viewMode === 'run') return;
    if (pinnedSelectionRef.current) {
      const pinnedNodeId = pinnedSelectionRef.current;
      if (state.selectedNodeId === pinnedNodeId) {
        return;
      }
      pinnedSelectionRef.current = null;
    }
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
  }, [state.viewMode, state.advanceQueued, state.activeNodeId, viewModel.firstUnresolvedId, viewModel.nodes]);

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
      const summary = nodes[idx].summary || catalogItem?.description || '';
      const previousActiveNodeId = state.activeNodeId;
      const previousOpenDock = state.openDock;
      const scaffold = scaffoldMoonPrimitiveNode(nodes[idx], { actionValue, title, summary });

      // Update the node locally
      nodes[idx] = {
        ...nodes[idx],
        route: actionValue,
        status: 'ready',
        title,
        summary,
        trigger: buildTriggerConfig(actionValue, nodes[idx].trigger),
        ...scaffold,
      };

      // If no unresolved nodes remain after this, add a new empty one
      const hasUnresolved = nodes.some((n, i) => i !== idx && !(n.route || '').trim());
      if (!hasUnresolved) {
        const newId = `node-${nodes.length + 1}`;
        nodes.push({ node_id: newId, kind: 'step', title: 'Next step', route: '', status: '', summary: '' });
        edges.push({ edge_id: `edge-${nodeId}-${newId}`, kind: 'sequence', from_node_id: nodeId, to_node_id: newId });
      }

      await commitMoonGraphAction({
        label: 'Set node action',
        reason: `Assign ${title} to ${nodeDisplayName(nodes[idx])}.`,
        outcome: hasUnresolved
          ? `${nodeDisplayName(nodes[idx])} now runs ${title}.`
          : `${nodeDisplayName(nodes[idx])} now runs ${title}, and Moon appended a new empty next step.`,
        target: nodeTarget(nodes[idx]),
        changeSummary: hasUnresolved
          ? ['Route assignment', title]
          : ['Route assignment', title, 'Appended next step'],
        nextPayload: {
          ...payload,
          build_graph: { ...graph, nodes, edges },
        },
        afterApply: () => {
          pinnedSelectionRef.current = nodeId;
          dispatch({ type: 'SET_ACTIVE', nodeId });
          dispatch({ type: 'SELECT_NODE', nodeId });
          dispatch({ type: 'CLOSE_POPOUT' });
          dispatch({ type: 'OPEN_DOCK', dock: 'context' });
        },
        afterUndo: () => {
          dispatch({ type: 'SET_ACTIVE', nodeId: previousActiveNodeId });
          dispatch({ type: 'SELECT_NODE', nodeId });
          if (previousOpenDock) {
            dispatch({ type: 'OPEN_DOCK', dock: previousOpenDock });
          }
        },
      });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [catalog, commitMoonGraphAction, payload, state.activeNodeId, state.openDock]);

  const handleCreateBranch = useCallback(async (edgeId: string, branchSide: BranchSide) => {
    if (!payload?.build_graph) return;

    try {
      const graph = payload.build_graph;
      const nodes = [...(graph.nodes || [])];
      const edges = [...(graph.edges || [])];
      const idx = edges.findIndex((e: any) => e.edge_id === edgeId);
      if (idx < 0) return;

      const edge = edges[idx];
      const currentRelease = normalizeBuildEdgeRelease(edge);
      const condition = cloneBranchCondition(baseConditionFromRelease(currentRelease));
      const sourceNode = nodes.find(node => node.node_id === edge.from_node_id);
      const targetNode = nodes.find(node => node.node_id === edge.to_node_id);
      edges[idx] = withBuildEdgeRelease(edge, {
        family: 'conditional',
        edge_type: 'conditional',
        state: 'configured',
        label: branchLabel('then') || 'Then',
        branch_reason: 'then',
        release_condition: cloneBranchCondition(condition),
        config: {
          ...(currentRelease.config || {}),
          condition: cloneBranchCondition(condition),
          branch_side: oppositeBranchSide(branchSide),
        },
      });

      const hasSiblingBranch = edges.some(other =>
        other.edge_id !== edge.edge_id &&
        other.from_node_id === edge.from_node_id &&
        normalizeBuildEdgeRelease(other).family === 'conditional',
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
        edges.push(withBuildEdgeRelease({
          edge_id: nextGraphEdgeId(edges, edge.from_node_id, elseNodeId),
          kind: 'sequence',
          from_node_id: edge.from_node_id,
          to_node_id: elseNodeId,
        }, {
          family: 'conditional',
          edge_type: 'conditional',
          state: 'configured',
          label: branchLabel('else') || 'Else',
          branch_reason: 'else',
          release_condition: { op: 'not', conditions: [cloneBranchCondition(condition)] },
          config: {
            condition: cloneBranchCondition(condition),
            branch_side: branchSide,
          },
        }));

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

      await commitMoonGraphAction({
        label: 'Create branch',
        reason: `Convert the path from ${nodeDisplayName(sourceNode)} into a conditional split.`,
        outcome: `Moon created conditional branches from ${nodeDisplayName(sourceNode)}.`,
        target: edgeTarget(edge, graph),
        changeSummary: hasSiblingBranch
          ? ['Conditional gate']
          : ['Conditional gate', 'Created else path'],
        nextPayload: {
          ...payload,
          build_graph: { ...graph, nodes, edges },
        },
        afterApply: () => {
          dispatch({ type: 'SELECT_EDGE', edgeId });
          dispatch({ type: 'OPEN_DOCK', dock: 'context' });
        },
        afterUndo: () => {
          dispatch({ type: 'SELECT_EDGE', edgeId });
          dispatch({ type: 'OPEN_DOCK', dock: 'context' });
        },
      });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [commitMoonGraphAction, payload]);

  /**
   * Create a brand-new downstream step + edge from an existing source node,
   * guarded by the picked gate family. This is the "branch again" affordance
   * — unlike handleCreateBranch (which mutates an existing edge into a
   * conditional then/else pair), this always adds a fresh sibling branch so
   * a node can fan out to 3, 4, N next steps.
   */
  const handleCreateSiblingBranch = useCallback(async (sourceNodeId: string, gateFamily: string) => {
    if (!payload?.build_graph) return;
    try {
      const graph = payload.build_graph;
      const nodes = [...(graph.nodes || [])];
      const edges = [...(graph.edges || [])];
      const sourceNode = nodes.find(node => node.node_id === sourceNodeId);
      if (!sourceNode) return;

      const siblingCount = edges.filter(e => e.from_node_id === sourceNodeId).length;
      const familyLabel = (() => {
        switch (gateFamily) {
          case 'conditional': return 'Condition';
          case 'after_failure': return 'On failure';
          case 'after_any': return 'On any';
          default: return 'On success';
        }
      })();

      const newNodeId = nextGraphNodeId(nodes, 'branch');
      const newNode: BuildNode = {
        node_id: newNodeId,
        kind: 'step',
        title: `${familyLabel} path`,
        summary: `Runs from ${nodeDisplayName(sourceNode)} when the ${familyLabel.toLowerCase()} branch fires.`,
        route: '',
        status: '',
      };
      nodes.push(newNode);

      const baseEdge: BuildEdge = {
        edge_id: nextGraphEdgeId(edges, sourceNodeId, newNodeId),
        kind: 'sequence',
        from_node_id: sourceNodeId,
        to_node_id: newNodeId,
      };

      let releasedEdge: BuildEdge;
      if (gateFamily === 'conditional') {
        const condition = cloneBranchCondition(null);
        releasedEdge = withBuildEdgeRelease(baseEdge, {
          family: 'conditional',
          edge_type: 'conditional',
          state: 'configured',
          label: branchLabel('then') || 'Then',
          branch_reason: 'then',
          release_condition: cloneBranchCondition(condition),
          config: {
            condition: cloneBranchCondition(condition),
            branch_side: siblingCount % 2 === 0 ? 'below' : 'above',
          },
        });
      } else {
        const edgeType: 'after_success' | 'after_failure' | 'after_any' =
          gateFamily === 'after_failure' ? 'after_failure'
            : gateFamily === 'after_any' ? 'after_any'
              : 'after_success';
        releasedEdge = withBuildEdgeRelease(baseEdge, {
          family: gateFamily,
          edge_type: edgeType,
          state: 'configured',
          label: familyLabel,
          branch_reason: undefined,
          release_condition: { kind: 'always' },
          config: {},
        });
      }
      edges.push(releasedEdge);

      await commitMoonGraphAction({
        label: 'Add branch',
        reason: `Add a ${familyLabel.toLowerCase()} branch out of ${nodeDisplayName(sourceNode)}.`,
        outcome: `${nodeDisplayName(sourceNode)} now has ${siblingCount + 1} outgoing branch${siblingCount === 0 ? '' : 'es'}.`,
        target: nodeTarget(sourceNode),
        changeSummary: ['Branch family', familyLabel],
        nextPayload: {
          ...payload,
          build_graph: { ...graph, nodes, edges },
        },
        afterApply: () => {
          setBranchPickerNodeId(null);
          dispatch({ type: 'SELECT_NODE', nodeId: newNodeId });
        },
        afterUndo: () => {
          setBranchPickerNodeId(null);
          dispatch({ type: 'SELECT_NODE', nodeId: sourceNodeId });
        },
      });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [commitMoonGraphAction, payload]);

  const handleApplyGate = useCallback(async (edgeId: string, gateFamily: string) => {
    if (!payload?.build_graph) return;

    try {
      if (gateFamily === 'conditional') {
        await handleCreateBranch(edgeId, 'below');
        return;
      }

      const gateItem = catalog.find(c => c.gateFamily === gateFamily);
      const gateLabel = gateItem?.label || gateFamily;
      const graph = payload.build_graph;
      const edges = [...(graph.edges || [])];
      const idx = edges.findIndex((e: any) => e.edge_id === edgeId);
      if (idx >= 0) {
        edges[idx] = withBuildEdgeRelease(edges[idx], {
          family: gateFamily,
          edge_type: gateFamily === 'after_failure' ? 'after_failure' : 'after_success',
          state: 'configured',
          label: gateLabel,
          branch_reason: undefined,
          release_condition: { kind: 'always' },
          config: {},
        });
        const edge = edges[idx];
        const fromNode = (graph.nodes || []).find((node) => node.node_id === edge.from_node_id);
        const toNode = (graph.nodes || []).find((node) => node.node_id === edge.to_node_id);
        await commitMoonGraphAction({
          label: 'Configure gate',
          reason: `Apply ${gateLabel} to the connection from ${nodeDisplayName(fromNode)} to ${nodeDisplayName(toNode)}.`,
          outcome: `That edge now enforces ${gateLabel}.`,
          target: edgeTarget(edge, graph),
          changeSummary: ['Gate family', gateLabel],
          nextPayload: {
            ...payload,
            build_graph: { ...graph, edges },
          },
          afterApply: () => dispatch({ type: 'CLOSE_POPOUT' }),
          afterUndo: () => {
            dispatch({ type: 'SELECT_EDGE', edgeId });
            dispatch({ type: 'OPEN_DOCK', dock: 'context' });
          },
        });
      }
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [catalog, commitMoonGraphAction, handleCreateBranch, payload]);

  const handleCompile = useCallback(async () => {
    if (!state.compileProse.trim()) return;
    dispatch({ type: 'COMPILE_START' });
    try {
      const prefix = state.selectedTrigger
        ? `Starting with a ${state.selectedTrigger.label} trigger: `
        : '';
      const result = await compileDefinition(prefix + state.compileProse.trim(), {
        workflowId,
      });
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
  }, [onWorkflowCreated, setPayload, state.compileProse, state.selectedTrigger, workflowId]);

  const handleTriggerSelect = useCallback((item: CatalogItem) => {
    const trigger = {
      id: item.id, label: item.label, icon: item.icon, actionValue: item.actionValue!,
    };
    const nextPayload = {
      build_graph: {
        nodes: [
          { node_id: 'node-1', kind: 'step', title: item.label, route: item.actionValue, trigger: buildTriggerConfig(item.actionValue), status: 'ready', summary: `${item.label} trigger` },
          { node_id: 'node-2', kind: 'step', title: 'Next step', route: '', status: '', summary: '' },
        ],
        edges: [
          { edge_id: 'edge-1-2', kind: 'sequence', from_node_id: 'node-1', to_node_id: 'node-2' },
        ],
      },
      build_state: 'draft',
      definition: {},
      build_issues: [],
      build_blockers: [],
      authority_attachments: [],
      binding_ledger: [],
      import_snapshots: [],
    } as BuildPayload;
    void commitMoonGraphAction({
      label: 'Choose trigger',
      reason: `Start the workflow from a ${item.label} trigger.`,
      outcome: `Moon created a two-step draft with ${item.label} as the first node.`,
      target: {
        kind: 'trigger',
        label: item.label,
        id: item.id,
      },
      changeSummary: ['Seeded draft graph', item.label],
      nextPayload,
      afterApply: () => {
        dispatch({ type: 'SELECT_TRIGGER', trigger });
        dispatch({ type: 'COMPILE_SUCCESS' });
      },
      afterUndo: () => {
        dispatch({ type: 'EMPTY_RESET' });
        dispatch({ type: 'SELECT_NODE', nodeId: null });
        dispatch({ type: 'SET_ACTIVE', nodeId: null });
        dispatch({ type: 'CLOSE_DOCK' });
      },
    });
  }, [commitMoonGraphAction]);

  const triggerMenuSections = useMemo<MenuSection[]>(() => [
    {
      id: 'trigger',
      title: 'Triggers',
      items: catalog
        .filter((item) => item.family === 'trigger' && getCatalogSurfacePolicy(item).tier === 'primary')
        .map((item) => {
          const truth = getCatalogTruth(item);
          const policy = getCatalogSurfacePolicy(item);
          return {
            id: item.id,
            label: item.label,
            description: policy.detail,
            keywords: [item.actionValue || '', item.family, item.status, item.connectionStatus || '', truth.badge, policy.badge],
            disabled: item.status !== 'ready',
            selected: state.selectedTrigger?.id === item.id,
            meta: item.status === 'coming_soon'
              ? 'Soon'
              : item.source === 'integration' && item.connectionStatus && item.connectionStatus !== 'connected'
                ? item.connectionStatus
                : truth.badge,
            icon: <MoonGlyph type={item.icon} size={16} color={item.status === 'ready' ? 'currentColor' : 'var(--moon-status-idle)'} />,
            onSelect: () => handleTriggerSelect(item),
          };
        }),
    },
  ], [catalog, handleTriggerSelect, state.selectedTrigger?.id]);

  const selectedNodeAnchorRect = useMemo(() => {
    if (!state.selectedNodeId || !centerRef.current) return null;
    const selectedNode = viewModel.nodes.find((node) => node.id === state.selectedNodeId);
    if (!selectedNode) return null;
    return getMoonNodeAnchorRect(centerRef.current.getBoundingClientRect(), selectedNode);
  }, [state.selectedNodeId, viewModel.nodes]);

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

  // Click fallback: if a catalog item is staged, clicking a node applies it.
  // In run-view mode, clicking a node selects the run job for the detail dock
  // (no catalog/build mutations apply).
  const handleNodeClick = useCallback((nodeId: string, isSelected: boolean) => {
    if (state.viewMode === 'run') {
      dispatch({ type: 'SELECT_RUN_JOB', jobId: isSelected ? null : nodeId });
      return;
    }
    if (state.pendingCatalogId) {
      const catalogId = state.pendingCatalogId;
      dispatch({ type: 'CLEAR_CATALOG' });
      dispatch({ type: 'SELECT_NODE', nodeId });
      applyCatalogToNode(catalogId, nodeId);
      return;
    }
    // Unresolved nodes need the action-picker popout; SELECT_NODE suppresses it
    // while a dock is open, so close the stale context dock first.
    const clicked = viewModel.nodes.find(n => n.id === nodeId);
    const isUnresolved = !!clicked && !(clicked.route || '').trim();
    if (isUnresolved && state.openDock !== null) {
      dispatch({ type: 'CLOSE_DOCK' });
    }
    if (isSelected && nodeId !== state.activeNodeId) {
      dispatch({ type: 'SELECT_NODE', nodeId: null });
    } else {
      dispatch({ type: 'SELECT_NODE', nodeId });
    }
  }, [state.viewMode, state.pendingCatalogId, state.activeNodeId, state.openDock, applyCatalogToNode, viewModel.nodes]);

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
      await commitMoonGraphAction({
        label: 'Append node',
        reason: `Add a new step after ${nodeDisplayName(lastStep)}.`,
        outcome: `${nodeDisplayName(newNode)} was appended to the chain.`,
        target: nodeTarget(newNode),
        changeSummary: ['Appended step', `After ${nodeDisplayName(lastStep)}`],
        nextPayload: {
          ...payload,
          build_graph: { ...graph, nodes: newNodes, edges: newEdges },
        },
        afterApply: () => dispatch({ type: 'SELECT_NODE', nodeId: newId }),
        afterUndo: () => dispatch({ type: 'SELECT_NODE', nodeId: lastStep?.node_id || null }),
      });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [commitMoonGraphAction, payload]);

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
      await commitMoonGraphAction({
        label: 'Reorder nodes',
        reason: `Move ${nodeDisplayName(moved)} in front of ${nodeDisplayName(nodes[toIdx])}.`,
        outcome: `${nodeDisplayName(moved)} moved to a new position in the dominant path.`,
        target: nodeTarget(moved),
        changeSummary: ['Dominant path order', `Before ${nodeDisplayName(nodes[toIdx])}`],
        nextPayload: {
          ...payload,
          build_graph: { ...graph, nodes, edges },
        },
        afterApply: () => dispatch({ type: 'SELECT_NODE', nodeId: moved.node_id }),
        afterUndo: () => dispatch({ type: 'SELECT_NODE', nodeId: sourceNodeId }),
      });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Mutation failed');
    }
  }, [commitMoonGraphAction, payload]);

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

  // Pan the canvas so the selected node is centered in the viewport.
  const focusedNodeId = state.viewMode === 'run' ? state.selectedRunJobId : state.selectedNodeId;
  useEffect(() => {
    const container = centerRef.current;
    if (!container || !focusedNodeId) return;
    const el = container.querySelector<HTMLElement>(`[data-drop-node="${CSS.escape(focusedNodeId)}"]`);
    if (!el) return;
    const cRect = container.getBoundingClientRect();
    const nRect = el.getBoundingClientRect();
    const targetLeft = container.scrollLeft + (nRect.left + nRect.width / 2) - (cRect.left + cRect.width / 2);
    const targetTop = container.scrollTop + (nRect.top + nRect.height / 2) - (cRect.top + cRect.height / 2);
    container.scrollTo({ left: targetLeft, top: targetTop, behavior: 'smooth' });
  }, [focusedNodeId, centerWidth]);

  // Translate chain so active node sits at visual center of container
  // Use 50% CSS fallback when JS measurement isn't ready yet
  const hasMeasured = centerWidth > 0;
  const translateX = useMemo(() => {
    // Center on active node, or first node if no active node
    const idx = activeIdx >= 0 ? activeIdx : 0;
    const activePos = idx * MOON_LAYOUT.nodeSpacing + MOON_LAYOUT.nodeSize / 2;
    if (!hasMeasured) return -activePos;
    const containerCenter = centerWidth / 2;
    return containerCenter - activePos;
  }, [activeIdx, centerWidth, hasMeasured]);

  const hasNodes = viewModel.nodes.length > 0;
  const showComposePanel = !hasNodes && state.emptyMode === 'compose';
  const actionOpen = state.openDock === 'action';
  const contextOpen = state.openDock === 'context';
  const releaseOpen = state.releaseOpen;
  const compiling = state.compilePhase === 'compiling';
  const previewTargetId = drag.drag.hoveredTarget?.id ?? null;
  const previewEdgeId = drag.drag.hoveredTarget?.zone === 'edge'
    ? drag.drag.hoveredTarget.id
    : null;
  const nodeById = useMemo(
    () => new Map(viewModel.nodes.map((node) => [node.id, node] as const)),
    [viewModel.nodes],
  );
  const gateCatalogByFamily = useMemo(() => {
    const byFamily = new Map<string, CatalogItem>();
    for (const item of catalog) {
      if (item.dropKind === 'edge' && item.gateFamily) byFamily.set(item.gateFamily, item);
    }
    return byFamily;
  }, [catalog]);
  const primaryGateModels = useMemo(() => {
    return catalog
      .filter(item => item.family === 'control' && item.status === 'ready')
      .map(item => ({
        item,
        truth: getCatalogTruth(item),
        policy: getCatalogSurfacePolicy(item),
      }))
      .filter(({ policy }) => policy.tier === 'primary');
  }, [catalog]);

  // Selected edge for gate config in Detail dock
  const selectedEdge = state.selectedEdgeId
    ? viewModel.edges.find(e => e.id === state.selectedEdgeId) || null
    : null;
  const edgeFromNode = selectedEdge ? viewModel.nodes.find(n => n.id === selectedEdge.from) : null;
  const edgeToNode = selectedEdge ? viewModel.nodes.find(n => n.id === selectedEdge.to) : null;
  const dismissSelectedEdgeMenus = useCallback(() => {
    if (!state.selectedEdgeId) return;
    dispatch({ type: 'SELECT_EDGE', edgeId: null });
    if (state.openDock === 'context') {
      dispatch({ type: 'CLOSE_DOCK' });
    }
  }, [state.openDock, state.selectedEdgeId]);
  const handleSelectEdge = useCallback((edgeId: string, options?: { openDetail?: boolean }) => {
    dispatch({ type: 'SELECT_EDGE', edgeId });
    if (options?.openDetail) {
      dispatch({ type: 'OPEN_DOCK', dock: 'context' });
    }
  }, []);
  useEffect(() => {
    if (!state.selectedEdgeId) return;

    const handlePointerDown = (event: MouseEvent) => {
      if (shouldKeepEdgeMenusOpen(event.target)) return;
      dismissSelectedEdgeMenus();
    };

    document.addEventListener('mousedown', handlePointerDown);
    return () => {
      document.removeEventListener('mousedown', handlePointerDown);
    };
  }, [dismissSelectedEdgeMenus, state.selectedEdgeId]);

  // Dismiss the node-scoped branch picker on any click outside its pod.
  useEffect(() => {
    if (!branchPickerNodeId) return;
    const handlePointerDown = (event: MouseEvent) => {
      const target = event.target;
      if (target instanceof Element && target.closest('.moon-node-branch-pod')) return;
      setBranchPickerNodeId(null);
    };
    document.addEventListener('mousedown', handlePointerDown);
    return () => document.removeEventListener('mousedown', handlePointerDown);
  }, [branchPickerNodeId]);
  const edgeControls = useMemo(() => viewModel.edges.flatMap((edge) => {
    if (state.viewMode === 'run') return [];
    const geometry = getEdgeGeometry(edge, viewModel.layout);
    if (!geometry) return [];

    const gateItem = edge.gateFamily ? gateCatalogByFamily.get(edge.gateFamily) || null : null;
    const gateTruth = gateItem ? getCatalogTruth(gateItem) : null;
    const gatePolicy = gateItem ? getCatalogSurfacePolicy(gateItem) : null;

    return [{
      centerX: geometry.centerX,
      centerY: geometry.centerY,
      edge,
      fromLabel: nodeById.get(edge.from)?.title || edge.from,
      gateItem,
      gatePolicy,
      gateTruth,
      label: gatePodLabel(edge, gateItem),
      toLabel: nodeById.get(edge.to)?.title || edge.to,
      tone: gatePodTone(edge, gateItem),
    }];
  }), [gateCatalogByFamily, nodeById, state.viewMode, viewModel.edges, viewModel.layout]);
  const appendPosition = useMemo(
    () => getMoonAppendPosition(viewModel.layout.width),
    [viewModel.layout.width],
  );
  const middleClassName = [
    'moon-middle',
    actionOpen ? 'moon-middle--action-open' : '',
    contextOpen ? 'moon-middle--context-open' : '',
    releaseOpen ? 'moon-middle--release-open' : '',
  ].filter(Boolean).join(' ');

  // Run-view cancel: fires the public v1 cancel endpoint. The run's next
  // SSE event will reflect the terminal status automatically.
  const handleRunCancel = useCallback(async () => {
    const runIdToCancel = state.activeRunId;
    if (!runIdToCancel) return;
    try {
      await fetch(`/v1/runs/${encodeURIComponent(runIdToCancel)}:cancel`, { method: 'POST' });
    } catch (err) {
      setMutationError(err instanceof Error ? err.message : 'Cancel failed');
    }
  }, [state.activeRunId]);

  const handleRunExit = useCallback(() => {
    dispatch({ type: 'EXIT_RUN_VIEW' });
    if (onBack) onBack();
  }, [onBack]);

  return (
    <div
      className={`moon-page${state.viewMode === 'run' ? ' moon-page--run-view' : ''}`}
      style={MOON_LAYOUT_CSS_VARS}
      data-moon-glow-profile={moonGlowProfile}
    >
      {mutationError && (
        <div className="moon-error-toast" role="alert" aria-live="polite">
          {mutationError}
          <button type="button" onClick={() => setMutationError(null)} aria-label="Dismiss error">
            &times;
          </button>
        </div>
      )}
      {state.viewMode === 'run' && (
        <MoonRunOverlay
          run={activeRun}
          loading={activeRunLoading}
          error={activeRunError}
          selectedJobId={state.selectedRunJobId}
          onSelectJob={(jobId) => dispatch({ type: 'SELECT_RUN_JOB', jobId })}
          onExit={handleRunExit}
          onCancel={handleRunCancel}
        />
      )}
      <div className="moon-body">
        {/* Middle row */}
        <div className={middleClassName} data-testid="moon-middle">
          {/* Left dock: Action (Overlay) */}
          <div className={`moon-dock-overlay moon-dock-overlay--left${actionOpen ? ' moon-dock-overlay--open' : ''}`}>
            {actionOpen && (
              <MoonActionDock
                workflowId={workflowId}
                payload={payload}
                selectedNodeId={state.selectedNodeId}
                onReload={reload}

                onClose={() => dispatch({ type: 'CLOSE_DOCK' })}
                onStartCatalogDrag={startCatalogDrag}
                onPayloadChange={(nextPayload) => setPayload(nextPayload)}
                onWorkflowCreated={onWorkflowCreated}
                onCatalogChange={setCatalog}
              />
            )}
          </div>

          {/* Center */}
          <div className={`moon-center${!hasNodes ? ' moon-center--empty' : ''}`} ref={centerRef}>
            {hasNodes && state.viewMode !== 'run' && (
              <>
                <div className="moon-center__dock-actions" aria-label="Workspace panels">
                  <div className="moon-center__dock-group">
                    <DockToggleButton active={actionOpen} label="Action" onClick={() => openDock('action')} />
                    <DockToggleButton active={contextOpen} label="Detail" onClick={() => openDock('context')} />
                  </div>
                </div>
                {!releaseOpen && !state.runViewOpen && <HalfMoon position="bottom" label="Release" onClick={() => dispatch({ type: 'TOGGLE_RELEASE' })} />}
              </>
            )}

            {!hasNodes ? (
              <div className="moon-start">
                {/* Nucleus circle — clickable to pick or change trigger */}
                <div
                  ref={triggerAnchorRef}
                  className={`moon-nucleus${state.emptyMode === 'choice' ? ' moon-nucleus--interactive' : ''}`}
                  onClick={state.emptyMode === 'choice' ? () => dispatch({ type: 'EMPTY_OPEN_SELECTION' }) : undefined}
                >
                  <div className={`moon-nucleus__ring${state.selectedTrigger ? ' moon-nucleus__ring--decided' : ''}`}>
                  </div>
                </div>

                {state.emptyMode === 'selection' && (
                  <div className="moon-selection-window">
                    <button
                      type="button"
                      className="moon-selection-card"
                      onClick={() => dispatch({ type: 'EMPTY_PICK_TRIGGER' })}
                    >
                      <span className="moon-selection-card__title">Pick a trigger</span>
                      <span className="moon-selection-card__desc">Choose a route first if you already know how this workflow starts.</span>
                    </button>
                    <button
                      type="button"
                      className="moon-selection-card"
                      onClick={() => dispatch({ type: 'EMPTY_PICK_COMPOSE' })}
                    >
                      <span className="moon-selection-card__title">Describe it</span>
                      <span className="moon-selection-card__desc">Type plain English. The builder will best-guess the steps, inputs, and outputs.</span>
                    </button>
                  </div>
                )}

                {showComposePanel && (
                  <div className="moon-compose moon-compose--intro" style={{ marginTop: 32 }}>
                    <div className="moon-compose__title">Describe the workflow</div>
                    <div className="moon-compose__hint">
                      Free text is enough. The builder will infer the graph shape, then you can tighten it up in the canvas.
                    </div>
                    <textarea
                      value={state.compileProse}
                      onChange={(e) => dispatch({ type: 'SET_PROSE', prose: e.target.value })}
                      onKeyDown={(event) => {
                        if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
                          event.preventDefault();
                          if (!compiling && state.compileProse.trim()) {
                            void handleCompile();
                          }
                        }
                      }}
                      placeholder="Example: Scrape Gmail, summarize applications, then route each one to the right reviewer."
                      rows={4}
                      disabled={compiling}
                      autoFocus
                    />
                    <div className="moon-compose__actions">
                      <button className="moon-compose__btn" onClick={handleCompile} disabled={compiling || !state.compileProse.trim()}>
                        {compiling ? 'Building...' : 'Build from prompt'}
                      </button>
                    </div>
                    {state.compileError && (
                      <div className="moon-compose__error" role="alert">
                        {state.compileError}
                      </div>
                    )}
                    <div className="moon-compose__shortcut">Press Ctrl/Cmd+Enter to build.</div>
                    <div className="moon-compose__examples" aria-label="Example prompts">
                      {EXAMPLE_PROMPTS.map((prompt) => (
                        <button
                          key={prompt}
                          type="button"
                          className="moon-compose__chip"
                          onClick={() => dispatch({ type: 'SET_PROSE', prose: prompt })}
                        >
                          {prompt}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div
                className="moon-graph"
                style={{
                  position: 'relative',
                  ...getMoonCanvasDimensions(viewModel.layout),
                  margin: '0 auto',
                  minHeight: MOON_LAYOUT.minGraphHeight,
                }}
              >
                <MoonEdges
                  edges={viewModel.edges}
                  layout={viewModel.layout}
                  selectedEdgeId={state.selectedEdgeId}
                  onEdgeClick={handleSelectEdge}
                />
                <div style={{ position: 'absolute', top: 0, left: 0 }}>
                  {edgeControls.map((control) => {
                    const isSelected = control.edge.id === state.selectedEdgeId;
                    const isDragOver = previewEdgeId === control.edge.id;
                    const isEmpty = !control.edge.gateFamily;
                    const isConditional = control.edge.gateFamily === 'conditional';
                    const summary = isEmpty
                      ? 'Pick a gate type to guard this connection.'
                      : control.gatePolicy?.detail || control.gateTruth?.detail || 'This connection already carries gate metadata.';
                    const title = isConditional
                      ? `${control.label} path`
                      : control.label;

                    return (
                      <div
                        key={control.edge.id}
                        className={`moon-graph-gate moon-graph-gate--${control.tone}${isSelected ? ' moon-graph-gate--selected' : ''}${isDragOver ? ' moon-graph-gate--drag-over' : ''}${control.edge.gateFamily ? ` moon-graph-gate--family-${control.edge.gateFamily}` : ''}`}
                        style={{ left: control.centerX, top: control.centerY }}
                        data-drop-edge={control.edge.id}
                        data-gate-family={control.edge.gateFamily || undefined}
                      >
                          <button
                            type="button"
                            className="moon-graph-gate__trigger"
                            onClick={() => handleSelectEdge(control.edge.id)}
                            aria-label={`Select gate between ${control.fromLabel} and ${control.toLabel}`}
                          >
                          <span
                            className={`moon-graph-gate__icon${isEmpty ? ' moon-graph-gate__icon--plus' : ''}`}
                            aria-hidden="true"
                          >
                            {isEmpty ? '+' : <MoonGlyph type={control.gateItem?.icon || gatePodGlyphType(control.edge)} size={12} color="currentColor" />}
                          </span>
                          <span className="moon-graph-gate__trigger-label">{control.label}</span>
                        </button>

                        {isSelected && (
                          <div className="moon-graph-gate__card">
                            <div className="moon-graph-gate__meta">
                              <span className="moon-surface-badge">
                                {isEmpty ? 'Core now' : control.gatePolicy?.badge || 'Gate'}
                              </span>
                              {control.gateTruth && (
                                <span className={`moon-truth-badge moon-truth-badge--${control.gateTruth.category}`}>
                                  {control.gateTruth.badge}
                                </span>
                              )}
                            </div>
                            <div className="moon-graph-gate__title">{title}</div>
                            <div className="moon-graph-gate__path">
                              {control.fromLabel} to {control.toLabel}
                            </div>
                            <div className="moon-graph-gate__summary">{summary}</div>

                            {primaryGateModels.length > 0 && (
                              <div className="moon-graph-gate__actions">
                                <label className="moon-dock-form__label" htmlFor={`moon-gate-family-${control.edge.id}`}>Type</label>
                                <select
                                  id={`moon-gate-family-${control.edge.id}`}
                                  className="moon-dock-form__select"
                                  value={control.edge.gateFamily || ''}
                                  onChange={e => {
                                    const family = e.target.value;
                                    if (family) void handleApplyGate(control.edge.id, family);
                                  }}
                                >
                                  <option value="" disabled>{isEmpty ? 'Choose a gate' : 'Choose gate type'}</option>
                                  {primaryGateModels.map(({ item }) => (
                                    <option key={item.id} value={item.gateFamily}>{item.label}</option>
                                  ))}
                                </select>
                                <label className="moon-dock-form__label" htmlFor={`moon-gate-detail-${control.edge.id}`}>Detail</label>
                                <select
                                  id={`moon-gate-detail-${control.edge.id}`}
                                  className="moon-dock-form__select"
                                  value={control.gateItem?.id || ''}
                                  onChange={e => {
                                    const picked = primaryGateModels.find(({ item }) => item.id === e.target.value);
                                    if (picked?.item.gateFamily) void handleApplyGate(control.edge.id, picked.item.gateFamily);
                                  }}
                                  disabled={!control.edge.gateFamily}
                                >
                                  <option value="" disabled>Pick a type first</option>
                                  {primaryGateModels
                                    .filter(({ item }) => item.gateFamily === control.edge.gateFamily)
                                    .map(({ item, truth, policy }) => (
                                      <option key={item.id} value={item.id}>
                                        {policy.badge} - {truth.badge}
                                      </option>
                                    ))}
                                </select>
                              </div>
                            )}

                            <button
                              type="button"
                              className="moon-graph-gate__detail-button"
                              onClick={() => handleSelectEdge(control.edge.id, { openDetail: true })}
                            >
                              Edit gate
                            </button>
                          </div>
                        )}
                      </div>
                    );
                  })}
                  {viewModel.nodes.map((node) => {
                    const isSelected = state.viewMode === 'run'
                      ? node.id === state.selectedRunJobId
                      : node.id === state.selectedNodeId;
                    const position = getMoonNodeCanvasPosition(node);
                    const multiplicityAttr = node.multiplicity?.kind ?? null;
                    const multiplicityCount = node.multiplicity?.count ?? null;
                    const nodeClass = [
                      'moon-graph-node',
                      ringClass(node, isSelected),
                      isSelected ? 'moon-graph-node--selected' : '',
                      previewTargetId === node.id ? 'moon-graph-node--drag-over' : '',
                      multiplicityAttr ? `moon-graph-node--stack moon-graph-node--stack-${multiplicityAttr}` : '',
                    ].filter(Boolean).join(' ');
                    return (
                      <div
                        key={node.id}
                        className={nodeClass}
                        style={position}
                        data-drop-node={node.id}
                        data-multiplicity={multiplicityAttr || undefined}
                        onClick={() => handleNodeClick(node.id, isSelected)}
                        onPointerDown={state.viewMode === 'run' ? undefined : e => startNodeDrag(e, node)}
                      >
                        {/*
                          Back rings for the multiplicity stack. Rendered as
                          real siblings (not pseudo-elements) so the main ring
                          keeps its flex centering for the glyph/index.
                        */}
                        {multiplicityAttr && (
                          <>
                            <span className="moon-graph-node__stack-ring moon-graph-node__stack-ring--back" aria-hidden="true" />
                            <span className="moon-graph-node__stack-ring moon-graph-node__stack-ring--mid" aria-hidden="true" />
                          </>
                        )}
                        {showIcon(node) ? (
                          <MoonGlyph type={node.glyphType} size={22} color="#fff" />
                        ) : (
                          <span className="moon-chain__step-index">{node.dominantPathIndex >= 0 ? node.dominantPathIndex + 1 : ''}</span>
                        )}
                        {multiplicityAttr && (
                          <span className="moon-graph-node__count-pill" aria-label={`${multiplicityAttr} count`}>
                            {multiplicityCount !== null
                              ? `${multiplicityCount}\u00d7`
                              : multiplicityAttr === 'loop' ? 'N\u00d7' : 'N\u2016'}
                          </span>
                        )}
                        <span className="moon-graph-node__label">{node.title}</span>
                      </div>
                    );
                  })}
                  {/*
                    Node-scoped branch pod. Sits just past a node's right edge
                    and opens a family picker. Unlike the edge pod (which
                    configures an existing edge), this one creates a new
                    outgoing edge + downstream node — the affordance for
                    branching a step 2, 3, N ways.
                  */}
                  {state.viewMode !== 'run' && viewModel.nodes
                    .filter(node => node.kind === 'step')
                    .map((node) => {
                      const position = getMoonNodeCanvasPosition(node);
                      const podLeft = position.left + MOON_LAYOUT.nodeSize + 10;
                      const podTop = position.top + MOON_LAYOUT.nodeRadius - 14;
                      const open = branchPickerNodeId === node.id;
                      return (
                        <div
                          key={`branch-pod-${node.id}`}
                          className={`moon-node-branch-pod${open ? ' moon-node-branch-pod--open' : ''}`}
                          style={{ left: podLeft, top: podTop }}
                          data-keep-edge-menu-open="true"
                        >
                          <button
                            type="button"
                            className="moon-node-branch-pod__trigger"
                            aria-label={`Add branch from ${node.title}`}
                            aria-expanded={open}
                            onClick={(e) => {
                              e.stopPropagation();
                              setBranchPickerNodeId(open ? null : node.id);
                            }}
                          >
                            +
                          </button>
                          {open && (
                            <div className="moon-node-branch-pod__menu" role="menu">
                              <div className="moon-node-branch-pod__title">Branch from {node.title}</div>
                              {[
                                { family: 'after_success', label: 'On success', hint: 'Runs when this step succeeds.' },
                                { family: 'after_failure', label: 'On failure', hint: 'Runs only when this step fails.' },
                                { family: 'conditional',   label: 'Condition',  hint: 'Runs when a condition evaluates true.' },
                                { family: 'after_any',     label: 'On any',     hint: 'Always runs, success or failure.' },
                              ].map(opt => (
                                <button
                                  key={opt.family}
                                  type="button"
                                  role="menuitem"
                                  className={`moon-node-branch-pod__option moon-node-branch-pod__option--${opt.family}`}
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    void handleCreateSiblingBranch(node.id, opt.family);
                                  }}
                                >
                                  <span className="moon-node-branch-pod__option-label">{opt.label}</span>
                                  <span className="moon-node-branch-pod__option-hint">{opt.hint}</span>
                                </button>
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  {state.viewMode !== 'run' && (
                    <div
                      className={`moon-graph-append${previewTargetId === '__append__' ? ' moon-graph-append--drag-over' : ''}`}
                      style={appendPosition}
                      data-drop-append="true"
                      onClick={() => appendNode()}
                    >
                      <span className="moon-graph-append__plus" aria-hidden="true">
                        +
                      </span>
                    </div>
                  )}
                </div>
                <MoonDragGhost drag={drag.drag} />
              </div>
            )}

          </div>

          {/* Right dock: Detail (Overlay) */}
          <div className={`moon-dock-overlay moon-dock-overlay--right${contextOpen ? ' moon-dock-overlay--open' : ''}`}>
            {contextOpen && (
              <MoonNodeDetail
                node={viewModel.selectedNode}
                content={viewModel.dockContent}
                contractSuggestionExtras={contractSuggestionExtras}
                workflowId={workflowId}
                onMutate={async (subpath, body) => {
                  await runMutation(subpath, body);
                }}
                buildGraph={payload?.build_graph}
                onUpdateBuildGraph={updateBuildGraph}
                onCommitGraphAction={async (graph, meta) => {
                  await commitMoonGraphAction({
                    ...meta,
                    nextPayload: payload ? { ...payload, build_graph: graph } : null,
                    afterApply: () => {
                      if (viewModel.selectedNode) {
                        dispatch({ type: 'SELECT_NODE', nodeId: viewModel.selectedNode.id });
                      } else if (selectedEdge) {
                        dispatch({ type: 'SELECT_EDGE', edgeId: selectedEdge.id });
                        dispatch({ type: 'OPEN_DOCK', dock: 'context' });
                      }
                    },
                    afterUndo: () => {
                      if (viewModel.selectedNode) {
                        dispatch({ type: 'SELECT_NODE', nodeId: viewModel.selectedNode.id });
                      } else if (selectedEdge) {
                        dispatch({ type: 'SELECT_EDGE', edgeId: selectedEdge.id });
                        dispatch({ type: 'OPEN_DOCK', dock: 'context' });
                      }
                    },
                  });
                }}
                onCommitAuthorityAction={async (subpath, body, meta) => {
                  await commitMoonAuthorityAction({
                    subpath,
                    body,
                    ...meta,
                  });
                }}
                onClose={() => {
                  if (selectedEdge) dismissSelectedEdgeMenus();
                  else dispatch({ type: 'CLOSE_DOCK' });
                }}
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
              onWorkflowCreated={onWorkflowCreated}
              onClose={() => dispatch({ type: 'TOGGLE_RELEASE' })}
              onSelectNode={(nodeId) => dispatch({ type: 'SELECT_NODE', nodeId })}
              onOpenDock={(dock) => dispatch({ type: 'OPEN_DOCK', dock: dock as 'action' | 'context' })}
              onViewRun={onViewRun}
              onDispatchSuccess={(runId) => dispatch({ type: 'DISPATCH_SUCCESS', runId })}
            />
          ) : null}
        </div>
      </div>

      {state.emptyMode === 'trigger-picker' && (
        <MenuPanel
          open
          anchorRect={triggerAnchorRef.current?.getBoundingClientRect() ?? null}
          title="Choose a trigger"
          subtitle="Start from an event, a schedule, or a manual run."
          emptyLabel={catalog.length === 0 ? 'Loading available triggers…' : 'No matching triggers'}
          searchPlaceholder="Search triggers…"
          sections={triggerMenuSections}
          onClose={() => dispatch({ type: 'EMPTY_RESET' })}
          width={MOON_LAYOUT.triggerMenuWidth}
        />
      )}

      {state.selectedNodeId && state.popoutOpen && viewModel.selectedNode && (
        <MoonPopout
          node={viewModel.selectedNode}
          content={viewModel.dockContent}
          anchorRect={selectedNodeAnchorRect}
          onClose={() => dispatch({ type: 'CLOSE_POPOUT' })}
          onSelect={handleNodeAction}
          catalog={catalog}
          onStartCatalogDrag={startCatalogDrag}
        />
      )}

      <Toast />
    </div>
  );
}
