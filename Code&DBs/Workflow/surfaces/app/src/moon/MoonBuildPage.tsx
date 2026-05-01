import React, { useReducer, useMemo, useCallback, useEffect, useRef, useState } from 'react';
import { useBuildPayload } from '../shared/hooks/useBuildPayload';
import { useObjectTypes } from '../shared/hooks/useObjectTypes';
import { materializePlan, previewCompile } from '../shared/buildController';
import { presentBuild } from './moonBuildPresenter';
import type { MoonBuildViewModel, OrbitNode, OrbitEdge, RunJobStatus } from './moonBuildPresenter';
import { presentRun } from './moonRunPresenter';
import { useLiveRunSnapshot } from '../dashboard/useLiveRunSnapshot';
import { moonBuildReducer, initialMoonBuildState } from './moonBuildReducer';
import { MoonGlyph } from './MoonGlyph';
import { MoonPopout } from './MoonPopout';
import { MoonNodeDetail, type AuthorityActionMeta, type WorkflowInspectorSummary } from './MoonNodeDetail';
import {
  fetchWorkflowContextAuthority,
  workflowContextFromPayload,
  workflowContextSummaryFromAuthority,
} from './workflowContext';
import { MoonActionDock } from './MoonActionDock';
import { MoonReleaseTray } from './MoonReleaseTray';
import { MoonBindingReviewQueue, reviewReadinessCount } from './MoonBindingReviewQueue';
import { MoonRunPanel } from './MoonRunPanel';
import { MoonRunOverlay } from './MoonRunOverlay';
import { MoonDragGhost } from './MoonDragGhost';
import { MoonOutcomeContract } from './MoonOutcomeContract';
import { MoonEdges, edgePresentation, getEdgeGeometry } from './MoonEdges';
import { useMoonDrag } from './useMoonDrag';
import { loadCatalog, getCatalog } from './catalog';
import type { CatalogItem } from './catalog';
import type { BuildNode, BuildEdge, BuildPayload, CompilePreviewPayload } from '../shared/types';
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
  buildAuthorityCompileProse,
  summarizeComposeAuthority,
  type MoonComposeAuthoritySummary,
} from './moonComposeAuthority';
import {
  checkingBuilderValidationStatus,
  fetchWorkflowContextCompositeStatus,
  fetchWorkflowBuilderValidationStatus,
  statusFromCompositeError,
  statusFromBuilderValidationError,
  type ClientOperatingModelBuilderStatus,
  type ClientOperatingModelCompositeStatus,
} from './clientOperatingModel';
import { appendOutcomeContract } from './outcomeContract';
import { buildPrimitiveContractSuggestions } from './moonContractSuggestions';
import { setMoonChatContext, clearMoonChatContext, publishMoonChatHandoff } from './moonChatContext';
import {
  getMoonAppendPosition,
  getMoonCanvasDimensions,
  getMoonNodeAnchorRect,
  getMoonNodeCanvasPosition,
  MOON_LAYOUT,
  MOON_LAYOUT_CSS_VARS,
} from './moonLayout';
import { Toast, useToast } from '../primitives/Toast';
import { Button } from '../primitives';
import { UiActionFeed } from '../control/UiActionFeed';
import {
  registerUiActionUndoExecutor,
  runUiAction,
  undoUiAction,
  type UiActionTarget,
  type UiActionUndoDescriptor,
} from '../control/uiActionLedger';
import './moon-build.css';

interface Props {
  workflowId: string | null;
  /**
   * When set, Moon renders a run-view over its canvas for this run_id.
   * The URL `/app/run/:id` routes here via shell state.
   */
  runId?: string | null;
  onBack?: () => void;
  onWorkflowCreated?: (id: string) => void;
  onEditWorkflow?: (id: string) => void;
  onViewRun?: (runId: string) => void;
  onDraftStateChange?: (draft: { dirty: boolean; message?: string | null }) => void;
  onMaterializeHandoff?: () => void;
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

function buildMaterializeFallbackPrompt(params: {
  intent: string;
  workflowId?: string | null;
  reason: string;
  graphSummary?: Record<string, unknown> | null;
}): string {
  return [
    "Open with: \"I'm looking at this right now. Let me see what I can do.\"",
    'Materialize did not produce a usable agent-authored workflow graph, so you own the recovery pass.',
    params.workflowId ? `Workflow id: ${params.workflowId}` : 'No durable workflow id was confirmed.',
    `Blocker: ${params.reason}`,
    params.graphSummary ? `Graph summary: ${JSON.stringify(params.graphSummary)}` : '',
    'Original operator intent:',
    params.intent,
    '',
    'Do not apply a generic workflow.',
    'If a workflow id exists, first call moon_get_build, the legacy Workflow graph read tool, before making any claim about the graph.',
    'If you can repair the draft, use moon_mutate_field, the legacy Workflow graph mutation tool, one field of one node at a time.',
    'Use one repeated tool lane per packet; do not mix unrelated tools inside a single packet.',
    'If the draft cannot be repaired, say the exact blocker and what Nate can approve manually.',
    'Do not launch the workflow unless the user explicitly asks.',
  ].filter(Boolean).join('\n');
}

function recordFromUnknown(value: unknown): Record<string, any> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, any>
    : {};
}

function materializeFailureInfo(error: unknown): {
  message: string;
  workflowId: string | null;
  receiptId: string | null;
  correlationId: string | null;
  graphSummary: Record<string, unknown> | null;
} {
  const fallbackMessage = error instanceof Error ? error.message : 'Materialize failed';
  const root = recordFromUnknown((error as { response?: unknown })?.response ?? (error as { body?: unknown })?.body);
  const details = recordFromUnknown(root.details ?? root.detail ?? root.result ?? root.payload);
  const operationReceipt = recordFromUnknown(root.operation_receipt ?? details.operation_receipt);
  const graphSummary = recordFromUnknown(root.graph_summary ?? details.graph_summary);
  const workflowId = String(root.workflow_id || details.workflow_id || graphSummary.workflow_id || '').trim() || null;
  const receiptId = String(
    operationReceipt.receipt_id
    || root.operation_receipt_id
    || details.operation_receipt_id
    || root.receipt_id
    || details.receipt_id
    || '',
  ).trim() || null;
  const correlationId = String(
    operationReceipt.correlation_id
    || root.correlation_id
    || details.correlation_id
    || '',
  ).trim() || null;
  const reason = String(root.reason_code || details.reason_code || root.error_code || details.error_code || '').trim();
  const message = reason
    ? `${fallbackMessage} (${reason})`
    : fallbackMessage;
  return {
    message,
    workflowId,
    receiptId,
    correlationId,
    graphSummary: Object.keys(graphSummary).length > 0 ? graphSummary : null,
  };
}

function DockToggleButton({
  active,
  ariaLabel,
  label,
  onClick,
  tone = 'default',
}: {
  active: boolean;
  ariaLabel?: string;
  label: string;
  onClick: () => void;
  tone?: 'default' | 'warning' | 'blocked' | 'ready';
}) {
  const toneClass = tone === 'default' ? '' : ` moon-center__dock-btn--${tone}`;
  return (
    <button
      type="button"
      className={`moon-center__dock-btn${toneClass}${active ? ' moon-center__dock-btn--active' : ''}`}
      data-keep-edge-menu-open="true"
      aria-label={ariaLabel || `Open ${label} dock`}
      aria-pressed={active}
      onClick={onClick}
    >
      <span className="moon-center__dock-btn-label">{label}</span>
    </button>
  );
}

function shortReadableRef(value: unknown): string | null {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return null;
  if (raw.length <= 12) return raw;
  return `${raw.slice(0, 8)}...${raw.slice(-4)}`;
}

function isMechanicalDisplayName(value: string, nodeId?: string): boolean {
  const normalized = value.trim().toLowerCase();
  if (!normalized) return true;
  if (nodeId && normalized === nodeId.trim().toLowerCase()) return true;
  return /^(node|step|task|stage|untitled)[-_ ]?\d*$/i.test(normalized)
    || /^wf[_-]/i.test(normalized)
    || /^[a-f0-9]{8,}$/i.test(normalized);
}

function compactWorkflowLabel(value: string): string {
  const cleaned = value
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return cleaned;
  const replacements: Array<[RegExp, string]> = [
    [/^normalize app identity$/i, 'Normalize app'],
    [/^plan discovery strategy$/i, 'Plan discovery'],
    [/^execute search and retrieval$/i, 'Search & retrieve'],
    [/^evaluate integration feasibility$/i, 'Evaluate fit'],
    [/^build custom integration$/i, 'Build integration'],
    [/^retrieve documentation and api details$/i, 'Retrieve docs'],
    [/^evaluate integration options$/i, 'Evaluate options'],
  ];
  for (const [pattern, label] of replacements) {
    if (pattern.test(cleaned)) return label;
  }
  const sentenceLike = cleaned
    .replace(/\bAnd\b/g, '&')
    .replace(/\bApi\b/g, 'API')
    .replace(/\bUrl\b/g, 'URL');
  if (!/^[A-Z][a-z]+(?: [A-Z][a-z]+){2,}$/.test(sentenceLike)) return sentenceLike;
  const words = sentenceLike.split(' ');
  return words.map((word, index) => (
    index === 0 || /^[A-Z0-9&]+$/.test(word)
      ? word
      : word.toLowerCase()
  )).join(' ');
}

function reviewNodeName(
  node: ({ node_id?: string; id?: string; title?: string } | null | undefined),
  fallbackIndex = 0,
): string {
  const nodeId = node?.node_id ?? node?.id;
  const rawTitle = typeof node?.title === 'string' ? node.title.replace(/\.\s*Step$/i, '').trim() : '';
  if (rawTitle.includes('/')) {
    const compact = rawTitle.split('/').filter(Boolean).pop();
    if (compact) return compact;
  }
  if (!isMechanicalDisplayName(rawTitle, nodeId)) return compactWorkflowLabel(rawTitle);
  const numeric = nodeId?.match(/(\d+)(?!.*\d)/)?.[1];
  return `Step ${numeric ? Number(numeric) : fallbackIndex + 1}`;
}

function workflowReviewTitle(payload: BuildPayload | null): string {
  const workflowName = typeof payload?.workflow?.name === 'string' ? payload.workflow.name.trim() : '';
  if (workflowName && !isMechanicalDisplayName(workflowName)) return workflowName;
  const definition = recordFromUnknown(payload?.definition);
  const title = String(definition.title || definition.name || definition.workflow_name || '').trim();
  return title && !isMechanicalDisplayName(title) ? title : 'Workflow review';
}

function graphDisconnectedCount(graph: NonNullable<BuildPayload['build_graph']> | null | undefined): number {
  const nodes = graph?.nodes || [];
  if (nodes.length <= 1) return 0;
  const connected = new Set<string>();
  for (const edge of graph?.edges || []) {
    connected.add(edge.from_node_id);
    connected.add(edge.to_node_id);
  }
  return nodes.filter((node) => !connected.has(node.node_id)).length;
}

function collectDataPillLabels(payload: BuildPayload | null, limit = 5): string[] {
  const labels: string[] = [];
  const seen = new Set<string>();
  const normalize = (value: string) => value
    .split('/')
    .filter(Boolean)
    .pop()
    ?.replace(/[_-]+/g, ' ')
    .trim()
    || value.trim();
  const add = (_prefix: string, value: unknown) => {
    const label = typeof value === 'string' ? normalize(value) : '';
    if (!label) return;
    const display = label;
    const key = display.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    labels.push(display);
  };
  for (const node of payload?.build_graph?.nodes || []) {
    (node.required_inputs || []).forEach((value) => add('input', value));
    (node.outputs || []).forEach((value) => add('output', value));
    if (node.kind === 'state') {
      add('artifact', node.title);
    }
  }
  if (labels.length < limit) {
    for (const match of payload?.compile_preview?.scope_packet?.matches || []) {
      add(match.object_kind || 'context', match.label || match.span_text);
      if (labels.length >= limit) break;
    }
  }
  return labels.slice(0, limit);
}

function agentToolPlanSummary(viewModel: MoonBuildViewModel): string {
  const toolCounts = new Map<string, number>();
  for (const node of viewModel.nodes) {
    const tool = typeof node.agentToolPlan?.tool_name === 'string' ? node.agentToolPlan.tool_name.trim() : '';
    if (!tool) continue;
    const repeats = typeof node.agentToolPlan?.repeats === 'number' && node.agentToolPlan.repeats > 0
      ? node.agentToolPlan.repeats
      : 1;
    toolCounts.set(tool, (toolCounts.get(tool) || 0) + repeats);
  }
  const first = [...toolCounts.entries()].sort((a, b) => b[1] - a[1])[0];
  return first ? `${first[0]} x${first[1]}` : 'Tool lanes pending';
}

function branchReviewSummary(viewModel: MoonBuildViewModel): string {
  const branchPoints = viewModel.branchBoard;
  if (!branchPoints.length) return 'none';
  const first = branchPoints[0];
  const branchLabels = first.lanes
    .map((lane) => lane.label)
    .slice(0, 3)
    .join(' / ');
  const suffix = branchPoints.length > 1 ? ` +${branchPoints.length - 1}` : '';
  return `${branchPoints.length} split${branchPoints.length > 1 ? 's' : ''}: ${first.sourceTitle || 'step'}${branchLabels ? ` (${branchLabels})` : ''}${suffix}`;
}

function workflowInspectorSummary(
  payload: BuildPayload | null,
  viewModel: MoonBuildViewModel,
  pendingReviewCount: number,
  workflowContextAuthority: ReturnType<typeof workflowContextFromPayload>,
): WorkflowInspectorSummary | null {
  if (!payload) return null;
  const graph = payload.build_graph;
  const stepNodes = viewModel.nodes.filter((node) => node.kind === 'step');
  const workStepCount = stepNodes.length || viewModel.totalNodes;
  const dataPills = collectDataPillLabels(payload);
  const receipt = recordFromUnknown(payload.operation_receipt);
  const receiptId = shortReadableRef(receipt.receipt_id || payload.mutation_event_id || null);
  return {
    title: workflowReviewTitle(payload),
    readiness: viewModel.release.readiness,
    stepCount: workStepCount,
    linkCount: viewModel.edges.length,
    reviewCount: pendingReviewCount,
    toolLane: agentToolPlanSummary(viewModel),
    branches: branchReviewSummary(viewModel),
    dataPills,
    receipt: receiptId,
    disconnected: graphDisconnectedCount(graph),
    contextAuthority: workflowContextSummaryFromAuthority(workflowContextAuthority),
  };
}

function compactMoonVisibleNode(node: BuildNode): Record<string, unknown> {
  const toolPlan = recordFromUnknown(node.agent_tool_plan);
  const contract = recordFromUnknown(node.completion_contract);
  const toolName = typeof toolPlan.tool_name === 'string' ? toolPlan.tool_name : null;
  const repeats = typeof toolPlan.repeats === 'number' && toolPlan.repeats > 0 ? toolPlan.repeats : null;
  const out: Record<string, unknown> = {
    node_id: node.node_id,
    kind: node.kind,
    title: node.title || node.summary || node.node_id,
  };
  if (node.route) out.route = node.route;
  if (node.status) out.status = node.status;
  if (node.task_type) out.task_type = node.task_type;
  if (toolName) out.tool_name = toolName;
  if (repeats) out.tool_repeats = repeats;
  if (Array.isArray(node.required_inputs) && node.required_inputs.length) {
    out.required_inputs = node.required_inputs.slice(0, 6);
  }
  if (Array.isArray(node.outputs) && node.outputs.length) {
    out.outputs = node.outputs.slice(0, 6);
  }
  if (contract.result_kind) out.result_kind = contract.result_kind;
  return out;
}

function compactMoonVisibleEdge(edge: BuildEdge): Record<string, unknown> {
  const release = normalizeBuildEdgeRelease(edge);
  const branchSide = typeof release.config?.branch_side === 'string'
    ? release.config.branch_side
    : null;
  const releaseReason = typeof release.branch_reason === 'string'
    ? release.branch_reason
    : null;
  return {
    edge_id: edge.edge_id,
    from_node_id: edge.from_node_id,
    to_node_id: edge.to_node_id,
    kind: edge.kind,
    release_family: release.family,
    release_type: release.edge_type,
    label: release.label || branchLabel(branchSide || releaseReason || release.edge_type),
  };
}

function moonVisibleSnapshot(
  payload: BuildPayload | null,
  selectedNodeId: string | null,
  selectedEdgeId: string | null,
  pendingReviewCount: number,
): Record<string, unknown> | null {
  if (!payload) return null;
  const graph = payload.build_graph;
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  const selectedNode = selectedNodeId
    ? nodes.find((node) => node.node_id === selectedNodeId) || null
    : null;
  const selectedEdge = selectedEdgeId
    ? edges.find((edge) => edge.edge_id === selectedEdgeId) || null
    : null;
  const workflow = payload.workflow || null;
  const receipt = recordFromUnknown(payload.operation_receipt);
  const snapshot: Record<string, unknown> = {
    kind: 'moon_visible_snapshot',
    source: 'ui',
    read_only: true,
    durability: 'visible_ui_snapshot_not_write_authority',
    workflow_id: workflow?.id ?? null,
    workflow_name: workflow?.name ?? null,
    node_count: nodes.length,
    edge_count: edges.length,
    selected_node_id: selectedNodeId,
    selected_edge_id: selectedEdgeId,
    review_count: pendingReviewCount,
    build_state: payload.build_state ?? null,
    nodes: nodes.slice(0, 16).map(compactMoonVisibleNode),
    edges: edges.slice(0, 24).map(compactMoonVisibleEdge),
  };
  if (selectedNode) {
    snapshot.selected_node_title = selectedNode.title || selectedNode.summary || selectedNode.node_id;
    if (selectedNode.route) snapshot.selected_node_route = selectedNode.route;
    if (selectedNode.status) snapshot.selected_node_status = selectedNode.status;
  }
  if (selectedEdge) {
    snapshot.selected_edge = compactMoonVisibleEdge(selectedEdge);
  }
  if (typeof receipt.receipt_id === 'string') snapshot.operation_receipt_id = receipt.receipt_id;
  if (typeof receipt.correlation_id === 'string') snapshot.correlation_id = receipt.correlation_id;
  return snapshot;
}

const MOON_MIN_SCALE = 0.5;
const MOON_MAX_SCALE = 2.5;

function clampScale(value: number): number {
  if (!Number.isFinite(value)) return 1;
  return Math.max(MOON_MIN_SCALE, Math.min(MOON_MAX_SCALE, value));
}

function finiteCoordinate(value: number): number {
  return Number.isFinite(value) ? value : 0;
}

function nodeCardClass(node: OrbitNode, isSelected: boolean, isDragOver: boolean): string {
  const classes = [
    'moon-graph-node',
    `moon-graph-node--kind-${node.kind}`,
    `moon-graph-node--shape-${node.shape}`,
    `moon-graph-node--state-${node.ringState}`,
    isSelected ? 'moon-graph-node--selected' : '',
    isDragOver ? 'moon-graph-node--drag-over' : '',
    node.multiplicity ? `moon-graph-node--stack moon-graph-node--stack-${node.multiplicity.kind}` : '',
  ];
  return classes.filter(Boolean).join(' ');
}

function nodeStatusLabel(node: OrbitNode): string {
  switch (node.ringState) {
    case 'blocked': return 'blocked';
    case 'decided-grounded': return 'ready';
    case 'decided-incomplete': return 'needs detail';
    case 'active-unresolved': return 'active';
    case 'run-pending': return 'queued';
    case 'run-active': return 'running';
    case 'run-succeeded': return 'done';
    case 'run-failed': return 'failed';
    case 'projected': return 'projected';
    case 'unresolved':
    default:
      return 'draft';
  }
}

function compactNodeToken(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const tail = trimmed.split(/[/.]/).filter(Boolean).pop() || trimmed;
  return tail.replace(/[_-]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 36) || null;
}

function nodeToolSummary(node: OrbitNode): string {
  const tool = compactNodeToken(node.agentToolPlan?.tool_name);
  const operation = compactNodeToken(node.agentToolPlan?.operation);
  const repeats = typeof node.agentToolPlan?.repeats === 'number' && node.agentToolPlan.repeats > 1
    ? `${node.agentToolPlan.repeats}x`
    : null;
  const lane = tool || operation || compactNodeToken(node.taskType) || compactNodeToken(node.route);
  if (!lane) return nodeStatusLabel(node);
  return repeats ? `${lane} ${repeats}` : lane;
}

function compactAgentLabel(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const tail = trimmed.split('/').filter(Boolean).pop() || trimmed;
  const label = tail
    .replace(/^google:/i, '')
    .replace(/^openrouter:/i, '')
    .replace(/[_-]+/g, ' ')
    .replace(/\bgemini\b/i, 'Gemini')
    .replace(/\bflash\b/i, 'Flash')
    .replace(/\s+/g, ' ')
    .trim();
  return label ? label.slice(0, 28) : null;
}

function nodeDecisionAgentLabel(node: OrbitNode): string {
  return compactAgentLabel(node.agent)
    || compactAgentLabel(node.agentToolPlan?.model)
    || compactAgentLabel(node.agentToolPlan?.provider_model)
    || compactAgentLabel(node.agentToolPlan?.agent)
    || 'LLM gate';
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

const INITIAL_COMPOSE_AUTHORITY: MoonComposeAuthoritySummary = {
  status: 'loading',
  buildControlCount: null,
  atlasFreshness: null,
  sourceAuthority: null,
  warning: null,
};

function compilePreviewChipLabels(preview: CompilePreviewPayload | null): {
  matched: string[];
  suggested: string[];
  gaps: string[];
} {
  const spans = preview?.scope_packet?.spans ?? [];
  const steps = preview?.scope_packet?.suggested_steps ?? [];
  const gaps = preview?.scope_packet?.gaps ?? [];
  return {
    matched: spans
      .map((span) => span.normalized || span.text)
      .filter((label): label is string => Boolean(label))
      .slice(0, 8),
    suggested: steps
      .map((step) => step.label)
      .filter((label): label is string => Boolean(label))
      .slice(0, 8),
    gaps: gaps
      .map((gap) => gap.kind || gap.span_text)
      .filter((label): label is string => Boolean(label))
      .slice(0, 6),
  };
}

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
  return reviewNodeName(node);
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
  if (payload.materialized_spec && Object.keys(payload.materialized_spec).length > 0) return true;
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

function isEditableTarget(target: EventTarget | null): boolean {
  return Boolean(
    target instanceof HTMLInputElement
    || target instanceof HTMLTextAreaElement
    || target instanceof HTMLSelectElement
    || target instanceof HTMLButtonElement
    || (target instanceof HTMLElement && target.isContentEditable),
  );
}

function buildDeletionTargetSummary(
  nodeId: string | null,
  edgeId: string | null,
  graph: NonNullable<BuildPayload['build_graph']>,
): { nodeName: string; target: UiActionTarget; reason: string } | null {
  if (nodeId) {
    const node = graph.nodes?.find((item) => item.node_id === nodeId);
    if (!node) return null;
    return {
      nodeName: nodeDisplayName(node),
      target: nodeTarget(node) || { kind: 'node', id: nodeId, label: nodeDisplayName(node) },
      reason: `Remove ${nodeDisplayName(node)} from the workflow graph.`,
    };
  }
  if (edgeId) {
    const edge = graph.edges?.find((item) => item.edge_id === edgeId);
    if (!edge) return null;
    const fromNode = (graph.nodes || []).find((node) => node.node_id === edge.from_node_id);
    const toNode = (graph.nodes || []).find((node) => node.node_id === edge.to_node_id);
    return {
      nodeName: `${nodeDisplayName(fromNode)} -> ${nodeDisplayName(toNode)}`,
      target: edgeTarget(edge, graph) || { kind: 'edge', id: edgeId, label: `${nodeDisplayName(fromNode)} -> ${nodeDisplayName(toNode)}` },
      reason: `Remove the connection ${nodeDisplayName(fromNode)} -> ${nodeDisplayName(toNode)}.`,
    };
  }
  return null;
}

export function MoonBuildPage({ workflowId, runId, onBack, onWorkflowCreated, onEditWorkflow, onViewRun, onDraftStateChange, onMaterializeHandoff, initialMode }: Props) {
  const { payload, loading, error, mutate, reload, setPayload } = useBuildPayload(workflowId);
  const [state, dispatch] = useReducer(moonBuildReducer, {
    ...initialMoonBuildState,
    emptyMode: initialMode ?? initialMoonBuildState.emptyMode,
  });
  const centerRef = useRef<HTMLDivElement>(null);
  const triggerAnchorRef = useRef<HTMLDivElement>(null);
  const pinnedSelectionRef = useRef<string | null>(null);
  const panStateRef = useRef<{
    pointerId: number;
    startX: number;
    startY: number;
    offsetX: number;
    offsetY: number;
    active: boolean;
  } | null>(null);
  const [canvasOffset, setCanvasOffset] = useState({ x: 0, y: 0 });
  const [canvasScale, setCanvasScale] = useState(1);
  const [catalog, setCatalog] = useState<CatalogItem[]>(getCatalog());
  const [moonGlowProfile, setMoonGlowProfile] = useState<MoonGlowProfile>(readMoonGlowProfile);
  const [mutationError, setMutationError] = useState<string | null>(null);
  const [composeAuthority, setComposeAuthority] = useState<MoonComposeAuthoritySummary>(INITIAL_COMPOSE_AUTHORITY);
  const [outcomeContractOpen, setOutcomeContractOpen] = useState(false);
  const [outcomeSuccessCriteria, setOutcomeSuccessCriteria] = useState('');
  const [outcomeFailureCriteria, setOutcomeFailureCriteria] = useState('');
  const [compilePreview, setCompilePreview] = useState<CompilePreviewPayload | null>(null);
  const [compilePreviewLoading, setCompilePreviewLoading] = useState(false);
  const [compilePreviewError, setCompilePreviewError] = useState<string | null>(null);
  const [builderValidationStatus, setBuilderValidationStatus] = useState<ClientOperatingModelBuilderStatus | null>(null);
  const [compositeOperatingModelStatus, setCompositeOperatingModelStatus] = useState<ClientOperatingModelCompositeStatus | null>(null);
  const [hydratedWorkflowContextAuthority, setHydratedWorkflowContextAuthority] = useState<ReturnType<typeof workflowContextFromPayload>>(null);
  /**
   * Which node has its branch-family picker open. A node-scoped picker is the
   * single affordance for adding a new outgoing edge from an existing step
   * (success / failure / any / conditional). It co-exists with the edge-pod
   * picker that configures an *existing* edge's gate.
   */
  const [branchPickerNodeId, setBranchPickerNodeId] = useState<string | null>(null);
  const { show } = useToast();
  const { objectTypes } = useObjectTypes();
  const persistedWorkflowId = useMemo(
    () => resolvePersistedWorkflowId(workflowId, payload),
    [payload, workflowId],
  );
  const pendingReviewCount = useMemo(() => reviewReadinessCount(payload), [payload]);
  const visibleMoonSnapshot = useMemo(
    () => moonVisibleSnapshot(payload, state.selectedNodeId, state.selectedEdgeId, pendingReviewCount),
    [payload, pendingReviewCount, state.selectedEdgeId, state.selectedNodeId],
  );
  const handleCheckClientOperatingModel = useCallback(async () => {
    setBuilderValidationStatus((current) => checkingBuilderValidationStatus(current));
    try {
      const status = await fetchWorkflowBuilderValidationStatus(
        payload,
        catalog,
        {
          scopeRef: persistedWorkflowId || workflowId || 'moon.workflow_builder',
        },
      );
      setBuilderValidationStatus(status);
      if (status.ok === false || status.errorCount > 0) {
        show(`Builder check found blockers: ${status.message}`, 'info');
      }
    } catch (err) {
      const status = statusFromBuilderValidationError(err);
      setBuilderValidationStatus(status);
      show(`Builder check unavailable: ${status.message}`, 'error');
    }
  }, [catalog, payload, persistedWorkflowId, show, workflowId]);
  // Push the active workflow + selection state into the shared chat context
  // store so ChatPanel can forward it as selection_context on every send.
  // The chat orchestrator threads this down to the moon_* tools so they
  // default-target this workflow when the LLM omits an explicit workflow_id.
  useEffect(() => {
    if (!persistedWorkflowId) {
      clearMoonChatContext();
      return;
    }
    const workflowName =
      (payload?.workflow as { name?: string | null } | undefined)?.name ?? null;
    const receipt = recordFromUnknown(payload?.operation_receipt);
    const resultGraphSummary = recordFromUnknown(payload?.graph_summary);
    const visibleGraphSummary = visibleMoonSnapshot
      ? {
          source: 'visible_ui',
          node_count: visibleMoonSnapshot.node_count,
          edge_count: visibleMoonSnapshot.edge_count,
          review_count: visibleMoonSnapshot.review_count,
        }
      : null;
    setMoonChatContext({
      workflow_id: persistedWorkflowId,
      workflow_name: workflowName,
      selected_node_id: state.selectedNodeId,
      selected_edge_id: state.selectedEdgeId,
      view_mode: state.viewMode,
      hint:
        state.viewMode === 'run'
          ? 'User is viewing a run. moon_get_build still works; mutations are typically not appropriate while observing a live run.'
          : 'User is authoring a workflow graph. Default-target this workflow_id when the user does not name another. Compare moon_get_build against visible_ui_snapshot before claiming the graph is empty.',
      materialize_status: payload?.build_state ?? null,
      operation_receipt_id: typeof receipt.receipt_id === 'string' ? receipt.receipt_id : null,
      correlation_id: typeof receipt.correlation_id === 'string' ? receipt.correlation_id : null,
      graph_summary: Object.keys(resultGraphSummary).length ? resultGraphSummary : visibleGraphSummary,
      visible_ui_snapshot: visibleMoonSnapshot,
    });
  }, [
    persistedWorkflowId,
    payload?.workflow,
    payload?.operation_receipt,
    payload?.graph_summary,
    payload?.build_state,
    state.selectedNodeId,
    state.selectedEdgeId,
    state.viewMode,
    visibleMoonSnapshot,
  ]);
  // Clear context on unmount so chat opened from elsewhere isn't haunted by
  // a stale Moon stanza pointing at a workflow the user has navigated away from.
  useEffect(() => {
    return () => {
      clearMoonChatContext();
    };
  }, []);
  const compileSource = useMemo(
    () => appendOutcomeContract(state.compileProse, {
      successCriteria: outcomeSuccessCriteria,
      failureCriteria: outcomeFailureCriteria,
    }),
    [outcomeFailureCriteria, outcomeSuccessCriteria, state.compileProse],
  );
  const draftGuardState = useMemo(() => {
    const dirty = !persistedWorkflowId
      && (
        Boolean(state.selectedTrigger)
        || Boolean(state.compileProse.trim())
        || Boolean(outcomeSuccessCriteria.trim())
        || Boolean(outcomeFailureCriteria.trim())
        || hasLocalDraftPayload(payload)
      );
    return {
      dirty,
      message: dirty
        ? 'This draft workflow only exists locally. Save it from Action or Release before leaving, or leave anyway and discard the draft.'
        : null,
    };
  }, [outcomeFailureCriteria, outcomeSuccessCriteria, payload, persistedWorkflowId, state.compileProse, state.selectedTrigger]);

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

  useEffect(() => {
    let cancelled = false;

    async function readJson(url: string, init?: RequestInit): Promise<unknown | null> {
      const response = await fetch(url, init);
      if (!response.ok) return null;
      return response.json().catch(() => null);
    }

    async function readUiExperienceGraph(): Promise<unknown | null> {
      const operated = await readJson('/api/operate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          operation: 'operator.ui_experience_graph',
          input: { surface_name: 'build', limit: 80 },
        }),
      });
      if (operated) return operated;
      return readJson('/api/operator/ui/experience-graph?surface_name=build&limit=80');
    }

    async function loadComposeAuthority() {
      const [uiGraph, atlasGraph] = await Promise.allSettled([
        readUiExperienceGraph(),
        readJson('/api/atlas/graph'),
      ]);
      if (cancelled) return;

      setComposeAuthority(summarizeComposeAuthority(
        uiGraph.status === 'fulfilled' ? uiGraph.value : null,
        atlasGraph.status === 'fulfilled' ? atlasGraph.value : null,
      ));
    }

    void loadComposeAuthority();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setHydratedWorkflowContextAuthority(null);
    void fetchWorkflowContextAuthority(payload)
      .then((context) => {
        if (!cancelled) setHydratedWorkflowContextAuthority(context);
      })
      .catch(() => {
        if (!cancelled) setHydratedWorkflowContextAuthority(null);
      });
    return () => {
      cancelled = true;
    };
  }, [payload]);

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
              payload.materialized_spec_projection?.materialized_spec ?? payload.materialized_spec ?? null,
            buildIssues: payload.build_issues ?? null,
          }
        : null,
    [payload],
  );
  const outcomeContractSuggestions = useMemo(
    () => buildPrimitiveContractSuggestions(
      payload?.build_graph,
      state.selectedNodeId,
      objectTypes,
      viewModel.dockContent,
      contractSuggestionExtras,
    ),
    [contractSuggestionExtras, objectTypes, payload?.build_graph, state.selectedNodeId, viewModel.dockContent],
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
          : `${nodeDisplayName(nodes[idx])} now runs ${title}, and Workflow appended a new empty next step.`,
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
        outcome: `Workflow created conditional branches from ${nodeDisplayName(sourceNode)}.`,
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
    if (!compileSource.trim()) return;
    dispatch({ type: 'COMPILE_START' });
    const handoffBaseId = `moon-materialize-${Date.now()}`;
    let materializeWorkflowId = workflowId || '';
    try {
      const compileProse = buildAuthorityCompileProse({
        prose: compileSource,
        triggerLabel: state.selectedTrigger?.label,
        summary: composeAuthority,
      });
      const handoffToMoonFallback = (params: {
        workflowId?: string | null;
        reason: string;
        receiptId?: string | null;
        correlationId?: string | null;
        graphSummary?: Record<string, unknown> | null;
      }) => {
        const fallbackWorkflowId = params.workflowId || null;
        if (fallbackWorkflowId) {
          setMoonChatContext({
            workflow_id: fallbackWorkflowId,
            workflow_name: null,
            selected_node_id: null,
            selected_edge_id: null,
            view_mode: 'build',
            hint: 'Materialize could not finish this graph. Recovery chat owns the recovery pass and must read before editing.',
            materialize_status: 'chat_fallback',
            operation_receipt_id: params.receiptId ?? null,
            correlation_id: params.correlationId ?? null,
            graph_summary: params.graphSummary ?? null,
          });
        }
        publishMoonChatHandoff({
          handoff_id: `${handoffBaseId}:chat-fallback`,
          workflow_id: fallbackWorkflowId,
          workflow_name: null,
          phase: 'chat_fallback',
          status_message: "Materialize could not build a usable graph. Recovery chat is looking at it now; the manual builder stays available.",
          prompt: buildMaterializeFallbackPrompt({
            intent: compileProse,
            workflowId: fallbackWorkflowId,
            reason: params.reason,
            graphSummary: params.graphSummary ?? null,
          }),
          operation_receipt_id: params.receiptId ?? null,
          correlation_id: params.correlationId ?? null,
          graph_summary: params.graphSummary ?? null,
        });
        onMaterializeHandoff?.();
        dispatch({ type: 'COMPILE_FALLBACK', error: params.reason });
      };
      const result = await materializePlan(compileProse, {
        workflowId,
        llmTimeoutSeconds: 35,
      });
      // Surface compose_provenance failures as compile errors so the user
      // sees the LLM-gate finding (validation, fork-out, etc.) instead of
      // an empty canvas. Provenance lives on definition.compose_provenance.
      const provenance = (result as any)?.definition?.compose_provenance;
      if (provenance && provenance.ok === false) {
        const reason = provenance.reason_code || 'unknown';
        const findings = (provenance.validation?.findings || []) as Array<{ severity?: string; label?: string; field?: string; code?: string; detail?: string }>;
        const errorFindings = findings
          .filter((f) => f.severity === 'error')
          .slice(0, 5)
          .map((f) => {
            const where = [f.label, f.field].filter(Boolean).join('.');
            const why = f.detail || f.code || '';
            return where ? `• ${where}: ${why}` : `• ${why}`;
          })
          .filter(Boolean)
          .join('\n');
        const summary = errorFindings
          ? `Compose blocked by ${reason} (${findings.length} finding${findings.length === 1 ? '' : 's'}):\n${errorFindings}`
          : `Compose blocked by ${reason}: ${provenance.error || 'no detail'}`;
        const wfIdFromResult = (result as any)?.definition?.workflow_id || (result as any)?.workflow?.id || materializeWorkflowId || null;
        handoffToMoonFallback({
          workflowId: wfIdFromResult,
          reason: summary,
          graphSummary: (result as any)?.graph_summary ?? null,
        });
        return;
      }
      const wfId = (result as any)?.definition?.workflow_id || (result as any)?.workflow?.id || materializeWorkflowId;
      const graph = (result as any)?.build_graph;
      const graphSummary = (result as any)?.graph_summary;
      const nodeCount = Array.isArray(graph?.nodes)
        ? graph.nodes.length
        : Number(graphSummary?.node_count || 0);
      const operationReceipt = (result as any)?.operation_receipt;
      const receiptId = typeof operationReceipt?.receipt_id === 'string' ? operationReceipt.receipt_id : null;
      const correlationId = typeof operationReceipt?.correlation_id === 'string' ? operationReceipt.correlation_id : null;
      if (!wfId || !receiptId || nodeCount < 2) {
        const summary = !wfId
          ? 'Materialize blocked: no workflow id was returned.'
          : !receiptId
            ? 'Materialize blocked: no operation receipt was returned.'
            : 'Materialize needs the agent to break this into multiple steps before it is ready.';
        handoffToMoonFallback({
          workflowId: wfId || null,
          reason: summary,
          receiptId,
          correlationId,
          graphSummary: graphSummary ?? null,
        });
        return;
      }
      materializeWorkflowId = wfId;

      // Patch first node with trigger route if trigger was selected
      if (state.selectedTrigger) {
        if (graph?.nodes?.length) {
          graph.nodes[0] = {
            ...graph.nodes[0],
            route: state.selectedTrigger.actionValue,
            status: 'ready',
            trigger: buildTriggerConfig(state.selectedTrigger.actionValue, graph.nodes[0].trigger),
          };
        }
      }
      const workflowName = (result as any)?.definition?.materialized_prose?.slice(0, 60) || '';
      const asPayload = {
        ...result,
        workflow: wfId ? { id: wfId, name: workflowName } : null,
      };
      setPayload(asPayload);
      setCompilePreview(result.compile_preview ?? compilePreview);
      if (wfId && onWorkflowCreated) onWorkflowCreated(wfId);
      if (wfId) {
        setMoonChatContext({
          workflow_id: wfId,
          workflow_name: workflowName || null,
          selected_node_id: null,
          selected_edge_id: null,
          view_mode: 'build',
          hint: 'Materialize completed for this workflow. Review with moon_get_build before making edits.',
          materialize_status: 'ready',
          operation_receipt_id: receiptId,
          correlation_id: correlationId,
          graph_summary: graphSummary ?? null,
        });
        publishMoonChatHandoff({
          handoff_id: `${handoffBaseId}:ready`,
          workflow_id: wfId,
          workflow_name: workflowName || null,
          phase: 'ready',
          status_message: 'Materialize produced a multi-step graph. Review is available when you ask for it.',
          operation_receipt_id: receiptId,
          correlation_id: correlationId,
          graph_summary: graphSummary ?? null,
        });
        onMaterializeHandoff?.();
      }
      dispatch({ type: 'COMPILE_SUCCESS' });
    } catch (e: any) {
      const failure = materializeFailureInfo(e);
      const fallbackWorkflowId = failure.workflowId || materializeWorkflowId || null;
      if (fallbackWorkflowId) {
        materializeWorkflowId = fallbackWorkflowId;
      }
      const compileProse = buildAuthorityCompileProse({
        prose: compileSource,
        triggerLabel: state.selectedTrigger?.label,
        summary: composeAuthority,
      });
      if (fallbackWorkflowId) {
        setMoonChatContext({
          workflow_id: fallbackWorkflowId,
          workflow_name: null,
          selected_node_id: null,
          selected_edge_id: null,
          view_mode: 'build',
          hint: 'Materialize failed. Recovery chat owns the recovery pass and must read before editing.',
          materialize_status: 'chat_fallback',
          operation_receipt_id: failure.receiptId,
          correlation_id: failure.correlationId,
          graph_summary: failure.graphSummary,
        });
      }
      publishMoonChatHandoff({
        handoff_id: `${handoffBaseId}:chat-fallback`,
        workflow_id: fallbackWorkflowId,
        workflow_name: null,
        phase: 'chat_fallback',
        status_message: "Materialize failed. Recovery chat is looking at it now; the manual builder stays available.",
        prompt: buildMaterializeFallbackPrompt({
          intent: compileProse,
          workflowId: fallbackWorkflowId,
          reason: failure.message,
          graphSummary: failure.graphSummary,
        }),
        operation_receipt_id: failure.receiptId,
        correlation_id: failure.correlationId,
        graph_summary: failure.graphSummary,
      });
      onMaterializeHandoff?.();
      dispatch({ type: 'COMPILE_FALLBACK', error: failure.message });
    }
  }, [compilePreview, compileSource, composeAuthority, onMaterializeHandoff, onWorkflowCreated, setPayload, state.selectedTrigger, workflowId]);

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
      outcome: `Workflow created a two-step draft with ${item.label} as the first node.`,
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

  const handleDeleteSelection = useCallback(async () => {
    if (state.viewMode === 'run' || !payload?.build_graph) return;
    const graph = payload.build_graph;
    const selectedNodeId = state.selectedNodeId;
    const selectedEdgeId = state.selectedEdgeId;
    const summary = buildDeletionTargetSummary(selectedNodeId, selectedEdgeId, graph);
    if (!summary) return;

    const previousOpenDock = state.openDock;
    const previousActiveNodeId = state.activeNodeId;

    const nextNodes = [...(graph.nodes || [])];
    const nextEdges = [...(graph.edges || [])];

    if (selectedNodeId) {
      const idx = nextNodes.findIndex((node) => node.node_id === selectedNodeId);
      if (idx < 0) return;
      nextNodes.splice(idx, 1);
      for (let i = nextEdges.length - 1; i >= 0; i -= 1) {
        const edge = nextEdges[i];
        if (edge.from_node_id === selectedNodeId || edge.to_node_id === selectedNodeId) {
          nextEdges.splice(i, 1);
        }
      }
    } else if (selectedEdgeId) {
      const idx = nextEdges.findIndex((edge) => edge.edge_id === selectedEdgeId);
      if (idx < 0) return;
      nextEdges.splice(idx, 1);
    }

    await commitMoonGraphAction({
      label: selectedNodeId ? 'Delete node' : 'Delete connection',
      reason: summary.reason,
      outcome: `Removed ${summary.nodeName}.`,
      target: summary.target,
      changeSummary: selectedNodeId ? ['Remove node', summary.nodeName] : ['Remove connection', summary.nodeName],
      nextPayload: {
        ...payload,
        build_graph: {
          ...graph,
          nodes: nextNodes,
          edges: nextEdges,
        },
      },
      afterApply: () => {
        dispatch({ type: 'SELECT_NODE', nodeId: null });
        dispatch({ type: 'SELECT_EDGE', edgeId: null });
        dispatch({ type: 'CLOSE_DOCK' });
        if (previousActiveNodeId && previousActiveNodeId === selectedNodeId) {
          dispatch({ type: 'SET_ACTIVE', nodeId: null });
        }
      },
      afterUndo: () => {
        if (selectedNodeId) {
          dispatch({ type: 'SELECT_NODE', nodeId: selectedNodeId });
        }
        if (selectedEdgeId) {
          dispatch({ type: 'SELECT_EDGE', edgeId: selectedEdgeId });
          dispatch({ type: 'OPEN_DOCK', dock: 'context' });
        }
        if (!selectedNodeId && previousOpenDock) {
          dispatch({ type: 'OPEN_DOCK', dock: previousOpenDock });
        }
      },
    });
  }, [commitMoonGraphAction, payload, state.activeNodeId, state.openDock, state.selectedEdgeId, state.selectedNodeId, state.viewMode]);

  const handleDeleteKeyboard = useCallback((event: KeyboardEvent) => {
    if (event.key !== 'Delete' && event.key !== 'Backspace') return;
    if (state.viewMode === 'run' || !state.selectedNodeId && !state.selectedEdgeId) return;
    if (isEditableTarget(event.target)) return;
    event.preventDefault();
    void handleDeleteSelection();
  }, [handleDeleteSelection, state.selectedEdgeId, state.selectedNodeId, state.viewMode]);

  useEffect(() => {
    window.addEventListener('keydown', handleDeleteKeyboard);
    return () => {
      window.removeEventListener('keydown', handleDeleteKeyboard);
    };
  }, [handleDeleteKeyboard]);

  const handleCanvasWheel = useCallback((event: React.WheelEvent<HTMLDivElement>) => {
    if (state.viewMode === 'run') return;
    event.preventDefault();

    const containerRect = centerRef.current?.getBoundingClientRect();
    if (!containerRect) return;

    const localX = finiteCoordinate(event.clientX) - containerRect.left;
    const localY = finiteCoordinate(event.clientY) - containerRect.top;
    const nextScale = clampScale(canvasScale * Math.exp(-event.deltaY * 0.001));
    if (nextScale === canvasScale) return;

    const worldX = (localX - canvasOffset.x) / canvasScale;
    const worldY = (localY - canvasOffset.y) / canvasScale;
    const nextOffsetX = localX - worldX * nextScale;
    const nextOffsetY = localY - worldY * nextScale;

    setCanvasScale(nextScale);
    setCanvasOffset({ x: nextOffsetX, y: nextOffsetY });
  }, [canvasOffset.x, canvasOffset.y, canvasScale, state.viewMode]);

  const handleCanvasPointerDown = useCallback((event: React.PointerEvent<HTMLDivElement>) => {
    if (state.viewMode === 'run') return;
    const pointerButton = typeof event.button === 'number' ? event.button : 0;
    if (pointerButton !== 0 || event.target !== event.currentTarget) return;
    if (isEditableTarget(event.target)) return;
    panStateRef.current = {
      pointerId: event.pointerId,
      startX: finiteCoordinate(event.clientX),
      startY: finiteCoordinate(event.clientY),
      offsetX: canvasOffset.x,
      offsetY: canvasOffset.y,
      active: true,
    };
    (event.currentTarget as HTMLElement).setPointerCapture?.(event.pointerId);
  }, [canvasOffset.x, canvasOffset.y, state.viewMode]);

  const handleCanvasPointerMove = useCallback((event: React.PointerEvent<HTMLDivElement>) => {
    const state = panStateRef.current;
    if (!state || !state.active || state.pointerId !== event.pointerId) return;
    const nextOffsetX = state.offsetX + (finiteCoordinate(event.clientX) - state.startX);
    const nextOffsetY = state.offsetY + (finiteCoordinate(event.clientY) - state.startY);
    setCanvasOffset({ x: nextOffsetX, y: nextOffsetY });
  }, []);

  const finishCanvasPan = useCallback((event: React.PointerEvent<HTMLDivElement>) => {
    if (!panStateRef.current || panStateRef.current.pointerId !== event.pointerId) return;
    panStateRef.current.active = false;
    (event.currentTarget as HTMLElement).releasePointerCapture?.(event.pointerId);
  }, []);

  useEffect(() => {
    if (state.viewMode === 'build') return;
    if (!panStateRef.current?.active) return;
    panStateRef.current.active = false;
  }, [state.viewMode]);

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
    const clicked = viewModel.nodes.find(n => n.id === nodeId);
    const isUnresolved = !!clicked && !(clicked.route || '').trim();
    if (isUnresolved && state.openDock !== null) {
      dispatch({ type: 'CLOSE_DOCK' });
    }
    if (isSelected && nodeId !== state.activeNodeId) {
      dispatch({ type: 'SELECT_NODE', nodeId: null });
      return;
    }

    dispatch({ type: 'SELECT_NODE', nodeId });
    dispatch({ type: 'OPEN_DOCK', dock: 'context' });
  }, [state.viewMode, state.pendingCatalogId, state.activeNodeId, state.openDock, applyCatalogToNode, viewModel.nodes]);

  const handleNodeDoubleClick = useCallback((nodeId: string) => {
    if (state.viewMode === 'run') {
      dispatch({ type: 'SELECT_RUN_JOB', jobId: nodeId });
      return;
    }

    dispatch({ type: 'SELECT_NODE', nodeId });
    dispatch({ type: 'OPEN_POPOUT' });
  }, [state.viewMode]);

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
  useEffect(() => {
    const prose = compileSource.trim();
    if (!showComposePanel || prose.length < 3) {
      setCompilePreview(null);
      setCompilePreviewError(null);
      setCompilePreviewLoading(false);
      return;
    }
    let cancelled = false;
    setCompilePreviewLoading(true);
    setCompilePreviewError(null);
    const timer = window.setTimeout(() => {
      previewCompile(prose)
        .then((preview) => {
          if (!cancelled) setCompilePreview(preview);
        })
        .catch((error) => {
          if (cancelled) return;
          setCompilePreview(null);
          setCompilePreviewError(error instanceof Error ? error.message : 'Preview failed');
        })
        .finally(() => {
          if (!cancelled) setCompilePreviewLoading(false);
        });
    }, 350);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [compileSource, showComposePanel]);
  const compilePreviewLabels = useMemo(
    () => compilePreviewChipLabels(compilePreview),
    [compilePreview],
  );
  const actionOpen = state.openDock === 'action';
  const contextOpen = state.openDock === 'context';
  const releaseOpen = state.releaseOpen;
  const embeddedWorkflowContextAuthority = useMemo(
    () => workflowContextFromPayload(payload),
    [payload],
  );
  const workflowContextAuthority = useMemo(
    () => hydratedWorkflowContextAuthority || embeddedWorkflowContextAuthority,
    [embeddedWorkflowContextAuthority, hydratedWorkflowContextAuthority],
  );
  useEffect(() => {
    let cancelled = false;
    if (!workflowContextAuthority) {
      setCompositeOperatingModelStatus(null);
      return () => {
        cancelled = true;
      };
    }
    void fetchWorkflowContextCompositeStatus(
      payload,
      workflowContextAuthority,
      builderValidationStatus,
      {
        scopeRef: persistedWorkflowId || workflowId || 'workflow.context_composite',
      },
    )
      .then((status) => {
        if (!cancelled) setCompositeOperatingModelStatus(status);
      })
      .catch((err) => {
        if (!cancelled) setCompositeOperatingModelStatus(statusFromCompositeError(err));
      });
    return () => {
      cancelled = true;
    };
  }, [builderValidationStatus, payload, persistedWorkflowId, workflowContextAuthority, workflowId]);
  const inspectorWorkflowSummary = useMemo(
    () => workflowInspectorSummary(payload, viewModel, pendingReviewCount, workflowContextAuthority),
    [payload, pendingReviewCount, viewModel, workflowContextAuthority],
  );
  const reviewDockLabel = pendingReviewCount === 1 ? 'Review 1 decision' : `Review ${pendingReviewCount} decisions`;
  const releaseNeedsReview = pendingReviewCount > 0;
  const releaseBlocked = releaseNeedsReview || viewModel.release.readiness !== 'ready';
  const releaseDockLabel = releaseBlocked ? 'Release blocked' : 'Release ready';
  const releaseDockTone = releaseBlocked ? 'blocked' : 'ready';
  const openReleaseChecklist = useCallback(() => {
    if (releaseNeedsReview) {
      if (!state.reviewQueueOpen) dispatch({ type: 'TOGGLE_REVIEW_QUEUE' });
      return;
    }
    dispatch({ type: 'TOGGLE_RELEASE' });
  }, [releaseNeedsReview, state.reviewQueueOpen]);
  const compiling = state.compilePhase === 'compiling';
  const previewTargetId = drag.drag.hoveredTarget?.id ?? null;
  const previewEdgeId = drag.drag.hoveredTarget?.zone === 'edge'
    ? drag.drag.hoveredTarget.id
    : null;
  const nodeById = useMemo(
    () => new Map(viewModel.nodes.map((node) => [node.id, node] as const)),
    [viewModel.nodes],
  );
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

    const presentation = edgePresentation(edge, edge.isOnDominantPath);

    return [{
      centerX: geometry.centerX,
      centerY: geometry.centerY,
      endX: geometry.endX,
      endY: geometry.endY,
      edge,
      fromLabel: nodeById.get(edge.from)?.title || edge.from,
      presentation,
      startX: geometry.startX,
      startY: geometry.startY,
      toLabel: nodeById.get(edge.to)?.title || edge.to,
    }];
  }), [nodeById, state.viewMode, viewModel.edges, viewModel.layout]);
  const appendPosition = useMemo(
    () => getMoonAppendPosition(viewModel.layout),
    [viewModel.layout],
  );
  const leftControlOpen = actionOpen || contextOpen || releaseOpen || state.reviewQueueOpen;
  const middleClassName = [
    'moon-middle',
    leftControlOpen ? 'moon-middle--action-open' : '',
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
          onEditWorkflow={onEditWorkflow}
        />
      )}
      <div className="moon-body">
        {/* Middle row */}
        <div className={middleClassName} data-testid="moon-middle">
          {/* Left dock: all review/workflow controls. Chat owns the right lane. */}
          <div className={`moon-dock-overlay moon-dock-overlay--left${leftControlOpen ? ' moon-dock-overlay--open' : ''}`}>
            {state.reviewQueueOpen ? (
              <MoonBindingReviewQueue
                payload={payload}
                onCommitAuthorityAction={async (subpath, body, meta) => {
                  await commitMoonAuthorityAction({
                    subpath,
                    body,
                    ...meta,
                  });
                }}
                onClose={() => dispatch({ type: 'TOGGLE_REVIEW_QUEUE' })}
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
            ) : actionOpen ? (
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
            ) : contextOpen ? (
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
                workflowSummary={inspectorWorkflowSummary}
                workflowContext={workflowContextAuthority}
                operatingModelStatus={builderValidationStatus}
                operatingModelCompositeStatus={compositeOperatingModelStatus}
                onCheckOperatingModel={handleCheckClientOperatingModel}
              />
            ) : null}
          </div>

          {/* Center */}
          <div className={`moon-center${!hasNodes ? ' moon-center--empty' : ''}`} ref={centerRef}>
            {hasNodes && state.viewMode !== 'run' && (
              <>
                <div className="moon-center__dock-actions" aria-label="Workspace panels">
                  <div className="moon-center__dock-group">
                    {pendingReviewCount > 0 && (
                      <DockToggleButton
                        active={state.reviewQueueOpen}
                        ariaLabel="Open review decisions"
                        label={reviewDockLabel}
                        onClick={() => dispatch({ type: 'TOGGLE_REVIEW_QUEUE' })}
                        tone="warning"
                      />
                    )}
                    <DockToggleButton
                      active={releaseOpen}
                      ariaLabel="Open release checklist"
                      label={`${releaseDockLabel} ›`}
                      onClick={openReleaseChecklist}
                      tone={releaseDockTone}
                    />
                    <DockToggleButton active={actionOpen} label="Authority" onClick={() => openDock('action')} />
                    <DockToggleButton active={contextOpen} label="Inspector" onClick={() => openDock('context')} />
                  </div>
                </div>
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

                {compiling && (
                  <div className="moon-materialize-wait" role="status" aria-live="polite">
                    <span className="moon-spinner" aria-hidden="true" />
                    <div>
                      <strong>Materialize is building the graph.</strong>
                      <p>The synthesis agent chooses the steps, then packet authors fill tools, gates, outputs, and handoffs.</p>
                    </div>
                  </div>
                )}

                {state.compileError && !showComposePanel && (
                  <div className="moon-compose__error" role="alert">
                    {state.compileError}
                    <div>Manual build is open; recovery chat is handling the recovery pass.</div>
                  </div>
                )}

                {showComposePanel && (
                  <div className="moon-compose moon-compose--intro" style={{ marginTop: 32 }}>
                    <div className="moon-compose__title">Describe the workflow</div>
                    <div className="moon-compose__hint">
                      Name the outcome, the authority it should trust, and the proof that should exist after release.
                    </div>
                    <textarea
                      aria-label="Workflow intent"
                      value={state.compileProse}
                      onChange={(e) => dispatch({ type: 'SET_PROSE', prose: e.target.value })}
                      onKeyDown={(event) => {
                        if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
                          event.preventDefault();
                          if (!compiling && compileSource.trim()) {
                            void handleCompile();
                          }
                        }
                      }}
                      placeholder="Example: Scrape Gmail, summarize applications, then route each one to the right reviewer."
                      rows={4}
                      disabled={compiling}
                      autoFocus
                    />
                    <MoonOutcomeContract
                      open={outcomeContractOpen}
                      disabled={compiling}
                      successCriteria={outcomeSuccessCriteria}
                      failureCriteria={outcomeFailureCriteria}
                      suggestions={outcomeContractSuggestions}
                      onOpenChange={setOutcomeContractOpen}
                      onSuccessChange={setOutcomeSuccessCriteria}
                      onFailureChange={setOutcomeFailureCriteria}
                    />
                    {(compilePreviewLoading || compilePreviewError || compilePreview) && (
                      <div className="moon-compose-preview" aria-live="polite">
                        {compilePreviewLoading && (
                          <div className="moon-compose-preview__status">Reading scope...</div>
                        )}
                        {!compilePreviewLoading && compilePreviewError && (
                          <div className="moon-compose-preview__status moon-compose-preview__status--error">
                            {compilePreviewError}
                          </div>
                        )}
                        {!compilePreviewLoading && !compilePreviewError && compilePreview && (
                          <>
                            {compilePreviewLabels.matched.length > 0 && (
                              <div className="moon-compose-preview__row">
                                <div className="moon-compose-preview__label">Matched</div>
                                <div className="moon-compose-preview__chips">
                                  {compilePreviewLabels.matched.map((label, index) => (
                                    <span className="moon-compose-preview__chip" key={`matched-${index}-${label}`}>{label}</span>
                                  ))}
                                </div>
                              </div>
                            )}
                            {compilePreviewLabels.suggested.length > 0 && (
                              <div className="moon-compose-preview__row">
                                <div className="moon-compose-preview__label">Suggested</div>
                                <div className="moon-compose-preview__chips">
                                  {compilePreviewLabels.suggested.map((label, index) => (
                                    <span className="moon-compose-preview__chip" key={`suggested-${index}-${label}`}>{label}</span>
                                  ))}
                                </div>
                              </div>
                            )}
                            {compilePreviewLabels.gaps.length > 0 && (
                              <div className="moon-compose-preview__row">
                                <div className="moon-compose-preview__label">Needs</div>
                                <div className="moon-compose-preview__chips">
                                  {compilePreviewLabels.gaps.map((label, index) => (
                                    <span className="moon-compose-preview__chip moon-compose-preview__chip--gap" key={`gap-${index}-${label}`}>{label}</span>
                                  ))}
                                </div>
                              </div>
                            )}
                          </>
                        )}
                      </div>
                    )}
                    <div className="moon-compose__actions prx-button-row">
                      <Button
                        tone="primary"
                        size="lg"
                        onClick={handleCompile}
                        disabled={compiling || !compileSource.trim()}
                      >
                        {compiling ? 'Composing workflow…' : 'Compose workflow'}
                      </Button>
                      <Button
                        tone="ghost"
                        onClick={() => dispatch({ type: 'EMPTY_PICK_TRIGGER' })}
                        disabled={compiling}
                      >
                        or build from scratch (skip LLM)
                      </Button>
                    </div>
                    {compiling && (
                      <div className="moon-compose__progress" aria-live="polite" style={{
                        marginTop: 16,
                        padding: '12px 16px',
                        border: '1px solid var(--moon-border, rgba(255,255,255,0.15))',
                        borderRadius: 6,
                        fontSize: 12,
                        color: 'var(--fg3)',
                      }}>
                        <div style={{ fontWeight: 500, marginBottom: 4 }}>Agent composition running</div>
                        <div>1. Compile preview reads context and existing authority</div>
                        <div>2. Synthesis chooses the work packets and step count</div>
                        <div>3. Packet authors fill one tool lane, gates, outputs, and submit contracts</div>
                        <div style={{ marginTop: 6, opacity: 0.7 }}>The graph appears only after those contracts are persisted.</div>
                      </div>
                    )}
                    {state.compileError && (
                      <div className="moon-compose__error" role="alert">
                        {state.compileError}
                      </div>
                    )}
                    <div className="moon-compose__shortcut">Press Ctrl/Cmd+Enter to compose.</div>
                  </div>
                )}
              </div>
            ) : (
              <div
                className="moon-graph"
                role="group"
                aria-label={state.viewMode === 'run' ? 'Workflow run graph' : 'Workflow build graph'}
                style={{
                  position: 'relative',
                  ...getMoonCanvasDimensions(viewModel.layout),
                  margin: '0 auto',
                  minHeight: MOON_LAYOUT.minGraphHeight,
                  transform: `translate(${canvasOffset.x}px, ${canvasOffset.y}px) scale(${canvasScale})`,
                  transformOrigin: '0 0',
                  touchAction: 'none',
                }}
                onWheel={handleCanvasWheel}
                onPointerDown={handleCanvasPointerDown}
                onPointerMove={handleCanvasPointerMove}
                onPointerUp={finishCanvasPan}
                onPointerCancel={finishCanvasPan}
                onClick={(e) => {
                  // Focus-lineage deselect: clicking the bare canvas
                  // background restores the rest state. Nodes and edge
                  // pods stop propagation via their own click handlers,
                  // so this only fires on empty canvas clicks.
                  if (e.target === e.currentTarget && state.viewMode !== 'run') {
                    dispatch({ type: 'SELECT_NODE', nodeId: null });
                  }
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
                    const presentation = control.presentation;
                    const title = isEmpty ? presentation.routeLabel : `${presentation.shortLabel} ${presentation.routeLabel}`;
                    const gateAriaLabel = isEmpty
                      ? `Add gate between ${control.fromLabel} and ${control.toLabel}`
                      : `Select ${presentation.shortLabel} gate between ${control.fromLabel} and ${control.toLabel}`;

                    // Focus-lineage dim: edge gate pods outside the selected
                    // lineage drop back so attention collapses to the branch
                    // the user is inspecting. inLineage defaults
                    // true at rest, so nothing dims until a node is selected.
                    const gateOpacity = isSelected
                      ? 1
                      : control.edge.inLineage
                        ? (control.edge.gateFamily ? 0.9 : 0.24)
                        : 0.12;
                    return (
                      <div
                        key={control.edge.id}
                        className={`moon-graph-gate moon-graph-gate--${presentation.tone}${isSelected ? ' moon-graph-gate--selected' : ''}${isDragOver ? ' moon-graph-gate--drag-over' : ''}${control.edge.gateFamily ? ` moon-graph-gate--family-${control.edge.gateFamily}` : ''}`}
                        style={{
                          left: control.centerX,
                          top: control.centerY,
                          opacity: gateOpacity,
                          transition: 'opacity 240ms ease',
                        }}
                        data-drop-edge={control.edge.id}
                        data-gate-family={control.edge.gateFamily || undefined}
                        data-in-lineage={control.edge.inLineage || undefined}
                      >
                          <button
                            type="button"
                            className="moon-graph-gate__trigger"
                            onClick={() => handleSelectEdge(control.edge.id)}
                            aria-label={gateAriaLabel}
                          >
                          <span
                            className={`moon-graph-gate__icon${isEmpty ? ' moon-graph-gate__icon--plus' : ''}`}
                            aria-hidden="true"
                          >
                            {isEmpty ? '+' : <MoonGlyph type={presentation.glyph} size={12} color="currentColor" />}
                          </span>
                          {!isEmpty && <span className="moon-graph-gate__trigger-label">{presentation.shortLabel}</span>}
                        </button>

                        {isSelected && (
                          <div className="moon-graph-gate__card">
                            <div className="moon-graph-gate__meta">
                              <span className={`moon-surface-badge moon-surface-badge--${presentation.tone}`}>
                                {presentation.stateLabel}
                              </span>
                            </div>
                            <div className="moon-graph-gate__route-label">{presentation.shortLabel}</div>
                            <div className="moon-graph-gate__title">{title}</div>
                            <div className="moon-graph-gate__path">
                              {control.fromLabel}{' -> '}{control.toLabel}
                            </div>

                            <button
                              type="button"
                              className="moon-graph-gate__detail-button"
                              onClick={() => handleSelectEdge(control.edge.id, { openDetail: true })}
                            >
                              {isEmpty ? 'Add gate' : 'Update gate'}
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
                    const nodeClass = nodeCardClass(node, isSelected, previewTargetId === node.id);
                    const displayTitle = nodeDisplayName({ node_id: node.id, title: node.title });
                    const summary = node.summary.trim();
                    const stepIndex = node.dominantPathIndex >= 0
                      ? String(node.dominantPathIndex + 1).padStart(2, '0')
                      : String(node.rank + 1).padStart(2, '0');
                    const statusLabel = nodeStatusLabel(node);
                    const toolSummary = nodeToolSummary(node);
                    const decisionAgentLabel = node.shape === 'decision'
                      ? nodeDecisionAgentLabel(node)
                      : null;
                    // Focus-lineage dim: nodes outside the selected lineage
                    // fade to ghost. inLineage defaults true when nothing is
                    // selected, so rest-state opacity is unchanged.
                    const nodeOpacity = node.inLineage ? 1 : 0.28;
                    const nodeStyle = {
                      ...position,
                      '--moon-node-instance-width': `${node.width}px`,
                      '--moon-node-instance-height': `${node.height}px`,
                      opacity: nodeOpacity,
                      transition: 'opacity 240ms ease',
                    } as React.CSSProperties;
                    const nodeAriaLabel = state.viewMode === 'run'
                      ? `Inspect run job ${node.title}${node.summary ? `, ${node.summary}` : ''}`
                      : `Select workflow step ${node.title}${node.summary ? `, ${node.summary}` : ''}`;
                    return (
                      <div
                        key={node.id}
                        className={nodeClass}
                        style={nodeStyle}
                        role="button"
                        tabIndex={0}
                        aria-label={nodeAriaLabel}
                        aria-pressed={isSelected}
                        data-drop-node={node.id}
                        data-multiplicity={multiplicityAttr || undefined}
                        data-node-shape={node.shape}
                        data-in-lineage={node.inLineage || undefined}
                        onClick={() => handleNodeClick(node.id, isSelected)}
                        onDoubleClick={() => handleNodeDoubleClick(node.id)}
                        onKeyDown={(event) => {
                          if (event.key !== 'Enter' && event.key !== ' ') return;
                          event.preventDefault();
                          handleNodeClick(node.id, isSelected);
                        }}
                        onPointerDown={state.viewMode === 'run' ? undefined : e => startNodeDrag(e, node)}
                      >
                        <span className="moon-graph-node__index" aria-hidden="true">
                          <span className="moon-graph-node__status-dot" />
                          <span className="moon-graph-node__index-number">{stepIndex}</span>
                        </span>
                        <span className="moon-graph-node__body">
                          <span className="moon-graph-node__title">{displayTitle}</span>
                          {decisionAgentLabel && (
                            <span className="moon-graph-node__decision-agent" aria-hidden="true">
                              <span>{decisionAgentLabel}</span>
                              <span className="moon-graph-node__dots" aria-hidden="true">
                                <span>.</span><span>.</span><span>.</span>
                              </span>
                            </span>
                          )}
                          <span className="moon-graph-node__terminal" aria-hidden="true">
                            <span className="moon-graph-node__terminal-label">tool</span>
                            <span className="moon-graph-node__terminal-value">{toolSummary}</span>
                            <span className="moon-graph-node__terminal-label">state</span>
                            <span className="moon-graph-node__terminal-value moon-graph-node__terminal-value--cadence">
                              {statusLabel}
                              <span className="moon-graph-node__dots" aria-hidden="true">
                                <span>.</span><span>.</span><span>.</span>
                              </span>
                            </span>
                          </span>
                        </span>
                        {multiplicityAttr && (
                          <span className="moon-graph-node__count-pill" aria-label={`${multiplicityAttr} count`}>
                            {multiplicityCount !== null
                              ? `${multiplicityCount}\u00d7`
                              : multiplicityAttr === 'loop' ? 'N\u00d7' : 'N\u2016'}
                          </span>
                        )}
                      </div>
                    );
                  })}
                  {/*
                    Node-scoped branch pod. Sits below a node's bottom edge
                    and opens a family picker. Unlike the edge pod (which
                    configures an existing edge), this one creates a new
                    outgoing edge + downstream node — the affordance for
                    branching a step 2, 3, N ways.
                  */}
                  {state.viewMode !== 'run' && viewModel.nodes
                    .filter(node => node.kind === 'step')
                    .map((node) => {
                      const position = getMoonNodeCanvasPosition(node);
                      const podLeft = position.left + node.width / 2 - 14;
                      const podTop = position.top + node.height + 8;
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

        </div>

        {/* Bottom dock: run status only. Workflow controls live on the left. */}
        <div className={`moon-dock-bottom${state.runViewOpen ? ' moon-dock-bottom--open' : ' moon-dock-bottom--closed'}`}>
          {state.runViewOpen && state.activeRunId ? (
            <MoonRunPanel
              runId={state.activeRunId}
              workflowId={workflowId}
              onClose={() => dispatch({ type: 'CLOSE_RUN' })}
              onSwitchRun={(newRunId) => dispatch({ type: 'DISPATCH_SUCCESS', runId: newRunId })}
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
