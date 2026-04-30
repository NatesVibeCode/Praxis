import React, { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import type { OrbitNode, OrbitEdge, DockContent } from './moonBuildPresenter';
import type {
  BindingLedgerEntry,
  BindingTarget,
  BuildPayload,
  HttpRequestPreset,
} from '../shared/types';
import {
  baseConditionFromRelease,
  branchLabel,
  normalizeBuildEdgeRelease,
  withBuildEdgeRelease,
} from '../shared/edgeRelease';
import type { CatalogItem } from './catalog';
import { getCatalogSurfacePolicy, getCatalogTruth } from './actionTruth';
import { MoonGlyph } from './MoonGlyph';
import {
  contractRowsFromStringArray,
  MoonContractStringListField,
  stringArrayFromContractRows,
  type ContractStringRow,
} from './MoonContractStringList';
import {
  buildPrimitiveContractSuggestions,
  type PrimitiveContractExtras,
} from './moonContractSuggestions';
import {
  buildHttpRequestIntegrationArgs,
  buildNotificationIntegrationArgs,
  buildWorkflowInvokeIntegrationArgs,
  DEFAULT_HTTP_REQUEST_PRESET,
  HTTP_REQUEST_PRESETS,
  httpRequestPresetDefinition,
  inferHttpRequestPreset,
  normalizeHttpHeaders,
  normalizeIntegrationArgs,
  requestMethodSupportsBody,
} from './moonPrimitives';
import { useObjectTypes } from '../shared/hooks/useObjectTypes';
import type { UiActionTarget } from '../control/uiActionLedger';
import {
  MoonPickerInput,
  MoonCronBuilder,
  MoonLocalHistoryRail,
  recordMoonHistory,
  MoonJsonEditor,
  MoonConfirmButton,
} from './MoonPickers';

export interface AuthorityActionMeta {
  label: string;
  reason: string;
  outcome: string;
  authority?: string;
  target?: UiActionTarget | null;
  changeSummary?: string[];
}

export interface WorkflowInspectorSummary {
  title: string;
  readiness: string;
  stepCount: number;
  linkCount: number;
  reviewCount: number;
  toolLane: string;
  branches: string;
  dataPills: string[];
  receipt: string | null;
  disconnected: number;
}

interface Props {
  node: OrbitNode | null;
  content: DockContent | null;
  workflowId: string | null;
  onMutate: (subpath: string, body: Record<string, unknown>) => Promise<void>;
  onCommitAuthorityAction?: (
    subpath: string,
    body: Record<string, unknown>,
    meta: AuthorityActionMeta,
  ) => Promise<void>;
  onClose: () => void;
  selectedEdge?: OrbitEdge | null;
  edgeFromLabel?: string;
  edgeToLabel?: string;
  onApplyGate?: (edgeId: string, gateFamily: string) => void;
  gateItems?: CatalogItem[];
  buildGraph?: BuildPayload['build_graph'] | null;
  onUpdateBuildGraph?: (graph: NonNullable<BuildPayload['build_graph']>) => Promise<void>;
  onCommitGraphAction?: (
    graph: NonNullable<BuildPayload['build_graph']>,
    meta: {
      label: string;
      reason: string;
      outcome: string;
      target?: UiActionTarget | null;
      changeSummary?: string[];
    },
  ) => Promise<void>;
  /** Compiled plan + issues for richer primitive field suggestions */
  contractSuggestionExtras?: PrimitiveContractExtras | null;
  workflowSummary?: WorkflowInspectorSummary | null;
}

const TRIGGER_MANUAL_ROUTE = 'trigger';
const TRIGGER_SCHEDULE_ROUTE = 'trigger/schedule';
const TRIGGER_WEBHOOK_ROUTE = 'trigger/webhook';
const WEBHOOK_TRIGGER_EVENT = 'db.webhook_events.insert';
type BranchConditionMode = 'simple' | 'json';
type BranchComposerOp = typeof BRANCH_OP_OPTIONS[number]['value'];
type TriggerFilterValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';

const BRANCH_OP_OPTIONS = [
  { value: 'equals', label: 'Equals', expectsValue: true },
  { value: 'not_equals', label: 'Does not equal', expectsValue: true },
  { value: 'in', label: 'Is one of', expectsValue: true },
  { value: 'not_in', label: 'Is not one of', expectsValue: true },
  { value: 'gt', label: 'Greater than', expectsValue: true },
  { value: 'gte', label: 'Greater than or equal', expectsValue: true },
  { value: 'lt', label: 'Less than', expectsValue: true },
  { value: 'lte', label: 'Less than or equal', expectsValue: true },
  { value: 'contains', label: 'Contains', expectsValue: true },
  { value: 'starts_with', label: 'Starts with', expectsValue: true },
  { value: 'ends_with', label: 'Ends with', expectsValue: true },
  { value: 'regex', label: 'Matches regex', expectsValue: true },
  { value: 'exists', label: 'Exists', expectsValue: false },
] as const;

const BRANCH_OP_LABELS = new Map(BRANCH_OP_OPTIONS.map((option) => [option.value, option.label]));

/** Short, grammar-first label for an edge family — used by the context ribbon.
 *  Matches the edge-family tokens so the ribbon reads like the canvas. */
function ribbonFamilyLabel(family: string): string {
  switch (family) {
    case 'after_any':
    case 'after_success':
      return 'after';
    case 'after_failure':
      return 'on fail';
    case 'conditional':
      return 'branch';
    case 'sequence':
      return 'then';
    default:
      return family.replace(/_/g, ' ');
  }
}

interface TriggerFilterFieldRow {
  id: string;
  key: string;
  valueType: TriggerFilterValueType;
  valueText: string;
}

const TRIGGER_FILTER_SUGGESTIONS: Array<{
  label: string;
  key: string;
  valueType: TriggerFilterValueType;
  valueText: string;
}> = [
  { label: 'Env: Prod', key: 'env', valueType: 'string', valueText: 'prod' },
  { label: 'Dry Run: False', key: 'dry_run', valueType: 'boolean', valueText: 'false' },
  { label: 'Priority: High', key: 'priority', valueType: 'string', valueText: 'high' },
  { label: 'Source: Api', key: 'source', valueType: 'string', valueText: 'api' },
];

function nodeTarget(node: OrbitNode | null | undefined): UiActionTarget | null {
  if (!node?.id) return null;
  return {
    kind: 'node',
    label: node.title || node.id,
    id: node.id,
  };
}

function edgeTarget(
  edge: OrbitEdge | null | undefined,
  fromLabel?: string | null,
  toLabel?: string | null,
): UiActionTarget | null {
  if (!edge?.id) return null;
  return {
    kind: 'edge',
    label: `${fromLabel || edge.from} -> ${toLabel || edge.to}`,
    id: edge.id,
  };
}

function isTriggerRoute(route?: string): boolean {
  return route === TRIGGER_MANUAL_ROUTE || route === TRIGGER_SCHEDULE_ROUTE || route === TRIGGER_WEBHOOK_ROUTE;
}

function normalizeTriggerFilter(filter: unknown): Record<string, unknown> {
  return filter && typeof filter === 'object' && !Array.isArray(filter)
    ? { ...(filter as Record<string, unknown>) }
    : {};
}

function parseTriggerFilter(text: string): Record<string, unknown> {
  if (!text.trim()) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error('Trigger filter must be valid JSON.');
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('Trigger filter must be a JSON object.');
  }
  return parsed as Record<string, unknown>;
}

function triggerFilterValueTypeFor(value: unknown): TriggerFilterValueType {
  if (value === null) return 'null';
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'string') return 'string';
  return 'json';
}

function formatTriggerFilterValueText(value: unknown, valueType: TriggerFilterValueType): string {
  if (valueType === 'null') return '';
  if (valueType === 'string') return typeof value === 'string' ? value : String(value ?? '');
  if (valueType === 'number' || valueType === 'boolean') return String(value);
  return formatJsonValue(value);
}

function WorkflowInspector({ summary }: { summary: WorkflowInspectorSummary | null | undefined }) {
  if (!summary) {
    return <div className="moon-dock__empty">Select a node or gate.</div>;
  }
  const mapTone = summary.disconnected > 0 ? 'warning' : summary.readiness === 'ready' ? 'ready' : summary.readiness;
  return (
    <div className="moon-dock-workflow" aria-label="Workflow inspector summary">
      <div className="moon-dock-section-card moon-dock-section-card--dense">
        <div className="moon-dock-section-card__header">
          <div className="moon-dock__section-label">Workflow</div>
          <span className={`moon-dock-workflow__tone moon-dock-workflow__tone--${mapTone}`}>
            {summary.readiness}
          </span>
        </div>
        <div className="moon-dock-workflow__title">{summary.title}</div>
      </div>

      <div className="moon-dock-workflow__grid">
        <div className="moon-dock-workflow__row">
          <span>Map</span>
          <strong>{summary.stepCount} steps · {summary.linkCount} links</strong>
        </div>
        <div className="moon-dock-workflow__row">
          <span>Review</span>
          <strong>{summary.reviewCount > 0 ? `${summary.reviewCount} decisions` : 'clear'}</strong>
        </div>
        <div className="moon-dock-workflow__row">
          <span>Tools</span>
          <strong>{summary.toolLane}</strong>
        </div>
        <div className="moon-dock-workflow__row">
          <span>Branches</span>
          <strong>{summary.branches}</strong>
        </div>
        <div className={`moon-dock-workflow__row${summary.disconnected > 0 ? ' moon-dock-workflow__row--warn' : ''}`}>
          <span>{summary.disconnected > 0 ? 'Unplaced' : 'Receipt'}</span>
          <strong>{summary.disconnected > 0 ? `${summary.disconnected} disconnected` : summary.receipt || 'pending'}</strong>
        </div>
      </div>

      <div className="moon-dock-section-card moon-dock-section-card--dense">
        <div className="moon-dock__section-label">Data pills</div>
        <div className="moon-dock-workflow__pills">
          {summary.dataPills.length > 0
            ? summary.dataPills.map((label) => <span key={label}>{label}</span>)
            : <span className="moon-dock-workflow__pill-empty">none declared</span>}
        </div>
      </div>
    </div>
  );
}

function triggerFilterRowsFromObject(filter: Record<string, unknown>): TriggerFilterFieldRow[] {
  const entries = Object.entries(filter);
  if (entries.length === 0) return [{ id: 'trigger-filter-1', key: '', valueType: 'string', valueText: '' }];
  return entries.map(([key, value], index) => {
    const valueType = triggerFilterValueTypeFor(value);
    return {
      id: `trigger-filter-${index + 1}`,
      key,
      valueType,
      valueText: formatTriggerFilterValueText(value, valueType),
    };
  });
}

function triggerFilterHasComplexValue(filter: Record<string, unknown>): boolean {
  return Object.values(filter).some((value) => (
    Array.isArray(value)
    || (value != null && typeof value === 'object')
  ));
}

function isVacuousBranchCondition(condition: Record<string, unknown>): boolean {
  const keys = Object.keys(condition || {});
  if (keys.length === 0) return true;
  if (keys.length === 1 && keys[0] === 'kind' && condition.kind === 'always') return true;
  return false;
}

function parseTriggerFilterRows(rows: TriggerFilterFieldRow[]): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const row of rows) {
    const key = row.key.trim();
    if (!key) continue;
    if (key in result) {
      throw new Error(`Trigger filter has duplicate key "${key}".`);
    }
    if (row.valueType === 'null') {
      result[key] = null;
      continue;
    }
    if (row.valueType === 'string') {
      result[key] = row.valueText;
      continue;
    }
    if (row.valueType === 'number') {
      const parsed = Number(row.valueText.trim());
      if (!Number.isFinite(parsed)) {
        throw new Error(`Trigger filter key "${key}" must be a valid number.`);
      }
      result[key] = parsed;
      continue;
    }
    if (row.valueType === 'boolean') {
      const normalized = row.valueText.trim().toLowerCase();
      if (normalized !== 'true' && normalized !== 'false') {
        throw new Error(`Trigger filter key "${key}" must be true or false.`);
      }
      result[key] = normalized === 'true';
      continue;
    }
    try {
      result[key] = row.valueText.trim() ? JSON.parse(row.valueText) : {};
    } catch {
      throw new Error(`Trigger filter key "${key}" JSON value is invalid.`);
    }
  }
  return result;
}

function triggerFilterChipText(row: TriggerFilterFieldRow): string {
  const rawKey = row.key.trim();
  if (!rawKey && !row.valueText.trim() && row.valueType === 'string') return 'Add a filter...';
  
  const key = (rawKey || 'New filter')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());

  if (row.valueType === 'null') return `${key}: Null`;
  if (!row.valueText.trim()) return `${key}: Value`;
  const value = row.valueType === 'boolean'
    ? row.valueText.trim().charAt(0).toUpperCase() + row.valueText.trim().slice(1)
    : row.valueText.trim();
  if (row.valueType === 'string') return `${key}: "${value}"`;
  return `${key}: ${value}`;
}

function formatJsonObject(value: unknown): string {
  return JSON.stringify(
    value && typeof value === 'object' && !Array.isArray(value) ? value : {},
    null,
    2,
  );
}

function parseJsonObject(text: string, emptyMessage: string): Record<string, unknown> {
  if (!text.trim()) throw new Error(emptyMessage);
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error('Condition must be valid JSON.');
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('Condition must be a JSON object.');
  }
  return parsed as Record<string, unknown>;
}

function parseOptionalJsonObject(text: string, label: string): Record<string, unknown> {
  if (!text.trim()) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error(`${label} must be valid JSON.`);
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object.`);
  }
  return parsed as Record<string, unknown>;
}

function formatJsonValue(value: unknown): string {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  return JSON.stringify(value, null, 2);
}

function parseOptionalJsonValue(text: string): unknown {
  if (!text.trim()) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function normalizeBranchComposerOp(value: unknown): BranchComposerOp | null {
  if (typeof value !== 'string') return null;
  const op = value.trim();
  if (!op) return null;
  if (op === 'eq') return 'equals';
  if (op === 'neq') return 'not_equals';
  return BRANCH_OP_LABELS.has(op as BranchComposerOp) ? op as BranchComposerOp : null;
}

function parseSimpleBranchCondition(condition: unknown): {
  field: string;
  op: BranchComposerOp;
  valueText: string;
} | null {
  if (!condition || typeof condition !== 'object' || Array.isArray(condition)) return null;
  const record = condition as Record<string, unknown>;
  if (Array.isArray(record.conditions)) return null;
  const field = typeof record.field === 'string' ? record.field.trim() : '';
  if (!field) return null;

  let op = normalizeBranchComposerOp(record.op);
  let value = record.value;

  if (!op) {
    for (const legacyOp of ['equals', 'not_equals', 'in', 'not_in'] as const) {
      if (legacyOp in record) {
        op = legacyOp;
        value = record[legacyOp];
        break;
      }
    }
  }
  if (!op) return null;

  return {
    field,
    op,
    valueText: op === 'exists' ? '' : formatJsonValue(value),
  };
}

function buildSimpleBranchCondition(
  field: string,
  op: BranchComposerOp,
  valueText: string,
): Record<string, unknown> {
  const normalizedField = field.trim();
  if (!normalizedField) {
    throw new Error('Branch field is required.');
  }
  const selectedOp = BRANCH_OP_OPTIONS.find((option) => option.value === op);
  if (!selectedOp) {
    throw new Error('Choose a supported branch operator.');
  }

  const condition: Record<string, unknown> = {
    field: normalizedField,
    op: selectedOp.value,
  };

  if (selectedOp.expectsValue) {
    const parsedValue = parseOptionalJsonValue(valueText);
    if (parsedValue === undefined) {
      throw new Error('Branch comparison value is required.');
    }
    condition.value = parsedValue;
  }

  return condition;
}

export function MoonNodeDetail({ node, content, workflowId, onMutate, onCommitAuthorityAction, onClose, selectedEdge, edgeFromLabel, edgeToLabel, onApplyGate, gateItems = [], buildGraph, onUpdateBuildGraph, onCommitGraphAction, contractSuggestionExtras = null, workflowSummary = null }: Props) {
  const { objectTypes, loading: objectTypesLoading } = useObjectTypes();
  const [objectSearch, setObjectSearch] = useState('');
  const [attachKind, setAttachKind] = useState('reference');
  const [attachRef, setAttachRef] = useState('');
  const [attachLabel, setAttachLabel] = useState('');
  const [attachRole, setAttachRole] = useState('input');
  const [attachPromote, setAttachPromote] = useState(false);
  const [attachLoading, setAttachLoading] = useState(false);
  const [attachError, setAttachError] = useState<string | null>(null);

  const [importLocator, setImportLocator] = useState('');
  const [importLabel, setImportLabel] = useState('');
  const [importLoading, setImportLoading] = useState(false);
  const [importError, setImportError] = useState<string | null>(null);
  const [triggerCronExpression, setTriggerCronExpression] = useState('@daily');
  const [triggerSourceRef, setTriggerSourceRef] = useState('');
  const [triggerFilterText, setTriggerFilterText] = useState('{}');
  const [triggerFilterRows, setTriggerFilterRows] = useState<TriggerFilterFieldRow[]>([
    { id: 'trigger-filter-1', key: '', valueType: 'string', valueText: '' },
  ]);
  const [activeTriggerFilterId, setActiveTriggerFilterId] = useState<string | null>('trigger-filter-1');
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [edgeConditionText, setEdgeConditionText] = useState('{}');
  const [edgeConditionMode, setEdgeConditionMode] = useState<BranchConditionMode>('simple');
  const [edgeConditionField, setEdgeConditionField] = useState('should_continue');
  const [edgeConditionOp, setEdgeConditionOp] = useState<BranchComposerOp>('equals');
  const [edgeConditionValueText, setEdgeConditionValueText] = useState('true');
  const [edgeConditionLoading, setEdgeConditionLoading] = useState(false);
  const [edgeConditionError, setEdgeConditionError] = useState<string | null>(null);
  const [gateClearLoading, setGateClearLoading] = useState(false);
  const [gateClearError, setGateClearError] = useState<string | null>(null);
  const [nodeTitle, setNodeTitle] = useState('');
  const [nodeSummary, setNodeSummary] = useState('');
  const [nodePrompt, setNodePrompt] = useState('');
  const [requiredInputRows, setRequiredInputRows] = useState<ContractStringRow[]>(() => (
    contractRowsFromStringArray(undefined, 'required-inputs')
  ));
  const [outputRows, setOutputRows] = useState<ContractStringRow[]>(() => (
    contractRowsFromStringArray(undefined, 'outputs')
  ));
  const [persistenceTargetRows, setPersistenceTargetRows] = useState<ContractStringRow[]>(() => (
    contractRowsFromStringArray(undefined, 'persistence-targets')
  ));
  const [nodeHandoffTarget, setNodeHandoffTarget] = useState('');
  const [notificationTitle, setNotificationTitle] = useState('');
  const [notificationMessage, setNotificationMessage] = useState('');
  const [notificationStatus, setNotificationStatus] = useState('info');
  const [httpRequestPreset, setHttpRequestPreset] = useState<HttpRequestPreset>(DEFAULT_HTTP_REQUEST_PRESET);
  const [webhookUrl, setWebhookUrl] = useState('');
  const [webhookMethod, setWebhookMethod] = useState('POST');
  const [webhookHeadersText, setWebhookHeadersText] = useState('{}');
  const [webhookBodyText, setWebhookBodyText] = useState('');
  const [webhookTimeoutText, setWebhookTimeoutText] = useState('');
  const [invokeWorkflowId, setInvokeWorkflowId] = useState('');
  const [invokePayloadText, setInvokePayloadText] = useState('');
  const [genericIntegrationArgsText, setGenericIntegrationArgsText] = useState('{}');
  const [workflowTargets, setWorkflowTargets] = useState<Array<{
    id: string;
    name: string;
    description: string;
    hasSpec: boolean;
  }>>([]);
  const [nodeSaveLoading, setNodeSaveLoading] = useState(false);
  const [nodeSaveError, setNodeSaveError] = useState<string | null>(null);
  const [showAdvancedContractFields, setShowAdvancedContractFields] = useState(false);
  const [payloadFieldSuggestions, setPayloadFieldSuggestions] = useState<Array<{ key: string; label: string; samples: string[] }>>([]);

  const buildNode = node
    ? (buildGraph?.nodes || []).find(graphNode => graphNode.node_id === node.id) || null
    : null;
  const buildEdge = selectedEdge
    ? (buildGraph?.edges || []).find(graphEdge => graphEdge.edge_id === selectedEdge.id) || null
    : null;
  const buildEdgeRelease = useMemo(
    () => (buildEdge ? normalizeBuildEdgeRelease(buildEdge) : null),
    [buildEdge],
  );
  const triggerRoute = buildNode?.route || node?.route || '';
  const triggerConfig = buildNode?.trigger;
  const isTriggerNode = Boolean(node && isTriggerRoute(triggerRoute));
  const isConditionalEdge = Boolean(selectedEdge && buildEdgeRelease?.family === 'conditional');
  const isFailureEdge = Boolean(selectedEdge && buildEdgeRelease?.family === 'after_failure');
  const integrationArgs = useMemo(
    () => normalizeIntegrationArgs(buildNode?.integration_args),
    [buildNode?.integration_args],
  );
  const isIntegrationRoute = triggerRoute.startsWith('@');
  const isNotificationRoute = triggerRoute === '@notifications/send';
  const isWebhookRoute = triggerRoute === '@webhook/post';
  const isWorkflowInvokeRoute = triggerRoute === '@workflow/invoke';
  const selectedHttpRequestPreset = useMemo(
    () => httpRequestPresetDefinition(httpRequestPreset),
    [httpRequestPreset],
  );
  const showHttpRequestBody = useMemo(() => {
    if (httpRequestPreset === 'custom') return true;
    return requestMethodSupportsBody(webhookMethod);
  }, [httpRequestPreset, webhookMethod]);
  const canCommitGraph = Boolean(onCommitGraphAction || onUpdateBuildGraph);
  const canEditNodePrimitive = Boolean(node && buildGraph && canCommitGraph && buildNode);
  const showPrimitiveEditor = Boolean(canEditNodePrimitive && buildNode?.kind === 'step');
  const requiresSavedWorkflow = !workflowId;
  const runnableWorkflowTargets = useMemo(
    () => workflowTargets.filter(target => target.hasSpec),
    [workflowTargets],
  );
  const selectedWorkflowTarget = useMemo(
    () => workflowTargets.find(target => target.id === invokeWorkflowId) || null,
    [invokeWorkflowId, workflowTargets],
  );
  const selectedWorkflowTargetIsRunnable = useMemo(
    () => runnableWorkflowTargets.some(target => target.id === invokeWorkflowId),
    [invokeWorkflowId, runnableWorkflowTargets],
  );
  const gateCatalogModels = useMemo(
    () => gateItems.map((item) => ({
      item,
      truth: getCatalogTruth(item),
      policy: getCatalogSurfacePolicy(item),
    })),
    [gateItems],
  );
  const selectedGateCatalogModel = gateCatalogModels.find(
    ({ item }) => item.gateFamily === selectedEdge?.gateFamily,
  ) || null;
  const selectedBranchOp = BRANCH_OP_OPTIONS.find((option) => option.value === edgeConditionOp) || BRANCH_OP_OPTIONS[0];
  const conditionalTargets = useMemo(() => {
    if (!buildEdge || !buildGraph || !isConditionalEdge) {
      return { thenTarget: null as string | null, elseTarget: null as string | null };
    }
    const nodeTitles = new Map(
      (buildGraph.nodes || []).map((graphNode) => [
        graphNode.node_id,
        (graphNode.title || graphNode.node_id || '').trim() || graphNode.node_id,
      ] as const),
    );
    let thenTarget: string | null = null;
    let elseTarget: string | null = null;
    for (const edge of buildGraph.edges || []) {
      const release = normalizeBuildEdgeRelease(edge);
      if (edge.from_node_id !== buildEdge.from_node_id || release.family !== 'conditional') continue;
      const targetTitle = nodeTitles.get(edge.to_node_id) || edge.to_node_id;
      if ((release.branch_reason || '').trim().toLowerCase() === 'else') elseTarget = targetTitle;
      else thenTarget = targetTitle;
    }
    return { thenTarget, elseTarget };
  }, [buildEdge, buildGraph, isConditionalEdge]);
  const failureSourceLabel = edgeFromLabel || selectedEdge?.from || 'upstream step';
  const failureTargetLabel = edgeToLabel || selectedEdge?.to || 'failure step';

  useEffect(() => {
    setShowAdvancedContractFields(false);
  }, [buildNode?.node_id, selectedEdge?.id]);

  const primitiveContractSuggestions = useMemo(
    () =>
      buildPrimitiveContractSuggestions(
        buildGraph,
        node?.id ?? null,
        objectTypes,
        content,
        contractSuggestionExtras,
      ),
    [buildGraph, node?.id, objectTypes, content, contractSuggestionExtras],
  );

  // Context ribbon — upstream producers and downstream consumers for the
  // selected node. Edge family drives the connector tone (after, on-fail,
  // branch) so the builder can read the structural grammar at a glance.
  const contextRibbon = useMemo(() => {
    if (!node || !buildGraph) return null;
    const edges = buildGraph.edges || [];
    const nodes = buildGraph.nodes || [];
    const titleFor = (id: string): string => {
      const n = nodes.find((x) => x.node_id === id);
      return (n?.title || id || '').trim() || id;
    };
    type Neighbor = { nodeId: string; title: string; family: string; edgeId: string };
    const upstream: Neighbor[] = [];
    const downstream: Neighbor[] = [];
    for (const edge of edges) {
      const release = normalizeBuildEdgeRelease(edge);
      const family = release.family || 'after_any';
      if (edge.to_node_id === node.id && edge.from_node_id) {
        upstream.push({
          nodeId: edge.from_node_id,
          title: titleFor(edge.from_node_id),
          family,
          edgeId: edge.edge_id,
        });
      } else if (edge.from_node_id === node.id && edge.to_node_id) {
        downstream.push({
          nodeId: edge.to_node_id,
          title: titleFor(edge.to_node_id),
          family,
          edgeId: edge.edge_id,
        });
      }
    }
    if (upstream.length === 0 && downstream.length === 0) return null;
    return { upstream, downstream };
  }, [buildGraph, node]);

  const triggerFilterFingerprint = useMemo(() => {
    try {
      return JSON.stringify(triggerConfig?.filter ?? null);
    } catch {
      return 'null';
    }
  }, [triggerConfig?.filter]);

  const triggerFilterRowParse = useMemo(() => {
    try {
      return { ok: true as const, value: parseTriggerFilterRows(triggerFilterRows) };
    } catch {
      return { ok: false as const, value: {} as Record<string, unknown> };
    }
  }, [triggerFilterRows]);

  const triggerNeedsJsonEditor = Boolean(
    triggerFilterRowParse.ok && triggerFilterHasComplexValue(triggerFilterRowParse.value),
  );

  const prevTriggerNeedsJsonRef = useRef(false);
  useEffect(() => {
    if (!isTriggerNode) return;
    if (triggerNeedsJsonEditor && !prevTriggerNeedsJsonRef.current && triggerFilterRowParse.ok) {
      setTriggerFilterText(JSON.stringify(triggerFilterRowParse.value, null, 2));
    }
    prevTriggerNeedsJsonRef.current = triggerNeedsJsonEditor;
  }, [isTriggerNode, triggerNeedsJsonEditor, triggerFilterRowParse]);

  useEffect(() => {
    if (!isTriggerNode) return;
    const normalizedFilter = normalizeTriggerFilter(triggerConfig?.filter);
    setTriggerCronExpression(
      (typeof triggerConfig?.cron_expression === 'string' && triggerConfig.cron_expression.trim()) || '@daily',
    );
    setTriggerSourceRef(typeof triggerConfig?.source_ref === 'string' ? triggerConfig.source_ref : '');
    setTriggerFilterText(JSON.stringify(normalizedFilter, null, 2));
    const rows = triggerFilterRowsFromObject(normalizedFilter);
    setTriggerFilterRows(rows);
    setActiveTriggerFilterId(rows[0]?.id || null);
    setTriggerError(null);
  }, [
    isTriggerNode,
    node?.id,
    triggerConfig?.cron_expression,
    triggerConfig?.source_ref,
    triggerFilterFingerprint,
  ]);

  useEffect(() => {
    if (!isConditionalEdge) return;
    const condition = baseConditionFromRelease(buildEdgeRelease || {
      family: 'conditional',
      edge_type: 'conditional',
      release_condition: {},
    } as any) || {};
    const simpleCondition = parseSimpleBranchCondition(condition);
    const vacuous = !simpleCondition && isVacuousBranchCondition(condition);
    if (simpleCondition) {
      setEdgeConditionMode('simple');
      setEdgeConditionField(simpleCondition.field);
      setEdgeConditionOp(simpleCondition.op);
      setEdgeConditionValueText(simpleCondition.valueText);
      setEdgeConditionText(formatJsonObject(condition));
    } else if (vacuous) {
      setEdgeConditionMode('simple');
      setEdgeConditionField('should_continue');
      setEdgeConditionOp('equals');
      setEdgeConditionValueText('true');
      setEdgeConditionText(formatJsonObject(
        buildSimpleBranchCondition('should_continue', 'equals', 'true'),
      ));
    } else {
      setEdgeConditionMode('json');
      setEdgeConditionField('should_continue');
      setEdgeConditionOp('equals');
      setEdgeConditionValueText('true');
      setEdgeConditionText(formatJsonObject(condition));
    }
    setEdgeConditionError(null);
  }, [
    isConditionalEdge,
    buildEdge?.edge_id,
    buildEdgeRelease?.release_condition,
    buildEdgeRelease?.branch_reason,
  ]);

  useEffect(() => {
    setGateClearError(null);
  }, [buildEdge?.edge_id]);

  useEffect(() => {
    if (!buildNode) return;
    setNodeTitle(buildNode.title || node?.title || '');
    setNodeSummary(buildNode.summary || node?.summary || '');
    setNodePrompt(buildNode.prompt || '');
    setRequiredInputRows(contractRowsFromStringArray(buildNode.required_inputs, 'required-inputs'));
    setOutputRows(contractRowsFromStringArray(buildNode.outputs, 'outputs'));
    setPersistenceTargetRows(contractRowsFromStringArray(buildNode.persistence_targets, 'persistence-targets'));
    setNodeHandoffTarget(buildNode.handoff_target || '');
    setNodeSaveError(null);

    setNotificationTitle(typeof integrationArgs.title === 'string' ? integrationArgs.title : buildNode.title || '');
    setNotificationMessage(typeof integrationArgs.message === 'string' ? integrationArgs.message : buildNode.summary || '');
    setNotificationStatus(typeof integrationArgs.status === 'string' ? integrationArgs.status : 'info');

    const resolvedHttpPreset = inferHttpRequestPreset(integrationArgs);
    const resolvedHttpPresetDefinition = httpRequestPresetDefinition(resolvedHttpPreset);
    const resolvedHttpHeaders = normalizeHttpHeaders(integrationArgs.headers);
    const resolvedHttpBody = integrationArgs.body ?? integrationArgs.body_template ?? resolvedHttpPresetDefinition.body;
    setHttpRequestPreset(resolvedHttpPreset);
    setWebhookUrl(
      typeof integrationArgs.url === 'string'
        ? integrationArgs.url
        : typeof integrationArgs.endpoint === 'string'
          ? integrationArgs.endpoint
          : '',
    );
    setWebhookMethod(
      typeof integrationArgs.method === 'string' && integrationArgs.method.trim()
        ? integrationArgs.method.toUpperCase()
        : resolvedHttpPresetDefinition.method,
    );
    setWebhookHeadersText(formatJsonObject(
      Object.keys(resolvedHttpHeaders).length > 0
        ? resolvedHttpHeaders
        : resolvedHttpPresetDefinition.headers,
    ));
    setWebhookBodyText(formatJsonValue(resolvedHttpBody));
    setWebhookTimeoutText(integrationArgs.timeout == null ? '' : String(integrationArgs.timeout));

    const invokeTarget = typeof integrationArgs.workflow_id === 'string'
      ? integrationArgs.workflow_id
      : typeof integrationArgs.target_workflow_id === 'string'
        ? integrationArgs.target_workflow_id
        : '';
    setInvokeWorkflowId(invokeTarget);
    setInvokePayloadText(formatJsonValue(integrationArgs.payload ?? integrationArgs.input ?? integrationArgs.inputs));
    setGenericIntegrationArgsText(formatJsonObject(integrationArgs));
  }, [buildNode, integrationArgs, node?.summary, node?.title]);

  useEffect(() => {
    if (!isTriggerNode) return;
    let cancelled = false;
    const qs = triggerSourceRef.trim()
      ? `?source_ref=${encodeURIComponent(triggerSourceRef.trim())}`
      : '';
    fetch(`/api/moon/pickers/payload-fields${qs}`)
      .then(async (response) => {
        if (!response.ok) return [] as Array<{ key: string; label: string; samples: string[] }>;
        const body = await response.json().catch(() => ({}));
        const fields = Array.isArray(body?.fields) ? body.fields : [];
        return fields
          .map((entry: any) => ({
            key: typeof entry?.key === 'string' ? entry.key : '',
            label: typeof entry?.label === 'string' && entry.label ? entry.label : (typeof entry?.key === 'string' ? entry.key : ''),
            samples: Array.isArray(entry?.samples) ? entry.samples.filter((s: any) => typeof s === 'string') : [],
          }))
          .filter((entry: { key: string }) => Boolean(entry.key));
      })
      .then((items) => {
        if (!cancelled) setPayloadFieldSuggestions(items);
      })
      .catch(() => {
        if (!cancelled) setPayloadFieldSuggestions([]);
      });
    return () => {
      cancelled = true;
    };
  }, [isTriggerNode, triggerSourceRef, node?.id]);

  useEffect(() => {
    if (!isWorkflowInvokeRoute) return;
    let cancelled = false;
    fetch('/api/workflows')
      .then(async (response) => {
        if (!response.ok) return [];
        const payload = await response.json().catch(() => ({}));
        const workflows = Array.isArray(payload?.workflows) ? payload.workflows : [];
        return workflows
          .map((entry: any) => ({
            id: typeof entry?.id === 'string' ? entry.id : '',
            name: typeof entry?.name === 'string' ? entry.name : '',
            description: typeof entry?.description === 'string' ? entry.description : '',
            hasSpec: Boolean(entry?.has_spec),
          }))
          .filter((entry: { id: string; name: string; description: string; hasSpec: boolean }) => (
            entry.id
            && entry.id !== workflowId
          ))
          .sort((a: { name: string }, b: { name: string }) => a.name.localeCompare(b.name));
      })
      .then((items) => {
        if (!cancelled) setWorkflowTargets(items);
      })
      .catch(() => {
        if (!cancelled) setWorkflowTargets([]);
      });
    return () => {
      cancelled = true;
    };
  }, [isWorkflowInvokeRoute, workflowId]);

  const handleAttach = useCallback(async () => {
    if (!workflowId) {
      setAttachError('Save draft first to attach authority-backed references.');
      return;
    }
    if (!node || !attachRef.trim()) return;
    setAttachLoading(true);
    setAttachError(null);
    try {
      const request = {
        node_id: node.id,
        authority_kind: attachKind,
        authority_ref: attachRef.trim(),
        role: attachRole,
        label: attachLabel.trim() || attachRef.trim(),
        promote_to_state: attachPromote,
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction('attachments', request, {
          label: 'Attach reference',
          reason: `Attach ${attachLabel.trim() || attachRef.trim()} to ${node.title || node.id}.`,
          outcome: `${attachLabel.trim() || attachRef.trim()} is now available to ${node.title || node.id}.`,
          authority: 'build.authority_attachments',
          target: nodeTarget(node),
          changeSummary: ['Attachment', attachLabel.trim() || attachRef.trim(), attachRole],
        });
      } else {
        await onMutate('attachments', request);
      }
      setAttachRef('');
      setAttachLabel('');
    } catch (e: any) {
      setAttachError(e.message || 'Failed to attach');
    } finally {
      setAttachLoading(false);
    }
  }, [workflowId, node, attachKind, attachRef, attachLabel, attachRole, attachPromote, onCommitAuthorityAction, onMutate]);

  const handleStageImport = useCallback(async () => {
    if (!workflowId) {
      setImportError('Save draft first to stage imports.');
      return;
    }
    if (!node || !importLocator.trim()) return;
    setImportLoading(true);
    setImportError(null);
    const label = importLabel.trim() || importLocator.trim().split('/').pop() || 'import';
    const targetRef = `#${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
    try {
      const request = {
        node_id: node.id,
        source_kind: 'net',
        source_locator: importLocator.trim(),
        requested_shape: {
          label,
          target_ref: targetRef,
          kind: 'type',
        },
        payload: { note: `Requested from ${importLocator.trim()}` },
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction('imports', request, {
          label: 'Stage import',
          reason: `Stage ${label} for ${node.title || node.id}.`,
          outcome: `${label} is now staged as an import for ${node.title || node.id}.`,
          authority: 'build.import_snapshots',
          target: nodeTarget(node),
          changeSummary: ['Import snapshot', label],
        });
      } else {
        await onMutate('imports', request);
      }
      setImportLocator('');
      setImportLabel('');
    } catch (e: any) {
      setImportError(e.message || 'Failed to stage import');
    } finally {
      setImportLoading(false);
    }
  }, [workflowId, node, importLocator, importLabel, onCommitAuthorityAction, onMutate]);

  const handleAdmitImport = useCallback(async (snapshotId: string, shape: Record<string, unknown>) => {
    if (!workflowId) {
      setImportError('Save draft first to admit imports.');
      return;
    }
    setImportLoading(true);
    setImportError(null);
    try {
      const request = {
        target_kind: 'import_snapshot',
        target_ref: snapshotId,
        decision: 'approve',
        candidate_payload: {
          target_ref: shape.target_ref || `#${snapshotId}`,
          label: shape.label || snapshotId,
          kind: shape.kind || 'type',
        },
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction('review_decisions', request, {
          label: 'Approve import candidate',
          reason: `Approve ${String(shape.label || snapshotId)} as a reviewed import candidate.`,
          outcome: `${String(shape.label || snapshotId)} is now explicitly approved for downstream binding.`,
          authority: 'build.import_snapshots',
          target: {
            kind: 'import',
            label: String(shape.label || snapshotId),
            id: snapshotId,
          },
          changeSummary: ['Admitted target', String(shape.target_ref || shape.label || snapshotId)],
        });
      } else {
        await onMutate('review_decisions', request);
      }
    } catch (e: any) {
      setImportError(e.message || 'Failed to admit import');
    } finally {
      setImportLoading(false);
    }
  }, [workflowId, onCommitAuthorityAction, onMutate]);

  const handleMaterialize = useCallback(async () => {
    if (!workflowId) {
      setImportError('Save draft first to materialize imports.');
      return;
    }
    if (!node || !importLocator.trim()) return;
    setImportLoading(true);
    setImportError(null);
    const label = importLabel.trim() || importLocator.trim().split('/').pop() || 'import';
    const targetRef = `#${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
    try {
      const request = {
        node_id: node.id,
        source_kind: 'net',
        source_locator: importLocator.trim(),
        requested_shape: {
          label,
          target_ref: targetRef,
          kind: 'type',
        },
        authority_kind: attachKind,
        authority_ref: targetRef,
        role: attachRole,
        label,
        promote_to_state: attachPromote,
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction('materialize-here', request, {
          label: 'Materialize import',
          reason: `Stage and attach ${label} directly to ${node.title || node.id}.`,
          outcome: `${label} is now materialized for ${node.title || node.id}.`,
          authority: 'build.authority_bundle',
          target: nodeTarget(node),
          changeSummary: ['Import snapshot', label, 'Attached to node'],
        });
      } else {
        await onMutate('materialize-here', request);
      }
      setImportLocator('');
      setImportLabel('');
    } catch (e: any) {
      setImportError(e.message || 'Failed to materialize');
    } finally {
      setImportLoading(false);
    }
  }, [workflowId, node, importLocator, importLabel, attachKind, attachRole, attachPromote, onCommitAuthorityAction, onMutate]);

  const handleSaveTrigger = useCallback(async () => {
    if (!node || !buildGraph || !canCommitGraph || !isTriggerRoute(triggerRoute)) return;
    setTriggerLoading(true);
    setTriggerError(null);
    try {
      const filter = triggerNeedsJsonEditor
        ? parseTriggerFilter(triggerFilterText)
        : parseTriggerFilterRows(triggerFilterRows);
      const nodes = [...(buildGraph.nodes || [])];
      const idx = nodes.findIndex(graphNode => graphNode.node_id === node.id);
      if (idx < 0) return;

      const nextTrigger =
        triggerRoute === TRIGGER_SCHEDULE_ROUTE
          ? {
              event_type: 'schedule',
              cron_expression: triggerCronExpression.trim() || '@daily',
              filter,
            }
          : triggerRoute === TRIGGER_WEBHOOK_ROUTE
            ? {
                event_type: WEBHOOK_TRIGGER_EVENT,
                source_ref: triggerSourceRef.trim() || undefined,
                filter,
              }
            : {
                event_type: 'manual',
                source_ref: triggerSourceRef.trim() || undefined,
                filter,
              };

      nodes[idx] = {
        ...nodes[idx],
        trigger: nextTrigger,
      };
      const nextGraph = { ...buildGraph, nodes };
      if (onCommitGraphAction) {
        await onCommitGraphAction(nextGraph, {
          label: 'Save trigger config',
          reason: `Update trigger settings for ${node.title || node.id}.`,
          outcome: `${node.title || node.id} now uses the updated trigger configuration.`,
          target: nodeTarget(node),
          changeSummary: ['Trigger settings', triggerRoute],
        });
      } else if (onUpdateBuildGraph) {
        await onUpdateBuildGraph(nextGraph);
      }
    } catch (e: any) {
      setTriggerError(e.message || 'Failed to save trigger');
    } finally {
      setTriggerLoading(false);
    }
  }, [
    buildGraph,
    canCommitGraph,
    node,
    onCommitGraphAction,
    onUpdateBuildGraph,
    triggerRoute,
    triggerCronExpression,
    triggerSourceRef,
    triggerNeedsJsonEditor,
    triggerFilterRows,
    triggerFilterText,
  ]);

  const handleTriggerJsonBlur = useCallback(() => {
    try {
      const parsed = parseTriggerFilter(triggerFilterText);
      if (!triggerFilterHasComplexValue(parsed)) {
        const rows = triggerFilterRowsFromObject(parsed);
        setTriggerFilterRows(rows);
        setActiveTriggerFilterId(rows[0]?.id || null);
        setTriggerError(null);
      }
    } catch {
      /* keep JSON draft while invalid */
    }
  }, [triggerFilterText]);

  const activeTriggerFilterRow = useMemo(
    () => triggerFilterRows.find((row) => row.id === activeTriggerFilterId) || null,
    [activeTriggerFilterId, triggerFilterRows],
  );

  const updateTriggerFilterRow = useCallback((id: string, patch: Partial<TriggerFilterFieldRow>) => {
    setTriggerFilterRows((previous) => previous.map((entry) => (
      entry.id === id
        ? { ...entry, ...patch }
        : entry
    )));
  }, []);

  const addTriggerFilterRow = useCallback(() => {
    const next = { id: `trigger-filter-${Date.now()}`, key: '', valueType: 'string' as const, valueText: '' };
    setTriggerFilterRows((previous) => [...previous, next]);
    setActiveTriggerFilterId(next.id);
  }, []);

  const removeTriggerFilterRow = useCallback((id: string) => {
    setTriggerFilterRows((previous) => {
      const next = previous.filter((entry) => entry.id !== id);
      if (next.length > 0) {
        setActiveTriggerFilterId((current) => (current === id ? next[0].id : current));
        return next;
      }
      const fallback = { id: `trigger-filter-${Date.now()}`, key: '', valueType: 'string' as const, valueText: '' };
      setActiveTriggerFilterId(fallback.id);
      return [fallback];
    });
  }, []);

  const applyTriggerFilterSuggestion = useCallback((suggestion: typeof TRIGGER_FILTER_SUGGESTIONS[number]) => {
    setTriggerFilterRows((previous) => {
      const existing = previous.find((entry) => entry.key.trim() === suggestion.key);
      if (existing) {
        setActiveTriggerFilterId(existing.id);
        return previous.map((entry) => (
          entry.id === existing.id
            ? { ...entry, valueType: suggestion.valueType, valueText: suggestion.valueText }
            : entry
        ));
      }
      const next = {
        id: `trigger-filter-${Date.now()}`,
        key: suggestion.key,
        valueType: suggestion.valueType,
        valueText: suggestion.valueText,
      };
      setActiveTriggerFilterId(next.id);
      return [...previous, next];
    });
  }, []);

  const handleHttpRequestPresetChange = useCallback((nextPreset: HttpRequestPreset) => {
    const presetDefinition = httpRequestPresetDefinition(nextPreset);
    setHttpRequestPreset(nextPreset);
    setWebhookMethod(presetDefinition.method);
    setWebhookHeadersText(formatJsonObject(presetDefinition.headers));
    setWebhookBodyText(formatJsonValue(presetDefinition.body));
    setNodeSaveError(null);
    setOutputRows((prev) => {
      if (stringArrayFromContractRows(prev).length > 0) return prev;
      return contractRowsFromStringArray(['http_response'], 'outputs');
    });
  }, []);

  const handleSaveNodePrimitive = useCallback(async () => {
    if (!node || !buildGraph || !canCommitGraph) return;
    setNodeSaveLoading(true);
    setNodeSaveError(null);
    try {
      const nodes = [...(buildGraph.nodes || [])];
      const idx = nodes.findIndex(graphNode => graphNode.node_id === node.id);
      if (idx < 0) return;

      const existingIntegrationArgs = normalizeIntegrationArgs(nodes[idx].integration_args);
      let nextIntegrationArgs: Record<string, unknown> | undefined;
      if (isNotificationRoute) {
        nextIntegrationArgs = buildNotificationIntegrationArgs(existingIntegrationArgs, {
          title: notificationTitle,
          message: notificationMessage,
          status: notificationStatus,
          fallbackTitle: nodeTitle.trim(),
          fallbackMessage: nodeSummary.trim(),
        });
      } else if (isWebhookRoute) {
        const normalizedUrl = webhookUrl.trim();
        if (!normalizedUrl) {
          throw new Error('HTTP request URL is required.');
        }
        const parsedHeaders = parseOptionalJsonObject(webhookHeadersText, 'HTTP request headers');
        const bodyValue = parseOptionalJsonValue(webhookBodyText);
        const normalizedMethod = webhookMethod.trim().toUpperCase() || 'POST';
        if (webhookTimeoutText.trim()) {
          const timeout = Number(webhookTimeoutText.trim());
          if (!Number.isFinite(timeout) || timeout <= 0) {
            throw new Error('HTTP request timeout must be a positive number.');
          }
        }
        nextIntegrationArgs = buildHttpRequestIntegrationArgs(existingIntegrationArgs, {
          preset: httpRequestPreset,
          url: normalizedUrl,
          method: normalizedMethod,
          headers: parsedHeaders,
          body: requestMethodSupportsBody(normalizedMethod) ? bodyValue : undefined,
          timeoutText: webhookTimeoutText,
        });
      } else if (isWorkflowInvokeRoute) {
        if (!invokeWorkflowId.trim()) {
          throw new Error('Choose a workflow to invoke.');
        }
        const payloadValue = parseOptionalJsonValue(invokePayloadText);
        nextIntegrationArgs = buildWorkflowInvokeIntegrationArgs(existingIntegrationArgs, {
          workflowId: invokeWorkflowId.trim(),
          payload: payloadValue,
        });
      } else if (isIntegrationRoute) {
        nextIntegrationArgs = parseOptionalJsonObject(genericIntegrationArgsText, 'Integration args');
      }

      nodes[idx] = {
        ...nodes[idx],
        title: nodeTitle.trim() || nodes[idx].title || 'Untitled step',
        summary: nodeSummary.trim(),
        prompt: nodePrompt.trim(),
        required_inputs: stringArrayFromContractRows(requiredInputRows),
        outputs: stringArrayFromContractRows(outputRows),
        persistence_targets: stringArrayFromContractRows(persistenceTargetRows),
        handoff_target: nodeHandoffTarget.trim() || null,
        integration_args: nextIntegrationArgs,
      };
      const nextGraph = { ...buildGraph, nodes };
      if (onCommitGraphAction) {
        await onCommitGraphAction(nextGraph, {
          label: 'Save block properties',
          reason: `Update the primitive contract for ${node.title || node.id}.`,
          outcome: `${nodeTitle.trim() || nodes[idx].title || node.id} now uses the saved block properties.`,
          target: nodeTarget(node),
          changeSummary: ['Primitive contract', triggerRoute || 'unassigned route'],
        });
      } else if (onUpdateBuildGraph) {
        await onUpdateBuildGraph(nextGraph);
      }
      if (workflowId && node.id && nodePrompt.trim()) {
        recordMoonHistory(`prompt:${workflowId}:${node.id}`, nodePrompt);
      }
    } catch (e: any) {
      setNodeSaveError(e.message || 'Failed to save properties');
    } finally {
      setNodeSaveLoading(false);
    }
  }, [
    buildGraph,
    canCommitGraph,
    genericIntegrationArgsText,
    invokePayloadText,
    invokeWorkflowId,
    isIntegrationRoute,
    isNotificationRoute,
    isWebhookRoute,
    isWorkflowInvokeRoute,
    node,
    nodeHandoffTarget,
    outputRows,
    persistenceTargetRows,
    nodePrompt,
    requiredInputRows,
    nodeSummary,
    nodeTitle,
    notificationMessage,
    notificationStatus,
    notificationTitle,
    onCommitGraphAction,
    onUpdateBuildGraph,
    httpRequestPreset,
    webhookBodyText,
    webhookHeadersText,
    webhookMethod,
    webhookTimeoutText,
    webhookUrl,
    workflowId,
    triggerRoute,
  ]);

  const handleEdgeConditionModeChange = useCallback((nextMode: BranchConditionMode) => {
    if (nextMode === edgeConditionMode) return;
    if (nextMode === 'json') {
      try {
        const nextCondition = buildSimpleBranchCondition(edgeConditionField, edgeConditionOp, edgeConditionValueText);
        setEdgeConditionText(formatJsonObject(nextCondition));
      } catch {
        // Let the user continue in JSON mode even if the simple composer is incomplete.
      }
      setEdgeConditionMode('json');
      setEdgeConditionError(null);
      return;
    }

    try {
      const parsed = parseJsonObject(edgeConditionText, 'Condition JSON is required.');
      const simpleCondition = parseSimpleBranchCondition(parsed);
      if (!simpleCondition) {
        throw new Error('This condition uses a nested tree. Keep JSON mode for advanced branch logic.');
      }
      setEdgeConditionField(simpleCondition.field);
      setEdgeConditionOp(simpleCondition.op);
      setEdgeConditionValueText(simpleCondition.valueText);
      setEdgeConditionMode('simple');
      setEdgeConditionError(null);
    } catch (error: any) {
      setEdgeConditionError(error.message || 'Unable to switch to the simple branch composer.');
    }
  }, [edgeConditionField, edgeConditionMode, edgeConditionOp, edgeConditionText, edgeConditionValueText]);

  const handleSaveConditionalEdge = useCallback(async () => {
    if (!selectedEdge || !buildGraph || !canCommitGraph || !buildEdge) return;
    setEdgeConditionLoading(true);
    setEdgeConditionError(null);
    try {
      const condition = edgeConditionMode === 'json'
        ? parseJsonObject(edgeConditionText, 'Condition JSON is required.')
        : buildSimpleBranchCondition(edgeConditionField, edgeConditionOp, edgeConditionValueText);
      const edges = [...(buildGraph.edges || [])];
      const sourceNodeId = buildEdge.from_node_id;
      for (let index = 0; index < edges.length; index += 1) {
        const edge = edges[index];
        const release = normalizeBuildEdgeRelease(edge);
        if (edge.from_node_id !== sourceNodeId || release.family !== 'conditional') continue;
        const branchReason = release.branch_reason || 'then';
        edges[index] = withBuildEdgeRelease(edge, {
          family: 'conditional',
          edge_type: 'conditional',
          state: 'configured',
          label: release.label || branchLabel(branchReason) || 'Branch',
          branch_reason: branchReason,
          release_condition: branchReason === 'else'
            ? { op: 'not', conditions: [condition] }
            : condition,
          config: {
            ...(release.config || {}),
            condition,
          },
        });
      }
      const nextGraph = { ...buildGraph, edges };
      if (onCommitGraphAction) {
        await onCommitGraphAction(nextGraph, {
          label: 'Save branch condition',
          reason: `Update the conditional gate from ${edgeFromLabel || selectedEdge.from}.`,
          outcome: `Conditional branches from ${edgeFromLabel || selectedEdge.from} now use the saved condition.`,
          target: edgeTarget(selectedEdge, edgeFromLabel, edgeToLabel),
          changeSummary: ['Conditional gate', 'Condition JSON'],
        });
      } else if (onUpdateBuildGraph) {
        await onUpdateBuildGraph(nextGraph);
      }
      if (workflowId && selectedEdge?.id && edgeConditionMode === 'json' && edgeConditionText.trim()) {
        recordMoonHistory(`condition:${workflowId}:${selectedEdge.id}`, edgeConditionText);
      }
    } catch (e: any) {
      setEdgeConditionError(e.message || 'Failed to save branch condition');
    } finally {
      setEdgeConditionLoading(false);
    }
  }, [
    buildEdge,
    buildGraph,
    canCommitGraph,
    edgeConditionField,
    edgeConditionMode,
    edgeConditionOp,
    edgeConditionText,
    edgeConditionValueText,
    edgeFromLabel,
    onCommitGraphAction,
    onUpdateBuildGraph,
    selectedEdge,
    workflowId,
  ]);

  const handleClearGate = useCallback(async () => {
    if (!selectedEdge || !buildGraph || !canCommitGraph || !buildEdge) return;
    setGateClearLoading(true);
    setGateClearError(null);
    try {
      const edges = [...(buildGraph.edges || [])];
      const index = edges.findIndex((edge) => edge.edge_id === buildEdge.edge_id);
      if (index < 0) return;

      edges[index] = withBuildEdgeRelease(edges[index], null);

      // Conditional branches come in then/else pairs — clear the sibling too.
      if (isConditionalEdge) {
        const sourceNodeId = buildEdge.from_node_id;
        for (let i = 0; i < edges.length; i += 1) {
          if (i === index) continue;
          const sibling = edges[i];
          if (sibling.from_node_id !== sourceNodeId) continue;
          if (normalizeBuildEdgeRelease(sibling).family !== 'conditional') continue;
          edges[i] = withBuildEdgeRelease(sibling, null);
        }
      }

      const nextGraph = { ...buildGraph, edges };
      if (onCommitGraphAction) {
        await onCommitGraphAction(nextGraph, {
          label: 'Remove gate',
          reason: `Remove the gate on the connection from ${edgeFromLabel || selectedEdge.from} to ${edgeToLabel || selectedEdge.to}.`,
          outcome: `${edgeToLabel || selectedEdge.to} now runs on the standard success path.`,
          target: edgeTarget(selectedEdge, edgeFromLabel, edgeToLabel),
          changeSummary: ['Gate family', 'After success'],
        });
      } else if (onUpdateBuildGraph) {
        await onUpdateBuildGraph(nextGraph);
      }
    } catch (error: any) {
      setGateClearError(error.message || 'Failed to remove the gate.');
    } finally {
      setGateClearLoading(false);
    }
  }, [
    buildEdge,
    buildGraph,
    canCommitGraph,
    edgeFromLabel,
    edgeToLabel,
    isConditionalEdge,
    onCommitGraphAction,
    onUpdateBuildGraph,
    selectedEdge,
  ]);

	  // Sort: unresolved first, then accepted, then rejected
  const bindings = [...(content?.connectBindings || [])].sort((a, b) => {
    const order: Record<string, number> = { unresolved: 0, accepted: 1, rejected: 2 };
    return (order[a.state || 'unresolved'] || 0) - (order[b.state || 'unresolved'] || 0);
  });
  const runCompletionContract = node?.completionContract ?? null;
  const agentToolPlan = node?.agentToolPlan ?? null;
  const runContractResultKind = typeof runCompletionContract?.result_kind === 'string'
    ? runCompletionContract.result_kind.trim()
    : '';
  const runContractSubmitTools = Array.isArray(runCompletionContract?.submit_tool_names)
    ? runCompletionContract.submit_tool_names.filter((tool): tool is string => typeof tool === 'string' && tool.trim().length > 0)
    : [];
  const agentToolName = typeof agentToolPlan?.tool_name === 'string' ? agentToolPlan.tool_name.trim() : '';
  const agentToolOperation = typeof agentToolPlan?.operation === 'string' ? agentToolPlan.operation.trim() : '';
  const agentToolFocus = typeof agentToolPlan?.focus === 'string' ? agentToolPlan.focus.trim() : '';
  const agentToolRepeats = typeof agentToolPlan?.repeats === 'number' && Number.isFinite(agentToolPlan.repeats)
    ? agentToolPlan.repeats
    : null;
  const agentToolTargets = Array.isArray(agentToolPlan?.target_fields)
    ? agentToolPlan.target_fields.filter((field): field is string => typeof field === 'string' && field.trim().length > 0)
    : [];
  const nodeCapabilities = Array.isArray(node?.capabilities) ? node.capabilities.filter(Boolean) : [];
  const nodeWriteScope = Array.isArray(node?.writeScope) ? node.writeScope.filter(Boolean) : [];
  const hasAgentPacket = Boolean(
    node
    && (
      agentToolPlan
      || node.agent
      || nodeCapabilities.length
      || nodeWriteScope.length
      || node.taskType
    ),
  );
  const hasRunContract = Boolean(
    node
    && (
      runCompletionContract
      || node.taskType
      || node.description
      || node.outcomeGoal
      || node.prompt
    ),
  );

  return (
    <>
      <button className="moon-dock__close" onClick={onClose} aria-label="Close detail panel">&times;</button>

      {/* Gate config — when an edge is selected */}
      {selectedEdge && onApplyGate ? (
        <>
          <div className="moon-dock-header">
            <div className="moon-dock__title">Gate</div>
            <div className="moon-dock__subtitle">{edgeFromLabel} &rarr; {edgeToLabel}</div>
          </div>
          <div className="moon-dock-section-card moon-dock-section-card--dense">
            <div className="moon-gate-panel">
              <div className="moon-gate-panel__meta">
                <span className="moon-surface-badge" title={selectedEdge.gateState === 'empty' ? "No approval required" : undefined}>
                  {selectedGateCatalogModel?.policy.badge || (selectedEdge.gateState === 'empty' ? 'Ungated' : 'Gate')}
                </span>
                {selectedGateCatalogModel && (
                  <span className={`moon-truth-badge moon-truth-badge--${selectedGateCatalogModel.truth.category}`}>
                    {selectedGateCatalogModel.truth.badge}
                  </span>
                )}
              </div>
              <div className="moon-gate-panel__title">
                {selectedEdge.gateState === 'empty'
                  ? 'This connection is ungated'
                  : selectedEdge.gateLabel || selectedGateCatalogModel?.item.label || 'Gate configured'}
              </div>
            </div>

            {isFailureEdge && (
              <div className="moon-branch-editor__hint">
                Runs only when {failureSourceLabel} fails.
              </div>
            )}

            {isConditionalEdge && (
              <>
                <div className="moon-branch-editor">
                  <div className="moon-branch-editor__routes" aria-label="Branch routes">
                    <div className={`moon-branch-editor__route${buildEdgeRelease?.branch_reason === 'then' ? ' moon-branch-editor__route--active' : ''}`}>
                      <span className="moon-branch-editor__route-label">Then</span>
                      <span className="moon-branch-editor__route-target">{conditionalTargets.thenTarget || edgeToLabel || 'Then path'}</span>
                    </div>
                    <div className={`moon-branch-editor__route${buildEdgeRelease?.branch_reason === 'else' ? ' moon-branch-editor__route--active' : ''}`}>
                      <span className="moon-branch-editor__route-label">Else</span>
                      <span className="moon-branch-editor__route-target">{conditionalTargets.elseTarget || 'Inverse path'}</span>
                    </div>
                  </div>

                  <div className="moon-branch-editor__control">
                    <label className="moon-dock-form__label" htmlFor="moon-branch-editor-mode">Condition mode</label>
                    <div className="moon-dock-form__row">
                      <select
                        id="moon-branch-editor-mode"
                        className="moon-dock-form__select"
                        value={edgeConditionMode}
                        onChange={e => handleEdgeConditionModeChange(e.target.value as BranchConditionMode)}
                      >
                        <option value="simple">Form (recommended)</option>
                        <option value="json">JSON (advanced)</option>
                      </select>
                    </div>
                  </div>

                  {edgeConditionMode === 'simple' ? (
                    <>
                      <input
                        className="moon-dock-form__input"
                        type="text"
                        value={edgeConditionField}
                        onChange={e => setEdgeConditionField(e.target.value)}
                        placeholder="Field or path, for example should_continue"
                      />
                      <div className="moon-dock-form__row">
                        <select
                          className="moon-dock-form__select"
                          value={edgeConditionOp}
                          onChange={e => setEdgeConditionOp(e.target.value as BranchComposerOp)}
                        >
                          {BRANCH_OP_OPTIONS.map((option) => (
                            <option key={option.value} value={option.value}>{option.label}</option>
                          ))}
                        </select>
                      </div>
                      {selectedBranchOp.expectsValue ? (
                        <textarea
                          className="moon-dock-form__input"
                          value={edgeConditionValueText}
                          onChange={e => setEdgeConditionValueText(e.target.value)}
                          placeholder="Value (text or ['array'])"
                          rows={3}
                          style={{ minHeight: 86, resize: 'vertical' }}
                        />
                      ) : null}
                    </>
                  ) : (
                    <>
                      <MoonJsonEditor
                        value={edgeConditionText}
                        onChange={setEdgeConditionText}
                        placeholder={'Branch condition JSON\n{\n  "op": "eq",\n  "key": "env",\n  "value": "prod"\n}'}
                        rows={8}
                        minHeight={132}
                        ariaLabel="Branch condition JSON"
                      />
                      {workflowId && selectedEdge?.id && (
                        <MoonLocalHistoryRail
                          scopeKey={`condition:${workflowId}:${selectedEdge.id}`}
                          currentValue={edgeConditionText}
                          onRestore={(value) => setEdgeConditionText(value)}
                          label="Previous branch conditions (this device)"
                        />
                      )}
                    </>
                  )}
                </div>
                <div className="moon-dock-form__actions">
                  <button
                    className="moon-dock-form__btn"
                    onClick={handleSaveConditionalEdge}
                    disabled={edgeConditionLoading || !buildGraph || !canCommitGraph}
                  >
                    {edgeConditionLoading ? <><span className="moon-spinner" /> Saving...</> : 'Save branch condition'}
                  </button>
                </div>
                {edgeConditionError && <div className="moon-dock-form__error">{edgeConditionError}</div>}
              </>
            )}

            {selectedEdge.gateState !== 'empty' && (
              <>
                <div className="moon-dock-form__actions">
                  <MoonConfirmButton
                    label={gateClearLoading ? <><span className="moon-spinner" /> Removing...</> : 'Remove gate'}
                    confirmLabel="Click again to remove"
                    primedHint="Removes the gate for good"
                    disabled={gateClearLoading || !buildGraph || !canCommitGraph}
                    onConfirm={handleClearGate}
                  />
                </div>
                {gateClearError && <div className="moon-dock-form__error">{gateClearError}</div>}
              </>
            )}
          </div>
        </>
      ) : (
        <div className="moon-dock-header">
          <div className="moon-dock__title">{node ? node.title : 'Detail'}</div>
          {node?.route ? <div className="moon-dock__subtitle">Route: {node.route}</div> : null}
        </div>
      )}

      {!node && !selectedEdge ? (
        <WorkflowInspector summary={workflowSummary} />
      ) : node ? (
        <>
          {/* Context ribbon — upstream producers and downstream consumers.
              Edge families show up as tone modifiers on the connector, so you
              can read the structural grammar without opening every edge. */}
          {contextRibbon && (
            <div className="moon-detail__ribbon" aria-label="Node context">
              <div className="moon-detail__ribbon-col moon-detail__ribbon-col--upstream">
                <div className="moon-dock__section-label moon-detail__ribbon-heading">Receives</div>
                {contextRibbon.upstream.length === 0 ? (
                  <div className="moon-detail__ribbon-empty">origin</div>
                ) : (
                  contextRibbon.upstream.map((n) => (
                    <div key={n.edgeId} className={`moon-detail__ribbon-item moon-detail__ribbon-item--${n.family}`}>
                      <span className="moon-detail__ribbon-dot" aria-hidden="true" />
                      <span className="moon-detail__ribbon-label">{n.title}</span>
                      <span className="moon-detail__ribbon-family">{ribbonFamilyLabel(n.family)}</span>
                    </div>
                  ))
                )}
              </div>
              <div className="moon-detail__ribbon-center">
                <span className="moon-detail__ribbon-center-dot" aria-hidden="true" />
                <span className="moon-detail__ribbon-center-label">{node.title || 'this'}</span>
              </div>
              <div className="moon-detail__ribbon-col moon-detail__ribbon-col--downstream">
                <div className="moon-dock__section-label moon-detail__ribbon-heading">Produces</div>
                {contextRibbon.downstream.length === 0 ? (
                  <div className="moon-detail__ribbon-empty">terminal</div>
                ) : (
                  contextRibbon.downstream.map((n) => (
                    <div key={n.edgeId} className={`moon-detail__ribbon-item moon-detail__ribbon-item--${n.family}`}>
                      <span className="moon-detail__ribbon-family">{ribbonFamilyLabel(n.family)}</span>
                      <span className="moon-detail__ribbon-label">{n.title}</span>
                      <span className="moon-detail__ribbon-dot" aria-hidden="true" />
                    </div>
                  ))
                )}
              </div>
            </div>
          )}
          {hasAgentPacket && (
            <div className="moon-dock-section-card moon-dock-section-card--dense" aria-label="Agent packet contract">
              <div className="moon-dock-section-card__header">
                <div className="moon-dock__section-label">Agent packet</div>
                <div className="moon-run-contract__badges">
                  <span className="moon-truth-badge moon-truth-badge--runtime">One node</span>
                  <span className="moon-truth-badge moon-truth-badge--persisted">Field edits</span>
                </div>
              </div>
              <div className="moon-run-contract__grid">
                {node.taskType && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Task</span>
                    <span className="moon-run-contract__value">{node.taskType}</span>
                  </div>
                )}
                {node.agent && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Agent</span>
                    <span className="moon-run-contract__value">{node.agent}</span>
                  </div>
                )}
                {agentToolName && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Tool lane</span>
                    <span className="moon-run-contract__value">
                      {agentToolName}{agentToolOperation ? ` · ${agentToolOperation}` : ''}
                      {agentToolRepeats ? ` · ${agentToolRepeats}x` : ''}
                    </span>
                  </div>
                )}
                {agentToolTargets.length > 0 && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Fields</span>
                    <span className="moon-run-contract__value">{agentToolTargets.join(', ')}</span>
                  </div>
                )}
                {nodeCapabilities.length > 0 && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Capabilities</span>
                    <span className="moon-run-contract__value">{nodeCapabilities.join(', ')}</span>
                  </div>
                )}
                {nodeWriteScope.length > 0 && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Writes</span>
                    <span className="moon-run-contract__value">{nodeWriteScope.join(', ')}</span>
                  </div>
                )}
                {agentToolFocus && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Focus</span>
                    <span className="moon-run-contract__value">{agentToolFocus}</span>
                  </div>
                )}
              </div>
            </div>
          )}
          {hasRunContract && (
            <div className="moon-dock-section-card moon-dock-section-card--dense" aria-label="Run completion gate">
              <div className="moon-dock-section-card__header">
                <div className="moon-dock__section-label">Completion gate</div>
                <div className="moon-run-contract__badges">
                  {runCompletionContract?.submission_required === true && (
                    <span className="moon-truth-badge moon-truth-badge--runtime">Submission required</span>
                  )}
                  {runCompletionContract?.verification_required === true && (
                    <span className="moon-truth-badge moon-truth-badge--alias">Verification required</span>
                  )}
                  {runCompletionContract && runCompletionContract.submission_required !== true && runCompletionContract.verification_required !== true && (
                    <span className="moon-truth-badge moon-truth-badge--persisted">Recorded contract</span>
                  )}
                </div>
              </div>
              <div className="moon-run-contract__grid">
                {node.taskType && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Task type</span>
                    <span className="moon-run-contract__value">{node.taskType}</span>
                  </div>
                )}
                {runContractResultKind && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Result</span>
                    <span className="moon-run-contract__value">{runContractResultKind}</span>
                  </div>
                )}
                {runContractSubmitTools.length > 0 && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Submit with</span>
                    <span className="moon-run-contract__value">{runContractSubmitTools.join(', ')}</span>
                  </div>
                )}
                {node.outcomeGoal && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Outcome</span>
                    <span className="moon-run-contract__value">{node.outcomeGoal}</span>
                  </div>
                )}
                {node.description && (
                  <div className="moon-run-contract__row">
                    <span className="moon-run-contract__label">Scope</span>
                    <span className="moon-run-contract__value">{node.description}</span>
                  </div>
                )}
              </div>
              {node.prompt && (
                <details className="moon-run-contract__prompt">
                  <summary>Prompt</summary>
                  <pre>{node.prompt}</pre>
                </details>
              )}
            </div>
          )}
          {isTriggerNode && (
            <div className="moon-dock-section-card">
              <div className="moon-dock-section-card__header">
                <div className="moon-dock__section-label">Trigger config</div>
                <div className="moon-dock__item-desc" style={{ marginTop: 0, fontWeight: 600 }}>
                  {triggerRoute === TRIGGER_SCHEDULE_ROUTE
                    ? 'Schedule trigger'
                    : triggerRoute === TRIGGER_WEBHOOK_ROUTE
                      ? 'Webhook trigger'
                      : 'Manual trigger'}
                </div>
              </div>
              {triggerRoute === TRIGGER_SCHEDULE_ROUTE ? (
                <>
                  <label className="moon-dock-form__label" htmlFor="moon-trigger-cron">Schedule</label>
                  <MoonCronBuilder
                    id="moon-trigger-cron"
                    value={triggerCronExpression}
                    onChange={setTriggerCronExpression}
                  />
                </>
              ) : (
                <>
                  <label className="moon-dock-form__label" htmlFor="moon-trigger-source-ref">
                    Source reference
                  </label>
                  <MoonPickerInput
                    id="moon-trigger-source-ref"
                    value={triggerSourceRef}
                    onChange={setTriggerSourceRef}
                    placeholder={triggerRoute === TRIGGER_WEBHOOK_ROUTE ? 'Choose a webhook endpoint' : 'Optional source identifier'}
                    suggestionsUrl={triggerRoute === TRIGGER_WEBHOOK_ROUTE ? '/api/moon/pickers/webhook-sources' : undefined}
                    suggestionsKey="sources"
                    hint={triggerRoute === TRIGGER_WEBHOOK_ROUTE
                      ? 'Pick one of your registered webhook endpoints. New endpoints appear after you create them in the Integrations panel.'
                      : undefined}
                    ariaLabel="Trigger source reference"
                  />
                </>
              )}
              <div className="moon-dock-subsection">
                <div className="moon-dock__section-label" style={{ marginTop: 0 }}>Event filter</div>
                {triggerNeedsJsonEditor ? (
                  <MoonJsonEditor
                    id="moon-trigger-event-filter"
                    value={triggerFilterText}
                    onChange={(next) => { setTriggerFilterText(next); }}
                    onValid={() => handleTriggerJsonBlur()}
                    placeholder={'Event filter JSON\n{\n  "env": "prod"\n}'}
                    rows={6}
                    minHeight={110}
                    ariaLabel="Trigger filter JSON"
                    templates={[
                      { label: 'env = "prod"', snippet: '"env": "prod"' },
                      { label: 'priority = "high"', snippet: '"priority": "high"' },
                      { label: 'dry_run = false', snippet: '"dry_run": false' },
                      { label: 'source = "api"', snippet: '"source": "api"' },
                    ]}
                  />
                ) : (
                  <>
                    <div className="moon-trigger-pill-row">
                      {triggerFilterRows.map((row) => (
                        <button
                          key={row.id}
                          type="button"
                          className={`moon-trigger-pill${activeTriggerFilterId === row.id ? ' moon-trigger-pill--active' : ''}`}
                          onClick={() => setActiveTriggerFilterId(row.id)}
                        >
                          <span className="moon-trigger-pill__label">{triggerFilterChipText(row)}</span>
                        </button>
                      ))}
                      <button
                        type="button"
                        className="moon-trigger-pill moon-trigger-pill--add"
                        onClick={addTriggerFilterRow}
                      >
                        + Filter
                      </button>
                    </div>
                    <div className="moon-trigger-pill-suggestions">
                      {TRIGGER_FILTER_SUGGESTIONS.map((suggestion) => (
                        <button
                          key={suggestion.label}
                          type="button"
                          className="moon-trigger-pill-suggestion"
                          onClick={() => applyTriggerFilterSuggestion(suggestion)}
                        >
                          {suggestion.label}
                        </button>
                      ))}
                    </div>
                    {activeTriggerFilterRow && (
                      <div className="moon-trigger-pill-editor">
                        <div className="moon-dock-form__row" style={{ marginBottom: 4 }}>
                          <MoonPickerInput
                            value={activeTriggerFilterRow.key}
                            onChange={(next) => updateTriggerFilterRow(activeTriggerFilterRow.id, { key: next })}
                            placeholder="Payload field, e.g. env"
                            extraSuggestions={payloadFieldSuggestions.map((f) => ({ value: f.key, label: f.label }))}
                            ariaLabel="Payload field name"
                            style={{ flex: 1, marginBottom: 0 }}
                          />
                          <select
                            className="moon-dock-form__select"
                            value={activeTriggerFilterRow.valueType}
                            onChange={(event) => updateTriggerFilterRow(activeTriggerFilterRow.id, { valueType: event.target.value as TriggerFilterValueType })}
                            style={{ minWidth: 120 }}
                          >
                            <option value="string">Text</option>
                            <option value="number">Number</option>
                            <option value="boolean">True/false</option>
                            <option value="null">Null</option>
                            <option value="json">JSON</option>
                          </select>
                        </div>
                        {activeTriggerFilterRow.valueType !== 'null' && (
                          <MoonPickerInput
                            value={activeTriggerFilterRow.valueText}
                            onChange={(next) => updateTriggerFilterRow(activeTriggerFilterRow.id, { valueText: next })}
                            placeholder="Expected value, e.g. prod"
                            extraSuggestions={
                              (payloadFieldSuggestions.find((f) => f.key === activeTriggerFilterRow.key.trim())?.samples || [])
                                .map((sample) => ({ value: sample, label: sample }))
                            }
                            ariaLabel="Expected payload value"
                            style={{ marginBottom: 4 }}
                          />
                        )}
                        <div className="moon-dock-form__actions">
                          <button
                            type="button"
                            className="moon-dock-form__btn--small"
                            onClick={() => removeTriggerFilterRow(activeTriggerFilterRow.id)}
                          >
                            Remove selected
                          </button>
                          <button
                            type="button"
                            className="moon-dock-form__btn--small"
                            onClick={addTriggerFilterRow}
                          >
                            Add another
                          </button>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </div>
              <div className="moon-dock-form__actions">
                <button
                  className="moon-dock-form__btn"
                  onClick={handleSaveTrigger}
                  disabled={triggerLoading || !buildGraph || !canCommitGraph}
                >
                  {triggerLoading ? <><span className="moon-spinner" /> Saving...</> : 'Save trigger'}
                </button>
              </div>
              {triggerError && <div className="moon-dock-form__error">{triggerError}</div>}
            </div>
          )}

          {showPrimitiveEditor && (
            <div className="moon-dock-section-card">
              <div className="moon-dock-section-card__header">
                <div className="moon-dock__section-label">Step details</div>
                <div className="moon-dock__item-desc" style={{ marginTop: 0 }}>
                  Route: {triggerRoute || 'Unassigned'}
                </div>
                <div className="moon-dock__item-desc" style={{ marginTop: 0 }}>
                  Write the step in plain English. The prompt is free text; the builder infers the graph shape from your description.
                </div>
              </div>
              <input
                className="moon-dock-form__input"
                type="text"
                value={nodeTitle}
                onChange={e => setNodeTitle(e.target.value)}
                placeholder="Step title, e.g. Research Customer"
              />
              <textarea
                className="moon-dock-form__input"
                value={nodeSummary}
                onChange={e => setNodeSummary(e.target.value)}
                placeholder="Brief summary of what this step does"
                rows={3}
                style={{ minHeight: 88, resize: 'vertical' }}
              />

              {!isTriggerNode && (
                <div className="moon-dock-subsection">
                  <div className="moon-dock__section-label" style={{ marginTop: 0 }}>Prompt</div>
                  <textarea
                    className="moon-dock-form__input"
                    value={nodePrompt}
                    onChange={e => setNodePrompt(e.target.value)}
                    placeholder="Describe what this step should do. Plain English is fine."
                    rows={6}
                    style={{ minHeight: 132, resize: 'vertical' }}
                  />
                  {node?.id && workflowId && (
                    <MoonLocalHistoryRail
                      scopeKey={`prompt:${workflowId}:${node.id}`}
                      currentValue={nodePrompt}
                      onRestore={(value) => setNodePrompt(value)}
                      label="Previous prompts (this device)"
                    />
                  )}
                  <MoonPickerInput
                    value={nodeHandoffTarget}
                    onChange={setNodeHandoffTarget}
                    placeholder="Handoff target (optional)"
                    suggestionsUrl="/api/moon/pickers/authorities"
                    suggestionsKey="authorities"
                    ariaLabel="Handoff target"
                  />
                  <div style={{ fontSize: 12, color: 'var(--fg3)', marginTop: 8 }}>
                    The authority or workflow that receives this step's output.
                  </div>
                </div>
              )}

              <div className="moon-node-advanced">
                <button
                  type="button"
                  className="moon-node-advanced__toggle"
                  onClick={() => setShowAdvancedContractFields((current) => !current)}
                  aria-expanded={showAdvancedContractFields}
                  aria-controls="moon-node-advanced-contracts"
                >
                  <span className="moon-node-advanced__toggle-title">
                    {showAdvancedContractFields ? 'Hide advanced contract fields' : 'Show advanced contract fields'}
                  </span>
                  <span className="moon-node-advanced__toggle-copy">
                    Required inputs, outputs, and persistence targets stay tucked away unless you need exact control.
                  </span>
                </button>
                {showAdvancedContractFields && (
                  <div id="moon-node-advanced-contracts" className="moon-node-advanced__body">
                    <div className="moon-dock__section-label" style={{ marginTop: 0 }}>Required inputs</div>
                    <MoonContractStringListField
                      fieldId="moon-node-required-inputs"
                      ariaLabel="Required inputs"
                      inputPlaceholder="e.g. customer_id"
                      suggestions={primitiveContractSuggestions}
                      rows={requiredInputRows}
                      onChange={setRequiredInputRows}
                    />
                    <div className="moon-dock__section-label" style={{ marginTop: 12 }}>Outputs</div>
                    <MoonContractStringListField
                      fieldId="moon-node-outputs"
                      ariaLabel="Outputs"
                      inputPlaceholder="e.g. summary"
                      suggestions={primitiveContractSuggestions}
                      rows={outputRows}
                      onChange={setOutputRows}
                    />
                    <div className="moon-dock__section-label" style={{ marginTop: 12 }}>Persistence targets</div>
                    <MoonContractStringListField
                      fieldId="moon-node-persistence-targets"
                      ariaLabel="Persistence targets"
                      inputPlaceholder="e.g. crm.notes"
                      suggestions={primitiveContractSuggestions}
                      rows={persistenceTargetRows}
                      onChange={setPersistenceTargetRows}
                    />
                  </div>
                )}
              </div>

              {isNotificationRoute && (
                <div className="moon-dock-subsection">
                  <div className="moon-dock__section-label" style={{ marginTop: 0 }}>Notification properties</div>
                  <input
                    className="moon-dock-form__input"
                    type="text"
                    value={notificationTitle}
                    onChange={e => setNotificationTitle(e.target.value)}
                    placeholder="Notification title"
                  />
                  <textarea
                    className="moon-dock-form__input"
                    value={notificationMessage}
                    onChange={e => setNotificationMessage(e.target.value)}
                    placeholder="Notification message"
                    rows={4}
                    style={{ minHeight: 104, resize: 'vertical' }}
                  />
                  <select
                    className="moon-dock-form__select"
                    value={notificationStatus}
                    onChange={e => setNotificationStatus(e.target.value)}
                    style={{ marginBottom: 6 }}
                  >
                    <option value="info">Info</option>
                    <option value="success">Success</option>
                    <option value="warning">Warning</option>
                    <option value="error">Error</option>
                  </select>
                </div>
              )}

              {isWebhookRoute && (
                <div className="moon-dock-subsection">
                  <div className="moon-dock__section-label" style={{ marginTop: 0 }}>HTTP request properties</div>
                  <div className="moon-http-request__preset-grid">
                    {HTTP_REQUEST_PRESETS.map((preset) => (
                      <button
                        key={preset.value}
                        type="button"
                        className={`moon-http-request__preset${httpRequestPreset === preset.value ? ' moon-http-request__preset--active' : ''}`}
                        onClick={() => handleHttpRequestPresetChange(preset.value)}
                      >
                        <span className="moon-http-request__preset-title">{preset.label}</span>
                        <span className="moon-http-request__preset-desc">{preset.description}</span>
                      </button>
                    ))}
                  </div>
                  <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                    {selectedHttpRequestPreset.description}
                  </div>
                  <input
                    className="moon-dock-form__input"
                    type="url"
                    value={webhookUrl}
                    onChange={e => setWebhookUrl(e.target.value)}
                    placeholder={selectedHttpRequestPreset.urlPlaceholder}
                  />
                  <select
                    className="moon-dock-form__select"
                    value={webhookMethod}
                    onChange={e => setWebhookMethod(e.target.value)}
                    style={{ marginBottom: 6 }}
                  >
                    {['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].map(method => (
                      <option key={method} value={method}>{method}</option>
                    ))}
                  </select>
                  <MoonJsonEditor
                    value={webhookHeadersText}
                    onChange={setWebhookHeadersText}
                    placeholder={'Headers JSON\n{\n  "Authorization": "Bearer ..."\n}'}
                    rows={5}
                    minHeight={118}
                    ariaLabel="Webhook headers JSON"
                    templates={[
                      { label: 'Authorization: Bearer …', snippet: '"Authorization": "Bearer YOUR_TOKEN"' },
                      { label: 'Content-Type: application/json', snippet: '"Content-Type": "application/json"' },
                      { label: 'Accept: application/json', snippet: '"Accept": "application/json"' },
                      { label: 'X-Webhook-Token', snippet: '"X-Webhook-Token": "YOUR_TOKEN"' },
                      { label: 'X-Request-Id template', snippet: '"X-Request-Id": "{{run_id}}"' },
                      { label: 'User-Agent: Praxis', snippet: '"User-Agent": "Praxis/1.0"' },
                    ]}
                  />
                  {showHttpRequestBody && (
                    <MoonJsonEditor
                      value={webhookBodyText}
                      onChange={setWebhookBodyText}
                      placeholder={selectedHttpRequestPreset.bodyPlaceholder}
                      rows={5}
                      minHeight={118}
                      ariaLabel="Webhook request body"
                      allowNonObject
                      templates={[
                        { label: 'Empty object { }', snippet: '{}', replace: true },
                        { label: 'event_type + payload', snippet: '"event_type": "updated",\n  "payload": {}' },
                        { label: 'run_id pass-through', snippet: '"run_id": "{{run_id}}"' },
                      ]}
                    />
                  )}
                  <input
                    className="moon-dock-form__input"
                    type="text"
                    value={webhookTimeoutText}
                    onChange={e => setWebhookTimeoutText(e.target.value)}
                    placeholder="Timeout seconds (optional)"
                  />
                  <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                    Save persists the request contract on this block, so the dropped step reopens with the same preset and fields later.
                  </div>
                </div>
              )}

              {isWorkflowInvokeRoute && (
                <div className="moon-dock-subsection">
                  <div className="moon-dock__section-label" style={{ marginTop: 0 }}>Workflow invoke properties</div>
                  <select
                    className="moon-dock-form__select"
                    value={invokeWorkflowId}
                    onChange={e => setInvokeWorkflowId(e.target.value)}
                    style={{ marginBottom: 6 }}
                  >
                    <option value="">{runnableWorkflowTargets.length > 0 ? 'Select a workflow…' : 'No runnable workflows available'}</option>
                    {invokeWorkflowId && selectedWorkflowTarget && !selectedWorkflowTargetIsRunnable && (
                      <option value={invokeWorkflowId}>
                        {selectedWorkflowTarget.name || invokeWorkflowId} (current target)
                      </option>
                    )}
                    {runnableWorkflowTargets.map(target => (
                      <option key={target.id} value={target.id}>
                        {target.name || target.id}
                      </option>
                    ))}
                  </select>
                  <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                    {selectedWorkflowTarget
                      ? `${selectedWorkflowTarget.name || selectedWorkflowTarget.id} will be invoked as a child workflow.`
                      : runnableWorkflowTargets.length > 0
                        ? 'Choose a saved workflow with an execution plan.'
                        : 'Save and plan another workflow first, then it will appear here.'}
                  </div>
                  {selectedWorkflowTarget && !selectedWorkflowTargetIsRunnable && (
                    <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                      The current target does not have a current execution plan, so invoke will fail until that workflow is planned again.
                    </div>
                  )}
                  {workflowTargets.some(target => !target.hasSpec) && (
                    <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                      Workflows without a current execution plan stay hidden from this picker.
                    </div>
                  )}
                  <MoonJsonEditor
                    value={invokePayloadText}
                    onChange={setInvokePayloadText}
                    placeholder={'Payload JSON\n{\n  "ticket_id": "{{ticket_id}}"\n}'}
                    rows={5}
                    minHeight={118}
                    ariaLabel="Invoke payload JSON"
                    allowNonObject
                    templates={[
                      { label: 'Empty {}', snippet: '{}', replace: true },
                      { label: 'ticket_id passthrough', snippet: '"ticket_id": "{{ticket_id}}"' },
                      { label: 'run_id passthrough', snippet: '"run_id": "{{run_id}}"' },
                      { label: 'payload.* passthrough', snippet: '"payload": "{{payload}}"' },
                    ]}
                  />
                </div>
              )}

              {isIntegrationRoute && !isNotificationRoute && !isWebhookRoute && !isWorkflowInvokeRoute && (
                <div className="moon-dock-subsection">
                  <div className="moon-dock__section-label" style={{ marginTop: 0 }}>Integration properties</div>
                  <MoonJsonEditor
                    value={genericIntegrationArgsText}
                    onChange={setGenericIntegrationArgsText}
                    placeholder={'Integration args JSON\n{\n  "mode": "default"\n}'}
                    rows={6}
                    minHeight={132}
                    ariaLabel="Integration args JSON"
                  />
                </div>
              )}

              <div className="moon-dock-form__actions">
                <button
                  className="moon-dock-form__btn"
                  onClick={handleSaveNodePrimitive}
                  disabled={nodeSaveLoading || !buildGraph || !canCommitGraph}
                >
                  {nodeSaveLoading ? <><span className="moon-spinner" /> Saving...</> : 'Save step'}
                </button>
              </div>
              {nodeSaveError && <div className="moon-dock-form__error">{nodeSaveError}</div>}
            </div>
          )}

          {requiresSavedWorkflow ? (
            <div className="moon-dock-section-card">
              <div className="moon-dock__section-label">Authority-backed tools</div>
              <div className="moon-dock__item" style={{ cursor: 'default' }}>
                <div className="moon-dock__item-title">Save draft to unlock imports and attachments</div>
              </div>
            </div>
          ) : (
            <>
              <div className="moon-dock-section-card">
                <div className="moon-dock__section-label">
                  Attached ({content?.contextAttachments?.length || 0})
                </div>
                {(content?.contextAttachments || []).map(a => (
                  <div
                    key={a.attachment_id}
                    className="moon-dock__item"
                    draggable
                    onDragStart={e => {
                      e.dataTransfer.setData('moon/dock', 'context');
                      e.dataTransfer.setData('text/plain', a.label || a.authority_ref || '');
                      e.dataTransfer.effectAllowed = 'link';
                    }}
                  >
                    <div className="moon-dock__item-title">{a.label || a.authority_ref || 'Reference'}</div>
                    <div className="moon-dock__item-desc">{a.authority_kind} / {a.role || 'input'}</div>
                  </div>
                ))}
                {!(content?.contextAttachments || []).length && (
                  <div className="moon-dock__empty">No attachments yet.</div>
                )}
              </div>

              <div className="moon-dock-section-card">
                <div className="moon-dock__section-label">
                  Imports ({content?.imports?.length || 0})
                </div>
                {(content?.imports || []).map(s => (
                  <div key={s.snapshot_id} className="moon-dock__item">
                    <div className="moon-dock__item-title">{s.source_locator || s.snapshot_id}</div>
                    <div className="moon-dock__item-desc">
                      {s.approval_state === 'admitted' ? 'Admitted' : 'Staged'}
                    </div>
                    {s.approval_state !== 'admitted' && (
                      <div className="moon-dock-form__actions">
                        <button
                          className="moon-dock-form__btn--small"
                          onClick={() => handleAdmitImport(s.snapshot_id, (s as any).requested_shape || {})}
                        >
                          Approve
                        </button>
                      </div>
                    )}
                  </div>
                ))}
                {!(content?.imports || []).length && (
                  <div className="moon-dock__empty">No imports staged.</div>
                )}
              </div>

              <div className="moon-dock-section-card">
                <div className="moon-dock__section-label">
                  Bindings ({bindings.length})
                </div>
                {bindings.map(binding => (
                  <BindingCard key={binding.binding_id} binding={binding} onMutate={onMutate} onCommitAuthorityAction={onCommitAuthorityAction} />
                ))}
                {!bindings.length && (
                  <div className="moon-dock__empty">No bindings.</div>
                )}
              </div>

              <div className="moon-dock-section-card">
                <div className="moon-dock__section-label">
                  DB Objects ({objectTypes.length})
                </div>
                {objectTypes.length > 6 && (
                  <input
                    className="moon-dock-form__input"
                    type="text"
                    value={objectSearch}
                    onChange={e => setObjectSearch(e.target.value)}
                    placeholder="Filter objects..."
                  />
                )}
                {objectTypesLoading ? (
                  <div className="moon-dock__empty"><span className="moon-spinner" /> Loading...</div>
                ) : objectTypes.length === 0 ? (
                  <div className="moon-dock__empty">No object types registered.</div>
                ) : (
                  <div className="moon-dock__catalog-grid">
                    {objectTypes
                      .filter(ot => !objectSearch || ot.name.toLowerCase().includes(objectSearch.toLowerCase()))
                      .slice(0, 20)
                      .map(ot => (
                        <div
                          key={ot.type_id}
                          className="moon-dock__catalog-item"
                          draggable
                          onDragStart={e => {
                            e.dataTransfer.setData('moon/object-type-id', ot.type_id);
                            e.dataTransfer.setData('moon/object-type-label', ot.name);
                            e.dataTransfer.setData('text/plain', ot.name);
                            e.dataTransfer.effectAllowed = 'copyLink';
                          }}
                          onClick={() => {
                            if (!node) return;
                            const request = {
                              node_id: node.id,
                              authority_kind: 'object_type',
                              authority_ref: ot.type_id,
                              role: 'input',
                              label: ot.name,
                              promote_to_state: false,
                            };
                            const promise = onCommitAuthorityAction
                              ? onCommitAuthorityAction('attachments', request, {
                                  label: 'Attach object type',
                                  reason: `Attach ${ot.name} to ${node.title || node.id}.`,
                                  outcome: `${ot.name} is now available to ${node.title || node.id}.`,
                                  authority: 'build.authority_attachments',
                                  target: nodeTarget(node),
                                  changeSummary: ['Object type', ot.name],
                                })
                              : onMutate('attachments', request);
                            promise.catch(() => {});
                          }}
                          title={ot.description || `Attach ${ot.name} to this step`}
                        >
                          <span style={{ fontSize: 14 }}>{ot.icon || '◆'}</span>
                          <span>{ot.name}</span>
                        </div>
                      ))}
                  </div>
                )}
              </div>

              <div className="moon-dock-section-card">
                <div className="moon-dock__section-label">Attach reference</div>

                <div className="moon-dock-form__row">
                  <select className="moon-dock-form__select" value={attachKind} onChange={e => setAttachKind(e.target.value)}>
                    <option value="reference">Reference</option>
                    <option value="object_type">Object Type</option>
                    <option value="object">Object</option>
                  </select>
                  <select className="moon-dock-form__select" value={attachRole} onChange={e => setAttachRole(e.target.value)}>
                    <option value="input">Input</option>
                    <option value="evidence">Evidence</option>
                    <option value="state_dependency">State dep</option>
                    <option value="output">Output</option>
                  </select>
                </div>

                <input className="moon-dock-form__input" type="text" value={attachRef} onChange={e => setAttachRef(e.target.value)} placeholder="Authority ref (slug or ID)" />
                <input className="moon-dock-form__input" type="text" value={attachLabel} onChange={e => setAttachLabel(e.target.value)} placeholder="Label (optional)" />

                <label className="moon-dock-form__checkbox">
                  <input type="checkbox" checked={attachPromote} onChange={e => setAttachPromote(e.target.checked)} />
                  Promote to state node
                </label>

                <div className="moon-dock-form__actions">
                  <button className="moon-dock-form__btn" onClick={handleAttach} disabled={attachLoading || !attachRef.trim()}>
                    {attachLoading ? <><span className="moon-spinner" /> Attaching...</> : 'Attach'}
                  </button>
                </div>
                {attachError && <div className="moon-dock-form__error">{attachError}</div>}
              </div>

              <div className="moon-dock-section-card">
                <div className="moon-dock__section-label">Stage import</div>

                <input className="moon-dock-form__input" type="text" value={importLocator} onChange={e => setImportLocator(e.target.value)} placeholder="URL or source locator" />
                <input className="moon-dock-form__input" type="text" value={importLabel} onChange={e => setImportLabel(e.target.value)} placeholder="Label (optional)" />

                <div className="moon-dock-form__actions">
                  <button className="moon-dock-form__btn" onClick={handleStageImport} disabled={importLoading || !importLocator.trim()}>
                    {importLoading ? <><span className="moon-spinner" /> Staging...</> : 'Stage'}
                  </button>
                  <button className="moon-dock-form__btn" onClick={handleMaterialize} disabled={importLoading || !importLocator.trim()}>
                    Materialize here
                  </button>
                </div>
                {importError && <div className="moon-dock-form__error">{importError}</div>}
              </div>
            </>
          )}
        </>
      ) : null}
    </>
  );
}

function BindingCard({
  binding,
  onMutate,
  onCommitAuthorityAction,
}: {
  binding: BindingLedgerEntry;
  onMutate: Props['onMutate'];
  onCommitAuthorityAction?: Props['onCommitAuthorityAction'];
}) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showReplace, setShowReplace] = useState(false);
  const [replaceRef, setReplaceRef] = useState('');

  const isAccepted = binding.state === 'accepted';
  const isRejected = binding.state === 'rejected';

  const handleAccept = useCallback(async (target: BindingTarget) => {
    setLoading(true);
    setError(null);
    try {
      const request = {
        target_kind: 'binding',
        target_ref: binding.binding_id,
        decision: 'approve',
        candidate_payload: target,
        rationale: 'Accepted from the workflow builder.',
      };
      if (onCommitAuthorityAction) {
        const targetLabel = target.enrichment?.integration_name || target.label || target.target_ref || 'selected target';
        await onCommitAuthorityAction('review_decisions', request, {
          label: 'Approve binding',
          reason: `Connect ${binding.source_label || binding.binding_id} to ${targetLabel}.`,
          outcome: `${binding.source_label || binding.binding_id} now resolves through ${targetLabel}.`,
          authority: 'build.binding_ledger',
          target: {
            kind: 'binding',
            label: binding.source_label || binding.binding_id,
            id: binding.binding_id,
          },
          changeSummary: ['Accepted target', targetLabel],
        });
      } else {
        await onMutate('review_decisions', request);
      }
    } catch (e: any) {
      setError(e.message || 'Failed to accept');
    } finally {
      setLoading(false);
    }
  }, [binding.binding_id, binding.source_label, onCommitAuthorityAction, onMutate]);

  const handleReplace = useCallback(async () => {
    if (!replaceRef.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const request = {
        target_kind: 'binding',
        target_ref: binding.binding_id,
        decision: 'approve',
        candidate_payload: {
          target_ref: replaceRef.trim(),
          label: replaceRef.trim(),
          kind: 'custom',
        },
        rationale: 'Replaced from the workflow builder.',
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction('review_decisions', request, {
          label: 'Approve replacement binding',
          reason: `Replace ${binding.source_label || binding.binding_id} with ${replaceRef.trim()}.`,
          outcome: `${binding.source_label || binding.binding_id} now resolves through ${replaceRef.trim()}.`,
          authority: 'build.binding_ledger',
          target: {
            kind: 'binding',
            label: binding.source_label || binding.binding_id,
            id: binding.binding_id,
          },
          changeSummary: ['Replacement target', replaceRef.trim()],
        });
      } else {
        await onMutate('review_decisions', request);
      }
      setShowReplace(false);
      setReplaceRef('');
    } catch (e: any) {
      setError(e.message || 'Failed to replace');
    } finally {
      setLoading(false);
    }
  }, [binding.binding_id, binding.source_label, onCommitAuthorityAction, replaceRef, onMutate]);

  const handleReject = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const request = {
        target_kind: 'binding',
        target_ref: binding.binding_id,
        decision: 'reject',
        rationale: 'Rejected from the workflow builder.',
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction('review_decisions', request, {
          label: 'Reject binding candidate',
          reason: `Mark ${binding.source_label || binding.binding_id} as intentionally skipped.`,
          outcome: `${binding.source_label || binding.binding_id} is now marked as rejected.`,
          authority: 'build.binding_ledger',
          target: {
            kind: 'binding',
            label: binding.source_label || binding.binding_id,
            id: binding.binding_id,
          },
          changeSummary: ['Binding state', 'Rejected'],
        });
      } else {
        await onMutate('review_decisions', request);
      }
    } catch (e: any) {
      setError(e.message || 'Failed to reject');
    } finally {
      setLoading(false);
    }
  }, [binding.binding_id, binding.source_label, onCommitAuthorityAction, onMutate]);

  const stateColor = isAccepted ? 'var(--moon-status-ok)' : isRejected ? 'var(--moon-error)' : 'var(--moon-fg-muted)';

  return (
    <div className={`moon-dock__item${isRejected ? ' moon-dock__item--muted' : ''}`}>
      <div className="moon-dock__item-title">
        {binding.source_label || binding.binding_id}
        <span style={{ marginLeft: 8, fontSize: 11, color: stateColor }}>
          {isAccepted && binding.accepted_target?.enrichment
            ? `Connected to ${binding.accepted_target.enrichment.integration_name}`
            : isRejected ? 'Skipped'
            : binding.state || 'Pick a connector'}
        </span>
      </div>

      {isAccepted && binding.accepted_target && (
        <div className="moon-dock__item-desc">
          {binding.accepted_target.enrichment ? (
            <>
              {binding.accepted_target.enrichment.integration_name}
              {binding.accepted_target.enrichment.auth_status && (
                <span style={{ marginLeft: 6, fontSize: 10, color: binding.accepted_target.enrichment.auth_status === 'connected' ? 'var(--moon-status-ok)' : 'var(--moon-fg-muted)' }}>
                  ({binding.accepted_target.enrichment.auth_status})
                </span>
              )}
              {binding.accepted_target.enrichment.description && (
                <div style={{ fontSize: 11, color: 'var(--moon-fg-muted)', marginTop: 2 }}>
                  {binding.accepted_target.enrichment.description}
                </div>
              )}
            </>
          ) : (
            <>Bound to: {binding.accepted_target.label || binding.accepted_target.target_ref}</>
          )}
        </div>
      )}

      {/* Candidate targets */}
      {!isAccepted && !isRejected && binding.candidate_targets?.length ? (
        <div className="moon-dock-subsection" style={{ marginTop: 8 }}>
          <div className="moon-dock__section-label">Pick a target:</div>
          {binding.candidate_targets.map((target, i) => (
            <button
              key={i}
              className="moon-dock__item"
              onClick={() => handleAccept(target)}
              disabled={loading}
              draggable={!loading}
              onDragStart={e => {
                e.dataTransfer.setData('moon/dock', 'context');
                e.dataTransfer.setData('text/plain', target.label || target.target_ref || '');
                e.dataTransfer.effectAllowed = 'link';
              }}
              style={{ display: 'block', width: '100%', textAlign: 'left', padding: '10px 12px', marginBottom: 6 }}
            >
              {target.enrichment?.integration_name || target.label || target.target_ref || 'Target'}
              {target.enrichment?.auth_status ? (
                <span className="moon-dock__item-desc" style={{ marginLeft: 6, color: target.enrichment.auth_status === 'connected' ? 'var(--moon-status-ok)' : 'var(--moon-fg-muted)' }}>
                  ({target.enrichment.auth_status})
                </span>
              ) : target.kind ? (
                <span className="moon-dock__item-desc" style={{ marginLeft: 6 }}>({target.kind})</span>
              ) : null}
            </button>
          ))}
        </div>
      ) : null}

      {/* Replace / Reject actions */}
      {!isAccepted && !isRejected && (
        <div className="moon-dock-form__actions">
          {!showReplace ? (
            <button className="moon-dock-form__btn--small" onClick={() => setShowReplace(true)}>
              Custom target
            </button>
          ) : (
            <>
              <input className="moon-dock-form__input" style={{ marginBottom: 0, flex: 1 }} type="text" value={replaceRef} onChange={e => setReplaceRef(e.target.value)} placeholder="Target ref" />
              <button className="moon-dock-form__btn" onClick={handleReplace} disabled={loading || !replaceRef.trim()}>
                {loading ? '...' : 'Use'}
              </button>
              <button className="moon-dock-form__btn--small" onClick={() => setShowReplace(false)}>Cancel</button>
            </>
          )}
          <button className="moon-dock-form__btn--small" style={{ color: 'var(--moon-error)', borderColor: 'var(--moon-error)' }} onClick={handleReject} disabled={loading}>
            Reject
          </button>
        </div>
      )}

      {error && <div className="moon-dock-form__error">{error}</div>}
    </div>
  );
}
