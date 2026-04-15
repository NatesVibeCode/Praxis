import React, { useState, useCallback, useEffect, useMemo } from 'react';
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

export interface AuthorityActionMeta {
  label: string;
  reason: string;
  outcome: string;
  authority?: string;
  target?: UiActionTarget | null;
  changeSummary?: string[];
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
}

const TRIGGER_MANUAL_ROUTE = 'trigger';
const TRIGGER_SCHEDULE_ROUTE = 'trigger/schedule';
const TRIGGER_WEBHOOK_ROUTE = 'trigger/webhook';
const WEBHOOK_TRIGGER_EVENT = 'db.webhook_events.insert';
type BranchConditionMode = 'simple' | 'json';
type BranchComposerOp = typeof BRANCH_OP_OPTIONS[number]['value'];

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

function formatTriggerFilter(filter: unknown): string {
  return JSON.stringify(normalizeTriggerFilter(filter), null, 2);
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

function formatStringList(value: unknown): string {
  if (!Array.isArray(value)) return '';
  return value
    .map(item => typeof item === 'string' ? item.trim() : '')
    .filter(Boolean)
    .join('\n');
}

function parseStringList(text: string): string[] {
  return text
    .split('\n')
    .map(item => item.trim())
    .filter(Boolean);
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

export function MoonNodeDetail({ node, content, workflowId, onMutate, onCommitAuthorityAction, onClose, selectedEdge, edgeFromLabel, edgeToLabel, onApplyGate, gateItems = [], buildGraph, onUpdateBuildGraph, onCommitGraphAction }: Props) {
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
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [edgeConditionText, setEdgeConditionText] = useState('{}');
  const [edgeConditionMode, setEdgeConditionMode] = useState<BranchConditionMode>('simple');
  const [edgeConditionField, setEdgeConditionField] = useState('should_continue');
  const [edgeConditionOp, setEdgeConditionOp] = useState<BranchComposerOp>('equals');
  const [edgeConditionValueText, setEdgeConditionValueText] = useState('true');
  const [edgeConditionLoading, setEdgeConditionLoading] = useState(false);
  const [edgeConditionError, setEdgeConditionError] = useState<string | null>(null);
  const [failureGateLoading, setFailureGateLoading] = useState(false);
  const [failureGateError, setFailureGateError] = useState<string | null>(null);
  const [nodeTitle, setNodeTitle] = useState('');
  const [nodeSummary, setNodeSummary] = useState('');
  const [nodePrompt, setNodePrompt] = useState('');
  const [nodeRequiredInputsText, setNodeRequiredInputsText] = useState('');
  const [nodeOutputsText, setNodeOutputsText] = useState('');
  const [nodePersistenceTargetsText, setNodePersistenceTargetsText] = useState('');
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
  const triggerFilterJson = formatTriggerFilter(triggerConfig?.filter);
  const isTriggerNode = Boolean(node && isTriggerRoute(triggerRoute));
  const isConditionalEdge = Boolean(selectedEdge && buildEdgeRelease?.family === 'conditional');
  const isFailureEdge = Boolean(selectedEdge && buildEdgeRelease?.family === 'after_failure');
  const conditionalBranchLabel = branchLabel(buildEdgeRelease?.branch_reason || selectedEdge?.branchReason) || 'Branch';
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
  const primaryGateItems = gateCatalogModels.filter(({ policy }) => policy.tier === 'primary');
  const advancedGateItems = gateCatalogModels.filter(({ policy }) => policy.tier === 'advanced');
  const hiddenGateItems = gateCatalogModels.filter(({ policy }) => policy.tier === 'hidden' && policy.hardChoice);
  const selectedGateCatalogModel = gateCatalogModels.find(
    ({ item }) => item.gateFamily === selectedEdge?.gateFamily,
  ) || null;
  const currentAdvancedGate = gateCatalogModels.find(
    ({ item, policy }) => policy.tier === 'advanced' && selectedEdge?.gateFamily === item.gateFamily,
  );
  const currentHiddenGate = gateCatalogModels.find(
    ({ item, policy }) => policy.tier === 'hidden' && selectedEdge?.gateFamily === item.gateFamily,
  );
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
    if (!isTriggerNode) return;
    setTriggerCronExpression(
      (typeof triggerConfig?.cron_expression === 'string' && triggerConfig.cron_expression.trim()) || '@daily',
    );
    setTriggerSourceRef(typeof triggerConfig?.source_ref === 'string' ? triggerConfig.source_ref : '');
    setTriggerFilterText(triggerFilterJson);
    setTriggerError(null);
  }, [
    isTriggerNode,
    node?.id,
    triggerConfig?.cron_expression,
    triggerConfig?.source_ref,
    triggerFilterJson,
  ]);

  useEffect(() => {
    if (!isConditionalEdge) return;
    const condition = baseConditionFromRelease(buildEdgeRelease || {
      family: 'conditional',
      edge_type: 'conditional',
      release_condition: {},
    } as any);
    const simpleCondition = parseSimpleBranchCondition(condition);
    setEdgeConditionText(formatJsonObject(condition));
    if (simpleCondition) {
      setEdgeConditionMode('simple');
      setEdgeConditionField(simpleCondition.field);
      setEdgeConditionOp(simpleCondition.op);
      setEdgeConditionValueText(simpleCondition.valueText);
    } else {
      setEdgeConditionMode('json');
      setEdgeConditionField('should_continue');
      setEdgeConditionOp('equals');
      setEdgeConditionValueText('true');
    }
    setEdgeConditionError(null);
  }, [
    isConditionalEdge,
    buildEdge?.edge_id,
    buildEdgeRelease?.release_condition,
    buildEdgeRelease?.branch_reason,
  ]);

  useEffect(() => {
    if (!isFailureEdge) return;
    setFailureGateError(null);
  }, [isFailureEdge, buildEdge?.edge_id]);

  useEffect(() => {
    if (!buildNode) return;
    setNodeTitle(buildNode.title || node?.title || '');
    setNodeSummary(buildNode.summary || node?.summary || '');
    setNodePrompt(buildNode.prompt || '');
    setNodeRequiredInputsText(formatStringList(buildNode.required_inputs));
    setNodeOutputsText(formatStringList(buildNode.outputs));
    setNodePersistenceTargetsText(formatStringList(buildNode.persistence_targets));
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
        admitted_target: {
          target_ref: shape.target_ref || `#${snapshotId}`,
          label: shape.label || snapshotId,
          kind: shape.kind || 'type',
        },
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction(`imports/${snapshotId}/admit`, request, {
          label: 'Admit import',
          reason: `Admit ${String(shape.label || snapshotId)} into the build authority set.`,
          outcome: `${String(shape.label || snapshotId)} is now admitted for downstream binding.`,
          authority: 'build.import_snapshots',
          target: {
            kind: 'import',
            label: String(shape.label || snapshotId),
            id: snapshotId,
          },
          changeSummary: ['Admitted target', String(shape.target_ref || shape.label || snapshotId)],
        });
      } else {
        await onMutate(`imports/${snapshotId}/admit`, request);
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
      const filter = parseTriggerFilter(triggerFilterText);
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
    triggerFilterText,
  ]);

  const handleHttpRequestPresetChange = useCallback((nextPreset: HttpRequestPreset) => {
    const presetDefinition = httpRequestPresetDefinition(nextPreset);
    setHttpRequestPreset(nextPreset);
    setWebhookMethod(presetDefinition.method);
    setWebhookHeadersText(formatJsonObject(presetDefinition.headers));
    setWebhookBodyText(formatJsonValue(presetDefinition.body));
    setNodeSaveError(null);
    if (!nodeOutputsText.trim()) {
      setNodeOutputsText('http_response');
    }
  }, [nodeOutputsText]);

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
        required_inputs: parseStringList(nodeRequiredInputsText),
        outputs: parseStringList(nodeOutputsText),
        persistence_targets: parseStringList(nodePersistenceTargetsText),
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
    nodeOutputsText,
    nodePersistenceTargetsText,
    nodePrompt,
    nodeRequiredInputsText,
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
  ]);

  const handleClearFailureGate = useCallback(async () => {
    if (!selectedEdge || !buildGraph || !canCommitGraph || !buildEdge || !isFailureEdge) return;
    setFailureGateLoading(true);
    setFailureGateError(null);
    try {
      const edges = [...(buildGraph.edges || [])];
      const index = edges.findIndex((edge) => edge.edge_id === buildEdge.edge_id);
      if (index < 0) return;

      const currentEdge = edges[index];
      const nextEdge = withBuildEdgeRelease(currentEdge, null);
      edges[index] = nextEdge;

      const nextGraph = { ...buildGraph, edges };
      if (onCommitGraphAction) {
        await onCommitGraphAction(nextGraph, {
          label: 'Clear failure gate',
          reason: `Convert the connection from ${failureSourceLabel} to ${failureTargetLabel} back to a normal success path.`,
          outcome: `${failureTargetLabel} now runs on the standard success path instead of only after failure.`,
          target: edgeTarget(selectedEdge, edgeFromLabel, edgeToLabel),
          changeSummary: ['Gate family', 'After success'],
        });
      } else if (onUpdateBuildGraph) {
        await onUpdateBuildGraph(nextGraph);
      }
    } catch (error: any) {
      setFailureGateError(error.message || 'Failed to clear the failure gate.');
    } finally {
      setFailureGateLoading(false);
    }
  }, [
    buildEdge,
    buildGraph,
    canCommitGraph,
    edgeFromLabel,
    edgeToLabel,
    failureSourceLabel,
    failureTargetLabel,
    isFailureEdge,
    onCommitGraphAction,
    onUpdateBuildGraph,
    selectedEdge,
  ]);

	  // Sort: unresolved first, then accepted, then rejected
  const bindings = [...(content?.connectBindings || [])].sort((a, b) => {
    const order: Record<string, number> = { unresolved: 0, accepted: 1, rejected: 2 };
    return (order[a.state || 'unresolved'] || 0) - (order[b.state || 'unresolved'] || 0);
  });

  return (
    <>
      <button className="moon-dock__close" onClick={onClose} aria-label="Close detail panel">&times;</button>

      {/* Gate config — when an edge is selected */}
      {selectedEdge && onApplyGate ? (
        <>
          <div className="moon-dock__title">Gate</div>
          <div className="moon-dock__subtitle">{edgeFromLabel} &rarr; {edgeToLabel}</div>
          <div className="moon-dock__sep" />
          <div className="moon-gate-panel">
            <div className="moon-gate-panel__meta">
              <span className="moon-surface-badge">
                {selectedGateCatalogModel?.policy.badge || (selectedEdge.gateState === 'empty' ? 'Core now' : 'Gate')}
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
            <div className="moon-gate-panel__summary">
              {selectedEdge.gateState === 'empty'
                ? 'Moon only promotes Branch and On Failure here because they are the only gate controls that change execution today.'
                : selectedGateCatalogModel?.policy.detail || selectedGateCatalogModel?.truth.detail || 'This edge carries saved gate metadata.'}
            </div>
          </div>

          {primaryGateItems.length > 0 && (
            <>
              <div className="moon-dock__section-label">Control now</div>
              <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                These are the only edge controls that change runtime flow today.
              </div>
              <div className="moon-dock__catalog-grid">
                {primaryGateItems.map(({ item, truth, policy }) => (
                  <button
                    key={item.id}
                    className={`moon-dock__catalog-item moon-dock__catalog-item--${truth.category}${selectedEdge.gateFamily === item.gateFamily ? ' moon-dock__catalog-item--active' : ''}`}
                    onClick={() => item.gateFamily && onApplyGate?.(selectedEdge.id, item.gateFamily)}
                    draggable
                    title={`${item.description || item.label} — ${policy.detail}`}
                    onDragStart={e => {
                      e.dataTransfer.setData('moon/catalog-id', item.id);
                      e.dataTransfer.setData('text/plain', item.label);
                      e.dataTransfer.effectAllowed = 'copyLink';
                    }}
                  >
                    <MoonGlyph type={item.icon} size={14} />
                    <span className="moon-catalog-item__stack">
                      <span className="moon-catalog-item__label">{item.label}</span>
                      <span className="moon-catalog-item__detail">{policy.detail}</span>
                    </span>
                    <span className="moon-catalog-item__meta-row">
                      <span className="moon-surface-badge">{policy.badge}</span>
                      <span className={`moon-truth-badge moon-truth-badge--${truth.category}`}>{truth.badge}</span>
                    </span>
                  </button>
                ))}
              </div>
            </>
          )}

          {advancedGateItems.length > 0 && (
            <>
              <div className="moon-dock__section-label">Worth building later</div>
              <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                These gates stay in Detail because they are still saved-only or incomplete at runtime, but they are still worth shaping deliberately.
              </div>
              <div className="moon-dock__catalog-grid">
                {advancedGateItems.map(({ item, truth, policy }) => (
                  <button
                    key={item.id}
                    type="button"
                    className={`moon-dock__catalog-item moon-dock__catalog-item--preview moon-dock__catalog-item--${truth.category}${selectedEdge.gateFamily === item.gateFamily ? ' moon-dock__catalog-item--active' : ''}`}
                    title={`${item.description || item.label} — ${policy.detail}`}
                    disabled
                  >
                    <MoonGlyph type={item.icon} size={14} />
                    <span className="moon-catalog-item__stack">
                      <span className="moon-catalog-item__label">{item.label}</span>
                      <span className="moon-catalog-item__detail">{policy.detail}</span>
                    </span>
                    <span className="moon-catalog-item__meta-row">
                      <span className="moon-surface-badge">{policy.badge}</span>
                      <span className={`moon-truth-badge moon-truth-badge--${truth.category}`}>{truth.badge}</span>
                    </span>
                  </button>
                ))}
              </div>
            </>
          )}

          {currentAdvancedGate && (
            <div className="moon-action__legacy-note">
              Current gate <code>{currentAdvancedGate.item.label}</code> stays on this edge as saved data, but Moon treats it as preview-only until it changes runtime for real.
            </div>
          )}

          {hiddenGateItems.length > 0 && (
            <>
              <div className="moon-dock__section-label">Hard choices</div>
              <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                These are intentionally kept out of the main gate surface so Moon does not promise control it cannot prove.
              </div>
              <div className="moon-action__hard-list" style={{ marginBottom: 12 }}>
                {hiddenGateItems.map(({ item, policy }) => (
                  <div key={item.id} className="moon-action__hard-item">
                    <div className="moon-action__hard-label">{item.label}</div>
                    <div className="moon-action__hard-detail">{policy.hardChoice || policy.detail}</div>
                  </div>
                ))}
              </div>
            </>
          )}

          {currentHiddenGate && (
            <div className="moon-action__legacy-note">
              Current gate <code>{currentHiddenGate.item.label}</code> stays in saved data, but it is no longer offered as a main Moon control.
            </div>
          )}

          {isFailureEdge && (
            <>
              <div className="moon-dock__section-label">Failure path</div>
              <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                Moon treats this as a pure status gate. There is no extra fallback protocol hiding underneath it today.
              </div>
              <div className="moon-failure-path">
                <div className="moon-failure-path__lane">
                  <div className="moon-failure-path__lane-label">Success</div>
                  <div className="moon-failure-path__lane-title">{failureSourceLabel} succeeds</div>
                  <div className="moon-failure-path__lane-detail">This edge stays closed and {failureTargetLabel} does not run from this path.</div>
                </div>
                <div className="moon-failure-path__lane moon-failure-path__lane--active">
                  <div className="moon-failure-path__lane-label">Failure</div>
                  <div className="moon-failure-path__lane-title">{failureSourceLabel} fails</div>
                  <div className="moon-failure-path__lane-detail">{failureTargetLabel} releases as the remediation path.</div>
                </div>
              </div>
              <div className="moon-branch-editor__hint" style={{ marginBottom: 8 }}>
                Hard choice: Moon only exposes the failure-status semantics here because runtime does not yet honor richer fallback settings on this gate.
              </div>
              <div className="moon-gate-panel__actions">
                <button
                  className="moon-dock-form__btn"
                  onClick={handleClearFailureGate}
                  disabled={failureGateLoading || !buildGraph || !canCommitGraph}
                >
                  {failureGateLoading ? <><span className="moon-spinner" /> Updating...</> : 'Make normal path'}
                </button>
              </div>
              {failureGateError && <div className="moon-dock-form__error">{failureGateError}</div>}
            </>
          )}

          {isConditionalEdge && (
            <>
              <div className="moon-dock__section-label">Condition</div>
              <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                {conditionalBranchLabel} branch from this source.
                {buildEdgeRelease?.branch_reason === 'else' ? ' The else branch automatically inverts the same condition.' : ' The else branch, if present, will invert the same condition.'}
              </div>
              <div className="moon-branch-editor">
                <div className="moon-branch-editor__lanes">
                  <div className={`moon-branch-editor__lane${buildEdgeRelease?.branch_reason === 'then' ? ' moon-branch-editor__lane--active' : ''}`}>
                    <div className="moon-branch-editor__lane-label">Then</div>
                    <div className="moon-branch-editor__lane-target">{conditionalTargets.thenTarget || edgeToLabel || 'Then path'}</div>
                  </div>
                  <div className="moon-branch-editor__lane-separator" aria-hidden="true">Else</div>
                  <div className={`moon-branch-editor__lane${buildEdgeRelease?.branch_reason === 'else' ? ' moon-branch-editor__lane--active' : ''}`}>
                    <div className="moon-branch-editor__lane-label">Else</div>
                    <div className="moon-branch-editor__lane-target">{conditionalTargets.elseTarget || 'Inverse path'}</div>
                  </div>
                </div>

                <div className="moon-branch-editor__modes">
                  <button
                    type="button"
                    className={`moon-branch-editor__mode${edgeConditionMode === 'simple' ? ' moon-branch-editor__mode--active' : ''}`}
                    onClick={() => handleEdgeConditionModeChange('simple')}
                  >
                    Composer
                  </button>
                  <button
                    type="button"
                    className={`moon-branch-editor__mode${edgeConditionMode === 'json' ? ' moon-branch-editor__mode--active' : ''}`}
                    onClick={() => handleEdgeConditionModeChange('json')}
                  >
                    JSON
                  </button>
                </div>

                {edgeConditionMode === 'simple' ? (
                  <>
                    <input
                      className="moon-dock-form__input"
                      type="text"
                      value={edgeConditionField}
                      onChange={e => setEdgeConditionField(e.target.value)}
                      placeholder="Output field or path, for example should_continue or result.score"
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
                        placeholder={edgeConditionOp === 'in' || edgeConditionOp === 'not_in'
                          ? 'Comparison value. Plain text or JSON, for example [\"gold\", \"platinum\"]'
                          : 'Comparison value. Plain text or JSON, for example true, 10, or \"approved\"'}
                        rows={3}
                        style={{ minHeight: 86, resize: 'vertical' }}
                      />
                    ) : (
                      <div className="moon-branch-editor__hint">
                        This operator does not need a comparison value. Moon only checks whether the field exists.
                      </div>
                    )}
                    <div className="moon-branch-editor__hint">
                      Then runs when <code>{edgeConditionField.trim() || 'field'}</code> {BRANCH_OP_LABELS.get(edgeConditionOp)?.toLowerCase() || 'matches'}{selectedBranchOp.expectsValue && edgeConditionValueText.trim() ? ` ${edgeConditionValueText.trim()}` : ''}. Else automatically uses the inverse.
                    </div>
                  </>
                ) : (
                  <>
                    <textarea
                      className="moon-dock-form__input"
                      value={edgeConditionText}
                      onChange={e => setEdgeConditionText(e.target.value)}
                      placeholder="Branch condition JSON"
                      rows={8}
                      style={{ minHeight: 132, resize: 'vertical' }}
                    />
                    <div className="moon-branch-editor__hint">
                      Use JSON mode for nested `and` / `or` / `not` trees or any condition shape that does not fit the single-predicate composer.
                    </div>
                  </>
                )}
              </div>
              <button
                className="moon-dock-form__btn"
                onClick={handleSaveConditionalEdge}
                disabled={edgeConditionLoading || !buildGraph || !canCommitGraph}
              >
                {edgeConditionLoading ? <><span className="moon-spinner" /> Saving...</> : 'Save branch condition'}
              </button>
              {edgeConditionError && <div className="moon-dock-form__error">{edgeConditionError}</div>}
            </>
          )}
        </>
      ) : (
        <>
          <div className="moon-dock__title">{node ? node.title : 'Detail'}</div>
        </>
      )}

      {!node && !selectedEdge ? (
        <div className="moon-dock__empty">Select a node or gate.</div>
      ) : node ? (
        <>
          {isTriggerNode && (
            <>
              <div className="moon-dock__sep" />
              <div className="moon-dock__section-label">Trigger config</div>
              <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                {triggerRoute === TRIGGER_SCHEDULE_ROUTE
                  ? 'Schedule trigger'
                  : triggerRoute === TRIGGER_WEBHOOK_ROUTE
                    ? 'Webhook trigger'
                    : 'Manual trigger'}
              </div>
              {triggerRoute === TRIGGER_SCHEDULE_ROUTE ? (
                <input
                  className="moon-dock-form__input"
                  type="text"
                  value={triggerCronExpression}
                  onChange={e => setTriggerCronExpression(e.target.value)}
                  placeholder="Cron expression"
                />
              ) : (
                <input
                  className="moon-dock-form__input"
                  type="text"
                  value={triggerSourceRef}
                  onChange={e => setTriggerSourceRef(e.target.value)}
                  placeholder="Source ref (optional)"
                />
              )}
              <textarea
                className="moon-dock-form__input"
                value={triggerFilterText}
                onChange={e => setTriggerFilterText(e.target.value)}
                placeholder="Trigger filter JSON"
                rows={6}
                style={{ minHeight: 110, resize: 'vertical' }}
              />
              <button
                className="moon-dock-form__btn"
                onClick={handleSaveTrigger}
                disabled={triggerLoading || !buildGraph || !canCommitGraph}
              >
                {triggerLoading ? <><span className="moon-spinner" /> Saving...</> : 'Save trigger'}
              </button>
              {triggerError && <div className="moon-dock-form__error">{triggerError}</div>}
            </>
          )}

          {showPrimitiveEditor && (
            <>
              <div className="moon-dock__sep" />
              <div className="moon-dock__section-label">Block properties</div>
              <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                Edit the primitive this node owns. Labels, execution contract, and route config all save through the same graph authority.
              </div>
              <div className="moon-dock__item-desc" style={{ marginBottom: 8 }}>
                Route: {triggerRoute || 'Unassigned'}
              </div>
              <input
                className="moon-dock-form__input"
                type="text"
                value={nodeTitle}
                onChange={e => setNodeTitle(e.target.value)}
                placeholder="Step title"
              />
              <textarea
                className="moon-dock-form__input"
                value={nodeSummary}
                onChange={e => setNodeSummary(e.target.value)}
                placeholder="Summary"
                rows={3}
                style={{ minHeight: 88, resize: 'vertical' }}
              />

              {!isTriggerNode && (
                <>
                  <textarea
                    className="moon-dock-form__input"
                    value={nodePrompt}
                    onChange={e => setNodePrompt(e.target.value)}
                    placeholder="Prompt / system instructions"
                    rows={6}
                    style={{ minHeight: 132, resize: 'vertical' }}
                  />
                  <textarea
                    className="moon-dock-form__input"
                    value={nodeRequiredInputsText}
                    onChange={e => setNodeRequiredInputsText(e.target.value)}
                    placeholder={'Required inputs, one per line\ncustomer_id\nthread_context'}
                    rows={4}
                    style={{ minHeight: 96, resize: 'vertical' }}
                  />
                  <textarea
                    className="moon-dock-form__input"
                    value={nodeOutputsText}
                    onChange={e => setNodeOutputsText(e.target.value)}
                    placeholder={'Outputs, one per line\nsummary\nnext_action'}
                    rows={4}
                    style={{ minHeight: 96, resize: 'vertical' }}
                  />
                  <textarea
                    className="moon-dock-form__input"
                    value={nodePersistenceTargetsText}
                    onChange={e => setNodePersistenceTargetsText(e.target.value)}
                    placeholder={'Persistence targets, one per line\ncrm.reply_drafts'}
                    rows={3}
                    style={{ minHeight: 88, resize: 'vertical' }}
                  />
                  <input
                    className="moon-dock-form__input"
                    type="text"
                    value={nodeHandoffTarget}
                    onChange={e => setNodeHandoffTarget(e.target.value)}
                    placeholder="Handoff target (optional)"
                  />
                </>
              )}

              {isNotificationRoute && (
                <>
                  <div className="moon-dock__section-label" style={{ marginTop: 16 }}>Notification properties</div>
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
                </>
              )}

              {isWebhookRoute && (
                <>
                  <div className="moon-dock__section-label" style={{ marginTop: 16 }}>HTTP request properties</div>
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
                  <textarea
                    className="moon-dock-form__input"
                    value={webhookHeadersText}
                    onChange={e => setWebhookHeadersText(e.target.value)}
                    placeholder={'Headers JSON\n{\n  "Authorization": "Bearer ..."\n}'}
                    rows={5}
                    style={{ minHeight: 118, resize: 'vertical' }}
                  />
                  {showHttpRequestBody && (
                    <textarea
                      className="moon-dock-form__input"
                      value={webhookBodyText}
                      onChange={e => setWebhookBodyText(e.target.value)}
                      placeholder={selectedHttpRequestPreset.bodyPlaceholder}
                      rows={5}
                      style={{ minHeight: 118, resize: 'vertical' }}
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
                </>
              )}

              {isWorkflowInvokeRoute && (
                <>
                  <div className="moon-dock__section-label" style={{ marginTop: 16 }}>Workflow invoke properties</div>
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
                  <textarea
                    className="moon-dock-form__input"
                    value={invokePayloadText}
                    onChange={e => setInvokePayloadText(e.target.value)}
                    placeholder={'Payload JSON or text\n{\n  "ticket_id": "{{ticket_id}}"\n}'}
                    rows={5}
                    style={{ minHeight: 118, resize: 'vertical' }}
                  />
                </>
              )}

              {isIntegrationRoute && !isNotificationRoute && !isWebhookRoute && !isWorkflowInvokeRoute && (
                <>
                  <div className="moon-dock__section-label" style={{ marginTop: 16 }}>Integration properties</div>
                  <textarea
                    className="moon-dock-form__input"
                    value={genericIntegrationArgsText}
                    onChange={e => setGenericIntegrationArgsText(e.target.value)}
                    placeholder={'Integration args JSON\n{\n  "mode": "default"\n}'}
                    rows={6}
                    style={{ minHeight: 132, resize: 'vertical' }}
                  />
                </>
              )}

              <button
                className="moon-dock-form__btn"
                onClick={handleSaveNodePrimitive}
                disabled={nodeSaveLoading || !buildGraph || !canCommitGraph}
              >
                {nodeSaveLoading ? <><span className="moon-spinner" /> Saving...</> : 'Save block properties'}
              </button>
              {nodeSaveError && <div className="moon-dock-form__error">{nodeSaveError}</div>}
            </>
          )}

          {requiresSavedWorkflow ? (
            <>
              <div className="moon-dock__sep" style={{ marginTop: 20 }} />
              <div className="moon-dock__section-label">Authority-backed tools</div>
              <div className="moon-dock__item" style={{ cursor: 'default' }}>
                <div className="moon-dock__item-title">Save draft to unlock imports, attachments, and bindings</div>
                <div className="moon-dock__item-desc">
                  Primitive edits above already persist in local graph state. Authority-backed tools need a saved workflow id so the backend can own the result.
                </div>
              </div>
            </>
          ) : (
            <>
              {/* Attached */}
              <div className="moon-dock__sep" />
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

              {/* Imports */}
              <div className="moon-dock__section-label" style={{ marginTop: 16 }}>
                Imports ({content?.imports?.length || 0})
              </div>
              {(content?.imports || []).map(s => (
                <div key={s.snapshot_id} className="moon-dock__item">
                  <div className="moon-dock__item-title">{s.source_locator || s.snapshot_id}</div>
                  <div className="moon-dock__item-desc">
                    {s.approval_state === 'admitted' ? 'Admitted' : 'Staged'}
                    {s.approval_state !== 'admitted' && (
                      <button
                        className="moon-dock-form__btn--small"
                        onClick={() => handleAdmitImport(s.snapshot_id, (s as any).requested_shape || {})}
                      >
                        Approve
                      </button>
                    )}
                  </div>
                </div>
              ))}

              {/* Bindings */}
              <div className="moon-dock__section-label" style={{ marginTop: 16 }}>
                Bindings ({bindings.length})
              </div>
              {bindings.map(binding => (
                <BindingCard key={binding.binding_id} binding={binding} onMutate={onMutate} onCommitAuthorityAction={onCommitAuthorityAction} />
              ))}
              {!bindings.length && (
                <div className="moon-dock__empty">No bindings.</div>
              )}

              {/* DB Objects — drag onto chain nodes to attach */}
              <div className="moon-dock__sep" style={{ marginTop: 20 }} />
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
                  style={{ marginBottom: 8, fontSize: 12, padding: '5px 8px' }}
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

              {/* Attach reference form */}
              <div className="moon-dock__sep" style={{ marginTop: 20 }} />
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

              <button className="moon-dock-form__btn" onClick={handleAttach} disabled={attachLoading || !attachRef.trim()}>
                {attachLoading ? <><span className="moon-spinner" /> Attaching...</> : 'Attach'}
              </button>
              {attachError && <div className="moon-dock-form__error">{attachError}</div>}

              {/* Stage import form */}
              <div className="moon-dock__sep" style={{ marginTop: 20 }} />
              <div className="moon-dock__section-label">Stage import</div>

              <input className="moon-dock-form__input" type="text" value={importLocator} onChange={e => setImportLocator(e.target.value)} placeholder="URL or source locator" />
              <input className="moon-dock-form__input" type="text" value={importLabel} onChange={e => setImportLabel(e.target.value)} placeholder="Label (optional)" />

              <div className="moon-dock-form__row">
                <button className="moon-dock-form__btn" onClick={handleStageImport} disabled={importLoading || !importLocator.trim()}>
                  {importLoading ? <><span className="moon-spinner" /> Staging...</> : 'Stage'}
                </button>
                <button className="moon-dock-form__btn" onClick={handleMaterialize} disabled={importLoading || !importLocator.trim()}>
                  Materialize here
                </button>
              </div>
              {importError && <div className="moon-dock-form__error">{importError}</div>}
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
        accepted_target: target,
        rationale: 'Accepted from Moon Build.',
      };
      if (onCommitAuthorityAction) {
        const targetLabel = target.enrichment?.integration_name || target.label || target.target_ref || 'selected target';
        await onCommitAuthorityAction(`bindings/${binding.binding_id}/accept`, request, {
          label: 'Accept binding',
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
        await onMutate(`bindings/${binding.binding_id}/accept`, request);
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
        accepted_target: {
          target_ref: replaceRef.trim(),
          label: replaceRef.trim(),
          kind: 'custom',
        },
        rationale: 'Replaced from Moon Build.',
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction(`bindings/${binding.binding_id}/replace`, request, {
          label: 'Replace binding',
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
        await onMutate(`bindings/${binding.binding_id}/replace`, request);
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
        rationale: 'Rejected from Moon Build.',
      };
      if (onCommitAuthorityAction) {
        await onCommitAuthorityAction(`bindings/${binding.binding_id}/reject`, request, {
          label: 'Reject binding',
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
        await onMutate(`bindings/${binding.binding_id}/reject`, request);
      }
    } catch (e: any) {
      setError(e.message || 'Failed to reject');
    } finally {
      setLoading(false);
    }
  }, [binding.binding_id, binding.source_label, onCommitAuthorityAction, onMutate]);

  const stateColor = isAccepted ? '#3fb950' : isRejected ? 'var(--moon-error)' : 'var(--moon-fg-muted)';

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
                <span style={{ marginLeft: 6, fontSize: 10, color: binding.accepted_target.enrichment.auth_status === 'connected' ? '#3fb950' : 'var(--moon-fg-muted)' }}>
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
        <div style={{ marginTop: 8 }}>
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
              style={{ display: 'block', width: '100%', textAlign: 'left', padding: '6px 10px', marginBottom: 4 }}
            >
              {target.enrichment?.integration_name || target.label || target.target_ref || 'Target'}
              {target.enrichment?.auth_status ? (
                <span className="moon-dock__item-desc" style={{ marginLeft: 6, color: target.enrichment.auth_status === 'connected' ? '#3fb950' : 'var(--moon-fg-muted)' }}>
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
        <div style={{ marginTop: 8, display: 'flex', gap: 8, alignItems: 'center' }}>
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
