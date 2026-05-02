import type { BuildEdge, BuildNode, BuildPayload } from '../shared/types';
import type { WorkflowContextAuthorityPayload } from '../shared/types';
import type { CatalogItem } from './catalog';

export type ClientOperatingModelState =
  | 'unknown'
  | 'missing'
  | 'not_authorized'
  | 'stale'
  | 'blocked'
  | 'conflict'
  | 'healthy'
  | 'empty'
  | 'partial'
  | 'checking'
  | 'unavailable'
  | 'unchecked';

export interface ClientOperatingModelBuilderStatus {
  state: ClientOperatingModelState;
  ok: boolean | null;
  checking: boolean;
  errorCount: number;
  warningCount: number;
  safeActionCount: number;
  approvedBlockCount: number;
  nodeCount: number;
  edgeCount: number;
  reasonCodes: string[];
  receiptId: string | null;
  viewId: string | null;
  checkedAt: string | null;
  message: string;
}

export interface ClientOperatingModelCompositeStatus {
  state: ClientOperatingModelState;
  deployabilityState: string;
  buildabilityState: string;
  syntheticProofState: string;
  bindingCoverageState: string;
  realEvidenceState: string;
  confidence: string;
  blockerCount: number;
  reviewDecisionCount: number;
  canBuild: boolean;
  canSimulate: boolean;
  canPromote: boolean;
  costAmount: string | null;
  truthStateCount: number;
  receiptId: string | null;
  viewId: string | null;
  checkedAt: string | null;
  message: string;
}

export interface BuilderValidationInputOptions {
  scopeRef?: string | null;
  correlationIds?: string[];
  evidenceRefs?: string[];
}

function recordFromUnknown(value: unknown): Record<string, any> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, any>
    : {};
}

function listFromUnknown(value: unknown): any[] {
  return Array.isArray(value) ? value : [];
}

function cleanText(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function compactRef(value: unknown): string | null {
  const raw = cleanText(value);
  if (!raw) return null;
  if (raw.length <= 18) return raw;
  return `${raw.slice(0, 10)}...${raw.slice(-6)}`;
}

export function blockRefForNode(node: BuildNode): string {
  const toolPlan = recordFromUnknown(node.agent_tool_plan);
  return cleanText(node.route)
    || cleanText(toolPlan.operation)
    || cleanText(toolPlan.tool_name)
    || node.kind
    || 'unknown';
}

function approvedBlocksFromCatalog(catalog: CatalogItem[]): Record<string, Record<string, unknown>> {
  const approved: Record<string, Record<string, unknown>> = {};
  for (const item of catalog) {
    if (item.status !== 'ready') continue;
    const refs = [item.actionValue, item.id, item.gateFamily].flatMap((value) => {
      const text = cleanText(value);
      return text ? [text] : [];
    });
    for (const ref of refs) {
      approved[ref] = {
        label: item.label,
        family: item.family,
        source: item.source || 'surface_registry',
      };
    }
  }
  return approved;
}

function normalizeBuilderNode(node: BuildNode): Record<string, unknown> {
  const blockRef = blockRefForNode(node);
  return {
    node_id: node.node_id,
    block_ref: blockRef,
    title: node.title || node.summary || node.node_id,
    kind: node.kind,
    route: node.route || null,
    requires: Array.isArray(node.required_inputs) ? node.required_inputs : [],
    provides: Array.isArray(node.outputs) ? node.outputs : [],
  };
}

function normalizeBuilderEdge(edge: BuildEdge): Record<string, unknown> {
  return {
    edge_id: edge.edge_id,
    from: edge.from_node_id,
    to: edge.to_node_id,
    kind: edge.kind,
  };
}

export function buildWorkflowBuilderValidationInputs(
  payload: BuildPayload | null,
  catalog: CatalogItem[],
): Record<string, unknown> | null {
  const graph = payload?.build_graph;
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  if (!nodes.length && !edges.length) return null;
  return {
    graph: {
      nodes: nodes.map(normalizeBuilderNode),
      edges: edges.map(normalizeBuilderEdge),
    },
    approved_blocks: approvedBlocksFromCatalog(catalog),
    allowed_edges: [],
  };
}

function statusFromOperatorPayload(payload: Record<string, any>, fallbackReceiptId: string | null): ClientOperatingModelBuilderStatus {
  const result = recordFromUnknown(payload.result ?? payload.payload ?? payload);
  const receipt = recordFromUnknown(payload.operation_receipt ?? result.operation_receipt);
  const operatorView = recordFromUnknown(result.operator_view ?? result.view ?? result);
  const viewPayload = recordFromUnknown(operatorView.payload);
  const validation = recordFromUnknown(viewPayload.validation);
  const errors = listFromUnknown(validation.errors);
  const warnings = listFromUnknown(validation.warnings);
  const safeActions = listFromUnknown(viewPayload.safe_action_summary);
  const state = cleanText(operatorView.state ?? result.state ?? viewPayload.state) || 'unknown';
  const reasonCodes = errors
    .map((error) => cleanText(recordFromUnknown(error).reason_code))
    .filter((value): value is string => Boolean(value));
  const ok = typeof validation.ok === 'boolean'
    ? validation.ok
    : typeof result.ok === 'boolean'
      ? result.ok
      : null;
  const rawReceiptId = cleanText(receipt.receipt_id) || cleanText(result.operation_receipt_id) || fallbackReceiptId;
  const checkedAt = cleanText(receipt.completed_at) || cleanText(receipt.started_at) || new Date().toISOString();
  const errorCount = errors.length;
  const warningCount = warnings.length;
  return {
    state: state as ClientOperatingModelState,
    ok,
    checking: false,
    errorCount,
    warningCount,
    safeActionCount: safeActions.length,
    approvedBlockCount: Number(validation.approved_block_count || 0),
    nodeCount: Number(validation.node_count || 0),
    edgeCount: Number(validation.edge_count || 0),
    reasonCodes,
    receiptId: compactRef(rawReceiptId),
    viewId: cleanText(operatorView.view_id ?? result.view_id),
    checkedAt,
    message: ok === true
      ? 'Builder validation passed through the client model authority.'
      : errorCount > 0
        ? `${errorCount} builder blocker${errorCount === 1 ? '' : 's'} found.`
        : 'Builder validation returned without a green result.',
  };
}

function confidenceSummary(payload: Record<string, any>): string {
  const score = typeof payload.score === 'number' ? payload.score : null;
  const state = cleanText(payload.state) || 'unknown';
  if (score == null) return state;
  return `${state} ${Math.round(score * 100)}%`;
}

function pseudoBuilderValidationView(status: ClientOperatingModelBuilderStatus | null): Record<string, unknown> | null {
  if (!status) return null;
  return {
    operator_view: {
      state: status.state,
      payload: {
        validation: {
          ok: status.ok,
          errors: status.errorCount > 0
            ? status.reasonCodes.map((reasonCode) => ({ reason_code: reasonCode }))
            : [],
          warnings: status.errorCount > 0
            ? []
            : status.reasonCodes.map((reasonCode) => ({ reason_code: reasonCode })),
          approved_block_count: status.approvedBlockCount,
          node_count: status.nodeCount,
          edge_count: status.edgeCount,
        },
        safe_action_summary: Array.from({ length: status.safeActionCount }, (_, index) => ({
          action_ref: `workflow_builder.safe_action.${index + 1}`,
        })),
      },
    },
  };
}

export function buildWorkflowContextCompositeInputs(
  payload: BuildPayload | null,
  context: WorkflowContextAuthorityPayload | null,
  builderStatus: ClientOperatingModelBuilderStatus | null = null,
): Record<string, unknown> | null {
  if (!context) return null;
  return {
    workflow_ref: context.workflow_ref
      || payload?.workflow?.id
      || cleanText(recordFromUnknown(payload?.graph_summary).workflow_id)
      || payload?.build_graph?.graph_id
      || 'workflow.unspecified',
    context_pack: context,
    builder_validation_view: pseudoBuilderValidationView(builderStatus),
  };
}

function compositeStatusFromOperatorPayload(payload: Record<string, any>, fallbackReceiptId: string | null): ClientOperatingModelCompositeStatus {
  const result = recordFromUnknown(payload.result ?? payload.payload ?? payload);
  const receipt = recordFromUnknown(payload.operation_receipt ?? result.operation_receipt);
  const operatorView = recordFromUnknown(result.operator_view ?? result.view ?? result);
  const viewPayload = recordFromUnknown(operatorView.payload);
  const deployability = recordFromUnknown(viewPayload.deployability);
  const buildability = recordFromUnknown(viewPayload.buildability);
  const syntheticProof = recordFromUnknown(viewPayload.synthetic_proof);
  const bindingCoverage = recordFromUnknown(viewPayload.binding_coverage);
  const realEvidence = recordFromUnknown(viewPayload.real_evidence);
  const blockers = recordFromUnknown(viewPayload.blockers);
  const confidence = recordFromUnknown(viewPayload.confidence);
  const cost = recordFromUnknown(viewPayload.cost);
  const truthStates = recordFromUnknown(viewPayload.truth_state_classes);
  const rawReceiptId = cleanText(receipt.receipt_id) || cleanText(result.operation_receipt_id) || fallbackReceiptId;
  const state = cleanText(operatorView.state ?? result.state ?? viewPayload.state) || 'unknown';
  const deployabilityState = cleanText(deployability.state) || 'not_ready';
  const canPromote = deployability.can_promote === true;
  const canSimulate = deployability.can_simulate === true;
  const canBuild = deployability.can_build === true;
  const blockerCount = Number(blockers.hard_count || 0) + Number(blockers.soft_count || 0);
  const reviewDecisionCount = Number(blockers.review_decision_count || 0);
  return {
    state: state as ClientOperatingModelState,
    deployabilityState,
    buildabilityState: cleanText(buildability.state) || 'missing',
    syntheticProofState: cleanText(syntheticProof.state) || 'missing',
    bindingCoverageState: cleanText(bindingCoverage.state) || 'missing',
    realEvidenceState: cleanText(realEvidence.state) || 'missing',
    confidence: confidenceSummary(confidence),
    blockerCount,
    reviewDecisionCount,
    canBuild,
    canSimulate,
    canPromote,
    costAmount: cleanText(cost.amount),
    truthStateCount: Object.values(truthStates).reduce((sum, value) => (
      sum + (typeof value === 'number' && Number.isFinite(value) ? value : 0)
    ), 0),
    receiptId: compactRef(rawReceiptId),
    viewId: cleanText(operatorView.view_id ?? result.view_id),
    checkedAt: cleanText(receipt.completed_at) || cleanText(receipt.started_at) || new Date().toISOString(),
    message: canPromote
      ? 'Context is a promotion candidate with live evidence.'
      : deployabilityState === 'blocked'
        ? `${blockerCount || reviewDecisionCount || 1} deployability blocker${(blockerCount || reviewDecisionCount || 1) === 1 ? '' : 's'} found.`
        : canSimulate
          ? 'Synthetic proof is ready; live promotion still needs trusted evidence.'
          : canBuild
            ? 'Workflow is buildable; proof and binding coverage are still forming.'
            : 'Composite view is waiting on context authority.',
  };
}

export function statusFromBuilderValidationError(error: unknown): ClientOperatingModelBuilderStatus {
  const message = error instanceof Error ? error.message : String(error || 'Client model check failed.');
  return {
    state: 'unavailable',
    ok: false,
    checking: false,
    errorCount: 1,
    warningCount: 0,
    safeActionCount: 0,
    approvedBlockCount: 0,
    nodeCount: 0,
    edgeCount: 0,
    reasonCodes: ['client_operating_model.check_unavailable'],
    receiptId: null,
    viewId: null,
    checkedAt: new Date().toISOString(),
    message,
  };
}

export function statusFromCompositeError(error: unknown): ClientOperatingModelCompositeStatus {
  const message = error instanceof Error ? error.message : String(error || 'Client model composite unavailable.');
  return {
    state: 'unavailable',
    deployabilityState: 'unavailable',
    buildabilityState: 'unavailable',
    syntheticProofState: 'unavailable',
    bindingCoverageState: 'unavailable',
    realEvidenceState: 'unavailable',
    confidence: 'unknown',
    blockerCount: 1,
    reviewDecisionCount: 0,
    canBuild: false,
    canSimulate: false,
    canPromote: false,
    costAmount: null,
    truthStateCount: 0,
    receiptId: null,
    viewId: null,
    checkedAt: new Date().toISOString(),
    message,
  };
}

export function checkingBuilderValidationStatus(
  previous: ClientOperatingModelBuilderStatus | null,
): ClientOperatingModelBuilderStatus {
  return {
    ...(previous || {
      state: 'unchecked',
      ok: null,
      errorCount: 0,
      warningCount: 0,
      safeActionCount: 0,
      approvedBlockCount: 0,
      nodeCount: 0,
      edgeCount: 0,
      reasonCodes: [],
      receiptId: null,
      viewId: null,
      checkedAt: null,
      message: 'Builder has not been checked yet.',
    }),
    state: 'checking',
    checking: true,
    message: 'Checking builder graph through the client model authority.',
  };
}

export async function fetchWorkflowBuilderValidationStatus(
  payload: BuildPayload | null,
  catalog: CatalogItem[],
  options: BuilderValidationInputOptions = {},
): Promise<ClientOperatingModelBuilderStatus> {
  const inputs = buildWorkflowBuilderValidationInputs(payload, catalog);
  if (!inputs) {
    return {
      state: 'empty',
      ok: null,
      checking: false,
      errorCount: 0,
      warningCount: 0,
      safeActionCount: 0,
      approvedBlockCount: 0,
      nodeCount: 0,
      edgeCount: 0,
      reasonCodes: [],
      receiptId: null,
      viewId: null,
      checkedAt: new Date().toISOString(),
      message: 'No builder graph is available to check.',
    };
  }
  const operationReceipt = recordFromUnknown(payload?.operation_receipt);
  const response = await fetch('/api/operate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      operation: 'client_operating_model_operator_view',
      input: {
        view: 'workflow_builder_validation',
        inputs,
        permission_scope: {
          scope_ref: options.scopeRef || payload?.workflow?.id || 'canvas.workflow_builder',
          visibility: 'full',
        },
        correlation_ids: options.correlationIds || [
          cleanText(operationReceipt.correlation_id),
          cleanText(operationReceipt.receipt_id),
        ].filter((value): value is string => Boolean(value)),
        evidence_refs: options.evidenceRefs || [
          payload?.workflow?.id ? `workflow.${payload.workflow.id}` : null,
          cleanText(payload?.build_graph?.graph_id),
          'canvas.build_graph',
        ].filter((value): value is string => Boolean(value)),
      },
    }),
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(cleanText(recordFromUnknown(body).error || recordFromUnknown(body).detail) || `HTTP ${response.status}`);
  }
  return statusFromOperatorPayload(recordFromUnknown(body), compactRef(operationReceipt.receipt_id));
}

export async function fetchWorkflowContextCompositeStatus(
  payload: BuildPayload | null,
  context: WorkflowContextAuthorityPayload | null,
  builderStatus: ClientOperatingModelBuilderStatus | null = null,
  options: BuilderValidationInputOptions = {},
): Promise<ClientOperatingModelCompositeStatus | null> {
  const inputs = buildWorkflowContextCompositeInputs(payload, context, builderStatus);
  if (!inputs) return null;
  const operationReceipt = recordFromUnknown(payload?.operation_receipt);
  const response = await fetch('/api/operate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      operation: 'client_operating_model_operator_view',
      input: {
        view: 'workflow_context_composite',
        inputs,
        permission_scope: {
          scope_ref: options.scopeRef || context?.workflow_ref || payload?.workflow?.id || 'workflow.context_composite',
          visibility: 'full',
        },
        correlation_ids: options.correlationIds || [
          cleanText(operationReceipt.correlation_id),
          cleanText(operationReceipt.receipt_id),
        ].filter((value): value is string => Boolean(value)),
        evidence_refs: options.evidenceRefs || [
          context?.context_ref ? `workflow_context.${context.context_ref}` : null,
          payload?.workflow?.id ? `workflow.${payload.workflow.id}` : null,
        ].filter((value): value is string => Boolean(value)),
      },
    }),
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(cleanText(recordFromUnknown(body).error || recordFromUnknown(body).detail) || `HTTP ${response.status}`);
  }
  return compositeStatusFromOperatorPayload(recordFromUnknown(body), compactRef(operationReceipt.receipt_id));
}
