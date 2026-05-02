import type { BuildPayload, WorkflowContextAuthorityPayload } from '../shared/types';

export interface WorkflowContextReadInput {
  context_ref?: string;
  workflow_ref?: string;
  include_entities: boolean;
  include_bindings: boolean;
  include_transitions: boolean;
  limit: number;
}

export interface WorkflowContextFetchOptions {
  fetchImpl?: typeof fetch;
}

export interface WorkflowContextInspectorSummary {
  contextRef: string;
  mode: string;
  pill: string;
  confidence: string;
  objectLabels: string[];
  blockerCount: number;
  hardBlockerCount: number;
  verifierCount: number;
  ioModes: string[];
  nextActions: string[];
  simulationStatus: string | null;
  virtualLabRevision: string | null;
}

export interface WorkflowContextNodeSummary {
  contextRef: string;
  pill: string;
  ioMode: string;
  objectLabels: string[];
  blockerCount: number;
  verifierCount: number;
  nextAction: string | null;
}

function recordFromUnknown(value: unknown): Record<string, any> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, any>
    : {};
}

function text(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function numberValue(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function listFromUnknown<T = unknown>(value: unknown): T[] {
  return Array.isArray(value) ? value as T[] : [];
}

function hasKeys(value: Record<string, any>): boolean {
  return Object.keys(value).length > 0;
}

function compactRef(value: unknown): string {
  const raw = text(value);
  if (!raw) return 'pending';
  if (raw.length <= 34) return raw;
  return `${raw.slice(0, 18)}...${raw.slice(-10)}`;
}

function confidenceLabel(payload: WorkflowContextAuthorityPayload): string {
  const confidence = recordFromUnknown(payload.confidence);
  const score = numberValue(payload.confidence_score) ?? numberValue(confidence.score);
  const state = text(payload.confidence_state) ?? text(confidence.state);
  if (score == null) return state || 'unknown';
  const percent = Math.round(score * 100);
  return state ? `${state} ${percent}%` : `${percent}%`;
}

function uniqueText(values: Array<string | null>, limit: number): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const value of values) {
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
    if (out.length >= limit) break;
  }
  return out;
}

export function workflowContextFromPayload(payload: BuildPayload | null): WorkflowContextAuthorityPayload | null {
  const direct = payload?.workflow_context;
  if (direct && typeof direct === 'object') return direct;
  const graphContext = payload?.build_graph?.context_authority;
  if (graphContext && typeof graphContext === 'object') return graphContext;
  return null;
}

export function workflowContextFromReadResponse(value: unknown): WorkflowContextAuthorityPayload | null {
  const envelope = recordFromUnknown(value);
  const result = recordFromUnknown(envelope.result ?? envelope.payload ?? value);
  const direct = recordFromUnknown(result.context_pack);
  if (hasKeys(direct)) return direct as WorkflowContextAuthorityPayload;
  const packs = listFromUnknown<WorkflowContextAuthorityPayload>(result.context_packs);
  return packs.find((pack) => Boolean(text(pack?.context_ref))) || null;
}

export function workflowContextReadInputFromPayload(payload: BuildPayload | null): WorkflowContextReadInput | null {
  const embedded = workflowContextFromPayload(payload);
  const contextRef = text(embedded?.context_ref);
  const workflowRef = text(embedded?.workflow_ref)
    || text(payload?.workflow?.id)
    || text(recordFromUnknown(payload?.graph_summary).workflow_id)
    || text(payload?.build_graph?.graph_id);
  const base = {
    include_entities: true,
    include_bindings: true,
    include_transitions: false,
    limit: 1,
  };
  if (contextRef) return { ...base, context_ref: contextRef };
  if (workflowRef) return { ...base, workflow_ref: workflowRef };
  return null;
}

export async function fetchWorkflowContextAuthority(
  payload: BuildPayload | null,
  options: WorkflowContextFetchOptions = {},
): Promise<WorkflowContextAuthorityPayload | null> {
  const input = workflowContextReadInputFromPayload(payload);
  if (!input) return null;
  const fetchImpl = options.fetchImpl || fetch;
  const response = await fetchImpl('/api/operate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      operation: 'workflow_context_read',
      input,
    }),
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const details = recordFromUnknown(body);
    const message = text(details.error) || text(details.detail) || `HTTP ${response.status}`;
    throw new Error(message);
  }
  return workflowContextFromReadResponse(body);
}

export function workflowContextSummaryFromAuthority(
  context: WorkflowContextAuthorityPayload | null | undefined,
): WorkflowContextInspectorSummary | null {
  if (!context) return null;
  const entities = listFromUnknown<NonNullable<WorkflowContextAuthorityPayload['entities']>[number]>(context.entities);
  const blockers = listFromUnknown<NonNullable<WorkflowContextAuthorityPayload['blockers']>[number]>(context.blockers);
  const verifierExpectations = listFromUnknown(context.verifier_expectations);
  const guardrail = recordFromUnknown(context.guardrail);
  const noGo = listFromUnknown(guardrail.no_go_conditions);
  const virtualLab = recordFromUnknown(context.synthetic_world?.virtual_lab);
  const environmentRevision = recordFromUnknown(virtualLab.environment_revision);
  const latestSimulation = recordFromUnknown(context.latest_virtual_lab_simulation);
  const objectLabels = uniqueText(
    entities
      .filter((entity) => text(entity.entity_kind) === 'object')
      .map((entity) => text(entity.label)),
    5,
  );
  const ioModes = uniqueText(entities.map((entity) => text(entity.io_mode)), 4);
  const safeNext = listFromUnknown(guardrail.safe_next_llm_actions)
    .map((value) => text(value))
    .filter((value): value is string => Boolean(value));
  const allowedNext = listFromUnknown(guardrail.allowed_next_actions)
    .map((value) => text(value))
    .filter((value): value is string => Boolean(value));
  const nextActions = uniqueText([...safeNext, ...allowedNext], 4);
  const simulationStatus = text(latestSimulation.status)
    || (virtualLab.simulation_run_payload ? 'ready' : null);
  return {
    contextRef: compactRef(context.context_ref),
    mode: text(context.context_mode) || 'unknown',
    pill: text(context.context_pill) || text(context.truth_state) || 'none',
    confidence: confidenceLabel(context),
    objectLabels,
    blockerCount: blockers.length + noGo.length,
    hardBlockerCount: blockers.filter((blocker) => text(blocker.severity) === 'hard').length,
    verifierCount: verifierExpectations.length,
    ioModes,
    nextActions,
    simulationStatus,
    virtualLabRevision: text(environmentRevision.revision_id),
  };
}

export function workflowContextSummaryFromPayload(payload: BuildPayload | null): WorkflowContextInspectorSummary | null {
  return workflowContextSummaryFromAuthority(workflowContextFromPayload(payload));
}

export function workflowContextNodeSummary(
  context: WorkflowContextAuthorityPayload | null | undefined,
  nodeId: string | null | undefined,
): WorkflowContextNodeSummary | null {
  if (!context || !nodeId) return null;
  const entities = listFromUnknown<NonNullable<WorkflowContextAuthorityPayload['entities']>[number]>(context.entities);
  const nodeEntity = entities.find((entity) => {
    if (text(entity.entity_kind) !== 'workflow_node') return false;
    const payload = recordFromUnknown(entity.payload);
    return text(payload.node_id) === nodeId;
  }) || null;
  const blockers = listFromUnknown<NonNullable<WorkflowContextAuthorityPayload['blockers']>[number]>(context.blockers);
  const verifierExpectations = listFromUnknown(context.verifier_expectations);
  const guardrail = recordFromUnknown(context.guardrail);
  const noGo = listFromUnknown(guardrail.no_go_conditions);
  const safeNext = listFromUnknown(guardrail.safe_next_llm_actions)
    .map((value) => text(value))
    .filter((value): value is string => Boolean(value));
  const allowedNext = listFromUnknown(guardrail.allowed_next_actions)
    .map((value) => text(value))
    .filter((value): value is string => Boolean(value));
  const objectLabels = uniqueText(
    entities
      .filter((entity) => text(entity.entity_kind) === 'object')
      .map((entity) => text(entity.label)),
    4,
  );
  return {
    contextRef: compactRef(context.context_ref),
    pill: text(nodeEntity?.context_pill)
      || text(nodeEntity?.truth_state)
      || text(context.context_pill)
      || text(context.truth_state)
      || 'none',
    ioMode: text(nodeEntity?.io_mode) || text(context.context_mode) || 'none',
    objectLabels,
    blockerCount: blockers.length + noGo.length,
    verifierCount: verifierExpectations.length,
    nextAction: safeNext[0] || allowedNext[0] || null,
  };
}
