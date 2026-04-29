// Shared build controller — all API interactions for build state.
// No React. Pure async functions that take IDs and return typed results.

import type { BuildPayload, CompilePreviewPayload } from './types';
import { fetchJson, type JsonRequestOptions } from './request';

interface BuildDefinitionRequest {
  workflowId?: string | null;
  title?: string;
  definition?: Record<string, unknown>;
  buildGraph?: BuildPayload['build_graph'] | null;
  compiled_spec?: Record<string, unknown> | null;
}

export interface CatalogReviewDecisionRequest {
  surface_name?: string;
  target_kind: 'catalog_item' | 'source_policy';
  target_ref: string;
  decision: 'approve' | 'widen' | 'reject' | 'defer' | 'revoke';
  actor_type?: string;
  actor_ref?: string;
  approval_mode?: string;
  rationale?: string;
  candidate_payload?: Record<string, unknown>;
}

export async function loadWorkflowDefinition(workflowId: string): Promise<any> {
  return fetchJson(`/api/workflows/${workflowId}`);
}

export async function loadWorkflowBuild(workflowId: string, options?: JsonRequestOptions): Promise<BuildPayload> {
  return fetchJson(`/api/workflows/${workflowId}/build`, {}, options);
}

export async function saveWorkflowDefinition(workflowId: string, definition: any): Promise<any> {
  return fetchJson(`/api/workflows/${workflowId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(definition),
  }, { timeoutMs: 20000 });
}

async function _ensureWorkflowId(
  workflowId: string | null | undefined,
  title?: string,
  opts?: Omit<BuildDefinitionRequest, 'workflowId' | 'title'>,
): Promise<string> {
  if (workflowId) return workflowId;
  const created = await createWorkflow(title || 'Moon draft', {
    definition: opts?.definition ?? {},
    buildGraph: opts?.buildGraph,
    compiled_spec: opts?.compiled_spec,
  });
  return created.id || created.workflow_id || '';
}

export async function compileDefinition(
  prose: string,
  opts?: Pick<BuildDefinitionRequest, 'workflowId' | 'title'> & { fullCompose?: boolean },
): Promise<BuildPayload> {
  const workflowId = await _ensureWorkflowId(opts?.workflowId, opts?.title, { definition: {} });
  // Default to the full compose path: synthesis + 20 parallel fork-out
  // author calls. The build canvas renders nothing until this returns —
  // operator standing order feedback_autonomous_first_no_review_gates +
  // feedback_fanout_by_work_volume.
  const enableFullCompose = opts?.fullCompose !== false;
  return postBuildMutation(workflowId, 'bootstrap', {
    prose,
    title: opts?.title,
    enable_llm: true,
    enable_full_compose: enableFullCompose,
  });
}

export async function previewCompile(prose: string): Promise<CompilePreviewPayload> {
  return fetchJson('/api/compile/preview', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ intent: prose }),
  }, { timeoutMs: 10000 });
}

export async function refineDefinition(
  prose: string,
  opts?: Pick<BuildDefinitionRequest, 'workflowId' | 'title'>,
): Promise<BuildPayload> {
  const workflowId = await _ensureWorkflowId(opts?.workflowId, opts?.title, { definition: {} });
  return postBuildMutation(workflowId, 'bootstrap', {
    prose,
    title: opts?.title,
    enable_llm: true,
  });
}

export async function progressiveBuildStep(
  prose: string,
  opts?: Pick<BuildDefinitionRequest, 'workflowId' | 'title' | 'buildGraph'>,
): Promise<BuildPayload> {
  const workflowId = await _ensureWorkflowId(opts?.workflowId, opts?.title, {
    definition: {},
    buildGraph: opts?.buildGraph,
  });
  return postBuildMutation(workflowId, 'progressive', {
    prose,
    title: opts?.title,
    build_graph: opts?.buildGraph,
  });
}

export async function commitDefinition(
  workflowId: string,
  opts?: BuildDefinitionRequest,
): Promise<any> {
  return fetchJson(`/api/workflows/${workflowId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: opts?.title,
      definition: opts?.definition,
      build_graph: opts?.buildGraph,
      compiled_spec: opts?.compiled_spec,
    }),
  }, { timeoutMs: 20000 });
}

export async function fetchCatalogEnvelope(): Promise<any> {
  return fetchJson('/api/catalog', undefined, { timeoutMs: 10000 });
}

export async function fetchCatalog(): Promise<any[]> {
  return fetchCatalogEnvelope().then(r => r.items);
}

export async function fetchCatalogReviewDecisions(params?: {
  surface?: string;
  target_kind?: string;
  target_ref?: string;
}): Promise<any> {
  const search = new URLSearchParams();
  if (params?.surface) search.set('surface', params.surface);
  if (params?.target_kind) search.set('target_kind', params.target_kind);
  if (params?.target_ref) search.set('target_ref', params.target_ref);
  const query = search.toString();
  return fetchJson(`/api/catalog/review-decisions${query ? `?${query}` : ''}`);
}

export async function postCatalogReviewDecision(body: CatalogReviewDecisionRequest): Promise<any> {
  return fetchJson('/api/catalog/review-decisions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }, { timeoutMs: 15000 });
}

export async function createWorkflow(
  name: string,
  opts?: Omit<BuildDefinitionRequest, 'workflowId' | 'title'>,
): Promise<{ id: string; workflow_id?: string }> {
  const data = await fetchJson<any>('/api/workflows', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name,
      definition: opts?.definition,
      build_graph: opts?.buildGraph,
      compiled_spec: opts?.compiled_spec,
    }),
  }, { timeoutMs: 15000 });
  // API returns { workflow: { id, name, ... } }
  return data.workflow || data;
}

export async function planDefinition(opts?: Omit<BuildDefinitionRequest, 'compiled_spec'>): Promise<any> {
  const workflowId = await _ensureWorkflowId(opts?.workflowId, opts?.title, {
    definition: opts?.definition,
    buildGraph: opts?.buildGraph,
  });
  return postBuildMutation(workflowId, 'harden', {
    definition: opts?.definition,
    build_graph: opts?.buildGraph,
    title: opts?.title,
  });
}

export async function triggerWorkflow(workflowId: string): Promise<{ run_id: string; status: string }> {
  return fetchJson(`/api/trigger/${workflowId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
  }, { timeoutMs: 15000 });
}

export async function suggestNextSteps(workflowId: string, nodeId: string, buildGraph: Record<string, unknown>): Promise<{ likely_next_steps: any[], possible_next_steps: any[] }> {
  return fetchJson(`/api/workflows/${workflowId}/build/suggest-next`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ node_id: nodeId, build_graph: buildGraph }),
  }, { timeoutMs: 20000 });
}

export async function postBuildMutation(
  workflowId: string,
  subpath: string,
  body: Record<string, unknown>,
): Promise<BuildPayload> {
  // Full compose path runs synthesis (~30s) + 20 parallel fork-out author
  // calls (~3 min wall-clock). Single-call compile fits the 45s budget;
  // give bootstrap-with-full-compose a 6 min ceiling so the LLM compile
  // gates can finish before the canvas renders anything.
  const isBootstrap = subpath === 'bootstrap';
  const fullCompose = isBootstrap && body['enable_full_compose'] !== false;
  const timeoutMs = fullCompose ? 360000 : (isBootstrap ? 45000 : 25000);
  return fetchJson(`/api/workflows/${workflowId}/build/${subpath}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }, { timeoutMs });
}
