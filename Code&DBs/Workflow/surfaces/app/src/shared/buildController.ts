// Shared build controller — all API interactions for build state.
// No React. Pure async functions that take IDs and return typed results.

import type { BuildPayload } from './types';

interface BuildDefinitionRequest {
  workflowId?: string | null;
  title?: string;
  definition?: Record<string, unknown>;
  buildGraph?: BuildPayload['build_graph'] | null;
  compiled_spec?: Record<string, unknown> | null;
}

async function _json(resp: Response): Promise<any> {
  const body = await resp.json();
  if (!resp.ok) throw new Error(body?.error || body?.detail || `HTTP ${resp.status}`);
  return body;
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
  return _json(await fetch(`/api/workflows/${workflowId}`));
}

export async function loadWorkflowBuild(workflowId: string): Promise<BuildPayload> {
  return _json(await fetch(`/api/workflows/${workflowId}/build`));
}

export async function saveWorkflowDefinition(workflowId: string, definition: any): Promise<any> {
  return _json(await fetch(`/api/workflows/${workflowId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(definition),
  }));
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
  opts?: Pick<BuildDefinitionRequest, 'workflowId' | 'title'>,
): Promise<BuildPayload> {
  const workflowId = await _ensureWorkflowId(opts?.workflowId, opts?.title, { definition: {} });
  return postBuildMutation(workflowId, 'bootstrap', {
    prose,
    title: opts?.title,
    enable_llm: false,
  });
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

export async function commitDefinition(
  workflowId: string,
  opts?: BuildDefinitionRequest,
): Promise<any> {
  return _json(await fetch(`/api/workflows/${workflowId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: opts?.title,
      definition: opts?.definition,
      build_graph: opts?.buildGraph,
      compiled_spec: opts?.compiled_spec,
    }),
  }));
}

export async function fetchCatalogEnvelope(): Promise<any> {
  return _json(await fetch('/api/catalog'));
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
  return _json(await fetch(`/api/catalog/review-decisions${query ? `?${query}` : ''}`));
}

export async function postCatalogReviewDecision(body: CatalogReviewDecisionRequest): Promise<any> {
  return _json(await fetch('/api/catalog/review-decisions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }));
}

export async function createWorkflow(
  name: string,
  opts?: Omit<BuildDefinitionRequest, 'workflowId' | 'title'>,
): Promise<{ id: string; workflow_id?: string }> {
  const data = await _json(await fetch('/api/workflows', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name,
      definition: opts?.definition,
      build_graph: opts?.buildGraph,
      compiled_spec: opts?.compiled_spec,
    }),
  }));
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
  return _json(await fetch(`/api/trigger/${workflowId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
  }));
}

export async function suggestNextSteps(workflowId: string, nodeId: string, buildGraph: Record<string, unknown>): Promise<{ likely_next_steps: any[], possible_next_steps: any[] }> {
  return _json(await fetch(`/api/workflows/${workflowId}/build/suggest-next`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ node_id: nodeId, build_graph: buildGraph }),
  }));
}

export async function postBuildMutation(
  workflowId: string,
  subpath: string,
  body: Record<string, unknown>,
): Promise<BuildPayload> {
  return _json(await fetch(`/api/workflows/${workflowId}/build/${subpath}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }));
}
