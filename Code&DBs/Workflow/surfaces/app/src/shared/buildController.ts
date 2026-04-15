// Shared build controller — all API interactions for build state.
// No React. Pure async functions that take IDs and return typed results.

import type { BuildPayload } from './types';

interface BuildDefinitionRequest {
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

export async function compileDefinition(prose: string, title?: string): Promise<BuildPayload> {
  return _json(await fetch('/api/compile', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prose, title }),
  }));
}

export async function refineDefinition(prose: string, definition: any): Promise<BuildPayload> {
  return _json(await fetch('/api/refine-definition', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prose, definition }),
  }));
}

export async function commitDefinition(
  workflowId: string,
  opts?: BuildDefinitionRequest,
): Promise<BuildPayload> {
  return _json(await fetch('/api/commit', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      workflow_id: workflowId,
      title: opts?.title,
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
  opts?: Omit<BuildDefinitionRequest, 'title'>,
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
  return _json(await fetch('/api/plan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      definition: opts?.definition,
      build_graph: opts?.buildGraph,
      title: opts?.title,
    }),
  }));
}

export async function triggerWorkflow(workflowId: string): Promise<{ run_id: string; status: string }> {
  return _json(await fetch(`/api/trigger/${workflowId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
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
