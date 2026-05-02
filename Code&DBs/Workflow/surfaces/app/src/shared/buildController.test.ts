import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { materializePlan, postBuildMutation } from './buildController';

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

describe('materializePlan', () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it('calls the canonical compile materialize route without pre-creating a workflow', async () => {
    const onWorkflowReady = vi.fn();
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: true,
      workflow_id: 'wf_materialized',
      build_payload: {
        workflow: { id: 'wf_materialized', name: 'Canvas delivery' },
        build_graph: {
          nodes: [{ node_id: 'n1', title: 'Trigger' }],
          edges: [],
        },
      },
      graph_summary: { node_count: 1, edge_count: 0 },
      operation_receipt: { receipt_id: 'receipt-1', correlation_id: 'corr-1' },
    }));

    const payload = await materializePlan('Build the Canvas delivery probe', {
      workflowId: 'wf_existing',
      title: 'Canvas delivery',
      fullCompose: true,
      enableLlm: false,
      onWorkflowReady,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('/api/compile/materialize');
    expect(url).not.toBe('/api/workflows');
    expect(init.method).toBe('POST');
    expect(JSON.parse(init.body as string)).toEqual({
      intent: 'Build the Canvas delivery probe',
      workflow_id: 'wf_existing',
      title: 'Canvas delivery',
      enable_llm: false,
      enable_full_compose: true,
    });
    expect(payload.workflow?.id).toBe('wf_materialized');
    expect(payload.graph_summary).toEqual({ node_count: 1, edge_count: 0 });
    expect(payload.operation_receipt).toEqual({ receipt_id: 'receipt-1', correlation_id: 'corr-1' });
    expect(onWorkflowReady).toHaveBeenCalledWith('wf_materialized');
  });

  it('surfaces blocked materialization instead of returning an empty successful payload', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: false,
      error: 'empty graph',
      error_code: 'compile.materialize.empty_graph',
      operation_receipt: { receipt_id: 'receipt-failed', execution_status: 'failed' },
    }));

    await expect(materializePlan('too vague')).rejects.toThrow('empty graph');
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe('/api/compile/materialize');
  });
});

describe('postBuildMutation', () => {
  it('rejects bootstrap mutations on the UI surface', async () => {
    await expect(postBuildMutation('wf_1', 'bootstrap', {})).rejects.toThrow(
      'Bootstrap materialization moved to materializePlan().',
    );
  });
});
