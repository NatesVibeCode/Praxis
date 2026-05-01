import { describe, expect, it, vi, afterEach } from 'vitest';
import {
  blockRefForNode,
  buildWorkflowContextCompositeInputs,
  buildWorkflowBuilderValidationInputs,
  fetchWorkflowContextCompositeStatus,
  fetchWorkflowBuilderValidationStatus,
  statusFromCompositeError,
  statusFromBuilderValidationError,
} from './clientOperatingModel';
import type { BuildPayload } from '../shared/types';
import type { CatalogItem } from './catalog';

const catalog: CatalogItem[] = [
  {
    id: 'cap-http-request',
    label: 'HTTP Request',
    icon: 'tool',
    family: 'act',
    status: 'ready',
    dropKind: 'node',
    actionValue: 'http.request',
  },
  {
    id: 'cap-manual-trigger',
    label: 'Manual Trigger',
    icon: 'trigger',
    family: 'trigger',
    status: 'ready',
    dropKind: 'node',
    actionValue: 'trigger',
  },
  {
    id: 'cap-future',
    label: 'Future Thing',
    icon: 'tool',
    family: 'act',
    status: 'coming_soon',
    dropKind: 'node',
    actionValue: 'future.thing',
  },
];

const payload: BuildPayload = {
  workflow: { id: 'workflow.billing', name: 'Billing Sync' },
  definition: {},
  operation_receipt: {
    receipt_id: 'receipt.original-builder',
    correlation_id: 'correlation-1',
  },
  build_graph: {
    graph_id: 'graph.billing',
    nodes: [
      { node_id: 'start', kind: 'step', title: 'Start', route: 'trigger' },
      { node_id: 'send', kind: 'step', title: 'Send', route: 'http.request' },
    ],
    edges: [
      { edge_id: 'edge.1', kind: 'sequence', from_node_id: 'start', to_node_id: 'send' },
    ],
  },
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe('clientOperatingModel Moon helpers', () => {
  it('maps Moon build graphs into workflow-builder validation inputs', () => {
    const inputs = buildWorkflowBuilderValidationInputs(payload, catalog);

    expect(inputs?.graph).toEqual({
      nodes: [
        {
          node_id: 'start',
          block_ref: 'trigger',
          title: 'Start',
          kind: 'step',
          route: 'trigger',
          requires: [],
          provides: [],
        },
        {
          node_id: 'send',
          block_ref: 'http.request',
          title: 'Send',
          kind: 'step',
          route: 'http.request',
          requires: [],
          provides: [],
        },
      ],
      edges: [{ edge_id: 'edge.1', from: 'start', to: 'send', kind: 'sequence' }],
    });
    expect(inputs?.approved_blocks).toHaveProperty('trigger');
    expect(inputs?.approved_blocks).toHaveProperty('http.request');
    expect(inputs?.approved_blocks).not.toHaveProperty('future.thing');
  });

  it('uses operation and tool refs as block refs when a node has no route', () => {
    expect(blockRefForNode({
      node_id: 'agent',
      kind: 'step',
      agent_tool_plan: { operation: 'authority.check' },
    })).toBe('authority.check');
  });

  it('calls the CQRS operator-view query and summarizes the receipt-backed result', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({
      ok: true,
      json: async () => ({
        result: {
          ok: true,
          state: 'healthy',
          view_id: 'workflow_builder_validation.abc',
          operator_view: {
            state: 'healthy',
            view_id: 'workflow_builder_validation.abc',
            payload: {
              validation: {
                ok: true,
                errors: [],
                warnings: [{ reason_code: 'builder.optional_gap' }],
                approved_block_count: 2,
                node_count: 2,
                edge_count: 1,
              },
              safe_action_summary: [{ action_ref: 'workflow_builder.save_candidate' }],
            },
          },
        },
        operation_receipt: { receipt_id: 'receipt.validation.1234567890' },
      }),
    })));

    const status = await fetchWorkflowBuilderValidationStatus(payload, catalog, { scopeRef: 'scope.billing' });
    const call = vi.mocked(fetch).mock.calls[0];

    expect(call[0]).toBe('/api/operate');
    expect(JSON.parse(String(call[1]?.body))).toMatchObject({
      operation: 'client_operating_model_operator_view',
      input: {
        view: 'workflow_builder_validation',
        permission_scope: { scope_ref: 'scope.billing', visibility: 'full' },
      },
    });
    expect(status).toMatchObject({
      state: 'healthy',
      ok: true,
      errorCount: 0,
      warningCount: 1,
      safeActionCount: 1,
      approvedBlockCount: 2,
      nodeCount: 2,
      edgeCount: 1,
      viewId: 'workflow_builder_validation.abc',
    });
    expect(status.receiptId).toBe('receipt.va...567890');
  });

  it('returns a machine-readable unavailable state for failed checks', () => {
    const status = statusFromBuilderValidationError(new Error('network down'));

    expect(status.state).toBe('unavailable');
    expect(status.ok).toBe(false);
    expect(status.reasonCodes).toEqual(['client_operating_model.check_unavailable']);
    expect(status.message).toBe('network down');
  });

  it('maps Workflow Context authority into composite view inputs', () => {
    const inputs = buildWorkflowContextCompositeInputs(
      payload,
      {
        context_ref: 'workflow_context:billing',
        workflow_ref: 'workflow.billing',
        context_mode: 'synthetic',
        truth_state: 'synthetic',
        confidence_score: 0.42,
        entities: [{ entity_kind: 'object', label: 'Invoice', truth_state: 'synthetic' }],
      },
      {
        state: 'healthy',
        ok: true,
        checking: false,
        errorCount: 0,
        warningCount: 0,
        safeActionCount: 1,
        approvedBlockCount: 2,
        nodeCount: 2,
        edgeCount: 1,
        reasonCodes: [],
        receiptId: 'receipt.builder',
        viewId: 'workflow_builder_validation.abc',
        checkedAt: '2026-04-30T12:00:00Z',
        message: 'ok',
      },
    );

    expect(inputs).toMatchObject({
      workflow_ref: 'workflow.billing',
      context_pack: {
        context_ref: 'workflow_context:billing',
      },
      builder_validation_view: {
        operator_view: {
          state: 'healthy',
        },
      },
    });
  });

  it('calls the CQRS composite view and summarizes deployability', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => ({
      ok: true,
      json: async () => ({
        result: {
          ok: true,
          state: 'partial',
          operator_view: {
            state: 'partial',
            view_id: 'workflow_context_composite.abc',
            payload: {
              deployability: {
                state: 'simulation_ready',
                can_build: true,
                can_simulate: true,
                can_promote: false,
              },
              buildability: { state: 'healthy' },
              synthetic_proof: { state: 'ready' },
              binding_coverage: { state: 'missing' },
              real_evidence: { state: 'missing' },
              confidence: { state: 'low', score: 0.42 },
              blockers: { hard_count: 0, soft_count: 1, review_decision_count: 0 },
              cost: { amount: '0.000000' },
              truth_state_classes: { synthetic: 2, inferred: 0 },
            },
          },
        },
        operation_receipt: { receipt_id: 'receipt.composite.1234567890' },
      }),
    })));

    const status = await fetchWorkflowContextCompositeStatus(
      payload,
      {
        context_ref: 'workflow_context:billing',
        workflow_ref: 'workflow.billing',
        context_mode: 'synthetic',
        truth_state: 'synthetic',
      },
      null,
      { scopeRef: 'scope.billing' },
    );
    const call = vi.mocked(fetch).mock.calls[0];

    expect(JSON.parse(String(call[1]?.body))).toMatchObject({
      operation: 'client_operating_model_operator_view',
      input: {
        view: 'workflow_context_composite',
        permission_scope: { scope_ref: 'scope.billing', visibility: 'full' },
      },
    });
    expect(status).toMatchObject({
      state: 'partial',
      deployabilityState: 'simulation_ready',
      buildabilityState: 'healthy',
      syntheticProofState: 'ready',
      bindingCoverageState: 'missing',
      realEvidenceState: 'missing',
      canBuild: true,
      canSimulate: true,
      canPromote: false,
      confidence: 'low 42%',
      blockerCount: 1,
      viewId: 'workflow_context_composite.abc',
    });
  });

  it('returns a machine-readable unavailable state for composite failures', () => {
    const status = statusFromCompositeError(new Error('composite down'));

    expect(status.state).toBe('unavailable');
    expect(status.deployabilityState).toBe('unavailable');
    expect(status.canPromote).toBe(false);
    expect(status.message).toBe('composite down');
  });
});
