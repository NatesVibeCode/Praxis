import { describe, expect, test, vi } from 'vitest';

import type { BuildPayload } from '../shared/types';
import {
  fetchWorkflowContextAuthority,
  workflowContextFromReadResponse,
  workflowContextNodeSummary,
  workflowContextReadInputFromPayload,
  workflowContextSummaryFromPayload,
} from './workflowContext';

describe('workflowContextSummaryFromPayload', () => {
  test('normalizes a Workflow Context authority packet for inspector display', () => {
    const payload: BuildPayload = {
      workflow: { id: 'wf_1', name: 'Renewal risk' },
      definition: {},
      workflow_context: {
        context_ref: 'workflow_context:renewal-risk:abcdef1234567890abcdef',
        context_mode: 'synthetic',
        truth_state: 'synthetic',
        confidence: { score: 0.28, state: 'low' },
        entities: [
          { entity_kind: 'object', label: 'Account', io_mode: 'synthetic' },
          { entity_kind: 'object', label: 'Subscription', io_mode: 'synthetic' },
          { entity_kind: 'system', label: 'CRM', io_mode: 'synthetic' },
        ],
        blockers: [
          { severity: 'soft', reason_code: 'workflow_context.stale_import_possible' },
        ],
        verifier_expectations: [
          { verifier_ref: 'verifier.workflow_context.renewal_risk.risk_score_explained' },
        ],
        guardrail: {
          safe_next_llm_actions: ['run_synthetic_simulation', 'generate_review_packet'],
        },
        synthetic_world: {
          virtual_lab: {
            environment_revision: {
              revision_id: 'virtual_lab_revision.workflow_context.abc123',
            },
            simulation_run_payload: { scenario: {} },
          },
        },
      },
    };

    const summary = workflowContextSummaryFromPayload(payload);

    expect(summary).toMatchObject({
      mode: 'synthetic',
      pill: 'synthetic',
      confidence: 'low 28%',
      objectLabels: ['Account', 'Subscription'],
      blockerCount: 1,
      hardBlockerCount: 0,
      verifierCount: 1,
      ioModes: ['synthetic'],
      nextActions: ['run_synthetic_simulation', 'generate_review_packet'],
      simulationStatus: 'ready',
      virtualLabRevision: 'virtual_lab_revision.workflow_context.abc123',
    });
  });

  test('returns null when no Workflow Context authority packet exists', () => {
    expect(workflowContextSummaryFromPayload({ definition: {} })).toBeNull();
  });

  test('builds selected-node context from Workflow Context entities', () => {
    const summary = workflowContextNodeSummary({
      context_ref: 'workflow_context:node:abcdef',
      context_mode: 'synthetic',
      truth_state: 'synthetic',
      entities: [
        {
          entity_kind: 'workflow_node',
          label: 'Score risk',
          truth_state: 'synthetic',
          io_mode: 'runtime_generated',
          payload: { node_id: 'node-risk' },
        },
        { entity_kind: 'object', label: 'Account', io_mode: 'synthetic' },
      ],
      blockers: [],
      verifier_expectations: [
        { verifier_ref: 'verifier.workflow_context.renewal_risk.risk_score_explained' },
      ],
      guardrail: {
        safe_next_llm_actions: ['run_synthetic_simulation'],
      },
    }, 'node-risk');

    expect(summary).toMatchObject({
      pill: 'synthetic',
      ioMode: 'runtime_generated',
      objectLabels: ['Account'],
      blockerCount: 0,
      verifierCount: 1,
      nextAction: 'run_synthetic_simulation',
    });
  });

  test('reads Workflow Context from unified operate gateway envelopes', () => {
    const context = workflowContextFromReadResponse({
      ok: true,
      result: {
        count: 1,
        context_packs: [
          {
            context_ref: 'workflow_context:live:abc123',
            context_mode: 'synthetic',
            truth_state: 'synthetic',
            entities: [{ entity_kind: 'object', label: 'Account' }],
          },
        ],
      },
    });

    expect(context).toMatchObject({
      context_ref: 'workflow_context:live:abc123',
      entities: [{ entity_kind: 'object', label: 'Account' }],
    });
  });

  test('prefers exact context refs when building read input', () => {
    const payload: BuildPayload = {
      workflow: { id: 'workflow.renewal', name: 'Renewal' },
      definition: {},
      workflow_context: {
        context_ref: 'workflow_context:renewal:abc123',
        workflow_ref: 'workflow.renewal',
      },
    };

    expect(workflowContextReadInputFromPayload(payload)).toEqual({
      context_ref: 'workflow_context:renewal:abc123',
      include_entities: true,
      include_bindings: true,
      include_transitions: false,
      limit: 1,
    });
  });

  test('fetches Workflow Context through the operate gateway', async () => {
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      const request = JSON.parse(String(init?.body || '{}'));
      expect(request).toMatchObject({
        operation: 'workflow_context_read',
        input: {
          workflow_ref: 'workflow.renewal',
          include_entities: true,
          include_bindings: true,
          include_transitions: false,
          limit: 1,
        },
      });
      return new Response(JSON.stringify({
        ok: true,
        result: {
          context_packs: [
            {
              context_ref: 'workflow_context:renewal:live',
              context_mode: 'synthetic',
              truth_state: 'synthetic',
            },
          ],
        },
      }), { status: 200 });
    });

    const context = await fetchWorkflowContextAuthority({
      workflow: { id: 'workflow.renewal', name: 'Renewal' },
      definition: {},
    }, { fetchImpl: fetchImpl as unknown as typeof fetch });

    expect(context?.context_ref).toBe('workflow_context:renewal:live');
    expect(fetchImpl).toHaveBeenCalledWith('/api/operate', expect.objectContaining({
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    }));
  });
});
