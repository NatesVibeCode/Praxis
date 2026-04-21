import { buildGraphToDefinition, resolveReleasePlanSource } from './buildGraphDefinition';

describe('buildGraphToDefinition', () => {
  it('preserves graph-authored phase metadata when synthesizing a definition', () => {
    const definition = buildGraphToDefinition({
      nodes: [
        {
          node_id: 'trigger-001',
          kind: 'step',
          title: 'Webhook trigger',
          route: 'trigger/webhook',
          trigger: {
            event_type: 'db.webhook_events.insert',
            source_ref: 'connector://webhook/support',
            filter: { topic: 'support' },
          },
        },
        {
          node_id: 'step-001',
          kind: 'step',
          title: 'Draft reply',
          summary: 'Prepare the first-pass response.',
          route: '@workflow/invoke',
          prompt: 'Draft a concise customer reply.',
          required_inputs: ['ticket_id', 'customer_tone'],
          outputs: ['draft_reply'],
          persistence_targets: ['crm.reply_drafts'],
          handoff_target: 'review-step',
          integration_args: {
            workflow_id: 'wf_followup',
            payload: { ticket_id: '{{ticket_id}}' },
          },
          source_block_ids: ['block-001'],
        },
      ],
      edges: [
        {
          edge_id: 'edge-trigger-step',
          kind: 'sequence',
          from_node_id: 'trigger-001',
          to_node_id: 'step-001',
        },
      ],
    });

    expect(definition).toMatchObject({
      trigger_intent: [
        {
          source_node_id: 'trigger-001',
          event_type: 'db.webhook_events.insert',
          source_ref: 'connector://webhook/support',
          filter: { topic: 'support' },
        },
      ],
      draft_flow: [
        {
          id: 'step-001',
          depends_on: [],
          source_block_ids: ['block-001'],
        },
      ],
      execution_setup: {
        phases: [
          {
            step_id: 'step-001',
            agent_route: '@workflow/invoke',
            system_prompt: 'Draft a concise customer reply.',
            required_inputs: ['ticket_id', 'customer_tone'],
            outputs: ['draft_reply'],
            persistence_targets: ['crm.reply_drafts'],
            handoff_target: 'review-step',
            integration_args: {
              workflow_id: 'wf_followup',
              payload: { ticket_id: '{{ticket_id}}' },
            },
          },
        ],
      },
    });
  });

  it('builds a stable release fingerprint independent of object key order', () => {
    const first = resolveReleasePlanSource({
      workflow: { id: 'wf_1', name: 'Alpha' },
      definition: {
        execution_setup: {
          phases: [{ route: 'auto/draft', prompt: 'Draft it' }],
        },
        trigger_intent: [{ event_type: 'manual' }],
      },
    } as any);
    const second = resolveReleasePlanSource({
      workflow: { id: 'wf_1', name: 'Alpha' },
      definition: {
        trigger_intent: [{ event_type: 'manual' }],
        execution_setup: {
          phases: [{ prompt: 'Draft it', route: 'auto/draft' }],
        },
      },
    } as any);

    expect(first?.fingerprint).toBe(second?.fingerprint);
    expect(first?.title).toBe('Alpha');
  });

  it('includes compiled spec projection in the release fingerprint', () => {
    const first = resolveReleasePlanSource({
      workflow: { id: 'wf_1', name: 'Alpha' },
      definition: {
        trigger_intent: [{ event_type: 'manual' }],
        execution_setup: {
          phases: [{ route: 'auto/draft', prompt: 'Draft it' }],
        },
      },
      compiled_spec_projection: {
        compiled_spec: {
          jobs: [{ label: 'Projected draft', agent: 'auto/draft' }],
          triggers: [{ event_type: 'manual' }],
        },
      },
    } as any);
    const second = resolveReleasePlanSource({
      workflow: { id: 'wf_1', name: 'Alpha' },
      definition: {
        trigger_intent: [{ event_type: 'manual' }],
        execution_setup: {
          phases: [{ route: 'auto/draft', prompt: 'Draft it' }],
        },
      },
      compiled_spec_projection: {
        compiled_spec: {
          jobs: [{ label: 'Projected reviewed draft', agent: 'auto/draft' }],
          triggers: [{ event_type: 'manual' }],
        },
      },
    } as any);

    expect(first?.fingerprint).not.toBe(second?.fingerprint);
  });

  it('uses build_graph as a first-class release source without projecting it locally', () => {
    const first = resolveReleasePlanSource({
      workflow: { id: 'wf_graph', name: 'Graph Alpha' },
      definition: {},
      build_graph: {
        nodes: [
          { node_id: 'trigger-001', kind: 'step', route: 'trigger', title: 'Manual' },
          { node_id: 'step-001', kind: 'step', route: '@notifications/send', title: 'Notify ops' },
        ],
        edges: [
          {
            edge_id: 'edge-1',
            kind: 'sequence',
            from_node_id: 'trigger-001',
            to_node_id: 'step-001',
          },
        ],
      },
    } as any);
    const second = resolveReleasePlanSource({
      workflow: { id: 'wf_graph', name: 'Graph Alpha' },
      definition: {},
      build_graph: {
        edges: [
          {
            to_node_id: 'step-001',
            kind: 'sequence',
            from_node_id: 'trigger-001',
            edge_id: 'edge-1',
          },
        ],
        nodes: [
          { title: 'Manual', route: 'trigger', kind: 'step', node_id: 'trigger-001' },
          { title: 'Notify ops', route: '@notifications/send', kind: 'step', node_id: 'step-001' },
        ],
      },
    } as any);

    expect(first?.buildGraph).toMatchObject({
      nodes: expect.arrayContaining([
        expect.objectContaining({ node_id: 'step-001', route: '@notifications/send' }),
      ]),
    });
    expect(first?.definition).toBeUndefined();
    expect(first?.fingerprint).toBe(second?.fingerprint);
  });

  it('projects canonical edge release metadata into execution_setup.edge_gates', () => {
    const condition = { field: 'should_continue', op: 'equals', value: true };
    const definition = buildGraphToDefinition({
      nodes: [
        {
          node_id: 'step-001',
          kind: 'step',
          title: 'Route step',
          route: 'auto/classify',
        },
        {
          node_id: 'step-002',
          kind: 'step',
          title: 'Then path',
          route: 'auto/draft',
        },
      ],
      edges: [
        {
          edge_id: 'edge-step-001-step-002',
          kind: 'conditional',
          from_node_id: 'step-001',
          to_node_id: 'step-002',
          release: {
            family: 'conditional',
            edge_type: 'conditional',
            label: 'Then',
            branch_reason: 'then',
            release_condition: condition,
            config: { condition, branch_side: 'above' },
          },
        },
      ],
    });

    expect(definition).toMatchObject({
      execution_setup: {
        edge_gates: [
          {
            edge_id: 'edge-step-001-step-002',
            release: {
              family: 'conditional',
              edge_type: 'conditional',
              label: 'Then',
              branch_reason: 'then',
              release_condition: condition,
              config: {
                condition,
                branch_side: 'above',
              },
            },
          },
        ],
      },
    });
  });

  it('projects validation edge gates with verify_refs only', () => {
    const definition = buildGraphToDefinition({
      nodes: [
        {
          node_id: 'step-001',
          kind: 'step',
          title: 'Build step',
          route: 'auto/build',
        },
        {
          node_id: 'step-002',
          kind: 'step',
          title: 'Then path',
          route: 'auto/review',
        },
      ],
      edges: [
        {
          edge_id: 'edge-step-001-step-002',
          kind: 'sequence',
          from_node_id: 'step-001',
          to_node_id: 'step-002',
          release: {
            family: 'validation',
            edge_type: 'validation',
            label: 'Validation',
            release_condition: { kind: 'always' },
            config: { verify_refs: ['verify_ref.python.py_compile.app'] },
          } as any,
        },
      ],
    });

    expect(definition).toMatchObject({
      execution_setup: {
        edge_gates: [
          {
            edge_id: 'edge-step-001-step-002',
            release: {
              family: 'validation',
              config: {
                verify_refs: ['verify_ref.python.py_compile.app'],
              },
            },
          },
        ],
      },
      draft_flow: [
        expect.any(Object),
        {
          id: 'step-002',
          gates: [
            {
              type: 'validation',
              verify_refs: ['verify_ref.python.py_compile.app'],
            },
          ],
        },
      ],
    });
    expect(JSON.stringify(definition)).not.toContain('verify_command');
  });
});
