import { buildGraphToDefinition } from './buildGraphDefinition';

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
          route: 'auto/draft',
          prompt: 'Draft a concise customer reply.',
          required_inputs: ['ticket_id', 'customer_tone'],
          outputs: ['draft_reply'],
          persistence_targets: ['crm.reply_drafts'],
          handoff_target: 'review-step',
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
            agent_route: 'auto/draft',
            system_prompt: 'Draft a concise customer reply.',
            required_inputs: ['ticket_id', 'customer_tone'],
            outputs: ['draft_reply'],
            persistence_targets: ['crm.reply_drafts'],
            handoff_target: 'review-step',
          },
        ],
      },
    });
  });
});
