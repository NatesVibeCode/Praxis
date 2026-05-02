import { describe, expect, it } from 'vitest';

import type { BuildPayload } from '../shared/types';
import type { DockContent } from './canvasBuildPresenter';
import { buildPrimitiveContractSuggestions } from './canvasContractSuggestions';

describe('buildPrimitiveContractSuggestions', () => {
  it('collects outputs and inputs from peer nodes and excludes current node', () => {
    const buildGraph: NonNullable<BuildPayload['build_graph']> = {
      nodes: [
        {
          node_id: 'a',
          kind: 'step',
          title: 'Upstream',
          outputs: ['user_id', 'thread_id'],
          required_inputs: ['query'],
        },
        {
          node_id: 'b',
          kind: 'step',
          title: 'Current',
          outputs: ['summary'],
        },
      ],
      edges: [],
    };

    const suggestions = buildPrimitiveContractSuggestions(buildGraph, 'b', []);

    const values = suggestions.map((s) => s.value).sort();
    expect(values).toContain('user_id');
    expect(values).toContain('thread_id');
    expect(values).toContain('query');
    // Current node also contributes its own contract tokens for cross-field picks.
    expect(values).toContain('summary');
  });

  it('adds object type property names', () => {
    const buildGraph: NonNullable<BuildPayload['build_graph']> = { nodes: [], edges: [] };
    const suggestions = buildPrimitiveContractSuggestions(buildGraph, null, [
      {
        type_id: 'ticket',
        name: 'Ticket',
        description: '',
        icon: '',
        fields: [{ name: 'priority', type: 'string' }],
      },
    ]);

    expect(suggestions.some((s) => s.value === 'priority')).toBe(true);
    expect(suggestions.find((s) => s.value === 'priority')?.detail).toMatch(/Ticket/);
  });

  it('adds dock attachment, binding, and import tokens', () => {
    const buildGraph: NonNullable<BuildPayload['build_graph']> = { nodes: [], edges: [] };
    const dock: DockContent = {
      contextAttachments: [
        {
          attachment_id: 'a1',
          node_id: 'n1',
          authority_kind: 'doc',
          authority_ref: 'ref/from/attachment',
          label: 'My doc',
        },
      ],
      connectBindings: [
        {
          binding_id: 'b1',
          source_label: 'user_email',
          candidate_targets: [{ target_ref: 'crm.contact.email', label: 'CRM Email' }],
          accepted_target: { target_ref: 'accepted.ref' },
        },
      ],
      imports: [
        {
          snapshot_id: 'i1',
          source_locator: 'https://example.com/api/v2/export.csv',
          requested_shape: { label: 'shape_label', target_ref: '#some_target' },
          admitted_targets: [{ target_ref: 'admitted.slot' }],
        },
      ],
    };

    const suggestions = buildPrimitiveContractSuggestions(buildGraph, null, [], dock);
    const values = new Set(suggestions.map((s) => s.value));

    expect(values.has('ref/from/attachment')).toBe(true);
    expect(values.has('My doc')).toBe(true);
    expect(values.has('user_email')).toBe(true);
    expect(values.has('crm.contact.email')).toBe(true);
    expect(values.has('CRM Email')).toBe(true);
    expect(values.has('accepted.ref')).toBe(true);
    expect(values.has('shape_label')).toBe(true);
    expect(values.has('some_target')).toBe(true);
    expect(values.has('admitted.slot')).toBe(true);
    expect(values.has('export.csv')).toBe(true);
  });

  it('mines integration_args, nested trigger filters, gate_rule, compiled spec, and scoped issues', () => {
    const buildGraph: NonNullable<BuildPayload['build_graph']> = {
      nodes: [
        {
          node_id: 'n1',
          kind: 'step',
          title: 'HTTP step',
          integration_args: {
            url: 'https://api.example.com',
            headers: { Authorization: 'bearer', 'X-Trace': '1' },
            metadata: { run_id: 'r1' },
          },
          trigger: {
            filter: { env: 'prod', nested: { deep_key: true } },
          },
        },
        {
          node_id: 'n2',
          kind: 'gate',
          title: 'Check',
          gate_rule: { max_attempts: 3 },
        },
      ],
      edges: [],
    };

    const suggestions = buildPrimitiveContractSuggestions(
      buildGraph,
      'n1',
      [],
      null,
      {
        compiledSpec: {
          jobs: [{ label: 'Compile me', agent: 'worker-a' }],
          triggers: [{ event_type: 'manual.run', source_ref: 'ref/trigger' }],
        },
        buildIssues: [
          { issue_id: 'i-x', node_id: 'n1', label: 'Fix binding', binding_id: 'b-99' },
          { issue_id: 'i-y', node_id: 'other', label: 'Ignore me' },
        ],
      },
    );
    const values = new Set(suggestions.map((s) => s.value));

    expect(values.has('url')).toBe(true);
    expect(values.has('Authorization')).toBe(true);
    expect(values.has('metadata.run_id')).toBe(true);
    expect(values.has('env')).toBe(true);
    expect(values.has('nested.deep_key')).toBe(true);
    expect(values.has('max_attempts')).toBe(true);
    expect(values.has('Compile me')).toBe(true);
    expect(values.has('worker-a')).toBe(true);
    expect(values.has('manual.run')).toBe(true);
    expect(values.has('ref/trigger')).toBe(true);
    expect(values.has('Fix binding')).toBe(true);
    expect(values.has('b-99')).toBe(true);
    expect(values.has('Ignore me')).toBe(false);
  });
});
