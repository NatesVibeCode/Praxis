import { describe, expect, it } from 'vitest';

import {
  buildHttpRequestIntegrationArgs,
  buildNotificationIntegrationArgs,
  buildWorkflowInvokeIntegrationArgs,
  scaffoldMoonPrimitiveNode,
} from './moonPrimitives';

describe('moonPrimitives', () => {
  it('preserves notification metadata while updating visible fields', () => {
    const next = buildNotificationIntegrationArgs(
      {
        title: 'Old title',
        message: 'Old message',
        status: 'warning',
        metadata: { channel: 'ops', source_step_id: 'step-001' },
      },
      {
        title: 'Notify ops',
        message: 'Send the final run status.',
        status: 'info',
        fallbackTitle: 'Fallback title',
        fallbackMessage: 'Fallback message',
      },
    );

    expect(next).toEqual({
      title: 'Notify ops',
      message: 'Send the final run status.',
      status: 'info',
      metadata: { channel: 'ops', source_step_id: 'step-001' },
    });
  });

  it('preserves HTTP metadata and drops stale bodies for GET requests', () => {
    const next = buildHttpRequestIntegrationArgs(
      {
        request_preset: 'post_json',
        body: { stale: true },
        auth_strategy: { kind: 'bearer' },
        endpoint_map: [{ name: 'primary' }],
        connector_spec: { provider: 'webhook' },
      },
      {
        preset: 'fetch_json',
        url: 'https://api.example.com/items/42',
        method: 'GET',
        headers: { Accept: 'application/json' },
        body: { ignored: true },
        timeoutText: '',
      },
    );

    expect(next).toMatchObject({
      request_preset: 'fetch_json',
      url: 'https://api.example.com/items/42',
      endpoint: 'https://api.example.com/items/42',
      method: 'GET',
      headers: { Accept: 'application/json' },
      auth_strategy: { kind: 'bearer' },
      endpoint_map: [{ name: 'primary' }],
      connector_spec: { provider: 'webhook' },
    });
    expect(next).not.toHaveProperty('body');
    expect(next).not.toHaveProperty('body_template');
    expect(next).not.toHaveProperty('timeout');
  });

  it('preserves invoke metadata while clearing legacy input aliases', () => {
    const next = buildWorkflowInvokeIntegrationArgs(
      {
        retry_policy: 'once',
        input: { stale: true },
        inputs: { also: 'stale' },
        payload: { old: true },
      },
      {
        workflowId: 'wf_child',
        payload: undefined,
      },
    );

    expect(next).toEqual({
      retry_policy: 'once',
      workflow_id: 'wf_child',
      target_workflow_id: 'wf_child',
    });
  });

  it('scaffolds HTTP request nodes with saved defaults and outputs', () => {
    const node = scaffoldMoonPrimitiveNode(
      {
        node_id: 'step-001',
        kind: 'step',
        title: 'Call API',
        route: '@webhook/post',
      },
      {
        actionValue: '@webhook/post',
        title: 'Call API',
        summary: 'Send data to the downstream API.',
      },
    );

    expect(node.outputs).toEqual(['http_response']);
    expect(node.integration_args).toMatchObject({
      request_preset: 'post_json',
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
    });
  });
});
