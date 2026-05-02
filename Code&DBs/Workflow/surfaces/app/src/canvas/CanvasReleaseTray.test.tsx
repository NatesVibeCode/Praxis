import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import type { ReleaseStatus } from './canvasBuildPresenter';
import { CanvasReleaseTray } from './CanvasReleaseTray';
import type { BuildPayload } from '../shared/types';
import { planDefinition } from '../shared/buildController';

const buildControllerMocks = vi.hoisted(() => ({
  planDefinition: vi.fn(),
  commitDefinition: vi.fn(),
  triggerWorkflow: vi.fn(),
  createWorkflow: vi.fn(),
}));

vi.mock('../shared/buildController', () => buildControllerMocks);

const release: ReleaseStatus = {
  readiness: 'draft',
  blockers: [],
  projectedJobs: [],
  checklist: [],
};

interface BuildPayloadOptions {
  projectedJobLabel?: string;
  triggers?: Array<{ event_type?: string; source_ref?: string }>;
}

function buildPayload(prompt: string, options: BuildPayloadOptions = {}): BuildPayload {
  const triggers = options.triggers || [{ event_type: 'manual' }];
  return {
    workflow: { id: 'wf_alpha', name: 'Alpha' },
    definition: {
      trigger_intent: [{ event_type: 'manual' }],
      execution_setup: {
        phases: [
          {
            step_id: 'step-001',
            agent_route: 'auto/draft',
            prompt,
          },
        ],
      },
    },
    materialized_spec_projection: {
      materialized_spec: {
        jobs: [{ label: options.projectedJobLabel || 'Projected draft', agent: 'auto/draft' }],
        triggers,
      },
    },
  };
}

function buildGraphPayload(): BuildPayload {
  return {
    workflow: { id: 'wf_graph', name: 'Graph Alpha' },
    definition: {},
    build_graph: {
      nodes: [
        {
          node_id: 'trigger-001',
          kind: 'step',
          title: 'Manual',
          route: 'trigger',
          trigger: { event_type: 'manual', filter: {} },
        },
        {
          node_id: 'step-001',
          kind: 'step',
          title: 'Fetch status',
          route: '@webhook/post',
          integration_args: {
            request_preset: 'fetch_json',
            url: 'https://api.example.com/status',
            method: 'GET',
            headers: { Accept: 'application/json' },
          },
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
    },
    materialized_spec_projection: {
      materialized_spec: {
        jobs: [{ label: 'Projected fetch', agent: 'integration/webhook/post' }],
        triggers: [{ event_type: 'manual' }],
      },
    },
  };
}

describe('CanvasReleaseTray', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    buildControllerMocks.planDefinition.mockResolvedValue({
      materialized_spec: {
        jobs: [{ label: 'Planned draft', agent: 'auto/draft' }],
      },
    });
    buildControllerMocks.createWorkflow.mockResolvedValue({ id: 'wf_created' });
    buildControllerMocks.commitDefinition.mockResolvedValue({});
    buildControllerMocks.triggerWorkflow.mockResolvedValue({ run_id: 'run_123', status: 'queued' });
  });

  test('invalidates a previewed plan immediately when the workflow definition changes', async () => {
    const { rerender } = render(
      <CanvasReleaseTray
        release={release}
        payload={buildPayload('Draft the initial response.')}
        workflowId="wf_alpha"
        onClose={() => undefined}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Preview plan' }));

    await screen.findAllByText('Planned draft');
    expect(screen.getByRole('button', { name: 'Dispatch' })).toBeEnabled();

    rerender(
      <CanvasReleaseTray
        release={release}
        payload={buildPayload('Draft the updated response with routing notes.')}
        workflowId="wf_alpha"
        onClose={() => undefined}
      />,
    );

    await waitFor(() => {
      expect(screen.getByText('The workflow changed after preview. Preview plan again before dispatch.')).toBeInTheDocument();
    });

    expect(screen.getByRole('button', { name: 'Preview plan' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Dispatch' })).toBeDisabled();
    expect(planDefinition).toHaveBeenCalledTimes(1);
  });

  test('invalidates a previewed plan when only the compiled projection changes', async () => {
    const { rerender } = render(
      <CanvasReleaseTray
        release={release}
        payload={buildPayload('Draft the initial response.', { projectedJobLabel: 'Projected draft' })}
        workflowId="wf_alpha"
        onClose={() => undefined}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Preview plan' }));

    await screen.findAllByText('Planned draft');
    expect(screen.getByRole('button', { name: 'Dispatch' })).toBeEnabled();

    rerender(
      <CanvasReleaseTray
        release={release}
        payload={buildPayload('Draft the initial response.', { projectedJobLabel: 'Projected reviewed draft' })}
        workflowId="wf_alpha"
        onClose={() => undefined}
      />,
    );

    await waitFor(() => {
      expect(screen.getByText('The workflow changed after preview. Preview plan again before dispatch.')).toBeInTheDocument();
    });

    expect(screen.getByRole('button', { name: 'Preview plan' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Dispatch' })).toBeDisabled();
    expect(planDefinition).toHaveBeenCalledTimes(1);
  });

  test('summarizes every projected trigger intent instead of only the first', () => {
    render(
      <CanvasReleaseTray
        release={release}
        payload={buildPayload('Draft the initial response.', {
          triggers: [
            { event_type: 'manual' },
            { event_type: 'schedule' },
            { source_ref: 'webhook.customer.created' },
          ],
        })}
        workflowId="wf_alpha"
        onClose={() => undefined}
      />,
    );

    expect(screen.getByText(/triggered by manual \+ schedule \+ webhook\.customer\.created/)).toBeInTheDocument();
  });

  test('plans and dispatches graph-backed workflows through the canonical build_graph path', async () => {
    buildControllerMocks.planDefinition.mockResolvedValue({
      workflow: { id: 'wf_graph', name: 'Graph Alpha' },
      materialized_spec: {
        jobs: [{ label: 'Planned fetch', agent: 'integration/webhook/post' }],
      },
    });

    render(
      <CanvasReleaseTray
        release={release}
        payload={buildGraphPayload()}
        workflowId={null}
        onClose={() => undefined}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Preview plan' }));

    await waitFor(() => {
      expect(buildControllerMocks.planDefinition).toHaveBeenCalledTimes(1);
    });
    expect(buildControllerMocks.planDefinition).toHaveBeenCalledWith(expect.objectContaining({
      workflowId: 'wf_graph',
      title: 'Graph Alpha',
      buildGraph: buildGraphPayload().build_graph,
    }));

    fireEvent.click(screen.getByRole('button', { name: 'Dispatch' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Confirm Release' }));

    await waitFor(() => {
      expect(buildControllerMocks.commitDefinition).toHaveBeenCalledTimes(1);
    });
    expect(buildControllerMocks.createWorkflow).not.toHaveBeenCalled();
    expect(buildControllerMocks.commitDefinition).toHaveBeenCalledWith(
      'wf_graph',
      expect.objectContaining({
        title: 'Graph Alpha',
        buildGraph: buildGraphPayload().build_graph,
      }),
    );
    expect(buildControllerMocks.triggerWorkflow).toHaveBeenCalledWith('wf_graph');
  });
});
