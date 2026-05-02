import '@testing-library/jest-dom';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { Dashboard } from './Dashboard';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

describe('Dashboard', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  test('renders backend-authored dashboard snapshot data instead of client-side summary probes', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/dashboard') {
        return jsonResponse({
          generated_at: '2026-04-14T12:00:00+00:00',
          summary: {
            workflow_counts: {
              total: 3,
              live: 1,
              saved: 1,
              draft: 1,
            },
            health: {
              readiness: 'healthy',
              label: 'Healthy',
              tone: 'healthy',
              copy: 'Recent workflow outcomes are strong and the control plane looks settled.',
            },
            runs_24h: 9,
            active_runs: 2,
            pass_rate_24h: 0.92,
            total_cost_24h: 14.25,
            top_agent: 'openai/gpt-5.4',
            models_online: 2,
            queue: {
              depth: 3,
              status: 'ok',
              utilization_pct: 0.3,
              pending: 2,
              ready: 1,
              claimed: 0,
              running: 0,
              error: null,
            },
          },
          sections: [
            { key: 'live', count: 1, workflow_ids: ['wf_live'] },
            { key: 'saved', count: 1, workflow_ids: ['wf_saved'] },
            { key: 'draft', count: 1, workflow_ids: ['wf_draft'] },
          ],
          workflows: [
            {
              id: 'wf_live',
              name: 'Support Intake',
              description: 'Handle inbound support requests.',
              definition_type: 'operating_model',
              invocation_count: 12,
              last_invoked_at: '2026-04-14T11:45:00+00:00',
              dashboard_bucket: 'live',
              dashboard_badge: {
                label: 'Scheduled',
                tone: 'scheduled',
                class_name: 'wf-card__badge--scheduled',
              },
              trigger: {
                id: 'trigger_live',
                event_type: 'schedule',
                enabled: true,
                cron_expression: '@hourly',
                last_fired_at: '2026-04-14T11:00:00+00:00',
                fire_count: 8,
              },
            },
            {
              id: 'wf_saved',
              name: 'Daily Report',
              description: 'Generate the daily report.',
              invocation_count: 3,
              dashboard_bucket: 'saved',
              dashboard_badge: {
                label: 'Validated',
                tone: 'validated',
                class_name: 'wf-card__badge--validated',
              },
            },
            {
              id: 'wf_draft',
              name: 'Unlaunched Draft',
              description: 'A draft workflow.',
              invocation_count: 0,
              dashboard_bucket: 'draft',
              dashboard_badge: {
                label: 'Draft',
                tone: 'draft',
                class_name: 'wf-card__badge--draft',
              },
            },
          ],
          recent_runs: [
            {
              run_id: 'run_live',
              spec_name: 'Support Intake',
              status: 'running',
              total_jobs: 4,
              completed_jobs: 2,
              total_cost: 3.5,
              created_at: '2026-04-14T11:50:00+00:00',
              finished_at: null,
            },
          ],
          leaderboard: [],
        });
      }
      if (url === '/api/files?scope=instance') {
        return jsonResponse({
          files: [
            { id: 'file_1', filename: 'brief.md' },
            { id: 'file_2', filename: 'notes.txt' },
          ],
        });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const onDescribe = vi.fn();

    render(
      <Dashboard
        onEditWorkflow={() => undefined}
        onEditModel={() => undefined}
        onViewRun={() => undefined}
        onNewWorkflow={() => undefined}
        onChat={() => undefined}
        onDescribe={onDescribe}
        onOpenCosts={() => undefined}
      />,
    );

    await screen.findByText(/3 workflows/);
    expect(screen.getByText('1 live - 1 saved - 1 draft')).toBeInTheDocument();
    expect(screen.getAllByText('Healthy').length).toBeGreaterThan(0);
    expect(screen.getByText(/\$14\.25/)).toBeInTheDocument();
    expect(screen.getByText('Toolbelt Review')).toBeInTheDocument();
    expect(screen.getByText('Execution queue')).toBeInTheDocument();
    expect(screen.queryByText('workflow_contract · tec_workflow_lane')).not.toBeInTheDocument();
    expect(screen.queryByText('$ praxis workflow query overview')).not.toBeInTheDocument();

    await waitFor(() => {
      const urls = fetchMock.mock.calls.map(([url]) => String(url));
      expect(urls).toContain('/api/dashboard');
      expect(urls).toContain('/api/files?scope=instance');
      expect(urls).not.toContain('/api/workflows');
      expect(urls).not.toContain('/api/status');
      expect(urls).not.toContain('/api/leaderboard');
    });
  });

  test('surfaces instance file inventory failure instead of silently rendering zero files', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/dashboard') {
        return jsonResponse({
          generated_at: '2026-04-14T12:00:00+00:00',
          summary: {
            workflow_counts: { total: 0, live: 0, saved: 0, draft: 0 },
            health: {
              readiness: 'healthy',
              label: 'Healthy',
              tone: 'healthy',
              copy: 'Ready.',
            },
            runs_24h: 0,
            active_runs: 0,
            pass_rate_24h: null,
            total_cost_24h: 0,
            top_agent: null,
            models_online: 0,
            queue: {
              depth: 0,
              status: 'ok',
              utilization_pct: 0,
              pending: 0,
              ready: 0,
              claimed: 0,
              running: 0,
              error: null,
            },
          },
          sections: [
            { key: 'live', count: 0, workflow_ids: [] },
            { key: 'saved', count: 0, workflow_ids: [] },
            { key: 'draft', count: 0, workflow_ids: [] },
          ],
          workflows: [],
          recent_runs: [],
          tool_opportunities: [],
        });
      }
      if (url === '/api/files?scope=instance') {
        return jsonResponse({ error: 'bad scope' }, 400);
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <Dashboard
        onEditWorkflow={() => undefined}
        onEditModel={() => undefined}
        onViewRun={() => undefined}
        onNewWorkflow={() => undefined}
        onChat={() => undefined}
        onDescribe={() => undefined}
        onOpenCosts={() => undefined}
      />,
    );

    expect(await screen.findByText('File inventory unavailable')).toBeInTheDocument();
    expect(screen.getAllByText('File inventory returned 400').length).toBeGreaterThan(0);
    expect(screen.queryByText('No files attached')).not.toBeInTheDocument();
  });

  test('routes tool opportunities to the workflow composer instead of rendering them as static decoration', async () => {
    const onDescribe = vi.fn();
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/dashboard') {
        return jsonResponse({
          generated_at: '2026-04-14T12:00:00+00:00',
          summary: {
            workflow_counts: { total: 0, live: 0, saved: 0, draft: 0 },
            health: {
              readiness: 'healthy',
              label: 'Healthy',
              tone: 'healthy',
              copy: 'Ready.',
            },
            runs_24h: 0,
            active_runs: 0,
            pass_rate_24h: null,
            total_cost_24h: 0,
            top_agent: null,
            models_online: 0,
            queue: {
              depth: 0,
              status: 'ok',
              utilization_pct: 0,
              pending: 0,
              ready: 0,
              claimed: 0,
              running: 0,
              error: null,
            },
          },
          sections: [
            { key: 'live', count: 0, workflow_ids: [] },
            { key: 'saved', count: 0, workflow_ids: [] },
            { key: 'draft', count: 0, workflow_ids: [] },
          ],
          workflows: [],
          recent_runs: [],
          tool_opportunities: [
            {
              shape_hash: 'abc123toolopportunity',
              decision_key: 'decision.example',
              occurrence_count: 3,
              distinct_surfaces: 2,
              action_kinds: ['gateway_op'],
              operation_names: ['workflow.run'],
              sample_commands: [],
              sample_path_shapes: [],
              last_seen: '2026-04-14T11:50:00+00:00',
            },
          ],
        });
      }
      if (url === '/api/files?scope=instance') {
        return jsonResponse({ files: [] });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <Dashboard
        onEditWorkflow={() => undefined}
        onEditModel={() => undefined}
        onViewRun={() => undefined}
        onNewWorkflow={() => undefined}
        onChat={() => undefined}
        onDescribe={onDescribe}
        onOpenCosts={() => undefined}
      />,
    );

    // Tool Opportunity rows render via StatusRow primitive; the gateway-op label
    // for 'workflow.run' humanizes to 'Run'. Click the row.
    const opportunityRow = await screen.findByRole('button', { name: /Run/ });
    fireEvent.click(opportunityRow);
    expect(onDescribe).toHaveBeenCalled();
  });
});
