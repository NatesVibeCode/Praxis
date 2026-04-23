import { render, screen } from '@testing-library/react';
import { describe, expect, test, vi } from 'vitest';

import { MoonRunOverlay } from './MoonRunOverlay';
import type { RunDetail } from '../dashboard/useLiveRunSnapshot';

function makeRun(): RunDetail {
  return {
    run_id: 'workflow_contract',
    spec_name: 'Contract run',
    status: 'running',
    total_jobs: 1,
    completed_jobs: 1,
    total_cost: 0,
    created_at: null,
    finished_at: null,
    total_duration_ms: 1200,
    jobs: [
      {
        id: 1,
        label: 'enter_data',
        status: 'succeeded',
        job_type: 'workflow',
        phase: 'execute',
        agent_slug: 'openai/gpt-5.4',
        resolved_agent: 'openai/gpt-5.4',
        integration_id: null,
        integration_action: null,
        integration_args: null,
        attempt: 1,
        duration_ms: 1200,
        cost_usd: 0,
        exit_code: 0,
        last_error_code: null,
        stdout_preview: '',
        has_output: false,
        started_at: null,
        finished_at: null,
        created_at: null,
      },
    ],
    graph: {
      nodes: [
        {
          id: 'enter_data',
          label: 'enter_data',
          type: 'job',
          adapter: 'auto/build',
          position: 0,
          status: 'succeeded',
          task_type: 'data_entry',
          outcome_goal: 'CRM record is populated.',
          prompt: 'Enter the applicant data in the CRM tool.',
          completion_contract: {
            result_kind: 'artifact_bundle',
            submit_tool_names: ['praxis_submit_artifact_bundle'],
            submission_required: true,
            verification_required: false,
          },
        },
      ],
      edges: [],
    },
    health: null,
  };
}

describe('MoonRunOverlay', () => {
  test('shows completion gate contracts in the selected job receipt', () => {
    render(
      <MoonRunOverlay
        run={makeRun()}
        loading={false}
        error={null}
        selectedJobId="enter_data"
        onSelectJob={vi.fn()}
        onExit={vi.fn()}
      />,
    );

    expect(screen.getByLabelText('Job completion gate')).toBeInTheDocument();
    expect(screen.getByText('Submission required')).toBeInTheDocument();
    expect(screen.getByText('data_entry')).toBeInTheDocument();
    expect(screen.getByText('artifact_bundle')).toBeInTheDocument();
    expect(screen.getByText('praxis_submit_artifact_bundle')).toBeInTheDocument();
    expect(screen.getByText('CRM record is populated.')).toBeInTheDocument();
    expect(screen.getByText('Enter the applicant data in the CRM tool.')).toBeInTheDocument();
  });
});
