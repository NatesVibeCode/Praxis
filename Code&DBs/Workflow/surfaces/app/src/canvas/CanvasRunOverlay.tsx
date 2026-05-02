import React, { useEffect, useState } from 'react';
import type { RunDetail, RunGraphNode, RunJob } from '../dashboard/useLiveRunSnapshot';
import { CanvasStatusRing } from './CanvasStatusRing';
import { statusState, type CanvasStatusState } from './canvasStatus';

interface Props {
  run: RunDetail | null;
  loading: boolean;
  error: string | null;
  selectedJobId: string | null;
  onSelectJob: (jobId: string | null) => void;
  onExit: () => void;
  onCancel?: () => void;
  onEditWorkflow?: (workflowId: string) => void;
}

const TERMINAL_JOB_STATUSES = new Set<RunJob['status']>([
  'succeeded',
  'failed',
  'dead_letter',
  'blocked',
  'cancelled',
  'parent_failed',
]);
const FAILED_JOB_STATUSES = new Set<RunJob['status']>([
  'failed',
  'dead_letter',
  'blocked',
  'cancelled',
  'parent_failed',
]);
const ACTIVE_JOB_STATUSES = new Set<RunJob['status']>(['claimed', 'running']);
const WAITING_JOB_STATUSES = new Set<RunJob['status']>(['pending', 'ready']);
const TERMINAL_RUN_STATUSES = new Set<string>(['succeeded', 'failed', 'cancelled']);

function formatDuration(ms: number): string {
  if (!ms || ms < 0) return '—';
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  const r = Math.round(s - m * 60);
  return `${m}m ${r}s`;
}

function formatCost(usd: number): string {
  if (!usd || usd <= 0) return '$0';
  return usd < 0.01 ? `$${usd.toFixed(4)}` : `$${usd.toFixed(2)}`;
}

function formatRawStatus(status: string): string {
  return status.replace(/[_-]+/g, ' ');
}

function jobSelectionId(job: RunJob): string {
  return job.label || String(job.id);
}

function compactStringList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
    : [];
}

function hasRunGraphContract(node: RunGraphNode | null): boolean {
  return Boolean(
    node
    && (
      node.completion_contract
      || node.task_type
      || node.description
      || node.outcome_goal
      || node.prompt
    ),
  );
}

export function CanvasRunOverlay({
  run,
  loading,
  error,
  selectedJobId,
  onSelectJob,
  onExit,
  onCancel,
  onEditWorkflow,
}: Props) {
  const [expandedOutput, setExpandedOutput] = useState(false);

  useEffect(() => {
    setExpandedOutput(false);
  }, [selectedJobId]);

  const jobs = run?.jobs ?? [];
  const selectedGraphNode = selectedJobId && run?.graph?.nodes
    ? run.graph.nodes.find((node) => node.id === selectedJobId) ?? null
    : null;
  const selectedJob: RunJob | null = selectedJobId
    ? (
        jobs.find((j) =>
          j.label === selectedJobId
          || String(j.id) === selectedJobId
          || (selectedGraphNode ? j.label === selectedGraphNode.label : false),
        ) ?? null
      )
    : null;
  const selectedCompletionContract = selectedGraphNode?.completion_contract ?? null;
  const selectedContractResultKind = typeof selectedCompletionContract?.result_kind === 'string'
    ? selectedCompletionContract.result_kind.trim()
    : '';
  const selectedContractSubmitTools = compactStringList(selectedCompletionContract?.submit_tool_names);
  const selectedHasContract = hasRunGraphContract(selectedGraphNode);

  const runStatusRaw = run?.status ?? (loading ? 'loading' : 'queued');
  const runState: CanvasStatusState = statusState(runStatusRaw);
  const isTerminal = TERMINAL_RUN_STATUSES.has(runStatusRaw);
  const editableWorkflowId = run?.workflow_id ?? null;
  const canEditWorkflow = Boolean(editableWorkflowId && onEditWorkflow);

  const totalJobs = jobs.length || run?.total_jobs || 0;
  const completedJobs = jobs.length
    ? jobs.filter((j) => TERMINAL_JOB_STATUSES.has(j.status)).length
    : run?.completed_jobs ?? 0;
  const failedJobs = jobs.filter((j) => FAILED_JOB_STATUSES.has(j.status)).length;
  const activeJobs = jobs.filter((j) => ACTIVE_JOB_STATUSES.has(j.status)).length;
  const waitingJobs = jobs.filter((j) => WAITING_JOB_STATUSES.has(j.status)).length;
  const totalCost = jobs.length
    ? jobs.reduce((acc, j) => acc + (j.cost_usd || 0), 0)
    : run?.total_cost ?? 0;
  const totalDuration = run?.total_duration_ms ?? jobs.reduce((acc, j) => acc + (j.duration_ms || 0), 0);

  return (
    <>
      <aside className="canvas-run-overlay__summary" aria-label="Run ledger">
        <header className="canvas-run-overlay__summary-head">
          <button
            type="button"
            className="canvas-run-overlay__summary-back"
            onClick={onExit}
            aria-label="Exit run view"
          >
            ←
          </button>
          <div>
            <div className="canvas-run-overlay__summary-kicker">Run</div>
            <div className="canvas-run-overlay__summary-title">{run?.spec_name ?? 'Loading run'}</div>
            {run?.run_id && (
              <div className="canvas-run-overlay__summary-run-id">{run.run_id}</div>
            )}
          </div>
          <span className={`canvas-run-overlay__summary-pill canvas-run-overlay__summary-pill--${runState}`}>
            <CanvasStatusRing status={runState} size={10} label={formatRawStatus(runStatusRaw)} />
            <span>{formatRawStatus(runStatusRaw)}</span>
          </span>
        </header>

        {(canEditWorkflow || (!isTerminal && onCancel)) && (
          <div className="canvas-run-overlay__summary-actions">
            {canEditWorkflow && editableWorkflowId && (
              <button
                type="button"
                className="canvas-run-overlay__summary-action"
                onClick={() => onEditWorkflow?.(editableWorkflowId)}
              >
                Edit source workflow
              </button>
            )}
            {!isTerminal && onCancel && (
              <button
                type="button"
                className="canvas-run-overlay__summary-cancel"
                onClick={onCancel}
              >
                Cancel
              </button>
            )}
          </div>
        )}

        <dl className="canvas-run-overlay__stat-grid">
          <div>
            <dt>Jobs</dt>
            <dd>{completedJobs}/{totalJobs}</dd>
          </div>
          <div>
            <dt>Failed</dt>
            <dd>{failedJobs}</dd>
          </div>
          <div>
            <dt>Active</dt>
            <dd>{activeJobs}</dd>
          </div>
          <div>
            <dt>Waiting</dt>
            <dd>{waitingJobs}</dd>
          </div>
          <div>
            <dt>Time</dt>
            <dd>{formatDuration(totalDuration)}</dd>
          </div>
          <div>
            <dt>Cost</dt>
            <dd>{formatCost(totalCost)}</dd>
          </div>
        </dl>

        {loading && !run && (
          <div className="canvas-run-overlay__summary-empty">Loading run…</div>
        )}
        {error && (
          <div className="canvas-run-overlay__summary-error">{error}</div>
        )}

        {jobs.length > 0 ? (
          <div className="canvas-run-overlay__job-list">
            {jobs.map((job) => {
              const selected = selectedJob?.id === job.id || selectedJobId === job.label || selectedJobId === String(job.id);
              return (
                <button
                  key={job.id}
                  type="button"
                  className={`canvas-run-overlay__job-row${selected ? ' canvas-run-overlay__job-row--selected' : ''}`}
                  onClick={() => onSelectJob(selected ? null : jobSelectionId(job))}
                  aria-pressed={selected}
                >
                  <CanvasStatusRing status={job.status} size={14} halo={selected} />
                  <span className="canvas-run-overlay__job-label">{job.label}</span>
                  <span className="canvas-run-overlay__job-status">{job.status}</span>
                  {(job.duration_ms > 0 || job.last_error_code) && (
                    <span className="canvas-run-overlay__job-meta">
                      {job.duration_ms > 0 && (
                        <span className="canvas-run-overlay__job-metric">{formatDuration(job.duration_ms)}</span>
                      )}
                      {job.last_error_code && (
                        <span className="canvas-run-overlay__job-error">{job.last_error_code}</span>
                      )}
                    </span>
                  )}
                </button>
              );
            })}
          </div>
        ) : !loading && (
          <div className="canvas-run-overlay__summary-empty">No jobs recorded.</div>
        )}
      </aside>

      {selectedJob && (
        <aside className="canvas-run-overlay__receipt" aria-label="Job receipt">
          <header className="canvas-run-overlay__receipt-head">
            <CanvasStatusRing status={selectedJob.status} size={14} />
            <span className="canvas-run-overlay__receipt-label">{selectedJob.label}</span>
            <button
              type="button"
              className="canvas-run-overlay__receipt-close"
              onClick={() => onSelectJob(null)}
              aria-label="Close receipt"
            >
              ×
            </button>
          </header>
          <dl className="canvas-run-overlay__receipt-meta">
            <div>
              <dt>Status</dt>
              <dd>{selectedJob.status}</dd>
            </div>
            {selectedJob.resolved_agent && (
              <div>
                <dt>Agent</dt>
                <dd>{selectedJob.resolved_agent}</dd>
              </div>
            )}
            {selectedJob.duration_ms > 0 && (
              <div>
                <dt>Duration</dt>
                <dd>{formatDuration(selectedJob.duration_ms)}</dd>
              </div>
            )}
            {selectedJob.cost_usd > 0 && (
              <div>
                <dt>Cost</dt>
                <dd>{formatCost(selectedJob.cost_usd)}</dd>
              </div>
            )}
            {selectedJob.attempt > 1 && (
              <div>
                <dt>Attempt</dt>
                <dd>{selectedJob.attempt}</dd>
              </div>
            )}
            {selectedJob.last_error_code && (
              <div>
                <dt>Error</dt>
                <dd>{selectedJob.last_error_code}</dd>
              </div>
            )}
          </dl>
          {selectedHasContract && selectedGraphNode && (
            <section className="canvas-run-overlay__contract" aria-label="Job completion gate">
              <div className="canvas-run-overlay__contract-head">
                <span>Completion gate</span>
                <span className="canvas-run-overlay__contract-badges">
                  {selectedCompletionContract?.submission_required === true && (
                    <span className="canvas-truth-badge canvas-truth-badge--runtime">Submission required</span>
                  )}
                  {selectedCompletionContract?.verification_required === true && (
                    <span className="canvas-truth-badge canvas-truth-badge--alias">Verification required</span>
                  )}
                  {selectedCompletionContract && selectedCompletionContract.submission_required !== true && selectedCompletionContract.verification_required !== true && (
                    <span className="canvas-truth-badge canvas-truth-badge--persisted">Recorded contract</span>
                  )}
                </span>
              </div>
              <dl className="canvas-run-overlay__contract-grid">
                {selectedGraphNode.task_type && (
                  <div>
                    <dt>Task type</dt>
                    <dd>{selectedGraphNode.task_type}</dd>
                  </div>
                )}
                {selectedContractResultKind && (
                  <div>
                    <dt>Result</dt>
                    <dd>{selectedContractResultKind}</dd>
                  </div>
                )}
                {selectedContractSubmitTools.length > 0 && (
                  <div>
                    <dt>Submit with</dt>
                    <dd>{selectedContractSubmitTools.join(', ')}</dd>
                  </div>
                )}
                {selectedGraphNode.outcome_goal && (
                  <div>
                    <dt>Outcome</dt>
                    <dd>{selectedGraphNode.outcome_goal}</dd>
                  </div>
                )}
                {selectedGraphNode.description && (
                  <div>
                    <dt>Scope</dt>
                    <dd>{selectedGraphNode.description}</dd>
                  </div>
                )}
              </dl>
              {selectedGraphNode.prompt && (
                <details className="canvas-run-overlay__contract-prompt">
                  <summary>Prompt</summary>
                  <pre>{selectedGraphNode.prompt}</pre>
                </details>
              )}
            </section>
          )}
          {selectedJob.stdout_preview && (
            <div className="canvas-run-overlay__receipt-output">
              <div className="canvas-run-overlay__receipt-output-head">
                <span>Output preview</span>
                {selectedJob.has_output && (
                  <button
                    type="button"
                    className="canvas-run-overlay__receipt-output-toggle"
                    onClick={() => setExpandedOutput((v) => !v)}
                  >
                    {expandedOutput ? 'Collapse' : 'Expand'}
                  </button>
                )}
              </div>
              <pre className={`canvas-run-overlay__receipt-pre${expandedOutput ? ' canvas-run-overlay__receipt-pre--expanded' : ''}`}>
                {selectedJob.stdout_preview}
              </pre>
            </div>
          )}
        </aside>
      )}
    </>
  );
}
