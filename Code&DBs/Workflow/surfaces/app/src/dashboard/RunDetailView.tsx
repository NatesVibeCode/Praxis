import React, { useState } from 'react';
import './dashboard.css';
import {
  RunJob,
  RunStatus,
  JobStatus,
  RunGraph as RunGraphData,
  useLiveRunSnapshot,
} from './useLiveRunSnapshot';
import { RunGraphView } from '../shared/RunGraphView';

export interface RunDetailViewProps {
  runId: string;
  onBack: () => void;
}

const RUNNING_JOB_STATUSES = new Set<JobStatus>(['running', 'claimed']);
const PENDING_JOB_STATUSES = new Set<JobStatus>(['pending', 'ready']);
const FAILED_JOB_STATUSES = new Set<JobStatus>(['failed', 'dead_letter', 'cancelled']);

function formatDuration(durationMs: number | null | undefined): string {
  if (!durationMs || durationMs <= 0) {
    return '0.0s';
  }
  return `${(durationMs / 1000).toFixed(1)}s`;
}

function formatCurrency(value: number | null | undefined): string {
  return `$${(value ?? 0).toFixed(2)}`;
}

function formatSeconds(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) {
    return 'n/a';
  }
  return `${value.toFixed(1)}s`;
}

function formatCount(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) {
    return 'n/a';
  }
  return new Intl.NumberFormat('en-US').format(value);
}

function humanizeLabel(label: string): string {
  if (!label) {
    return 'Unnamed Step';
  }
  return label
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function getStepTitle(job: RunJob, index: number): string {
  if (/^step-\d+$/i.test(job.label)) {
    return `Step ${index + 1}`;
  }
  return `Step ${index + 1}: ${humanizeLabel(job.label)}`;
}

function getRunStatusColor(status: RunStatus): string {
  if (status === 'succeeded') return 'var(--success)';
  if (status === 'running') return 'var(--accent)';
  if (status === 'failed' || status === 'cancelled') return 'var(--danger)';
  return 'var(--text-muted)';
}

function getHealthStatusColor(state: string): string {
  if (state === 'healthy') return 'var(--success)';
  if (state === 'degraded') return 'var(--warning)';
  if (state === 'failed') return 'var(--danger)';
  return 'var(--text-muted)';
}

function getJobStatusVariant(status: JobStatus): 'succeeded' | 'running' | 'pending' | 'failed' {
  if (status === 'succeeded') return 'succeeded';
  if (RUNNING_JOB_STATUSES.has(status)) return 'running';
  if (FAILED_JOB_STATUSES.has(status)) return 'failed';
  if (PENDING_JOB_STATUSES.has(status)) return 'pending';
  return 'pending';
}

function getJobStatusLabel(status: JobStatus): string {
  if (status === 'claimed') return 'running...';
  if (status === 'ready') return 'pending';
  if (status === 'dead_letter') return 'failed';
  return status;
}

interface JobDetail extends RunJob {
  output?: string;
  output_json?: unknown;
  output_source?: string;
  receipt_id?: string | null;
}

function parseStructuredOutput(job: RunJob, detail?: JobDetail): string {
  if (detail?.output_json != null) {
    return JSON.stringify(detail.output_json, null, 2);
  }

  const rawOutput = detail?.output ?? job.stdout_preview ?? '';
  const shouldAttemptJson = Boolean(job.integration_id || rawOutput.trim().startsWith('{') || rawOutput.trim().startsWith('['));
  if (!shouldAttemptJson) {
    return rawOutput || 'No output captured yet.';
  }

  try {
    return JSON.stringify(JSON.parse(rawOutput), null, 2);
  } catch {
    return rawOutput || 'No output captured yet.';
  }
}

async function fetchJobDetail(runId: string, jobId: number): Promise<JobDetail> {
  const response = await fetch(`/api/runs/${encodeURIComponent(runId)}/jobs/${jobId}`);
  if (!response.ok) {
    throw new Error(`Failed to load job ${jobId} (${response.status})`);
  }
  return (await response.json()) as JobDetail;
}

export function RunDetailView({ runId, onBack }: RunDetailViewProps) {
  const {
    run,
    loading,
    error,
    streamStatus,
  } = useLiveRunSnapshot(runId);
  const [expandedSteps, setExpandedSteps] = useState<Record<number, boolean>>({});
  const [jobDetails, setJobDetails] = useState<Record<number, JobDetail>>({});
  const [loadingOutputs, setLoadingOutputs] = useState<Record<number, boolean>>({});

  const toggleStep = (job: RunJob) => {
    const willExpand = !expandedSteps[job.id];
    setExpandedSteps((prev) => ({
      ...prev,
      [job.id]: willExpand,
    }));

    if (!willExpand || jobDetails[job.id] || !job.has_output) {
      return;
    }

    setLoadingOutputs((prev) => ({ ...prev, [job.id]: true }));
    fetchJobDetail(runId, job.id)
      .then((detail) => {
        setJobDetails((prev) => ({ ...prev, [job.id]: detail }));
      })
      .catch(() => {
        setJobDetails((prev) => ({
          ...prev,
          [job.id]: {
            ...job,
            output: job.stdout_preview || 'No output captured yet.',
            output_source: 'preview',
          },
        }));
      })
      .finally(() => {
        setLoadingOutputs((prev) => ({ ...prev, [job.id]: false }));
      });
  };

  if (loading) {
    return (
      <div className="run-detail">
        <div className="run-detail__empty">Loading live run details...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="run-detail">
        <div className="run-detail__header">
          <button type="button" className="run-detail__back" onClick={onBack}>
            ← Back to Overview
          </button>
        </div>
        <div className="run-detail__empty">
          <div>{error}</div>
          {streamStatus !== 'idle' ? <div style={{ marginTop: 8, opacity: 0.75 }}>Live stream: {streamStatus}</div> : null}
        </div>
      </div>
    );
  }

  if (!run) {
    return (
      <div className="run-detail">
        <div className="run-detail__header">
          <button type="button" className="run-detail__back" onClick={onBack}>
            ← Back to Overview
          </button>
        </div>
        <div className="run-detail__empty">
          Run not found.
          {streamStatus !== 'idle' ? <div style={{ marginTop: 8, opacity: 0.75 }}>Live stream: {streamStatus}</div> : null}
        </div>
      </div>
    );
  }

  const succeeded = run.jobs.filter((j) => j.status === 'succeeded').length;
  const total = run.total_jobs ?? run.jobs.length;
  const health = run.health;
  const telemetry = health?.resource_telemetry;

  return (
    <div className="run-detail">
      <div className="run-detail__header">
        <button type="button" className="run-detail__back" onClick={onBack}>
          ← Back to Overview
        </button>
        <div className="run-detail__title">Run: {run.spec_name || run.run_id}</div>
        <div className="run-detail__status" style={{
          color: getRunStatusColor(run.status),
          background: run.status === 'succeeded'
            ? 'var(--surface-success-soft)'
            : run.status === 'failed' || run.status === 'cancelled'
              ? 'var(--surface-danger-soft)'
              : run.status === 'running'
                ? 'var(--surface-accent-soft)'
                : 'var(--surface-muted)',
        }}>
          {run.status}
          {streamStatus === 'connected'
            ? ' · live'
            : streamStatus === 'reconnecting'
              ? ' · reconnecting'
              : streamStatus === 'connecting'
                ? ' · connecting'
                : ''}
        </div>
      </div>

      {run.summary && (
        <div className="run-detail__outcome">
          {run.summary}
        </div>
      )}

      {health && (
        <div className="run-detail__health">
          <div className="run-detail__health-header">
            <div className="run-detail__health-title">Health</div>
            <div
              className="run-detail__health-chip"
              style={{
                color: getHealthStatusColor(health.state),
                background:
                  health.state === 'healthy'
                    ? 'var(--surface-success-soft)'
                    : health.state === 'degraded'
                      ? 'var(--surface-warning-soft)'
                      : health.state === 'failed'
                        ? 'var(--surface-danger-soft)'
                        : 'var(--surface-muted)',
              }}
            >
              {health.state}
            </div>
            {health.likely_failed ? (
              <div className="run-detail__health-chip run-detail__health-chip--danger">
                likely failed
              </div>
            ) : null}
          </div>

          <div className="run-detail__health-grid">
            <div className="run-detail__health-stat">
              <span>Elapsed</span>
              <strong>{formatSeconds(health.elapsed_seconds)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Completed</span>
              <strong>{formatCount(health.completed_jobs)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Running</span>
              <strong>{formatCount(health.running_or_claimed)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Terminal</span>
              <strong>{formatCount(health.terminal_jobs)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Signals</span>
              <strong>{formatCount(health.signals.length)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Non-retryable</span>
              <strong>{formatCount(health.non_retryable_failed_jobs.length)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Token total</span>
              <strong>{telemetry?.tokens_total == null ? 'n/a' : formatCount(telemetry.tokens_total)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Token rate</span>
              <strong>{telemetry?.tokens_per_minute == null ? 'n/a' : `${telemetry.tokens_per_minute.toFixed(1)}/m`}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Avg job</span>
              <strong>{telemetry?.avg_job_duration_ms == null ? 'n/a' : formatDuration(telemetry.avg_job_duration_ms)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Heartbeat</span>
              <strong>{telemetry?.heartbeat_freshness ?? 'n/a'}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Stale jobs</span>
              <strong>{telemetry?.stale_heartbeat_jobs == null ? '0' : formatCount(telemetry.stale_heartbeat_jobs)}</strong>
            </div>
            <div className="run-detail__health-stat">
              <span>Last activity</span>
              <strong>{telemetry?.seconds_since_last_activity == null ? 'n/a' : formatSeconds(telemetry.seconds_since_last_activity)}</strong>
            </div>
          </div>
        </div>
      )}

      {/* Run graph visualization — shows dependency graph when available */}
      {run.graph && run.graph.nodes?.length > 0 && (
        <RunGraphView graph={run.graph} onSelectJob={(label) => {
          // Find the job by label and expand it
          const job = run.jobs.find(j => j.label === label);
          if (job) {
            setExpandedSteps(prev => ({ ...prev, [job.id]: true }));
            toggleStep(job);
            // Scroll to the job
            setTimeout(() => {
              document.getElementById(`run-step-${job.id}`)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
            }, 100);
          }
        }} />
      )}

      {run.jobs.length === 0 ? (
        <div className="run-detail__empty">No step details are available for this run yet.</div>
      ) : (
        run.jobs.map((job, index) => {
          const detail = jobDetails[job.id];
          const isExpanded = Boolean(expandedSteps[job.id]);
          const outputText = parseStructuredOutput(job, detail);

          return (
            <div key={job.id} id={`run-step-${job.id}`} className="run-step">
              <button
                type="button"
                className="run-step__row"
                onClick={() => toggleStep(job)}
                aria-expanded={isExpanded}
              >
                <span className={`run-step__dot run-step__dot--${getJobStatusVariant(job.status)}`} />
                <span className="run-step__label">{getStepTitle(job, index)}</span>
                <span className="run-step__status">{getJobStatusLabel(job.status)}</span>
                <span className="run-step__duration">
                  {job.status === 'succeeded' || FAILED_JOB_STATUSES.has(job.status)
                    ? formatDuration(job.duration_ms)
                    : ''}
                </span>
              </button>

              {isExpanded && (
                <pre className="run-step__output">
                  {loadingOutputs[job.id] ? 'Loading full output...' : outputText}
                </pre>
              )}
            </div>
          );
        })
      )}

      <div className="run-detail__summary">
        <span>
          Total: <strong>{formatDuration(run.total_duration_ms)}</strong>
        </span>
        <span>
          Cost: <strong>{formatCurrency(run.total_cost)}</strong>
        </span>
        <span>
          Progress: <strong>{succeeded}/{total} complete</strong>
        </span>
      </div>
    </div>
  );
}
