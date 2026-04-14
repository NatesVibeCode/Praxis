import React, { useState } from 'react';
import './dashboard.css';
import {
  RunJob,
  RunStatus,
  JobStatus,
  RunGraph as RunGraphData,
  useLiveRunSnapshot,
} from './useLiveRunSnapshot';

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

// -- Run graph renderer --
import type { RunGraphNode as GraphNode } from './useLiveRunSnapshot';

function RunGraph({ graph, onSelectJob }: { graph: RunGraphData; onSelectJob?: (label: string) => void }) {
  const depths: Record<string, number> = {};
  const inDeg: Record<string, number> = {};
  const outEdges: Record<string, string[]> = {};

  for (const n of graph.nodes) {
    depths[n.id] = 0;
    inDeg[n.id] = 0;
    outEdges[n.id] = [];
  }
  for (const e of graph.edges) {
    inDeg[e.to] = (inDeg[e.to] || 0) + 1;
    outEdges[e.from] = outEdges[e.from] || [];
    outEdges[e.from].push(e.to);
  }

  const queue = graph.nodes.filter(n => (inDeg[n.id] || 0) === 0).map(n => n.id);
  while (queue.length > 0) {
    const cur = queue.shift()!;
    for (const next of (outEdges[cur] || [])) {
      depths[next] = Math.max(depths[next] || 0, (depths[cur] || 0) + 1);
      inDeg[next]--;
      if (inDeg[next] === 0) queue.push(next);
    }
  }

  const maxDepth = Math.max(0, ...Object.values(depths));
  const columns: GraphNode[][] = Array.from({ length: maxDepth + 1 }, () => []);
  for (const n of graph.nodes) columns[depths[n.id] || 0].push(n);

  return (
    <div className="run-graph">
      {columns.map((col, ci) => (
        <React.Fragment key={ci}>
          {ci > 0 && (
            <div className="run-graph__edge">
              <svg width="32" height="2" style={{ display: 'block' }}>
                <line x1="0" y1="1" x2="32" y2="1" stroke="var(--border)" strokeWidth="1.5" />
                <polygon points="28,0 32,1 28,2" fill="var(--text-muted)" opacity="0.5" />
              </svg>
            </div>
          )}
          <div className="run-graph__column">
            {col.map(n => {
              const variant = getJobStatusVariant(n.status as JobStatus);
              const subtitle = n.fan_out
                ? `${n.fan_out.succeeded}/${n.fan_out.count} done`
                : (n.error_code
                    ? n.error_code.replace(/^workflow_submission\./, '')
                    : n.status);
              return (
                <button
                  key={n.id}
                  type="button"
                  className={`run-graph__node run-graph__node--${variant}`}
                  onClick={() => onSelectJob?.(n.label)}
                >
                  <span className="run-graph__node-title">{humanizeLabel(n.label)}</span>
                  <span className="run-graph__node-sub">
                    {subtitle}
                    {n.duration_ms ? ` · ${(n.duration_ms / 1000).toFixed(1)}s` : ''}
                  </span>
                </button>
              );
            })}
          </div>
        </React.Fragment>
      ))}
    </div>
  );
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

      {/* Run graph visualization — shows dependency graph when available */}
      {run.graph && run.graph.nodes?.length > 0 && (
        <RunGraph graph={run.graph} onSelectJob={(label) => {
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
