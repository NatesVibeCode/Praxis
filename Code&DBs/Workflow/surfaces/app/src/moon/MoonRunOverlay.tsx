import React, { useEffect, useState } from 'react';
import type { RunDetail, RunJob } from '../dashboard/useLiveRunSnapshot';

interface Props {
  run: RunDetail | null;
  loading: boolean;
  error: string | null;
  selectedJobId: string | null;
  onSelectJob: (jobId: string | null) => void;
  onExit: () => void;
  onCancel?: () => void;
}

const RUN_STATUS_COLOR: Record<string, string> = {
  succeeded: '#3fb950',
  running: '#58a6ff',
  failed: '#f85149',
  cancelled: '#8b949e',
  queued: '#484f58',
};

const JOB_STATUS_COLOR: Record<string, string> = {
  succeeded: '#3fb950',
  running: '#58a6ff',
  claimed: '#58a6ff',
  failed: '#f85149',
  dead_letter: '#f85149',
  pending: '#484f58',
  ready: '#8b949e',
  cancelled: '#8b949e',
};

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

export function MoonRunOverlay({
  run,
  loading,
  error,
  selectedJobId,
  onSelectJob,
  onExit,
  onCancel,
}: Props) {
  const [expandedOutput, setExpandedOutput] = useState(false);

  // Clear expanded view when selection changes.
  useEffect(() => {
    setExpandedOutput(false);
  }, [selectedJobId]);

  // selectedJobId is the graph-node id (string) from RunGraphNode. Match by
  // label; RunGraphNode.id is typically the job's label or a stable key
  // tied to the compiled spec, which is what RunJob.label also reports.
  const selectedJob: RunJob | null = selectedJobId && run?.jobs
    ? (run.jobs.find((j) => j.label === selectedJobId) ?? null)
    : null;

  const runStatus = run?.status ?? 'queued';
  const isTerminal = runStatus === 'succeeded' || runStatus === 'failed' || runStatus === 'cancelled';
  const statusColor = RUN_STATUS_COLOR[runStatus] ?? '#8b949e';

  const totalJobs = run?.jobs?.length ?? 0;
  const completedJobs = run?.jobs?.filter((j) =>
    j.status === 'succeeded' || j.status === 'failed' || j.status === 'dead_letter' || j.status === 'cancelled',
  ).length ?? 0;
  const totalCost = run?.jobs?.reduce((acc, j) => acc + (j.cost_usd || 0), 0) ?? 0;
  const totalDuration = run?.total_duration_ms ?? 0;

  return (
    <>
      <div className="moon-run-overlay__header">
        <button
          type="button"
          className="moon-run-overlay__back"
          onClick={onExit}
          aria-label="Exit run view"
        >
          ← Back
        </button>
        <div className="moon-run-overlay__title">
          <div className="moon-run-overlay__spec-name">{run?.spec_name ?? 'Run'}</div>
          <div className="moon-run-overlay__run-id">{run?.run_id ?? ''}</div>
        </div>
        <div className="moon-run-overlay__chip" style={{ background: statusColor }}>
          {runStatus}
        </div>
        <div className="moon-run-overlay__metrics">
          <span>{completedJobs}/{totalJobs}</span>
          <span>{formatDuration(totalDuration)}</span>
          <span>{formatCost(totalCost)}</span>
        </div>
        {!isTerminal && onCancel && (
          <button
            type="button"
            className="moon-run-overlay__cancel"
            onClick={onCancel}
          >
            Cancel
          </button>
        )}
      </div>

      {loading && !run && (
        <div className="moon-run-overlay__loading">Loading run…</div>
      )}
      {error && (
        <div className="moon-run-overlay__error">{error}</div>
      )}

      {selectedJob && (
        <aside className="moon-run-overlay__receipt" aria-label="Job receipt">
          <header className="moon-run-overlay__receipt-head">
            <span
              className="moon-run-overlay__receipt-dot"
              style={{ background: JOB_STATUS_COLOR[selectedJob.status] ?? '#484f58' }}
            />
            <span className="moon-run-overlay__receipt-label">{selectedJob.label}</span>
            <button
              type="button"
              className="moon-run-overlay__receipt-close"
              onClick={() => onSelectJob(null)}
              aria-label="Close receipt"
            >
              ×
            </button>
          </header>
          <dl className="moon-run-overlay__receipt-meta">
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
          {selectedJob.stdout_preview && (
            <div className="moon-run-overlay__receipt-output">
              <div className="moon-run-overlay__receipt-output-head">
                <span>Output preview</span>
                {selectedJob.has_output && (
                  <button
                    type="button"
                    className="moon-run-overlay__receipt-output-toggle"
                    onClick={() => setExpandedOutput((v) => !v)}
                  >
                    {expandedOutput ? 'Collapse' : 'Expand'}
                  </button>
                )}
              </div>
              <pre className={`moon-run-overlay__receipt-pre${expandedOutput ? ' moon-run-overlay__receipt-pre--expanded' : ''}`}>
                {selectedJob.stdout_preview}
              </pre>
            </div>
          )}
        </aside>
      )}
    </>
  );
}
