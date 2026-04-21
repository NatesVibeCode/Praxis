import React, { useState, useCallback, useEffect } from 'react';
import { useLiveRunSnapshot } from '../dashboard/useLiveRunSnapshot';
import type { RunJob, RunStatus, RecentRun, RunDetail } from '../dashboard/useLiveRunSnapshot';
import {
  runJobsPath,
  runsRecentPath,
  workflowRunStreamPath,
} from '../dashboard/runApi';
import { triggerWorkflow } from '../shared/buildController';
import { MoonStatusRing } from './MoonStatusRing';

interface Props {
  runId: string;
  workflowId: string | null;
  onClose: () => void;
  onSwitchRun?: (runId: string) => void;
}

const TERMINAL: Set<RunStatus> = new Set(['succeeded', 'failed', 'cancelled']);
const TERMINAL_JOB_STATUSES = new Set<RunJob['status']>([
  'succeeded',
  'failed',
  'dead_letter',
  'blocked',
  'cancelled',
  'parent_failed',
]);

function formatRawStatus(status: string): string {
  return status.replace(/[_-]+/g, ' ');
}

interface RunStreamEvent {
  jobs?: RunJob[];
  status?: RunStatus;
}

function JobRow({ job, onClick }: { job: RunJob; onClick?: () => void }) {
  return (
    <div className="moon-run__job" onClick={onClick} style={onClick ? { cursor: 'pointer' } : undefined}>
      <MoonStatusRing status={job.status} size={14} />
      <span className="moon-run__job-label">{job.label}</span>
      <span className="moon-run__job-status">{job.status}</span>
      {job.duration_ms > 0 && (
        <span className="moon-run__job-duration">{(job.duration_ms / 1000).toFixed(1)}s</span>
      )}
      {job.cost_usd > 0 && (
        <span className="moon-run__job-cost">${job.cost_usd.toFixed(4)}</span>
      )}
      {job.last_error_code && (
        <span className="moon-run__job-error">{job.last_error_code}</span>
      )}
    </div>
  );
}

export function MoonRunPanel({ runId, workflowId, onClose, onSwitchRun }: Props) {
  const { run, loading, error } = useLiveRunSnapshot(runId);
  const [rerunning, setRerunning] = useState(false);
  const [rerunError, setRerunError] = useState<string | null>(null);
  const [history, setHistory] = useState<RecentRun[]>([]);
  const [expandedJob, setExpandedJob] = useState<number | null>(null);
  const [jobOutput, setJobOutput] = useState<string | null>(null);
  const [liveRun, setLiveRun] = useState<RunDetail | null>(null);

  useEffect(() => {
    setLiveRun(run);
  }, [run]);

  const runStatus = liveRun?.status ?? run?.status ?? null;
  const isTerminal = runStatus ? TERMINAL.has(runStatus) : false;

  // Load run history for this workflow
  useEffect(() => {
    if (!workflowId) return;
    fetch(runsRecentPath(10))
      .then(r => r.json())
      .then((runs: RecentRun[]) => {
        setHistory(runs.filter(r => r.run_id !== runId).slice(0, 5));
      })
      .catch(() => {});
  }, [workflowId, runId]);

  useEffect(() => {
    if (!runId || !runStatus || TERMINAL.has(runStatus)) {
      return undefined;
    }

    const es = new EventSource(workflowRunStreamPath(runId));
    es.onmessage = (event) => {
      let data: RunStreamEvent;
      try {
        data = JSON.parse(event.data) as RunStreamEvent;
      } catch {
        return;
      }

      if (!data.jobs && !data.status) {
        return;
      }

      setLiveRun((current) => {
        if (!current) {
          return current;
        }
        const jobs = data.jobs ?? current.jobs;
        const nextStatus = data.status ?? current.status;
        const completedJobs = jobs.filter((job) => TERMINAL_JOB_STATUSES.has(job.status)).length;
        const totalJobs = jobs.length || current.total_jobs;
        const totalCost = jobs.reduce((sum, job) => sum + (job.cost_usd || 0), 0);

        return {
          ...current,
          jobs,
          status: nextStatus,
          completed_jobs: completedJobs,
          total_jobs: totalJobs,
          total_cost: totalCost,
          finished_at: TERMINAL.has(nextStatus) ? current.finished_at ?? new Date().toISOString() : current.finished_at,
        };
      });
    };
    es.onerror = () => {
      es.close();
    };

    return () => {
      es.close();
    };
  }, [runId, runStatus]);

  const handleRerun = useCallback(async () => {
    if (!workflowId) return;
    setRerunning(true);
    setRerunError(null);
    try {
      const result = await triggerWorkflow(workflowId);
      if (result.run_id && onSwitchRun) {
        onSwitchRun(result.run_id);
      }
    } catch (e: any) {
      setRerunError(e.message || 'Re-run failed');
    } finally {
      setRerunning(false);
    }
  }, [workflowId, onSwitchRun]);

  // Fetch job output on expand
  const handleJobClick = useCallback(async (job: RunJob) => {
    if (expandedJob === job.id) {
      setExpandedJob(null);
      setJobOutput(null);
      return;
    }
    setExpandedJob(job.id);
    setJobOutput(null);
    try {
      const resp = await fetch(runJobsPath(runId, job.id));
      if (resp.ok) {
        const data = await resp.json();
        setJobOutput(data.output || data.stdout_preview || 'No output');
      }
    } catch { /* ignore */ }
  }, [runId, expandedJob]);

  return (
    <>
      <button className="moon-dock__close" onClick={onClose} aria-label="Close run panel">&times;</button>
      <div className="moon-dock__title">
        Run
        {liveRun && (
          <span className="moon-run__status-chip" style={{ marginLeft: 8 }}>
            <MoonStatusRing status={liveRun.status} size={10} />
            <span>{formatRawStatus(liveRun.status)}</span>
          </span>
        )}
      </div>
      <div className="moon-dock__sep" />

      {loading && !liveRun && (
        <div className="moon-dock__empty">Loading run...</div>
      )}
      {error && (
        <div className="moon-dock-form__error">{error}</div>
      )}

      {liveRun && (
        <div className="moon-run__content">
          <div className="moon-run__summary">
            <span>{liveRun.completed_jobs}/{liveRun.total_jobs} jobs</span>
            {liveRun.total_cost > 0 && <span> &middot; ${liveRun.total_cost.toFixed(4)}</span>}
            {liveRun.finished_at && <span> &middot; done</span>}
          </div>

          <div className="moon-run__jobs">
            {liveRun.jobs.map((job: RunJob) => (
              <React.Fragment key={job.id}>
                <JobRow job={job} onClick={() => handleJobClick(job)} />
                {expandedJob === job.id && jobOutput && (
                  <pre className="moon-run__job-output">{jobOutput}</pre>
                )}
                {expandedJob === job.id && job.last_error_code && (
                  <div className="moon-run__job-error-detail">
                    Error: {job.last_error_code}
                    {job.stdout_preview && <pre className="moon-run__job-output">{job.stdout_preview}</pre>}
                  </div>
                )}
              </React.Fragment>
            ))}
          </div>

          {/* Re-run button (shown when terminal) */}
          {isTerminal && workflowId && (
            <div style={{ marginTop: 12 }}>
              <button
                className="moon-release__dispatch-btn moon-release__dispatch-btn--ready"
                onClick={handleRerun}
                disabled={rerunning}
                style={{ width: '100%' }}
              >
                {rerunning ? 'Re-running...' : 'Re-run'}
              </button>
              {rerunError && <div className="moon-dock-form__error">{rerunError}</div>}
            </div>
          )}

          {/* Run history */}
          {history.length > 0 && (
            <div style={{ marginTop: 12 }}>
              <div className="moon-dock__section-label">Recent runs</div>
              {history.map(h => (
                <div
                  key={h.run_id}
                  className="moon-run__history-item"
                  onClick={() => onSwitchRun?.(h.run_id)}
                >
                  <MoonStatusRing status={h.status} size={12} />
                  <span className="moon-run__job-label">{h.spec_name || h.run_id.slice(0, 16)}</span>
                  <span className="moon-run__job-status">{h.status}</span>
                  <span className="moon-run__job-duration">{h.total_jobs} jobs</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </>
  );
}
