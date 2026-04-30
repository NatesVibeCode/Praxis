import { useCallback, useEffect, useRef, useState } from 'react';
import {
  runDetailPath,
  runsRecentPath,
  workflowRunStreamPath,
} from './runApi';
import { fetchJson, isAbortError, isHttpRequestError, type JsonRequestOptions } from '../shared/request';

export type RunStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';

export type JobStatus =
  | 'pending'
  | 'ready'
  | 'claimed'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'dead_letter'
  | 'blocked'
  | 'cancelled'
  | 'parent_failed';

export interface RecentRun {
  run_id: string;
  workflow_id?: string | null;
  spec_name: string;
  status: RunStatus;
  total_jobs: number;
  completed_jobs: number;
  total_cost: number;
  created_at: string | null;
  finished_at: string | null;
}

export interface RunJobAuthorityBindingSummary {
  bound: true;
  canonical_count: number;
  predecessor_count: number;
  blocked_compat_count: number;
  redirected_count: number;
}

export interface RunJob {
  id: number;
  label: string;
  status: JobStatus;
  job_type: string;
  phase: string;
  agent_slug: string | null;
  resolved_agent: string | null;
  integration_id: string | null;
  integration_action: string | null;
  integration_args: unknown;
  attempt: number;
  duration_ms: number;
  cost_usd: number;
  exit_code: number | null;
  last_error_code: string | null;
  stdout_preview: string;
  has_output: boolean;
  started_at: string | null;
  finished_at: string | null;
  created_at: string | null;
  // Compose-time canonical authority binding summary, present when the job
  // was composed against authority-bearing targets. Full binding payload is
  // available via the per-job detail endpoint; this is the compact-idle
  // projection for list rendering. See architecture-policy::platform-
  // architecture::workflow-job-authority-binding-persisted.
  authority_binding?: RunJobAuthorityBindingSummary | null;
}

export interface RunCompletionContract {
  result_kind?: string;
  submit_tool_names?: string[];
  submission_required?: boolean;
  verification_required?: boolean;
  [key: string]: unknown;
}

export interface RunGraphNode {
  id: string;
  label: string;
  type: string;
  adapter: string;
  position: number;
  status: string;
  cost_usd?: number;
  duration_ms?: number;
  agent?: string;
  attempt?: number;
  error_code?: string;
  loop?: { count: number; succeeded: number; failed: number; running: number };
  task_type?: string;
  description?: string;
  outcome_goal?: string;
  prompt?: string;
  completion_contract?: RunCompletionContract | null;
}

export interface RunGraphEdge {
  id: string;
  from: string;
  to: string;
  type: string;
  condition?: Record<string, unknown>;
  data_mapping?: Record<string, unknown>;
}

export interface RunGraph {
  nodes: RunGraphNode[];
  edges: RunGraphEdge[];
}

export interface RunHealthTelemetry {
  tokens_total?: number;
  tokens_per_minute?: number;
  avg_job_duration_ms?: number;
  stale_heartbeat_jobs?: number;
  heartbeat_freshness?: string;
  seconds_since_last_activity?: number;
}

export interface RunHealth {
  state: string;
  likely_failed: boolean;
  signals: Array<Record<string, unknown>>;
  elapsed_seconds: number | null;
  completed_jobs: number;
  running_or_claimed: number;
  terminal_jobs: number;
  resource_telemetry?: RunHealthTelemetry;
  stalled_jobs?: Record<string, string[] | number[]>;
  non_retryable_failed_jobs: string[];
}

export interface RunDetail extends RecentRun {
  total_duration_ms: number;
  jobs: RunJob[];
  summary?: string | null;
  graph?: RunGraph | null;
  health?: RunHealth | null;
}

const TERMINAL_RUN_STATUSES = new Set<RunStatus>(['succeeded', 'failed', 'cancelled']);
const FALLBACK_REFRESH_INTERVAL_MS = 10_000;

export async function loadRunSnapshot(runId: string, options?: JsonRequestOptions): Promise<RunDetail> {
  try {
    return await fetchJson<RunDetail>(runDetailPath(runId), {}, options);
  } catch (error) {
    if (isAbortError(error)) throw error;
    if (!isHttpRequestError(error, 404)) {
      const suffix = isHttpRequestError(error) ? ` (${error.status})` : '';
      throw new Error(`Failed to load run ${runId}${suffix}`);
    }
  }

  let recentRuns: RecentRun[];
  try {
    recentRuns = await fetchJson<RecentRun[]>(runsRecentPath(100), {}, options);
  } catch (error) {
    if (isAbortError(error)) throw error;
    throw new Error(`Run ${runId} was not found.`);
  }

  const recentMatch = recentRuns.find((run) => run.run_id === runId) ?? null;
  if (!recentMatch) {
    throw new Error(`Run ${runId} was not found.`);
  }

  return {
    ...recentMatch,
    total_duration_ms: 0,
    jobs: [],
    health: null,
  };
}

export interface LiveRunSnapshotState {
  run: RunDetail | null;
  loading: boolean;
  error: string | null;
  streamStatus: 'idle' | 'connecting' | 'connected' | 'reconnecting';
  refresh: () => void;
}

export function useLiveRunSnapshot(runId: string | null): LiveRunSnapshotState {
  const [run, setRun] = useState<RunDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [streamStatus, setStreamStatus] = useState<'idle' | 'connecting' | 'connected' | 'reconnecting'>('idle');
  const [refreshTick, setRefreshTick] = useState(0);

  const sourceRef = useRef<EventSource | null>(null);
  const initialLoadRef = useRef(true);
  const terminalRef = useRef(false);
  const refreshTimerRef = useRef<number | null>(null);
  const connectedRef = useRef(false);
  const loadAbortRef = useRef<AbortController | null>(null);
  const loadRequestSeqRef = useRef(0);

  const refresh = useCallback(() => {
    setRefreshTick((tick) => tick + 1);
  }, []);

  const closeStream = useCallback(() => {
    connectedRef.current = false;
    sourceRef.current?.close();
    sourceRef.current = null;
  }, []);

  const clearQueuedRefresh = useCallback(() => {
    if (refreshTimerRef.current !== null) {
      window.clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
  }, []);

  const queueStreamRefresh = useCallback(() => {
    if (terminalRef.current || refreshTimerRef.current !== null) {
      return;
    }
    refreshTimerRef.current = window.setTimeout(() => {
      refreshTimerRef.current = null;
      if (!terminalRef.current) {
        refresh();
      }
    }, 0);
  }, [refresh]);

  useEffect(() => {
    terminalRef.current = false;
    initialLoadRef.current = true;
    connectedRef.current = false;
    loadAbortRef.current?.abort();
    loadAbortRef.current = null;
    loadRequestSeqRef.current += 1;
    setRun(null);
    setError(null);
    setLoading(Boolean(runId));
    setStreamStatus(runId ? 'connecting' : 'idle');
    setRefreshTick(0);
    clearQueuedRefresh();
    closeStream();
  }, [clearQueuedRefresh, closeStream, runId]);

  useEffect(() => {
    if (!runId) {
      return undefined;
    }

    const streamUrl = workflowRunStreamPath(runId);
    if (typeof window === 'undefined' || typeof window.EventSource !== 'function') {
      setStreamStatus('idle');
      return undefined;
    }

    const source = new window.EventSource(streamUrl);
    sourceRef.current = source;

    const refreshFromStream = () => {
      if (sourceRef.current === source) {
        queueStreamRefresh();
      }
    };

    const handleDone = () => {
      terminalRef.current = true;
      connectedRef.current = false;
      setStreamStatus('idle');
      clearQueuedRefresh();
      source.close();
      if (sourceRef.current === source) {
        sourceRef.current = null;
      }
      refresh();
    };

    source.onopen = () => {
      if (terminalRef.current || sourceRef.current !== source) {
        return;
      }
      connectedRef.current = true;
      setStreamStatus('connected');
      setError(null);
    };

    source.addEventListener('start', refreshFromStream);
    source.addEventListener('job', refreshFromStream);
    source.addEventListener('progress', refreshFromStream);
    source.addEventListener('done', handleDone);

    source.onerror = () => {
      if (terminalRef.current || sourceRef.current !== source) {
        return;
      }
      connectedRef.current = false;
      setStreamStatus('reconnecting');
      setError('Live stream disconnected. Refreshing in the background.');
    };

    return () => {
      source.close();
      if (sourceRef.current === source) {
        sourceRef.current = null;
      }
      clearQueuedRefresh();
    };
  }, [clearQueuedRefresh, queueStreamRefresh, refresh, runId]);

  useEffect(() => {
    if (!runId) {
      return undefined;
    }

    loadAbortRef.current?.abort();
    const controller = new AbortController();
    loadAbortRef.current = controller;
    const requestSeq = loadRequestSeqRef.current + 1;
    loadRequestSeqRef.current = requestSeq;
    const isCurrentRequest = () =>
      loadRequestSeqRef.current === requestSeq
      && loadAbortRef.current === controller
      && !controller.signal.aborted;
    const isInitialLoad = initialLoadRef.current;
    if (isInitialLoad) {
      setLoading(true);
    }

    void (async () => {
      try {
        const nextRun = await loadRunSnapshot(runId, { signal: controller.signal });
        if (!isCurrentRequest()) {
          return;
        }
        initialLoadRef.current = false;
        setRun(nextRun);
        setError(null);
        setLoading(false);
        if (TERMINAL_RUN_STATUSES.has(nextRun.status)) {
          terminalRef.current = true;
          clearQueuedRefresh();
          setStreamStatus('idle');
          closeStream();
        } else if (connectedRef.current) {
          setStreamStatus('connected');
        }
      } catch (err: unknown) {
        if (!isCurrentRequest() || isAbortError(err)) {
          return;
        }
        initialLoadRef.current = false;
        if (isInitialLoad) {
          setLoading(false);
        }
        setError(err instanceof Error ? err.message : `Failed to load run ${runId}.`);
      } finally {
        if (loadAbortRef.current === controller) {
          loadAbortRef.current = null;
        }
      }
    })();

    return () => {
      controller.abort();
    };
  }, [clearQueuedRefresh, closeStream, refreshTick, runId]);

  useEffect(() => {
    if (!runId || !run || TERMINAL_RUN_STATUSES.has(run.status) || streamStatus === 'connected') {
      return undefined;
    }

    const intervalId = window.setInterval(() => {
      if (!terminalRef.current && !document.hidden) {
        refresh();
      }
    }, FALLBACK_REFRESH_INTERVAL_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [refresh, run, runId, streamStatus]);

  return {
    run,
    loading,
    error,
    streamStatus,
    refresh,
  };
}
