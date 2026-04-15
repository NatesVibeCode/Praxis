import { useState, useEffect, useCallback } from 'react';

export interface RecentRun {
  run_id: string;
  spec_name: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  total_jobs: number;
  completed_jobs: number;
  total_cost: number;
  created_at: string | null;
  finished_at: string | null;
}

export interface SystemStatus {
  // From /api/health or /api/status
  modelsOnline: number;
  activeRuns: number;
  queueDepth: number;
  queueDepthStatus: 'ok' | 'warning' | 'critical' | 'unknown';
  queueDepthUtilizationPct: number;
  queueDepthPending: number;
  queueDepthReady: number;
  queueDepthClaimed: number;
  queueDepthRunning: number;
  queueDepthError: string | null;
  // From /api/metrics or /api/status
  passRate24h: number | null;
  totalRuns24h: number;
  totalCost24h: number;
  topAgent: string | null;
  // From /api/leaderboard
  agents: Array<{
    name: string;
    passRate: number;
    avgLatency: number;
    totalCost: number;
    workflows: number;
  }>;
  // Recent workflow runs
  recentRuns: RecentRun[];
  // Errors
  error: string | null;
  loading: boolean;
}

const POLL_INTERVAL = 15_000; // 15 seconds

export function useSystemStatus(): SystemStatus {
  const [status, setStatus] = useState<SystemStatus>({
    modelsOnline: 0,
    activeRuns: 0,
    queueDepth: 0,
    queueDepthStatus: 'unknown',
    queueDepthUtilizationPct: 0,
    queueDepthPending: 0,
    queueDepthReady: 0,
    queueDepthClaimed: 0,
    queueDepthRunning: 0,
    queueDepthError: null,
    passRate24h: null,
    totalRuns24h: 0,
    totalCost24h: 0,
    topAgent: null,
    agents: [],
    recentRuns: [],
    error: null,
    loading: true,
  });

  const fetchStatus = useCallback(async () => {
    try {
      // Fetch status + leaderboard + recent runs in parallel
      const [statusRes, leaderboardRes, runsRes] = await Promise.all([
        fetch('/api/status').then(r => r.ok ? r.json() : null).catch(() => null),
        fetch('/api/leaderboard').then(r => r.ok ? r.json() : null).catch(() => null),
        fetch('/api/runs/recent?limit=20').then(r => r.ok ? r.json() : null).catch(() => null),
      ]);

      const s = statusRes ?? {};
      const lb = leaderboardRes ?? {};
      const recentRuns: RecentRun[] = (runsRes ?? []) as RecentRun[];

      // Count only truly running workflows (not queued/stale)
      const activeRuns = recentRuns.filter(r => r.status === 'running').length;
      const queueDepth = Number(s.queue_depth ?? s.queue_depth_total ?? 0);
      const queueDepthStatus = (s.queue_depth_status ?? (
        queueDepth >= 1000 ? 'critical' : queueDepth >= 500 ? 'warning' : 'ok'
      )) as SystemStatus['queueDepthStatus'];
      const queueDepthUtilizationPct = Number.isFinite(Number(s.queue_depth_utilization_pct))
        ? Number(s.queue_depth_utilization_pct)
        : Number(((queueDepth / 1000) * 100).toFixed(1));

      // Parse leaderboard
      const agents = (lb.agents ?? lb.leaderboard ?? []).map((a: any) => ({
        name: `${a.provider_slug ?? ''}/${a.model_slug ?? ''}`.replace(/^\//, ''),
        passRate: a.pass_rate ?? 0,
        avgLatency: a.avg_latency_ms ?? 0,
        totalCost: a.total_cost_usd ?? 0,
        workflows: a.total_workflows ?? 0,
      })).filter((a: any) => a.workflows > 0);

      const topAgent = agents.length > 0 ? agents[0].name : null;

      setStatus({
        modelsOnline: agents.length,
        activeRuns,
        queueDepth,
        queueDepthStatus,
        queueDepthUtilizationPct,
        queueDepthPending: Number(s.queue_depth_pending ?? 0),
        queueDepthReady: Number(s.queue_depth_ready ?? 0),
        queueDepthClaimed: Number(s.queue_depth_claimed ?? 0),
        queueDepthRunning: Number(s.queue_depth_running ?? 0),
        queueDepthError: typeof s.queue_depth_error === 'string' && s.queue_depth_error.trim()
          ? s.queue_depth_error
          : null,
        passRate24h: s.pass_rate ?? null,
        totalRuns24h: s.total_workflows ?? 0,
        totalCost24h: agents.reduce((sum: number, a: any) => sum + (a.totalCost || 0), 0),
        topAgent,
        agents,
        recentRuns,
        error: null,
        loading: false,
      });
    } catch (err: any) {
      setStatus(prev => ({ ...prev, error: err.message, loading: false }));
    }
  }, []);

  useEffect(() => {
    fetchStatus();
    const interval = setInterval(() => { if (!document.hidden) fetchStatus(); }, POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchStatus]);

  return status;
}
