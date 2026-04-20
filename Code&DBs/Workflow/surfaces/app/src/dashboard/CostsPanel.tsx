import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { ChartView } from '../primitives/ChartView';
import { DataTable } from '../primitives/DataTable';
import { StatsRow } from '../primitives/StatsRow';
import { runsRecentPath } from './runApi';
import './dashboard.css';

interface CostSummary {
  total_cost_usd: number;
  total_input_tokens: number;
  total_output_tokens: number;
  cost_by_agent: Record<string, number>;
  record_count: number;
}

interface RecentRun {
  run_id: string;
  spec_name: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  total_jobs: number;
  completed_jobs: number;
  total_cost: number;
  created_at: string | null;
  finished_at: string | null;
}

interface CostsPanelProps {
  onBack: () => void;
  onViewRun: (runId: string) => void;
}

function formatCurrency(value: number): string {
  if (!Number.isFinite(value)) return '$0.00';
  if (value >= 100) return `$${value.toFixed(0)}`;
  if (value >= 10) return `$${value.toFixed(2)}`;
  return `$${value.toFixed(3)}`;
}

function formatTime(value: string | null): string {
  if (!value) return '—';
  return new Date(value).toLocaleString();
}

function useCostsSnapshot() {
  const [summary, setSummary] = useState<CostSummary | null>(null);
  const [recentRuns, setRecentRuns] = useState<RecentRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setError(null);
      const [summaryResponse, runsResponse] = await Promise.all([
        fetch('/api/costs'),
        fetch(runsRecentPath(12)),
      ]);
      if (!summaryResponse.ok) {
        throw new Error(`Cost summary unavailable (${summaryResponse.status})`);
      }
      if (!runsResponse.ok) {
        throw new Error(`Recent runs unavailable (${runsResponse.status})`);
      }
      const summaryPayload = (await summaryResponse.json()) as CostSummary;
      const runsPayload = (await runsResponse.json()) as RecentRun[];
      setSummary(summaryPayload);
      setRecentRuns(Array.isArray(runsPayload) ? runsPayload : []);
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : 'Costs surface unavailable.');
      setSummary(null);
      setRecentRuns([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => {
      if (!document.hidden) {
        void refresh();
      }
    }, 30000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  return { summary, recentRuns, loading, error, refresh };
}

export function CostsPanel({ onBack, onViewRun }: CostsPanelProps) {
  const { summary, recentRuns, loading, error, refresh } = useCostsSnapshot();

  const chartData = useMemo(
    () => Object.entries(summary?.cost_by_agent ?? {}).map(([label, value]) => ({ label, value })),
    [summary],
  );

  const topSpender = useMemo(() => {
    const entries = Object.entries(summary?.cost_by_agent ?? {});
    if (entries.length === 0) return null;
    return entries.reduce((best, current) => (current[1] > best[1] ? current : best));
  }, [summary]);

  const rows = useMemo(
    () => recentRuns.map((run) => ({
      run_id: run.run_id,
      workflow: run.spec_name,
      status: run.status,
      cost: formatCurrency(run.total_cost),
      jobs: `${run.completed_jobs}/${run.total_jobs}`,
      created_at: formatTime(run.created_at),
    })),
    [recentRuns],
  );

  return (
    <div className="om-editor">
      <div className="om-panel__header">
        <div>
          <div className="om-panel__eyebrow">Costs</div>
          <h1 className="om-panel__title">Cost Summary</h1>
          <p className="om-panel__caption">
            Live spend from <code>/api/costs</code> with recent run costs from the workflow ledger.
          </p>
        </div>
        <div className="om-panel__header-actions">
          <button type="button" className="om-btn om-btn--primary" onClick={refresh}>
            Refresh
          </button>
          <button type="button" className="om-btn" onClick={onBack}>
            Back to Overview
          </button>
        </div>
      </div>

      {error && <div className="om-advanced__warning">{error}</div>}

      <StatsRow
        stats={[
          {
            label: 'Total Cost',
            value: loading || !summary ? '...' : formatCurrency(summary.total_cost_usd),
            color: '#58a6ff',
          },
          {
            label: 'Input Tokens',
            value: loading || !summary ? '...' : summary.total_input_tokens.toLocaleString(),
          },
          {
            label: 'Output Tokens',
            value: loading || !summary ? '...' : summary.total_output_tokens.toLocaleString(),
          },
          {
            label: 'Ledger Records',
            value: loading || !summary ? '...' : summary.record_count.toLocaleString(),
          },
        ]}
      />

      <div className="om-review">
        <section className="om-panel">
          <div className="om-panel__header">
            <div>
              <div className="om-panel__eyebrow">Spend mix</div>
              <h2 className="om-panel__title">Cost by agent</h2>
            </div>
            <div className="om-panel__caption">
              {topSpender ? `${topSpender[0]} leads spend` : 'No ledger entries yet'}
            </div>
          </div>
          <ChartView
            chartType="pie"
            data={chartData}
            xKey="label"
            yKey="value"
          />
        </section>

        <section className="om-panel">
          <div className="om-panel__header">
            <div>
              <div className="om-panel__eyebrow">Recent runs</div>
              <h2 className="om-panel__title">Costed workflow runs</h2>
            </div>
            <div className="om-panel__caption">
              Click a row to open the run detail tab.
            </div>
          </div>
          <DataTable
            columns={[
              { key: 'workflow', label: 'Workflow' },
              { key: 'run_id', label: 'Run' },
              { key: 'cost', label: 'Cost' },
              { key: 'jobs', label: 'Jobs' },
              { key: 'status', label: 'Status' },
              { key: 'created_at', label: 'Created' },
            ]}
            data={rows}
            onRowClick={(row) => onViewRun(String(row.run_id || ''))}
          />
        </section>
      </div>
    </div>
  );
}
