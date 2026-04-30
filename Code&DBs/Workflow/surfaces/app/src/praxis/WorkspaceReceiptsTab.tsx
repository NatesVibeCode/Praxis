import React, { useEffect, useMemo, useState } from 'react';
import { emitPraxisOpenTab } from './events';

export interface WorkspaceRunRow {
  manifest_id?: string;
  workflow_id: string;
  run_id: string;
  run_status?: string;
  terminal_reason_code?: string | null;
  requested_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
  latest_receipt_id?: string | null;
  latest_receipt_status?: string | null;
  latest_failure_code?: string | null;
  latest_receipt_at?: string | null;
}

interface WorkspaceReceiptRow {
  receipt_id: string;
  workflow_id: string;
  run_id: string;
  node_id?: string | null;
  attempt_no?: number | null;
  started_at?: string;
  finished_at?: string;
  executor_type?: string;
  status?: string;
  failure_code?: string | null;
  run_status?: string | null;
  terminal_reason_code?: string | null;
  inputs?: Record<string, unknown>;
  outputs?: Record<string, unknown>;
  artifacts?: Record<string, unknown>;
}

type Verdict = 'sealed' | 'repaired' | 'rejected' | 'pending';
type Filter = 'all' | Verdict;

interface WorkspaceReceiptsTabProps {
  manifestId: string;
  initialRuns?: WorkspaceRunRow[];
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function text(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

function numberValue(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function formatTime(value: string | null | undefined): string {
  if (!value) return 'No timestamp';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  }).format(date);
}

function compactId(value: string | null | undefined): string {
  if (!value) return 'none';
  return value.length > 18 ? `${value.slice(0, 10)}...${value.slice(-6)}` : value;
}

function verdictForReceipt(row: WorkspaceReceiptRow): Verdict {
  const status = text(row.status).toLowerCase();
  const failureCode = text(row.failure_code).trim();
  const outputs = asRecord(row.outputs);
  const verificationStatus = text(outputs.verification_status).toLowerCase();
  if (failureCode || ['failed', 'error', 'cancelled', 'rejected'].includes(status)) return 'rejected';
  if (status.includes('repair') || verificationStatus === 'repaired') return 'repaired';
  if (['success', 'succeeded', 'passed', 'completed', 'ok'].includes(status)) return 'sealed';
  return 'pending';
}

function receiptCost(row: WorkspaceReceiptRow): number {
  const outputs = asRecord(row.outputs);
  const inputs = asRecord(row.inputs);
  return numberValue(outputs.cost_usd) || numberValue(inputs.cost_usd);
}

function receiptTokens(row: WorkspaceReceiptRow): number {
  const outputs = asRecord(row.outputs);
  const inputs = asRecord(row.inputs);
  return (
    numberValue(outputs.input_tokens)
    + numberValue(outputs.output_tokens)
    + numberValue(inputs.input_tokens)
    + numberValue(inputs.output_tokens)
  );
}

export function WorkspaceReceiptsTab({ manifestId, initialRuns = [] }: WorkspaceReceiptsTabProps) {
  const [runs, setRuns] = useState<WorkspaceRunRow[]>(initialRuns);
  const [receipts, setReceipts] = useState<WorkspaceReceiptRow[]>([]);
  const [filter, setFilter] = useState<Filter>('all');
  const [selectedReceiptId, setSelectedReceiptId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const [runsResponse, receiptsResponse] = await Promise.all([
          fetch(`/api/workspaces/${encodeURIComponent(manifestId)}/runs?limit=100`),
          fetch(`/api/workspaces/${encodeURIComponent(manifestId)}/receipts?limit=200`),
        ]);
        const runsPayload = await runsResponse.json().catch(() => null);
        const receiptsPayload = await receiptsResponse.json().catch(() => null);
        if (!runsResponse.ok) throw new Error(runsPayload?.detail || runsPayload?.error || 'Runs could not load');
        if (!receiptsResponse.ok) throw new Error(receiptsPayload?.detail || receiptsPayload?.error || 'Receipts could not load');
        if (cancelled) return;
        const nextRuns = Array.isArray(runsPayload?.items) ? runsPayload.items as WorkspaceRunRow[] : [];
        const nextReceipts = Array.isArray(receiptsPayload?.items) ? receiptsPayload.items as WorkspaceReceiptRow[] : [];
        setRuns(nextRuns);
        setReceipts(nextReceipts);
        setSelectedReceiptId((current) => current && nextReceipts.some((item) => item.receipt_id === current)
          ? current
          : nextReceipts[0]?.receipt_id ?? null);
      } catch (loadError) {
        if (!cancelled) setError(loadError instanceof Error ? loadError.message : 'Receipts could not load');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [manifestId]);

  const filteredReceipts = useMemo(() => (
    filter === 'all'
      ? receipts
      : receipts.filter((receipt) => verdictForReceipt(receipt) === filter)
  ), [filter, receipts]);

  const selectedReceipt = useMemo(() => (
    filteredReceipts.find((receipt) => receipt.receipt_id === selectedReceiptId)
    ?? filteredReceipts[0]
    ?? null
  ), [filteredReceipts, selectedReceiptId]);

  const totals = useMemo(() => {
    const cost = receipts.reduce((sum, receipt) => sum + receiptCost(receipt), 0);
    const tokens = receipts.reduce((sum, receipt) => sum + receiptTokens(receipt), 0);
    const rejected = receipts.filter((receipt) => verdictForReceipt(receipt) === 'rejected').length;
    const sealed = receipts.filter((receipt) => verdictForReceipt(receipt) === 'sealed').length;
    return { cost, tokens, rejected, sealed };
  }, [receipts]);

  const latestRun = runs[0] ?? null;

  const moveSelection = (direction: 1 | -1) => {
    if (!filteredReceipts.length) return;
    const currentIndex = Math.max(0, filteredReceipts.findIndex((receipt) => receipt.receipt_id === selectedReceipt?.receipt_id));
    const nextIndex = Math.min(filteredReceipts.length - 1, Math.max(0, currentIndex + direction));
    setSelectedReceiptId(filteredReceipts[nextIndex]?.receipt_id ?? null);
  };

  return (
    <div
      className="workspace-receipts"
      tabIndex={0}
      onKeyDown={(event) => {
        if (event.key === 'j' || event.key === 'ArrowDown') {
          event.preventDefault();
          moveSelection(1);
        }
        if (event.key === 'k' || event.key === 'ArrowUp') {
          event.preventDefault();
          moveSelection(-1);
        }
      }}
    >
      <section className="workspace-receipts__list" aria-label="Workspace receipts">
        <div className="workspace-receipts__toolbar">
          <div>
            <div className="workspace-receipts__eyebrow">Receipts</div>
            <h2>Proof of work</h2>
          </div>
          <div className="workspace-receipts__filters">
            {(['all', 'sealed', 'repaired', 'rejected'] as Filter[]).map((entry) => (
              <button
                key={entry}
                type="button"
                className={entry === filter ? 'workspace-receipts__filter workspace-receipts__filter--active' : 'workspace-receipts__filter'}
                onClick={() => {
                  setFilter(entry);
                  setSelectedReceiptId(null);
                }}
              >
                {entry}
              </button>
            ))}
          </div>
        </div>

        {loading ? (
          <div className="workspace-receipts__empty">Loading receipts...</div>
        ) : error ? (
          <div className="workspace-receipts__empty workspace-receipts__empty--error">{error}</div>
        ) : filteredReceipts.length === 0 ? (
          <div className="workspace-receipts__empty">No matching receipts for this workspace.</div>
        ) : (
          <div className="workspace-receipts__rows">
            {filteredReceipts.map((receipt) => {
              const verdict = verdictForReceipt(receipt);
              const active = selectedReceipt?.receipt_id === receipt.receipt_id;
              return (
                <button
                  key={receipt.receipt_id}
                  type="button"
                  className={[
                    'workspace-receipts__row',
                    active ? 'workspace-receipts__row--active' : '',
                    `workspace-receipts__row--${verdict}`,
                  ].filter(Boolean).join(' ')}
                  onClick={() => setSelectedReceiptId(receipt.receipt_id)}
                >
                  <span className="workspace-receipts__verdict">{verdict}</span>
                  <span className="workspace-receipts__row-main">
                    <strong>{receipt.node_id || receipt.executor_type || 'Workflow receipt'}</strong>
                    <small>{compactId(receipt.receipt_id)} / {formatTime(receipt.finished_at || receipt.started_at)}</small>
                  </span>
                  <span className="workspace-receipts__row-run">{compactId(receipt.run_id)}</span>
                </button>
              );
            })}
          </div>
        )}
      </section>

      <aside className="workspace-receipts__detail" aria-label="Receipt detail">
        <div className="workspace-receipts__summary">
          <div>
            <span>runs</span>
            <strong>{runs.length}</strong>
          </div>
          <div>
            <span>sealed</span>
            <strong>{totals.sealed}</strong>
          </div>
          <div>
            <span>rejected</span>
            <strong>{totals.rejected}</strong>
          </div>
          <div>
            <span>cost</span>
            <strong>{totals.cost ? `$${totals.cost.toFixed(4)}` : '-'}</strong>
          </div>
        </div>

        {latestRun ? (
          <div className="workspace-receipts__run">
            <div className="workspace-receipts__eyebrow">Latest run</div>
            <strong>{latestRun.run_status || 'recorded'}</strong>
            <span>{compactId(latestRun.run_id)} / {formatTime(latestRun.started_at || latestRun.requested_at)}</span>
            <button
              type="button"
              className="workspace-receipts__open-run"
              onClick={() => emitPraxisOpenTab({ kind: 'run-detail', runId: latestRun.run_id })}
            >
              Open run
            </button>
          </div>
        ) : null}

        {selectedReceipt ? (
          <div className="workspace-receipts__receipt-detail">
            <div className="workspace-receipts__eyebrow">Selected receipt</div>
            <h3>{selectedReceipt.node_id || selectedReceipt.executor_type || 'Receipt'}</h3>
            <dl>
              <div>
                <dt>verdict</dt>
                <dd>{verdictForReceipt(selectedReceipt)}</dd>
              </div>
              <div>
                <dt>status</dt>
                <dd>{selectedReceipt.status || '-'}</dd>
              </div>
              <div>
                <dt>receipt</dt>
                <dd>{selectedReceipt.receipt_id}</dd>
              </div>
              <div>
                <dt>workflow</dt>
                <dd>{selectedReceipt.workflow_id}</dd>
              </div>
              <div>
                <dt>tokens</dt>
                <dd>{receiptTokens(selectedReceipt) || '-'}</dd>
              </div>
            </dl>
            {selectedReceipt.failure_code ? (
              <div className="workspace-receipts__residue">{selectedReceipt.failure_code}</div>
            ) : null}
          </div>
        ) : (
          <div className="workspace-receipts__empty">Select a receipt to inspect.</div>
        )}
      </aside>
    </div>
  );
}
