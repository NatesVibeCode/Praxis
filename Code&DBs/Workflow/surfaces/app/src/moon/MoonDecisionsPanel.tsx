import React, { useCallback, useEffect, useMemo, useState } from 'react';

interface ScopeClamp {
  applies_to: string[];
  does_not_apply_to: string[];
}

interface DecisionRow {
  operator_decision_id: string;
  decision_key: string;
  decision_kind: string;
  decision_status: string;
  title: string;
  rationale: string;
  decided_by: string;
  decision_source: string;
  effective_from: string;
  effective_to: string | null;
  decided_at: string;
  decision_scope_kind: string | null;
  decision_scope_ref: string | null;
  scope_clamp: ScopeClamp;
}

const KINDS = [
  'all',
  'architecture_policy',
  'delivery_plan',
  'cutover_gate',
  'native_primary_cutover',
  'circuit_breaker_force_open',
  'circuit_breaker_force_closed',
  'circuit_breaker_reset',
  'binding',
  'query',
  'operator_graph',
  'dataset_promotion',
  'dataset_rejection',
  'dataset_promotion_supersede',
] as const;
type Kind = typeof KINDS[number];

async function fetchDecisions(kind: Kind, limit = 50): Promise<DecisionRow[]> {
  const input: Record<string, unknown> = { limit };
  if (kind !== 'all') input.decision_kind = kind;
  const resp = await fetch('/api/operate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ operation: 'operator.decision_list', input }),
  });
  let body: any = null;
  try {
    body = await resp.json();
  } catch {
    body = null;
  }
  if (!resp.ok) {
    throw new Error(body?.error || body?.detail || `HTTP ${resp.status}`);
  }
  const rows =
    body?.result?.operator_decisions ||
    body?.payload?.operator_decisions ||
    body?.operator_decisions ||
    [];
  return Array.isArray(rows) ? rows : [];
}

function isPendingReview(clamp: ScopeClamp | null | undefined): boolean {
  if (!clamp || !Array.isArray(clamp.applies_to)) return false;
  return clamp.applies_to.includes('pending_review');
}

function formatDate(iso: string | null | undefined): string {
  if (!iso) return '';
  try {
    return new Date(iso).toISOString().slice(0, 10);
  } catch {
    return iso;
  }
}

export function MoonDecisionsPanel() {
  const [kind, setKind] = useState<Kind>('architecture_policy');
  const [rows, setRows] = useState<DecisionRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [showOnlyPending, setShowOnlyPending] = useState(false);

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const decisions = await fetchDecisions(kind);
      setRows(decisions);
    } catch (exc: any) {
      setError(String(exc?.message || exc));
    } finally {
      setLoading(false);
    }
  }, [kind]);

  useEffect(() => {
    reload();
  }, [reload]);

  const filtered = useMemo(
    () => (showOnlyPending ? rows.filter((r) => isPendingReview(r.scope_clamp)) : rows),
    [rows, showOnlyPending],
  );

  const pendingCount = useMemo(
    () => rows.filter((r) => isPendingReview(r.scope_clamp)).length,
    [rows],
  );

  return (
    <details className="moon-decisions-panel">
      <summary className="moon-dock__section-label moon-decisions-panel__summary">
        Decisions
        {pendingCount > 0 && (
          <span className="moon-decisions-panel__pending-badge">
            {' '}· {pendingCount} pending review
          </span>
        )}
      </summary>

      <div className="moon-decisions-panel__controls">
        <select
          className="moon-decisions-panel__select"
          value={kind}
          onChange={(e) => setKind(e.target.value as Kind)}
        >
          {KINDS.map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
        </select>
        <label className="moon-decisions-panel__checkbox">
          <input
            type="checkbox"
            checked={showOnlyPending}
            onChange={(e) => setShowOnlyPending(e.target.checked)}
          />
          Pending only
        </label>
        <button
          type="button"
          className="moon-decisions-panel__refresh"
          onClick={reload}
          disabled={loading}
        >
          {loading ? 'Loading…' : 'Refresh'}
        </button>
      </div>

      {error && (
        <div className="moon-decisions-panel__error">scope_clamp fetch failed: {error}</div>
      )}

      <ul className="moon-decisions-panel__list">
        {filtered.map((row) => {
          const pending = isPendingReview(row.scope_clamp);
          const expanded = expandedId === row.operator_decision_id;
          const appliesTo = row.scope_clamp?.applies_to || [];
          const doesNotApplyTo = row.scope_clamp?.does_not_apply_to || [];
          return (
            <li
              key={row.operator_decision_id}
              className={`moon-decision-card${pending ? ' moon-decision-card--pending' : ''}`}
            >
              <div className="moon-decision-card__header">
                <div className="moon-decision-card__title">{row.title}</div>
                <div className="moon-decision-card__meta">
                  <span>{row.decision_kind}</span>
                  {row.decision_scope_ref && <span>· {row.decision_scope_ref}</span>}
                  <span>· {row.decided_by}</span>
                  <span>· {formatDate(row.decided_at)}</span>
                </div>
              </div>

              <div className="moon-decision-card__clamp">
                {pending ? (
                  <div className="moon-decision-card__clamp-pending">
                    ⚠ scope_clamp pending operator review — fill in applies_to /
                    does_not_apply_to so summaries cannot paraphrase the rationale away.
                  </div>
                ) : (
                  <>
                    {appliesTo.length > 0 && (
                      <div className="moon-decision-card__clamp-block">
                        <div className="moon-decision-card__clamp-label">Applies to</div>
                        <ul>
                          {appliesTo.map((s, i) => (
                            <li key={`a-${i}`}>{s}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                    {doesNotApplyTo.length > 0 && (
                      <div className="moon-decision-card__clamp-block moon-decision-card__clamp-block--negative">
                        <div className="moon-decision-card__clamp-label">Does NOT apply to</div>
                        <ul>
                          {doesNotApplyTo.map((s, i) => (
                            <li key={`d-${i}`}>{s}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </>
                )}
              </div>

              <div className="moon-decision-card__footer">
                <code className="moon-decision-card__key">{row.decision_key}</code>
                <button
                  type="button"
                  className="moon-decision-card__expand"
                  onClick={() =>
                    setExpandedId(expanded ? null : row.operator_decision_id)
                  }
                >
                  {expanded ? 'Hide rationale' : 'Show rationale'}
                </button>
              </div>

              {expanded && (
                <pre className="moon-decision-card__rationale">{row.rationale}</pre>
              )}
            </li>
          );
        })}
        {filtered.length === 0 && !loading && (
          <li className="moon-decisions-panel__empty">No decisions matched.</li>
        )}
      </ul>
    </details>
  );
}
