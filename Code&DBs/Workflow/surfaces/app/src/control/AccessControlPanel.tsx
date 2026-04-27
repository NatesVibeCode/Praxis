import { useCallback, useEffect, useMemo, useState } from 'react';

interface DenialRow {
  runtime_profile_ref: string;
  job_type: string;
  transport_type: string;
  adapter_type: string;
  provider_slug: string;
  model_slug: string;
  denied: boolean;
  reason_code: string;
  operator_message: string;
  decision_ref: string;
  updated_at?: string | null;
}

interface MatrixRow {
  runtime_profile_ref: string;
  job_type: string;
  transport_type: string;
  provider_slug: string;
  model_slug: string;
  control_state: 'on' | 'off';
}

type Selector = {
  runtime_profile_ref: string;
  transport_type: '*' | 'CLI' | 'API';
  job_type: string;
  adapter_type: string;
  provider_slug: string;
  model_slug: string;
};

const DEFAULT_SELECTOR: Selector = {
  runtime_profile_ref: 'praxis',
  transport_type: '*',
  job_type: '*',
  adapter_type: '*',
  provider_slug: '*',
  model_slug: '*',
};

async function callOperate(operation: string, input: Record<string, unknown>): Promise<any> {
  const resp = await fetch('/api/operate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ operation, input }),
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
  return body?.result || body?.payload || body || {};
}

async function fetchMatrix(selector: Selector): Promise<MatrixRow[]> {
  const input: Record<string, unknown> = {
    runtime_profile_ref: selector.runtime_profile_ref,
    limit: 1000,
  };
  if (selector.transport_type !== '*') input.transport_type = selector.transport_type;
  if (selector.provider_slug !== '*') input.provider_slug = selector.provider_slug;
  if (selector.job_type !== '*') input.job_type = selector.job_type;
  if (selector.model_slug !== '*') input.model_slug = selector.model_slug;
  const result = await callOperate('operator.model_access_control_matrix', input);
  const rows = result?.rows || result?.matrix || [];
  return Array.isArray(rows) ? (rows as MatrixRow[]) : [];
}

async function fetchDenials(selector: Selector): Promise<DenialRow[]> {
  const result = await callOperate('access_control', { action: 'list', ...selector });
  const rows = result?.rows || [];
  return Array.isArray(rows) ? (rows as DenialRow[]) : [];
}

async function setDenial(selector: Selector, denied: boolean, decisionRef: string): Promise<void> {
  const action = denied ? 'disable' : 'enable';
  const input: Record<string, unknown> = { action, ...selector };
  if (denied) input.decision_ref = decisionRef;
  await callOperate('access_control', input);
}

function uniqueProviders(rows: MatrixRow[]): string[] {
  return Array.from(new Set(rows.map((r) => r.provider_slug))).sort();
}

interface ProviderCellProps {
  provider: string;
  transport: 'CLI' | 'API';
  effectiveOn: boolean;
  hasDenial: boolean;
  busy: boolean;
  onToggle: (nextDenied: boolean) => void;
}

function ProviderCell({ provider, transport, effectiveOn, hasDenial, busy, onToggle }: ProviderCellProps) {
  const checked = !hasDenial && effectiveOn;
  const title = hasDenial
    ? `${provider} / ${transport} is turned OFF at the control panel`
    : effectiveOn
      ? `${provider} / ${transport} is ON (no denial row)`
      : `${provider} / ${transport} has no admitted route`;
  return (
    <td className="acp__cell" title={title}>
      <input
        type="checkbox"
        checked={checked}
        disabled={busy}
        onChange={(ev) => onToggle(!ev.target.checked)}
        aria-label={`${provider} ${transport} access`}
      />
    </td>
  );
}

export function AccessControlPanel() {
  const [selector, setSelector] = useState<Selector>(DEFAULT_SELECTOR);
  const [matrix, setMatrix] = useState<MatrixRow[]>([]);
  const [denials, setDenials] = useState<DenialRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [decisionRef, setDecisionRef] = useState<string>('decision.operator.control-panel-toggle');

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [m, d] = await Promise.all([fetchMatrix(selector), fetchDenials(selector)]);
      setMatrix(m);
      setDenials(d);
    } catch (exc: any) {
      setError(String(exc?.message || exc));
    } finally {
      setLoading(false);
    }
  }, [selector]);

  useEffect(() => {
    reload();
  }, [reload]);

  const providers = useMemo(() => uniqueProviders(matrix), [matrix]);

  const denialIndex = useMemo(() => {
    const idx = new Map<string, DenialRow>();
    for (const d of denials) {
      const key = `${d.provider_slug}|${d.transport_type}|${d.job_type}|${d.model_slug}`;
      idx.set(key, d);
    }
    return idx;
  }, [denials]);

  const onIndex = useMemo(() => {
    const idx = new Set<string>();
    for (const r of matrix) {
      if (r.control_state === 'on') {
        idx.add(`${r.provider_slug}|${r.transport_type}`);
      }
    }
    return idx;
  }, [matrix]);

  const handleToggle = useCallback(
    async (provider: string, transport: 'CLI' | 'API', nextDenied: boolean) => {
      const cellKey = `${provider}|${transport}`;
      setBusy(cellKey);
      setError(null);
      try {
        await setDenial(
          {
            runtime_profile_ref: selector.runtime_profile_ref,
            transport_type: transport,
            provider_slug: provider,
            job_type: '*',
            adapter_type: '*',
            model_slug: '*',
          },
          nextDenied,
          decisionRef,
        );
        await reload();
      } catch (exc: any) {
        setError(String(exc?.message || exc));
      } finally {
        setBusy(null);
      }
    },
    [selector.runtime_profile_ref, decisionRef, reload],
  );

  return (
    <section className="access-control-panel acp">
      <header className="acp__header">
        <h2 className="acp__title">Model access control panel</h2>
        <p className="acp__subtitle">
          Each checkbox is a denial row in <code>private_provider_model_access_denials</code>.
          Unchecking calls <code>access_control(disable)</code>; checking calls{' '}
          <code>access_control(enable)</code>. Both refresh the projection automatically.
        </p>
      </header>

      <div className="acp__controls">
        <label className="acp__field">
          <span>Runtime profile</span>
          <input
            value={selector.runtime_profile_ref}
            onChange={(ev) =>
              setSelector((s) => ({ ...s, runtime_profile_ref: ev.target.value || 'praxis' }))
            }
          />
        </label>
        <label className="acp__field">
          <span>Decision ref</span>
          <input
            value={decisionRef}
            onChange={(ev) => setDecisionRef(ev.target.value)}
            placeholder="decision.YYYY-MM-DD.your-decision"
          />
        </label>
        <button type="button" className="acp__refresh" onClick={reload} disabled={loading}>
          {loading ? 'Loading…' : 'Refresh'}
        </button>
      </div>

      {error ? <div className="acp__error">{error}</div> : null}

      <table className="acp__matrix">
        <thead>
          <tr>
            <th>Provider</th>
            <th>CLI</th>
            <th>API</th>
          </tr>
        </thead>
        <tbody>
          {providers.length === 0 && !loading ? (
            <tr>
              <td colSpan={3} className="acp__empty">
                No providers in the matrix for this filter.
              </td>
            </tr>
          ) : null}
          {providers.map((provider) => {
            const cliKey = `${provider}|CLI|*|*`;
            const apiKey = `${provider}|API|*|*`;
            const cliDenied = denialIndex.has(cliKey);
            const apiDenied = denialIndex.has(apiKey);
            const cliEffectiveOn = onIndex.has(`${provider}|CLI`);
            const apiEffectiveOn = onIndex.has(`${provider}|API`);
            const cliBusyKey = `${provider}|CLI`;
            const apiBusyKey = `${provider}|API`;
            return (
              <tr key={provider}>
                <th scope="row">{provider}</th>
                <ProviderCell
                  provider={provider}
                  transport="CLI"
                  effectiveOn={cliEffectiveOn}
                  hasDenial={cliDenied}
                  busy={busy === cliBusyKey}
                  onToggle={(nextDenied) => handleToggle(provider, 'CLI', nextDenied)}
                />
                <ProviderCell
                  provider={provider}
                  transport="API"
                  effectiveOn={apiEffectiveOn}
                  hasDenial={apiDenied}
                  busy={busy === apiBusyKey}
                  onToggle={(nextDenied) => handleToggle(provider, 'API', nextDenied)}
                />
              </tr>
            );
          })}
        </tbody>
      </table>

      {denials.length > 0 ? (
        <details className="acp__denials">
          <summary>{denials.length} active denial row(s)</summary>
          <ul>
            {denials.map((d, idx) => (
              <li key={`${d.provider_slug}|${d.transport_type}|${d.job_type}|${d.model_slug}|${idx}`}>
                <code>
                  {d.provider_slug} / {d.transport_type} / {d.job_type} / {d.model_slug}
                </code>{' '}
                — <span className="acp__decision-ref">{d.decision_ref}</span>
              </li>
            ))}
          </ul>
        </details>
      ) : null}
    </section>
  );
}
