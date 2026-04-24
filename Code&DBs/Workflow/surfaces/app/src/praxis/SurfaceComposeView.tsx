import React, { useEffect, useMemo, useState } from 'react';
import { QuadrantGrid } from '../grid/QuadrantGrid';
import {
  normalizePraxisBundle,
  resolvePraxisBundleSurface,
  resolvePraxisBundleTab,
  type PraxisSurfaceBundleV4,
} from './manifest';

interface SurfaceComposeViewProps {
  intent: string | null | undefined;
  pillRefs: string[] | null | undefined;
}

interface RankedTemplate {
  template_ref: string;
  legal: boolean;
  rank?: number | null;
  specificity?: number;
  binding_weight?: number;
  reason?: string;
  template_name?: string;
  bound_slots?: Array<{
    slot_name: string;
    ordinal: number;
    slot_type: string;
    pill_ref: string;
    lattice_depth: number;
  }>;
}

interface TypedGap {
  gap_kind: string;
  reason_code: string;
  intent_ref?: string;
  pill_refs?: string[];
  candidate_template_refs?: string[];
  legal_repair_actions?: Array<Record<string, unknown>>;
}

interface LegalTemplatesOutput {
  intent_ref: string | null;
  pill_refs: string[];
  ranked_templates: RankedTemplate[];
  winner: string | null;
  compiled_bundle: PraxisSurfaceBundleV4 | null;
  typed_gap: TypedGap | null;
}

interface ProjectionEnvelope {
  projection_ref: string;
  output: LegalTemplatesOutput | null;
  last_event_id: string | null;
  last_receipt_id: string | null;
  last_refreshed_at: string | null;
  freshness_status: string;
  warnings: string[];
}

const PROJECTION_REF = 'projection.surface.legal_templates';

export function SurfaceComposeView({ intent, pillRefs }: SurfaceComposeViewProps) {
  const [envelope, setEnvelope] = useState<ProjectionEnvelope | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const pillsKey = useMemo(() => (pillRefs ?? []).join('|'), [pillRefs]);

  useEffect(() => {
    if (!intent) {
      setLoading(false);
      setError('compose requires an intent query param');
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    const load = async () => {
      const params = new URLSearchParams();
      params.set('intent', intent);
      for (const pill of pillRefs ?? []) {
        if (pill) params.append('pill', pill);
      }
      try {
        const response = await fetch(`/api/projections/${PROJECTION_REF}?${params.toString()}`);
        const payload = await response.json().catch(() => null) as ProjectionEnvelope | null;
        if (!response.ok) {
          throw new Error(`projection fetch failed: ${response.status} ${response.statusText}`);
        }
        if (!cancelled && payload) setEnvelope(payload);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [intent, pillsKey, pillRefs]);

  if (loading) {
    return (
      <div className="app-shell__fallback">
        <div className="app-shell__fallback-kicker">Compose</div>
        <div className="app-shell__fallback-title">
          Resolving legal template for {intent || 'unknown intent'}…
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="app-shell__fallback app-shell__fallback--error">
        <div className="app-shell__fallback-kicker">Compose unavailable</div>
        <div className="app-shell__fallback-title">{error}</div>
      </div>
    );
  }

  const output = envelope?.output ?? null;

  if (!output) {
    return (
      <div className="app-shell__fallback app-shell__fallback--error">
        <div className="app-shell__fallback-kicker">Compose unavailable</div>
        <div className="app-shell__fallback-title">
          projection.surface.legal_templates returned no output
        </div>
        {envelope?.warnings.length ? (
          <pre style={{ whiteSpace: 'pre-wrap' }}>{envelope.warnings.join('\n')}</pre>
        ) : null}
      </div>
    );
  }

  if (output.typed_gap || !output.compiled_bundle) {
    return <ComposeTypedGapCard output={output} />;
  }

  const bundle = normalizePraxisBundle(output.compiled_bundle, {
    id: `compose:${output.intent_ref ?? 'unknown'}`,
    title: output.compiled_bundle.title,
    description: output.compiled_bundle.description ?? `Compiled from ${output.intent_ref} + ${output.pill_refs.length} pill(s).`,
  });
  const selectedTab = resolvePraxisBundleTab(bundle, null);
  const selectedSurface = resolvePraxisBundleSurface(bundle, null);

  if (!selectedSurface) {
    return (
      <div className="app-shell__fallback app-shell__fallback--error">
        <div className="app-shell__fallback-kicker">Compose unavailable</div>
        <div className="app-shell__fallback-title">Compiled bundle is missing a default surface.</div>
      </div>
    );
  }

  const freshness = envelope?.freshness_status;
  const lastRefreshed = envelope?.last_refreshed_at;

  return (
    <div>
      <header className="app-shell__surface-header">
        <div className="app-shell__surface-heading">
          <div className="app-shell__fallback-kicker">Compose</div>
          <div className="app-shell__surface-title">{bundle.title}</div>
          <p className="app-shell__surface-copy">
            Winner: <code>{output.winner}</code> · {output.pill_refs.length} pill(s) bound ·
            freshness <code>{freshness}</code>
            {lastRefreshed ? ` · refreshed ${new Date(lastRefreshed).toLocaleTimeString()}` : ''}
          </p>
          {output.ranked_templates.length > 1 ? (
            <details>
              <summary>Ranking detail ({output.ranked_templates.length} candidates)</summary>
              <ul>
                {output.ranked_templates.map((t) => (
                  <li key={`${t.template_ref}:${t.legal}`}>
                    <code>{t.template_ref}</code>{' '}
                    {t.legal ? `legal · rank ${t.rank?.toFixed(2)} (specificity ${t.specificity} × weight ${t.binding_weight?.toFixed(2)})` : `illegal · ${t.reason}`}
                    {t === output.ranked_templates.find((x) => x.template_ref === output.winner) ? ' · winner' : ''}
                  </li>
                ))}
              </ul>
            </details>
          ) : null}
        </div>
      </header>
      <QuadrantGrid manifest={selectedSurface.manifest} saveTarget={null} />
    </div>
  );
}

function ComposeTypedGapCard({ output }: { output: LegalTemplatesOutput }) {
  const gap = output.typed_gap;
  return (
    <div style={{ padding: 'var(--space-lg)', maxWidth: 760, margin: '0 auto' }}>
      <div className="app-shell__fallback-kicker">Compose · typed gap</div>
      <h2 style={{ marginTop: 8 }}>No legal template for this intent</h2>
      <p style={{ color: 'var(--text-muted)' }}>
        {gap?.reason_code
          ? `Reason: ${gap.reason_code}`
          : 'projection.surface.legal_templates returned no compiled bundle.'}
      </p>
      {gap?.intent_ref ? (
        <p>
          Intent: <code>{gap.intent_ref}</code>
        </p>
      ) : null}
      {Array.isArray(gap?.pill_refs) && gap.pill_refs.length > 0 ? (
        <p>
          Bound pills:{' '}
          {gap.pill_refs.map((p) => (
            <code key={p} style={{ marginRight: 6 }}>
              {p}
            </code>
          ))}
        </p>
      ) : null}
      {Array.isArray(gap?.candidate_template_refs) && gap.candidate_template_refs.length > 0 ? (
        <p>Candidate templates considered but illegal for this state: {gap.candidate_template_refs.join(', ')}</p>
      ) : null}
      {Array.isArray(gap?.legal_repair_actions) && gap.legal_repair_actions.length > 0 ? (
        <div>
          <h3>Legal repair actions</h3>
          <pre style={{ whiteSpace: 'pre-wrap', background: 'var(--bg-card)', padding: 'var(--space-md)', borderRadius: 'var(--radius)' }}>
            {JSON.stringify(gap.legal_repair_actions, null, 2)}
          </pre>
        </div>
      ) : null}
    </div>
  );
}

export default SurfaceComposeView;
