import React, { useState } from 'react';
import {
  AntipatternHit,
  ExecutionProof,
  LlmInvocationDetails,
  ProofEvidence,
  ProofStrength,
  antipatternHits,
  llmInvocation,
} from './proofApi';

export interface RunEvidencePanelProps {
  runId: string;
  proof: ExecutionProof | null;
  status: 'idle' | 'loading' | 'ready' | 'error';
  error: string | null;
  onRefresh?: () => void;
}

const STRENGTH_BG: Record<ProofStrength, string> = {
  strong: 'var(--surface-success-soft)',
  medium: 'var(--surface-accent-soft)',
  weak: 'var(--surface-warning-soft)',
  missing: 'var(--surface-muted)',
};
const STRENGTH_FG: Record<ProofStrength, string> = {
  strong: 'var(--text-success)',
  medium: 'var(--text-accent)',
  weak: 'var(--text-warning)',
  missing: 'var(--text-muted)',
};

function formatTokens(n: number | null | undefined): string {
  if (!n || n <= 0) return '0';
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return String(n);
}

function formatCost(n: number | null | undefined): string {
  return `$${(n ?? 0).toFixed(2)}`;
}

function formatRelative(iso: string | null | undefined): string {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return iso;
  const ageSec = Math.max(0, (Date.now() - t) / 1000);
  if (ageSec < 60) return `${Math.round(ageSec)}s ago`;
  if (ageSec < 3600) return `${Math.round(ageSec / 60)}m ago`;
  if (ageSec < 86400) return `${Math.round(ageSec / 3600)}h ago`;
  return `${Math.round(ageSec / 86400)}d ago`;
}

function StrengthBadge({ strength }: { strength: ProofStrength }) {
  return (
    <span
      className="run-evidence__strength-badge"
      style={{
        background: STRENGTH_BG[strength],
        color: STRENGTH_FG[strength],
        padding: '2px 8px',
        borderRadius: 4,
        fontSize: '0.75rem',
        fontWeight: 500,
        textTransform: 'capitalize',
      }}
    >
      {strength}
    </span>
  );
}

function LlmInvocationRow({ details }: { details: LlmInvocationDetails }) {
  const silent = details.zero_token_succeeded_jobs > 0 && details.jobs_with_tokens < details.llm_job_count;
  return (
    <div className="run-evidence__llm">
      <div className="run-evidence__llm-stats" style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
        <div><strong>{formatTokens(details.total_token_input)}</strong> in / <strong>{formatTokens(details.total_token_output)}</strong> out</div>
        <div><strong>{formatCost(details.total_cost_usd)}</strong></div>
        <div>{details.jobs_with_tokens}/{details.llm_job_count} jobs with tokens</div>
        {details.zero_token_succeeded_jobs > 0 && (
          <div style={{ color: 'var(--text-warning)' }}>
            {details.zero_token_succeeded_jobs} zero-token successes
          </div>
        )}
      </div>
      {silent && (
        <div
          className="run-evidence__llm-banner"
          style={{
            marginTop: 8,
            padding: '6px 10px',
            background: 'var(--surface-warning-soft)',
            color: 'var(--text-warning)',
            borderRadius: 4,
            fontSize: '0.85rem',
          }}
        >
          ⚠ Silent failure mode: jobs succeeded without invoking the LLM. Provider may be failing auth or short-circuiting.
        </div>
      )}
      {details.agents.length > 0 && (
        <div style={{ marginTop: 8, fontSize: '0.8rem', opacity: 0.8 }}>
          Agents: {details.agents.join(', ')}
        </div>
      )}
    </div>
  );
}

function AntipatternHitChip({ hit }: { hit: AntipatternHit }) {
  return (
    <div
      className="run-evidence__antipattern-chip"
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 8,
        padding: '4px 10px',
        background: 'var(--surface-danger-soft)',
        color: 'var(--text-danger)',
        borderRadius: 4,
        fontSize: '0.85rem',
        marginRight: 8,
        marginBottom: 4,
      }}
    >
      <span style={{ fontWeight: 500 }}>{hit.rule_slug}</span>
      <span style={{ opacity: 0.85 }}>· {hit.resolved_agent}</span>
      <span style={{ opacity: 0.85 }}>· streak {hit.streak_count}</span>
      {hit.detected_at && <span style={{ opacity: 0.65 }}>· {formatRelative(hit.detected_at)}</span>}
    </div>
  );
}

function EvidenceRow({ ev }: { ev: ProofEvidence }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div
      className="run-evidence__row"
      style={{
        padding: '8px 0',
        borderBottom: '1px solid var(--surface-divider)',
      }}
    >
      <div
        style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer' }}
        onClick={() => setExpanded((v) => !v)}
      >
        <span style={{ width: 14, opacity: 0.7 }}>{ev.present ? '✓' : '✗'}</span>
        <span style={{ flex: 1, fontFamily: 'var(--font-mono)', fontSize: '0.85rem' }}>{ev.source}</span>
        <StrengthBadge strength={ev.proof_strength} />
        <span style={{ opacity: 0.5, fontSize: '0.75rem' }}>{expanded ? '▾' : '▸'}</span>
      </div>
      {expanded && ev.details && (
        <div
          style={{
            marginTop: 6,
            padding: '8px 10px',
            background: 'var(--surface-muted)',
            borderRadius: 4,
            fontFamily: 'var(--font-mono)',
            fontSize: '0.75rem',
            whiteSpace: 'pre-wrap',
            maxHeight: 240,
            overflow: 'auto',
          }}
        >
          {JSON.stringify(ev.details, null, 2)}
        </div>
      )}
    </div>
  );
}

export function RunEvidencePanel({ runId, proof, status, error, onRefresh }: RunEvidencePanelProps) {
  if (status === 'loading' && !proof) {
    return (
      <div className="run-evidence run-evidence--loading">
        <div className="run-evidence__title">Evidence</div>
        <div style={{ opacity: 0.7 }}>Loading proof…</div>
      </div>
    );
  }
  if (status === 'error') {
    return (
      <div className="run-evidence run-evidence--error">
        <div className="run-evidence__title">Evidence</div>
        <div style={{ color: 'var(--text-danger)' }}>{error || 'Failed to load proof.'}</div>
        {onRefresh && (
          <button type="button" onClick={onRefresh} style={{ marginTop: 8 }}>
            Retry
          </button>
        )}
      </div>
    );
  }
  if (!proof) {
    return null;
  }

  const llm = llmInvocation(proof);
  const hits = antipatternHits(proof);

  return (
    <div className="run-evidence">
      <div
        className="run-evidence__header"
        style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 8 }}
      >
        <div className="run-evidence__title" style={{ fontWeight: 600, fontSize: '1rem' }}>
          Evidence
        </div>
        <div style={{ opacity: 0.7, fontSize: '0.85rem' }}>
          verdict: <strong>{proof.verdict}</strong> · confidence: {proof.confidence}
        </div>
        {onRefresh && (
          <button
            type="button"
            onClick={onRefresh}
            style={{ marginLeft: 'auto', fontSize: '0.8rem' }}
          >
            Refresh
          </button>
        )}
      </div>

      {hits.length > 0 && (
        <div className="run-evidence__antipattern-banner" style={{ marginBottom: 12 }}>
          <div style={{ fontWeight: 500, marginBottom: 6 }}>
            ⚠ Open anti-pattern hits ({hits.length})
          </div>
          <div>
            {hits.map((h) => (
              <AntipatternHitChip key={`${h.rule_slug}:${h.resolved_agent}`} hit={h} />
            ))}
          </div>
        </div>
      )}

      {llm && (
        <div
          className="run-evidence__llm-section"
          style={{ marginBottom: 12, padding: '8px 10px', background: 'var(--surface-card)', borderRadius: 4 }}
        >
          <div style={{ fontWeight: 500, marginBottom: 6 }}>LLM invocation</div>
          <LlmInvocationRow details={llm} />
        </div>
      )}

      {proof.missing_evidence.length > 0 && (
        <div className="run-evidence__missing" style={{ marginBottom: 12 }}>
          <div style={{ fontWeight: 500, marginBottom: 4, fontSize: '0.85rem' }}>Missing evidence</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {proof.missing_evidence.map((m) => (
              <span
                key={m}
                style={{
                  padding: '2px 8px',
                  background: 'var(--surface-warning-soft)',
                  color: 'var(--text-warning)',
                  borderRadius: 4,
                  fontSize: '0.75rem',
                  fontFamily: 'var(--font-mono)',
                }}
              >
                {m}
              </span>
            ))}
          </div>
        </div>
      )}

      <div className="run-evidence__sources">
        <div style={{ fontWeight: 500, marginBottom: 6, fontSize: '0.85rem' }}>
          Authority sources ({proof.evidence.length})
        </div>
        {proof.evidence.map((ev) => (
          <EvidenceRow key={ev.source} ev={ev} />
        ))}
      </div>
    </div>
  );
}
