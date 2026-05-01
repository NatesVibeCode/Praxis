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
import {
  EvidenceReader,
  EvidenceStack,
  StatusRail,
  TokenChip,
  type EvidenceItem,
  type EvidenceKind,
  type StatusRailItem,
} from '../primitives';

export interface RunEvidencePanelProps {
  runId: string;
  proof: ExecutionProof | null;
  status: 'idle' | 'loading' | 'ready' | 'error';
  error: string | null;
  onRefresh?: () => void;
}

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

function strengthTone(strength: ProofStrength): 'ok' | 'read' | 'locked' | 'bad' {
  if (strength === 'strong') return 'ok';
  if (strength === 'medium') return 'read';
  if (strength === 'weak') return 'locked';
  return 'bad';
}

function verdictTone(verdict: string): 'ok' | 'warn' | 'err' | 'dim' {
  if (verdict === 'fired_terminal') return 'ok';
  if (verdict === 'executing') return 'warn';
  if (verdict === 'not_fired') return 'dim';
  if (verdict === 'fired_but_stale') return 'warn';
  return 'dim';
}

function confidenceTone(confidence: string): 'ok' | 'warn' | 'err' | 'dim' {
  if (confidence === 'high') return 'ok';
  if (confidence === 'medium') return 'warn';
  if (confidence === 'low') return 'err';
  return 'dim';
}

function evidenceKindForSource(source: string): EvidenceKind {
  const key = source.toLowerCase();
  if (key.includes('receipt')) return 'receipt';
  if (key.includes('test') || key.includes('verifier')) return 'test';
  if (key.includes('bug') || key.includes('antipattern')) return 'bug';
  if (key.includes('decision') || key.includes('authority')) return 'decision';
  return 'run';
}

function evidenceBody(ev: ProofEvidence): string {
  if (!ev.details) return ev.present ? 'Evidence source reported present.' : 'Evidence source is missing.';
  return JSON.stringify(ev.details, null, 2);
}

function StrengthBadge({ strength }: { strength: ProofStrength }) {
  return (
    <TokenChip
      className="run-evidence__strength-badge"
      source="derived"
      tone={strengthTone(strength)}
    >
      {strength}
    </TokenChip>
  );
}

function LlmInvocationRow({ details }: { details: LlmInvocationDetails }) {
  const silent = details.zero_token_succeeded_jobs > 0 && details.jobs_with_tokens < details.llm_job_count;
  const items: StatusRailItem[] = [
    {
      label: 'tokens',
      value: `${formatTokens(details.total_token_input)} in / ${formatTokens(details.total_token_output)} out`,
    },
    { label: 'cost', value: formatCost(details.total_cost_usd) },
    {
      label: 'jobs',
      value: `${details.jobs_with_tokens}/${details.llm_job_count}`,
      tone: details.jobs_with_tokens === details.llm_job_count ? 'ok' : 'warn',
    },
  ];
  if (details.zero_token_succeeded_jobs > 0) {
    items.push({
      label: 'zero-token',
      value: details.zero_token_succeeded_jobs,
      tone: 'warn',
    });
  }
  return (
    <div className="run-evidence__llm">
      <StatusRail className="run-evidence__llm-stats" items={items} />
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
    <TokenChip
      className="run-evidence__antipattern-chip"
      source="derived"
      tone="bad"
      style={{
        marginRight: 8,
        marginBottom: 4,
      }}
    >
      <span style={{ fontWeight: 500 }}>{hit.rule_slug}</span>
      <span style={{ opacity: 0.85 }}>· {hit.resolved_agent}</span>
      <span style={{ opacity: 0.85 }}>· streak {hit.streak_count}</span>
      {hit.detected_at && <span style={{ opacity: 0.65 }}>· {formatRelative(hit.detected_at)}</span>}
    </TokenChip>
  );
}

export function RunEvidencePanel({ runId, proof, status, error, onRefresh }: RunEvidencePanelProps) {
  const [selectedEvidenceIndex, setSelectedEvidenceIndex] = useState(0);

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
  const evidenceItems: EvidenceItem[] = proof.evidence.map((ev) => ({
    kind: evidenceKindForSource(ev.source),
    title: ev.source,
    meta: (
      <>
        {ev.present ? 'present' : 'missing'} · <StrengthBadge strength={ev.proof_strength} />
      </>
    ),
    body: evidenceBody(ev),
  }));
  const safeSelectedEvidenceIndex = Math.min(
    selectedEvidenceIndex,
    Math.max(0, proof.evidence.length - 1),
  );
  const selectedEvidence = proof.evidence[safeSelectedEvidenceIndex];

  return (
    <div className="run-evidence">
      <div
        className="run-evidence__header"
        style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 8 }}
      >
        <div className="run-evidence__title" style={{ fontWeight: 600, fontSize: '1rem' }}>
          Evidence
        </div>
        <StatusRail
          className="run-evidence__proof-rail"
          items={[
            { label: 'run', value: runId.slice(0, 8), tone: 'dim' },
            { label: 'verdict', value: proof.verdict, tone: verdictTone(proof.verdict) },
            { label: 'confidence', value: proof.confidence, tone: confidenceTone(proof.confidence) },
          ]}
        />
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
            Open anti-pattern hits ({hits.length})
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
              <TokenChip
                key={m}
                source="derived"
                tone="locked"
              >
                {m}
              </TokenChip>
            ))}
          </div>
        </div>
      )}

      <div className="run-evidence__sources">
        <div style={{ fontWeight: 500, marginBottom: 6, fontSize: '0.85rem' }}>
          Authority sources ({proof.evidence.length})
        </div>
        {evidenceItems.length > 0 ? (
          <>
            <EvidenceStack
              items={evidenceItems}
              selectedIndex={safeSelectedEvidenceIndex}
              onSelect={(_item, index) => setSelectedEvidenceIndex(index)}
            />
            {selectedEvidence ? (
              <EvidenceReader
                cap={selectedEvidence.present ? 'selected evidence' : 'missing evidence'}
                title={selectedEvidence.source}
                body={evidenceBody(selectedEvidence)}
                style={{
                  marginTop: 8,
                  maxHeight: 260,
                  overflow: 'auto',
                  whiteSpace: 'pre-wrap',
                  fontFamily: 'var(--font-mono)',
                  fontSize: '0.75rem',
                }}
              />
            ) : null}
          </>
        ) : (
          <div style={{ opacity: 0.7, fontSize: '0.85rem' }}>No authority sources reported.</div>
        )}
      </div>
    </div>
  );
}
