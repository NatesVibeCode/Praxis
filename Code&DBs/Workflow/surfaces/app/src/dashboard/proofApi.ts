// REST contract for run-level execution proof.
// Backend: GET /api/runs/{run_id}/proof, delegates to operator.execution_proof
// via the catalog gateway. Returns the same JSON shape as the MCP path.
//
// Lazy-fetched (not inlined into /api/runs/{id}) because the proof query
// runs 10+ aggregation queries across workflow_jobs, receipts, outbox,
// authority_events, build_antipattern_hits, etc. — only worth computing
// when an operator opens a run-detail view.

export type ProofStrength = 'strong' | 'medium' | 'weak' | 'missing';

export interface ProofEvidence {
  source: string;
  present: boolean;
  proof_strength: ProofStrength;
  details: Record<string, unknown> | null;
}

export interface AntipatternHit {
  rule_slug: string;
  resolved_agent: string;
  provider_slug: string | null;
  model_slug: string | null;
  streak_count: number;
  latest_finished_at: string | null;
  detected_at: string | null;
  remediation_action: string | null;
}

export interface LlmInvocationDetails {
  llm_job_count: number;
  jobs_with_tokens: number;
  zero_token_succeeded_jobs: number;
  total_token_input: number;
  total_token_output: number;
  total_tokens: number;
  total_cost_usd: number;
  agents: string[];
}

export type RunVerdict =
  | 'executing'
  | 'fired_terminal'
  | 'fired_but_stale'
  | 'not_fired'
  | string;

export interface ExecutionProof {
  view: 'execution_proof';
  run_id: string;
  verdict: RunVerdict;
  confidence: 'high' | 'medium' | 'low' | string;
  fired: boolean;
  currently_executing: boolean;
  stale_after_seconds: number;
  evidence: ProofEvidence[];
  missing_evidence: string[];
  authority_sources: string[];
  query_errors?: unknown;
  recommended_next_action?: { action: string; reason?: string } | null;
}

export function executionProofPath(runId: string): string {
  return `/api/runs/${encodeURIComponent(runId)}/proof`;
}

/** Look up an evidence row by source name. Returns undefined if absent. */
export function findEvidence(
  proof: ExecutionProof | null | undefined,
  source: string,
): ProofEvidence | undefined {
  if (!proof) return undefined;
  return proof.evidence.find((e) => e.source === source);
}

/** Type-narrow the llm_invocation evidence's details. */
export function llmInvocation(proof: ExecutionProof | null | undefined): LlmInvocationDetails | null {
  const ev = findEvidence(proof, 'llm_invocation');
  if (!ev || !ev.details) return null;
  return ev.details as unknown as LlmInvocationDetails;
}

/** Pull the open antipattern hits attached to this run's agents. */
export function antipatternHits(proof: ExecutionProof | null | undefined): AntipatternHit[] {
  const ev = findEvidence(proof, 'build_antipattern_hits');
  if (!ev || !ev.details) return [];
  const details = ev.details as { hits?: AntipatternHit[] };
  return details.hits || [];
}
