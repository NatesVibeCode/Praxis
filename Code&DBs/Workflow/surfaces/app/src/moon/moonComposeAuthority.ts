export type AuthorityStatus = 'loading' | 'ready' | 'degraded';

export interface MoonComposeAuthoritySummary {
  status: AuthorityStatus;
  buildControlCount: number | null;
  atlasFreshness: string | null;
  sourceAuthority: string | null;
  warning: string | null;
}

export interface ComposeAuthorityInput {
  prose: string;
  triggerLabel?: string | null;
  summary?: MoonComposeAuthoritySummary | null;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

export function readUiExperiencePayload(value: unknown): Record<string, unknown> | null {
  const envelope = asRecord(value);
  if (!envelope) return null;
  const result = asRecord(envelope.result);
  if (result) return asRecord(result.payload) ?? result;
  return asRecord(envelope.payload) ?? envelope;
}

export function summarizeComposeAuthority(
  uiExperienceGraph: unknown,
  atlasPayload: unknown,
): MoonComposeAuthoritySummary {
  const uiPayload = readUiExperiencePayload(uiExperienceGraph);
  const atlas = asRecord(atlasPayload);
  const counts = asRecord(uiPayload?.counts);
  const metadata = asRecord(atlas?.metadata);
  const freshness = asRecord(metadata?.freshness) ?? metadata;

  const buildControlCount = asNumber(counts?.surface_controls_returned)
    ?? (Array.isArray(uiPayload?.surface_controls) ? uiPayload.surface_controls.length : null);
  const atlasFreshness = asString(freshness?.graph_freshness_state)
    ?? asString(metadata?.graph_freshness_state)
    ?? (atlas ? 'unknown' : null);
  const sourceAuthority = asString(uiPayload?.source_authority)
    ?? asString(metadata?.source_authority)
    ?? null;

  if (!uiPayload && !atlas) {
    return {
      status: 'degraded',
      buildControlCount: null,
      atlasFreshness: null,
      sourceAuthority: null,
      warning: 'Authority surfaces unavailable',
    };
  }

  return {
    status: uiPayload && atlas ? 'ready' : 'degraded',
    buildControlCount,
    atlasFreshness,
    sourceAuthority,
    warning: uiPayload && atlas ? null : 'Partial authority snapshot',
  };
}

export function buildAuthorityCompileProse(input: ComposeAuthorityInput): string {
  const prose = input.prose.trim();
  const trigger = input.triggerLabel?.trim();
  const summary = input.summary;
  const authority = summary?.sourceAuthority || 'Praxis workflow build authority';
  const atlasFreshness = summary?.atlasFreshness || 'unknown';
  const controlCount = summary?.buildControlCount;

  const lines = [
    'Build this as a Praxis workflow graph, not as loose prose.',
    `Workflow authority: ${authority}.`,
    `Atlas freshness: ${atlasFreshness}.`,
    controlCount === null ? null : `Available builder controls: ${controlCount}.`,
    trigger ? `Selected trigger: ${trigger}.` : null,
    'Use explicit nodes, edges, inputs, outputs, route intent, release conditions, and verification points.',
    'Prefer small progressive units that can be inspected, released through the workflow engine, and represented in Atlas.',
    'Operator request:',
    prose,
  ];

  return lines.filter((line): line is string => Boolean(line)).join('\n');
}
