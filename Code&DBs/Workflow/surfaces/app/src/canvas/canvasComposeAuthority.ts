export type AuthorityStatus = 'loading' | 'ready' | 'degraded';

export interface CanvasComposeAuthoritySummary {
  status: AuthorityStatus;
  buildControlCount: number | null;
  atlasFreshness: string | null;
  sourceAuthority: string | null;
  warning: string | null;
}

export interface ComposeAuthorityInput {
  prose: string;
  triggerLabel?: string | null;
  summary?: CanvasComposeAuthoritySummary | null;
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
): CanvasComposeAuthoritySummary {
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
  // BUG-C6EE740C fix: previously this function prepended 8 lines of system
  // context ("Build this as a Praxis workflow graph...", "Workflow authority:
  // ...", "Atlas freshness: ...", etc.) onto the operator's prose before
  // sending to the compiler. The backend compile_prose has its own system
  // prompt with the full catalog and routing rules; the prepended meta-prose
  // got compiled INTO the workflow as prose-shaped nodes (system instructions
  // became workflow nodes). Now this function returns ONLY the operator's
  // prose, optionally annotated with the selected trigger label as the
  // operator typed it. All authority/freshness/build-control diagnostics
  // belong in the system prompt, not the user's input.
  const prose = input.prose.trim();
  const trigger = input.triggerLabel?.trim();
  if (trigger) {
    return `${prose}\n\nSelected trigger: ${trigger}`;
  }
  return prose;
}
