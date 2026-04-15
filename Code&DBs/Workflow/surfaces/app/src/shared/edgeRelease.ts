import type { BuildEdge, BuildEdgeRelease } from './types';

const ALWAYS_RELEASE_CONDITION: Record<string, unknown> = { kind: 'always' };
const EDGE_TYPES = new Set<BuildEdgeRelease['edge_type']>([
  'after_success',
  'after_failure',
  'after_any',
  'conditional',
]);

type LegacyBuildEdgeShape = BuildEdge & {
  branch_reason?: string | null;
  release_condition?: Record<string, unknown>;
  gate?: {
    state?: string;
    label?: string;
    family?: string;
    config?: BuildEdgeRelease['config'];
  } | null;
};

function cloneRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
}

function cloneConfig(value: unknown): BuildEdgeRelease['config'] | undefined {
  const record = cloneRecord(value);
  return record ? record as BuildEdgeRelease['config'] : undefined;
}

function text(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function invertCondition(condition: Record<string, unknown>): Record<string, unknown> {
  return { op: 'not', conditions: [cloneRecord(condition) || { ...condition }] };
}

function unwrapElseCondition(condition: Record<string, unknown>): Record<string, unknown> {
  const op = text(condition.op).toLowerCase();
  const conditions = Array.isArray(condition.conditions) ? condition.conditions : [];
  if (op === 'not' && conditions.length === 1) {
    const candidate = cloneRecord(conditions[0]);
    if (candidate) return candidate;
  }
  return cloneRecord(condition) || { ...condition };
}

function familyFromEdgeType(edgeType: BuildEdgeRelease['edge_type']): string {
  if (edgeType === 'conditional') return 'conditional';
  if (edgeType === 'after_failure') return 'after_failure';
  if (edgeType === 'after_any') return 'after_any';
  return 'after_success';
}

function normalizeEdgeType(
  value: unknown,
  family: string,
): BuildEdgeRelease['edge_type'] {
  const candidate = text(value) as BuildEdgeRelease['edge_type'];
  if (EDGE_TYPES.has(candidate)) return candidate;
  if (family === 'conditional') return 'conditional';
  if (family === 'after_failure') return 'after_failure';
  if (family === 'after_any') return 'after_any';
  return 'after_success';
}

function cloneReleaseCondition(value: unknown): Record<string, unknown> {
  return cloneRecord(value) || { ...ALWAYS_RELEASE_CONDITION };
}

export function branchLabel(reason: string | null | undefined): string | undefined {
  const normalized = (reason || '').trim();
  if (!normalized) return undefined;
  if (normalized === 'then') return 'Then';
  if (normalized === 'else') return 'Else';
  return normalized
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

export function normalizeBuildEdgeRelease(edge: LegacyBuildEdgeShape): BuildEdgeRelease {
  const existingRelease = edge.release && typeof edge.release === 'object' ? edge.release : null;
  const family = text(existingRelease?.family)
    || text(edge.gate?.family)
    || familyFromEdgeType(normalizeEdgeType(existingRelease?.edge_type, text(edge.gate?.family)));
  const edgeType = normalizeEdgeType(existingRelease?.edge_type, family || (edge.kind === 'conditional' ? 'conditional' : ''));
  const branchReason = text(existingRelease?.branch_reason) || text(edge.branch_reason);
  const explicitRuntimeCondition = cloneRecord(existingRelease?.release_condition) || cloneRecord(edge.release_condition);

  let releaseCondition = explicitRuntimeCondition;
  if (!releaseCondition && (family === 'conditional' || edgeType === 'conditional')) {
    const baseCondition = cloneRecord(edge.gate?.config?.condition) || cloneRecord(existingRelease?.config?.condition);
    if (baseCondition) {
      releaseCondition = branchReason.toLowerCase() === 'else'
        ? invertCondition(baseCondition)
        : baseCondition;
    }
  }

  const label = text(existingRelease?.label) || text(edge.gate?.label) || branchLabel(branchReason) || undefined;
  const config = cloneConfig(existingRelease?.config) || cloneConfig(edge.gate?.config);

  return {
    family: family || familyFromEdgeType(edgeType),
    edge_type: edgeType,
    release_condition: cloneReleaseCondition(releaseCondition),
    ...(label ? { label } : {}),
    ...(branchReason ? { branch_reason: branchReason } : {}),
    ...(text(existingRelease?.state) || text(edge.gate?.state) ? { state: text(existingRelease?.state) || text(edge.gate?.state) } : {}),
    ...(config ? { config } : {}),
  };
}

export function baseConditionFromRelease(release: BuildEdgeRelease): Record<string, unknown> | undefined {
  if (release.edge_type !== 'conditional') {
    return cloneRecord(release.config?.condition) || undefined;
  }
  const runtimeCondition = cloneRecord(release.release_condition) || cloneRecord(release.config?.condition);
  if (!runtimeCondition) return undefined;
  return text(release.branch_reason).toLowerCase() === 'else'
    ? unwrapElseCondition(runtimeCondition)
    : runtimeCondition;
}

export function edgeKindFromRelease(release: BuildEdgeRelease, existingKind?: string): string {
  if (existingKind === 'authority_gate' || existingKind === 'state_informs') return existingKind;
  if (release.edge_type === 'conditional') return 'conditional';
  return existingKind && existingKind !== 'conditional' ? existingKind : 'sequence';
}

export function withBuildEdgeRelease(
  edge: BuildEdge,
  nextRelease?: Partial<BuildEdgeRelease> | null,
): BuildEdge {
  const mergedRelease: Partial<BuildEdgeRelease> = nextRelease == null
    ? {
        family: 'after_success',
        edge_type: 'after_success',
        release_condition: { ...ALWAYS_RELEASE_CONDITION },
      }
    : {
        ...(edge.release || {}),
        ...nextRelease,
      };
  const release = normalizeBuildEdgeRelease({
    ...edge,
    release: mergedRelease as BuildEdgeRelease,
  });
  return {
    ...edge,
    kind: edgeKindFromRelease(release, edge.kind),
    release,
  };
}
