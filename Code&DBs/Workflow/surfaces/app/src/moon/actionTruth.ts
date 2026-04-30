import type { CatalogItem } from './catalog';
import { getMoonPrimitive } from './moonPrimitives';

export type CatalogTruthCategory = 'runtime' | 'persisted' | 'alias' | 'partial' | 'coming_soon';
export type CatalogSurfaceTier = 'primary' | 'advanced' | 'hidden';

export interface CatalogTruth {
  category: CatalogTruthCategory;
  badge: string;
  detail: string;
}

export interface CatalogSurfacePolicy {
  tier: CatalogSurfaceTier;
  badge: string;
  detail: string;
  hardChoice?: string;
}

export interface CatalogTruthSummary {
  edgeCounts: Record<CatalogTruthCategory, number>;
  edgeTotal: number;
  nodeCounts: Record<CatalogTruthCategory, number>;
  nodeTotal: number;
}

export interface CatalogSurfaceSummary {
  edgeCounts: Record<CatalogSurfaceTier, number>;
  edgeTotal: number;
  nodeCounts: Record<CatalogSurfaceTier, number>;
  nodeTotal: number;
}

const EXECUTABLE_GATE_FAMILIES = new Set(['approval', 'conditional', 'after_failure']);
const PERSISTED_GATE_FAMILIES = new Set(['human_review']);
const RUNTIME_NODE_ROUTES = new Set([
  'trigger',
  'trigger/schedule',
  'trigger/webhook',
  'auto/research',
  'auto/draft',
  'auto/classify',
  'workflow.fanout',
  'workflow.loop',
  '@notifications/send',
  '@webhook/post',
  '@workflow/invoke',
]);
const VALID_TRUTH_CATEGORIES = new Set<CatalogTruthCategory>(['runtime', 'persisted', 'alias', 'partial', 'coming_soon']);
const VALID_SURFACE_TIERS = new Set<CatalogSurfaceTier>(['primary', 'advanced', 'hidden']);

function emptySurfaceCounts(): Record<CatalogSurfaceTier, number> {
  return {
    primary: 0,
    advanced: 0,
    hidden: 0,
  };
}

function emptyCounts(): Record<CatalogTruthCategory, number> {
  return {
    runtime: 0,
    persisted: 0,
    alias: 0,
    partial: 0,
    coming_soon: 0,
  };
}

export function getCatalogTruth(item: CatalogItem): CatalogTruth {
  if (
    item.truth
    && VALID_TRUTH_CATEGORIES.has(item.truth.category)
    && typeof item.truth.badge === 'string'
    && typeof item.truth.detail === 'string'
  ) {
    return item.truth;
  }

  if (item.status === 'coming_soon') {
    return {
      category: 'coming_soon',
      badge: 'Soon',
      detail: 'Listed in the catalog, but not enabled in the current surface.',
    };
  }

  if (item.dropKind === 'edge') {
    if (EXECUTABLE_GATE_FAMILIES.has(item.gateFamily || '')) {
      return {
        category: 'runtime',
        badge: 'Executes',
        detail: 'Compiled into dependency edges that change runtime flow today.',
      };
    }
    if (item.gateFamily === 'validation') {
      return {
        category: 'runtime',
        badge: 'Executes',
        detail: 'Runs the configured verification command before the downstream step can continue.',
      };
    }
    if (item.gateFamily === 'retry') {
      return {
        category: 'runtime',
        badge: 'Executes',
        detail: 'Sets downstream job max_attempts so the runtime retry loop can requeue failed work.',
      };
    }
    if (PERSISTED_GATE_FAMILIES.has(item.gateFamily || '')) {
      return {
        category: 'persisted',
        badge: 'Saved only',
        detail: 'Stored in edge metadata now, but not enforced by the planner yet.',
      };
    }
    return {
      category: 'partial',
      badge: 'Unverified',
      detail: 'Stored in the graph, but the runtime meaning is not verified yet.',
    };
  }

  if (item.id === 'gather-docs') {
    return {
      category: 'alias',
      badge: 'Alias',
      detail: 'Uses the same `auto/research` route as Web Research today.',
    };
  }

  if (item.actionValue === 'auto/classify') {
    return {
      category: 'runtime',
      badge: 'Runs on release',
      detail: 'Uses the analysis lane backed by task_type_route_profiles and task_type_routing authority.',
    };
  }

  if (item.actionValue === 'workflow.fanout') {
    return {
      category: 'runtime',
      badge: 'Runs on release',
      detail: 'Fan-out compiles into a count-based burst of parallel SLM API workers. CLI adapters are rejected — they break under concurrency.',
    };
  }

  if (item.actionValue === 'workflow.loop') {
    return {
      category: 'runtime',
      badge: 'Runs on release',
      detail: 'Loop compiles into one spec per item via replicate_with and dispatches them through the shared parallel runtime; any provider is allowed.',
    };
  }

  if (
    item.source === 'capability' ||
    item.source === 'integration' ||
    item.source === 'connector' ||
    RUNTIME_NODE_ROUTES.has(item.actionValue || '')
  ) {
    return {
      category: 'runtime',
      badge: 'Runs on release',
      detail: item.actionValue?.startsWith('trigger')
        ? 'Creates trigger intent that is preserved into compiled triggers.'
        : 'Persists into the build graph and becomes a planned runtime route at release.',
    };
  }

  return {
    category: 'partial',
    badge: 'Unverified',
    detail: 'The UI can assign this action, but the runtime lane is not verified in source yet.',
  };
}

export function getCatalogSurfacePolicy(item: CatalogItem): CatalogSurfacePolicy {
  if (
    item.surfacePolicy
    && VALID_SURFACE_TIERS.has(item.surfacePolicy.tier)
    && typeof item.surfacePolicy.badge === 'string'
    && typeof item.surfacePolicy.detail === 'string'
  ) {
    return item.surfacePolicy;
  }

  const truth = getCatalogTruth(item);

  if (item.id === 'gather-docs') {
    return {
      tier: 'hidden',
      badge: 'Merged',
      detail: 'Merged into Web Research because both buttons point at the same route today.',
      hardChoice: 'Merged into Web Research. One route gets one obvious button.',
    };
  }

  if (item.actionValue === 'auto/classify') {
    return {
      tier: 'primary',
      badge: 'Core',
      detail: 'Backed by a real analysis lane instead of borrowing the support route.',
    };
  }

  const primitive = getMoonPrimitive(item.actionValue);
  if (primitive?.surface) {
    return primitive.surface;
  }

  if (item.status === 'coming_soon') {
    return {
      tier: 'hidden',
      badge: 'Soon',
      detail: 'Keep this off the main builder until the route and config surface are real.',
    };
  }

  if (item.dropKind === 'edge') {
    if (item.gateFamily === 'conditional' || item.gateFamily === 'after_failure') {
      return {
        tier: 'primary',
        badge: 'Core',
        detail: 'This is one of the few gate types that changes execution today.',
      };
    }
    if (item.gateFamily === 'approval') {
      return {
        tier: 'primary',
        badge: 'Core',
        detail: 'Pauses the downstream step behind a human approval checkpoint before execution continues.',
      };
    }
    if (item.gateFamily === 'validation') {
      return {
        tier: 'primary',
        badge: 'Core',
        detail: 'Executes the configured verification command before the downstream step proceeds.',
      };
    }
    if (item.gateFamily === 'human_review') {
      return {
        tier: 'hidden',
        badge: 'Removed',
        detail: 'Folded into Approval so the builder keeps one obvious human gate concept.',
        hardChoice: 'Collapsed into Approval. Two human gate names for one future concept would be noise.',
      };
    }
    if (item.gateFamily === 'retry') {
      return {
        tier: 'advanced',
        badge: 'Later',
        detail: 'Feeds retry policy into downstream job max_attempts, but stays outside the core gate set.',
      };
    }
    return truth.category === 'runtime'
      ? {
          tier: 'advanced',
          badge: 'Later',
          detail: 'Real edge behavior, but not part of the curated gate set yet.',
        }
      : {
          tier: 'hidden',
          badge: 'Removed',
          detail: 'Saved-only edge metadata stays out of the main gate surface until it changes execution.',
        };
  }

  if (item.family === 'trigger') {
    let detail = 'Primary trigger primitive with real compile and release authority.';
    if (item.actionValue === 'trigger') detail = 'Starts when an operator clicks Run.';
    else if (item.actionValue === 'trigger/webhook') detail = 'Starts when a POST arrives on a registered endpoint.';
    else if (item.actionValue === 'trigger/schedule') detail = 'Starts on a cron or interval.';

    return {
      tier: 'primary',
      badge: 'Core',
      detail,
    };
  }

  if (item.source === 'capability' || item.source === 'integration' || item.source === 'connector') {
    return truth.category === 'runtime'
      ? {
          tier: 'hidden',
          badge: 'Hidden',
          detail: 'Live catalog lanes stay out of the main builder until they map cleanly onto the core primitive set.',
        }
      : {
          tier: 'hidden',
          badge: 'Removed',
          detail: 'Keep non-core live catalog items off the main builder unless their runtime contract is explicit.',
        };
  }

  if (truth.category === 'runtime') {
    return {
      tier: 'advanced',
      badge: 'Later',
      detail: 'Real route, but not part of the curated core surface yet.',
    };
  }

  return {
    tier: 'hidden',
    badge: 'Removed',
    detail: truth.category === 'alias'
      ? 'Alias routes stay out of the main builder.'
      : 'Non-core buttons stay hidden until they have one obvious runtime meaning.',
  };
}

export function isMoonSurfaceAuthorityItem(item: CatalogItem): boolean {
  return item.source === 'surface_registry';
}

export function summarizeCatalogTruth(items: CatalogItem[]): CatalogTruthSummary {
  const summary: CatalogTruthSummary = {
    edgeCounts: emptyCounts(),
    edgeTotal: 0,
    nodeCounts: emptyCounts(),
    nodeTotal: 0,
  };

  for (const item of items) {
    const truth = getCatalogTruth(item);
    if (item.dropKind === 'edge') {
      summary.edgeTotal += 1;
      summary.edgeCounts[truth.category] += 1;
      continue;
    }
    summary.nodeTotal += 1;
    summary.nodeCounts[truth.category] += 1;
  }

  return summary;
}

export function summarizeCatalogSurface(items: CatalogItem[]): CatalogSurfaceSummary {
  const summary: CatalogSurfaceSummary = {
    edgeCounts: emptySurfaceCounts(),
    edgeTotal: 0,
    nodeCounts: emptySurfaceCounts(),
    nodeTotal: 0,
  };

  for (const item of items) {
    const policy = getCatalogSurfacePolicy(item);
    if (item.dropKind === 'edge') {
      summary.edgeTotal += 1;
      summary.edgeCounts[policy.tier] += 1;
      continue;
    }
    summary.nodeTotal += 1;
    summary.nodeCounts[policy.tier] += 1;
  }

  return summary;
}

function joinParts(parts: string[]): string {
  return parts.filter(Boolean).join(', ');
}

export function formatTruthSummaryLine(
  summary: CatalogTruthSummary,
  kind: 'node' | 'edge',
): string {
  const counts = kind === 'node' ? summary.nodeCounts : summary.edgeCounts;
  const total = kind === 'node' ? summary.nodeTotal : summary.edgeTotal;
  const parts: string[] = [];

  if (kind === 'node') {
    if (counts.runtime) parts.push(`${counts.runtime} distinct runnable lane${counts.runtime === 1 ? '' : 's'}`);
    if (counts.alias) parts.push(`${counts.alias} alias${counts.alias === 1 ? '' : 'es'}`);
    if (counts.persisted) parts.push(`${counts.persisted} save metadata only`);
    if (counts.partial) parts.push(`${counts.partial} missing verified lane${counts.partial === 1 ? '' : 's'}`);
    if (counts.coming_soon) parts.push(`${counts.coming_soon} marked soon`);
    return `${total} step button${total === 1 ? '' : 's'} traced: ${joinParts(parts)}.`;
  }

  if (counts.runtime) parts.push(`${counts.runtime} affect execution today`);
  if (counts.persisted) parts.push(`${counts.persisted} save metadata only`);
  if (counts.alias) parts.push(`${counts.alias} alias${counts.alias === 1 ? '' : 'es'}`);
  if (counts.partial) parts.push(`${counts.partial} missing verified execution`);
  if (counts.coming_soon) parts.push(`${counts.coming_soon} marked soon`);
  return `${total} gate button${total === 1 ? '' : 's'} traced: ${joinParts(parts)}.`;
}

export function formatSurfaceSummaryLine(
  summary: CatalogSurfaceSummary,
  kind: 'node' | 'edge',
): string {
  const counts = kind === 'node' ? summary.nodeCounts : summary.edgeCounts;
  const parts: string[] = [];

  if (counts.primary) parts.push(`${counts.primary} core`);
  if (counts.advanced) parts.push(`${counts.advanced} advanced/later`);
  if (counts.hidden) parts.push(`${counts.hidden} removed from main UI`);

  return kind === 'node'
    ? `Curated step surface: ${joinParts(parts)}.`
    : `Curated gate surface: ${joinParts(parts)}.`;
}
