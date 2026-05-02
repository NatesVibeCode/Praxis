// Canvas catalog authority lives at /api/catalog and is backed by the DB surface registry.
// The browser keeps only a short-lived cache of the backend-authored payload.

import { fetchCatalogEnvelope } from '../shared/buildController';
import type { GlyphType } from './canvasBuildPresenter';
import type { DragDropKind } from './canvasBuildReducer';

export interface CatalogTruthPayload {
  category: 'runtime' | 'persisted' | 'alias' | 'partial' | 'coming_soon';
  badge: string;
  detail: string;
}

export interface CatalogSurfacePolicyPayload {
  tier: 'primary' | 'advanced' | 'hidden';
  badge: string;
  detail: string;
  hardChoice?: string;
}

export interface CatalogItem {
  id: string;
  label: string;
  icon: GlyphType;
  family: 'trigger' | 'gather' | 'think' | 'act' | 'control';
  status: 'ready' | 'coming_soon';
  dropKind: DragDropKind;
  description?: string;
  actionValue?: string;
  gateFamily?: string;
  source?: 'surface_registry' | 'capability' | 'integration' | 'connector';
  connectionStatus?: string;
  truth?: CatalogTruthPayload;
  surfacePolicy?: CatalogSurfacePolicyPayload;
}

export interface CatalogSourcePolicy {
  sourceKind: 'capability' | 'integration' | 'connector';
  truth?: CatalogTruthPayload;
  surfacePolicy?: CatalogSurfacePolicyPayload;
}

export interface CatalogEnvelope {
  items: CatalogItem[];
  sourcePolicies: CatalogSourcePolicy[];
  sources: Record<string, number>;
  fetchedAt: string | null;
}

const EMPTY_CATALOG_ENVELOPE: CatalogEnvelope = {
  items: [],
  sourcePolicies: [],
  sources: {},
  fetchedAt: null,
};

let catalogCache: CatalogEnvelope = EMPTY_CATALOG_ENVELOPE;
let catalogLoad: Promise<CatalogEnvelope> | null = null;

export type CatalogFamily = CatalogItem['family'];

export const FAMILY_LABELS: Record<CatalogFamily, string> = {
  trigger: 'Trigger',
  gather: 'Gather',
  think: 'Think',
  act: 'Act',
  control: 'Control',
};

function normalizeCatalogPayload(payload: unknown): CatalogItem[] {
  if (!Array.isArray(payload)) return [];
  return payload.filter((item): item is CatalogItem => {
    if (!item || typeof item !== 'object') return false;
    const candidate = item as Partial<CatalogItem>;
    return (
      typeof candidate.id === 'string'
      && typeof candidate.label === 'string'
      && typeof candidate.icon === 'string'
      && typeof candidate.family === 'string'
      && typeof candidate.status === 'string'
      && typeof candidate.dropKind === 'string'
    );
  });
}

function normalizeCatalogTruth(value: unknown): CatalogTruthPayload | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const candidate = value as Partial<CatalogTruthPayload>;
  if (
    typeof candidate.category === 'string'
    && typeof candidate.badge === 'string'
    && typeof candidate.detail === 'string'
  ) {
    return {
      category: candidate.category as CatalogTruthPayload['category'],
      badge: candidate.badge,
      detail: candidate.detail,
    };
  }
  return undefined;
}

function normalizeCatalogSurfacePolicy(value: unknown): CatalogSurfacePolicyPayload | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const candidate = value as Partial<CatalogSurfacePolicyPayload>;
  if (
    typeof candidate.tier === 'string'
    && typeof candidate.badge === 'string'
    && typeof candidate.detail === 'string'
  ) {
    return {
      tier: candidate.tier as CatalogSurfacePolicyPayload['tier'],
      badge: candidate.badge,
      detail: candidate.detail,
      hardChoice: typeof candidate.hardChoice === 'string' ? candidate.hardChoice : undefined,
    };
  }
  return undefined;
}

function normalizeSourcePolicies(payload: unknown): CatalogSourcePolicy[] {
  if (!Array.isArray(payload)) return [];
  return payload.flatMap((value) => {
    if (!value || typeof value !== 'object') return [];
    const candidate = value as {
      source_kind?: unknown;
      truth?: unknown;
      surfacePolicy?: unknown;
    };
    if (
      candidate.source_kind !== 'capability'
      && candidate.source_kind !== 'integration'
      && candidate.source_kind !== 'connector'
    ) {
      return [];
    }
    return [{
      sourceKind: candidate.source_kind,
      truth: normalizeCatalogTruth(candidate.truth),
      surfacePolicy: normalizeCatalogSurfacePolicy(candidate.surfacePolicy),
    }];
  });
}

function normalizeSourceCounts(payload: unknown): Record<string, number> {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return {};
  return Object.fromEntries(
    Object.entries(payload as Record<string, unknown>).flatMap(([key, value]) => (
      typeof value === 'number' && Number.isFinite(value)
        ? [[key, value]]
        : []
    )),
  );
}

function normalizeCatalogEnvelope(payload: unknown): CatalogEnvelope {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return EMPTY_CATALOG_ENVELOPE;
  const candidate = payload as {
    items?: unknown;
    source_policies?: unknown;
    sources?: unknown;
    fetched_at?: unknown;
  };
  return {
    items: normalizeCatalogPayload(candidate.items),
    sourcePolicies: normalizeSourcePolicies(candidate.source_policies),
    sources: normalizeSourceCounts(candidate.sources),
    fetchedAt: typeof candidate.fetched_at === 'string' ? candidate.fetched_at : null,
  };
}

export function catalogByFamily(family?: CatalogFamily): CatalogItem[] {
  const catalog = getCatalog();
  if (!family) return catalog;
  return catalog.filter((item) => item.family === family);
}

export async function loadCatalogEnvelope(force = false): Promise<CatalogEnvelope> {
  if (
    !force
    && (
      catalogCache.items.length > 0
      || catalogCache.sourcePolicies.length > 0
      || catalogCache.fetchedAt !== null
    )
  ) {
    return catalogCache;
  }
  if (catalogLoad) return catalogLoad;

  catalogLoad = fetchCatalogEnvelope()
    .then((envelope) => {
      catalogCache = normalizeCatalogEnvelope(envelope);
      return catalogCache;
    })
    .catch(() => catalogCache)
    .finally(() => {
      catalogLoad = null;
    });

  return catalogLoad;
}

export async function loadCatalog(): Promise<CatalogItem[]> {
  return (await loadCatalogEnvelope()).items;
}

export async function refreshCatalogEnvelope(): Promise<CatalogEnvelope> {
  return loadCatalogEnvelope(true);
}

export async function refreshCatalog(): Promise<CatalogItem[]> {
  return (await refreshCatalogEnvelope()).items;
}

export function getCatalog(): CatalogItem[] {
  return catalogCache.items;
}

export function getCatalogEnvelope(): CatalogEnvelope {
  return catalogCache;
}
