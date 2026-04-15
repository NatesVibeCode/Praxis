// Moon catalog authority lives at /api/catalog and is backed by the DB surface registry.
// The browser keeps only a short-lived cache of the backend-authored payload.

import { fetchCatalog } from '../shared/buildController';
import type { GlyphType } from './moonBuildPresenter';
import type { DragDropKind } from './moonBuildReducer';

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

let catalogCache: CatalogItem[] = [];
let catalogLoad: Promise<CatalogItem[]> | null = null;

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

export function catalogByFamily(family?: CatalogFamily): CatalogItem[] {
  const catalog = getCatalog();
  if (!family) return catalog;
  return catalog.filter((item) => item.family === family);
}

export async function loadCatalog(): Promise<CatalogItem[]> {
  if (catalogCache.length > 0) return catalogCache;
  if (catalogLoad) return catalogLoad;

  catalogLoad = fetchCatalog()
    .then((items) => {
      catalogCache = normalizeCatalogPayload(items);
      return catalogCache;
    })
    .catch(() => catalogCache)
    .finally(() => {
      catalogLoad = null;
    });

  return catalogLoad;
}

export function getCatalog(): CatalogItem[] {
  return catalogCache;
}
