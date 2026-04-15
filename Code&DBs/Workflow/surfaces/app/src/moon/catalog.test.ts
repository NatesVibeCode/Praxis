import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { CatalogItem } from './catalog';

const catalogMocks = vi.hoisted(() => ({
  fetchCatalogEnvelope: vi.fn(),
}));

vi.mock('../shared/buildController', () => ({
  fetchCatalogEnvelope: catalogMocks.fetchCatalogEnvelope,
}));

describe('moon catalog authority', () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
  });

  it('starts empty, then caches the backend-authored catalog', async () => {
    const backendItems: CatalogItem[] = [
      {
        id: 'trigger-manual',
        label: 'Manual',
        icon: 'trigger',
        family: 'trigger',
        status: 'ready',
        dropKind: 'node',
        actionValue: 'trigger',
        source: 'surface_registry',
      },
      {
        id: 'ctrl-branch',
        label: 'Branch',
        icon: 'gate',
        family: 'control',
        status: 'ready',
        dropKind: 'edge',
        gateFamily: 'conditional',
        source: 'surface_registry',
      },
    ];
    catalogMocks.fetchCatalogEnvelope.mockResolvedValueOnce({ items: backendItems });

    const catalog = await import('./catalog');

    expect(catalog.getCatalog()).toEqual([]);
    expect(await catalog.loadCatalog()).toEqual(backendItems);
    expect(catalog.getCatalog()).toEqual(backendItems);
    expect(catalog.catalogByFamily('trigger')).toEqual([backendItems[0]]);

    await catalog.loadCatalog();
    expect(catalogMocks.fetchCatalogEnvelope).toHaveBeenCalledTimes(1);
  });

  it('does not fabricate a frontend fallback catalog when the backend is unavailable', async () => {
    catalogMocks.fetchCatalogEnvelope.mockRejectedValueOnce(new Error('catalog unavailable'));

    const catalog = await import('./catalog');

    expect(await catalog.loadCatalog()).toEqual([]);
    expect(catalog.getCatalog()).toEqual([]);
    expect(catalog.catalogByFamily()).toEqual([]);
  });

  it('normalizes source policies from the backend-authored envelope', async () => {
    catalogMocks.fetchCatalogEnvelope.mockResolvedValueOnce({
      items: [],
      source_policies: [
        {
          source_kind: 'capability',
          truth: {
            category: 'runtime',
            badge: 'Runs on release',
            detail: 'Capability rows are backed by runtime lanes.',
          },
          surfacePolicy: {
            tier: 'hidden',
            badge: 'Hidden',
            detail: 'Capability rows stay off the main builder.',
          },
        },
      ],
      sources: {
        surface_registry: 2,
      },
      fetched_at: '2026-04-15T18:00:00Z',
    });

    const catalog = await import('./catalog');
    const envelope = await catalog.loadCatalogEnvelope();

    expect(envelope.sourcePolicies).toEqual([
      {
        sourceKind: 'capability',
        truth: {
          category: 'runtime',
          badge: 'Runs on release',
          detail: 'Capability rows are backed by runtime lanes.',
        },
        surfacePolicy: {
          tier: 'hidden',
          badge: 'Hidden',
          detail: 'Capability rows stay off the main builder.',
          hardChoice: undefined,
        },
      },
    ]);
    expect(envelope.sources.surface_registry).toBe(2);
    expect(envelope.fetchedAt).toBe('2026-04-15T18:00:00Z');
  });
});
