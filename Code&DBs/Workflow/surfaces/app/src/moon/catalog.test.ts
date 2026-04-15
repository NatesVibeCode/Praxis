import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { CatalogItem } from './catalog';

const catalogMocks = vi.hoisted(() => ({
  fetchCatalog: vi.fn(),
}));

vi.mock('../shared/buildController', () => ({
  fetchCatalog: catalogMocks.fetchCatalog,
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
    catalogMocks.fetchCatalog.mockResolvedValueOnce(backendItems);

    const catalog = await import('./catalog');

    expect(catalog.getCatalog()).toEqual([]);
    expect(await catalog.loadCatalog()).toEqual(backendItems);
    expect(catalog.getCatalog()).toEqual(backendItems);
    expect(catalog.catalogByFamily('trigger')).toEqual([backendItems[0]]);

    await catalog.loadCatalog();
    expect(catalogMocks.fetchCatalog).toHaveBeenCalledTimes(1);
  });

  it('does not fabricate a frontend fallback catalog when the backend is unavailable', async () => {
    catalogMocks.fetchCatalog.mockRejectedValueOnce(new Error('catalog unavailable'));

    const catalog = await import('./catalog');

    expect(await catalog.loadCatalog()).toEqual([]);
    expect(catalog.getCatalog()).toEqual([]);
    expect(catalog.catalogByFamily()).toEqual([]);
  });
});
