import { beforeEach, describe, expect, it, vi } from 'vitest';

describe('world persistence', () => {
  type StorageMock = {
    getItem: (key: string) => string | null;
    setItem: (key: string, value: string) => void;
    removeItem: (key: string) => void;
    clear: () => void;
  };

  let storage: StorageMock;

  beforeEach(() => {
    const backing = new Map<string, string>();
    storage = {
      getItem: (key: string) => (backing.has(key) ? backing.get(key) ?? null : null),
      setItem: (key: string, value: string) => {
        backing.set(key, String(value));
      },
      removeItem: (key: string) => {
        backing.delete(key);
      },
      clear: () => backing.clear(),
    };
    Object.defineProperty(globalThis, 'localStorage', {
      configurable: true,
      value: storage,
    });
    vi.resetModules();
  });

  it('persists committed state and restores it on reload', async () => {
    const { world } = await import('./world');

    world.hydrate({ state: {}, version: 0 });
    world.set('ui.layout.quadrants', { A1: { module: 'chart' } });
    world.set('ui.control.actions', [
      {
        id: 'action-1',
        label: 'Move chart',
        authority: 'ui.layout.quadrants',
      },
    ]);

    const stored = JSON.parse(storage.getItem('praxis.world.snapshot.v1') || '{}') as {
      state?: Record<string, unknown>;
      version?: number;
    };
    expect(stored.version).toBe(2);
    expect(stored.state?.ui).toMatchObject({
      layout: {
        quadrants: {
          A1: { module: 'chart' },
        },
      },
      control: {
        actions: [
          {
            id: 'action-1',
            label: 'Move chart',
            authority: 'ui.layout.quadrants',
          },
        ],
      },
    });

    vi.resetModules();
    const { world: reloadedWorld } = await import('./world');

    expect(reloadedWorld.get('ui.layout.quadrants')).toEqual({ A1: { module: 'chart' } });
    expect(reloadedWorld.get('ui.control.actions')).toEqual([
      {
        id: 'action-1',
        label: 'Move chart',
        authority: 'ui.layout.quadrants',
      },
    ]);
  });

  it('keeps draft proposals out of persistence until committed', async () => {
    const { world } = await import('./world');

    world.hydrate({ state: {}, version: 0 });
    world.propose('ui.layout.quadrants', { A1: { module: 'table' } });

    const stored = JSON.parse(storage.getItem('praxis.world.snapshot.v1') || '{}') as {
      state?: Record<string, unknown>;
      version?: number;
    };
    expect(stored.state).toEqual({});
    expect(world.get('ui.layout.quadrants')).toEqual({ A1: { module: 'table' } });
  });
});
