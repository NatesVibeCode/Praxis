import { describe, expect, it } from 'vitest';
import { blockCatalog, listBlockCatalog } from './catalog';

describe('block catalog icons', () => {
  it('every catalog entry carries a non-empty icon glyph', () => {
    const missing = blockCatalog.filter(
      (entry) => !entry.icon || entry.icon.trim() === '',
    );
    expect(missing.map((m) => m.id)).toEqual([]);
  });

  it('icons are unique across the catalog', () => {
    const seen = new Map<string, string>();
    const collisions: Array<{ icon: string; ids: string[] }> = [];
    for (const entry of listBlockCatalog()) {
      const icon = (entry.icon ?? '').trim();
      if (!icon) continue;
      const previous = seen.get(icon);
      if (previous) {
        collisions.push({ icon, ids: [previous, entry.id] });
      } else {
        seen.set(icon, entry.id);
      }
    }
    expect(collisions).toEqual([]);
  });

  it('icons are single visible glyphs (no whitespace, no multi-codepoint surrogates beyond the basic plane)', () => {
    for (const entry of listBlockCatalog()) {
      const icon = entry.icon ?? '';
      expect(icon).not.toMatch(/\s/);
      // Allow up to 2 UTF-16 units (covers basic plane + simple surrogate pairs).
      expect(icon.length).toBeLessThanOrEqual(2);
      expect(icon.length).toBeGreaterThan(0);
    }
  });
});
