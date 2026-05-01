import { describe, expect, it } from 'vitest';
import { getPreset, listPresetsByCategory, CATEGORY_LABELS } from '../presets';

describe('data plane presets', () => {
  const expectedActions: Array<[string, string]> = [
    ['data-profile', 'profile'],
    ['data-validate', 'validate'],
    ['data-dedupe', 'dedupe'],
    ['data-reconcile', 'reconcile'],
    ['data-repair-loop', 'repair_loop'],
    ['data-redact', 'redact'],
  ];

  it.each(expectedActions)(
    'preset %s routes to praxis_data action %s',
    (presetId, action) => {
      const preset = getPreset(presetId);
      expect(preset).toBeDefined();
      expect(preset?.moduleId).toBe('data-op');
      expect(preset?.category).toBe('data');
      expect(preset?.config?.action).toBe(action);
    },
  );

  it('all six data tiles surface under the Data Plane category', () => {
    const grouped = listPresetsByCategory();
    const dataGroup = grouped.find((g) => g.category === 'data');
    expect(dataGroup).toBeDefined();
    expect(dataGroup?.label).toBe(CATEGORY_LABELS.data);
    const presetIds = dataGroup?.presets.map((p) => p.presetId) ?? [];
    expect(presetIds.sort()).toEqual([
      'data-dedupe',
      'data-profile',
      'data-reconcile',
      'data-redact',
      'data-repair-loop',
      'data-validate',
    ]);
  });

  it('every data tile carries label + description so the canvas surfaces both', () => {
    for (const [presetId] of expectedActions) {
      const preset = getPreset(presetId);
      expect(typeof preset?.config?.label).toBe('string');
      expect(typeof preset?.config?.description).toBe('string');
    }
  });
});
