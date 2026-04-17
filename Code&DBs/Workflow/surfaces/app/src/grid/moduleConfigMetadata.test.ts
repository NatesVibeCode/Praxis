import { describe, expect, it } from 'vitest';
import { gridSpanLabel, normalizeGridEndpoint } from './moduleConfigMetadata';

describe('moduleConfigMetadata', () => {
  it('normalizes API-prefixed endpoint paths', () => {
    expect(normalizeGridEndpoint('/api/observability/platform')).toBe('observability/platform');
    expect(normalizeGridEndpoint('api/objects?type=task')).toBe('objects?type=task');
    expect(normalizeGridEndpoint('https://example.com/api/platform-overview')).toBe('platform-overview');
  });

  it('formats grid span labels for UI controls', () => {
    expect(gridSpanLabel('2x3')).toBe('2 × 3');
  });
});
