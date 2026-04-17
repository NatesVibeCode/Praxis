import { describe, expect, it } from 'vitest';
import { availableSpansForQuadrant, canQuadrantOccupySpan } from './quadrantUtils';

describe('quadrantUtils span authority', () => {
  it('allows resizing within the quadrant footprint when no other module blocks it', () => {
    const quadrants = {
      A1: { span: '2x2' },
      D4: { span: '1x1' },
    };

    expect(canQuadrantOccupySpan(quadrants, 'A1', '2x1')).toBe(true);
    expect(canQuadrantOccupySpan(quadrants, 'A1', '3x2')).toBe(true);
  });

  it('rejects spans that would overlap a different quadrant', () => {
    const quadrants = {
      A1: { span: '2x1' },
      A3: { span: '2x1' },
    };

    expect(canQuadrantOccupySpan(quadrants, 'A1', '3x1')).toBe(false);
  });

  it('returns only the spans that fit the current cell and open footprint', () => {
    const quadrants = {
      B2: { span: '1x1' },
      C2: { span: '1x1' },
    };

    expect(availableSpansForQuadrant(quadrants, 'B2', ['1x1', '2x1', '1x2', '2x2'])).toEqual(['1x1', '2x1']);
  });
});
