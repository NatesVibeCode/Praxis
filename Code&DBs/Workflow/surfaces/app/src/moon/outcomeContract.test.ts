import { describe, expect, test } from 'vitest';
import { appendOutcomeContract, buildOutcomeContractProse } from './outcomeContract';

describe('outcome contract prose', () => {
  test('renders success and failure criteria as explicit run outcomes', () => {
    expect(buildOutcomeContractProse({
      successCriteria: 'receipt.ok = true',
      failureCriteria: 'missing_receipt = true',
    })).toBe([
      'This run succeeds if: receipt.ok = true',
      'This run fails if: missing_receipt = true',
    ].join('\n'));
  });

  test('appends the contract without inventing criteria', () => {
    expect(appendOutcomeContract('Import files', {
      successCriteria: 'schema.valid = true',
    })).toBe('Import files\n\nThis run succeeds if: schema.valid = true');
  });
});
