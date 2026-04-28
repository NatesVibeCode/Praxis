export interface OutcomeContractDraft {
  successCriteria?: string;
  failureCriteria?: string;
}

export function buildOutcomeContractProse(draft: OutcomeContractDraft): string {
  const success = draft.successCriteria?.trim();
  const failure = draft.failureCriteria?.trim();
  const lines: string[] = [];

  if (success) {
    lines.push(`This run succeeds if: ${success}`);
  }
  if (failure) {
    lines.push(`This run fails if: ${failure}`);
  }

  return lines.join('\n');
}

export function appendOutcomeContract(prose: string, draft: OutcomeContractDraft): string {
  const intent = prose.trim();
  const contract = buildOutcomeContractProse(draft);
  return [intent, contract].filter(Boolean).join('\n\n');
}
