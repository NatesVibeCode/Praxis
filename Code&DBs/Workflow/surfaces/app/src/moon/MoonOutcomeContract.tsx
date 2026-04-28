import React, { useMemo, useState } from 'react';

import type { ContractFieldSuggestion } from './moonContractSuggestions';

const DEFAULT_OUTCOME_PILLS: ContractFieldSuggestion[] = [
  { value: 'receipt.ok', detail: 'Run receipt' },
  { value: 'verifier.status', detail: 'Verifier result' },
  { value: 'run.status', detail: 'Run state' },
  { value: 'error.kind', detail: 'Failure classifier' },
  { value: 'audit_record.status', detail: 'Audit trail' },
  { value: 'schema_error.count', detail: 'Validation errors' },
  { value: 'missing_receipt', detail: 'Receipt guard' },
];

interface OutcomeCriteriaInputProps {
  label: string;
  value: string;
  onChange: (next: string) => void;
  placeholder: string;
  suggestions: ContractFieldSuggestion[];
  tone: 'success' | 'failure';
  disabled?: boolean;
}

interface MoonOutcomeContractProps {
  open: boolean;
  compact?: boolean;
  disabled?: boolean;
  successCriteria: string;
  failureCriteria: string;
  suggestions?: ContractFieldSuggestion[];
  onOpenChange: (open: boolean) => void;
  onSuccessChange: (next: string) => void;
  onFailureChange: (next: string) => void;
}

function slashQuery(value: string): string | null {
  const match = value.match(/(^|[\s(])\/([A-Za-z0-9._-]*)$/);
  return match ? match[2].toLowerCase() : null;
}

function insertPill(value: string, pill: string): string {
  const match = value.match(/(^|[\s(])\/([A-Za-z0-9._-]*)$/);
  if (!match || typeof match.index !== 'number') {
    const spacer = value.trim() && !value.endsWith(' ') ? ' ' : '';
    return `${value}${spacer}{${pill}}`;
  }
  const prefix = value.slice(0, match.index);
  return `${prefix}${match[1]}{${pill}}`;
}

function mergeSuggestions(suggestions: ContractFieldSuggestion[]): ContractFieldSuggestion[] {
  const out: ContractFieldSuggestion[] = [];
  const seen = new Set<string>();
  for (const item of [...suggestions, ...DEFAULT_OUTCOME_PILLS]) {
    const value = item.value.trim();
    if (!value || seen.has(value)) continue;
    seen.add(value);
    out.push({ ...item, value });
  }
  return out;
}

function OutcomeCriteriaInput({
  label,
  value,
  onChange,
  placeholder,
  suggestions,
  tone,
  disabled,
}: OutcomeCriteriaInputProps) {
  const [focused, setFocused] = useState(false);
  const query = slashQuery(value);
  const filteredSuggestions = useMemo(() => {
    if (query === null) return [];
    const pool = suggestions.filter((item) => {
      const needle = query.toLowerCase();
      return !needle || item.value.toLowerCase().includes(needle);
    });
    return pool.slice(0, 8);
  }, [query, suggestions]);

  return (
    <label className={`moon-outcome-contract__field moon-outcome-contract__field--${tone}`}>
      <span>{label}</span>
      <textarea
        aria-label={tone === 'success' ? 'This run succeeds if' : 'This run fails if'}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        onFocus={() => setFocused(true)}
        onBlur={() => window.setTimeout(() => setFocused(false), 120)}
        placeholder={placeholder}
        rows={2}
        disabled={disabled}
      />
      {focused && filteredSuggestions.length > 0 ? (
        <div className="moon-outcome-contract__slash-menu" role="listbox" aria-label={`${label} data pills`}>
          {filteredSuggestions.map((suggestion) => (
            <button
              key={`${tone}-${suggestion.value}-${suggestion.detail ?? ''}`}
              type="button"
              role="option"
              className="moon-contract-suggestion"
              title={suggestion.detail}
              onMouseDown={(event) => {
                event.preventDefault();
                onChange(insertPill(value, suggestion.value));
              }}
            >
              <span className="moon-contract-suggestion__value">{suggestion.value}</span>
              {suggestion.detail ? (
                <span className="moon-contract-suggestion__detail">{suggestion.detail}</span>
              ) : null}
            </button>
          ))}
        </div>
      ) : null}
    </label>
  );
}

export function MoonOutcomeContract({
  open,
  compact = false,
  disabled = false,
  successCriteria,
  failureCriteria,
  suggestions = [],
  onOpenChange,
  onSuccessChange,
  onFailureChange,
}: MoonOutcomeContractProps) {
  const hasCriteria = Boolean(successCriteria.trim() || failureCriteria.trim());
  const mergedSuggestions = useMemo(() => mergeSuggestions(suggestions), [suggestions]);
  const activeCount = [successCriteria, failureCriteria].filter((item) => item.trim()).length;

  if (!open) {
    return (
      <button
        type="button"
        className={`moon-outcome-contract-toggle${compact ? ' moon-outcome-contract-toggle--compact' : ''}`}
        onClick={() => onOpenChange(true)}
      >
        <span>Run contract</span>
        <strong>Pattern / anti-pattern</strong>
        <em>{activeCount > 0 ? `${activeCount} active` : 'Optional'}</em>
      </button>
    );
  }

  return (
    <div className={`moon-outcome-contract${compact ? ' moon-outcome-contract--dock' : ''}`}>
      <div className="moon-outcome-contract__head">
        <div>
          <div className="moon-outcome-contract__kicker">Run contract</div>
          <div className="moon-outcome-contract__title">Pattern / anti-pattern</div>
        </div>
        <div className="moon-outcome-contract__actions">
          {hasCriteria ? (
            <button
              type="button"
              className="moon-outcome-contract__mini-btn"
              onClick={() => {
                onSuccessChange('');
                onFailureChange('');
              }}
              disabled={disabled}
            >
              Clear
            </button>
          ) : null}
          <button
            type="button"
            className="moon-outcome-contract__mini-btn"
            onClick={() => onOpenChange(false)}
            disabled={disabled && hasCriteria}
          >
            Hide
          </button>
        </div>
      </div>
      <div className="moon-outcome-contract__grid">
        <OutcomeCriteriaInput
          tone="success"
          label="Pattern: succeeds if"
          value={successCriteria}
          onChange={onSuccessChange}
          placeholder="Type / for data pills, then set the value"
          suggestions={mergedSuggestions}
          disabled={disabled}
        />
        <OutcomeCriteriaInput
          tone="failure"
          label="Anti-pattern: fails if"
          value={failureCriteria}
          onChange={onFailureChange}
          placeholder="Type / for data pills, then set the value"
          suggestions={mergedSuggestions}
          disabled={disabled}
        />
      </div>
    </div>
  );
}
