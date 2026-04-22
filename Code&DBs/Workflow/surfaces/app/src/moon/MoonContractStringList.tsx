import React, { useCallback, useEffect, useMemo, useState } from 'react';

import type { ContractFieldSuggestion } from './moonContractSuggestions';

export interface ContractStringRow {
  id: string;
  value: string;
}

let _rowSerial = 0;

function nextRowId(prefix: string): string {
  _rowSerial += 1;
  return `${prefix}-${Date.now()}-${_rowSerial}`;
}

/** Build editable rows from persisted string arrays; always at least one row so users can type. */
export function contractRowsFromStringArray(value: unknown, idPrefix: string): ContractStringRow[] {
  if (!Array.isArray(value)) {
    return [{ id: nextRowId(idPrefix), value: '' }];
  }
  const strings = value
    .map((item) => (typeof item === 'string' ? item : ''))
    .map((s) => s.trim())
    .filter(Boolean);
  if (strings.length === 0) {
    return [{ id: nextRowId(idPrefix), value: '' }];
  }
  return strings.map((s) => ({ id: nextRowId(idPrefix), value: s }));
}

export function stringArrayFromContractRows(rows: ContractStringRow[]): string[] {
  return rows.map((r) => r.value.trim()).filter(Boolean);
}

function chipLabel(row: ContractStringRow): string {
  const t = row.value.trim();
  if (!t) return '(unnamed)';
  if (t.length > 36) return `${t.slice(0, 33)}…`;
  return t;
}

export interface MoonContractStringListFieldProps {
  fieldId: string;
  ariaLabel: string;
  hint?: string;
  inputPlaceholder?: string;
  /** Tokens from other graph nodes + object-type fields; typing narrows the list. */
  suggestions?: ContractFieldSuggestion[];
  rows: ContractStringRow[];
  onChange: (next: ContractStringRow[]) => void;
}

export function MoonContractStringListField({
  fieldId,
  ariaLabel,
  hint,
  inputPlaceholder = 'identifier',
  suggestions = [],
  rows,
  onChange,
}: MoonContractStringListFieldProps) {
  const [activeId, setActiveId] = useState<string | null>(null);

  const activeRow = useMemo(
    () => rows.find((r) => r.id === activeId) || null,
    [activeId, rows],
  );

  const filteredContractSuggestions = useMemo(() => {
    if (!suggestions.length || !activeRow) return [];
    const q = activeRow.value.trim().toLowerCase();
    let pool = [...suggestions];
    if (q) {
      pool = pool.filter((s) => s.value.toLowerCase().includes(q));
      pool.sort((a, b) => {
        const av = a.value.toLowerCase();
        const bv = b.value.toLowerCase();
        const ap = av.startsWith(q) ? 0 : 1;
        const bp = bv.startsWith(q) ? 0 : 1;
        if (ap !== bp) return ap - bp;
        return av.localeCompare(bv);
      });
    } else {
      pool.sort((a, b) => a.value.localeCompare(b.value));
    }
    const dedupe: ContractFieldSuggestion[] = [];
    const seen = new Set<string>();
    for (const s of pool) {
      if (seen.has(s.value)) continue;
      seen.add(s.value);
      dedupe.push(s);
      if (dedupe.length >= 12) break;
    }
    return dedupe;
  }, [activeRow, suggestions]);

  useEffect(() => {
    if (rows.length === 0) {
      setActiveId(null);
      return;
    }
    if (!activeId || !rows.some((r) => r.id === activeId)) {
      setActiveId(rows[0].id);
    }
  }, [activeId, rows]);

  const updateRow = useCallback(
    (id: string, patch: Partial<ContractStringRow>) => {
      onChange(rows.map((r) => (r.id === id ? { ...r, ...patch } : r)));
    },
    [onChange, rows],
  );

  const addRow = useCallback(() => {
    const id = nextRowId(fieldId);
    onChange([...rows, { id, value: '' }]);
    setActiveId(id);
  }, [fieldId, onChange, rows]);

  const removeRow = useCallback(
    (id: string) => {
      const next = rows.filter((r) => r.id !== id);
      if (next.length === 0) {
        const fallback = { id: nextRowId(fieldId), value: '' };
        onChange([fallback]);
        setActiveId(fallback.id);
        return;
      }
      onChange(next);
      setActiveId((cur) => (cur === id ? next[0].id : cur));
    },
    [fieldId, onChange, rows],
  );

  const moveRow = useCallback(
    (id: string, delta: -1 | 1) => {
      const index = rows.findIndex((r) => r.id === id);
      if (index < 0) return;
      const target = index + delta;
      if (target < 0 || target >= rows.length) return;
      const next = [...rows];
      const [removed] = next.splice(index, 1);
      next.splice(target, 0, removed);
      onChange(next);
    },
    [onChange, rows],
  );

  const inputId = `${fieldId}-input`;

  return (
    <div className="moon-contract-string-list" aria-label={ariaLabel}>
      <div className="moon-trigger-pill-row" role="list">
        {rows.map((row) => (
          <button
            key={row.id}
            type="button"
            role="listitem"
            className={`moon-trigger-pill${activeId === row.id ? ' moon-trigger-pill--active' : ''}`}
            onClick={() => setActiveId(row.id)}
            aria-pressed={activeId === row.id}
            aria-label={`${ariaLabel} field: ${chipLabel(row)}`}
          >
            <span className="moon-trigger-pill__label">{chipLabel(row)}</span>
          </button>
        ))}
        <button
          type="button"
          className="moon-trigger-pill moon-trigger-pill--add"
          onClick={addRow}
          aria-label={`Add ${ariaLabel} field`}
        >
          + Add field
        </button>
      </div>
      {activeRow && (
        <div className="moon-trigger-pill-editor">
          <label className="moon-dock-form__label" htmlFor={inputId}>
            Edit selected field
          </label>
          <input
            id={inputId}
            className="moon-dock-form__input"
            type="text"
            value={activeRow.value}
            onChange={(e) => updateRow(activeRow.id, { value: e.target.value })}
            placeholder={inputPlaceholder}
            autoComplete="off"
            spellCheck={false}
            aria-autocomplete="list"
            aria-controls={`${fieldId}-suggest`}
          />
          {filteredContractSuggestions.length > 0 ? (
            <>
              <div className="moon-contract-suggestions-label" id={`${fieldId}-suggest-label`}>
                Suggested from this graph and object types
              </div>
              <div
                className="moon-contract-suggestions"
                id={`${fieldId}-suggest`}
                role="listbox"
                aria-label={`${ariaLabel} suggestions`}
                aria-labelledby={`${fieldId}-suggest-label`}
              >
                {filteredContractSuggestions.map((s) => (
                  <button
                    key={`${s.value}__${s.detail ?? ''}`}
                    type="button"
                    role="option"
                    className="moon-contract-suggestion"
                    onClick={() => updateRow(activeRow.id, { value: s.value })}
                    title={s.detail}
                  >
                    <span className="moon-contract-suggestion__value">{s.value}</span>
                    {s.detail ? (
                      <span className="moon-contract-suggestion__detail">{s.detail}</span>
                    ) : null}
                  </button>
                ))}
              </div>
            </>
          ) : null}
          <div className="moon-dock-form__actions">
            <button
              type="button"
              className="moon-dock-form__btn--small"
              onClick={() => moveRow(activeRow.id, -1)}
              disabled={rows.findIndex((r) => r.id === activeRow.id) <= 0}
            >
              Move up
            </button>
            <button
              type="button"
              className="moon-dock-form__btn--small"
              onClick={() => moveRow(activeRow.id, 1)}
              disabled={rows.findIndex((r) => r.id === activeRow.id) >= rows.length - 1}
            >
              Move down
            </button>
            <button
              type="button"
              className="moon-dock-form__btn--small"
              onClick={() => removeRow(activeRow.id)}
            >
              Remove
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
