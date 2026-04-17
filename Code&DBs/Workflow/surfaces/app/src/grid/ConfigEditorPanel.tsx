import React, { useEffect, useId, useMemo, useState } from 'react';
import './ConfigEditorPanel.css';
import {
  defaultConfigValueForKey,
  GRID_ACTION_VARIANTS,
  GRID_CHART_TYPE_KEYS,
  GRID_CHART_TYPES,
  GRID_TEXT_KEYS,
  gridFieldLabel,
  gridSpanLabel,
  normalizeGridEndpoint,
} from './moduleConfigMetadata';
import { useEndpointOptions } from './useEndpointOptions';

interface ConfigEditorPanelProps {
  quadrantId: string;
  moduleId: string;
  span: string;
  availableSpans: string[];
  config: Record<string, unknown>;
  focusKey?: string | null;
  onSave: (result: { config: Record<string, unknown>; span: string }) => void;
  onClose: () => void;
}

interface ColumnDef {
  key: string;
  label: string;
  sortable: boolean;
}

interface ActionDef {
  label: string;
  variant: string;
}

function TextInput({ label, value, onChange }: { label: string; value: string; onChange: (v: string) => void }) {
  return (
    <div className="config-editor-field">
      <label>{label}</label>
      <input type="text" value={value} onChange={e => onChange(e.target.value)} />
    </div>
  );
}

function NumberInput({ label, value, onChange }: { label: string; value: number; onChange: (v: number) => void }) {
  return (
    <div className="config-editor-field">
      <label>{label}</label>
      <input type="number" value={value} onChange={e => onChange(Number(e.target.value))} />
    </div>
  );
}

function DropdownInput({ label, value, options, onChange }: { label: string; value: string; options: string[]; onChange: (v: string) => void }) {
  return (
    <div className="config-editor-field">
      <label>{label}</label>
      <select value={value} onChange={e => onChange(e.target.value)}>
        {options.map(o => <option key={o} value={o}>{o}</option>)}
      </select>
    </div>
  );
}

function ColumnsEditor({ columns, onChange }: { columns: ColumnDef[]; onChange: (v: ColumnDef[]) => void }) {
  const update = (idx: number, patch: Partial<ColumnDef>) => {
    const next = columns.map((c, i) => i === idx ? { ...c, ...patch } : c);
    onChange(next);
  };
  const remove = (idx: number) => onChange(columns.filter((_, i) => i !== idx));
  const add = () => onChange([...columns, { key: '', label: '', sortable: false }]);

  return (
    <div className="config-editor-field">
      <label>{gridFieldLabel('columns')}</label>
      {columns.map((col, i) => (
        <div key={i} className="config-editor-array-item">
          <input placeholder="key" value={col.key} onChange={e => update(i, { key: e.target.value })} />
          <input placeholder="label" value={col.label} onChange={e => update(i, { label: e.target.value })} />
          <label style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <input
              type="checkbox"
              checked={col.sortable}
              onChange={e => update(i, { sortable: e.target.checked })}
            />
            sort
          </label>
          <button type="button" className="config-editor-array-remove" onClick={() => remove(i)} aria-label="Remove column">
            &times;
          </button>
        </div>
      ))}
      <button type="button" className="config-editor-array-add" onClick={add}>
        + column
      </button>
    </div>
  );
}

function ActionsEditor({ actions, onChange }: { actions: ActionDef[]; onChange: (v: ActionDef[]) => void }) {
  const update = (idx: number, patch: Partial<ActionDef>) => {
    const next = actions.map((a, i) => i === idx ? { ...a, ...patch } : a);
    onChange(next);
  };
  const remove = (idx: number) => onChange(actions.filter((_, i) => i !== idx));
  const add = () => onChange([...actions, { label: '', variant: 'primary' }]);

  return (
    <div className="config-editor-field">
      <label>{gridFieldLabel('actions')}</label>
      {actions.map((action, i) => (
        <div key={i} className="config-editor-array-item">
          <input placeholder="label" value={action.label} onChange={e => update(i, { label: e.target.value })} />
          <select value={action.variant} onChange={e => update(i, { variant: e.target.value })}>
            {GRID_ACTION_VARIANTS.map(v => <option key={v} value={v}>{v}</option>)}
          </select>
          <button type="button" className="config-editor-array-remove" onClick={() => remove(i)} aria-label="Remove action">
            &times;
          </button>
        </div>
      ))}
      <button type="button" className="config-editor-array-add" onClick={add}>
        + action
      </button>
    </div>
  );
}

function RawJsonEditor({ label, value, onChange }: { label: string; value: unknown; onChange: (v: unknown) => void }) {
  const [text, setText] = useState(JSON.stringify(value, null, 2));
  const [error, setError] = useState(false);

  useEffect(() => {
    setText(JSON.stringify(value, null, 2));
  }, [value]);

  const handleChange = (raw: string) => {
    setText(raw);
    try {
      onChange(JSON.parse(raw));
      setError(false);
    } catch {
      setError(true);
    }
  };

  return (
    <div className="config-editor-field">
      <label>{label}</label>
      <textarea
        value={text}
        onChange={e => handleChange(e.target.value)}
        style={error ? { borderColor: 'var(--danger)' } : undefined}
      />
    </div>
  );
}

function EndpointInput({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const listId = useId();
  const { options, loading, error } = useEndpointOptions();
  const [inputValue, setInputValue] = useState(value);

  useEffect(() => {
    setInputValue(value);
  }, [value]);

  return (
    <div className="config-editor-field">
      <label>{gridFieldLabel('endpoint')}</label>
      <input
        type="text"
        list={listId}
        value={inputValue}
        onChange={e => {
          const rawValue = e.target.value;
          setInputValue(rawValue);
          onChange(normalizeGridEndpoint(rawValue));
        }}
        onBlur={() => {
          const normalized = normalizeGridEndpoint(inputValue);
          setInputValue(normalized);
          onChange(normalized);
        }}
        placeholder="/api/platform-overview"
        spellCheck={false}
      />
      <datalist id={listId}>
        {options.map((option) => (
          <option
            key={option.value}
            value={option.value}
            label={option.description ? `${option.label} — ${option.description}` : option.label}
          />
        ))}
      </datalist>
      <div className="config-editor-field__hint">
        {error
          ? 'Route suggestions are unavailable right now. You can still enter any readable /api path manually.'
          : loading
            ? 'Loading live GET endpoints from the route catalog…'
            : 'Suggestions come from the live route catalog. You can also type any readable /api path manually.'}
      </div>
    </div>
  );
}

function SpanInput({
  value,
  options,
  onChange,
}: {
  value: string;
  options: string[];
  onChange: (v: string) => void;
}) {
  return (
    <div className="config-editor-field">
      <label>{gridFieldLabel('span')}</label>
      <select
        value={value}
        onChange={(event) => onChange(event.target.value)}
        disabled={options.length === 0}
      >
        {options.map((spanOption) => (
          <option key={spanOption} value={spanOption}>
            {gridSpanLabel(spanOption)}
          </option>
        ))}
      </select>
      <div className="config-editor-field__hint">
        Resize the module footprint directly from the editor. Only sizes that fit the current grid position are shown.
      </div>
    </div>
  );
}

function orderConfigKeys(keys: string[]): string[] {
  const priority = [
    'title',
    'label',
    'placeholder',
    'subscribeSelection',
    'publishSelection',
    'endpoint',
    'path',
    'refreshInterval',
    'columns',
    'actions',
  ];

  return [...keys].sort((left, right) => {
    const leftRank = priority.indexOf(left);
    const rightRank = priority.indexOf(right);
    if (leftRank >= 0 || rightRank >= 0) {
      if (leftRank < 0) return 1;
      if (rightRank < 0) return -1;
      return leftRank - rightRank;
    }
    return left.localeCompare(right);
  });
}

export function ConfigEditorPanel({
  quadrantId,
  moduleId,
  span,
  availableSpans,
  config,
  focusKey,
  onSave,
  onClose,
}: ConfigEditorPanelProps) {
  const [draft, setDraft] = useState<Record<string, unknown>>(() => structuredClone(config));
  const [draftSpan, setDraftSpan] = useState(span);

  useEffect(() => {
    setDraft(structuredClone(config));
    setDraftSpan(span);
  }, [config, moduleId, quadrantId, span]);

  useEffect(() => {
    if (!focusKey) return;
    if (focusKey === 'span') return;
    setDraft((prev) => {
      if (focusKey in prev) return prev;
      return {
        ...prev,
        [focusKey]: defaultConfigValueForKey(focusKey),
      };
    });
  }, [focusKey]);

  const set = (key: string, value: unknown) => {
    setDraft(prev => ({ ...prev, [key]: value }));
  };

  const keys = useMemo(() => orderConfigKeys(Object.keys(draft)), [draft]);

  useEffect(() => {
    if (!focusKey) return;
    const timer = window.setTimeout(() => {
      const root = document.querySelector('.config-editor-panel');
      const field = root?.querySelector(`[data-config-key="${focusKey}"]`) as HTMLElement | null;
      const target = field?.querySelector('input, select, textarea') as HTMLElement | null;
      field?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
      target?.focus();
    }, 40);
    return () => window.clearTimeout(timer);
  }, [draft, draftSpan, focusKey]);

  return (
    <div className="config-editor-panel">
      <div className="config-editor-header">
        <h3>{moduleId}</h3>
        <button type="button" className="config-editor-close" onClick={onClose} aria-label="Close config editor">
          &times;
        </button>
      </div>

      <div className="config-editor-body">
        <div data-config-key="span">
          <SpanInput
            value={draftSpan}
            options={availableSpans.length > 0 ? availableSpans : [draftSpan]}
            onChange={setDraftSpan}
          />
        </div>
        {keys.map(key => {
          if (key === 'endpoint') {
            return (
              <div key={key} data-config-key={key}>
                <EndpointInput value={String(draft[key] ?? '')} onChange={v => set(key, v)} />
              </div>
            );
          }
          if (key === 'presetId') return null;
          if (GRID_TEXT_KEYS.has(key)) {
            return (
              <div key={key} data-config-key={key}>
                <TextInput label={gridFieldLabel(key)} value={String(draft[key] ?? '')} onChange={v => set(key, v)} />
              </div>
            );
          }
          if (GRID_CHART_TYPE_KEYS.has(key)) {
            return (
              <div key={key} data-config-key={key}>
                <DropdownInput label={gridFieldLabel(key)} value={String(draft[key] ?? 'bar')} options={[...GRID_CHART_TYPES]} onChange={v => set(key, v)} />
              </div>
            );
          }
          if (key === 'refreshInterval') {
            return (
              <div key={key} data-config-key={key}>
                <NumberInput label={gridFieldLabel(key)} value={Number(draft[key] ?? 0)} onChange={v => set(key, v)} />
              </div>
            );
          }
          if (key === 'columns') {
            const cols = (Array.isArray(draft[key]) ? draft[key] : []) as ColumnDef[];
            return (
              <div key={key} data-config-key={key}>
                <ColumnsEditor columns={cols} onChange={v => set(key, v)} />
              </div>
            );
          }
          if (key === 'actions') {
            const acts = (Array.isArray(draft[key]) ? draft[key] : []) as ActionDef[];
            return (
              <div key={key} data-config-key={key}>
                <ActionsEditor actions={acts} onChange={v => set(key, v)} />
              </div>
            );
          }
          return (
            <div key={key} data-config-key={key}>
              <RawJsonEditor label={gridFieldLabel(key)} value={draft[key]} onChange={v => set(key, v)} />
            </div>
          );
        })}
      </div>

      <div className="config-editor-footer">
        <button
          type="button"
          className="config-editor-save"
          onClick={() => onSave({ config: draft, span: draftSpan })}
        >
          Save changes
        </button>
      </div>
    </div>
  );
}
