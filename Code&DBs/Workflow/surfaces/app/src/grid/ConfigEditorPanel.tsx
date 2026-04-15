import React, { useState, useEffect } from 'react';
import './ConfigEditorPanel.css';
import {
  defaultConfigValueForKey,
  GRID_ACTION_VARIANTS,
  GRID_CHART_TYPE_KEYS,
  GRID_CHART_TYPES,
  GRID_DATA_SOURCES,
  GRID_TEXT_KEYS,
} from './moduleConfigMetadata';

interface ConfigEditorPanelProps {
  quadrantId: string;
  moduleId: string;
  config: Record<string, unknown>;
  focusKey?: string | null;
  onSave: (newConfig: Record<string, unknown>) => void;
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
      <label>columns</label>
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
      <label>actions</label>
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

function DataSourceDropdown({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const isCustom = value !== '' && !GRID_DATA_SOURCES.some(ds => ds.value === value);
  const [showCustom, setShowCustom] = useState(isCustom);

  if (showCustom) {
    return (
      <div className="config-editor-field">
        <label>endpoint</label>
        <div style={{ display: 'flex', gap: 4 }}>
          <input
            type="text"
            value={value}
            onChange={e => onChange(e.target.value)}
            placeholder="custom endpoint path"
            style={{ flex: 1 }}
          />
          <button
            type="button"
            className="config-editor-array-remove"
            onClick={() => setShowCustom(false)}
            title="Switch to dropdown"
          >
            ↩
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="config-editor-field">
      <label>endpoint</label>
      <select
        value={GRID_DATA_SOURCES.some(ds => ds.value === value) ? value : ''}
        onChange={e => {
          if (e.target.value === '__custom__') {
            setShowCustom(true);
          } else {
            onChange(e.target.value);
          }
        }}
      >
        <option value="" disabled>Select data source...</option>
        {GRID_DATA_SOURCES.map(ds => (
          <option key={ds.value} value={ds.value}>{ds.label}</option>
        ))}
        <option value="__custom__">Custom endpoint...</option>
      </select>
    </div>
  );
}

export function ConfigEditorPanel({ quadrantId, moduleId, config, focusKey, onSave, onClose }: ConfigEditorPanelProps) {
  const [draft, setDraft] = useState<Record<string, unknown>>(() => structuredClone(config));

  useEffect(() => {
    setDraft(structuredClone(config));
  }, [config, moduleId, quadrantId]);

  useEffect(() => {
    if (!focusKey) return;
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

  const keys = Object.keys(draft);

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
  }, [draft, focusKey]);

  return (
    <div className="config-editor-panel">
      <div className="config-editor-header">
        <h3>{moduleId}</h3>
        <button type="button" className="config-editor-close" onClick={onClose} aria-label="Close config editor">
          &times;
        </button>
      </div>

      <div className="config-editor-body">
        {keys.map(key => {
          if (key === 'endpoint') {
            return (
              <div key={key} data-config-key={key}>
                <DataSourceDropdown value={String(draft[key] ?? '')} onChange={v => set(key, v)} />
              </div>
            );
          }
          if (key === 'presetId') return null;
          if (GRID_TEXT_KEYS.has(key)) {
            return (
              <div key={key} data-config-key={key}>
                <TextInput label={key} value={String(draft[key] ?? '')} onChange={v => set(key, v)} />
              </div>
            );
          }
          if (GRID_CHART_TYPE_KEYS.has(key)) {
            return (
              <div key={key} data-config-key={key}>
                <DropdownInput label={key} value={String(draft[key] ?? 'bar')} options={[...GRID_CHART_TYPES]} onChange={v => set(key, v)} />
              </div>
            );
          }
          if (key === 'refreshInterval') {
            return (
              <div key={key} data-config-key={key}>
                <NumberInput label={key} value={Number(draft[key] ?? 0)} onChange={v => set(key, v)} />
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
              <RawJsonEditor label={key} value={draft[key]} onChange={v => set(key, v)} />
            </div>
          );
        })}
      </div>

      <div className="config-editor-footer">
        <button type="button" className="config-editor-save" onClick={() => onSave(draft)}>
          Save changes
        </button>
      </div>
    </div>
  );
}
