import React, { useState, useEffect } from 'react';
import { QuadrantProps } from '../types';
import { useSlice } from '../../hooks/useSlice';
import { world } from '../../world';

interface PropertyDef {
  name: string;
  type: string;
  required?: boolean;
  options?: string[];
  default?: string;
}

interface ObjectType {
  type_id: string;
  name: string;
  description: string;
  icon?: string;
  property_definitions: PropertyDef[];
}

function SchemaEditorModule({ config }: QuadrantProps) {
  void config;
  const selectedRaw = useSlice(world, 'shared.selected_object_type');
  const selected = selectedRaw as ObjectType | null;
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [props, setProps] = useState<PropertyDef[]>([]);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    if (selected) {
      setName(selected.name);
      setDescription(selected.description || '');
      setProps([...(selected.property_definitions || [])]);
      setSaved(false);
    }
  }, [selected?.type_id]);

  const addProp = () => setProps([...props, { name: '', type: 'text' }]);
  const removeProp = (i: number) => setProps(props.filter((_, idx) => idx !== i));
  const updateProp = (i: number, field: string, value: string | boolean) => {
    const updated = [...props];
    (updated[i] as any)[field] = value;
    setProps(updated);
  };

  const handleSave = async () => {
    if (!selected) return;
    setSaving(true);
    setSaved(false);
    try {
      await fetch('/api/object-types', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name, description, type_id: selected.type_id,
          property_definitions: props.filter(p => p.name.trim()),
        }),
      });
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch { /* ignore */ }
    setSaving(false);
  };

  if (!selected) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 13, padding: 16 }}>
        Select an object type to edit its schema
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: 16, height: '100%', boxSizing: 'border-box', overflowY: 'auto' }}>
      <div style={{ fontWeight: 600, fontSize: 15 }}>Schema: {selected.name}</div>
      <input value={name} onChange={e => setName(e.target.value)} placeholder="Type name"
        style={{ background: 'var(--bg)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: 6, padding: '6px 10px', fontSize: 13 }} />
      <textarea value={description} onChange={e => setDescription(e.target.value)} placeholder="Description" rows={2}
        style={{ background: 'var(--bg)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: 6, padding: '6px 10px', fontSize: 13, resize: 'none' }} />

      <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-muted)', marginTop: 4 }}>Properties ({props.length})</div>
      {props.map((p, i) => (
        <div key={i} style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
          <input value={p.name} onChange={e => updateProp(i, 'name', e.target.value)} placeholder="name"
            style={{ flex: 1, background: 'var(--bg)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: 4, padding: '4px 8px', fontSize: 12 }} />
          <select value={p.type} onChange={e => updateProp(i, 'type', e.target.value)}
            style={{ background: 'var(--bg)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: 4, padding: '4px', fontSize: 12 }}>
            {['text', 'number', 'date', 'email', 'url', 'boolean', 'dropdown', 'currency'].map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          <label style={{ fontSize: 11, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 2 }}>
            <input type="checkbox" checked={!!p.required} onChange={e => updateProp(i, 'required', e.target.checked)} /> req
          </label>
          <button onClick={() => removeProp(i)} style={{ background: 'none', border: 'none', color: 'var(--danger)', cursor: 'pointer', fontSize: 14 }}>×</button>
        </div>
      ))}

      <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
        <button onClick={addProp} style={{ background: 'none', border: 'none', color: 'var(--accent)', cursor: 'pointer', fontSize: 12 }}>+ Add property</button>
        <button onClick={handleSave} disabled={saving} style={{
          marginLeft: 'auto', background: 'var(--accent)', color: '#fff', border: 'none', borderRadius: 4, padding: '4px 12px', fontSize: 12, cursor: 'pointer', opacity: saving ? 0.7 : 1
        }}>{saving ? 'Saving...' : 'Save Schema'}</button>
      </div>
      {saved && <div style={{ color: 'var(--success)', fontSize: 12 }}>Schema saved</div>}
    </div>
  );
}
export default SchemaEditorModule;
