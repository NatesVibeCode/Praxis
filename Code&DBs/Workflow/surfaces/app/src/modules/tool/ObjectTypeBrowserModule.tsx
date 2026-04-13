import React, { useState, useEffect } from 'react';
import { QuadrantProps } from '../types';
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

function ObjectTypeBrowserModule({ config }: QuadrantProps) {
  void config;
  const [types, setTypes] = useState<ObjectType[]>([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState('');
  const [newDesc, setNewDesc] = useState('');
  const [newProps, setNewProps] = useState<PropertyDef[]>([{ name: '', type: 'text' }]);

  const loadTypes = async () => {
    try {
      const res = await fetch('/api/object-types');
      const data = await res.json();
      setTypes(data.types ?? []);
    } catch { /* ignore */ }
    setLoading(false);
  };

  useEffect(() => { loadTypes(); }, []);

  const selectType = (t: ObjectType) => {
    setSelected(t.type_id);
    world.set('shared.selected_object_type', t);
  };

  const addProp = () => setNewProps([...newProps, { name: '', type: 'text' }]);
  const removeProp = (i: number) => setNewProps(newProps.filter((_, idx) => idx !== i));
  const updateProp = (i: number, field: string, value: string | boolean) => {
    const updated = [...newProps];
    (updated[i] as any)[field] = value;
    setNewProps(updated);
  };

  const handleCreate = async () => {
    if (!newName.trim()) return;
    try {
      await fetch('/api/object-types', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newName, description: newDesc,
          property_definitions: newProps.filter(p => p.name.trim()),
        }),
      });
      setCreating(false);
      setNewName(''); setNewDesc(''); setNewProps([{ name: '', type: 'text' }]);
      loadTypes();
    } catch { /* ignore */ }
  };

  const s = { bg: 'var(--bg-card)', border: 'var(--border)', radius: 'var(--radius)', accent: 'var(--accent)', text: 'var(--text)', muted: 'var(--text-muted)' };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: 16, height: '100%', boxSizing: 'border-box' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontWeight: 600, fontSize: 15 }}>Object Types</span>
        <button onClick={() => setCreating(!creating)} style={{
          background: s.accent, color: '#fff', border: 'none', borderRadius: 6, padding: '4px 12px', fontSize: 12, fontWeight: 600, cursor: 'pointer'
        }}>{creating ? 'Cancel' : '+ Create'}</button>
      </div>

      {creating && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, padding: 12, background: 'var(--bg)', borderRadius: 6, border: '1px solid var(--border)' }}>
          <input placeholder="Type name" value={newName} onChange={e => setNewName(e.target.value)}
            style={{ background: 'var(--bg)', color: s.text, border: '1px solid var(--border)', borderRadius: 6, padding: '6px 10px', fontSize: 13 }} />
          <textarea placeholder="Description" value={newDesc} onChange={e => setNewDesc(e.target.value)} rows={2}
            style={{ background: 'var(--bg)', color: s.text, border: '1px solid var(--border)', borderRadius: 6, padding: '6px 10px', fontSize: 13, resize: 'none' }} />
          <div style={{ fontSize: 12, fontWeight: 600, color: s.muted }}>Properties</div>
          {newProps.map((p, i) => (
            <div key={i} style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
              <input placeholder="name" value={p.name} onChange={e => updateProp(i, 'name', e.target.value)}
                style={{ flex: 1, background: 'var(--bg)', color: s.text, border: '1px solid var(--border)', borderRadius: 4, padding: '4px 8px', fontSize: 12 }} />
              <select value={p.type} onChange={e => updateProp(i, 'type', e.target.value)}
                style={{ background: 'var(--bg)', color: s.text, border: '1px solid var(--border)', borderRadius: 4, padding: '4px', fontSize: 12 }}>
                {['text', 'number', 'date', 'email', 'url', 'boolean', 'dropdown', 'currency'].map(t => <option key={t} value={t}>{t}</option>)}
              </select>
              <button onClick={() => removeProp(i)} style={{ background: 'none', border: 'none', color: 'var(--danger)', cursor: 'pointer', fontSize: 14 }}>×</button>
            </div>
          ))}
          <div style={{ display: 'flex', gap: 8 }}>
            <button onClick={addProp} style={{ background: 'none', border: 'none', color: s.accent, cursor: 'pointer', fontSize: 12 }}>+ Add property</button>
            <button onClick={handleCreate} style={{ marginLeft: 'auto', background: s.accent, color: '#fff', border: 'none', borderRadius: 4, padding: '4px 12px', fontSize: 12, cursor: 'pointer' }}>Save</button>
          </div>
        </div>
      )}

      <div style={{ flex: 1, overflowY: 'auto' }}>
        {loading && <div style={{ color: s.muted, fontSize: 13 }}>Loading...</div>}
        {types.map(t => (
          <div key={t.type_id} onClick={() => selectType(t)} style={{
            padding: '8px 10px', cursor: 'pointer', borderBottom: '1px solid var(--border)',
            background: selected === t.type_id ? 'rgba(88,166,255,0.08)' : undefined,
          }}>
            <div style={{ fontWeight: 500, fontSize: 13 }}>{t.icon || '📦'} {t.name}</div>
            <div style={{ color: s.muted, fontSize: 11 }}>{t.property_definitions?.length ?? 0} properties · {t.description?.slice(0, 60)}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
export default ObjectTypeBrowserModule;
