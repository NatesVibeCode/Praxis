import React, { useState, useMemo, useEffect, useRef } from 'react';
import type { OrbitNode, DockContent } from './moonBuildPresenter';
import type { CatalogItem, CatalogFamily } from './catalog';
import { MoonGlyph } from './MoonGlyph';

interface Props {
  node: OrbitNode;
  content: DockContent | null;
  onClose: () => void;
  onSelect: (nodeId: string, value: string) => void;
  catalog: CatalogItem[];
}

const NODE_FAMILIES: CatalogFamily[] = ['trigger', 'gather', 'think', 'act'];

function questionFor(node: OrbitNode): string {
  if (!node.route) return 'What should this step do?';
  if (node.needsBadge) return 'What else does this step need?';
  return 'Change this step?';
}

const MAX_VISIBLE = 8;

export function MoonPopout({ node, content, onClose, onSelect, catalog }: Props) {
  // Default to a sensible family based on node state
  const defaultFamily = useMemo((): CatalogFamily | null => {
    if (!node.route) return 'think'; // Unresolved nodes → show think/process actions first
    return null;
  }, [node.route]);
  const [familyFilter, setFamilyFilter] = useState<CatalogFamily | null>(defaultFamily);
  const [search, setSearch] = useState('');
  const ref = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const mountedRef = useRef(true);

  const nodeActions = useMemo(() => catalog.filter(c => c.dropKind === 'node' && c.status === 'ready'), [catalog]);

  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);

  // Auto-focus search on open
  useEffect(() => { searchRef.current?.focus(); }, []);

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [onClose]);

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose();
      }
    };
    const timer = setTimeout(() => {
      if (mountedRef.current) document.addEventListener('click', handleClick);
    }, 50);
    return () => {
      clearTimeout(timer);
      document.removeEventListener('click', handleClick);
    };
  }, [onClose]);

  const filtered = useMemo(() => {
    let items = familyFilter ? nodeActions.filter(c => c.family === familyFilter) : nodeActions;
    if (search.trim()) {
      const q = search.toLowerCase();
      items = items.filter(c =>
        c.label.toLowerCase().includes(q) ||
        (c.description || '').toLowerCase().includes(q) ||
        (c.actionValue || '').toLowerCase().includes(q)
      );
    }
    return items;
  }, [nodeActions, familyFilter, search]);

  const visible = filtered.slice(0, MAX_VISIBLE);
  const overflow = filtered.length - visible.length;

  return (
    <div className="moon-popout" ref={ref}>
      <div className="moon-popout__card">
        <div className="moon-popout__title">{node.title}</div>
        <div className="moon-popout__subtitle">{questionFor(node)}</div>

        <input
          ref={searchRef}
          type="text"
          className="moon-dock-form__input"
          placeholder="Search capabilities..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{ marginBottom: 6, fontSize: 12, padding: '5px 8px' }}
        />

        <div className="moon-popout__filters">
          {NODE_FAMILIES.map(f => {
            const count = nodeActions.filter(c => c.family === f).length;
            return (
              <button
                key={f}
                className={`moon-catalog__filter${familyFilter === f ? ' moon-catalog__filter--active' : ''}`}
                onClick={() => { setFamilyFilter(familyFilter === f ? null : f); setSearch(''); }}
              >{f[0].toUpperCase() + f.slice(1)} ({count})</button>
            );
          })}
        </div>

        <div className="moon-popout__catalog">
          {visible.map(item => (
            <button
              key={item.id}
              className={`moon-popout__catalog-item${node.route === item.actionValue ? ' moon-popout__catalog-item--active' : ''}`}
              onClick={(e) => { e.stopPropagation(); if (item.actionValue) onSelect(node.id, item.actionValue); }}
              title={item.description}
              draggable
              onDragStart={e => {
                e.dataTransfer.setData('moon/catalog-id', item.id);
                e.dataTransfer.setData('text/plain', item.label);
                e.dataTransfer.effectAllowed = 'copyLink';
              }}
            >
              <MoonGlyph type={item.icon} size={14} color={node.route === item.actionValue ? '#6CB6FF' : '#F4F6F8'} />
              <span>{item.label}</span>
              {item.source === 'integration' && item.connectionStatus && item.connectionStatus !== 'connected' && (
                <span style={{ fontSize: 9, color: 'var(--moon-fg-muted)', marginLeft: 4 }}>({item.connectionStatus})</span>
              )}
            </button>
          ))}
          {overflow > 0 && (
            <div style={{ fontSize: 11, color: 'var(--moon-fg-muted)', padding: '4px 8px', textAlign: 'center' }}>
              {overflow} more — type to search
            </div>
          )}
          {filtered.length === 0 && (
            <div style={{ fontSize: 11, color: 'var(--moon-fg-muted)', padding: '8px', textAlign: 'center' }}>
              No matches
            </div>
          )}
        </div>
      </div>
      <div className="moon-popout__line" />
    </div>
  );
}
