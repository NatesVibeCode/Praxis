import React, { useState, useEffect, useRef } from 'react';
import { QuadrantProps } from '../types';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';

interface RegistryEntry {
  name: string;
  description?: string;
  category?: string;
}

type TabFilter = 'All' | 'UI' | 'Calcs' | 'Workflows';

const tabs: TabFilter[] = ['All', 'UI', 'Calcs', 'Workflows'];

function RegistryBrowserModule({ config }: QuadrantProps) {
  const [query, setQuery] = useState('');
  const [activeTab, setActiveTab] = useState<TabFilter>('All');
  const [results, setResults] = useState<RegistryEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (timerRef.current) clearTimeout(timerRef.current);
    if (!query.trim()) {
      setResults([]);
      return;
    }
    timerRef.current = setTimeout(async () => {
      setLoading(true);
      try {
        const res = await fetch(`/api/registries/search?q=${encodeURIComponent(query)}`);
        if (!res.ok) throw new Error(res.statusText);
        const data = await res.json();
        setResults(Array.isArray(data) ? data : data.results ?? []);
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 300);
    return () => { if (timerRef.current) clearTimeout(timerRef.current); };
  }, [query]);

  const filtered = activeTab === 'All'
    ? results
    : results.filter(r => r.category?.toLowerCase() === activeTab.toLowerCase());

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', gap: 'var(--space-sm, 8px)',
      padding: 'var(--space-md, 16px)', width: '100%', height: '100%',
      boxSizing: 'border-box', backgroundColor: 'var(--bg-card, #161b22)',
      borderRadius: 'var(--radius, 8px)',
    }}>
      {/* Search + tabs */}
      <input
        type="text"
        placeholder="Search registries..."
        value={query}
        onChange={e => setQuery(e.target.value)}
        style={{
          backgroundColor: 'var(--bg, #0d1117)', color: 'var(--text, #c9d1d9)',
          border: '1px solid var(--border, #30363d)', borderRadius: 'var(--radius, 8px)',
          padding: '10px 12px', fontSize: '14px', fontFamily: 'var(--font-sans, sans-serif)',
        }}
      />

      <div style={{ display: 'flex', gap: '4px' }}>
        {tabs.map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={{
              backgroundColor: activeTab === tab ? 'var(--accent, #58a6ff)' : 'var(--bg, #0d1117)',
              color: activeTab === tab ? '#fff' : 'var(--text-muted, #8b949e)',
              border: '1px solid var(--border, #30363d)', borderRadius: 'var(--radius, 8px)',
              padding: '4px 12px', fontSize: '12px', cursor: 'pointer', fontWeight: 500,
            }}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* Results */}
      <div style={{ flex: 1, overflowY: 'auto' }}>
        {loading && (
          <div style={{ padding: '8px 0' }}>
            <LoadingSkeleton lines={4} height={16} widths={['100%', '88%', '94%', '72%']} />
          </div>
        )}
        {!loading && query.trim() && filtered.length === 0 && (
          <div style={{ color: 'var(--text-muted, #8b949e)', padding: '8px 0', fontSize: '13px' }}>
            No results
          </div>
        )}
        {filtered.map((r, i) => (
          <div key={i} style={{
            padding: '8px 10px', borderBottom: '1px solid var(--border, #30363d)',
            display: 'flex', alignItems: 'center', gap: '8px',
          }}>
            <div style={{ flex: 1 }}>
              <div style={{ color: 'var(--text, #c9d1d9)', fontSize: '14px', fontWeight: 500 }}>
                {r.name}
              </div>
              {r.description && (
                <div style={{ color: 'var(--text-muted, #8b949e)', fontSize: '12px', marginTop: '2px' }}>
                  {r.description.length > 120 ? r.description.slice(0, 120) + '...' : r.description}
                </div>
              )}
            </div>
            {r.category && (
              <span style={{
                backgroundColor: 'var(--bg, #0d1117)', color: 'var(--accent, #58a6ff)',
                padding: '2px 8px', borderRadius: '12px', fontSize: '11px', fontWeight: 600,
                border: '1px solid var(--border, #30363d)', whiteSpace: 'nowrap',
              }}>
                {r.category}
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default RegistryBrowserModule;
