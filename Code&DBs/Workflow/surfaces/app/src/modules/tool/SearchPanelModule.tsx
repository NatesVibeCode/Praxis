import React, { useState, useEffect, useRef, useCallback } from 'react';
import { QuadrantProps } from '../types';
import { LoadingSkeleton } from '../../primitives/LoadingSkeleton';
import { publishSelection } from '../../hooks/useWorldSelection';
import { world } from '../../world';

interface SearchResult {
  name: string;
  description?: string;
  kind?: string;
  [key: string]: unknown;
}

interface SearchPanelConfig {
  endpoint?: string;
  placeholder?: string;
  objectType?: string;
  publishSelection?: string;  // world key for selected result (e.g. 'search_result')
  publishQuery?: string;      // world key for live query text (e.g. 'search_query')
}

function SearchPanelModule({ config }: QuadrantProps) {
  const cfg = (config ?? {}) as SearchPanelConfig;
  const endpoint = cfg.endpoint ?? 'search';
  const placeholder = cfg.placeholder ?? 'Search...';

  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Publish query to world state so other modules can filter reactively
  const publishQueryState = useCallback((q: string) => {
    if (cfg.publishQuery) {
      world.applyDeltas([{
        op: 'put',
        path: `shared.${cfg.publishQuery}`,
        value: q || null,
        version: world.version + 1,
      }]);
    }
  }, [cfg.publishQuery]);

  // Publish selected result + fire DOM event for imperative listeners
  const handleResultClick = useCallback((result: SearchResult) => {
    if (cfg.publishSelection) {
      publishSelection(cfg.publishSelection, result);
    }
    window.dispatchEvent(new CustomEvent('module-search', {
      detail: { query, result, objectType: cfg.objectType ?? result.kind },
    }));
  }, [query, cfg.publishSelection, cfg.objectType]);

  useEffect(() => {
    if (timerRef.current) clearTimeout(timerRef.current);
    if (!query.trim()) {
      setResults([]);
      publishQueryState('');
      return;
    }
    timerRef.current = setTimeout(async () => {
      setLoading(true);
      publishQueryState(query);
      try {
        let mapped: SearchResult[];
        if (cfg.objectType) {
          const res = await fetch(`/api/objects?type=${encodeURIComponent(cfg.objectType)}&q=${encodeURIComponent(query)}`);
          if (!res.ok) throw new Error(res.statusText);
          const data = await res.json();
          const objects = Array.isArray(data) ? data : data.objects ?? [];
          mapped = objects.map((obj: any) => {
            const props = obj.properties ?? {};
            const entries = Object.entries(props);
            const textEntries = entries.filter(([, v]) => typeof v === 'string');
            const name = textEntries.length > 0 ? String(textEntries[0][1]) : obj.object_id;
            const rest = textEntries.slice(1).map(([, v]) => String(v)).join(' · ');
            return { name, description: rest || undefined, kind: cfg.objectType };
          });
        } else {
          const res = await fetch(`/api/${endpoint}?q=${encodeURIComponent(query)}`);
          if (!res.ok) throw new Error(res.statusText);
          const data = await res.json();
          mapped = Array.isArray(data) ? data : data.results ?? [];
        }
        setResults(mapped);
        // Emit search event with full results for imperative consumers
        window.dispatchEvent(new CustomEvent('module-search', {
          detail: { query, results: mapped, objectType: cfg.objectType },
        }));
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 300);
    return () => { if (timerRef.current) clearTimeout(timerRef.current); };
  }, [query, endpoint]);

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', gap: 'var(--space-sm, 8px)',
      padding: 'var(--space-md, 16px)', width: '100%', height: '100%',
      boxSizing: 'border-box', backgroundColor: 'var(--bg-card, #161b22)',
      borderRadius: 'var(--radius, 8px)',
    }}>
      <input
        type="text"
        placeholder={placeholder}
        value={query}
        onChange={e => setQuery(e.target.value)}
        style={{
          backgroundColor: 'var(--bg, #0d1117)', color: 'var(--text, #c9d1d9)',
          border: '1px solid var(--border, #30363d)', borderRadius: 'var(--radius, 8px)',
          padding: '10px 12px', fontSize: '14px', fontFamily: 'var(--font-sans, sans-serif)',
        }}
      />

      <div style={{ flex: 1, overflowY: 'auto' }}>
        {loading && (
          <div style={{ padding: '8px 0' }}>
            <LoadingSkeleton lines={4} height={16} widths={['100%', '88%', '94%', '72%']} />
          </div>
        )}
        {!loading && query.trim() && results.length === 0 && (
          <div style={{ color: 'var(--text-muted, #8b949e)', padding: '8px 0', fontSize: '13px' }}>
            No results
          </div>
        )}
        {results.map((r, i) => (
          <div key={i} onClick={() => handleResultClick(r)} style={{
            padding: '8px 10px', borderBottom: '1px solid var(--border, #30363d)',
            display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer',
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
            {r.kind && (
              <span style={{
                backgroundColor: 'var(--bg, #0d1117)', color: 'var(--accent, #58a6ff)',
                padding: '2px 8px', borderRadius: '12px', fontSize: '11px', fontWeight: 600,
                border: '1px solid var(--border, #30363d)', whiteSpace: 'nowrap',
              }}>
                {r.kind}
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default SearchPanelModule;
