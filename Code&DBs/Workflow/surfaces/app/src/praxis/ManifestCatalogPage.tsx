import React, { useEffect, useMemo, useState } from 'react';
import './ManifestCatalogPage.css';

interface ManifestCatalogRow {
  id: string;
  name?: string | null;
  description?: string | null;
  status?: string | null;
  manifest_family?: string | null;
  manifest_type?: string | null;
  updated_at?: string | null;
}

interface ManifestCatalogResponse {
  manifests?: ManifestCatalogRow[];
  count?: number;
  filters?: {
    q?: string | null;
    manifest_family?: string | null;
    manifest_type?: string | null;
    status?: string | null;
    limit?: number | null;
  };
  error?: string;
}

interface ManifestCatalogPageProps {
  onOpenManifest: (manifestId: string) => void;
  onEditManifest: (manifestId: string) => void;
}

function formatUpdatedAt(value?: string | null): string {
  if (!value) return 'Unknown';
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debouncedValue, setDebouncedValue] = useState(value);

  useEffect(() => {
    const timer = window.setTimeout(() => setDebouncedValue(value), delayMs);
    return () => window.clearTimeout(timer);
  }, [delayMs, value]);

  return debouncedValue;
}

export function ManifestCatalogPage({ onOpenManifest, onEditManifest }: ManifestCatalogPageProps) {
  const [query, setQuery] = useState('');
  const [manifestFamily, setManifestFamily] = useState('control_plane');
  const [manifestType, setManifestType] = useState('');
  const [status, setStatus] = useState('');
  const [limit, setLimit] = useState(25);
  const [manifests, setManifests] = useState<ManifestCatalogRow[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const debouncedQuery = useDebouncedValue(query, 250);

  useEffect(() => {
    const controller = new AbortController();
    const params = new URLSearchParams();
    const trimmedQuery = debouncedQuery.trim();
    const trimmedFamily = manifestFamily.trim();
    const trimmedType = manifestType.trim();
    const trimmedStatus = status.trim();

    if (trimmedQuery) params.set('q', trimmedQuery);
    if (trimmedFamily) params.set('manifest_family', trimmedFamily);
    if (trimmedType) params.set('manifest_type', trimmedType);
    if (trimmedStatus) params.set('status', trimmedStatus);
    params.set('limit', String(limit));

    setLoading(true);
    setError(null);

    const load = async () => {
      try {
        const response = await fetch(`/api/manifests?${params.toString()}`, { signal: controller.signal });
        const payload = await response.json().catch(() => null) as ManifestCatalogResponse | null;
        if (!response.ok || !payload) {
          throw new Error(payload?.error || 'Failed to load manifest catalog');
        }
        setManifests(Array.isArray(payload.manifests) ? payload.manifests : []);
        setCount(typeof payload.count === 'number' ? payload.count : 0);
      } catch (loadError) {
        if ((loadError as Error)?.name === 'AbortError') return;
        setError(loadError instanceof Error ? loadError.message : 'Failed to load manifest catalog');
      } finally {
        setLoading(false);
      }
    };

    void load();
    return () => controller.abort();
  }, [debouncedQuery, limit, manifestFamily, manifestType, status]);

  const activeFilterSummary = useMemo(() => {
    const parts = [
      debouncedQuery.trim() ? `q="${debouncedQuery.trim()}"` : null,
      manifestFamily.trim() ? `family=${manifestFamily.trim()}` : null,
      manifestType.trim() ? `type=${manifestType.trim()}` : null,
      status.trim() ? `status=${status.trim()}` : null,
      `limit=${limit}`,
    ].filter((value): value is string => Boolean(value));
    return parts.join(' · ');
  }, [debouncedQuery, limit, manifestFamily, manifestType, status]);

  return (
    <div className="manifest-catalog">
      <header className="manifest-catalog__header app-shell__surface-header">
        <div className="app-shell__surface-heading">
          <div className="app-shell__fallback-kicker">Manifest catalog</div>
          <div className="app-shell__surface-title">Discover control-plane manifests</div>
          <p className="app-shell__surface-copy">
            Search raw manifests by family, type, status, or text before opening the exact id.
          </p>
        </div>
        <div className="manifest-catalog__count">
          <strong>{count}</strong>
          <span>rows</span>
        </div>
      </header>

      <section className="manifest-catalog__filters">
        <label className="manifest-catalog__field">
          <span>Search</span>
          <input
            type="text"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="plan, approval, cleanup..."
          />
        </label>
        <label className="manifest-catalog__field">
          <span>Family</span>
          <input
            type="text"
            value={manifestFamily}
            onChange={(event) => setManifestFamily(event.target.value)}
            placeholder="control_plane"
          />
        </label>
        <label className="manifest-catalog__field">
          <span>Type</span>
          <input
            type="text"
            value={manifestType}
            onChange={(event) => setManifestType(event.target.value)}
            placeholder="data_plan"
          />
        </label>
        <label className="manifest-catalog__field">
          <span>Status</span>
          <input
            type="text"
            value={status}
            onChange={(event) => setStatus(event.target.value)}
            placeholder="draft"
          />
        </label>
        <label className="manifest-catalog__field manifest-catalog__field--small">
          <span>Limit</span>
          <input
            type="number"
            min={1}
            max={100}
            value={limit}
            onChange={(event) => setLimit(Math.min(100, Math.max(1, Number.parseInt(event.target.value || '25', 10) || 25)))}
          />
        </label>
        <button
          type="button"
          className="manifest-catalog__reset"
          onClick={() => {
            setQuery('');
            setManifestFamily('control_plane');
            setManifestType('');
            setStatus('');
            setLimit(25);
          }}
        >
          Reset filters
        </button>
      </section>

      <div className="manifest-catalog__summary">
        {loading ? 'Loading manifests...' : error ? error : activeFilterSummary}
      </div>

      <section className="manifest-catalog__results">
        {loading ? (
          <div className="manifest-catalog__empty">Loading catalog...</div>
        ) : error ? (
          <div className="manifest-catalog__empty manifest-catalog__empty--error">{error}</div>
        ) : manifests.length === 0 ? (
          <div className="manifest-catalog__empty">No manifests matched the current filters.</div>
        ) : (
          manifests.map((manifest) => (
            <article key={manifest.id} className="manifest-catalog__card">
              <div className="manifest-catalog__card-header">
                <div>
                  <div className="manifest-catalog__card-title">{manifest.name || manifest.id}</div>
                  <div className="manifest-catalog__card-id">{manifest.id}</div>
                </div>
                <div className="manifest-catalog__card-actions">
                  <button type="button" onClick={() => onOpenManifest(manifest.id)}>
                    Open
                  </button>
                  <button type="button" onClick={() => onEditManifest(manifest.id)}>
                    Edit JSON
                  </button>
                </div>
              </div>
              <p className="manifest-catalog__card-copy">
                {manifest.description || 'No description provided.'}
              </p>
              <div className="manifest-catalog__tags">
                <span className="manifest-catalog__tag">status: {manifest.status || 'unknown'}</span>
                <span className="manifest-catalog__tag">family: {manifest.manifest_family || 'unknown'}</span>
                <span className="manifest-catalog__tag">type: {manifest.manifest_type || 'unknown'}</span>
                <span className="manifest-catalog__tag">updated: {formatUpdatedAt(manifest.updated_at)}</span>
              </div>
            </article>
          ))
        )}
      </section>
    </div>
  );
}
