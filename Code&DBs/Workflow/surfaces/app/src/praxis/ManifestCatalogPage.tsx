import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { normalizePraxisBundle, type ComposeSurfaceSpec, type PraxisSurfaceBundleV4 } from './manifest';
import { WorkspaceComposeSurface } from './WorkspaceComposeSurface';
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function retitleManifestPayload(value: unknown, nextTitle: string, previousTitle: string): Record<string, unknown> {
  const manifest = structuredClone(isRecord(value) ? value : {}) as Record<string, unknown>;
  manifest.title = nextTitle;
  manifest.name = nextTitle;

  const surfaces = isRecord(manifest.surfaces) ? manifest.surfaces : {};
  for (const surface of Object.values(surfaces)) {
    if (!isRecord(surface)) continue;
    if (surface.title === previousTitle || surface.title === undefined) {
      surface.title = nextTitle;
    }
    const surfaceManifest = isRecord(surface.manifest) ? surface.manifest : null;
    if (surfaceManifest && (surfaceManifest.title === previousTitle || surfaceManifest.title === undefined)) {
      surfaceManifest.title = nextTitle;
    }
  }

  return manifest;
}

async function saveManifestTitle(manifest: ManifestCatalogRow, nextTitle: string): Promise<ManifestCatalogRow> {
  const currentTitle = manifest.name || manifest.id;
  const manifestResponse = await fetch(`/api/manifests/${manifest.id}`);
  const manifestPayload = await manifestResponse.json().catch(() => null);
  if (!manifestResponse.ok || !manifestPayload) {
    throw new Error(manifestPayload?.error || `Failed to load ${manifest.id}`);
  }

  const nextManifest = retitleManifestPayload(manifestPayload, nextTitle, currentTitle);
  const saveResponse = await fetch('/api/manifests/save', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: manifest.id,
      name: nextTitle,
      description: manifestPayload.description ?? manifest.description ?? '',
      manifest: nextManifest,
    }),
  });
  const savePayload = await saveResponse.json().catch(() => null);
  if (!saveResponse.ok) {
    throw new Error(savePayload?.error || `Failed to rename ${manifest.id}`);
  }

  return {
    ...manifest,
    name: typeof savePayload?.name === 'string' ? savePayload.name : nextTitle,
    description: typeof savePayload?.description === 'string' ? savePayload.description : manifest.description,
    updated_at: new Date().toISOString(),
  };
}

function EditableCatalogTitle({
  manifest,
  onSaved,
  onError,
}: {
  manifest: ManifestCatalogRow;
  onSaved: (row: ManifestCatalogRow) => void;
  onError: (message: string) => void;
}) {
  const currentTitle = manifest.name || manifest.id;
  const [draftTitle, setDraftTitle] = useState(currentTitle);
  const [saving, setSaving] = useState(false);
  const skipNextBlurSave = useRef(false);

  useEffect(() => {
    setDraftTitle(currentTitle);
  }, [currentTitle]);

  const commit = async () => {
    const nextTitle = draftTitle.trim();
    if (!nextTitle || nextTitle === currentTitle) {
      setDraftTitle(currentTitle);
      return;
    }
    setSaving(true);
    try {
      const saved = await saveManifestTitle(manifest, nextTitle);
      onSaved(saved);
    } catch (err) {
      setDraftTitle(currentTitle);
      onError(err instanceof Error ? err.message : 'Rename failed');
    } finally {
      setSaving(false);
    }
  };

  return (
    <input
      className="manifest-catalog__card-title manifest-catalog__card-title-input"
      aria-label={`Rename ${currentTitle} (${manifest.id})`}
      value={draftTitle}
      disabled={saving}
      onChange={(event) => setDraftTitle(event.target.value)}
      onFocus={(event) => event.currentTarget.select()}
      onBlur={() => {
        if (skipNextBlurSave.current) {
          skipNextBlurSave.current = false;
          return;
        }
        void commit();
      }}
      onKeyDown={(event) => {
        if (event.key === 'Enter') {
          event.currentTarget.blur();
        }
        if (event.key === 'Escape') {
          skipNextBlurSave.current = true;
          setDraftTitle(currentTitle);
          event.currentTarget.blur();
        }
      }}
      spellCheck={false}
    />
  );
}

function formatUpdatedAt(value?: string | null): string {
  if (!value) return 'Unknown';
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

function isComposeWorkspaceRow(manifest: ManifestCatalogRow): boolean {
  return /^blank-workspace-[a-z0-9]+$/i.test(manifest.id);
}

function isComposeSeedRow(manifest: ManifestCatalogRow): boolean {
  return manifest.id === 'seed.workspace.blank'
    || ((manifest.name || '').trim().toLowerCase() === 'blank workspace'
      && (manifest.description || '').trim().toLowerCase().includes('workspace contract'));
}

function isComposeAuthoringRow(manifest: ManifestCatalogRow): boolean {
  return isComposeWorkspaceRow(manifest) || isComposeSeedRow(manifest);
}

function displayWorkspaceName(manifest: ManifestCatalogRow): string {
  if (isComposeWorkspaceRow(manifest) && (!manifest.name || manifest.name.toLowerCase() === 'blank workspace')) {
    return 'Compose';
  }
  if (isComposeSeedRow(manifest)) {
    return 'Compose seed';
  }
  return manifest.name || manifest.id;
}

function displayWorkspaceDescription(manifest: ManifestCatalogRow): string {
  const description = (manifest.description || '').trim();
  if (isComposeWorkspaceRow(manifest) && (!description || description.toLowerCase().includes('minimal workspace'))) {
    return 'Intent to contract to dispatch. This is the standalone authoring surface, backed by a workspace record.';
  }
  if (isComposeSeedRow(manifest)) {
    return 'Template for creating a Compose authoring workspace.';
  }
  return description || 'No description provided.';
}

function findComposeSurface(bundle: PraxisSurfaceBundleV4 | null): ComposeSurfaceSpec | null {
  if (!bundle) return null;
  const surface = Object.values(bundle.surfaces).find((candidate) => candidate.kind === 'compose');
  return surface?.kind === 'compose' ? surface : null;
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
  const [manifestFamily, setManifestFamily] = useState('');
  const [manifestType, setManifestType] = useState('');
  const [status, setStatus] = useState('');
  const [limit, setLimit] = useState(25);
  const [manifests, setManifests] = useState<ManifestCatalogRow[]>([]);
  const [composeBundle, setComposeBundle] = useState<PraxisSurfaceBundleV4 | null>(null);
  const [composeLoading, setComposeLoading] = useState(false);
  const [composeLoadError, setComposeLoadError] = useState<string | null>(null);
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
  const composeWorkspace = useMemo(
    () => manifests.find((manifest) => isComposeWorkspaceRow(manifest)) ?? null,
    [manifests],
  );
  const composeWorkspaceId = composeWorkspace?.id ?? null;
  const composeSurface = useMemo(() => findComposeSurface(composeBundle), [composeBundle]);
  const composeTitle = composeWorkspace ? displayWorkspaceName(composeWorkspace) : 'Compose';

  useEffect(() => {
    if (!composeWorkspaceId) {
      setComposeBundle(null);
      setComposeLoadError(null);
      setComposeLoading(false);
      return;
    }

    const controller = new AbortController();
    setComposeLoading(true);
    setComposeLoadError(null);

    const loadCompose = async () => {
      try {
        const response = await fetch(`/api/manifests/${composeWorkspaceId}`, { signal: controller.signal });
        const payload = await response.json().catch(() => null);
        if (!response.ok || !payload) {
          throw new Error(payload?.error || `Failed to load ${composeWorkspaceId}`);
        }
        const bundle = normalizePraxisBundle(payload, {
          id: composeWorkspaceId,
          title: typeof payload?.name === 'string' ? payload.name : composeTitle,
          description: typeof payload?.description === 'string' ? payload.description : composeWorkspace?.description ?? undefined,
        });
        if (!findComposeSurface(bundle)) {
          throw new Error('Compose workspace is missing its compose surface');
        }
        setComposeBundle(bundle);
      } catch (loadError) {
        if ((loadError as Error)?.name === 'AbortError') return;
        setComposeBundle(null);
        setComposeLoadError(loadError instanceof Error ? loadError.message : 'Compose workspace could not load');
      } finally {
        setComposeLoading(false);
      }
    };

    void loadCompose();
    return () => controller.abort();
  }, [composeTitle, composeWorkspace?.description, composeWorkspaceId]);

  const persistComposeBundle = useCallback(async (nextBundle: PraxisSurfaceBundleV4): Promise<PraxisSurfaceBundleV4> => {
    if (!composeWorkspaceId) {
      throw new Error('Compose workspace is not loaded');
    }
    const response = await fetch('/api/manifests/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: composeWorkspaceId,
        name: nextBundle.name ?? nextBundle.title,
        description: nextBundle.description ?? composeWorkspace?.description ?? '',
        manifest: nextBundle,
      }),
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      throw new Error(payload?.error || payload?.detail || 'Compose workspace save failed');
    }
    const savedBundle = normalizePraxisBundle(payload?.manifest ?? nextBundle, {
      id: composeWorkspaceId,
      title: typeof payload?.name === 'string' ? payload.name : nextBundle.title,
      description: typeof payload?.description === 'string' ? payload.description : nextBundle.description,
    });
    setComposeBundle(savedBundle);
    return savedBundle;
  }, [composeWorkspace?.description, composeWorkspaceId]);

  return (
    <div className="manifest-catalog">
      <section className="manifest-catalog__compose-workbench">
        {composeLoading ? (
          <div className="manifest-catalog__empty">Loading Compose...</div>
        ) : composeLoadError ? (
          <div className="manifest-catalog__empty manifest-catalog__empty--error">{composeLoadError}</div>
        ) : composeBundle && composeSurface && composeWorkspaceId ? (
          <WorkspaceComposeSurface
            manifestId={composeWorkspaceId}
            bundle={composeBundle}
            surface={composeSurface}
            workspaceTitle={composeTitle}
            onSaveBundle={persistComposeBundle}
          />
        ) : (
          <div className="manifest-catalog__empty">Compose workspace not loaded.</div>
        )}
      </section>

      <details className="manifest-catalog__advanced">
        <summary>Advanced workspace records</summary>
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
              placeholder="Any family"
            />
          </label>
          <label className="manifest-catalog__field">
            <span>Type</span>
            <input
              type="text"
              value={manifestType}
              onChange={(event) => setManifestType(event.target.value)}
              placeholder="Any type"
            />
          </label>
          <label className="manifest-catalog__field">
            <span>Status</span>
            <input
              type="text"
              value={status}
              onChange={(event) => setStatus(event.target.value)}
              placeholder="Any status"
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
              setManifestFamily('');
              setManifestType('');
              setStatus('');
              setLimit(25);
            }}
          >
            Reset filters
          </button>
        </section>

        <div className="manifest-catalog__summary">
          {loading ? 'Loading workspaces...' : error ? error : activeFilterSummary || 'Showing recent workspace records'}
        </div>

        <section className="manifest-catalog__results">
          {loading ? (
            <div className="manifest-catalog__empty">Loading workspaces...</div>
          ) : error ? (
            <div className="manifest-catalog__empty manifest-catalog__empty--error">{error}</div>
          ) : manifests.length === 0 ? (
            <div className="manifest-catalog__empty">No workspaces matched the current filters.</div>
          ) : (
            manifests.map((manifest) => (
              <article key={manifest.id} className="manifest-catalog__card">
                <div className="manifest-catalog__card-header">
                  <div>
                    {isComposeAuthoringRow(manifest) ? (
                      <div className="manifest-catalog__card-title">{displayWorkspaceName(manifest)}</div>
                    ) : (
                      <EditableCatalogTitle
                        manifest={manifest}
                        onSaved={(saved) => {
                          setManifests((current) => current.map((row) => (row.id === saved.id ? saved : row)));
                        }}
                        onError={(message) => setError(message)}
                      />
                    )}
                    <div className="manifest-catalog__card-id">
                      {isComposeWorkspaceRow(manifest)
                        ? 'authoring workspace'
                        : isComposeSeedRow(manifest)
                          ? 'workspace template'
                          : manifest.id}
                    </div>
                  </div>
                  <div className="manifest-catalog__card-actions">
                    <button type="button" onClick={() => onOpenManifest(manifest.id)}>
                      {isComposeWorkspaceRow(manifest) ? 'Open Compose' : 'Open'}
                    </button>
                    <button type="button" onClick={() => onEditManifest(manifest.id)}>
                      Advanced JSON
                    </button>
                  </div>
                </div>
                <p className="manifest-catalog__card-copy">
                  {displayWorkspaceDescription(manifest)}
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
      </details>
    </div>
  );
}
