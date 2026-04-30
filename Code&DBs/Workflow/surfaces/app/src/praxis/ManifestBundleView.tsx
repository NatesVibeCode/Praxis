import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { QuadrantGrid } from '../grid/QuadrantGrid';
import { emitPraxisOpenTab } from './events';
import { normalizePraxisBundle, resolvePraxisBundleSurface, resolvePraxisBundleTab, type PraxisSurfaceBundleV4, type SourceOption } from './manifest';
import { SourceOptionPills } from './SourceOptionPills';

interface ManifestBundleViewProps {
  manifestId: string;
  tabId?: string | null;
}

interface SourceOptionPayload {
  source_options?: SourceOption[];
}

function manifestLoadFailure(rawMessage: string | null): { title: string; copy: string } {
  const message = rawMessage?.trim() || 'The workspace could not be loaded.';
  const lowered = message.toLowerCase();
  if (
    lowered.includes('remaining connection slots') ||
    lowered.includes('too many connections') ||
    lowered.includes('connection slots are reserved')
  ) {
    return {
      title: 'Database connection limit hit',
      copy: 'Praxis has too many open database connections right now. The workspace is still there; wait for services to recover, then retry.',
    };
  }
  return {
    title: 'Workspace could not open',
    copy: message,
  };
}

export function ManifestBundleView({ manifestId, tabId }: ManifestBundleViewProps) {
  const [bundle, setBundle] = useState<PraxisSurfaceBundleV4 | null>(null);
  const [sourceOptions, setSourceOptions] = useState<SourceOption[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [loadAttempt, setLoadAttempt] = useState(0);
  const [customizing, setCustomizing] = useState(false);
  const [draftTitle, setDraftTitle] = useState('');
  const [renaming, setRenaming] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);
  const skipNextTitleBlurSave = useRef(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    const load = async () => {
      try {
        const manifestResponse = await fetch(`/api/manifests/${manifestId}`);
        const manifestPayload = await manifestResponse.json().catch(() => null);
        if (!manifestResponse.ok) {
          throw new Error(manifestPayload?.error || `Failed to load manifest ${manifestId}`);
        }
        if (cancelled) return;
        const nextBundle = normalizePraxisBundle(manifestPayload, {
          id: manifestId,
          title: typeof manifestPayload?.name === 'string' ? manifestPayload.name : undefined,
          description: typeof manifestPayload?.description === 'string' ? manifestPayload.description : undefined,
        });
        setBundle(nextBundle);
        setDraftTitle(nextBundle.title);
        setRenameError(null);

        const selectedTab = resolvePraxisBundleTab(nextBundle, tabId);
        const params = new URLSearchParams({ manifest_id: manifestId, tab_id: selectedTab.id });
        const sourceResponse = await fetch(`/api/source-options?${params.toString()}`);
        const sourcePayload = await sourceResponse.json().catch(() => null) as SourceOptionPayload | null;
        if (!sourceResponse.ok) {
          throw new Error(sourcePayload && 'error' in sourcePayload ? String((sourcePayload as Record<string, unknown>).error) : 'Failed to load source options');
        }
        if (cancelled) return;
        setSourceOptions(Array.isArray(sourcePayload?.source_options) ? sourcePayload?.source_options ?? [] : []);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [manifestId, tabId, loadAttempt]);

  const selectedTab = useMemo(() => (bundle ? resolvePraxisBundleTab(bundle, tabId) : null), [bundle, tabId]);
  const selectedSurface = useMemo(() => (bundle ? resolvePraxisBundleSurface(bundle, tabId) : null), [bundle, tabId]);
  const layoutPath = useMemo(() => (
    selectedSurface
      ? `ui.manifest_layout.${encodeURIComponent(manifestId)}.${encodeURIComponent(selectedSurface.id)}.quadrants`
      : 'ui.layout.quadrants'
  ), [manifestId, selectedSurface]);

  const saveTitle = useCallback(async () => {
    if (!bundle) return;
    const nextTitle = draftTitle.trim();
    if (!nextTitle || nextTitle === bundle.title) {
      setDraftTitle(bundle.title);
      setRenameError(null);
      return;
    }

    const previousTitle = bundle.title;
    const nextBundle = structuredClone(bundle) as PraxisSurfaceBundleV4;
    nextBundle.title = nextTitle;
    nextBundle.name = nextTitle;

    for (const surface of Object.values(nextBundle.surfaces)) {
      if (surface.title === previousTitle || surface.id === selectedSurface?.id) {
        surface.title = nextTitle;
      }
      if (surface.manifest.title === previousTitle || surface.id === selectedSurface?.id) {
        surface.manifest.title = nextTitle;
      }
    }

    setRenaming(true);
    setRenameError(null);
    try {
      const response = await fetch('/api/manifests/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: manifestId,
          name: nextTitle,
          description: nextBundle.description ?? '',
          manifest: nextBundle,
        }),
      });
      const payload = await response.json().catch(() => null);
      if (!response.ok) {
        throw new Error(payload?.error || 'Rename failed');
      }
      const savedBundle = normalizePraxisBundle(payload?.manifest ?? nextBundle, {
        id: manifestId,
        title: typeof payload?.name === 'string' ? payload.name : nextTitle,
        description: typeof payload?.description === 'string' ? payload.description : nextBundle.description,
      });
      setBundle(savedBundle);
      setDraftTitle(savedBundle.title);
    } catch (saveError) {
      setDraftTitle(bundle.title);
      setRenameError(saveError instanceof Error ? saveError.message : 'Rename failed');
    } finally {
      setRenaming(false);
    }
  }, [bundle, draftTitle, manifestId, selectedSurface?.id]);

  if (loading) {
    return (
      <div className="app-shell__fallback">
        <div className="app-shell__fallback-kicker">Manifest bundle</div>
        <div className="app-shell__fallback-title">Loading {manifestId}...</div>
      </div>
    );
  }

  if (error || !bundle || !selectedTab || !selectedSurface) {
    const failure = manifestLoadFailure(error);
    return (
      <div className="app-shell__fallback app-shell__fallback--error">
        <div className="app-shell__fallback-kicker">Manifest bundle</div>
        <div className="app-shell__fallback-title">{failure.title}</div>
        <p className="app-shell__fallback-copy">{failure.copy}</p>
        <div className="app-shell__fallback-actions">
          <button
            type="button"
            className="app-shell__crash-action"
            onClick={() => setLoadAttempt((attempt) => attempt + 1)}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="app-shell__surface">
      <div className="app-shell__surface-header app-shell__surface-header--workbench">
        <div className="app-shell__surface-heading app-shell__surface-heading--workbench">
          <div className="app-shell__surface-title-row">
            <input
              className="app-shell__surface-title app-shell__surface-title-input"
              aria-label="Rename workspace"
              value={draftTitle}
              disabled={renaming}
              onChange={(event) => {
                setDraftTitle(event.target.value);
                if (renameError) setRenameError(null);
              }}
              onFocus={(event) => event.currentTarget.select()}
              onBlur={() => {
                if (skipNextTitleBlurSave.current) {
                  skipNextTitleBlurSave.current = false;
                  return;
                }
                void saveTitle();
              }}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  event.currentTarget.blur();
                }
                if (event.key === 'Escape') {
                  skipNextTitleBlurSave.current = true;
                  setDraftTitle(bundle.title);
                  setRenameError(null);
                  event.currentTarget.blur();
                }
              }}
              spellCheck={false}
            />
            {bundle.description ? (
              <div className="app-shell__surface-copy app-shell__surface-copy--inline">{bundle.description}</div>
            ) : null}
            {renaming ? (
              <div className="app-shell__surface-rename-status">Saving</div>
            ) : renameError ? (
              <div className="app-shell__surface-rename-status app-shell__surface-rename-status--error">
                {renameError}
              </div>
            ) : null}
          </div>
          {bundle.tabs.length > 1 && (
            <div className="app-shell__surface-tabs app-shell__surface-tabs--inline">
              {bundle.tabs.map((entry) => (
                <button
                  key={entry.id}
                  type="button"
                  onClick={() => emitPraxisOpenTab({ kind: 'manifest', manifestId, tabId: entry.id })}
                  className={[
                    'app-shell__surface-tab',
                    entry.id === selectedTab.id ? 'app-shell__surface-tab--active' : '',
                  ].filter(Boolean).join(' ')}
                >
                  {entry.label}
                </button>
              ))}
            </div>
          )}
        </div>

        <div className="app-shell__surface-workbench-actions">
          {sourceOptions.length > 0 && (
            <div className="app-shell__surface-source-group" aria-label="Sources">
              <span className="app-shell__surface-source-label">Sources</span>
              <SourceOptionPills options={sourceOptions} />
            </div>
          )}
          <button
            type="button"
            onClick={() => setCustomizing((value) => !value)}
            className={[
              'app-shell__surface-action',
              'app-shell__surface-action--primary',
              customizing ? 'app-shell__surface-action--active' : '',
            ].filter(Boolean).join(' ')}
            aria-pressed={customizing}
          >
            {customizing ? 'Done' : 'Customize'}
          </button>
        </div>
      </div>

      <div className="app-shell__surface-body">
        <QuadrantGrid
          manifest={selectedSurface.manifest}
          editable={customizing}
          layoutPath={layoutPath}
          showHeaderTitle={false}
          saveTarget={{
            manifestId,
            name: bundle.name ?? bundle.title,
            description: bundle.description,
            bundle,
            surfaceId: selectedSurface.id,
          }}
        />
      </div>
    </div>
  );
}
