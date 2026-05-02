import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { QuadrantGrid } from '../grid/QuadrantGrid';
import { Button } from '../primitives';
import { emitPraxisOpenTab } from './events';
import { normalizePraxisBundle, resolvePraxisBundleTab, type PraxisSurfaceBundleV4, type PraxisTabDefinition, type SourceOption } from './manifest';
import { SourceOptionPills } from './SourceOptionPills';
import { WorkspaceComposeSurface } from './WorkspaceComposeSurface';
import { WorkspaceReceiptsTab, type WorkspaceRunRow } from './WorkspaceReceiptsTab';

interface ManifestBundleViewProps {
  manifestId: string;
  tabId?: string | null;
}

interface SourceOptionPayload {
  source_options?: SourceOption[];
}

function isDefaultBlankWorkspaceTitle(value: string | null | undefined, manifestId: string): boolean {
  const normalized = (value || '').trim().toLowerCase();
  return normalized === 'blank workspace' || normalized === manifestId.toLowerCase();
}

function displayWorkspaceTitle(bundle: PraxisSurfaceBundleV4, manifestId: string, isComposeSurface: boolean): string {
  if (isComposeSurface && isDefaultBlankWorkspaceTitle(bundle.title, manifestId)) {
    return 'Compose';
  }
  return bundle.title;
}

function displayWorkspaceDescription(bundle: PraxisSurfaceBundleV4, isComposeSurface: boolean): string {
  const description = (bundle.description || '').trim();
  if (isComposeSurface && (!description || description.toLowerCase().includes('minimal workspace'))) {
    return 'Compose intent into a contract, then dispatch the work and inspect the receipts.';
  }
  return description;
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
  const [workspaceRuns, setWorkspaceRuns] = useState<WorkspaceRunRow[]>([]);
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
    setWorkspaceRuns([]);

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
        setRenameError(null);

        const selectedTab = resolvePraxisBundleTab(nextBundle, tabId);
        const selectedSurface = nextBundle.surfaces[selectedTab.surface_id] ?? null;
        setDraftTitle(displayWorkspaceTitle(nextBundle, manifestId, selectedSurface?.kind === 'compose'));
        const params = new URLSearchParams({ manifest_id: manifestId, tab_id: selectedTab.id });
        const sourceResponse = await fetch(`/api/source-options?${params.toString()}`);
        const sourcePayload = await sourceResponse.json().catch(() => null) as SourceOptionPayload | null;
        if (!sourceResponse.ok) {
          throw new Error(sourcePayload && 'error' in sourcePayload ? String((sourcePayload as Record<string, unknown>).error) : 'Failed to load source options');
        }
        if (cancelled) return;
        setSourceOptions(Array.isArray(sourcePayload?.source_options) ? sourcePayload?.source_options ?? [] : []);

        try {
          const runsResponse = await fetch(`/api/workspaces/${encodeURIComponent(manifestId)}/runs?limit=20`);
          const runsPayload = await runsResponse.json().catch(() => null);
          if (!cancelled && runsResponse.ok) {
            setWorkspaceRuns(Array.isArray(runsPayload?.items) ? runsPayload.items as WorkspaceRunRow[] : []);
          }
        } catch {
          if (!cancelled) setWorkspaceRuns([]);
        }
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

  const receiptsTab = useMemo<PraxisTabDefinition | null>(() => (
    workspaceRuns.length > 0
      ? { id: 'receipts', label: 'Receipts', surface_id: '__receipts', source_option_ids: [] }
      : null
  ), [workspaceRuns.length]);
  const visibleTabs = useMemo(() => {
    if (!bundle) return [];
    return receiptsTab ? [...bundle.tabs, receiptsTab] : bundle.tabs;
  }, [bundle, receiptsTab]);
  const selectedTab = useMemo(() => {
    if (!bundle) return null;
    if (tabId === 'receipts' && receiptsTab) return receiptsTab;
    return resolvePraxisBundleTab(bundle, tabId);
  }, [bundle, receiptsTab, tabId]);
  const selectedSurface = useMemo(() => {
    if (!bundle || !selectedTab || selectedTab.id === 'receipts') return null;
    return bundle.surfaces[selectedTab.surface_id] ?? null;
  }, [bundle, selectedTab]);
  const layoutPath = useMemo(() => (
    selectedSurface
      ? `ui.manifest_layout.${encodeURIComponent(manifestId)}.${encodeURIComponent(selectedSurface.id)}.quadrants`
      : 'ui.layout.quadrants'
  ), [manifestId, selectedSurface]);
  const isReceiptsTab = selectedTab?.id === 'receipts';
  const isComposeSurface = selectedSurface?.kind === 'compose';
  const canCustomize = selectedSurface?.kind === 'quadrant_manifest';
  const workspaceTitle = useMemo(() => (
    bundle ? displayWorkspaceTitle(bundle, manifestId, isComposeSurface) : ''
  ), [bundle, isComposeSurface, manifestId]);
  const workspaceDescription = useMemo(() => (
    bundle ? displayWorkspaceDescription(bundle, isComposeSurface) : ''
  ), [bundle, isComposeSurface]);

  const persistBundle = useCallback(async (nextBundle: PraxisSurfaceBundleV4): Promise<PraxisSurfaceBundleV4> => {
    const response = await fetch('/api/manifests/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: manifestId,
        name: nextBundle.name ?? nextBundle.title,
        description: nextBundle.description ?? '',
        manifest: nextBundle,
      }),
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      throw new Error(payload?.error || payload?.detail || 'Workspace save failed');
    }
    const savedBundle = normalizePraxisBundle(payload?.manifest ?? nextBundle, {
      id: manifestId,
      title: typeof payload?.name === 'string' ? payload.name : nextBundle.title,
      description: typeof payload?.description === 'string' ? payload.description : nextBundle.description,
    });
    setBundle(savedBundle);
    const savedSelectedSurface = selectedSurface?.id
      ? savedBundle.surfaces[selectedSurface.id] ?? null
      : null;
    setDraftTitle(displayWorkspaceTitle(savedBundle, manifestId, savedSelectedSurface?.kind === 'compose'));
    return savedBundle;
  }, [manifestId, selectedSurface?.id]);

  const saveTitle = useCallback(async () => {
    if (!bundle) return;
    const nextTitle = draftTitle.trim();
    if (!nextTitle || nextTitle === workspaceTitle) {
      setDraftTitle(workspaceTitle);
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
      if (surface.kind === 'quadrant_manifest' && (surface.manifest.title === previousTitle || surface.id === selectedSurface?.id)) {
        surface.manifest.title = nextTitle;
      }
    }

    setRenaming(true);
    setRenameError(null);
    try {
      nextBundle.name = nextTitle;
      const savedBundle = await persistBundle(nextBundle);
      setBundle(savedBundle);
      setDraftTitle(displayWorkspaceTitle(savedBundle, manifestId, isComposeSurface));
    } catch (saveError) {
      setDraftTitle(workspaceTitle);
      setRenameError(saveError instanceof Error ? saveError.message : 'Rename failed');
    } finally {
      setRenaming(false);
    }
  }, [bundle, draftTitle, isComposeSurface, manifestId, persistBundle, selectedSurface?.id, workspaceTitle]);

  if (loading) {
    return (
      <div className="app-shell__fallback">
        <div className="app-shell__fallback-kicker">Manifest bundle</div>
        <div className="app-shell__fallback-title">Loading {manifestId}...</div>
      </div>
    );
  }

  if (error || !bundle || !selectedTab || (!isReceiptsTab && !selectedSurface)) {
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
    <div className={[
      'app-shell__surface',
      isComposeSurface ? 'app-shell__surface--workspace-compose' : '',
    ].filter(Boolean).join(' ')}>
      <div className={[
        'app-shell__surface-header',
        'app-shell__surface-header--workbench',
        isComposeSurface ? 'app-shell__surface-header--compose' : '',
      ].filter(Boolean).join(' ')}>
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
                  setDraftTitle(workspaceTitle);
                  setRenameError(null);
                  event.currentTarget.blur();
                }
              }}
              spellCheck={false}
            />
            {workspaceDescription ? (
              <div className="app-shell__surface-copy app-shell__surface-copy--inline">{workspaceDescription}</div>
            ) : null}
            {renaming ? (
              <div className="app-shell__surface-rename-status">Saving</div>
            ) : renameError ? (
              <div className="app-shell__surface-rename-status app-shell__surface-rename-status--error">
                {renameError}
              </div>
            ) : null}
          </div>
          {visibleTabs.length > 1 && (
            <div className="app-shell__surface-tabs app-shell__surface-tabs--inline prx-button-row">
              {visibleTabs.map((entry) => (
                <Button
                  key={entry.id}
                  size="sm"
                  tone={entry.id === selectedTab.id ? 'primary' : 'ghost'}
                  active={entry.id === selectedTab.id}
                  onClick={() => emitPraxisOpenTab({ kind: 'manifest', manifestId, tabId: entry.id })}
                >
                  {entry.label}
                </Button>
              ))}
            </div>
          )}
        </div>

        <div className="app-shell__surface-workbench-actions">
          {!isReceiptsTab && !isComposeSurface && sourceOptions.length > 0 && (
            <div className="app-shell__surface-source-group" aria-label="Sources">
              <span className="app-shell__surface-source-label">Sources</span>
              <SourceOptionPills options={sourceOptions} />
            </div>
          )}
          {canCustomize ? (
            <Button
              size="sm"
              tone={customizing ? 'primary' : 'ghost'}
              active={customizing}
              onClick={() => setCustomizing((value) => !value)}
              aria-pressed={customizing}
            >
              {customizing ? 'Done' : 'Customize'}
            </Button>
          ) : null}
        </div>
      </div>

      <div className="app-shell__surface-body">
        {isReceiptsTab ? (
          <WorkspaceReceiptsTab manifestId={manifestId} initialRuns={workspaceRuns} />
        ) : selectedSurface?.kind === 'compose' ? (
          <WorkspaceComposeSurface
            manifestId={manifestId}
            bundle={bundle}
            surface={selectedSurface}
            workspaceTitle={workspaceTitle}
            onSaveBundle={persistBundle}
          />
        ) : selectedSurface?.kind === 'quadrant_manifest' ? (
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
        ) : null}
      </div>
    </div>
  );
}
