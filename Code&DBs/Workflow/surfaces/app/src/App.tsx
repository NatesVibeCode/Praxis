import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { HistoryMode } from './dashboard/operatingModelSurfaceState';
import { APP_CONFIG } from './config';
import type { PraxisOpenTabDetail } from './praxis/events';
import { ManifestBundleView } from './praxis/ManifestBundleView';
import { useSeedBundles } from './hooks/useSeedBundles';
import { LauncherFrontdoor } from './launcher/LauncherFrontdoor';
import {
  type BuildView,
  buildShellUrl,
  closeDynamicTab,
  createDefaultShellState,
  manifestEditorShellId,
  manifestTabShellId,
  parseShellHistoryPayload,
  parseShellLocationState,
  runDetailShellId,
  upsertDynamicTab,
  type DynamicTab,
  type ShellHistoryPayload,
  type ShellState,
} from './shell/state';
import { isLauncherRoute } from './shell/routes';
import {
  buildShellNavigationItems,
  buildShellTabs,
  resolveActiveShellSurface,
} from './shell/surfaceRegistry';
import { MenuPanel, type MenuSection } from './menu';
import './styles/app-shell.css';

class AppErrorBoundary extends React.Component<React.PropsWithChildren, { error: Error | null }> {
  state = { error: null as Error | null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('App surface crashed:', error, errorInfo);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="app-shell__crash">
          <div className="app-shell__crash-card">
            <div className="app-shell__crash-kicker">Surface error</div>
            <div className="app-shell__crash-title">Something broke.</div>
            <p className="app-shell__crash-copy">
              The app shell caught a runtime error before it could take the rest of the UI down.
            </p>
            <details className="app-shell__crash-details">
              <summary>Inspect error details</summary>
              <pre>{this.state.error.stack || this.state.error.message}</pre>
            </details>
            <button type="button" onClick={() => window.location.reload()} className="app-shell__crash-action">
              Reload
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

const Dashboard = React.lazy(() =>
  import('./dashboard/Dashboard').then(m => ({ default: m.Dashboard })).catch(() => ({
    default: () => <SurfaceFallback title="Overview unavailable." copy="The dashboard failed to load." />
  }))
);

const CostsPanel = React.lazy(() =>
  import('./dashboard/CostsPanel').then(m => ({ default: m.CostsPanel })).catch(() => ({
    default: () => <SurfaceFallback title="Cost summary unavailable." copy="The cost surface failed to load." />
  }))
);

const ManifestCatalogPage = React.lazy(() =>
  import('./praxis/ManifestCatalogPage').then(m => ({ default: m.ManifestCatalogPage })).catch(() => ({
    default: () => <SurfacePlaceholder title="Manifest catalog loading..." />
  }))
);


const MoonBuildPage = React.lazy(() =>
  import('./moon/MoonBuildPage').then(m => ({ default: m.MoonBuildPage })).catch(() => ({
    default: () => <SurfacePlaceholder title="Moon Build loading..." />
  }))
);

const RunDetailView = React.lazy(() =>
  import('./dashboard/RunDetailView').then(m => ({ default: m.RunDetailView })).catch(() => ({
    default: () => <SurfacePlaceholder title="Run detail view loading..." />
  }))
);

const ChatPanel = React.lazy(() =>
  import('./dashboard/ChatPanel').then(m => ({ default: m.ChatPanel })).catch(() => ({
    default: (_props: { open: boolean; onClose: () => void }) => <></>
  }))
);

const ManifestEditorPage = React.lazy(() =>
  import('./grid/ManifestEditorPage').then(m => ({ default: m.ManifestEditorPage })).catch(() => ({
    default: () => <SurfacePlaceholder title="Manifest editor loading..." />
  }))
);

const SurfaceComposeView = React.lazy(() =>
  import('./praxis/SurfaceComposeView').then(m => ({ default: m.SurfaceComposeView })).catch(() => ({
    default: () => <SurfaceFallback title="Compose surface unavailable." copy="The compose view failed to load." />
  }))
);

const AtlasPage = React.lazy(() =>
  import('./atlas/AtlasPage').then(m => ({ default: m.AtlasPage })).catch(() => ({
    default: () => <SurfaceFallback title="Atlas unavailable." copy="The atlas surface failed to load." />
  }))
);

function SurfacePlaceholder({ title }: { title: string }) {
  return (
    <div className="app-shell__fallback">
      <div className="app-shell__fallback-kicker">Loading</div>
      <div className="app-shell__fallback-title">{title}</div>
    </div>
  );
}

function SurfaceFallback({ title, copy }: { title: string; copy: string }) {
  return (
    <div className="app-shell__fallback app-shell__fallback--error">
      <div className="app-shell__fallback-kicker">Unavailable</div>
      <div className="app-shell__fallback-title">{title}</div>
      <p className="app-shell__fallback-copy">{copy}</p>
    </div>
  );
}

function initialShellPayload(): ShellHistoryPayload {
  return parseShellHistoryPayload(window.history.state)
    ?? parseShellLocationState(window.location.search, window.location.pathname);
}

interface BuildDraftGuardState {
  dirty: boolean;
  message: string | null;
}

interface ShellTransitionOptions {
  bypassBuildDraftGuard?: boolean;
}

export function AppShell() {
  const initialPayload = initialShellPayload();
  const [state, setState] = useState<ShellState>(initialPayload.shellState);
  const [chatOpen, setChatOpen] = useState(initialPayload.chatOpen);
  const [commandMenuOpen, setCommandMenuOpen] = useState(false);
  const [creatingSeedId, setCreatingSeedId] = useState<string | null>(null);
  const [buildDraftGuard, setBuildDraftGuard] = useState<BuildDraftGuardState>({ dirty: false, message: null });
  const commandButtonRef = useRef<HTMLButtonElement | null>(null);
  const stateRef = useRef(state);
  const chatOpenRef = useRef(chatOpen);
  const buildDraftGuardRef = useRef(buildDraftGuard);

  const syncHistory = useCallback((nextState: ShellState, nextChatOpen: boolean, historyMode: HistoryMode) => {
    const payload: ShellHistoryPayload = { shellState: nextState, chatOpen: nextChatOpen };
    const url = buildShellUrl(nextState, nextChatOpen);
    if (historyMode === 'replace') {
      window.history.replaceState(payload, '', url);
      return;
    }
    window.history.pushState(payload, '', url);
  }, []);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  useEffect(() => {
    chatOpenRef.current = chatOpen;
  }, [chatOpen]);

  useEffect(() => {
    buildDraftGuardRef.current = buildDraftGuard;
  }, [buildDraftGuard]);

  const commitShellState = useCallback((nextState: ShellState, historyMode: HistoryMode) => {
    syncHistory(nextState, chatOpenRef.current, historyMode);
    setState(nextState);
  }, [syncHistory]);

  const restoreCurrentHistoryEntry = useCallback(() => {
    const currentState = stateRef.current;
    const currentChatOpen = chatOpenRef.current;
    const payload: ShellHistoryPayload = { shellState: currentState, chatOpen: currentChatOpen };
    const url = buildShellUrl(currentState, currentChatOpen);
    window.history.pushState(payload, '', url);
  }, []);

  const shouldBlockBuildDraftExit = useCallback((
    currentState: ShellState,
    nextState: ShellState,
    options?: ShellTransitionOptions,
  ): boolean => {
    if (options?.bypassBuildDraftGuard) return false;
    const draft = buildDraftGuardRef.current;
    if (!draft.dirty || currentState.activeTabId !== 'build') return false;

    const stayingOnSameBuilder =
      nextState.activeTabId === 'build'
      && nextState.buildWorkflowId === currentState.buildWorkflowId
      && nextState.buildIntent === currentState.buildIntent
      && nextState.builderSeed === currentState.builderSeed
      && nextState.buildView === currentState.buildView;

    if (stayingOnSameBuilder) return false;

    return !window.confirm(
      draft.message || 'This draft workflow is not saved yet. Leave anyway?',
    );
  }, []);

  const handleBuildDraftStateChange = useCallback((draft: { dirty: boolean; message?: string | null }) => {
    const nextMessage = draft.message ?? null;
    setBuildDraftGuard((current) => (
      current.dirty === draft.dirty && current.message === nextMessage
        ? current
        : { dirty: draft.dirty, message: nextMessage }
    ));
  }, []);

  useEffect(() => {
    syncHistory(state, chatOpen, 'replace');
  }, [chatOpen, state, syncHistory]);

  useEffect(() => {
    const onPopState = (event: PopStateEvent) => {
      const payload = parseShellHistoryPayload(event.state)
        ?? parseShellLocationState(window.location.search, window.location.pathname);
      const currentState = stateRef.current;
      if (shouldBlockBuildDraftExit(currentState, payload.shellState)) {
        restoreCurrentHistoryEntry();
        return;
      }
      setState(payload.shellState);
      setChatOpen(payload.chatOpen);
    };
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, [restoreCurrentHistoryEntry, shouldBlockBuildDraftExit]);

  useEffect(() => {
    if (!(state.activeTabId === 'build' && buildDraftGuard.dirty)) return;
    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => window.removeEventListener('beforeunload', handleBeforeUnload);
  }, [buildDraftGuard.dirty, state.activeTabId]);

  const activateTab = useCallback((tabId: string, historyMode: HistoryMode = 'push', options?: ShellTransitionOptions) => {
    const current = stateRef.current;
    // Clearing moonRunId on any tab switch: the run view is URL-addressable
    // via /app/run/:id; leaving the surface exits run mode.
    const nextState: ShellState = {
      ...current,
      activeTabId: tabId,
      moonRunId: null,
      dashboardDetail: null,
    };
    if (tabId === 'build' && !current.buildWorkflowId) {
      nextState.buildView = 'moon';
    }
    if (shouldBlockBuildDraftExit(current, nextState, options)) return;
    commitShellState(nextState, historyMode);
  }, [commitShellState, shouldBlockBuildDraftExit]);

  const openDashboardCosts = useCallback(() => {
    const current = stateRef.current;
    const nextState: ShellState = {
      ...current,
      activeTabId: 'dashboard',
      dashboardDetail: 'costs',
      moonRunId: null,
    };
    commitShellState(nextState, 'push');
  }, [commitShellState]);

  const openBuild = useCallback((opts?: {
    workflowId?: string | null;
    intent?: string | null;
    seed?: unknown;
    view?: BuildView;
  }, historyMode: HistoryMode = 'push', options?: ShellTransitionOptions) => {
    const current = stateRef.current;
    const nextState: ShellState = {
      ...current,
      activeTabId: 'build',
      buildWorkflowId: opts?.workflowId ?? null,
      buildIntent: opts?.intent ?? null,
      builderSeed: opts?.seed ?? null,
      buildView: opts?.view ?? current.buildView,
      moonRunId: null,
      dashboardDetail: null,
    };
    if (shouldBlockBuildDraftExit(current, nextState, options)) return;
    commitShellState(nextState, historyMode);
  }, [commitShellState, shouldBlockBuildDraftExit]);

  const openEditModel = useCallback((workflowId: string | null, historyMode: HistoryMode = 'push') => {
    openBuild({ workflowId, intent: null, seed: null, view: 'moon' }, historyMode);
  }, [openBuild]);

  const openRunDetail = useCallback((runId: string, historyMode: HistoryMode = 'push') => {
    // Moon owns run rendering — route into the static build surface with
    // moonRunId set rather than creating a legacy run-detail dynamic tab.
    const current = stateRef.current;
    const nextState: ShellState = {
      ...current,
      activeTabId: 'build',
      buildView: 'moon',
      moonRunId: runId,
      dashboardDetail: null,
    };
    if (shouldBlockBuildDraftExit(current, nextState)) return;
    commitShellState(nextState, historyMode);
  }, [commitShellState, shouldBlockBuildDraftExit]);

  const openManifest = useCallback((manifestId: string, manifestTabId?: string | null, historyMode: HistoryMode = 'push') => {
    const normalizedTabId = manifestTabId || 'main';
    const nextTab: DynamicTab = {
      id: manifestTabShellId(manifestId, normalizedTabId),
      kind: 'manifest',
      label: normalizedTabId === 'main' ? manifestId : `${manifestId} · ${normalizedTabId}`,
      closable: true,
      manifestId,
      manifestTabId: normalizedTabId,
    };
    const current = stateRef.current;
    const nextState: ShellState = {
      ...current,
      activeTabId: nextTab.id,
      dynamicTabs: upsertDynamicTab(current.dynamicTabs, nextTab),
      dashboardDetail: null,
    };
    if (shouldBlockBuildDraftExit(current, nextState)) return;
    commitShellState(nextState, historyMode);
  }, [commitShellState, shouldBlockBuildDraftExit]);

  const openManifestEditor = useCallback((manifestId: string, historyMode: HistoryMode = 'push') => {
    const nextTab: DynamicTab = {
      id: manifestEditorShellId(manifestId),
      kind: 'manifest-editor',
      label: `Edit ${manifestId}`,
      closable: true,
      manifestId,
    };
    const current = stateRef.current;
    const nextState: ShellState = {
      ...current,
      activeTabId: nextTab.id,
      dynamicTabs: upsertDynamicTab(current.dynamicTabs, nextTab),
      dashboardDetail: null,
    };
    if (shouldBlockBuildDraftExit(current, nextState)) return;
    commitShellState(nextState, historyMode);
  }, [commitShellState, shouldBlockBuildDraftExit]);

  const closeTab = useCallback((tabId: string, historyMode: HistoryMode = 'push') => {
    const current = stateRef.current;
    const resolved = closeDynamicTab(current.dynamicTabs, current.activeTabId, tabId);
    const nextState: ShellState = {
      ...current,
      dynamicTabs: resolved.dynamicTabs,
      activeTabId: resolved.activeTabId,
      dashboardDetail: null,
    };
    if (shouldBlockBuildDraftExit(current, nextState)) return;
    commitShellState(nextState, historyMode);
  }, [commitShellState, shouldBlockBuildDraftExit]);

  useEffect(() => {
    const onOpenTab = (event: Event) => {
      const detail = (event as CustomEvent<PraxisOpenTabDetail>).detail;
      if (!detail) return;
      if (detail.kind === 'build') {
        openBuild({
          workflowId: detail.workflowId ?? null,
          intent: detail.intent ?? null,
          seed: null,
          view: 'moon',
        });
        return;
      }
      if (detail.kind === 'manifest' && detail.manifestId) {
        openManifest(detail.manifestId, detail.tabId ?? 'main');
        return;
      }
      if (detail.kind === 'manifest-editor' && detail.manifestId) {
        openManifestEditor(detail.manifestId);
        return;
      }
      if (detail.kind === 'run-detail' && detail.runId) {
        openRunDetail(detail.runId);
        return;
      }
      if (detail.kind === 'edit-model') {
        openEditModel(detail.workflowId ?? null);
      }
    };
    window.addEventListener('praxis-open-tab', onOpenTab as EventListener);
    window.addEventListener('helm-open-tab', onOpenTab as EventListener);
    return () => {
      window.removeEventListener('praxis-open-tab', onOpenTab as EventListener);
      window.removeEventListener('helm-open-tab', onOpenTab as EventListener);
    };
  }, [openBuild, openEditModel, openManifest, openManifestEditor, openRunDetail]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const key = event.key.toLowerCase();
      if (event.metaKey || event.ctrlKey) {
        if (key === 'k' && !event.shiftKey) {
          event.preventDefault();
          setChatOpen((open) => !open);
          setCommandMenuOpen(false);
        }
        if (key === 'k' && event.shiftKey) {
          event.preventDefault();
          setCommandMenuOpen((open) => !open);
        }
        if (key === 'n') {
          event.preventDefault();
          openBuild({ workflowId: null, intent: null, seed: null, view: 'moon' });
        }
      }
      if (event.key === 'Escape') {
        if (commandMenuOpen) {
          setCommandMenuOpen(false);
          return;
        }
        if (chatOpen) {
          setChatOpen(false);
          return;
        }
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [chatOpen, commandMenuOpen, openBuild]);

  const activeDynamicTab = useMemo(
    () => state.dynamicTabs.find((tab) => tab.id === state.activeTabId) || null,
    [state.activeTabId, state.dynamicTabs],
  );
  const activeSurface = useMemo(
    () => resolveActiveShellSurface(state, activeDynamicTab),
    [activeDynamicTab, state],
  );
  const activeContext = activeSurface.context;
  const isBuildMode = state.activeTabId === 'build';

  const { seeds: seedBundles } = useSeedBundles();

  const createSeedTab = useCallback(async (seedId: string) => {
    const seed = seedBundles.find((candidate) => candidate.id === seedId);
    if (!seed) return;
    // Deep-clone so the user's new tab doesn't mutate the in-memory seed.
    const bundle = JSON.parse(JSON.stringify(seed.bundle));
    setCreatingSeedId(seedId);
    try {
      const response = await fetch('/api/manifests/save-as', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: seed.label,
          description: seed.description,
          manifest: bundle,
        }),
      });
      const payload = await response.json().catch(() => null);
      if (!response.ok || !payload?.id) {
        throw new Error(payload?.error || `Failed to create ${seed.label}`);
      }
      openManifest(payload.id, 'main');
      setCommandMenuOpen(false);
    } catch (error) {
      console.error(error);
    } finally {
      setCreatingSeedId(null);
    }
  }, [openManifest, seedBundles]);

  const commandMenuSections = useMemo<MenuSection[]>(() => {
    const createItems = [
      {
        id: 'create:builder',
        label: 'New Workflow',
        description: 'Describe what you want first, then refine the generated graph.',
        keywords: ['builder', 'workflow', 'moon', 'new', 'blank', 'describe', 'compose'],
        shortcut: 'Ctrl+N',
        onSelect: () => openBuild({ workflowId: null, intent: '__compose__', seed: null, view: 'moon' }),
      },
      ...seedBundles.map((seed) => ({
        id: `seed:${seed.id}`,
        label: seed.label,
        description: seed.description,
        keywords: ['seed', 'starter', 'surface', seed.id, seed.label],
        meta: creatingSeedId === seed.id ? 'Creating…' : 'Surface',
        disabled: creatingSeedId !== null,
        keepOpen: true,
        onSelect: () => {
          void createSeedTab(seed.id);
        },
      })),
    ];

    const navigateItems = buildShellNavigationItems({
      state,
      chatOpen,
      activateTab,
      setChatOpen,
    });

    return [
      { id: 'create', title: 'Create', items: createItems },
      { id: 'navigate', title: 'Navigate', items: navigateItems },
    ];
  }, [activateTab, chatOpen, createSeedTab, creatingSeedId, openBuild, openDashboardCosts, seedBundles, setChatOpen, state]);

  const renderActiveTab = () => {
    if (activeSurface.category === 'static' && activeSurface.id === 'dashboard' && state.dashboardDetail === 'costs') {
      return (
        <CostsPanel
          onBack={() => activateTab('dashboard')}
          onViewRun={(runId: string) => openRunDetail(runId)}
        />
      );
    }

    if (activeSurface.category === 'static' && activeSurface.id === 'dashboard') {
      return (
        <Dashboard
          onEditWorkflow={(id: string) => openBuild({ workflowId: id, intent: null, seed: null, view: 'moon' })}
          onEditModel={(id: string) => openEditModel(id)}
          onViewRun={(runId: string) => openRunDetail(runId)}
          onNewWorkflow={() => openBuild({ workflowId: null, intent: '__compose__', seed: null, view: 'moon' })}
          onChat={() => setChatOpen(true)}
          onDescribe={() => openBuild({ workflowId: null, intent: '__compose__', seed: null, view: 'moon' })}
          onOpenCosts={openDashboardCosts}
        />
      );
    }

    if (activeSurface.category === 'static' && activeSurface.id === 'build') {
      return (
        <MoonBuildPage
          workflowId={state.buildWorkflowId}
          runId={state.moonRunId}
          onBack={() => activateTab('dashboard')}
          onWorkflowCreated={(wfId) => openBuild(
            { workflowId: wfId, intent: null, seed: null, view: 'moon' },
            'push',
            { bypassBuildDraftGuard: true },
          )}
          onEditWorkflow={(wfId) => openBuild({ workflowId: wfId, intent: null, seed: null, view: 'moon' })}
          onViewRun={(runId) => openRunDetail(runId)}
          onDraftStateChange={handleBuildDraftStateChange}
          initialMode={state.buildIntent === '__compose__' || (!state.buildWorkflowId && !state.moonRunId) ? 'compose' : undefined}
        />
      );
    }

    if (activeSurface.category === 'static' && activeSurface.id === 'manifests') {
      return (
        <ManifestCatalogPage
          onOpenManifest={(manifestId) => openManifest(manifestId)}
          onEditManifest={(manifestId) => openManifestEditor(manifestId)}
        />
      );
    }

    if (activeSurface.category === 'static' && activeSurface.id === 'atlas') {
      return <AtlasPage />;
    }

    if (activeSurface.category === 'dynamic' && activeSurface.kind === 'run-detail' && activeSurface.dynamicTab.runId) {
      return (
        <RunDetailView
          runId={activeSurface.dynamicTab.runId}
          onBack={() => activateTab('dashboard')}
        />
      );
    }

    if (activeSurface.category === 'dynamic' && activeSurface.kind === 'manifest' && activeSurface.dynamicTab.manifestId) {
      return (
        <ManifestBundleView
          manifestId={activeSurface.dynamicTab.manifestId}
          tabId={activeSurface.dynamicTab.manifestTabId}
        />
      );
    }

    if (activeSurface.category === 'dynamic' && activeSurface.kind === 'manifest-editor' && activeSurface.dynamicTab.manifestId) {
      return <ManifestEditorPage manifestId={activeSurface.dynamicTab.manifestId} />;
    }

    if (activeSurface.category === 'dynamic' && activeSurface.kind === 'compose') {
      return (
        <SurfaceComposeView
          intent={activeSurface.dynamicTab.intent}
          pillRefs={activeSurface.dynamicTab.pillRefs}
        />
      );
    }

    return <SurfaceFallback title="No tab selected" copy="Select a tab to continue." />;
  };

  const tabs = useMemo(() => buildShellTabs(state), [state]);

  return (
    <React.Suspense fallback={<SurfacePlaceholder title="Loading workspace..." />}>
      <div className={`app-shell${isBuildMode ? ' app-shell--build-mode' : ''}`}>
        <header className={`app-shell__chrome${isBuildMode ? ' app-shell__chrome--collapsed' : ''}`}>
          <div className="app-shell__identity">
            <div className="app-shell__identity-mark" aria-hidden="true" />
            <div className="app-shell__identity-copy">
              <span>{APP_CONFIG.suiteName}</span>
              <strong>{APP_CONFIG.name}</strong>
              <em>{activeContext.label}</em>
              <p>{activeContext.detail}</p>
            </div>
          </div>

          <div className="app-shell__nav">
            <div className="app-shell__tabstrip" role="tablist" aria-label="Praxis views">
              {tabs.map((tab) => {
                const active = tab.id === state.activeTabId;
                return (
                  <div
                    key={tab.id}
                    className={`app-shell__tab ${active ? 'app-shell__tab--active' : ''}`}
                  >
                    <button
                      type="button"
                      role="tab"
                      aria-selected={active}
                      onClick={() => activateTab(tab.id)}
                      className="app-shell__tab-button"
                    >
                      <span className="app-shell__tab-glyph" aria-hidden="true">{tab.kind.slice(0, 1)}</span>
                      <span className="app-shell__tab-copy">
                        <span className="app-shell__tab-kind">{tab.kind}</span>
                        <span className="app-shell__tab-label">{tab.label}</span>
                      </span>
                    </button>
                    {tab.closable && (
                      <button
                        type="button"
                        aria-label={`Close ${tab.label}`}
                        onClick={() => closeTab(tab.id)}
                        className="app-shell__tab-close"
                      >
                        <span className="app-shell__tab-close-icon" aria-hidden="true" />
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
          </div>

          <div className="app-shell__actions">
            <div className="app-shell__menu">
              <button
                ref={commandButtonRef}
                type="button"
                onClick={() => setCommandMenuOpen((open) => !open)}
                className={`app-shell__action-button ${commandMenuOpen ? 'app-shell__action-button--active' : ''}`}
                aria-haspopup="dialog"
                aria-expanded={commandMenuOpen}
              >
                <span className="app-shell__action-icon app-shell__action-icon--new" aria-hidden="true" />
                <span className="app-shell__action-copy">
                  <span className="app-shell__action-kicker">Workspace</span>
                  <span>New</span>
                </span>
              </button>
            </div>
            <button
              type="button"
              onClick={() => setChatOpen((open) => !open)}
              className={`app-shell__action-button ${chatOpen ? 'app-shell__action-button--active' : ''}`}
            >
              <span className="app-shell__action-icon app-shell__action-icon--chat" aria-hidden="true" />
              <span className="app-shell__action-copy">
                <span className="app-shell__action-kicker">Cmd/Ctrl + K</span>
                <span>Chat</span>
              </span>
            </button>
          </div>
        </header>
        <MenuPanel
          open={commandMenuOpen}
          anchorRect={commandButtonRef.current?.getBoundingClientRect() ?? null}
          title="Open or Create"
          subtitle="Search tabs, surfaces, and shell actions."
          searchPlaceholder="Search tabs, surfaces, and actions…"
          sections={commandMenuSections}
          onClose={() => setCommandMenuOpen(false)}
        />
        <main className="app-shell__content">
          {renderActiveTab()}
        </main>
      </div>
      <ChatPanel open={chatOpen} onClose={() => setChatOpen(false)} />
    </React.Suspense>
  );
}

export function App() {
  const [showLauncher, setShowLauncher] = useState(() => isLauncherRoute());

  useEffect(() => {
    const handlePopState = () => {
      setShowLauncher(isLauncherRoute());
    };
    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  return (
    <AppErrorBoundary>
      {showLauncher ? <LauncherFrontdoor /> : <AppShell />}
    </AppErrorBoundary>
  );
}
