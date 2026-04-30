import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { APP_CONFIG } from './config';
import { useSeedBundles } from './hooks/useSeedBundles';
import { LauncherFrontdoor } from './launcher/LauncherFrontdoor';
import {
  type BuildView,
  composeShellId,
  createDefaultShellState,
  manifestEditorShellId,
  manifestTabShellId,
  runDetailShellId,
  upsertDynamicTab,
  type DynamicTab,
  type ShellState,
} from './shell/state';
import { isLauncherRoute } from './shell/routes';
import {
  buildPath,
  buildPathForSurface,
  interpolateLabel,
  matchPath,
  resolveComponent,
  type RouteRegistryRow,
} from './shell/routeRegistry';
import { useShellState } from './shell/useShellState';
import { MenuPanel, type MenuSection } from './menu';
import { StrategyConsole, type StrategyStage } from './dashboard/StrategyConsole';
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

function initialStrategyStageFromLocation(): StrategyStage {
  if (typeof window === 'undefined') return 'icon';
  const params = new URLSearchParams(window.location.search);
  const requested = (params.get('chat') || params.get('console') || params.get('assistant') || '').trim().toLowerCase();
  if (requested === 'icon' || requested === 'closed' || requested === 'off' || requested === '0') return 'icon';
  if (requested === 'focus' || requested === 'full') return 'full';
  if (requested === 'sidebar' || requested === 'dock' || requested === 'docked' || requested === 'open' || requested === '1') {
    return 'sidebar';
  }
  if ((window.location.pathname === '/app' || window.location.pathname === '/app/') && window.innerWidth >= 900) return 'sidebar';
  return 'icon';
}

interface BuildDraftGuardState {
  dirty: boolean;
  message: string | null;
}

interface ShellTransitionOptions {
  bypassBuildDraftGuard?: boolean;
}

export function AppShell() {
  const { state, routes, sessionAggregateRef, ready, dispatch } = useShellState();
  const [strategyStage, setStrategyStage] = useState<StrategyStage>(() => initialStrategyStageFromLocation());
  const [commandMenuOpen, setCommandMenuOpen] = useState(false);
  const [buildDraftGuard, setBuildDraftGuard] = useState<BuildDraftGuardState>({ dirty: false, message: null });
  const commandButtonRef = useRef<HTMLButtonElement | null>(null);
  const stateRef = useRef(state);
  const buildDraftGuardRef = useRef(buildDraftGuard);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);
  useEffect(() => {
    buildDraftGuardRef.current = buildDraftGuard;
  }, [buildDraftGuard]);

  const shouldBlockBuildDraftExit = useCallback(
    (
      currentState: ShellState,
      nextState: Partial<ShellState>,
      options?: ShellTransitionOptions,
    ): boolean => {
      if (options?.bypassBuildDraftGuard) return false;
      const draft = buildDraftGuardRef.current;
      if (!draft.dirty || currentState.activeTabId !== 'build') return false;

      const stayingOnSameBuilder =
        nextState.activeTabId === 'build'
        && (nextState.buildWorkflowId === undefined || nextState.buildWorkflowId === currentState.buildWorkflowId)
        && (nextState.buildIntent === undefined || nextState.buildIntent === currentState.buildIntent)
        && (nextState.builderSeed === undefined || nextState.builderSeed === currentState.builderSeed)
        && (nextState.buildView === undefined || nextState.buildView === currentState.buildView);
      if (stayingOnSameBuilder) return false;

      const confirmed = window.confirm(
        draft.message || 'This draft workflow is not saved yet. Leave anyway?',
      );

      void dispatch('shell.draft.guard.consulted', {
        decision: confirmed ? 'leave' : 'stay',
        source_route_id: currentState.activeRouteId,
        target_route_id: typeof nextState.activeRouteId === 'string'
          ? nextState.activeRouteId
          : currentState.activeRouteId,
        draft_message: draft.message || '',
        caller_ref: 'shell.app_shell.draft_guard',
      });

      return !confirmed;
    },
    [dispatch],
  );

  const handleBuildDraftStateChange = useCallback((draft: { dirty: boolean; message?: string | null }) => {
    const nextMessage = draft.message ?? null;
    setBuildDraftGuard((current) =>
      current.dirty === draft.dirty && current.message === nextMessage
        ? current
        : { dirty: draft.dirty, message: nextMessage },
    );
  }, []);

  const openSurface = useCallback(
    async (
      routeId: string,
      args: {
        slotValues?: Record<string, string | string[]>;
        diff?: Partial<ShellState>;
        reason?: 'click' | 'keyboard' | 'event_bus' | 'history_pop' | 'deep_link';
        callerRef?: string;
        bypassBuildDraftGuard?: boolean;
      } = {},
    ) => {
      const current = stateRef.current;
      const diff: Partial<ShellState> = {
        ...args.diff,
        activeRouteId: routeId,
      };
      if (shouldBlockBuildDraftExit(current, diff, { bypassBuildDraftGuard: args.bypassBuildDraftGuard })) {
        return;
      }
      await dispatch(
        'shell.surface.opened',
        {
          route_id: routeId,
          slot_values: args.slotValues || {},
          shell_state_diff: diff,
          reason: args.reason || 'click',
          caller_ref: args.callerRef || 'shell.app_shell',
        },
        diff,
      );
    },
    [dispatch, shouldBlockBuildDraftExit],
  );

  const activateStaticSurface = useCallback(
    async (
      surfaceName: 'dashboard' | 'build' | 'manifests' | 'atlas',
      args: { bypassBuildDraftGuard?: boolean } = {},
    ) => {
      const row = routes.find((r) => r.surface_name === surfaceName && r.is_canonical_for_surface);
      const routeId = row?.route_id || `route.app.${surfaceName}`;
      await openSurface(routeId, {
        diff: {
          activeTabId: surfaceName,
          dashboardDetail: null,
          moonRunId: null,
        },
        callerRef: `shell.tab_strip.${surfaceName}`,
        bypassBuildDraftGuard: args.bypassBuildDraftGuard,
      });
    },
    [openSurface, routes],
  );

  const openDashboardCosts = useCallback(async () => {
    await openSurface('route.app.dashboard', {
      diff: { activeTabId: 'dashboard', dashboardDetail: 'costs', moonRunId: null },
      callerRef: 'shell.dashboard.cost_drill_in',
    });
  }, [openSurface]);

  const openBuild = useCallback(
    async (opts: {
      workflowId?: string | null;
      intent?: string | null;
      seed?: unknown;
      view?: BuildView;
      bypassBuildDraftGuard?: boolean;
    } = {}) => {
      const slotValues: Record<string, string | string[]> = {};
      if (opts.workflowId) slotValues.workflow = opts.workflowId;
      if (opts.intent) slotValues.intent = opts.intent;
      await openSurface('route.app.workflow', {
        slotValues,
        diff: {
          activeTabId: 'build',
          buildWorkflowId: opts.workflowId ?? null,
          buildIntent: opts.intent ?? null,
          builderSeed: opts.seed ?? null,
          buildView: opts.view ?? stateRef.current.buildView,
          moonRunId: null,
          dashboardDetail: null,
        },
        callerRef: 'shell.app_shell.open_build',
        bypassBuildDraftGuard: opts.bypassBuildDraftGuard,
      });
    },
    [openSurface],
  );

  const openRunDetail = useCallback(
    async (runId: string) => {
      await openSurface('route.app.run', {
        slotValues: { run_id: runId },
        diff: {
          activeTabId: 'build',
          buildView: 'moon',
          moonRunId: runId,
          dashboardDetail: null,
        },
        callerRef: 'shell.app_shell.open_run_detail',
      });
    },
    [openSurface],
  );

  const openManifest = useCallback(
    async (manifestId: string, manifestTabId?: string | null) => {
      const normalizedTabId = manifestTabId || 'main';
      const dynamicId = manifestTabShellId(manifestId, normalizedTabId);
      const nextTab: DynamicTab = {
        id: dynamicId,
        kind: 'manifest',
        label: normalizedTabId === 'main' ? manifestId : `${manifestId} · ${normalizedTabId}`,
        closable: true,
        manifestId,
        manifestTabId: normalizedTabId,
      };
      await openSurface('route.app.manifest', {
        slotValues: { manifest_id: manifestId, manifest_tab_id: normalizedTabId },
        diff: {
          activeTabId: dynamicId,
          dynamicTabs: upsertDynamicTab(stateRef.current.dynamicTabs, nextTab),
          dashboardDetail: null,
        },
        callerRef: 'shell.app_shell.open_manifest',
      });
    },
    [openSurface],
  );

  const openManifestEditor = useCallback(
    async (manifestId: string) => {
      const dynamicId = manifestEditorShellId(manifestId);
      const nextTab: DynamicTab = {
        id: dynamicId,
        kind: 'manifest-editor',
        label: `Edit ${manifestId}`,
        closable: true,
        manifestId,
      };
      await openSurface('route.app.manifest_editor', {
        slotValues: { manifest_id: manifestId },
        diff: {
          activeTabId: dynamicId,
          dynamicTabs: upsertDynamicTab(stateRef.current.dynamicTabs, nextTab),
          dashboardDetail: null,
        },
        callerRef: 'shell.app_shell.open_manifest_editor',
      });
    },
    [openSurface],
  );

  const openCompose = useCallback(
    async (intent: string, pillRefs: readonly string[] = []) => {
      const dynamicId = composeShellId(intent, pillRefs);
      const labelPills = pillRefs.length > 0
        ? ` · ${pillRefs.length} pill${pillRefs.length === 1 ? '' : 's'}`
        : '';
      const nextTab: DynamicTab = {
        id: dynamicId,
        kind: 'compose',
        label: `Compose ${intent}${labelPills}`,
        closable: true,
        intent,
        pillRefs: [...pillRefs],
      };
      await openSurface('route.app.compose', {
        slotValues: { intent, pill_refs: [...pillRefs] },
        diff: {
          activeTabId: dynamicId,
          dynamicTabs: upsertDynamicTab(stateRef.current.dynamicTabs, nextTab),
          dashboardDetail: null,
        },
        callerRef: 'shell.app_shell.open_compose',
      });
    },
    [openSurface],
  );

  const closeTab = useCallback(
    async (tabId: string) => {
      const current = stateRef.current;
      const remaining = current.dynamicTabs.filter((tab) => tab.id !== tabId);
      const fallbackTabId =
        current.activeTabId === tabId
          ? remaining[remaining.length - 1]?.id || 'dashboard'
          : current.activeTabId;
      const fallbackRouteId =
        fallbackTabId === 'dashboard' ? 'route.app.dashboard' : current.activeRouteId;

      await dispatch(
        'shell.tab.closed',
        {
          dynamic_tab_id: tabId,
          fallback_route_id: fallbackRouteId,
          caller_ref: 'shell.tab_strip.close_button',
        },
        {
          dynamicTabs: remaining,
          activeTabId: fallbackTabId,
          activeRouteId: fallbackRouteId,
        },
      );
    },
    [dispatch],
  );

  useEffect(() => {
    if (!ready) return undefined;
    const onPopState = () => {
      const match = matchPath(window.location.pathname, window.location.search);
      const targetRouteId = match?.route_id || 'route.app.dashboard';
      const slotValues = match?.slot_values || {};
      void openSurface(targetRouteId, {
        slotValues,
        reason: 'history_pop',
        callerRef: 'shell.history.popstate',
        bypassBuildDraftGuard: true,
      });
    };
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, [openSurface, ready]);

  useEffect(() => {
    if (!(state.activeTabId === 'build' && buildDraftGuard.dirty)) return undefined;
    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => window.removeEventListener('beforeunload', handleBeforeUnload);
  }, [buildDraftGuard.dirty, state.activeTabId]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const key = event.key.toLowerCase();
      if (event.metaKey || event.ctrlKey) {
        if (key === 'k') {
          event.preventDefault();
          setStrategyStage(s => s === 'icon' ? 'sidebar' : 'icon');
          setCommandMenuOpen(false);
          return;
        }
        if (key === 'k' && event.shiftKey) {
          event.preventDefault();
          setCommandMenuOpen((open) => !open);
          return;
        }
      }
      if (event.key === 'Escape') {
        if (commandMenuOpen) {
          setCommandMenuOpen(false);
          return;
        }
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [commandMenuOpen]);

  const activeRow = useMemo(
    () => routes.find((r) => r.route_id === state.activeRouteId) || null,
    [routes, state.activeRouteId],
  );

  const isBuildMode = state.activeTabId === 'build';

  const { seeds: seedBundles } = useSeedBundles();

  const createSeedTab = useCallback(
    async (seedId: string) => {
      const seed = seedBundles.find((candidate) => candidate.id === seedId);
      if (!seed) return;
      const bundle = JSON.parse(JSON.stringify(seed.bundle));
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
        await openManifest(payload.id, 'main');
        setCommandMenuOpen(false);
      } catch (error) {
        console.error(error);
      }
    },
    [openManifest, seedBundles],
  );

  const tabStripRows = useMemo(
    () =>
      [...routes]
        .filter((r) => r.tab_strip_position !== null && r.status === 'ready')
        .sort((a, b) => (a.tab_strip_position ?? 0) - (b.tab_strip_position ?? 0)),
    [routes],
  );

  const commandMenuSections = useMemo<MenuSection[]>(() => {
    const createItems = [
      {
        id: 'create:builder',
        label: 'New Workflow',
        description: 'Describe what you want first, then refine the generated graph.',
        keywords: ['builder', 'workflow', 'moon', 'new', 'blank', 'describe', 'compose'],
        shortcut: 'Ctrl+N',
        onSelect: () => {
          void openBuild({ workflowId: null, intent: '__compose__', seed: null, view: 'moon' });
        },
      },
      ...seedBundles.map((seed) => ({
        id: `seed:${seed.id}`,
        label: seed.label,
        description: seed.description,
        keywords: ['seed', 'starter', 'surface', seed.id, seed.label],
        keepOpen: true,
        onSelect: () => {
          void createSeedTab(seed.id);
        },
      })),
    ];

    const navigateItems = tabStripRows.map((row) => ({
      id: `navigate:${row.route_id}`,
      label: interpolateLabel(row.tab_label_template, state) || row.surface_name,
      description: interpolateLabel(row.nav_description_template, state) || '',
      keywords: row.nav_keywords || [],
      selected: state.activeRouteId === row.route_id,
      onSelect: () => {
        void activateStaticSurface(row.surface_name as 'dashboard' | 'build' | 'manifests' | 'atlas');
      },
    }));

    return [
      { id: 'create', title: 'Create', items: createItems },
      { id: 'navigate', title: 'Navigate', items: navigateItems },
    ];
  }, [activateStaticSurface, createSeedTab, openBuild, seedBundles, state, tabStripRows]);

  const renderActiveTab = () => {
    if (!ready || !activeRow) {
      return <SurfacePlaceholder title="Loading workspace..." />;
    }
    const effectiveRouteId =
      activeRow.route_id === 'route.app.dashboard' && state.dashboardDetail === 'costs'
        ? 'route.app.dashboard_costs'
        : activeRow.route_id;
    const Component = resolveComponent(effectiveRouteId);
    if (!Component) {
      return <SurfaceFallback title="Surface unavailable" copy={`No component bound for ${effectiveRouteId}.`} />;
    }
    const props = renderPropsForRoute(effectiveRouteId, state, {
      activateStaticSurface,
      openBuild,
      openRunDetail,
      openManifest,
      openManifestEditor,
      openCompose,
      openDashboardCosts,
      setChatOpen: () => setStrategyStage('sidebar'),
      openMaterializeChat: () => setStrategyStage('sidebar'),
      handleBuildDraftStateChange,
    });
    return <Component {...props} />;
  };

  return (
    <React.Suspense fallback={<SurfacePlaceholder title="Loading workspace..." />}>
      <div className={`app-shell${isBuildMode ? ' app-shell--build-mode' : ''}`}>
        <header className={`app-shell__chrome${isBuildMode ? ' app-shell__chrome--collapsed' : ''}`}>
          <div className="app-shell__identity">
            <div className="app-shell__identity-mark" aria-hidden="true" />
            <div className="app-shell__identity-copy">
              <span>{APP_CONFIG.suiteName} · Moon</span>
              <strong>{APP_CONFIG.name}</strong>
            </div>
          </div>

          <div className="app-shell__nav">
            <div className="app-shell__tabstrip" role="tablist" aria-label="Praxis views">
              {tabStripRows.map((row) => {
                const surface = row.surface_name as 'dashboard' | 'build' | 'manifests' | 'atlas';
                const active = state.activeTabId === surface;
                return (
                  <div
                    key={row.route_id}
                    className={`app-shell__tab ${active ? 'app-shell__tab--active' : ''}`}
                  >
                    <button
                      type="button"
                      role="tab"
                      aria-selected={active}
                      onClick={() => {
                        void activateStaticSurface(surface);
                      }}
                      className="app-shell__tab-button"
                    >
                      <span className="app-shell__tab-glyph" aria-hidden="true">
                        {(row.tab_kind_label || row.surface_name).slice(0, 1)}
                      </span>
                      <span className="app-shell__tab-copy">
                        <span className="app-shell__tab-kind">{row.tab_kind_label || row.surface_name}</span>
                        <span className="app-shell__tab-label">{interpolateLabel(row.tab_label_template, state)}</span>
                      </span>
                    </button>
                  </div>
                );
              })}
              {state.dynamicTabs.map((tab) => {
                const active = state.activeTabId === tab.id;
                return (
                  <div
                    key={tab.id}
                    className={`app-shell__tab ${active ? 'app-shell__tab--active' : ''}`}
                  >
                    <button
                      type="button"
                      role="tab"
                      aria-selected={active}
                      onClick={() => {
                        void openSurface(routeIdForDynamicTab(tab), {
                          diff: { activeTabId: tab.id },
                          callerRef: 'shell.tab_strip.dynamic',
                        });
                      }}
                      className="app-shell__tab-button"
                    >
                      <span className="app-shell__tab-glyph" aria-hidden="true">{tab.kind.slice(0, 1)}</span>
                      <span className="app-shell__tab-copy">
                        <span className="app-shell__tab-kind">{tab.kind}</span>
                        <span className="app-shell__tab-label">{tab.label}</span>
                      </span>
                    </button>
                    <button
                      type="button"
                      aria-label={`Close ${tab.label}`}
                      onClick={() => {
                        void closeTab(tab.id);
                      }}
                      className="app-shell__tab-close"
                    >
                      <span className="app-shell__tab-close-icon" aria-hidden="true" />
                    </button>
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
              onClick={() => setStrategyStage(s => s === 'icon' ? 'sidebar' : 'icon')}
              className={`app-shell__action-button ${strategyStage !== 'icon' ? 'app-shell__action-button--active' : ''}`}
            >
              <span className="app-shell__action-icon app-shell__action-icon--chat" aria-hidden="true" />
              <span className="app-shell__action-copy">
                <span className="app-shell__action-kicker">Assistant</span>
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
        <div className="app-shell__content">
          <main
            className={[
              'app-shell__main',
              strategyStage === 'sidebar' ? 'app-shell__main--with-console' : '',
              strategyStage === 'full' ? 'app-shell__main--hidden' : '',
            ].filter(Boolean).join(' ')}
          >
            {renderActiveTab()}
          </main>
          {strategyStage !== 'icon' && (
            <StrategyConsole stage={strategyStage} onStageChange={setStrategyStage} />
          )}
        </div>
      </div>
    </React.Suspense>
  );
}

function routeIdForDynamicTab(tab: DynamicTab): string {
  switch (tab.kind) {
    case 'manifest': return 'route.app.manifest';
    case 'manifest-editor': return 'route.app.manifest_editor';
    case 'compose': return 'route.app.compose';
    case 'run-detail': return 'route.app.run_detail_legacy';
    default: return 'route.app.dashboard';
  }
}

interface RenderPropHelpers {
  activateStaticSurface: (surface: 'dashboard' | 'build' | 'manifests' | 'atlas') => Promise<void>;
  openBuild: (opts?: { workflowId?: string | null; intent?: string | null; seed?: unknown; view?: BuildView; bypassBuildDraftGuard?: boolean }) => Promise<void>;
  openRunDetail: (runId: string) => Promise<void>;
  openManifest: (manifestId: string, manifestTabId?: string | null) => Promise<void>;
  openManifestEditor: (manifestId: string) => Promise<void>;
  openCompose: (intent: string, pillRefs?: readonly string[]) => Promise<void>;
  openDashboardCosts: () => Promise<void>;
  setChatOpen: () => void;
  openMaterializeChat: () => void;
  handleBuildDraftStateChange: (draft: { dirty: boolean; message?: string | null }) => void;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function renderPropsForRoute(routeId: string, state: ShellState, helpers: RenderPropHelpers): any {
  const onBack = () => helpers.activateStaticSurface('dashboard');
  switch (routeId) {
    case 'route.app.dashboard':
      return {
        onEditWorkflow: (id: string) => helpers.openBuild({ workflowId: id, intent: null, seed: null, view: 'moon' }),
        onEditModel: (id: string) => helpers.openBuild({ workflowId: id, intent: null, seed: null, view: 'moon' }),
        onViewRun: (runId: string) => helpers.openRunDetail(runId),
        onNewWorkflow: () => helpers.openBuild({ workflowId: null, intent: null, seed: null, view: 'moon' }),
        onChat: () => helpers.setChatOpen(),
        onDescribe: () => helpers.openBuild({ workflowId: null, intent: '__compose__', seed: null, view: 'moon' }),
        onOpenCosts: () => helpers.openDashboardCosts(),
      };
    case 'route.app.dashboard_costs':
      return {
        onBack,
        onViewRun: (runId: string) => helpers.openRunDetail(runId),
      };
    case 'route.app.workflow':
    case 'route.app.build.legacy':
    case 'route.app.run':
      return {
        workflowId: state.buildWorkflowId,
        runId: state.moonRunId,
        onBack,
        onWorkflowCreated: (wfId: string) =>
          helpers.openBuild({ workflowId: wfId, intent: null, seed: null, view: 'moon', bypassBuildDraftGuard: true }),
        onEditWorkflow: (wfId: string) => helpers.openBuild({ workflowId: wfId, intent: null, seed: null, view: 'moon' }),
        onViewRun: (runId: string) => helpers.openRunDetail(runId),
        onDraftStateChange: helpers.handleBuildDraftStateChange,
        onMaterializeHandoff: helpers.openMaterializeChat,
        initialMode:
          state.buildIntent === '__compose__' || (!state.buildWorkflowId && !state.moonRunId)
            ? 'compose'
            : undefined,
      };
    case 'route.app.manifests':
      return {
        onOpenManifest: (manifestId: string) => helpers.openManifest(manifestId),
        onEditManifest: (manifestId: string) => helpers.openManifestEditor(manifestId),
      };
    case 'route.app.atlas':
      return {};
    case 'route.app.run_detail_legacy': {
      const tab = state.dynamicTabs.find((t) => t.id === state.activeTabId);
      return {
        runId: tab?.runId,
        onBack,
      };
    }
    case 'route.app.manifest': {
      const tab = state.dynamicTabs.find((t) => t.id === state.activeTabId);
      return {
        manifestId: tab?.manifestId,
        tabId: tab?.manifestTabId,
      };
    }
    case 'route.app.manifest_editor': {
      const tab = state.dynamicTabs.find((t) => t.id === state.activeTabId);
      return { manifestId: tab?.manifestId };
    }
    case 'route.app.compose': {
      const tab = state.dynamicTabs.find((t) => t.id === state.activeTabId);
      return {
        intent: tab?.intent,
        pillRefs: tab?.pillRefs,
      };
    }
    default:
      return {};
  }
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

// Re-exports retained for any callers reaching directly into App for type inference.
export type { ShellState } from './shell/state';
export { buildPath, buildPathForSurface, matchPath };
export { createDefaultShellState };
