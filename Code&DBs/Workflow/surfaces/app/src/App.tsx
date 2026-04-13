import React, { useCallback, useEffect, useMemo, useState } from 'react';
import type { HistoryMode } from './dashboard/operatingModelSurfaceState';
import { APP_CONFIG } from './config';
import type { PraxisOpenTabDetail } from './praxis/events';
import { ManifestBundleView } from './praxis/ManifestBundleView';
import { getSeedBundle, seedBundles } from './praxis/seedBundles';
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
  type StaticTabId,
} from './shell/state';
import { isLauncherRoute } from './shell/routes';

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
        <div style={{
          minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 32, background: 'var(--bg)', color: 'var(--text)',
        }}>
          <div style={{
            width: 'min(560px, 100%)', display: 'flex', flexDirection: 'column', gap: 16,
            padding: 32, background: 'var(--bg-card)', border: '1px solid var(--border)',
            borderRadius: 10, boxShadow: 'var(--shadow-modal)',
          }}>
            <div style={{ fontSize: 20, fontWeight: 700 }}>Something broke.</div>
            <details style={{ padding: 16, background: 'var(--bg-alt, var(--bg))', border: '1px solid var(--border)', borderRadius: 8 }}>
              <summary style={{ cursor: 'pointer', fontWeight: 600 }}>Error details</summary>
              <pre style={{ margin: '8px 0 0', whiteSpace: 'pre-wrap', fontFamily: 'var(--font-mono)', color: 'var(--danger)' }}>
                {this.state.error.message}
              </pre>
            </details>
            <button type="button" onClick={() => window.location.reload()} style={{
              alignSelf: 'flex-start', padding: '10px 16px', borderRadius: 8,
              border: 'none', background: 'var(--accent)', color: '#000', cursor: 'pointer', fontWeight: 600,
            }}>
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
    default: () => (
      <div style={{ minHeight: '100vh', display: 'grid', placeItems: 'center', background: 'var(--bg)', color: 'var(--text-muted)' }}>
        Overview unavailable.
      </div>
    )
  }))
);


const MoonBuildPage = React.lazy(() =>
  import('./moon/MoonBuildPage').then(m => ({ default: m.MoonBuildPage })).catch(() => ({
    default: () => <div style={{ padding: 32, color: 'var(--text-muted)' }}>Moon Build loading...</div>
  }))
);

const RunDetailView = React.lazy(() =>
  import('./dashboard/RunDetailView').then(m => ({ default: m.RunDetailView })).catch(() => ({
    default: () => <div style={{ padding: 32, color: 'var(--text-muted)' }}>Run detail view loading...</div>
  }))
);

const ChatPanel = React.lazy(() =>
  import('./dashboard/ChatPanel').then(m => ({ default: m.ChatPanel })).catch(() => ({
    default: (_props: { open: boolean; onClose: () => void }) => <></>
  }))
);

const ManifestEditorPage = React.lazy(() =>
  import('./grid/ManifestEditorPage').then(m => ({ default: m.ManifestEditorPage })).catch(() => ({
    default: () => <div style={{ padding: 32, color: 'var(--text-muted)' }}>Manifest editor loading...</div>
  }))
);

function initialShellPayload(): ShellHistoryPayload {
  return parseShellHistoryPayload(window.history.state) ?? parseShellLocationState(window.location.search);
}

function baseTabLabel(tabId: StaticTabId): string {
  if (tabId === 'build') return 'Build';
  return APP_CONFIG.name;
}

function tabButtonStyle(active: boolean): React.CSSProperties {
  return {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 8,
    padding: '8px 12px',
    borderRadius: 10,
    border: active ? '1px solid var(--accent)' : '1px solid var(--border)',
    background: active ? 'rgba(88,166,255,0.12)' : 'var(--bg-card)',
    color: 'var(--text)',
    cursor: 'pointer',
    fontSize: 13,
    fontWeight: 600,
    flexShrink: 0,
  };
}

function AppShell() {
  const initialPayload = initialShellPayload();
  const [state, setState] = useState<ShellState>(initialPayload.shellState);
  const [chatOpen, setChatOpen] = useState(initialPayload.chatOpen);
  const [seedMenuOpen, setSeedMenuOpen] = useState(false);
  const [creatingSeedId, setCreatingSeedId] = useState<string | null>(null);

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
    syncHistory(state, chatOpen, 'replace');
  }, [chatOpen, state, syncHistory]);

  useEffect(() => {
    const onPopState = (event: PopStateEvent) => {
      const payload = parseShellHistoryPayload(event.state) ?? parseShellLocationState(window.location.search);
      setState(payload.shellState);
      setChatOpen(payload.chatOpen);
    };
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, []);

  const activateTab = useCallback((tabId: string, historyMode: HistoryMode = 'push') => {
    setState((current) => {
      const nextState = { ...current, activeTabId: tabId };
      // Default to moon view when switching to build tab
      if (tabId === 'build' && !current.buildWorkflowId) {
        nextState.buildView = 'moon';
      }
      syncHistory(nextState, chatOpen, historyMode);
      return nextState;
    });
  }, [chatOpen, syncHistory]);

  const openBuild = useCallback((opts?: {
    workflowId?: string | null;
    intent?: string | null;
    seed?: unknown;
    view?: BuildView;
  }, historyMode: HistoryMode = 'push') => {
    setState((current) => {
      const nextState: ShellState = {
        ...current,
        activeTabId: 'build',
        buildWorkflowId: opts?.workflowId ?? null,
        buildIntent: opts?.intent ?? null,
        builderSeed: opts?.seed ?? null,
        buildView: opts?.view ?? current.buildView,
      };
      syncHistory(nextState, chatOpen, historyMode);
      return nextState;
    });
  }, [chatOpen, syncHistory]);

  const openEditModel = useCallback((workflowId: string | null, historyMode: HistoryMode = 'push') => {
    openBuild({ workflowId, intent: null, seed: null, view: 'moon' }, historyMode);
  }, [openBuild]);

  const openRunDetail = useCallback((runId: string, historyMode: HistoryMode = 'push') => {
    const nextTab: DynamicTab = {
      id: runDetailShellId(runId),
      kind: 'run-detail',
      label: `Run ${runId}`,
      closable: true,
      runId,
    };
    setState((current) => {
      const nextState: ShellState = {
        ...current,
        activeTabId: nextTab.id,
        dynamicTabs: upsertDynamicTab(current.dynamicTabs, nextTab),
      };
      syncHistory(nextState, chatOpen, historyMode);
      return nextState;
    });
  }, [chatOpen, syncHistory]);

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
    setState((current) => {
      const nextState: ShellState = {
        ...current,
        activeTabId: nextTab.id,
        dynamicTabs: upsertDynamicTab(current.dynamicTabs, nextTab),
      };
      syncHistory(nextState, chatOpen, historyMode);
      return nextState;
    });
  }, [chatOpen, syncHistory]);

  const openManifestEditor = useCallback((manifestId: string, historyMode: HistoryMode = 'push') => {
    const nextTab: DynamicTab = {
      id: manifestEditorShellId(manifestId),
      kind: 'manifest-editor',
      label: `Edit ${manifestId}`,
      closable: true,
      manifestId,
    };
    setState((current) => {
      const nextState: ShellState = {
        ...current,
        activeTabId: nextTab.id,
        dynamicTabs: upsertDynamicTab(current.dynamicTabs, nextTab),
      };
      syncHistory(nextState, chatOpen, historyMode);
      return nextState;
    });
  }, [chatOpen, syncHistory]);

  const closeTab = useCallback((tabId: string, historyMode: HistoryMode = 'push') => {
    setState((current) => {
      const resolved = closeDynamicTab(current.dynamicTabs, current.activeTabId, tabId);
      const nextState: ShellState = {
        ...current,
        dynamicTabs: resolved.dynamicTabs,
        activeTabId: resolved.activeTabId,
      };
      syncHistory(nextState, chatOpen, historyMode);
      return nextState;
    });
  }, [chatOpen, syncHistory]);

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
        if (key === 'k') {
          event.preventDefault();
          setChatOpen((open) => !open);
        }
        if (key === 'n') {
          event.preventDefault();
          openBuild({ workflowId: null, intent: null, seed: null, view: 'moon' });
        }
      }
      if (event.key === 'Escape') {
        if (chatOpen) {
          setChatOpen(false);
          return;
        }
        activateTab('dashboard');
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [activateTab, chatOpen, openBuild]);

  const activeDynamicTab = useMemo(
    () => state.dynamicTabs.find((tab) => tab.id === state.activeTabId) || null,
    [state.activeTabId, state.dynamicTabs],
  );

  const createSeedTab = useCallback(async (seedId: string) => {
    const seed = seedBundles.find((candidate) => candidate.id === seedId);
    const bundle = getSeedBundle(seedId);
    if (!seed || !bundle) return;
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
      setSeedMenuOpen(false);
    } catch (error) {
      console.error(error);
    } finally {
      setCreatingSeedId(null);
    }
  }, [openManifest]);

  const renderActiveTab = () => {
    if (state.activeTabId === 'dashboard') {
      return (
        <Dashboard
          onEditWorkflow={(id: string) => openBuild({ workflowId: id, intent: null, seed: null, view: 'moon' })}
          onEditModel={(id: string) => openEditModel(id)}
          onViewRun={(runId: string) => openRunDetail(runId)}
          onNewWorkflow={() => openBuild({ workflowId: null, intent: null, seed: null, view: 'moon' })}
          onChat={() => setChatOpen(true)}
          onDescribe={() => openBuild({ workflowId: null, intent: '__compose__', seed: null, view: 'moon' })}
        />
      );
    }

    if (state.activeTabId === 'build') {
      return (
        <MoonBuildPage
          workflowId={state.buildWorkflowId}
          onBack={() => activateTab('dashboard')}
          onWorkflowCreated={(wfId) => openBuild({ workflowId: wfId, intent: null, seed: null, view: 'moon' })}
          onViewRun={(runId) => openRunDetail(runId)}
          initialMode={state.buildIntent === '__compose__' ? 'compose' : undefined}
        />
      );
    }

    if (activeDynamicTab?.kind === 'run-detail' && activeDynamicTab.runId) {
      return (
        <RunDetailView
          runId={activeDynamicTab.runId}
          onBack={() => activateTab('dashboard')}
        />
      );
    }

    if (activeDynamicTab?.kind === 'manifest' && activeDynamicTab.manifestId) {
      return (
        <ManifestBundleView
          manifestId={activeDynamicTab.manifestId}
          tabId={activeDynamicTab.manifestTabId}
        />
      );
    }

    if (activeDynamicTab?.kind === 'manifest-editor' && activeDynamicTab.manifestId) {
      return <ManifestEditorPage manifestId={activeDynamicTab.manifestId} />;
    }

    return <div style={{ padding: 32, color: 'var(--text-muted)' }}>Select a tab to continue.</div>;
  };

  const tabs = [
    { id: 'dashboard', label: baseTabLabel('dashboard'), closable: false },
    { id: 'build', label: baseTabLabel('build'), closable: false },
    ...state.dynamicTabs.map((tab) => ({ id: tab.id, label: tab.label, closable: tab.closable })),
  ];

  return (
    <React.Suspense fallback={<div style={{ background: 'var(--bg)', minHeight: '100vh' }} />}>
      <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column', background: 'var(--bg)' }}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          padding: '10px 16px',
          borderBottom: '1px solid var(--border)',
          background: 'var(--bg)',
          position: 'sticky',
          top: 0,
          zIndex: 20,
        }}>
          <div style={{ display: 'flex', gap: 8, overflowX: 'auto', flex: 1, minWidth: 0, paddingBottom: 2 }}>
            {tabs.map((tab) => {
              const active = tab.id === state.activeTabId;
              return (
                <button
                  key={tab.id}
                  type="button"
                  onClick={() => activateTab(tab.id)}
                  style={tabButtonStyle(active)}
                >
                  <span>{tab.label}</span>
                  {tab.closable && (
                    <span
                      role="button"
                      aria-label={`Close ${tab.label}`}
                      onClick={(event) => {
                        event.stopPropagation();
                        closeTab(tab.id);
                      }}
                      style={{ color: 'var(--text-muted)', cursor: 'pointer', lineHeight: 1 }}
                    >
                      ×
                    </span>
                  )}
                </button>
              );
            })}
          </div>
          <div style={{ position: 'relative', flexShrink: 0 }}>
            <button
              type="button"
              onClick={() => setSeedMenuOpen((open) => !open)}
              style={{
                padding: '8px 12px',
                borderRadius: 10,
                border: seedMenuOpen ? '1px solid var(--accent)' : '1px solid var(--border)',
                background: seedMenuOpen ? 'rgba(88,166,255,0.12)' : 'var(--bg-card)',
                color: 'var(--text)',
                cursor: 'pointer',
                fontSize: 13,
                fontWeight: 600,
              }}
            >
              + Tab
            </button>
            {seedMenuOpen && (
              <div style={{
                position: 'absolute',
                top: 'calc(100% + 8px)',
                right: 0,
                width: 300,
                padding: 8,
                borderRadius: 12,
                border: '1px solid var(--border)',
                background: 'var(--bg-card)',
                boxShadow: 'var(--shadow-modal)',
                display: 'flex',
                flexDirection: 'column',
                gap: 8,
              }}>
                {seedBundles.map((seed) => (
                  <button
                    key={seed.id}
                    type="button"
                    onClick={() => void createSeedTab(seed.id)}
                    disabled={creatingSeedId !== null}
                    style={{
                      display: 'flex',
                      flexDirection: 'column',
                      alignItems: 'flex-start',
                      gap: 4,
                      padding: '10px 12px',
                      borderRadius: 10,
                      border: '1px solid var(--border)',
                      background: 'var(--bg)',
                      color: 'var(--text)',
                      cursor: creatingSeedId !== null ? 'not-allowed' : 'pointer',
                    }}
                  >
                    <span style={{ fontSize: 13, fontWeight: 700 }}>
                      {creatingSeedId === seed.id ? `Creating ${seed.label}...` : seed.label}
                    </span>
                    <span style={{ fontSize: 12, color: 'var(--text-muted)', textAlign: 'left' }}>
                      {seed.description}
                    </span>
                  </button>
                ))}
                <button
                  type="button"
                  onClick={() => {
                    setSeedMenuOpen(false);
                    openBuild({ workflowId: null, intent: null, seed: null, view: 'moon' });
                  }}
                  style={{
                    padding: '10px 12px',
                    borderRadius: 10,
                    border: '1px solid var(--border)',
                    background: 'var(--bg)',
                    color: 'var(--text)',
                    cursor: 'pointer',
                    fontSize: 13,
                    fontWeight: 600,
                    textAlign: 'left',
                  }}
                >
                  Open Builder
                </button>
              </div>
            )}
          </div>
          <button
            type="button"
            onClick={() => setChatOpen((open) => !open)}
            style={{
              padding: '8px 12px',
              borderRadius: 10,
              border: chatOpen ? '1px solid var(--accent)' : '1px solid var(--border)',
              background: chatOpen ? 'rgba(88,166,255,0.12)' : 'var(--bg-card)',
              color: 'var(--text)',
              cursor: 'pointer',
              fontSize: 13,
              fontWeight: 600,
              flexShrink: 0,
            }}
          >
            Chat
          </button>
        </div>
        <div style={{ flex: 1, minHeight: 0 }}>
          {renderActiveTab()}
        </div>
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
