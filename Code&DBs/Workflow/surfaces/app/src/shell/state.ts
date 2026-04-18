import { parseEditorSurface } from '../dashboard/operatingModelSurfaceState';
import { shellUrl } from './routes';

export type StaticTabId = 'dashboard' | 'build' | 'costs' | 'manifests';
export type DynamicTabKind = 'run-detail' | 'manifest' | 'manifest-editor';
export type AppTabId = StaticTabId | string;
export type BuildView = 'moon';

export interface DynamicTab {
  id: string;
  kind: DynamicTabKind;
  label: string;
  closable: true;
  runId?: string | null;
  manifestId?: string | null;
  manifestTabId?: string | null;
}

export interface ShellState {
  activeTabId: AppTabId;
  dynamicTabs: DynamicTab[];
  buildWorkflowId: string | null;
  buildIntent: string | null;
  builderSeed: unknown | null;
  buildView: BuildView;
  /** When set, Moon renders a run-view over its canvas using this run_id. */
  moonRunId: string | null;
}

export interface ShellHistoryPayload {
  shellState: ShellState;
  chatOpen: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null;
}

function normalizeDynamicTab(value: unknown): DynamicTab | null {
  if (!isRecord(value)) return null;
  const kind = value.kind;
  if (
    kind !== 'run-detail'
    && kind !== 'manifest'
    && kind !== 'manifest-editor'
  ) {
    return null;
  }
  const id = asString(value.id);
  const label = asString(value.label);
  if (!id || !label) return null;
  return {
    id,
    kind,
    label,
    closable: true,
    runId: asString(value.runId),
    manifestId: asString(value.manifestId),
    manifestTabId: asString(value.manifestTabId),
  };
}

export function manifestTabShellId(manifestId: string, tabId?: string | null): string {
  return `manifest:${manifestId}:${tabId || 'main'}`;
}

export function manifestEditorShellId(manifestId: string): string {
  return `manifest-editor:${manifestId}`;
}

export function runDetailShellId(runId: string): string {
  return `run-detail:${runId}`;
}

export function createDefaultShellState(): ShellState {
  return {
    activeTabId: 'dashboard',
    dynamicTabs: [],
    buildWorkflowId: null,
    buildIntent: null,
    builderSeed: null,
    buildView: 'moon',
    moonRunId: null,
  };
}

export function parseShellHistoryPayload(value: unknown): ShellHistoryPayload | null {
  if (!isRecord(value) || !isRecord(value.shellState)) return null;
  const shellState = value.shellState;
  const dynamicTabs = Array.isArray(shellState.dynamicTabs)
    ? shellState.dynamicTabs.map(normalizeDynamicTab).filter((item): item is DynamicTab => item !== null)
    : [];
  return {
    shellState: {
      activeTabId: asString(shellState.activeTabId) || 'dashboard',
      dynamicTabs,
      buildWorkflowId: asString(shellState.buildWorkflowId),
      buildIntent: asString(shellState.buildIntent),
      builderSeed: (shellState.builderSeed as unknown | null) ?? null,
      buildView: 'moon' as const,
      moonRunId: asString(shellState.moonRunId),
    },
    chatOpen: Boolean(value.chatOpen),
  };
}

export function parseShellLocationState(search: string, pathname: string = window.location.pathname): ShellHistoryPayload {
  const params = new URLSearchParams(search);
  const shellState = createDefaultShellState();

  // Path-based routes: /app/run/{runId}, /app/build, /app/costs
  const appRelative = pathname.replace(/^\/app\/?/, '');
  const runMatch = appRelative.match(/^run\/(.+)/);
  if (runMatch) {
    const runId = runMatch[1];
    // Moon owns run rendering — route into Moon's canvas with moonRunId set.
    // The legacy dashboard run-detail surface is deprecated.
    return {
      shellState: {
        ...shellState,
        activeTabId: 'build',
        buildView: 'moon',
        moonRunId: runId,
      },
      chatOpen: false,
    };
  }
  if (appRelative === 'build' || appRelative.startsWith('build/')) {
    return {
      shellState: {
        ...shellState,
        activeTabId: 'build',
        buildWorkflowId: asString(params.get('workflow')),
        buildIntent: asString(params.get('intent')),
        buildView: 'moon',
      },
      chatOpen: false,
    };
  }

  if (appRelative === 'costs' || appRelative.startsWith('costs/')) {
    return {
      shellState: {
        ...shellState,
        activeTabId: 'costs',
      },
      chatOpen: false,
    };
  }

  if (appRelative === 'manifests' || appRelative.startsWith('manifests/')) {
    return {
      shellState: {
        ...shellState,
        activeTabId: 'manifests',
      },
      chatOpen: false,
    };
  }

  const manifestId = asString(params.get('manifest'));
  if (manifestId && manifestId !== 'editor') {
    const manifestTabId = asString(params.get('tab'));
    const dynamicTab: DynamicTab = {
      id: manifestTabShellId(manifestId, manifestTabId),
      kind: 'manifest',
      label: manifestTabId && manifestTabId !== 'main' ? `${manifestId} · ${manifestTabId}` : manifestId,
      closable: true,
      manifestId,
      manifestTabId: manifestTabId || 'main',
    };
    return {
      shellState: {
        ...shellState,
        activeTabId: dynamicTab.id,
        dynamicTabs: [dynamicTab],
      },
      chatOpen: false,
    };
  }

  if (manifestId === 'editor') {
    const target = asString(params.get('target'));
    if (target) {
      const dynamicTab: DynamicTab = {
        id: manifestEditorShellId(target),
        kind: 'manifest-editor',
        label: `Edit ${target}`,
        closable: true,
        manifestId: target,
      };
      return {
        shellState: {
          ...shellState,
          activeTabId: dynamicTab.id,
          dynamicTabs: [dynamicTab],
        },
        chatOpen: false,
      };
    }
  }

  const page = params.get('page');
  if (page === 'costs') {
    return {
      shellState: {
        ...shellState,
        activeTabId: 'costs',
      },
      chatOpen: false,
    };
  }

  if (page === 'manifests') {
    return {
      shellState: {
        ...shellState,
        activeTabId: 'manifests',
      },
      chatOpen: false,
    };
  }

  if (page === 'build' || page === 'moon' || page === 'builder') {
    return {
      shellState: {
        ...shellState,
        activeTabId: 'build',
        buildWorkflowId: asString(params.get('workflow')),
        buildIntent: asString(params.get('intent')),
        buildView: 'moon',
      },
      chatOpen: false,
    };
  }

  if (page === 'run-detail') {
    const runId = asString(params.get('run'));
    if (runId) {
      // Legacy query form — also routes into Moon for consistency.
      return {
        shellState: {
          ...shellState,
          activeTabId: 'build',
          buildView: 'moon',
          moonRunId: runId,
        },
        chatOpen: false,
      };
    }
  }

  if (page === 'edit-model') {
    return {
      shellState: {
        ...shellState,
        activeTabId: 'build',
        buildWorkflowId: asString(params.get('workflow')),
        buildView: 'moon' as BuildView,
        buildIntent: parseEditorSurface(params.get('surface')) ? null : asString(params.get('intent')),
      },
      chatOpen: false,
    };
  }

  return {
    shellState,
    chatOpen: page === 'chat',
  };
}

export function buildShellUrl(state: ShellState, chatOpen: boolean): string {
  // Path-based routes for clean URLs
  if (state.activeTabId === 'build') {
    // Moon-owned run view: /app/run/{runId}
    if (state.moonRunId) {
      return `/app/run/${state.moonRunId}`;
    }
    const params = new URLSearchParams();
    if (state.buildWorkflowId) params.set('workflow', state.buildWorkflowId);
    if (state.buildIntent) params.set('intent', state.buildIntent);
    const query = params.toString();
    return `/app/build${query ? `?${query}` : ''}`;
  }

  if (state.activeTabId === 'costs') {
    return '/app/costs';
  }

  if (state.activeTabId === 'manifests') {
    return '/app/manifests';
  }

  const activeDynamicTab = state.dynamicTabs.find((tab) => tab.id === state.activeTabId) || null;
  if (activeDynamicTab?.kind === 'run-detail' && activeDynamicTab.runId) {
    return `/app/run/${activeDynamicTab.runId}`;
  }

  // Fallback to query params for manifest and other dynamic tabs
  const params = new URLSearchParams();
  if (activeDynamicTab?.kind === 'manifest' && activeDynamicTab.manifestId) {
    params.set('manifest', activeDynamicTab.manifestId);
    if (activeDynamicTab.manifestTabId && activeDynamicTab.manifestTabId !== 'main') {
      params.set('tab', activeDynamicTab.manifestTabId);
    }
  } else if (activeDynamicTab?.kind === 'manifest-editor' && activeDynamicTab.manifestId) {
    params.set('manifest', 'editor');
    params.set('target', activeDynamicTab.manifestId);
  } else if (chatOpen) {
    params.set('page', 'chat');
  }
  const query = params.toString();
  return shellUrl(query ? `?${query}` : '');
}

export function upsertDynamicTab(currentTabs: DynamicTab[], nextTab: DynamicTab): DynamicTab[] {
  const existingIndex = currentTabs.findIndex((tab) => tab.id === nextTab.id);
  if (existingIndex === -1) return [...currentTabs, nextTab];
  const updated = [...currentTabs];
  updated[existingIndex] = nextTab;
  return updated;
}

export function closeDynamicTab(
  currentTabs: DynamicTab[],
  activeTabId: AppTabId,
  targetTabId: string,
): { dynamicTabs: DynamicTab[]; activeTabId: AppTabId } {
  const remaining = currentTabs.filter((tab) => tab.id !== targetTabId);
  if (activeTabId !== targetTabId) {
    return { dynamicTabs: remaining, activeTabId };
  }
  const fallback = remaining[remaining.length - 1]?.id || 'dashboard';
  return { dynamicTabs: remaining, activeTabId: fallback };
}
