import type React from 'react';
import { APP_CONFIG } from '../config';
import type { MenuAction } from '../menu';
import type { DynamicTab, ShellState } from './state';

export interface ShellSurfaceContext {
  label: string;
  detail: string;
}

export interface ShellTabDescriptor {
  id: string;
  label: string;
  kind: string;
  closable: boolean;
}

export type ResolvedShellSurface =
  | {
      category: 'static';
      id: 'dashboard' | 'build' | 'manifests' | 'atlas';
      context: ShellSurfaceContext;
    }
  | {
      category: 'dynamic';
      id: string;
      kind: DynamicTab['kind'];
      dynamicTab: DynamicTab;
      context: ShellSurfaceContext;
    }
  | {
      category: 'unknown';
      id: string;
      context: ShellSurfaceContext;
    };

const STATIC_SURFACES = {
  dashboard: {
    kindLabel: 'Suite',
    getTabLabel: (_state: ShellState) => 'Overview',
    getContext: (_state: ShellState): ShellSurfaceContext => ({
      label: 'Control plane',
      detail: APP_CONFIG.tagline,
    }),
    getNavigateDescription: (_state: ShellState) => 'Return to the operating overview.',
  },
  build: {
    kindLabel: 'Build',
    getTabLabel: (state: ShellState) => (
      state.moonRunId ? 'Run view' : state.buildWorkflowId ? 'Workflow workspace' : 'New workflow'
    ),
    getContext: (state: ShellState): ShellSurfaceContext => (
      state.moonRunId
        ? {
            label: 'Run observer',
            detail: 'Trace the execution graph, inspect receipts, and jump to the source workflow.',
          }
        : {
            label: 'App builder',
            detail: 'Shape the workflow graph, inspect detail, and release from one workspace.',
          }
    ),
    getNavigateDescription: (state: ShellState) => (
      state.moonRunId ? 'Return to the active run view.' : 'Jump back into Moon Build.'
    ),
  },
  manifests: {
    kindLabel: 'Catalog',
    getTabLabel: (_state: ShellState) => 'Manifests',
    getContext: (_state: ShellState): ShellSurfaceContext => ({
      label: 'Manifest catalog',
      detail: 'Discover control-plane manifests before opening them by exact id.',
    }),
    getNavigateDescription: (_state: ShellState) => 'Open the manifest catalog.',
  },
  atlas: {
    kindLabel: 'Accent',
    getTabLabel: (_state: ShellState) => 'Graph Diagram',
    getContext: (_state: ShellState): ShellSurfaceContext => ({
      label: 'Knowledge graph',
      detail: 'Graph view of memory entities and authority-linked edges across the system.',
    }),
    getNavigateDescription: (_state: ShellState) => 'Open the knowledge-graph diagram.',
  },
} satisfies Record<'dashboard' | 'build' | 'manifests' | 'atlas', {
  kindLabel: string;
  getTabLabel: (state: ShellState) => string;
  getContext: (state: ShellState) => ShellSurfaceContext;
  getNavigateDescription: (state: ShellState) => string;
}>;

const DYNAMIC_SURFACES = {
  'run-detail': {
    kindLabel: 'Run',
    getContext: (_tab: DynamicTab): ShellSurfaceContext => ({
      label: 'Run detail',
      detail: 'Trace execution, inspect jobs, and jump back into the builder without losing context.',
    }),
    getNavigateDescription: (_tab: DynamicTab) => 'Open the run detail tab.',
  },
  manifest: {
    kindLabel: 'Surface',
    getContext: (_tab: DynamicTab): ShellSurfaceContext => ({
      label: 'Surface tab',
      detail: 'Review live manifest output alongside the builder and run detail tabs.',
    }),
    getNavigateDescription: (_tab: DynamicTab) => 'Open the surface tab.',
  },
  'manifest-editor': {
    kindLabel: 'Editor',
    getContext: (_tab: DynamicTab): ShellSurfaceContext => ({
      label: 'Manifest editor',
      detail: 'Edit the surface contract directly and reopen the live tab when you are ready.',
    }),
    getNavigateDescription: (_tab: DynamicTab) => 'Open the manifest editor tab.',
  },
  compose: {
    kindLabel: 'Compose',
    getContext: (tab: DynamicTab): ShellSurfaceContext => ({
      label: 'Compose surface',
      detail: tab.intent
        ? `Compiled from ${tab.intent}${tab.pillRefs?.length ? ` + ${tab.pillRefs.length} pill${tab.pillRefs.length === 1 ? '' : 's'}` : ''} via legal_templates projection.`
        : 'Compile an experience template from intent + pills through legal_templates.',
    }),
    getNavigateDescription: (_tab: DynamicTab) => 'Open the composed surface.',
  },
} satisfies Record<DynamicTab['kind'], {
  kindLabel: string;
  getContext: (tab: DynamicTab) => ShellSurfaceContext;
  getNavigateDescription: (tab: DynamicTab) => string;
}>;

const UNKNOWN_SURFACE_CONTEXT: ShellSurfaceContext = {
  label: 'Surface tab',
  detail: 'Review live manifest output alongside the builder and run detail tabs.',
};

export function resolveActiveShellSurface(
  state: ShellState,
  activeDynamicTab: DynamicTab | null,
): ResolvedShellSurface {
  if (
    state.activeTabId === 'dashboard'
    || state.activeTabId === 'build'
    || state.activeTabId === 'manifests'
    || state.activeTabId === 'atlas'
  ) {
    return {
      category: 'static',
      id: state.activeTabId,
      context: STATIC_SURFACES[state.activeTabId].getContext(state),
    };
  }

  if (activeDynamicTab) {
    return {
      category: 'dynamic',
      id: activeDynamicTab.id,
      kind: activeDynamicTab.kind,
      dynamicTab: activeDynamicTab,
      context: DYNAMIC_SURFACES[activeDynamicTab.kind].getContext(activeDynamicTab),
    };
  }

  return {
    category: 'unknown',
    id: state.activeTabId,
    context: UNKNOWN_SURFACE_CONTEXT,
  };
}

export function buildShellTabs(state: ShellState): ShellTabDescriptor[] {
  return [
    {
      id: 'dashboard',
      label: STATIC_SURFACES.dashboard.getTabLabel(state),
      kind: STATIC_SURFACES.dashboard.kindLabel,
      closable: false,
    },
    {
      id: 'build',
      label: STATIC_SURFACES.build.getTabLabel(state),
      kind: state.moonRunId ? 'Run' : STATIC_SURFACES.build.kindLabel,
      closable: false,
    },
    {
      id: 'atlas',
      label: STATIC_SURFACES.atlas.getTabLabel(state),
      kind: STATIC_SURFACES.atlas.kindLabel,
      closable: false,
    },
    {
      id: 'manifests',
      label: STATIC_SURFACES.manifests.getTabLabel(state),
      kind: STATIC_SURFACES.manifests.kindLabel,
      closable: false,
    },
    ...state.dynamicTabs.map((tab) => ({
      id: tab.id,
      label: tab.label,
      kind: DYNAMIC_SURFACES[tab.kind].kindLabel,
      closable: tab.closable,
    })),
  ];
}

export function buildShellNavigationItems(args: {
  state: ShellState;
  chatOpen: boolean;
  activateTab: (tabId: string) => void;
  setChatOpen: React.Dispatch<React.SetStateAction<boolean>>;
}): MenuAction[] {
  const { state, chatOpen, activateTab, setChatOpen } = args;

  const staticItems: MenuAction[] = (['dashboard', 'build', 'atlas', 'manifests'] as const).map((surfaceId) => ({
    id: `navigate:${surfaceId}`,
    label: STATIC_SURFACES[surfaceId].getTabLabel(state),
    description: STATIC_SURFACES[surfaceId].getNavigateDescription(state),
    keywords: surfaceId === 'dashboard'
      ? ['overview', 'dashboard', 'home']
      : surfaceId === 'build'
        ? ['build', 'workflow', 'moon']
        : surfaceId === 'manifests'
          ? ['manifest', 'manifests', 'catalog', 'search', 'list', 'discover', 'control-plane', 'plan', 'approval']
          : ['accent', 'atlas', 'graph', 'diagram', 'knowledge', 'memory', 'entities', 'map', 'overview'],
    selected: state.activeTabId === surfaceId,
    onSelect: () => {
      if (surfaceId === 'build' && state.moonRunId && state.activeTabId === 'build') return;
      activateTab(surfaceId);
    },
  }));

  const dynamicItems: MenuAction[] = state.dynamicTabs.map((tab) => ({
    id: `tab:${tab.id}`,
    label: tab.label,
    description: DYNAMIC_SURFACES[tab.kind].getNavigateDescription(tab),
    keywords: ['tab', tab.kind, tab.label],
    selected: state.activeTabId === tab.id,
    onSelect: () => activateTab(tab.id),
  }));

  return [
    ...staticItems,
    {
      id: 'navigate:chat',
      label: chatOpen ? 'Close Chat' : 'Open Chat',
      description: 'Toggle the side chat surface.',
      keywords: ['chat', 'assistant', 'conversation'],
      shortcut: 'Ctrl+K',
      selected: chatOpen,
      onSelect: () => setChatOpen((open) => !open),
    },
    ...dynamicItems,
  ];
}
