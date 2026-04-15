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
      id: 'dashboard' | 'build' | 'costs';
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
    getTabLabel: (state: ShellState) => (state.buildWorkflowId ? 'Workflow workspace' : 'New workflow'),
    getContext: (_state: ShellState): ShellSurfaceContext => ({
      label: 'App builder',
      detail: 'Shape the workflow graph, inspect detail, and release from one workspace.',
    }),
    getNavigateDescription: (_state: ShellState) => 'Jump back into Moon Build.',
  },
  costs: {
    kindLabel: 'Finance',
    getTabLabel: (_state: ShellState) => 'Cost Summary',
    getContext: (_state: ShellState): ShellSurfaceContext => ({
      label: 'Cost ledger',
      detail: 'Review token spend, recent receipts, and per-run cost concentration.',
    }),
    getNavigateDescription: (_state: ShellState) => 'Inspect token spend and recent costed runs.',
  },
} satisfies Record<'dashboard' | 'build' | 'costs', {
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
  if (state.activeTabId === 'dashboard' || state.activeTabId === 'build' || state.activeTabId === 'costs') {
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
      kind: STATIC_SURFACES.build.kindLabel,
      closable: false,
    },
    {
      id: 'costs',
      label: STATIC_SURFACES.costs.getTabLabel(state),
      kind: STATIC_SURFACES.costs.kindLabel,
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

  const staticItems: MenuAction[] = (['dashboard', 'build', 'costs'] as const).map((surfaceId) => ({
    id: `navigate:${surfaceId}`,
    label: STATIC_SURFACES[surfaceId].getTabLabel(state),
    description: STATIC_SURFACES[surfaceId].getNavigateDescription(state),
    keywords: surfaceId === 'dashboard'
      ? ['overview', 'dashboard', 'home']
      : surfaceId === 'build'
        ? ['build', 'workflow', 'moon']
        : ['costs', 'spend', 'finance', 'ledger'],
    selected: state.activeTabId === surfaceId,
    onSelect: () => activateTab(surfaceId),
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
