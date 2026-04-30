/**
 * ShellState types and dynamic-tab helpers.
 *
 * URL parsing and reverse-URL building moved to routeRegistry.ts (driven by
 * ui_shell_route_registry rows). State ownership moved to useShellState.ts
 * (driven by the ui_shell_state.live projection). What remains here are the
 * type definitions and the dynamic-tab id helpers consumed by both.
 *
 * Anchored to decision.shell_navigation_cqrs.20260426.
 */

export type StaticTabId = 'dashboard' | 'build' | 'manifests' | 'atlas';
/** Auxiliary view on the Overview surface (not a primary tab). */
export type DashboardDetail = 'costs' | null;
export type DynamicTabKind = 'run-detail' | 'manifest' | 'manifest-editor' | 'compose';
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
  /** Compose: entity id of the intent being compiled (e.g. 'intent.invoice_approval'). */
  intent?: string | null;
  /** Compose: ordered pill_type ids to bind into template slots. */
  pillRefs?: string[];
}

export interface ShellState {
  /** Registry route_id currently active (e.g. 'route.app.workflow'). */
  activeRouteId: string;
  /** Tab strip / dynamic tab id currently active. Derived from route for static surfaces. */
  activeTabId: AppTabId;
  dynamicTabs: DynamicTab[];
  buildWorkflowId: string | null;
  buildIntent: string | null;
  builderSeed: unknown | null;
  buildView: BuildView;
  /** When set, Moon renders a run-view over its canvas using this run_id. */
  moonRunId: string | null;
  /** Optional drill-in on Overview (e.g. token spend) without a top-level tab. */
  dashboardDetail: DashboardDetail;
}

export function manifestTabShellId(manifestId: string, tabId?: string | null): string {
  return `manifest:${manifestId}:${tabId || 'main'}`;
}

export function manifestWorkspaceLabel(manifestId: string, tabId?: string | null): string {
  const base = manifestId
    .replace(/^blank-workspace-[a-z0-9]+$/i, 'Compose')
    .replace(/^entity-workspace-[a-z0-9]+$/i, 'Entity Workspace')
    .replace(/^workspace-[a-z0-9]+$/i, 'Workspace');
  const label = base === manifestId
    ? manifestId
        .replace(/[-_]+/g, ' ')
        .replace(/\b\w/g, (character) => character.toUpperCase())
    : base;
  return !tabId || tabId === 'main' ? label : `${label} · ${tabId}`;
}

export function manifestEditorShellId(manifestId: string): string {
  return `manifest-editor:${manifestId}`;
}

export function runDetailShellId(runId: string): string {
  return `run-detail:${runId}`;
}

export function composeShellId(intent: string, pillRefs: readonly string[] = []): string {
  const fingerprint = pillRefs.length > 0 ? pillRefs.join('|') : 'no-pills';
  return `compose:${intent}:${fingerprint}`;
}

export function createDefaultShellState(): ShellState {
  return {
    activeRouteId: 'route.app.dashboard',
    activeTabId: 'dashboard',
    dynamicTabs: [],
    buildWorkflowId: null,
    buildIntent: null,
    builderSeed: null,
    buildView: 'moon',
    moonRunId: null,
    dashboardDetail: null,
  };
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
