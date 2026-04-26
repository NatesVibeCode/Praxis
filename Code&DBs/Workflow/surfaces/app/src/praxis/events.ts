/**
 * Shell open-tab dispatch helper.
 *
 * Historically this fired a window CustomEvent ('praxis-open-tab') consumed by
 * App.tsx. Under the registry-driven shell (decision.shell_navigation_cqrs.20260426)
 * the function maps the (kind, ids) call shape directly to a gateway dispatch
 * via shell.surface.opened — same call sites, no window event bus.
 */
import {
  composeShellId,
  manifestEditorShellId,
  manifestTabShellId,
  runDetailShellId,
  type DynamicTab,
} from '../shell/state';
import { dispatchShellNavigation } from '../shell/dispatchNavigation';
import { getOrCreateSessionAggregate } from '../shell/sessionAggregate';

export interface PraxisOpenTabDetail {
  kind: 'build' | 'manifest' | 'manifest-editor' | 'run-detail' | 'edit-model';
  workflowId?: string | null;
  intent?: string | null;
  manifestId?: string | null;
  tabId?: string | null;
  runId?: string | null;
  editorSurface?: 'definition' | 'plan' | 'run' | 'details' | null;
}

interface OpenTabPlan {
  routeId: string;
  slotValues: Record<string, string | string[]>;
  shellStateDiff: Record<string, unknown>;
}

function planForDetail(detail: PraxisOpenTabDetail): OpenTabPlan | null {
  switch (detail.kind) {
    case 'build':
    case 'edit-model': {
      const workflowId = detail.workflowId ?? null;
      const intent = detail.intent ?? null;
      const slotValues: Record<string, string | string[]> = {};
      if (workflowId) slotValues.workflow = workflowId;
      if (intent) slotValues.intent = intent;
      return {
        routeId: 'route.app.workflow',
        slotValues,
        shellStateDiff: {
          activeTabId: 'build',
          buildWorkflowId: workflowId,
          buildIntent: intent,
          builderSeed: null,
          buildView: 'moon',
          moonRunId: null,
          dashboardDetail: null,
        },
      };
    }
    case 'run-detail': {
      const runId = detail.runId ?? null;
      if (!runId) return null;
      return {
        routeId: 'route.app.run',
        slotValues: { run_id: runId },
        shellStateDiff: {
          activeTabId: 'build',
          buildView: 'moon',
          moonRunId: runId,
          dashboardDetail: null,
        },
      };
    }
    case 'manifest': {
      const manifestId = detail.manifestId ?? null;
      if (!manifestId) return null;
      const manifestTabId = detail.tabId || 'main';
      const dynamicId = manifestTabShellId(manifestId, manifestTabId);
      const tab: DynamicTab = {
        id: dynamicId,
        kind: 'manifest',
        label: manifestTabId === 'main' ? manifestId : `${manifestId} · ${manifestTabId}`,
        closable: true,
        manifestId,
        manifestTabId,
      };
      return {
        routeId: 'route.app.manifest',
        slotValues: { manifest_id: manifestId, manifest_tab_id: manifestTabId },
        shellStateDiff: {
          activeTabId: dynamicId,
          dynamicTabs: [tab],
          dashboardDetail: null,
        },
      };
    }
    case 'manifest-editor': {
      const manifestId = detail.manifestId ?? null;
      if (!manifestId) return null;
      const dynamicId = manifestEditorShellId(manifestId);
      const tab: DynamicTab = {
        id: dynamicId,
        kind: 'manifest-editor',
        label: `Edit ${manifestId}`,
        closable: true,
        manifestId,
      };
      return {
        routeId: 'route.app.manifest_editor',
        slotValues: { manifest_id: manifestId },
        shellStateDiff: {
          activeTabId: dynamicId,
          dynamicTabs: [tab],
          dashboardDetail: null,
        },
      };
    }
    default:
      return null;
  }
}

export function emitPraxisOpenTab(detail: PraxisOpenTabDetail): void {
  const plan = planForDetail(detail);
  if (!plan) return;
  const { sessionAggregateRef } = getOrCreateSessionAggregate();
  void dispatchShellNavigation({
    operation: 'shell.surface.opened',
    input: {
      session_aggregate_ref: sessionAggregateRef,
      route_id: plan.routeId,
      slot_values: plan.slotValues,
      shell_state_diff: plan.shellStateDiff,
      reason: 'event_bus',
      caller_ref: `shell.legacy_event_bus.${detail.kind}`,
    },
  });
  // Trigger a popstate so the App shell rehydrates from the new URL
  // immediately while the SSE projection update is in flight.
  if (typeof window !== 'undefined') {
    const params = new URLSearchParams();
    if (plan.slotValues.workflow) params.set('workflow', String(plan.slotValues.workflow));
    if (plan.slotValues.intent) params.set('intent', String(plan.slotValues.intent));
    // run-detail uses path slot, not query; URL reconstruction happens in
    // useShellState.dispatch.
  }
  void composeShellId; // silence unused-import in environments tree-shaking helpers
  void runDetailShellId;
}
