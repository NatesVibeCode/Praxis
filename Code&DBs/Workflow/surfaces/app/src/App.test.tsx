import '@testing-library/jest-dom';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';

import { AppShell } from './App';
import { clearRoutesForTest, setRoutesForTest, type RouteRegistryRow } from './shell/routeRegistry';
import { clearSessionAggregateForTest } from './shell/sessionAggregate';

const appShellMoonMocks = vi.hoisted(() => ({
  dirty: false,
  message: 'This draft workflow only exists locally.',
}));

vi.mock('./dashboard/Dashboard', () => ({
  Dashboard: ({ onOpenCosts }: { onOpenCosts?: () => void }) => (
    <div>
      <div>Dashboard Surface</div>
      <button type="button" onClick={onOpenCosts}>
        Open spend detail
      </button>
    </div>
  ),
}));

vi.mock('./dashboard/CostsPanel', () => ({
  CostsPanel: () => <div>Costs Surface</div>,
}));

vi.mock('./moon/MoonBuildPage', () => ({
  MoonBuildPage: ({
    workflowId,
    onDraftStateChange,
    onWorkflowCreated,
  }: {
    workflowId?: string | null;
    onDraftStateChange?: (draft: { dirty: boolean; message?: string | null }) => void;
    onWorkflowCreated?: (id: string) => void;
  }) => {
    React.useEffect(() => {
      const dirty = appShellMoonMocks.dirty && !workflowId;
      onDraftStateChange?.(
        dirty
          ? { dirty: true, message: appShellMoonMocks.message }
          : { dirty: false, message: null },
      );
      return () => onDraftStateChange?.({ dirty: false, message: null });
    }, [onDraftStateChange, workflowId]);

    return (
      <div>
        <div>Builder Surface</div>
        <div>Workflow: {workflowId || 'draft'}</div>
        <button type="button" onClick={() => onWorkflowCreated?.('wf-saved')}>
          Save Draft
        </button>
      </div>
    );
  },
}));

vi.mock('./dashboard/RunDetailView', () => ({
  RunDetailView: () => <div>Run Detail Surface</div>,
}));

vi.mock('./dashboard/ChatPanel', () => ({
  ChatPanel: ({ open }: { open: boolean }) => (open ? <div>Chat Surface</div> : null),
}));

vi.mock('./grid/ManifestEditorPage', () => ({
  ManifestEditorPage: () => <div>Manifest Editor Surface</div>,
}));

vi.mock('./praxis/ManifestBundleView', () => ({
  ManifestBundleView: () => <div>Manifest Bundle Surface</div>,
}));

vi.mock('./praxis/ManifestCatalogPage', () => ({
  ManifestCatalogPage: () => <div>Manifest Catalog Surface</div>,
}));

vi.mock('./atlas/AtlasPage', () => ({
  AtlasPage: () => <div>Atlas Surface</div>,
}));

vi.mock('./praxis/SurfaceComposeView', () => ({
  SurfaceComposeView: () => <div>Compose Surface</div>,
}));

const TEST_ROUTES: RouteRegistryRow[] = [
  {
    route_id: 'route.app.dashboard',
    path_template: '/app',
    surface_name: 'dashboard',
    state_effect: 'activeTabId=dashboard',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 10,
    binding_revision: 'test',
    decision_ref: 'test',
    component_ref: 'dashboard/Dashboard.Dashboard',
    tab_kind_label: 'Suite',
    tab_label_template: 'Overview',
    context_label: 'Control plane',
    context_detail_template: '',
    nav_description_template: 'Return to the operating overview.',
    nav_keywords: ['overview', 'dashboard', 'home'],
    event_bus_kind: null,
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: false,
    is_canonical_for_surface: true,
    tab_strip_position: 10,
  },
  {
    route_id: 'route.app.workflow',
    path_template: '/app/workflow',
    surface_name: 'build',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 20,
    binding_revision: 'test',
    decision_ref: 'test',
    component_ref: 'moon/MoonBuildPage.MoonBuildPage',
    tab_kind_label: 'Build',
    tab_label_template: '{{moonRunId ? "Run view" : buildWorkflowId ? "Workflow workspace" : "New workflow"}}',
    context_label: 'App builder',
    context_detail_template: 'Shape the workflow graph.',
    nav_description_template: 'Jump back into Moon Build.',
    nav_keywords: ['build', 'workflow', 'moon'],
    event_bus_kind: 'build',
    keyboard_shortcut: 'ctrl+n',
    draft_guard_required: true,
    is_dynamic: false,
    is_canonical_for_surface: true,
    tab_strip_position: 20,
  },
  {
    route_id: 'route.app.run',
    path_template: '/app/run/{run_id}',
    surface_name: 'build',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 40,
    binding_revision: 'test',
    decision_ref: 'test',
    component_ref: 'moon/MoonBuildPage.MoonBuildPage',
    tab_kind_label: 'Run',
    tab_label_template: 'Run view',
    context_label: 'Run observer',
    context_detail_template: '',
    nav_description_template: 'Return to the active run view.',
    nav_keywords: ['run'],
    event_bus_kind: 'run-detail',
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: false,
    is_canonical_for_surface: false,
    tab_strip_position: null,
  },
  {
    route_id: 'route.app.atlas',
    path_template: '/app/atlas',
    surface_name: 'atlas',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 60,
    binding_revision: 'test',
    decision_ref: 'test',
    component_ref: 'atlas/AtlasPage.AtlasPage',
    tab_kind_label: 'Accent',
    tab_label_template: 'Graph Diagram',
    context_label: 'Knowledge graph',
    context_detail_template: '',
    nav_description_template: 'Open the knowledge-graph diagram.',
    nav_keywords: ['atlas'],
    event_bus_kind: null,
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: false,
    is_canonical_for_surface: true,
    tab_strip_position: 30,
  },
  {
    route_id: 'route.app.manifests',
    path_template: '/app/manifests',
    surface_name: 'manifests',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 50,
    binding_revision: 'test',
    decision_ref: 'test',
    component_ref: 'praxis/ManifestCatalogPage.ManifestCatalogPage',
    tab_kind_label: 'Catalog',
    tab_label_template: 'Manifests',
    context_label: 'Manifest catalog',
    context_detail_template: '',
    nav_description_template: 'Open the manifest catalog.',
    nav_keywords: ['manifest'],
    event_bus_kind: null,
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: false,
    is_canonical_for_surface: true,
    tab_strip_position: 40,
  },
  {
    route_id: 'route.app.dashboard_costs',
    path_template: '/app',
    surface_name: 'dashboard',
    state_effect: '',
    notes: '',
    source_refs: [],
    status: 'ready',
    display_order: 15,
    binding_revision: 'test',
    decision_ref: 'test',
    component_ref: 'dashboard/CostsPanel.CostsPanel',
    tab_kind_label: 'Suite',
    tab_label_template: 'Costs',
    context_label: 'Cost summary',
    context_detail_template: '',
    nav_description_template: 'Open the cost summary drill-in.',
    nav_keywords: ['costs'],
    event_bus_kind: null,
    keyboard_shortcut: null,
    draft_guard_required: false,
    is_dynamic: false,
    is_canonical_for_surface: false,
    tab_strip_position: null,
  },
];

function setupShellMocks() {
  setRoutesForTest(TEST_ROUTES);
  vi.spyOn(globalThis, 'fetch').mockImplementation(async (input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input.toString();
    if (url.startsWith('/api/shell/routes')) {
      return new Response(JSON.stringify({ routes: TEST_ROUTES, count: TEST_ROUTES.length }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    if (url.startsWith('/api/projections/ui_shell_state.live')) {
      return new Response(JSON.stringify({ output: null, last_event_id: null, freshness_status: 'fresh' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    if (url.startsWith('/api/operate')) {
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({}), { status: 200 });
  });

  // Stub EventSource so useShellState's SSE subscription doesn't blow up.
  class StubEventSource {
    onmessage: ((this: EventSource, ev: MessageEvent) => unknown) | null = null;
    onerror: ((this: EventSource, ev: Event) => unknown) | null = null;
    constructor(public url: string) {}
    close() {}
  }
  // @ts-expect-error - test stub
  window.EventSource = StubEventSource;
}

function clickCommandMenuNewWorkflow() {
  const el = document.querySelector<HTMLButtonElement>('[data-menu-item-id="create:builder"]');
  if (!el) throw new Error('expected create:builder in command menu');
  fireEvent.click(el);
}

describe('AppShell', () => {
  beforeEach(() => {
    window.history.replaceState(null, '', '/');
    appShellMoonMocks.dirty = false;
    appShellMoonMocks.message = 'This draft workflow only exists locally.';
    clearSessionAggregateForTest();
    clearRoutesForTest();
    vi.restoreAllMocks();
    setupShellMocks();
  });

  afterEach(() => {
    clearRoutesForTest();
    clearSessionAggregateForTest();
  });

  test('opens the command menu and switches to the new workflow builder surface', async () => {
    render(<AppShell />);

    await screen.findByText('Dashboard Surface');

    fireEvent.click(screen.getByRole('button', { name: /workspace new/i }));

    await screen.findByRole('dialog', { name: /open or create/i });
    clickCommandMenuNewWorkflow();

    await screen.findByText('Builder Surface');
    expect(screen.getByText('App builder')).toBeInTheDocument();
  });

  test('opens spend detail from overview without promoting it to a primary tab', async () => {
    render(<AppShell />);

    await screen.findByText('Dashboard Surface');

    fireEvent.click(screen.getByRole('button', { name: /open spend detail/i }));

    await screen.findByText('Costs Surface');
    expect(screen.getByText('Control plane')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /overview/i })).toHaveAttribute('aria-selected', 'true');
  });

  test('does not leave the builder when escape is pressed', async () => {
    render(<AppShell />);

    await screen.findByText('Dashboard Surface');

    fireEvent.click(screen.getByRole('button', { name: /workspace new/i }));
    await screen.findByRole('dialog', { name: /open or create/i });
    clickCommandMenuNewWorkflow();

    await screen.findByText('Builder Surface');

    fireEvent.keyDown(window, { key: 'Escape' });

    expect(screen.getByText('Builder Surface')).toBeInTheDocument();
    expect(screen.getByText('App builder')).toBeInTheDocument();
    expect(document.querySelector('.app-shell__chrome--collapsed')).toBeInTheDocument();
  });

  test('prompts before leaving a dirty draft builder and stays put when cancelled', async () => {
    appShellMoonMocks.dirty = true;
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(false);

    render(<AppShell />);

    await screen.findByText('Dashboard Surface');

    fireEvent.click(screen.getByRole('button', { name: /workspace new/i }));
    await screen.findByRole('dialog', { name: /open or create/i });
    clickCommandMenuNewWorkflow();

    await screen.findByText('Builder Surface');

    fireEvent.click(screen.getByRole('tab', { name: /overview/i }));

    expect(confirmSpy).toHaveBeenCalledWith('This draft workflow only exists locally.');
    expect(screen.getByText('Builder Surface')).toBeInTheDocument();
    expect(screen.getByText('Workflow: draft')).toBeInTheDocument();
  });

  test('does not block the draft save handoff into a real workflow', async () => {
    appShellMoonMocks.dirty = true;
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(false);

    render(<AppShell />);

    await screen.findByText('Dashboard Surface');

    fireEvent.click(screen.getByRole('button', { name: /workspace new/i }));
    await screen.findByRole('dialog', { name: /open or create/i });
    clickCommandMenuNewWorkflow();

    await screen.findByText('Builder Surface');

    fireEvent.click(screen.getByRole('button', { name: 'Save Draft' }));

    expect(confirmSpy).not.toHaveBeenCalled();
    expect(screen.getByText('Workflow: wf-saved')).toBeInTheDocument();
  });
});
