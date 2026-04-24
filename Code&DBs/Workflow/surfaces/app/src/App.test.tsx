import '@testing-library/jest-dom';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import { AppShell } from './App';

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
    vi.restoreAllMocks();
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
