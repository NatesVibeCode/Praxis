import '@testing-library/jest-dom';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import { AppShell } from './App';

vi.mock('./dashboard/Dashboard', () => ({
  Dashboard: () => <div>Dashboard Surface</div>,
}));

vi.mock('./moon/MoonBuildPage', () => ({
  MoonBuildPage: () => <div>Builder Surface</div>,
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

describe('AppShell', () => {
  beforeEach(() => {
    window.history.replaceState(null, '', '/');
  });

  test('opens the command menu and switches to the blank builder surface', async () => {
    render(<AppShell />);

    await screen.findByText('Dashboard Surface');

    fireEvent.click(screen.getByRole('button', { name: /workspace new/i }));

    fireEvent.click(await screen.findByRole('button', { name: /blank builder/i }));

    await screen.findByText('Builder Surface');
    expect(screen.getByText('App builder')).toBeInTheDocument();
  });
});
