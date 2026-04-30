import '@testing-library/jest-dom';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import SearchPanelModule from './SearchPanelModule';
import { world } from '../../world';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

describe('SearchPanelModule', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
    world.set('shared.active_source_option', null);
  });

  test('defaults to the global search authority', async () => {
    vi.useFakeTimers();

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/search?q=alpha') {
        return jsonResponse({
          results: [
            { name: 'Alpha Result', description: 'Registry entry', kind: 'registry' },
          ],
        });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <SearchPanelModule
        quadrantId="search-1"
        span={{ cols: 2, rows: 1 }}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText('Search...'), {
      target: { value: 'alpha' },
    });

    await act(async () => {
      vi.advanceTimersByTime(300);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith('/api/search?q=alpha');
    });
  });

  test('uses the active source binding for object search', async () => {
    vi.useFakeTimers();
    world.set('shared.active_source_option', { id: 'workspace_records' });

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/objects?type=doc_type_document&q=alpha') {
        return jsonResponse({
          objects: [
            {
              object_id: 'doc.alpha',
              properties: { title: 'Alpha Policy', summary: 'Workspace record' },
            },
          ],
        });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <SearchPanelModule
        quadrantId="search-1"
        span={{ cols: 2, rows: 1 }}
        config={{
          sourceBindings: {
            workspace_records: { objectType: 'doc_type_document' },
          },
        }}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText('Search...'), {
      target: { value: 'alpha' },
    });

    await act(async () => {
      vi.advanceTimersByTime(300);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith('/api/objects?type=doc_type_document&q=alpha');
    });
    expect(await screen.findByText('Alpha Policy')).toBeInTheDocument();
  });

  test('does not query a disabled active source binding', async () => {
    vi.useFakeTimers();
    world.set('shared.active_source_option', { id: 'connected_crm' });

    const fetchMock = vi.fn();
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <SearchPanelModule
        quadrantId="search-1"
        span={{ cols: 2, rows: 1 }}
        config={{
          sourceBindings: {
            connected_crm: { disabledMessage: 'Connect CRM before searching records.' },
          },
        }}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText('Search...'), {
      target: { value: 'alpha' },
    });

    await act(async () => {
      vi.advanceTimersByTime(300);
      await Promise.resolve();
    });

    expect(fetchMock).not.toHaveBeenCalled();
    expect(screen.getByText('Connect CRM before searching records.')).toBeInTheDocument();
  });
});
