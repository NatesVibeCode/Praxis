import '@testing-library/jest-dom';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, test, vi } from 'vitest';

import { world } from '../../world';
import DataTableModule from './DataTableModule';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status >= 200 && status < 300 ? 'OK' : 'Error',
    json: async () => body,
  } as Response;
}

describe('DataTableModule source bindings', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    world.set('shared.active_source_option', null);
  });

  test('uses the active source binding for object records', async () => {
    world.set('shared.active_source_option', { id: 'workspace_records' });

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/object-types/doc_type_document') {
        return jsonResponse({
          fields: [{ name: 'title', type: 'text' }],
        });
      }
      if (url === '/api/objects?type=doc_type_document') {
        return jsonResponse({
          objects: [
            {
              object_id: 'doc.alpha',
              status: 'active',
              properties: { title: 'Alpha Policy' },
            },
          ],
        });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <DataTableModule
        quadrantId="table-1"
        span={{ cols: 4, rows: 3 }}
        config={{
          sourceBindings: {
            workspace_records: { objectType: 'doc_type_document' },
          },
        }}
      />,
    );

    expect(await screen.findByText('Alpha Policy')).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith('/api/object-types/doc_type_document');
      expect(fetchMock).toHaveBeenCalledWith('/api/objects?type=doc_type_document', expect.any(Object));
    });
  });

  test('shows disabled source message without fetching', () => {
    world.set('shared.active_source_option', { id: 'connected_crm' });

    const fetchMock = vi.fn();
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <DataTableModule
        quadrantId="table-1"
        span={{ cols: 4, rows: 3 }}
        config={{
          sourceBindings: {
            connected_crm: { disabledMessage: 'Connect CRM before listing records.' },
          },
        }}
      />,
    );

    expect(screen.getByText('Connect CRM before listing records.')).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  test('shows source diagnostics for an attached source with no rows', async () => {
    world.set('shared.active_source_option', { id: 'workspace_records', label: 'Workspace Records' });

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === '/api/object-types/doc_type_document') {
        return jsonResponse({
          fields: [{ name: 'title', type: 'text' }],
        });
      }
      if (url === '/api/objects?type=doc_type_document') {
        return jsonResponse({ objects: [] });
      }
      throw new Error(`Unexpected fetch: ${url}`);
    });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(
      <DataTableModule
        quadrantId="table-1"
        span={{ cols: 4, rows: 3 }}
        config={{
          sourceBindings: {
            workspace_records: {
              objectType: 'doc_type_document',
              emptyMessage: 'No workspace records yet',
              emptyDetail: 'The source is attached; the workspace object store returned no records.',
            },
          },
        }}
      />,
    );

    expect(await screen.findByText('No workspace records yet')).toBeInTheDocument();
    expect(screen.getByText('Source: Workspace Records')).toBeInTheDocument();
    expect(screen.getByText('Object type: doc_type_document')).toBeInTheDocument();
    expect(screen.getByText('Rows: 0')).toBeInTheDocument();
  });
});
