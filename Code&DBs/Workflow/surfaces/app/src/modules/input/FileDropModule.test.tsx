import '@testing-library/jest-dom';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { world } from '../../world';
import { FileDropModule } from './FileDropModule';

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as Response;
}

describe('FileDropModule', () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test('uploads dropped files to the file service and writes the returned record to world state', async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({
        file: {
          id: 'file_123',
          filename: 'brief.txt',
          content_type: 'text/plain',
          size_bytes: 11,
          scope: 'workflow',
          storage_path: 'artifacts/uploads/file_123.txt',
        },
      }),
    );
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const worldSetSpy = vi.spyOn(world, 'set').mockImplementation(() => undefined);

    render(
      <FileDropModule
        quadrantId="quadrant-1"
        span={{ cols: 2, rows: 1 }}
        config={{
          label: 'Drop file here',
          worldPath: 'shared.uploaded_file',
          scope: 'workflow',
          workflowId: 'wf_123',
          description: 'Workflow intake brief',
        }}
      />,
    );

    const dropTarget = screen.getByText('Drop file here').parentElement;
    expect(dropTarget).not.toBeNull();

    const file = new File(['hello world'], 'brief.txt', { type: 'text/plain' });
    fireEvent.drop(dropTarget as HTMLElement, {
      dataTransfer: {
        files: [file],
      },
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalled();
    });

    const [requestUrl, requestInit] = fetchMock.mock.calls[0] as [
      string,
      RequestInit,
    ];
    expect(requestUrl).toBe('/api/files');
    expect(requestInit.method).toBe('POST');

    const body = JSON.parse(String(requestInit.body));
    expect(body).toMatchObject({
      filename: 'brief.txt',
      content_type: 'text/plain',
      scope: 'workflow',
      workflow_id: 'wf_123',
      description: 'Workflow intake brief',
    });
    expect(body.content).toBe('aGVsbG8gd29ybGQ=');

    await waitFor(() => {
      expect(worldSetSpy).toHaveBeenCalledWith(
        'shared.uploaded_file',
        expect.objectContaining({
          id: 'file_123',
          filename: 'brief.txt',
          original_filename: 'brief.txt',
        }),
      );
    });

    expect(await screen.findByText('Uploaded as brief.txt')).toBeInTheDocument();
  });
});
