/**
 * Tests for useChat.ts — covers all six bug-fix areas.
 *
 * Run with: jest diff_B_useChat.test.ts
 *
 * Env assumptions:
 *   - jest + ts-jest (or Babel)
 *   - @testing-library/react ≥ 14 (renderHook / act exported)
 *   - Node 18+ (ReadableStream, TextEncoder in globalThis)
 *   - jest.useFakeTimers available
 */

import { renderHook, act, waitFor } from '@testing-library/react';
import { useChat, ChatMessage } from './useChat';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeReadableStream(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
}

function sseBlock(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

function makeSseResponse(blocks: string[]): Response {
  const body = makeReadableStream(blocks);
  return new Response(body, {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  });
}

function makeJsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

let fetchMock: jest.MockedFunction<typeof fetch>;

beforeEach(() => {
  fetchMock = jest.fn();
  global.fetch = fetchMock;
  jest.useRealTimers();
});

afterEach(() => {
  jest.clearAllMocks();
  jest.clearAllTimers();
});

// ---------------------------------------------------------------------------
// Helper: render hook with a pre-seeded conversation
// ---------------------------------------------------------------------------

async function renderWithConversation() {
  fetchMock.mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }));
  const utils = renderHook(() => useChat());
  await act(async () => {
    await utils.result.current.createConversation();
  });
  return utils;
}

// ---------------------------------------------------------------------------
// FIX #2a – abort on unmount
// ---------------------------------------------------------------------------

describe('FIX #2a – abort on unmount', () => {
  it('calls abort() on the in-flight controller when the hook unmounts', async () => {
    // Create a fetch that never resolves so the request stays in-flight.
    let capturedSignal: AbortSignal | undefined;
    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockImplementationOnce((_url, init) => {
        capturedSignal = (init as RequestInit).signal ?? undefined;
        return new Promise(() => {/* never resolves */});
      });

    const { result, unmount } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    act(() => {
      void result.current.sendMessage('hello');
    });

    expect(capturedSignal?.aborted).toBe(false);

    unmount();

    expect(capturedSignal?.aborted).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// FIX #2b – abort previous request on new send
// ---------------------------------------------------------------------------

describe('FIX #2b – abort previous request on new send', () => {
  it('aborts the first in-flight request when a second sendMessage is called', async () => {
    let firstSignal: AbortSignal | undefined;
    let resolveSecond!: (v: Response) => void;

    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockImplementationOnce((_url, init) => {
        firstSignal = (init as RequestInit).signal ?? undefined;
        return new Promise(() => {/* first request never resolves */});
      })
      .mockImplementationOnce(() =>
        new Promise<Response>(res => { resolveSecond = res; }),
      );

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    act(() => { void result.current.sendMessage('first'); });
    expect(firstSignal?.aborted).toBe(false);

    act(() => { void result.current.sendMessage('second'); });
    expect(firstSignal?.aborted).toBe(true);

    // Clean up: resolve the second request to avoid open handles.
    await act(async () => {
      resolveSecond(makeJsonResponse({ id: 'msg-2', content: 'ok' }));
      await Promise.resolve();
    });
  });
});

// ---------------------------------------------------------------------------
// FIX #6 – 60-second timeout
// ---------------------------------------------------------------------------

describe('FIX #6 – 60 s timeout', () => {
  it('aborts the fetch and surfaces a timeout error after 60 seconds', async () => {
    jest.useFakeTimers();

    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockImplementationOnce((_url, init) =>
        new Promise((_resolve, reject) => {
          const signal = (init as RequestInit).signal;
          signal?.addEventListener('abort', () => {
            reject(new DOMException('The operation was aborted.', 'AbortError'));
          });
        }),
      );

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    act(() => { void result.current.sendMessage('slow request'); });

    // Advance past the 60-second timeout.
    await act(async () => {
      jest.advanceTimersByTime(60_001);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
    });

    expect(result.current.error).toMatch(/timed out/i);

    const errMsg = result.current.messages.find(m => m.isError === true);
    expect(errMsg).toBeDefined();
    expect(errMsg?.role).toBe('assistant');
    expect(errMsg?.content).toMatch(/timed out/i);

    jest.useRealTimers();
  });

  it('clears the timeout when the request completes before 60 seconds', async () => {
    jest.useFakeTimers();

    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockResolvedValueOnce(
        makeSseResponse([
          sseBlock('text_delta', { text: 'Hello' }),
          sseBlock('done', { message_id: 'msg-1', model_used: 'claude-test' }),
        ]),
      );

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    await act(async () => {
      await result.current.sendMessage('fast request');
    });

    // Advance well past 60 s — should NOT trigger abort or error.
    act(() => { jest.advanceTimersByTime(90_000); });

    expect(result.current.error).toBeNull();

    jest.useRealTimers();
  });
});

// ---------------------------------------------------------------------------
// FIX #5 – SSE error resets streamingText and surfaces error message
// ---------------------------------------------------------------------------

describe('FIX #5 – SSE error handling', () => {
  it('resets streamingText to empty string after an SSE error event', async () => {
    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockResolvedValueOnce(
        makeSseResponse([
          sseBlock('text_delta', { text: 'Partial ' }),
          sseBlock('error', { message: 'Internal stream failure' }),
        ]),
      );

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    await act(async () => {
      await result.current.sendMessage('trigger error');
    });

    expect(result.current.streamingText).toBe('');
  });

  it('surfaces the SSE error as an error-flavored assistant message', async () => {
    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockResolvedValueOnce(
        makeSseResponse([
          sseBlock('text_delta', { text: 'Partial ' }),
          sseBlock('error', { message: 'Stream exploded' }),
        ]),
      );

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    await act(async () => {
      await result.current.sendMessage('trigger error');
    });

    const errMsg = result.current.messages.find(m => m.isError === true);
    expect(errMsg).toBeDefined();
    expect(errMsg?.role).toBe('assistant');
    expect(errMsg?.content).toBe('Stream exploded');
  });

  it('surfaces fetch-level (non-stream) errors as error-flavored assistant messages', async () => {
    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockResolvedValueOnce(makeJsonResponse({ error: 'Upstream unavailable' }, 503));

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    await act(async () => {
      await result.current.sendMessage('bad request');
    });

    expect(result.current.error).toMatch(/upstream unavailable/i);
    expect(result.current.streamingText).toBe('');

    const errMsg = result.current.messages.find(m => m.isError === true);
    expect(errMsg).toBeDefined();
    expect(errMsg?.role).toBe('assistant');
  });

  it('sets loading to false after an error', async () => {
    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockResolvedValueOnce(makeJsonResponse({ error: 'fail' }, 500));

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    await act(async () => {
      await result.current.sendMessage('fail');
    });

    expect(result.current.loading).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Successful SSE stream
// ---------------------------------------------------------------------------

describe('successful SSE streaming', () => {
  it('accumulates text_delta chunks and commits an assistant message on done', async () => {
    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockResolvedValueOnce(
        makeSseResponse([
          sseBlock('text_delta', { text: 'Hello' }),
          sseBlock('text_delta', { text: ', world!' }),
          sseBlock('done', { message_id: 'msg-abc', model_used: 'claude-test' }),
        ]),
      );

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.createConversation();
    });

    await act(async () => {
      await result.current.sendMessage('hi');
    });

    const assistant = result.current.messages.find(m => m.role === 'assistant');
    expect(assistant).toBeDefined();
    expect(assistant?.content).toBe('Hello, world!');
    expect(assistant?.id).toBe('msg-abc');
    expect(assistant?.model_used).toBe('claude-test');
    expect(result.current.streamingText).toBe('');
    expect(result.current.loading).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it('appends tool_result messages during streaming', async () => {
    const toolData = { summary: 'Query ran', rows: 3 };
    fetchMock
      .mockResolvedValueOnce(makeJsonResponse({ id: 'conv-1' }))
      .mockResolvedValueOnce(
        makeSseResponse([
          sseBlock('tool_result', toolData),
          sseBlock('text_delta', { text: 'Done.' }),
          sseBlock('done', { message_id: 'msg-2' }),
        ]),
      );

    const { result } = renderHook(() => useChat());

    await act(async () => { await result.current.createConversation(); });
    await act(async () => { await result.current.sendMessage('run query'); });

    const toolMsg = result.current.messages.find(m => m.role === 'tool_result');
    expect(toolMsg).toBeDefined();
    expect(toolMsg?.tool_results).toEqual(toolData);
  });
});

// ---------------------------------------------------------------------------
// createConversation / loadConversation
// ---------------------------------------------------------------------------

describe('createConversation', () => {
  it('sets conversationId and clears messages', async () => {
    fetchMock.mockResolvedValueOnce(makeJsonResponse({ id: 'conv-42' }));

    const { result } = renderHook(() => useChat());

    await act(async () => {
      const id = await result.current.createConversation('Test');
      expect(id).toBe('conv-42');
    });

    expect(result.current.conversationId).toBe('conv-42');
    expect(result.current.messages).toHaveLength(0);
    expect(result.current.error).toBeNull();
  });

  it('sets error on network failure and returns null', async () => {
    fetchMock.mockRejectedValueOnce(new Error('Network down'));

    const { result } = renderHook(() => useChat());

    let returned: string | null = 'sentinel';
    await act(async () => {
      returned = await result.current.createConversation();
    });

    expect(returned).toBeNull();
    expect(result.current.error).toBe('Network down');
  });
});

describe('loadConversation', () => {
  it('populates messages from API response', async () => {
    const msgs: ChatMessage[] = [
      { id: 'm1', role: 'user', content: 'Hi' },
      { id: 'm2', role: 'assistant', content: 'Hello' },
    ];
    fetchMock.mockResolvedValueOnce(makeJsonResponse({ messages: msgs }));

    const { result } = renderHook(() => useChat());

    await act(async () => {
      await result.current.loadConversation('conv-99');
    });

    expect(result.current.conversationId).toBe('conv-99');
    expect(result.current.messages).toHaveLength(2);
    expect(result.current.messages[0].content).toBe('Hi');
  });
});

// ---------------------------------------------------------------------------
// Public return shape is unchanged
// ---------------------------------------------------------------------------

describe('hook return shape', () => {
  it('exposes all required public fields', () => {
    const { result } = renderHook(() => useChat());
    const keys = Object.keys(result.current);
    expect(keys).toEqual(
      expect.arrayContaining([
        'conversationId',
        'messages',
        'loading',
        'error',
        'streamingText',
        'createConversation',
        'loadConversation',
        'listConversations',
        'sendMessage',
      ]),
    );
  });
});
