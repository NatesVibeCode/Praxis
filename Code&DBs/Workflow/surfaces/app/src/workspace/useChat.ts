import { useState, useCallback, useRef, useEffect } from 'react';

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant' | 'tool_result';
  content: string;
  tool_results?: any;
  model_used?: string;
  latency_ms?: number;
  created_at?: string;
  /** True when this message is an error surfaced as an inline assistant bubble. */
  isError?: boolean;
}

export interface Conversation {
  id: string;
  title: string;
  message_count?: number;
  created_at?: string;
  updated_at?: string;
}

const EVENT_STREAM_CONTENT_TYPE = 'text/event-stream';
const FETCH_TIMEOUT_MS = 60_000;

interface ParsedSseEvent {
  event: string;
  data: any;
}

function makeAbortError(): Error {
  if (typeof DOMException !== 'undefined') {
    return new DOMException('Request aborted', 'AbortError');
  }
  const error = new Error('Request aborted');
  error.name = 'AbortError';
  return error;
}

function parseSseEventBlock(block: string): ParsedSseEvent | null {
  const normalized = block.replace(/\r\n/g, '\n');
  if (!normalized.trim()) {
    return null;
  }

  let event = 'message';
  const dataLines: string[] = [];

  for (const line of normalized.split('\n')) {
    if (!line || line.startsWith(':')) {
      continue;
    }
    if (line.startsWith('event:')) {
      event = line.slice('event:'.length).trim() || 'message';
      continue;
    }
    if (line.startsWith('data:')) {
      let value = line.slice('data:'.length);
      if (value.startsWith(' ')) {
        value = value.slice(1);
      }
      dataLines.push(value);
    }
  }

  const rawData = dataLines.join('\n');
  if (!rawData) {
    return { event, data: null };
  }

  try {
    return { event, data: JSON.parse(rawData) };
  } catch {
    return { event, data: rawData };
  }
}

async function readSseEvents(
  response: Response,
  onEvent: (event: ParsedSseEvent) => void,
): Promise<void> {
  if (!response.body) {
    throw new Error('Streaming response body unavailable');
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }

    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
    const blocks = buffer.split('\n\n');
    buffer = blocks.pop() ?? '';

    for (const block of blocks) {
      const parsed = parseSseEventBlock(block);
      if (parsed) {
        onEvent(parsed);
      }
    }
  }

  buffer += decoder.decode();
  const finalBlock = parseSseEventBlock(buffer);
  if (finalBlock) {
    onEvent(finalBlock);
  }
}

export function useChat() {
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [streamingText, setStreamingText] = useState('');
  const abortRef = useRef<AbortController | null>(null);

  // FIX #2 (unmount cleanup): abort any in-flight request when the hook unmounts.
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  const createConversation = useCallback(async (title?: string) => {
    try {
      const res = await fetch('/api/chat/conversations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: title ?? 'New conversation' }),
      });
      const data = await res.json();
      if (data.id) {
        setConversationId(data.id);
        setMessages([]);
        setError(null);
      }
      return data.id;
    } catch (err: any) {
      setError(err.message);
      return null;
    }
  }, []);

  const loadConversation = useCallback(async (id: string) => {
    try {
      const res = await fetch(`/api/chat/conversations/${id}`);
      const data = await res.json();
      if (data.messages) {
        setConversationId(id);
        setMessages(data.messages);
        setError(null);
      }
    } catch (err: any) {
      setError(err.message);
    }
  }, []);

  const listConversations = useCallback(async (): Promise<Conversation[]> => {
    try {
      const res = await fetch('/api/chat/conversations');
      const data = await res.json();
      return data.conversations ?? [];
    } catch {
      return [];
    }
  }, []);

  const appendChatResponse = useCallback((data: any) => {
    const newMessages: ChatMessage[] = [];
    for (const tr of data.tool_results ?? []) {
      newMessages.push({
        id: `tr-${Date.now()}-${Math.random().toString(36)}`,
        role: 'tool_result',
        content: tr.result?.summary ?? '',
        tool_results: tr.result,
      });
    }

    if (data.content) {
      newMessages.push({
        id: data.message_id ?? `msg-${Date.now()}`,
        role: 'assistant',
        content: data.content,
        model_used: data.model_used,
        latency_ms: data.latency_ms,
      });
    }

    setMessages(prev => [...prev, ...newMessages]);
  }, []);

  const sendMessage = useCallback(async (content: string, selectionContext?: any[]) => {
    if (!conversationId || !content.trim()) return;

    // Add user message optimistically
    const userMsg: ChatMessage = {
      id: `temp-${Date.now()}`,
      role: 'user',
      content,
      created_at: new Date().toISOString(),
    };
    setMessages(prev => [...prev, userMsg]);
    setLoading(true);
    setStreamingText('');
    setError(null);

    // FIX #2 (new-send abort): abort any previous in-flight request.
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    // FIX #6: 60-second AbortController-based timeout.
    // Track whether the abort came from the timeout so we can surface a useful message.
    let timedOut = false;
    const timeoutId = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, FETCH_TIMEOUT_MS);

    try {
      let abortListener: (() => void) | null = null;
      const abortPromise = new Promise<never>((_resolve, reject) => {
        const rejectAbort = () => reject(makeAbortError());
        if (controller.signal.aborted) {
          rejectAbort();
          return;
        }
        abortListener = rejectAbort;
        controller.signal.addEventListener('abort', rejectAbort, { once: true });
      });
      const res = await Promise.race([
        fetch(`/api/chat/conversations/${conversationId}/messages`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Accept': EVENT_STREAM_CONTENT_TYPE,
          },
          body: JSON.stringify({ content, selection_context: selectionContext }),
          signal: controller.signal,
        }),
        abortPromise,
      ])
        .finally(() => {
          if (abortListener) {
            controller.signal.removeEventListener('abort', abortListener);
          }
        });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error ?? `Request failed (${res.status})`);
      }

      const contentType = res.headers.get('content-type') ?? '';
      if (!contentType.includes(EVENT_STREAM_CONTENT_TYPE) || !res.body) {
        appendChatResponse(await res.json());
        return;
      }

      let streamedText = '';
      let assistantMessageId: string | null = null;
      let modelUsed: string | undefined;
      let streamError: string | null = null;

      await readSseEvents(res, ({ event, data }) => {
        if (event === 'text_delta') {
          const chunk = typeof data?.text === 'string' ? data.text : '';
          if (!chunk) {
            return;
          }
          streamedText += chunk;
          setStreamingText(prev => prev + chunk);
          return;
        }

        if (event === 'tool_result') {
          setMessages(prev => [
            ...prev,
            {
              id: `tr-${Date.now()}-${Math.random().toString(36)}`,
              role: 'tool_result',
              content: data?.summary ?? '',
              tool_results: data,
            },
          ]);
          return;
        }

        if (event === 'done') {
          assistantMessageId = typeof data?.message_id === 'string' ? data.message_id : null;
          modelUsed = typeof data?.model_used === 'string' ? data.model_used : undefined;
          return;
        }

        if (event === 'error') {
          // FIX #5: capture SSE-level error; streamingText is reset in finally.
          streamError = typeof data?.message === 'string' ? data.message : 'Streaming request failed';
        }
      });

      if (streamError) {
        throw new Error(streamError);
      }

      if (streamedText || assistantMessageId || modelUsed) {
        setMessages(prev => [
          ...prev,
          {
            id: assistantMessageId ?? `msg-${Date.now()}`,
            role: 'assistant',
            content: streamedText,
            model_used: modelUsed,
          },
        ]);
      }

      return;
    } catch (err: any) {
      if (err.name === 'AbortError') {
        // FIX #6: timeout abort — surface a user-readable error message.
        if (timedOut) {
          const msg = 'Request timed out. Please try again.';
          setError(msg);
          // FIX #5: surface error as an error-flavored assistant message.
          setMessages(prev => [
            ...prev,
            {
              id: `err-${Date.now()}`,
              role: 'assistant',
              content: msg,
              isError: true,
            },
          ]);
        }
        // User-initiated abort (new send or unmount): stay silent.
      } else {
        const msg = err.message ?? 'Something went wrong.';
        setError(msg);
        // FIX #5: surface network/SSE errors as an error-flavored assistant message.
        setMessages(prev => [
          ...prev,
          {
            id: `err-${Date.now()}`,
            role: 'assistant',
            content: msg,
            isError: true,
          },
        ]);
      }
    } finally {
      clearTimeout(timeoutId);
      if (abortRef.current === controller) {
        abortRef.current = null;
        // FIX #5: always reset streamingText on completion or error.
        setStreamingText('');
        setLoading(false);
      }
    }
  }, [appendChatResponse, conversationId]);

  return {
    conversationId,
    messages,
    loading,
    error,
    streamingText,
    createConversation,
    loadConversation,
    listConversations,
    sendMessage,
  };
}
