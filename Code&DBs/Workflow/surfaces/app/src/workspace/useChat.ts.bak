import { useState, useCallback, useRef } from 'react';

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant' | 'tool_result';
  content: string;
  tool_results?: any;
  model_used?: string;
  latency_ms?: number;
  created_at?: string;
}

export interface Conversation {
  id: string;
  title: string;
  message_count?: number;
  created_at?: string;
  updated_at?: string;
}

const EVENT_STREAM_CONTENT_TYPE = 'text/event-stream';

interface ParsedSseEvent {
  event: string;
  data: any;
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

    // Abort any previous request
    if (abortRef.current) abortRef.current.abort();
    abortRef.current = new AbortController();

    try {
      const res = await fetch(`/api/chat/conversations/${conversationId}/messages`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': EVENT_STREAM_CONTENT_TYPE,
        },
        body: JSON.stringify({ content, selection_context: selectionContext }),
        signal: abortRef.current.signal,
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error ?? `Request failed (${res.status})`);
      }

      const contentType = res.headers.get('content-type') ?? '';
      if (!contentType.includes(EVENT_STREAM_CONTENT_TYPE) || !res.body) {
        appendChatResponse(await res.json());
        setStreamingText('');
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

      setStreamingText('');
      return;
    } catch (err: any) {
      if (err.name !== 'AbortError') {
        setError(err.message);
      }
    } finally {
      setStreamingText('');
      setLoading(false);
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
