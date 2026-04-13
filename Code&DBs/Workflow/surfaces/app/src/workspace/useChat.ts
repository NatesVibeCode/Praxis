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
          'Accept': 'application/json',
        },
        body: JSON.stringify({ content, selection_context: selectionContext }),
        signal: abortRef.current.signal,
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error ?? `Request failed (${res.status})`);
      }

      // JSON response (blocking — tool loop runs server-side)
      const data = await res.json();

      // Add tool results as separate messages BEFORE the assistant text
      const newMessages: ChatMessage[] = [];
      for (const tr of data.tool_results ?? []) {
        newMessages.push({
          id: `tr-${Date.now()}-${Math.random().toString(36)}`,
          role: 'tool_result',
          content: tr.result?.summary ?? '',
          tool_results: tr.result,
        });
      }

      // Add assistant response
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
      setLoading(false);
      return;
    } catch (err: any) {
      if (err.name !== 'AbortError') {
        setError(err.message);
      }
    } finally {
      setLoading(false);
    }
  }, [conversationId]);

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
