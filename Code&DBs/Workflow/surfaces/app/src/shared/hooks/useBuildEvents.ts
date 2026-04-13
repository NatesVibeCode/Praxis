import { useState, useEffect, useRef, useCallback } from 'react';
import type { BuildEvent } from '../types';

export function useBuildEvents(workflowId: string | null) {
  const [events, setEvents] = useState<BuildEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const esRef = useRef<EventSource | null>(null);
  const retryDelay = useRef(1000);

  const connect = useCallback(() => {
    if (!workflowId) return;

    const url = `/api/workflows/${workflowId}/build/stream`;
    const es = new EventSource(url);
    esRef.current = es;

    es.onopen = () => {
      setConnected(true);
      setError(null);
      retryDelay.current = 1000;
    };

    es.onmessage = (e) => {
      try {
        const event: BuildEvent = JSON.parse(e.data);
        setEvents((prev) => [...prev, event]);
      } catch {
        // ignore malformed events
      }
    };

    es.addEventListener('done', () => {
      es.close();
      setConnected(false);
    });

    es.onerror = () => {
      es.close();
      setConnected(false);
      setError('Connection lost');
      // Exponential backoff reconnect
      const delay = retryDelay.current;
      retryDelay.current = Math.min(delay * 2, 30000);
      setTimeout(connect, delay);
    };
  }, [workflowId]);

  useEffect(() => {
    setEvents([]);
    connect();
    return () => {
      esRef.current?.close();
      esRef.current = null;
    };
  }, [connect]);

  return {
    events,
    latestEvent: events.length > 0 ? events[events.length - 1] : null,
    connected,
    error,
  };
}
